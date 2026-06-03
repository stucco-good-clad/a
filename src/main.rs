use std::env;
use std::fs;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde_json::{json, Value};
use tokio::sync::Semaphore;

const DEFAULT_NUM_BLOCKS: usize = 1000;
const MAX_CONCURRENT_REQUESTS: usize = 40;
const REQUEST_TIMEOUT_SECS: u64 = 30;
const MAX_RETRY_ATTEMPTS: u32 = 3;
const INITIAL_RETRY_DELAY_MS: u64 = 1000;

struct RpcConfig {
    url: String,
    keys: Vec<String>,
}

struct FetchStats {
    block_ok: AtomicU64,
    block_err: AtomicU64,
    block_filtered: AtomicU64,
    tx_total: AtomicU64,
    tx_kept: AtomicU64,
    req_ok: AtomicU64,
    req_err: AtomicU64,
    per_key_ok: Vec<AtomicU64>,
    per_key_err: Vec<AtomicU64>,
}

impl FetchStats {
    fn new(n_keys: usize) -> Self {
        Self {
            block_ok: AtomicU64::new(0),
            block_err: AtomicU64::new(0),
            block_filtered: AtomicU64::new(0),
            tx_total: AtomicU64::new(0),
            tx_kept: AtomicU64::new(0),
            req_ok: AtomicU64::new(0),
            req_err: AtomicU64::new(0),
            per_key_ok: (0..n_keys).map(|_| AtomicU64::new(0)).collect(),
            per_key_err: (0..n_keys).map(|_| AtomicU64::new(0)).collect(),
        }
    }
}

fn load_config() -> Result<RpcConfig, Box<dyn std::error::Error>> {
    let url = env::var("RPC_URLS")
        .map(|v| v.split(',').next().unwrap_or("").trim().to_string())
        .unwrap_or_else(|_| "https://solana.api.onfinality.io/rpc".to_string());

    let mut keys: Vec<String> = Vec::new();
    for n in 1.. {
        match env::var(format!("KEY_{}", n)) {
            Ok(val) => keys.push(val),
            Err(_) => break,
        }
    }
    if keys.is_empty() {
        return Err("At least KEY_1 must be set".into());
    }

    println!("RPC URL: {}", url);
    println!("Keys: {} (round-robin, ~10 RPS each)", keys.len());

    Ok(RpcConfig { url, keys })
}

fn read_slots_from_file(path: &str) -> Result<Vec<u64>, Box<dyn std::error::Error>> {
    println!("Reading slots from file: {}", path);
    let content = fs::read_to_string(path)
        .map_err(|e| format!("Failed to read slots file '{}': {}", path, e))?;
    let slots: Vec<u64> = content
        .lines()
        .filter_map(|l| {
            let t = l.trim();
            if t.is_empty() {
                None
            } else {
                t.parse::<u64>().ok()
            }
        })
        .collect();
    println!("Loaded {} slots from file", slots.len());
    Ok(slots)
}

async fn get_current_slot(
    client: &reqwest::Client,
    rpc_url: &str,
    api_key: &str,
) -> Result<u64, Box<dyn std::error::Error>> {
    println!("Getting current slot...");
    let body = json!({
        "jsonrpc": "2.0", "id": 1, "method": "getSlot", "params": []
    });
    let resp: Value = client
        .post(format!("{}?apikey={}", rpc_url, api_key))
        .json(&body)
        .send()
        .await?
        .json()
        .await?;
    let current = resp["result"]
        .as_u64()
        .ok_or("failed to parse current slot")?;
    println!("Current slot: {}", current);
    Ok(current)
}

async fn discover_slots(
    client: &reqwest::Client,
    rpc_url: &str,
    api_key: &str,
    num_blocks: usize,
    range_start: Option<u64>,
    range_end: Option<u64>,
) -> Result<Vec<u64>, Box<dyn std::error::Error>> {
    let search_end = if let Some(end) = range_end {
        end
    } else {
        let current = get_current_slot(client, rpc_url, api_key).await?;
        current.saturating_sub(2)
    };

    let search_start = range_start.unwrap_or_else(|| search_end.saturating_sub(num_blocks as u64 * 3).max(search_end.saturating_sub(999)));

    let max_search = 500_000u64;
    let mut search_range = (search_end.saturating_sub(search_start) + 1).max(1000);
    let mut all_blocks: Vec<u64> = Vec::new();

    while all_blocks.len() < num_blocks && search_range <= max_search {
        let range_start = search_end.saturating_sub(search_range - 1);
        println!(
            "Querying getBlocks({}, {}) ...",
            range_start, search_end
        );
        let body = json!({
            "jsonrpc": "2.0", "id": 1, "method": "getBlocks",
            "params": [range_start, search_end]
        });
        let resp: Value = client
            .post(format!("{}?apikey={}", rpc_url, api_key))
            .json(&body)
            .send()
            .await?
            .json()
            .await?;
        all_blocks = resp["result"]
            .as_array()
            .map(|arr| arr.iter().filter_map(|v| v.as_u64()).collect())
            .unwrap_or_default();
        println!("  Found {} valid blocks", all_blocks.len());
        if all_blocks.len() < num_blocks {
            let next = (search_range * 2).min(max_search);
            println!("  Need {}, expanding search window to {}", num_blocks, next);
            search_range = next;
        }
    }

    if all_blocks.len() < num_blocks {
        eprintln!(
            "WARNING: only {} valid blocks in range, taking all of them",
            all_blocks.len()
        );
    }

    let slots: Vec<u64> = all_blocks
        .into_iter()
        .rev()
        .take(num_blocks)
        .rev()
        .collect();
    Ok(slots)
}

fn filter_block(block: &Value) -> Option<(Value, u64, u64, u64)> {
    let txs = block.get("transactions").and_then(|v| v.as_array())?;

    let mut total = 0u64;
    let mut kept = 0u64;
    let filtered_txs: Vec<Value> = txs
        .iter()
        .filter(|tx| {
            total += 1;
            let has_inner = tx
                .get("meta")
                .and_then(|m| m.get("innerInstructions"))
                .and_then(|i| i.as_array())
                .map(|arr| !arr.is_empty())
                .unwrap_or(false);
            if has_inner {
                kept += 1;
            }
            has_inner
        })
        .cloned()
        .collect();

    let dropped = total - kept;
    let mut block = block.clone();
    if let Some(obj) = block.as_object_mut() {
        obj.insert(
            "transactions".to_string(),
            Value::Array(filtered_txs),
        );
    }
    Some((block, total, kept, dropped))
}

fn write_block(slot: u64, block: &Value, stats: &FetchStats) {
    match serde_json::to_string(block) {
        Ok(text) => {
            let path = format!("raw/{}.txt", slot);
            match fs::write(&path, &text) {
                Ok(()) => {
                    stats.block_ok.fetch_add(1, Ordering::Relaxed);
                }
                Err(e) => {
                    eprintln!("Warning: failed to write {}: {}", path, e);
                    stats.block_err.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
        Err(e) => {
            eprintln!("Warning: failed to serialize block {}: {}", slot, e);
            stats.block_err.fetch_add(1, Ordering::Relaxed);
        }
    }
}

async fn fetch_block_with_retry(
    client: reqwest::Client,
    url: String,
    slot: u64,
    stats: Arc<FetchStats>,
    key_idx: usize,
) {
    let body = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getBlock",
        "params": [
            slot,
            {
                "encoding": "json",
                "transactionDetails": "full",
                "rewards": false,
                "maxSupportedTransactionVersion": 0
            }
        ]
    });

    let mut last_error = None;

    for attempt in 0..MAX_RETRY_ATTEMPTS {
        match client.post(&url).json(&body).send().await {
            Ok(resp) => {
                if resp.status().is_success() {
                    stats.req_ok.fetch_add(1, Ordering::Relaxed);
                    stats.per_key_ok[key_idx].fetch_add(1, Ordering::Relaxed);

                    match resp.json::<Value>().await {
                        Ok(result) => {
                            if let Some(block) = result.get("result") {
                                if block.is_object() {
                                    stats.tx_total.fetch_add(
                                        block
                                            .get("transactions")
                                            .and_then(|v| v.as_array())
                                            .map(|a| a.len() as u64)
                                            .unwrap_or(0),
                                        Ordering::Relaxed,
                                    );
                                    if let Some((filtered, _total, kept, dropped)) =
                                        filter_block(block)
                                    {
                                        stats.tx_kept.fetch_add(kept, Ordering::Relaxed);
                                        if dropped > 0 {
                                            stats
                                                .block_filtered
                                                .fetch_add(1, Ordering::Relaxed);
                                        }
                                        write_block(slot, &filtered, &stats);
                                    }
                                } else if block.is_null() {
                                    stats.block_err.fetch_add(1, Ordering::Relaxed);
                                }
                            } else if result.get("error").is_some() {
                                stats.block_err.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                        Err(e) => {
                            eprintln!("Warning: failed to parse response for slot {}: {}", slot, e);
                            stats.req_err.fetch_add(1, Ordering::Relaxed);
                            stats.per_key_err[key_idx].fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    return;
                } else {
                    last_error = Some(format!("HTTP {}", resp.status()));
                }
            }
            Err(e) => {
                last_error = Some(e.to_string());
            }
        }

        if attempt < MAX_RETRY_ATTEMPTS - 1 {
            let delay = Duration::from_millis(INITIAL_RETRY_DELAY_MS * 2u64.pow(attempt));
            tokio::time::sleep(delay).await;
        }
    }

    eprintln!(
        "Error: slot {} failed after {} attempts: {}",
        slot,
        MAX_RETRY_ATTEMPTS,
        last_error.unwrap_or_else(|| "unknown error".to_string())
    );
    stats.req_err.fetch_add(1, Ordering::Relaxed);
    stats.per_key_err[key_idx].fetch_add(1, Ordering::Relaxed);
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = load_config()?;
    let n_keys = config.keys.len();

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(REQUEST_TIMEOUT_SECS))
        .build()?;

    fs::create_dir_all("raw")?;

    let args: Vec<String> = env::args().collect();
    let slots: Vec<u64> = if args.len() > 2 && args[1] == "--slots-file" {
        read_slots_from_file(&args[2])?
    } else {
        let num_blocks: usize = env::var("NUM_BLOCKS")
            .unwrap_or_else(|_| DEFAULT_NUM_BLOCKS.to_string())
            .parse()
            .unwrap_or(DEFAULT_NUM_BLOCKS);

        let range_start = env::var("RANGE_START")
            .ok()
            .and_then(|s| s.parse::<u64>().ok());
        let range_end = env::var("RANGE_END")
            .ok()
            .and_then(|s| s.parse::<u64>().ok());

        discover_slots(&client, &config.url, &config.keys[0], num_blocks, range_start, range_end).await?
    };

    if slots.is_empty() {
        return Err("No slots to fetch".into());
    }

    let actual = slots.len();
    let first_slot = slots[0];
    let last_slot = slots[slots.len() - 1];

    println!();
    println!("--- Slot range resolved ---");
    println!("  Blocks: {}", actual);
    println!("  Range: slot {} to {}", first_slot, last_slot);

    fs::write("range.txt", format!("{}-{}", first_slot, last_slot))?;

    println!(
        "Fetching {} blocks (slots {}-{}), {} keys, individual requests",
        actual, first_slot, last_slot, n_keys
    );

    let max_concurrent: usize = env::var("MAX_CONCURRENT")
        .unwrap_or_else(|_| MAX_CONCURRENT_REQUESTS.to_string())
        .parse()
        .unwrap_or(MAX_CONCURRENT_REQUESTS);

    let total_start = Instant::now();
    let stats = Arc::new(FetchStats::new(n_keys));
    let semaphore = Arc::new(Semaphore::new(max_concurrent));
    let keys = Arc::new(config.keys);
    let rpc_url = Arc::new(config.url);

    let mut handles = Vec::with_capacity(actual);

    for (slot_idx, &slot) in slots.iter().enumerate() {
        let client = client.clone();
        let keys = Arc::clone(&keys);
        let rpc_url = Arc::clone(&rpc_url);
        let stats = Arc::clone(&stats);
        let semaphore = Arc::clone(&semaphore);

        let handle = tokio::spawn(async move {
            let _permit = semaphore.acquire().await.expect("semaphore closed");

            let key_idx = slot_idx % keys.len();
            let url = format!("{}?apikey={}", rpc_url, keys[key_idx]);

            fetch_block_with_retry(client, url, slot, stats, key_idx).await;
        });
        handles.push(handle);
    }

    for h in handles {
        match h.await {
            Ok(_) => {}
            Err(e) => eprintln!("Warning: task panicked: {}", e),
        }
    }

    let total_elapsed = total_start.elapsed();
    let total_ok = stats.block_ok.load(Ordering::Relaxed);
    let total_err = stats.block_err.load(Ordering::Relaxed);
    let r_ok = stats.req_ok.load(Ordering::Relaxed);
    let r_err = stats.req_err.load(Ordering::Relaxed);
    let dur_s = total_elapsed.as_secs_f64();

    println!();
    println!("=== Results ===");
    println!("  Duration:     {:.2}s", dur_s);
    println!("  Requests ok:  {} ({} err)", r_ok, r_err);
    println!("  Blocks saved: {}", total_ok);
    println!("  Blocks err:   {}", total_err);
    let total_filtered = stats.block_filtered.load(Ordering::Relaxed);
    let total_tx = stats.tx_total.load(Ordering::Relaxed);
    let total_kept = stats.tx_kept.load(Ordering::Relaxed);
    let tx_dropped = total_tx - total_kept;
    if total_filtered > 0 {
        println!("  Blocks with filtered txs: {}", total_filtered);
        println!("  Transactions: {} total, {} kept, {} dropped (null inner)", total_tx, total_kept, tx_dropped);
    }
    if dur_s > 0.0 {
        println!("  Blocks/s:     {:.0}", total_ok as f64 / dur_s);
    }
    println!();
    println!("  Per-key distribution:");
    for idx in 0..n_keys {
        let ok = stats.per_key_ok[idx].load(Ordering::Relaxed);
        let err = stats.per_key_err[idx].load(Ordering::Relaxed);
        println!("    key{}: {} ok, {} err", idx + 1, ok, err);
    }

    Ok(())
}
