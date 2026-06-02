use std::env;
use std::fs;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde_json::{json, Value};
use tokio::sync::Semaphore;

const DEFAULT_RPC_URL: &str = "http://slc.rpc.orbitflare.com";
const DEFAULT_BATCH_SIZE: usize = 10;
const DEFAULT_NUM_BLOCKS: usize = 1000;
const MAX_CONCURRENT_REQUESTS: usize = 20;
const REQUEST_TIMEOUT_SECS: u64 = 30;
const MAX_RETRY_ATTEMPTS: u32 = 3;
const INITIAL_RETRY_DELAY_MS: u64 = 1000;

struct RpcConfig {
    url: String,
    keys: Vec<String>,
    batch_size: usize,
}

struct FetchStats {
    block_ok: AtomicU64,
    block_err: AtomicU64,
    batch_ok: AtomicU64,
    batch_err: AtomicU64,
    per_key_ok: Vec<AtomicU64>,
    per_key_err: Vec<AtomicU64>,
}

impl FetchStats {
    fn new(n_keys: usize) -> Self {
        Self {
            block_ok: AtomicU64::new(0),
            block_err: AtomicU64::new(0),
            batch_ok: AtomicU64::new(0),
            batch_err: AtomicU64::new(0),
            per_key_ok: (0..n_keys).map(|_| AtomicU64::new(0)).collect(),
            per_key_err: (0..n_keys).map(|_| AtomicU64::new(0)).collect(),
        }
    }
}

fn load_config() -> Result<RpcConfig, Box<dyn std::error::Error>> {
    let rpc_url = env::var("RPC_URL").unwrap_or_else(|_| DEFAULT_RPC_URL.to_string());

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

    let batch_size: usize = env::var("BATCH_SIZE")
        .unwrap_or_else(|_| DEFAULT_BATCH_SIZE.to_string())
        .parse()
        .unwrap_or(DEFAULT_BATCH_SIZE);

    Ok(RpcConfig {
        url: rpc_url,
        keys,
        batch_size,
    })
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
        .post(format!("{}?api_key={}", rpc_url, api_key))
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
            .post(format!("{}?api_key={}", rpc_url, api_key))
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

    // Take the last `num_blocks` blocks (most recent)
    let slots: Vec<u64> = all_blocks
        .into_iter()
        .rev()
        .take(num_blocks)
        .rev()
        .collect();
    Ok(slots)
}

async fn fetch_batch_with_retry(
    client: reqwest::Client,
    url: String,
    batch_reqs: Vec<Value>,
    slot_batch: Vec<u64>,
    stats: Arc<FetchStats>,
    key_idx: usize,
) {
    let mut last_error = None;

    for attempt in 0..MAX_RETRY_ATTEMPTS {
        match client.post(&url).json(&batch_reqs).send().await {
            Ok(resp) => {
                if resp.status().is_success() {
                    stats.batch_ok.fetch_add(1, Ordering::Relaxed);
                    stats.per_key_ok[key_idx].fetch_add(1, Ordering::Relaxed);

                    match resp.json::<Value>().await {
                        Ok(results) => {
                            if let Some(arr) = results.as_array() {
                                for item in arr {
                                    let req_id = item["id"].as_u64().unwrap_or(0) as usize;
                                    if req_id == 0 || req_id > slot_batch.len() {
                                        continue;
                                    }
                                    let slot = slot_batch[req_id - 1];

                                    if let Some(block) = item.get("result") {
                                        if block.is_object() {
                                            if let Ok(text) = serde_json::to_string_pretty(block) {
                                                let path = format!("raw/{}.txt", slot);
                                                if let Err(e) = fs::write(&path, &text) {
                                                    eprintln!("Warning: failed to write {}: {}", path, e);
                                                }
                                            }
                                            stats.block_ok.fetch_add(1, Ordering::Relaxed);
                                        } else if block.is_null() {
                                            stats.block_err.fetch_add(1, Ordering::Relaxed);
                                        }
                                    } else if item.get("error").is_some() {
                                        stats.block_err.fetch_add(1, Ordering::Relaxed);
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            eprintln!("Warning: failed to parse batch response: {}", e);
                            stats.batch_err.fetch_add(1, Ordering::Relaxed);
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

        // Exponential backoff before retry
        if attempt < MAX_RETRY_ATTEMPTS - 1 {
            let delay = Duration::from_millis(INITIAL_RETRY_DELAY_MS * 2u64.pow(attempt));
            tokio::time::sleep(delay).await;
        }
    }

    // All retries exhausted
    eprintln!(
        "Error: batch failed after {} attempts: {}",
        MAX_RETRY_ATTEMPTS,
        last_error.unwrap_or_else(|| "unknown error".to_string())
    );
    stats.batch_err.fetch_add(1, Ordering::Relaxed);
    stats.per_key_err[key_idx].fetch_add(1, Ordering::Relaxed);
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = load_config()?;
    let n_keys = config.keys.len();

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(REQUEST_TIMEOUT_SECS))
        .build()?;

    // Ensure raw/ directory exists
    fs::create_dir_all("raw")?;

    // Determine slots to fetch
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
        "Fetching {} blocks (slots {}-{}), {} per batch, {} keys",
        actual, first_slot, last_slot, config.batch_size, n_keys
    );

    let batches: Vec<Vec<u64>> = slots.chunks(config.batch_size).map(|c| c.to_vec()).collect();
    let total_batches = batches.len();
    println!("Total batches: {}", total_batches);

    let total_start = Instant::now();
    let stats = Arc::new(FetchStats::new(n_keys));
    let semaphore = Arc::new(Semaphore::new(MAX_CONCURRENT_REQUESTS));
    let keys = Arc::new(config.keys);

    let mut handles = Vec::with_capacity(total_batches);

    for (batch_idx, slot_batch) in batches.into_iter().enumerate() {
        let client = client.clone();
        let rpc_url = config.url.clone();
        let keys = Arc::clone(&keys);
        let stats = Arc::clone(&stats);
        let semaphore = Arc::clone(&semaphore);

        let handle = tokio::spawn(async move {
            let _permit = semaphore.acquire().await.expect("semaphore closed");

            let key_idx = batch_idx % keys.len();
            let url = format!("{}?api_key={}", rpc_url.trim_end_matches('/'), keys[key_idx]);

            let mut batch_reqs = Vec::with_capacity(slot_batch.len());
            for (i, &slot) in slot_batch.iter().enumerate() {
                batch_reqs.push(json!({
                    "jsonrpc": "2.0",
                    "id": i + 1,
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
                }));
            }

            fetch_batch_with_retry(client, url, batch_reqs, slot_batch, stats, key_idx).await;
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
    let b_ok = stats.batch_ok.load(Ordering::Relaxed);
    let b_err = stats.batch_err.load(Ordering::Relaxed);
    let dur_s = total_elapsed.as_secs_f64();

    println!();
    println!("=== Results ===");
    println!("  Duration:     {:.2}s", dur_s);
    println!("  Batches ok:   {}/{} ({} err)", b_ok, total_batches, b_err);
    println!("  Blocks ok:    {}", total_ok);
    println!("  Blocks err:   {}", total_err);
    if dur_s > 0.0 {
        println!("  Blocks/s:     {:.0}", total_ok as f64 / dur_s);
    }
    println!();
    println!("  Per-key batch distribution:");
    for idx in 0..n_keys {
        let ok = stats.per_key_ok[idx].load(Ordering::Relaxed);
        let err = stats.per_key_err[idx].load(Ordering::Relaxed);
        println!("    key{}: {} batches ok, {} err", idx + 1, ok, err);
    }

    Ok(())
}
