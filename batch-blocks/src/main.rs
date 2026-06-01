use std::env;
use std::fs;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde_json::{json, Value};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let rpc_url = env::var("RPC_URL")
        .unwrap_or_else(|_| "http://slc.rpc.orbitflare.com".to_string());

    let mut keys: Vec<String> = Vec::new();
    for n in 1.. {
        match env::var(format!("KEY_{}", n)) {
            Ok(val) => keys.push(val),
            Err(_) => break,
        }
    }
    assert!(!keys.is_empty(), "At least KEY_1 must be set");
    let n_keys = keys.len();

    let batch_size: usize = env::var("BATCH_SIZE")
        .unwrap_or_else(|_| "10".to_string())
        .parse()
        .unwrap_or(10);

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(120))
        .build()?;

    // Check for --slots-file flag (worker mode: skip discovery, read slots from file)
    let args: Vec<String> = env::args().collect();
    let slots: Vec<u64> = if args.len() > 2 && args[1] == "--slots-file" {
        let path = &args[2];
        println!("Reading slots from file: {}", path);
        let content = fs::read_to_string(path)
            .unwrap_or_else(|e| panic!("Failed to read slots file '{}': {}", path, e));
        let s: Vec<u64> = content
            .lines()
            .filter_map(|l| {
                let t = l.trim();
                if t.is_empty() { None } else { t.parse::<u64>().ok() }
            })
            .collect();
        println!("Loaded {} slots from file", s.len());
        s
    } else {
        // Coordinator mode: discover valid blocks via getBlocks
        let num_blocks: usize = env::var("NUM_BLOCKS")
            .unwrap_or_else(|_| "1000".to_string())
            .parse()
            .unwrap_or(1000);

        let search_end = if let (Ok(_s), Ok(e)) =
            (env::var("RANGE_START"), env::var("RANGE_END"))
        {
            e.parse::<u64>()?
        } else {
            println!("Getting current slot...");
            let body = json!({
                "jsonrpc": "2.0", "id": 1, "method": "getSlot", "params": []
            });
            let resp: Value = client
                .post(format!("{}?api_key={}", &rpc_url, keys[0]))
                .json(&body)
                .send()
                .await?
                .json()
                .await?;
            let current = resp["result"].as_u64().expect("failed to parse slot");
            println!("Current slot: {}", current);
            current.saturating_sub(2)
        };

        let max_search = 500_000u64;
        let mut search_range = (num_blocks as u64 * 3).max(1000);
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
                .post(format!("{}?api_key={}", &rpc_url, keys[0]))
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
                "WARNING: only {} valid blocks in 500K range, taking all of them",
                all_blocks.len()
            );
        }

        let s: Vec<u64> = all_blocks
            .into_iter()
            .rev()
            .take(num_blocks)
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .collect();
        s
    };

    let actual = slots.len();
    let first_slot = slots.first().copied().unwrap_or(0);
    let last_slot = slots.last().copied().unwrap_or(0);

    println!();
    println!("--- Dynamic range resolved via getBlocks ---");
    println!("  Requested: {} blocks", actual);
    println!("  Actual range: slot {} to {} ({} blocks)", first_slot, last_slot, actual);

    // Output the actual range
    fs::write("range.txt", format!("{}-{}", first_slot, last_slot))?;

    println!(
        "Fetching {} blocks (slots {}-{}), {} per batch, {} keys",
        actual, first_slot, last_slot, batch_size, n_keys
    );

    let batches: Vec<Vec<u64>> = slots.chunks(batch_size).map(|c| c.to_vec()).collect();
    let total_batches = batches.len();
    println!("Total batches: {}", total_batches);

    let total_start = Instant::now();

    let block_ok = Arc::new(AtomicU64::new(0));
    let block_err = Arc::new(AtomicU64::new(0));
    let batch_ok = Arc::new(AtomicU64::new(0));
    let batch_err = Arc::new(AtomicU64::new(0));

    let per_key_ok: Vec<Arc<AtomicU64>> = (0..n_keys).map(|_| Arc::new(AtomicU64::new(0))).collect();
    let per_key_err: Vec<Arc<AtomicU64>> = (0..n_keys).map(|_| Arc::new(AtomicU64::new(0))).collect();

    let mut handles = Vec::new();
    for (batch_idx, slot_batch) in batches.into_iter().enumerate() {
        let client = client.clone();
        let rpc_url = rpc_url.clone();
        let keys: Vec<String> = keys.iter().map(|k| k.clone()).collect();
        let block_ok = Arc::clone(&block_ok);
        let block_err = Arc::clone(&block_err);
        let batch_ok = Arc::clone(&batch_ok);
        let batch_err = Arc::clone(&batch_err);
        let per_key_ok: Vec<Arc<AtomicU64>> = per_key_ok.iter().map(|a| Arc::clone(a)).collect();
        let per_key_err: Vec<Arc<AtomicU64>> = per_key_err.iter().map(|a| Arc::clone(a)).collect();

        let handle = tokio::spawn(async move {
            let key_idx = batch_idx % n_keys;
            let url = format!("{}?api_key={}", rpc_url.trim_end_matches('/'), keys[key_idx]);

            // Build batch of getBlock requests
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

            match client.post(&url).json(&batch_reqs).send().await {
                Ok(resp) => {
                    if resp.status().is_success() {
                        batch_ok.fetch_add(1, Ordering::Relaxed);
                        per_key_ok[key_idx].fetch_add(1, Ordering::Relaxed);
                        if let Ok(results) = resp.json::<Value>().await {
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
                                                let _ = fs::write(format!("{}.txt", slot), &text);
                                            }
                                            block_ok.fetch_add(1, Ordering::Relaxed);
                                        } else if block.is_null() {
                                            block_err.fetch_add(1, Ordering::Relaxed);
                                        }
                                    } else if item.get("error").is_some() {
                                        block_err.fetch_add(1, Ordering::Relaxed);
                                    }
                                }
                            }
                        }
                    } else {
                        batch_err.fetch_add(1, Ordering::Relaxed);
                        per_key_err[key_idx].fetch_add(1, Ordering::Relaxed);
                    }
                }
                Err(_) => {
                    batch_err.fetch_add(1, Ordering::Relaxed);
                    per_key_err[key_idx].fetch_add(1, Ordering::Relaxed);
                }
            }
        });
        handles.push(handle);
    }

    for h in handles {
        let _ = h.await;
    }
    let total_elapsed = total_start.elapsed();

    let total_ok = block_ok.load(Ordering::Relaxed);
    let total_err = block_err.load(Ordering::Relaxed);
    let b_ok = batch_ok.load(Ordering::Relaxed);
    let b_err = batch_err.load(Ordering::Relaxed);
    let dur_s = total_elapsed.as_secs_f64();

    println!();
    println!("=== Results ===");
    println!("  Duration:     {:.2}s", dur_s);
    println!("  Batches ok:   {}/{} ({} err)", b_ok, total_batches, b_err);
    println!("  Blocks ok:    {}", total_ok);
    println!("  Blocks err:   {}", total_err);
    println!("  Blocks/s:     {:.0}", total_ok as f64 / dur_s);
    println!();
    println!("  Per-key batch distribution:");
    for idx in 0..n_keys {
        let ok = per_key_ok[idx].load(Ordering::Relaxed);
        let err = per_key_err[idx].load(Ordering::Relaxed);
        println!("    key{}: {} batches ok, {} err", idx + 1, ok, err);
    }

    Ok(())
}
