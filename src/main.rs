use std::env;
use std::fs;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};
use ureq::Agent;

const RPC_URLS: &[&str] = &[
    "ams.rpc.orbitflare.com",
    "fra.rpc.orbitflare.com",
    "lon.rpc.orbitflare.com",
    "ny.rpc.orbitflare.com",
    "va.rpc.orbitflare.com",
    "slc.rpc.orbitflare.com",
    "la.rpc.orbitflare.com",
    "jp.rpc.orbitflare.com",
    "sg.rpc.orbitflare.com",
];

const NUM_WORKERS: usize = 20;
const TOTAL_BLOCKS: usize = NUM_WORKERS * 500; // 10_000
const MAX_RETRIES: u32 = 3;

fn rpc(agent: &Agent, host: &str, key: &str, body: &[u8]) -> serde_json::Value {
    let url = format!("http://{host}");
    let resp = agent
        .post(&url)
        .set("x-api-key", key)
        .set("Content-Type", "application/json")
        .send_bytes(body)
        .unwrap_or_else(|e| panic!("rpc failed: {e}"));
    serde_json::from_str(&resp.into_string().unwrap()).unwrap()
}

fn main() {
    let start = Instant::now();
    let key = env::var("key_1").expect("key_1 not set");
    let agent = Agent::new();

    // 1. Current slot
    let slot_body = br#"{"jsonrpc":"2.0","id":1,"method":"getSlot","params":[]}"#;
    let current: u64 = rpc(&agent, RPC_URLS[0], &key, slot_body)["result"]
        .as_u64()
        .unwrap();
    println!("current slot: {current}");

    // 2. Get valid blocks in a wide range
    let range_start = current.saturating_sub(500_000);
    let body = serde_json::json!({
        "jsonrpc": "2.0", "id": 1, "method": "getBlocks",
        "params": [range_start, current]
    })
    .to_string()
    .into_bytes();
    let mut valid: Vec<u64> = rpc(&agent, RPC_URLS[0], &key, &body)["result"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_u64().unwrap())
        .collect();

    let take = TOTAL_BLOCKS.min(valid.len());
    valid = valid.split_off(valid.len() - take);
    println!("valid blocks: {}", valid.len());

    let n = valid.len();
    let per_worker = (n + NUM_WORKERS - 1) / NUM_WORKERS;

    fs::create_dir_all("blocks_output").unwrap();

    let total_ok = Arc::new(AtomicUsize::new(0));
    let total_err = Arc::new(AtomicUsize::new(0));

    let handles: Vec<_> = valid
        .chunks(per_worker)
        .enumerate()
        .map(|(i, chunk)| {
            let k = key.clone();
            let chunk = chunk.to_vec();
            let host = RPC_URLS[i % RPC_URLS.len()].to_string();
            let ok = total_ok.clone();
            let err = total_err.clone();

            thread::spawn(move || {
                let agent = ureq::AgentBuilder::new()
                    .timeout_connect(Duration::from_secs(10))
                    .timeout_read(Duration::from_secs(120))
                    .timeout_write(Duration::from_secs(30))
                    .build();

                let dir = format!("blocks_output/worker_{i:05}");
                fs::create_dir_all(&dir).unwrap();

                let mut ok_count = 0usize;
                let mut err_count = 0usize;

                for (j, &slot) in chunk.iter().enumerate() {
                    let block_body = serde_json::json!({
                        "jsonrpc": "2.0",
                        "id": 1,
                        "method": "getBlock",
                        "params": [
                            slot,
                            {
                                "encoding": "json",
                                "transactionDetails": "full",
                                "maxSupportedTransactionVersion": 0,
                                "rewards": false
                            }
                        ]
                    })
                    .to_string()
                    .into_bytes();

                    let mut succeeded = false;
                    for _attempt in 0..MAX_RETRIES {
                        match agent
                            .post(&format!("http://{host}"))
                            .set("x-api-key", &k)
                            .set("Content-Type", "application/json")
                            .send_bytes(&block_body)
                        {
                            Ok(resp) => {
                                let text = resp.into_string().unwrap_or_default();
                                match serde_json::from_str::<serde_json::Value>(&text) {
                                    Ok(v) => {
                                        let block_data =
                                            serde_json::to_string(&v["result"]).unwrap_or_default();
                                        if block_data.is_empty() || block_data == "null" {
                                            eprintln!(
                                                "[W{i}] slot {slot}: null/empty result"
                                            );
                                        } else {
                                            fs::write(
                                                &format!("{dir}/block_{slot}.json"),
                                                &block_data,
                                            )
                                            .unwrap();
                                            succeeded = true;
                                        }
                                    }
                                    Err(e) => {
                                        eprintln!("[W{i}] slot {slot}: parse error: {e}");
                                    }
                                }
                                if succeeded {
                                    break;
                                }
                            }
                            Err(e) => {
                                eprintln!("[W{i}] slot {slot}: {e}");
                                thread::sleep(Duration::from_millis(500));
                            }
                        }
                    }

                    if succeeded {
                        ok_count += 1;
                    } else {
                        err_count += 1;
                    }

                    if (j + 1) % 100 == 0 || j + 1 == chunk.len() {
                        println!("  worker {i}: {}/{} ({} err)", j + 1, chunk.len(), err_count);
                    }
                }

                ok.fetch_add(ok_count, Ordering::Relaxed);
                err.fetch_add(err_count, Ordering::Relaxed);
                println!("worker {i} done — {ok_count} ok, {err_count} err, {dir}");
                i
            })
        })
        .collect();

    for h in handles {
        let _ = h.join();
    }

    let elapsed = start.elapsed();
    println!("\n--- SUMMARY ---");
    println!("Time: {:.1}s", elapsed.as_secs_f64());
    println!(
        "OK: {}  ERR: {}",
        total_ok.load(Ordering::Relaxed),
        total_err.load(Ordering::Relaxed)
    );
    println!("Output: blocks_output/worker_*/block_*.json");
}
