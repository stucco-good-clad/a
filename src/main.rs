use std::env;
use std::fs;
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

const MAX_RETRIES: u32 = 5;
const TARGET_BLOCKS: usize = 10_000;

fn rpc(agent: &Agent, host: &str, key: &str, body: &[u8]) -> Result<serde_json::Value, String> {
    let url = format!("http://{host}");
    match agent
        .post(&url)
        .set("x-api-key", key)
        .set("Content-Type", "application/json")
        .send_bytes(body)
    {
        Ok(resp) => {
            let text = resp.into_string().map_err(|e| format!("read err: {e}"))?;
            if text.trim().is_empty() {
                return Err("empty response".into());
            }
            serde_json::from_str(&text).map_err(|e| format!("parse err: {e}"))
        }
        Err(e) => Err(format!("http err: {e}")),
    }
}

fn main() {
    let args: Vec<String> = env::args().collect();
    match args.get(1).map(|s| s.as_str()) {
        Some("discover") => discover(),
        Some("worker") => {
            let index: usize = args[2]
                .parse()
                .expect("usage: worker <index> <total>");
            let total: usize = args[3]
                .parse()
                .expect("usage: worker <index> <total>");
            worker(index, total);
        }
        _ => panic!("usage: cargo run -- discover | worker <index> <total>"),
    }
}

fn discover() {
    let key = env::var("key_1").expect("key_1 not set");
    let agent = Agent::new();

    // Get current slot
    let body = br#"{"jsonrpc":"2.0","id":1,"method":"getSlot","params":[]}"#;
    let current: u64 = rpc(&agent, RPC_URLS[0], &key, body)
        .unwrap_or_else(|e| panic!("getSlot: {e}"))["result"]
        .as_u64()
        .unwrap();
    println!("current slot: {current}");

    // Get valid blocks in a wide range
    let range_start = current.saturating_sub(500_000);
    let body = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getBlocks",
        "params": [range_start, current]
    })
    .to_string()
    .into_bytes();
    let mut valid: Vec<u64> = rpc(&agent, RPC_URLS[0], &key, &body)
        .unwrap_or_else(|e| panic!("getBlocks: {e}"))["result"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_u64().unwrap())
        .collect();

    // Take last TARGET_BLOCKS (or fewer if not enough)
    let take = TARGET_BLOCKS.min(valid.len());
    valid = valid.split_off(valid.len() - take);
    println!("valid blocks: {} (from {take} targeted)", valid.len());

    fs::write("valid_blocks.json", serde_json::to_string(&valid).unwrap())
        .unwrap_or_else(|e| panic!("write valid_blocks.json: {e}"));
    println!("wrote valid_blocks.json");
}

fn worker(index: usize, total: usize) {
    let start = Instant::now();
    let key = env::var("key_1").expect("key_1 not set");

    // Read the valid blocks list produced by discover
    let text =
        fs::read_to_string("valid_blocks.json").unwrap_or_else(|e| {
            panic!("read valid_blocks.json (worker {index}): {e}")
        });
    let valid: Vec<u64> = serde_json::from_str(&text).unwrap_or_else(|e| {
        panic!("parse valid_blocks.json (worker {index}): {e}")
    });

    if valid.is_empty() {
        println!("worker {index}: no blocks to process");
        return;
    }

    // Calculate this worker's chunk
    let per_worker = (valid.len() + total - 1) / total;
    let start_idx = index * per_worker;
    let end_idx = (start_idx + per_worker).min(valid.len());
    let chunk = &valid[start_idx..end_idx];

    if chunk.is_empty() {
        println!("worker {index}: no blocks assigned");
        return;
    }

    let host = RPC_URLS[index % RPC_URLS.len()];
    let agent = ureq::AgentBuilder::new()
        .timeout_connect(Duration::from_secs(10))
        .timeout_read(Duration::from_secs(120))
        .timeout_write(Duration::from_secs(30))
        .build();

    let dir = format!("blocks_output/worker_{index:05}");
    fs::create_dir_all(&dir).unwrap();

    let mut ok = 0usize;
    let mut err = 0usize;

    for (j, &slot) in chunk.iter().enumerate() {
        let body = serde_json::json!({
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
        for attempt in 0..MAX_RETRIES {
            match rpc(&agent, host, &key, &body) {
                Ok(v) => {
                    let block_data =
                        serde_json::to_string(&v["result"]).unwrap_or_default();
                    if block_data.is_empty() || block_data == "null" {
                        eprintln!(
                            "[W{index}] slot {slot}: null result (attempt {})",
                            attempt + 1
                        );
                    } else {
                        fs::write(&format!("{dir}/block_{slot}.json"), &block_data)
                            .unwrap();
                        succeeded = true;
                        break;
                    }
                }
                Err(e) => {
                    let delay_ms = 500 * (1u64 << attempt); // 500ms, 1s, 2s, 4s, 8s
                    eprintln!(
                        "[W{index}] slot {slot}: {e} (attempt {}, retry in {}ms)",
                        attempt + 1,
                        delay_ms
                    );
                    thread::sleep(Duration::from_millis(delay_ms));
                }
            }
        }

        if succeeded {
            ok += 1;
        } else {
            err += 1;
        }

        if (j + 1) % 100 == 0 || j + 1 == chunk.len() {
            println!(
                "  worker {index}: {}/{} ({} err)",
                j + 1,
                chunk.len(),
                err
            );
        }
    }

    let elapsed = start.elapsed();
    println!(
        "worker {index} DONE — {ok} ok, {err} err, {dir} ({:.1}s)",
        elapsed.as_secs_f64()
    );
}
