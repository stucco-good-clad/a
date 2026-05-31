use std::env;
use std::fs::File;
use std::io::copy;
use std::time::{Duration, Instant};
use serde_json::json;
use ureq::{Agent, AgentBuilder};

fn read_response(r: ureq::Response, label: &str) -> Option<String> {
    match r.into_string() {
        Ok(s) => Some(s),
        Err(e) => {
            eprintln!("  {}: body read error: {}", label, e);
            None
        }
    }
}

fn get_first_block(agent: &Agent, url: &str, api_key: &Option<String>) -> Option<u64> {
    let body = br#"{"jsonrpc":"2.0","id":1,"method":"getFirstAvailableBlock","params":[]}"#;
    let mut req = agent.post(url).set("Content-Type", "application/json");
    if let Some(k) = api_key {
        req = req.set("x-api-key", k);
    }
    match req.send_bytes(body) {
        Ok(r) => read_response(r, "first_block").and_then(|text| {
            serde_json::from_str::<serde_json::Value>(&text)
                .ok()
                .and_then(|v| v["result"].as_u64())
        }),
        Err(e) => {
            eprintln!("  first block request failed: {e}");
            None
        }
    }
}

fn get_slot(agent: &Agent, url: &str, api_key: &Option<String>) -> Option<u64> {
    let body = br#"{"jsonrpc":"2.0","id":1,"method":"getSlot","params":[]}"#;
    let mut req = agent.post(url).set("Content-Type", "application/json");
    if let Some(k) = api_key {
        req = req.set("x-api-key", k);
    }
    match req.send_bytes(body) {
        Ok(r) => read_response(r, "getSlot").and_then(|text| {
            serde_json::from_str::<serde_json::Value>(&text)
                .ok()
                .and_then(|v| v["result"].as_u64())
        }),
        Err(e) => {
            eprintln!("  getSlot request failed: {e}");
            None
        }
    }
}

fn write_response_to_file(r: ureq::Response, path: &str) -> std::io::Result<u64> {
    let mut f = File::create(path)?;
    let mut reader = r.into_reader();
    let size = copy(&mut reader, &mut f)?;
    Ok(size)
}

fn test_batch(
    agent: &Agent,
    url: &str,
    api_key: &Option<String>,
    slot: u64,
    count: usize,
    full_tx: bool,
) -> (usize, usize, usize, Duration) {
    let mut batch = Vec::with_capacity(count);
    for i in 0..count {
        let b = slot - i as u64;
        let params = if full_tx {
            json!([b, {"encoding": "json", "transactionDetails": "full", "maxSupportedTransactionVersion": 0, "rewards": false}])
        } else {
            json!([b, {"encoding": "json", "transactionDetails": "none", "rewards": false}])
        };
        batch.push(json!({"jsonrpc":"2.0","id":i,"method":"getBlock","params":params}));
    }
    let body = serde_json::to_string(&batch).unwrap();

    let mut req = agent.post(url).set("Content-Type", "application/json");
    if let Some(k) = api_key {
        req = req.set("x-api-key", k);
    }

    let start = Instant::now();
    let tmp_path = format!("/tmp/batch_{}_{}.json", slot, count);
    
    match req.send_bytes(body.as_bytes()) {
        Ok(r) => {
            let write_size = match write_response_to_file(r, &tmp_path) {
                Ok(s) => s,
                Err(e) => {
                    eprintln!("  batch: file write error: {}", e);
                    return (0, count, 0, start.elapsed());
                }
            };
            let elapsed = start.elapsed();
            
            let text = match std::fs::read_to_string(&tmp_path) {
                Ok(s) => s,
                Err(e) => {
                    eprintln!("  batch: file read error: {}", e);
                    return (0, count, 0, elapsed);
                }
            };
            
            let size = text.len();
            if text.is_empty() {
                eprintln!("  batch: empty response from {} bytes", write_size);
                return (0, count, 0, elapsed);
            }
            match serde_json::from_str::<serde_json::Value>(&text) {
                Ok(v) => {
                    if let Some(arr) = v.as_array() {
                        let (ok, err) = arr.iter().fold((0, 0), |(o, e), item| {
                            if item.get("result").is_some() {
                                (o + 1, e)
                            } else {
                                (o, e + 1)
                            }
                        });
                        if err > 0 {
                            eprintln!("  batch errors: {}", err);
                        }
                        if ok > 0 {
                            println!("    -> saved to {}", tmp_path);
                        }
                        return (ok, err, size, elapsed);
                    }
                    eprintln!("  batch: response not an array (wrote {} bytes)", size);
                    (0, count, size, elapsed)
                }
                Err(e) => {
                    eprintln!("  batch JSON parse error: {}", e);
                    eprintln!("  response head (first 500 chars):\n{}", &text[..text.len().min(500)]);
                    (0, count, size, elapsed)
                }
            }
        }
        Err(e) => {
            eprintln!("  batch request failed: {}", e);
            (0, count, 0, start.elapsed())
        }
    }
}

fn run_batch_test(url: &str, api_key: &Option<String>) {
    let agent = AgentBuilder::new()
        .timeout_connect(Duration::from_secs(10))
        .timeout_read(Duration::from_secs(300))
        .timeout_write(Duration::from_secs(10))
        .build();

    print!("first available block: ");
    let fb = get_first_block(&agent, url, api_key);
    println!("{}", fb.unwrap_or_default());

    print!("current slot: ");
    let slot = get_slot(&agent, url, api_key).unwrap_or(0);
    println!("{slot}\n");

    println!();
    println!("--- batch getBlock (full transactions) ---");
    for &n in &[50, 100, 200, 300, 400, 500] {
        let (ok, err, size, elapsed) = test_batch(&agent, url, api_key, slot, n, true);
        println!(
            "  {n:4}x: {ok:3} ok, {err:3} err, {:>6} KB, {:.2}s",
            size / 1024,
            elapsed.as_secs_f64(),
        );
    }
}

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 3 {
        eprintln!("usage: {} batchtest <url> [api_key]", args[0]);
        std::process::exit(1);
    }
    let mode = &args[1];
    let url = &args[2];
    let api_key = if args.len() > 3 {
        Some(args[3].clone())
    } else {
        None
    };

    match mode.as_str() {
        "batchtest" => {
            println!("batch test: {url}");
            run_batch_test(url, &api_key);
        }
        _ => {
            eprintln!("unknown mode: {mode}");
            std::process::exit(1);
        }
    }
}
