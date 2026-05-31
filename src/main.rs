use std::env;
use std::time::{Duration, Instant};
use serde_json::json;
use ureq::Agent;

fn get_first_block(agent: &Agent, url: &str, api_key: &Option<String>) -> u64 {
    let body = br#"{"jsonrpc":"2.0","id":1,"method":"getFirstAvailableBlock","params":[]}"#;
    let mut req = agent.post(url).set("Content-Type", "application/json");
    if let Some(k) = api_key {
        req = req.set("x-api-key", k);
    }
    match req.send_bytes(body) {
        Ok(r) => {
            let text = r.into_string().unwrap_or_default();
            if let Ok(v) = serde_json::from_str::<serde_json::Value>(&text) {
                if let Some(slot) = v["result"].as_u64() {
                    return slot;
                }
            }
        }
        Err(e) => eprintln!("  first block request failed: {e}"),
    }
    0
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
    match req.send_bytes(body.as_bytes()) {
        Ok(r) => {
            let text = r.into_string().unwrap_or_default();
            let elapsed = start.elapsed();
            let size = text.len();
            if let Ok(v) = serde_json::from_str::<serde_json::Value>(&text) {
                if let Some(arr) = v.as_array() {
                    let (ok, err) = arr.iter().fold((0, 0), |(o, e), item| {
                        if item.get("result").is_some() { (o + 1, e) } else { (o, e + 1) }
                    });
                    return (ok, err, size, elapsed);
                }
            }
            (0, count, size, elapsed)
        }
        Err(e) => {
            eprintln!("  batch request failed: {e}");
            (0, count, 0, start.elapsed())
        }
    }
}

fn run_batch_test(url: &str, api_key: &Option<String>) {
    let agent = ureq::AgentBuilder::new()
        .timeout_connect(Duration::from_secs(10))
        .timeout_read(Duration::from_secs(120))
        .timeout_write(Duration::from_secs(10))
        .build();

    let fb = get_first_block(&agent, url, api_key);
    println!("first available block: {fb}");

    // Get current slot
    let slot_body = br#"{"jsonrpc":"2.0","id":1,"method":"getSlot","params":[]}"#;
    let mut req = agent.post(url).set("Content-Type", "application/json");
    if let Some(k) = api_key {
        req = req.set("x-api-key", k);
    }
    let slot_text = req.send_bytes(slot_body).unwrap().into_string().unwrap();
    let slot: u64 = serde_json::from_str::<serde_json::Value>(&slot_text)
        .unwrap()["result"]
        .as_u64()
        .unwrap();
    println!("current slot: {slot}");
    println!();
    println!("--- batch getBlock (no transactions) ---");

    for &n in &[10, 20, 50, 100, 200] {
        let (ok, err, size, elapsed) = test_batch(&agent, url, api_key, slot, n, false);
        println!(
            "  {n:4}x: {ok:3} ok, {err:3} err, {:>5} KB, {:.2}s",
            size / 1024,
            elapsed.as_secs_f64(),
        );
    }

    println!();
    println!("--- batch getBlock (full transactions) ---");

    for &n in &[3, 5, 10, 20] {
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
