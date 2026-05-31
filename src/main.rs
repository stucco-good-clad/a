use std::env;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

fn test_rate_limit(url: &str, api_key: &Option<String>) {
    let body = br#"{"jsonrpc":"2.0","id":1,"method":"getSlot","params":[]}"#;

    // First get first available block
    let fb_body = br#"{"jsonrpc":"2.0","id":1,"method":"getFirstAvailableBlock","params":[]}"#;
    let agent = ureq::AgentBuilder::new()
        .timeout_connect(Duration::from_secs(10))
        .timeout_read(Duration::from_secs(60))
        .timeout_write(Duration::from_secs(10))
        .build();

    let mut req = agent.post(url).set("Content-Type", "application/json");
    if let Some(k) = api_key {
        req = req.set("x-api-key", k);
    }
    match req.send_bytes(fb_body) {
        Ok(r) => {
            let text = r.into_string().unwrap_or_default();
            if let Ok(v) = serde_json::from_str::<serde_json::Value>(&text) {
                if let Some(slot) = v["result"].as_u64() {
                    println!("first available block: {slot}");
                } else if let Some(e) = v["error"].as_object() {
                    println!("first block error: {} ({})", e["message"], e["code"]);
                }
            }
        }
        Err(e) => println!("first block request failed: {e}"),
    }

    // Test increasing concurrency levels
    let levels: &[usize] = &[100, 500, 1000, 2000, 5000, 10000];

    for &n in levels {
        let agent = Arc::new(
            ureq::AgentBuilder::new()
                .timeout_connect(Duration::from_secs(10))
                .timeout_read(Duration::from_secs(120))
                .timeout_write(Duration::from_secs(10))
                .build(),
        );
        let ok = Arc::new(AtomicUsize::new(0));
        let err = Arc::new(AtomicUsize::new(0));
        let mut handles = Vec::with_capacity(n);

        let start = Instant::now();
        for _ in 0..n {
            let agent = Arc::clone(&agent);
            let ok = Arc::clone(&ok);
            let err = Arc::clone(&err);
            let url = url.to_string();
            let api_key = api_key.clone();
            handles.push(std::thread::spawn(move || {
                let mut req = agent.post(&url).set("Content-Type", "application/json");
                if let Some(k) = &api_key {
                    req = req.set("x-api-key", k);
                }
                match req.send_bytes(body) {
                    Ok(r) => {
                        let text = r.into_string().unwrap_or_default();
                        if text.contains("\"error\"") {
                            err.fetch_add(1, Ordering::Relaxed);
                        } else {
                            ok.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    Err(_) => {
                        err.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }));
        }
        for h in handles {
            let _ = h.join();
        }
        let elapsed = start.elapsed();
        let o = ok.load(Ordering::Relaxed);
        let e = err.load(Ordering::Relaxed);
        print!("  {n:5}x: {o:5} ok, {e:5} err ({:.2}s)", elapsed.as_secs_f64());
        if n >= 1000 {
            // also test getBlock for heavy payload
            let block_body = br#"{"jsonrpc":"2.0","id":1,"method":"getBlock","params":[423400000,{"encoding":"json","transactionDetails":"full","maxSupportedTransactionVersion":0,"rewards":false}]}"#;
            let agent2 = ureq::AgentBuilder::new()
                .timeout_connect(Duration::from_secs(10))
                .timeout_read(Duration::from_secs(120))
                .timeout_write(Duration::from_secs(10))
                .build();
            let start2 = Instant::now();
            let mut req2 = agent2.post(url).set("Content-Type", "application/json");
            if let Some(k) = &api_key {
                req2 = req2.set("x-api-key", k);
            }
            match req2.send_bytes(block_body) {
                Ok(r) => {
                    let text = r.into_string().unwrap_or_default();
                    let sz = text.len();
                    let t = start2.elapsed();
                    print!(" | getBlock: {sz} bytes in {:.2}s", t.as_secs_f64());
                }
                Err(e) => {
                    print!(" | getBlock: {e}");
                }
            }
        }
        println!();
        if o == 0 && n >= 100 {
            println!("  ceiling found at {n}");
            break;
        }
    }
}

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        eprintln!("usage: {} <url> [api_key]", args[0]);
        std::process::exit(1);
    }
    let url = &args[1];
    let api_key = if args.len() > 2 {
        Some(args[2].clone())
    } else {
        None
    };
    println!("testing: {url}");
    test_rate_limit(url, &api_key);
}
