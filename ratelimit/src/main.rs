use std::env;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde_json::json;

async fn burst_split(
    client: &reqwest::Client,
    base_url: &str,
    keys: &[String],
    body: &serde_json::Value,
    concurrency: usize,
) -> (u64, u64, u64, Duration) {
    let success = Arc::new(AtomicU64::new(0));
    let rate_limited = Arc::new(AtomicU64::new(0));
    let errors = Arc::new(AtomicU64::new(0));
    let n_keys = keys.len();

    let key_success: Vec<Arc<AtomicU64>> = (0..n_keys).map(|_| Arc::new(AtomicU64::new(0))).collect();
    let key_rl: Vec<Arc<AtomicU64>> = (0..n_keys).map(|_| Arc::new(AtomicU64::new(0))).collect();

    let start = Instant::now();

    let mut handles = Vec::with_capacity(concurrency);
    for i in 0..concurrency {
        let client = client.clone();
        let body = body.clone();
        let base_url = base_url.to_string();
        let keys: Vec<String> = keys.iter().map(|k| k.clone()).collect();
        let success = Arc::clone(&success);
        let rate_limited = Arc::clone(&rate_limited);
        let errors = Arc::clone(&errors);
        let key_success: Vec<Arc<AtomicU64>> = key_success.iter().map(|a| Arc::clone(a)).collect();
        let key_rl: Vec<Arc<AtomicU64>> = key_rl.iter().map(|a| Arc::clone(a)).collect();

        let handle = tokio::spawn(async move {
            let key_idx = i % n_keys;
            let url = format!("{}?api_key={}", base_url.trim_end_matches('/'), keys[key_idx]);

            match client.post(&url).json(&body).send().await {
                Ok(resp) => {
                    if resp.status() == 429 {
                        rate_limited.fetch_add(1, Ordering::Relaxed);
                        key_rl[key_idx].fetch_add(1, Ordering::Relaxed);
                    } else if resp.status().is_success() {
                        success.fetch_add(1, Ordering::Relaxed);
                        key_success[key_idx].fetch_add(1, Ordering::Relaxed);
                    } else {
                        errors.fetch_add(1, Ordering::Relaxed);
                    }
                }
                Err(_) => {
                    errors.fetch_add(1, Ordering::Relaxed);
                }
            }
        });
        handles.push(handle);
    }

    for h in handles {
        let _ = h.await;
    }
    let elapsed = start.elapsed();

    print!("    ");
    for idx in 0..n_keys {
        let s = key_success[idx].load(Ordering::Relaxed);
        let r = key_rl[idx].load(Ordering::Relaxed);
        print!("key{}: {:>4} ok, {:>4} rl  ", idx + 1, s, r);
    }
    println!();

    (
        success.load(Ordering::Relaxed),
        rate_limited.load(Ordering::Relaxed),
        errors.load(Ordering::Relaxed),
        elapsed,
    )
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let rpc_url =
        env::var("RPC_URL").unwrap_or_else(|_| "http://slc.rpc.orbitflare.com".to_string());

    let mut keys: Vec<String> = Vec::new();
    for n in 1.. {
        match env::var(format!("KEY_{}", n)) {
            Ok(val) => keys.push(val),
            Err(_) => break,
        }
    }
    assert!(!keys.is_empty(), "At least KEY_1 must be set");

    let max_concurrency: usize = env::var("MAX_CONCURRENCY")
        .unwrap_or_else(|_| "2000".to_string())
        .parse()
        .unwrap_or(2000);

    let body = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getSlot",
        "params": []
    });

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()?;

    let levels: Vec<usize> = (0..=10)
        .map(|i| 1 << i)
        .chain(std::iter::successors(Some(1500), |&n| {
            let next = n + 500;
            if next <= max_concurrency { Some(next) } else { None }
        }))
        .collect();

    println!(
        "{}-key sweep: {} -> {} concurrent (round-robin over {} keys)",
        keys.len(),
        levels[0],
        levels.last().unwrap(),
        keys.len()
    );
    println!();
    println!(
        "{:>10} {:>10} {:>10} {:>10} {:>10} {:>12} {:>10}",
        "concurrency", "total", "success", "rate-ltd", "errors", "duration", "RPS"
    );
    println!("{:-<72}", "");

    for &conc in &levels {
        let (s, rl, er, dur) = burst_split(&client, &rpc_url, &keys, &body, conc).await;
        let total = s + rl + er;
        let dur_s = dur.as_secs_f64();
        let rps = total as f64 / dur_s.max(0.001);

        println!(
            "{:>10} {:>10} {:>10} {:>10} {:>10} {:>8.3}s {:>8.0}",
            conc, total, s, rl, er, dur_s, rps,
        );
        println!();

        if total > 0 && (rl as f64 / total as f64) > 0.2 {
            println!("Ceiling at concurrency={}", conc);
            break;
        }

        tokio::time::sleep(Duration::from_secs(1)).await;
    }

    Ok(())
}
