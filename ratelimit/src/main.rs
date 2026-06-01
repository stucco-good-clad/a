use std::env;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde_json::json;

async fn burst_split(
    client: &reqwest::Client,
    base_url: &str,
    key1: &str,
    key2: &str,
    body: &serde_json::Value,
    concurrency: usize,
) -> (u64, u64, u64, Duration) {
    let success = Arc::new(AtomicU64::new(0));
    let rate_limited = Arc::new(AtomicU64::new(0));
    let errors = Arc::new(AtomicU64::new(0));

    // Track per-key stats
    let success_k1 = Arc::new(AtomicU64::new(0));
    let success_k2 = Arc::new(AtomicU64::new(0));
    let rl_k1 = Arc::new(AtomicU64::new(0));
    let rl_k2 = Arc::new(AtomicU64::new(0));

    let start = Instant::now();

    let mut handles = Vec::with_capacity(concurrency);
    for i in 0..concurrency {
        let client = client.clone();
        let body = body.clone();
        let base_url = base_url.to_string();
        let key1 = key1.to_string();
        let key2 = key2.to_string();
        let success = Arc::clone(&success);
        let rate_limited = Arc::clone(&rate_limited);
        let errors = Arc::clone(&errors);
        let success_k1 = Arc::clone(&success_k1);
        let success_k2 = Arc::clone(&success_k2);
        let rl_k1 = Arc::clone(&rl_k1);
        let rl_k2 = Arc::clone(&rl_k2);

        let handle = tokio::spawn(async move {
            // Alternate: even i uses key1, odd i uses key2
            let key = if i % 2 == 0 { &key1 } else { &key2 };
            let url = format!("{}?api_key={}", base_url.trim_end_matches('/'), key);

            match client.post(&url).json(&body).send().await {
                Ok(resp) => {
                    if resp.status() == 429 {
                        rate_limited.fetch_add(1, Ordering::Relaxed);
                        if i % 2 == 0 {
                            rl_k1.fetch_add(1, Ordering::Relaxed);
                        } else {
                            rl_k2.fetch_add(1, Ordering::Relaxed);
                        }
                    } else if resp.status().is_success() {
                        success.fetch_add(1, Ordering::Relaxed);
                        if i % 2 == 0 {
                            success_k1.fetch_add(1, Ordering::Relaxed);
                        } else {
                            success_k2.fetch_add(1, Ordering::Relaxed);
                        }
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

    println!(
        "    key1: {:>5} success, {:>5} rate-limited | key2: {:>5} success, {:>5} rate-limited",
        success_k1.load(Ordering::Relaxed),
        rl_k1.load(Ordering::Relaxed),
        success_k2.load(Ordering::Relaxed),
        rl_k2.load(Ordering::Relaxed),
    );

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
    let key1 = env::var("KEY_1").expect("KEY_1 env var required");
    let key2 = env::var("KEY_2").expect("KEY_2 env var required");
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

    // Sweep levels
    let levels: Vec<usize> = (0..=10)
        .map(|i| 1 << i) // 1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024
        .chain(std::iter::successors(Some(1500), |&n| {
            let next = n + 500;
            if next <= max_concurrency { Some(next) } else { None }
        }))
        .collect();

    println!(
        "Two-key sweep: {} -> {} concurrent (alternating KEY_1 / KEY_2)",
        levels[0],
        levels.last().unwrap()
    );
    println!();
    println!(
        "{:>10} {:>10} {:>10} {:>10} {:>10} {:>12} {:>10}",
        "concurrency", "total", "success", "rate-ltd", "errors", "duration", "RPS"
    );
    println!("{:-<72}", "");

    for &conc in &levels {
        let (s, rl, er, dur) = burst_split(&client, &rpc_url, &key1, &key2, &body, conc).await;
        let total = s + rl + er;
        let dur_s = dur.as_secs_f64();
        let rps = total as f64 / dur_s.max(0.001);

        println!(
            "{:>10} {:>10} {:>10} {:>10} {:>10} {:>8.3}s {:>8.0}",
            conc, total, s, rl, er, dur_s, rps,
        );
        println!();

        // Stop if rate limiting kicks in heavily (>20% rate-limited)
        if total > 0 && (rl as f64 / total as f64) > 0.2 {
            println!("Ceiling at concurrency={}", conc);
            break;
        }

        tokio::time::sleep(Duration::from_secs(1)).await;
    }

    Ok(())
}
