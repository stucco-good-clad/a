use std::env;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde_json::json;

async fn burst(
    client: &reqwest::Client,
    url: &str,
    body: &serde_json::Value,
    concurrency: usize,
) -> (u64, u64, u64, Duration) {
    let success = AtomicU64::new(0);
    let rate_limited = AtomicU64::new(0);
    let errors = AtomicU64::new(0);

    let start = Instant::now();

    let mut handles = Vec::with_capacity(concurrency);
    for _ in 0..concurrency {
        let client = client.clone();
        let body = body.clone();
        let url = url.to_string();

        let handle = tokio::spawn(async move {
            match client.post(&url).json(&body).send().await {
                Ok(resp) => {
                    if resp.status() == 429 {
                        rate_limited.fetch_add(1, Ordering::Relaxed);
                    } else if resp.status().is_success() {
                        success.fetch_add(1, Ordering::Relaxed);
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
    let api_key = env::var("API_KEY").expect("API_KEY env var required");
    let max_concurrency: usize = env::var("MAX_CONCURRENCY")
        .unwrap_or_else(|_| "2000".to_string())
        .parse()
        .unwrap_or(2000);

    let full_url = format!("{}?api_key={}", rpc_url.trim_end_matches('/'), api_key);

    let body = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getSlot",
        "params": []
    });

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()?;

    // Sweep: start at 1, double until hitting rate-limit ceiling
    let levels: Vec<usize> = (0..=10)
        .map(|i| 1 << i) // 1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024
        .chain(std::iter::successors(Some(1500), |&n| {
            let next = n + 500;
            if next <= max_concurrency { Some(next) } else { None }
        }))
        .collect();

    println!(
        "Sweep rate-limit test: {} -> {} concurrent requests",
        levels[0],
        levels.last().unwrap()
    );
    println!();
    println!(
        "{:>10} {:>10} {:>10} {:>10} {:>10} {:>12} {:>10}",
        "concurrency", "total", "success", "rate-ltd", "errors", "duration", "RPS"
    );
    println!("{:-<72}", "");

    let mut prev_rps = 0.0;

    for &conc in &levels {
        let (s, rl, er, dur) = burst(&client, &full_url, &body, conc).await;
        let total = s + rl + er;
        let dur_s = dur.as_secs_f64();
        let rps = total as f64 / dur_s.max(0.001);
        let rps_delta = if prev_rps > 0.0 {
            format!("{:+.0}", rps - prev_rps)
        } else {
            "  -".to_string()
        };

        println!(
            "{:>10} {:>10} {:>10} {:>10} {:>10} {:>8.3}s {:>8.0} {}",
            conc, total, s, rl, er, dur_s, rps, rps_delta
        );

        // Stop if rate limiting kicks in heavily (>20% rate-limited)
        if total > 0 && (rl as f64 / total as f64) > 0.2 {
            println!();
            println!("Rate-limit ceiling hit at concurrency={}", conc);
            println!(
                "  {:.1}% requests rate-limited — endpoint saturated",
                rl as f64 / total as f64 * 100.0
            );
            break;
        }

        prev_rps = rps;

        // Small cooldown between levels
        tokio::time::sleep(Duration::from_secs(1)).await;
    }

    Ok(())
}
