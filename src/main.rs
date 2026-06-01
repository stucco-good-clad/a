use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;

use anyhow::{Context, Result};
use serde_json::{json, Value};
use tokio::sync::Semaphore;

const RPC_BASE: &str = "https://solana-mainnet.g.alchemy.com/v2/";

#[tokio::main]
async fn main() -> Result<()> {
    let api_key =
        std::env::var("ALCHEMY_KEY").context("ALCHEMY_KEY env var not set")?;
    let rpc_url = format!("{RPC_BASE}{api_key}");

    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(60))
        .build()?;

    // 1. Get tip slot
    let tip: u64 = rpc_call(&client, &rpc_url, "getSlot", &[]).await?;
    println!("Current slot: {tip}\n");

    // 2. Find block slots once — reuse across all combos
    println!("Scanning for 5000 block slots...");
    let all_slots = find_block_slots(&client, &rpc_url, tip, 5000).await?;
    println!("Found {} block slots\n", all_slots.len());

    // 3. Benchmark all combos
    let chunks = [50u32, 75, 100];
    let concurrencies = [10u32, 20, 50, 100];

    println!(
        "{:>6} {:>12} {:>10} {:>8} {:>8}",
        "batch", "concurrency", "blocks", "time(s)", "blk/s"
    );
    println!("{}", "-".repeat(50));

    for &chunk in &chunks {
        for &conc in &concurrencies {
            let start = Instant::now();
            let (ok, err) =
                fetch_blocks(&client, &rpc_url, &all_slots, chunk, conc).await;
            let secs = start.elapsed().as_secs_f64();
            let rate = ok as f64 / secs;

            let err_suffix = if err > 0 {
                format!("  err:{err}")
            } else {
                String::new()
            };
            println!(
                "{chunk:>6} {conc:>8}     {ok:>6}/{e:<3}{suffix}",
                e = ok + err,
                suffix = err_suffix
            );
            println!("{:>6} {:>8} {:>10.2}s {:>8.1} blk/s", "", "", secs, rate);
        }
        println!();
    }
    Ok(())
}

/// --- helpers ---------------------------------------------------------------

/// Single JSON-RPC call (getSlot / getBlocks / etc.)
async fn rpc_call<T: serde::de::DeserializeOwned>(
    client: &reqwest::Client,
    url: &str,
    method: &str,
    params: &[serde_json::Value],
) -> Result<T> {
    let body = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": method,
        "params": params,
    });

    let resp: Value = client
        .post(url)
        .header("content-type", "application/json")
        .json(&body)
        .send()
        .await?
        .json()
        .await?;

    if let Some(e) = resp.get("error") {
        anyhow::bail!("RPC {}: {}", method, e["message"]);
    }
    let result = resp
        .get("result")
        .ok_or_else(|| anyhow::anyhow!("no result in {method} response"))?;
    Ok(serde_json::from_value(result.clone())?)
}

/// Scan backward from `tip` until we have `target` block slots.
async fn find_block_slots(
    client: &reqwest::Client,
    url: &str,
    tip: u64,
    target: u32,
) -> Result<Vec<u64>> {
    let mut out: Vec<u64> = Vec::with_capacity(target as usize);
    let mut end = tip.saturating_sub(1);

    while out.len() < target as usize && end > 0 {
        let start = end.saturating_sub(50_000);
        let blocks: Vec<u64> =
            rpc_call(client, url, "getBlocks", &[json!(start), json!(end)]).await?;
        for &b in blocks.iter().rev() {
            out.push(b);
            if out.len() >= target as usize {
                break;
            }
        }
        end = start.saturating_sub(1);
    }

    out.truncate(target as usize);
    Ok(out)
}

/// Fetch blocks via JSON-RPC batch requests.
///
/// Workers grab a chunk, send it as a single batch POST, then grab the next
/// chunk. A semaphore keeps at most `concurrency` chunk-workers alive at once.
async fn fetch_blocks(
    client: &reqwest::Client,
    url: &str,
    slots: &[u64],
    chunk_size: u32,
    concurrency: u32,
) -> (u64, u64) {
    let chunks: Vec<Vec<u64>> = slots
        .chunks(chunk_size as usize)
        .map(|c| c.to_vec())
        .collect();
    let total_chunks = chunks.len();
    let chunks = Arc::new(chunks);

    let idx = Arc::new(AtomicUsize::new(0));
    let ok = Arc::new(AtomicUsize::new(0));
    let err = Arc::new(AtomicUsize::new(0));
    let sem = Arc::new(Semaphore::new(concurrency as usize));

    let mut handles = Vec::with_capacity(concurrency as usize);
    for _ in 0..concurrency {
        let c = client.clone();
        let u = url.to_string();
        let (ch, i, o, e, s) =
            (chunks.clone(), idx.clone(), ok.clone(), err.clone(), sem.clone());

        handles.push(tokio::spawn(async move {
            loop {
                let _permit = s.acquire().await.unwrap();
                let pos = i.fetch_add(1, Ordering::SeqCst);
                if pos >= total_chunks {
                    break;
                }
                let (ok_count, err_count) =
                    fire_batch(&c, &u, &ch[pos]).await;
                o.fetch_add(ok_count, Ordering::SeqCst);
                e.fetch_add(err_count, Ordering::SeqCst);
            }
        }));
    }

    for h in handles {
        let _ = h.await;
    }

    (ok.load(Ordering::SeqCst) as u64, err.load(Ordering::SeqCst) as u64)
}

/// Send a single JSON-RPC batch containing all `getBlock` requests for these
/// slots. Returns (ok_count, err_count).
async fn fire_batch(
    client: &reqwest::Client,
    url: &str,
    slots: &[u64],
) -> (usize, usize) {
    let batch: Vec<Value> = slots
        .iter()
        .map(|&slot| {
            json!({
                "jsonrpc": "2.0",
                "id": slot,
                "method": "getBlock",
                "params": [slot, {
                    "encoding": "json",
                    "maxSupportedTransactionVersion": 0
                }],
            })
        })
        .collect();

    let resp: Value = match client
        .post(url)
        .header("content-type", "application/json")
        .json(&batch)
        .send()
        .await
    {
        Ok(r) => match r.json().await {
            Ok(v) => v,
            Err(_) => return (0, slots.len()),
        },
        Err(_) => return (0, slots.len()),
    };

    let responses = match resp.as_array() {
        Some(arr) => arr,
        None => return (0, slots.len()),
    };

    let mut ok = 0usize;
    for entry in responses {
        if entry.get("error").is_some() {
            continue;
        }
        if entry.get("result").is_some() {
            ok += 1;
        }
    }
    (ok, responses.len() - ok)
}
