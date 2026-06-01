use anyhow::Result;
use clap::Parser;
use reqwest::Client;
use serde_json::{json, Value};
use std::fs;
use std::path::PathBuf;
use std::time::{Duration, Instant};
use tokio::sync::Semaphore;

#[derive(Parser, Debug, Clone)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Solana RPC endpoint URL
    #[arg(short, long)]
    rpc: String,

    /// API key for RPC (optional)
    #[arg(short, long)]
    api_key: Option<String>,

    /// Start slot (inclusive, ignored if --from-latest is set)
    #[arg(short, long)]
    start_slot: Option<u64>,

    /// End slot (inclusive, ignored if --from-latest is set)
    #[arg(short, long)]
    end_slot: Option<u64>,

    /// Number of blocks per batch
    #[arg(short, long, default_value = "10")]
    batch_size: usize,

    /// Maximum concurrent batch requests
    #[arg(short, long, default_value = "20")]
    max_concurrent: usize,

    /// Output directory for block JSON files
    #[arg(short, long, default_value = "./blocks")]
    output: String,

    /// Request timeout in seconds
    #[arg(long, default_value = "60")]
    timeout: u64,

    /// Backfill N blocks ending at current slot (overrides --start-slot/--end-slot)
    #[arg(long)]
    from_latest: Option<usize>,

    /// Run the same config N times and print a comparison summary
    #[arg(long)]
    runs: Option<usize>,
}

#[derive(Debug)]
struct RunSummary {
    batch: usize,
    concurrency: usize,
    ok: usize,
    err: usize,
    mb_per_sec: f64,
    elapsed: f64,
    blocks_per_sec: f64,
}

impl RunSummary {
    fn new(batch: usize, concurrency: usize, ok: usize, err: usize, bytes: u64, elapsed: f64, total: usize) -> Self {
        let mb_per_sec = if elapsed > 0.0 {
            (bytes as f64 / 1024.0 / 1024.0) / elapsed
        } else {
            0.0
        };
        let blocks_per_sec = if elapsed > 0.0 {
            total as f64 / elapsed
        } else {
            0.0
        };
        Self { batch, concurrency, ok, err, mb_per_sec, elapsed, blocks_per_sec }
    }
}

async fn get_slot(client: &Client, url: &str, api_key: &Option<String>) -> Result<u64> {
    let body = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getSlot",
        "params": []
    });
    let mut req = client.post(url)
        .header("content-type", "application/json");
    if let Some(key) = api_key {
        req = req.header("x-api-key", key);
    }
    let resp = req.body(body.to_string()).send().await?;
    let v: Value = resp.json().await?;
    if v.get("error").is_some() {
        anyhow::bail!("RPC error: {}", v["error"]);
    }
    match v.get("result").and_then(Value::as_u64) {
        Some(slot) if slot > 0 => Ok(slot),
        _ => anyhow::bail!("invalid slot response from RPC"),
    }
}

async fn send_batch(
    client: &Client,
    url: &str,
    api_key: &Option<String>,
    slots: &[u64],
    batch_size: usize,
    offset: usize,
) -> Result<(usize, usize, u64), anyhow::Error> {
    let mut batch = Vec::with_capacity(batch_size.min(slots.len().saturating_sub(offset)));
    let len = batch_size.min(slots.len().saturating_sub(offset));
    for i in 0..len {
        let slot = slots[offset + i];
        let params = vec![
            json!(slot),
            json!({
                "encoding": "json",
                "transactionDetails": "full",
                "maxSupportedTransactionVersion": 0,
                "rewards": false,
            }),
        ];
        batch.push(json!({
            "jsonrpc": "2.0",
            "id": i,
            "method": "getBlock",
            "params": params,
        }));
    }

    let body = serde_json::to_vec(&batch)?;
    let mut req = client.post(url)
        .header("content-type", "application/json");
    if let Some(key) = api_key {
        req = req.header("x-api-key", key);
    }
    let resp = req.body(body).send().await?;
    let status = resp.status();
    let resp_bytes = resp.bytes().await?;
    let buf = resp_bytes.to_vec();
    let batch_bytes = buf.len() as u64;

    if !status.is_success() {
        return Ok((0, len, batch_bytes));
    }

    let v: Value = match serde_json::from_slice(&buf) {
        Ok(v) => v,
        Err(_) => return Ok((0, len, batch_bytes)),
    };
    let arr = match v.as_array() {
        Some(a) => a,
        None => return Ok((0, len, batch_bytes)),
    };

    let mut ok = 0usize;
    let mut err = 0usize;
    for item in arr {
        if item.get("result").is_some() {
            ok += 1;
        } else {
            err += 1;
        }
    }
    Ok((ok, err, batch_bytes))
}

async fn download(args: Args, run_index: usize) -> Result<RunSummary> {
    let client = Client::builder()
        .user_agent("solana-backfill/0.1")
        .timeout(Duration::from_secs(args.timeout))
        .pool_max_idle_per_host(20)
        .pool_idle_timeout(Duration::from_secs(30))
        .tcp_keepalive(Duration::from_secs(60))
        .build()?;

    let (start_slot, end_slot) = if let Some(latest_n) = args.from_latest {
        let current = get_slot(&client, &args.rpc, &args.api_key).await?;
        let start = current.saturating_sub(latest_n as u64 - 1);
        (start, current)
    } else {
        let start = args.start_slot.unwrap_or(0);
        let end = args.end_slot.unwrap_or(start);
        (start, end)
    };

    if end_slot < start_slot {
        anyhow::bail!("end_slot must be >= start_slot");
    }

    let slots: Vec<u64> = (start_slot..=end_slot).collect();
    let total = slots.len();
    let batch_size = args.batch_size;
    let output_dir = PathBuf::from(args.output);
    fs::create_dir_all(&output_dir)?;

    println!("[run {}] Slots {} -> {} ({} blocks) [{}/{}]", run_index + 1, start_slot, end_slot, total, batch_size, args.max_concurrent);

    let semaphore = std::sync::Arc::new(Semaphore::new(args.max_concurrent));
    let mut handles = Vec::new();
    let start = Instant::now();

    for start_idx in (0..total).step_by(batch_size) {
        let end_idx = (start_idx + batch_size).min(total);
        let url = args.rpc.clone();
        let api_key = args.api_key.clone();
        let slots = slots.clone();
        let client = client.clone();
        let permit = semaphore.clone().acquire_owned().await?;

        handles.push(tokio::spawn(async move {
            let _p = permit;
            let count = end_idx - start_idx;
            match send_batch(&client, &url, &api_key, &slots, count, start_idx).await {
                Ok((ok, err, bytes)) => {
                    println!("[{}] {}->{} ok={} err={} bytes={}", run_index + 1, start_idx, end_idx - 1, ok, err, bytes);
                    (ok, err, bytes)
                }
                Err(_) => (0, count, 0),
            }
        }));
    }

    let mut total_ok = 0usize;
    let mut total_err = 0usize;
    let mut total_bytes = 0u64;

    for h in handles {
        if let Ok((ok, err, bytes)) = h.await {
            total_ok += ok;
            total_err += err;
            total_bytes += bytes;
        }
    }

    let elapsed = start.elapsed().as_secs_f64();
    Ok(RunSummary::new(batch_size, args.max_concurrent, total_ok, total_err, total_bytes, elapsed, total))
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    if let Some(runs) = args.runs {
        let mut summaries = Vec::new();
        for i in 0..runs {
            let run_dir = format!("{}/run_{}", args.output, i + 1);
            let mut run_args = args.clone();
            run_args.output = run_dir.clone();
            let summary = download(run_args, i).await?;
            println!("[run {}] Done: {} ok, {} err, {:.2} MB/s, {:.2} blocks/s in {:.2}s", i + 1, summary.ok, summary.err, summary.mb_per_sec, summary.blocks_per_sec, summary.elapsed);
            summaries.push(summary);
        }

        println!("\n=== competition ({}) ===", runs);
        println!("{:<6} {:<12} {:<8} {:<8} {:<12} {:<14} {:<12}", "batch", "concurr", "ok", "err", "MB/s", "blocks/s", "elapsed(s)");
        for s in &summaries {
            println!("{:<6} {:<12} {:<8} {:<8} {:<12.2} {:<14.2} {:<12.2}", s.batch, s.concurrency, s.ok, s.err, s.mb_per_sec, s.blocks_per_sec, s.elapsed);
        }
        Ok(())
    } else {
        let summary = download(args.clone(), 0).await?;
        println!("Done: {} ok, {} err, {:.2} MB/s, {:.2} blocks/s in {:.2}s", summary.ok, summary.err, summary.mb_per_sec, summary.blocks_per_sec, summary.elapsed);
        Ok(())
    }
}
