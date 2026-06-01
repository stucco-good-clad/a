use anyhow::Result;
use clap::Parser;
use reqwest::Client;
use serde_json::{json, Value};
use std::fs;
use std::path::PathBuf;
use std::time::{Duration, Instant};
use tokio::sync::Semaphore;

#[derive(Parser, Debug)]
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
}

#[derive(Debug)]
struct RunResult {
    ok: usize,
    err: usize,
    mb_per_sec: f64,
}

impl RunResult {
    fn new(ok: usize, err: usize, mb_per_sec: f64) -> Self {
        Self { ok, err, mb_per_sec }
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
) -> Result<(usize, usize), anyhow::Error> {
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
    let bytes = resp.bytes().await?;
    let buf = bytes.to_vec();

    if !status.is_success() {
        return Ok((0, len));
    }

    let v: Value = match serde_json::from_slice(&buf) {
        Ok(v) => v,
        Err(_) => return Ok((0, len)),
    };
    let arr = match v.as_array() {
        Some(a) => a,
        None => return Ok((0, len)),
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
    Ok((ok, err))
}

async fn download_blocks(args: Args) -> Result<RunResult> {
    let client = Client::builder()
        .user_agent("solana-backfill/0.1")
        .timeout(Duration::from_secs(args.timeout))
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

    println!(
        "Slots {} -> {} ({} blocks)",
        start_slot, end_slot, total
    );

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
                Ok((ok, err)) => (ok, err, ok as u64),
                Err(_) => (0, count, 0),
            }
        }));
    }

    let mut total_ok = 0usize;
    let mut total_err = 0usize;
    let mut total_blocks_written = 0u64;

    for h in handles {
        if let Ok((ok, err, written)) = h.await {
            total_ok += ok;
            total_err += err;
            total_blocks_written += written;
        }
    }

    let elapsed = start.elapsed();
    let bytes = total_blocks_written * 1024;

    Ok(RunResult::new(total_ok, total_err, bytes, elapsed))
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let args = Args::parse();

    let result = download_blocks(args).await?;

    println!(
        "Done: {} ok, {} err, {:.2} MB/s",
        result.ok, result.err, result.mb_per_sec
    );

    Ok(())
}
