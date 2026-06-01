use anyhow::{Context, Result};
use clap::Parser;
use reqwest::Client;
use serde_json::{json, Value};
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Semaphore;

// ── CLI ───────────────────────────────────────────────────────────────────────

#[derive(Parser, Debug, Clone)]
#[command(author, version, about = "High-throughput Solana block backfiller")]
struct Args {
    /// Solana RPC endpoint URL
    #[arg(short, long)]
    rpc: String,

    /// API key for RPC (optional)
    #[arg(short, long)]
    api_key: Option<String>,

    /// Start slot (inclusive)
    #[arg(short, long)]
    start_slot: Option<u64>,

    /// End slot (inclusive)
    #[arg(short, long)]
    end_slot: Option<u64>,

    /// Number of blocks per JSON-RPC batch request
    #[arg(short, long, default_value = "10")]
    batch_size: usize,

    /// Maximum concurrent in-flight batch requests
    #[arg(short, long, default_value = "20")]
    max_concurrent: usize,

    /// Output directory for block JSON files
    #[arg(short, long, default_value = "./blocks")]
    output: String,

    /// Per-request timeout in seconds
    #[arg(long, default_value = "60")]
    timeout: u64,

    /// Backfill the last N slots ending at the current tip
    #[arg(long)]
    from_latest: Option<usize>,

    /// Retry count per batch on transient failure
    #[arg(long, default_value = "3")]
    retries: usize,

    /// Benchmark: run the same config N times and compare throughput
    #[arg(long)]
    runs: Option<usize>,

    /// Suppress per-batch progress lines (quieter output)
    #[arg(long)]
    quiet: bool,
}

// ── Summary ───────────────────────────────────────────────────────────────────

#[derive(Debug)]
struct RunSummary {
    batch: usize,
    concurrency: usize,
    ok: usize,
    err: usize,
    skipped: usize,
    mb_per_sec: f64,
    elapsed: f64,
    blocks_per_sec: f64,
}

impl RunSummary {
    fn new(
        batch: usize,
        concurrency: usize,
        ok: usize,
        err: usize,
        skipped: usize,
        bytes: u64,
        elapsed: f64,
        total: usize,
    ) -> Self {
        let mb_per_sec = if elapsed > 0.0 {
            (bytes as f64 / 1_048_576.0) / elapsed
        } else {
            0.0
        };
        let blocks_per_sec = if elapsed > 0.0 { total as f64 / elapsed } else { 0.0 };
        Self { batch, concurrency, ok, err, skipped, mb_per_sec, elapsed, blocks_per_sec }
    }
}

// ── RPC helpers ───────────────────────────────────────────────────────────────

/// Build the shared HTTP client once per run with tuned pool settings.
fn build_client(args: &Args) -> Result<Client> {
    let pool_max = (args.max_concurrent * 2).min(256);
    Client::builder()
    .user_agent("solana-backfill/0.2")
    .timeout(Duration::from_secs(args.timeout))
    .tcp_keepalive(Duration::from_secs(60))
    .tcp_nodelay(true)                          // no Nagle — we send large bodies
    .pool_max_idle_per_host(pool_max)
    .pool_idle_timeout(Duration::from_secs(30))
    // http2_prior_knowledge() omitted: only works if the server speaks H2 on plain HTTP.
    // Most Solana RPC providers (including plain http://) use HTTP/1.1; hyper will
    // negotiate H2 automatically on TLS (https://) endpoints via ALPN.
    .build()
    .context("failed to build HTTP client")
}

#[inline]
fn add_auth<'a>(
    mut req: reqwest::RequestBuilder,
    api_key: &Option<String>,
) -> reqwest::RequestBuilder {
    if let Some(key) = api_key {
        req = req.header("x-api-key", key.as_str());
    }
    req
}

async fn get_slot(client: &Client, url: &str, api_key: &Option<String>) -> Result<u64> {
    let body = json!({"jsonrpc":"2.0","id":1,"method":"getSlot","params":[]});
    let req = add_auth(
        client.post(url).header("content-type", "application/json"),
                       api_key,
    );
    let v: Value = req
    .body(body.to_string())
    .send()
    .await
    .context("getSlot request failed")?
    .json()
    .await
    .context("getSlot JSON parse failed")?;

    if let Some(e) = v.get("error") {
        anyhow::bail!("getSlot RPC error: {e}");
    }
    v.get("result")
    .and_then(Value::as_u64)
    .filter(|&s| s > 0)
    .context("invalid slot in getSlot response")
}

// ── Batch fetch ───────────────────────────────────────────────────────────────

/// Returns (ok_blocks, err_blocks, skipped_blocks, response_bytes).
/// "skipped" = RPC returned a null result (slot was skipped on-chain), not a real error.
async fn send_batch(
    client: &Client,
    url: &str,
    api_key: &Option<String>,
    slots: &[u64],
    retries: usize,
) -> (usize, usize, usize, u64) {
    // Build the batch request body once.
    let batch: Vec<Value> = slots
    .iter()
    .enumerate()
    .map(|(i, &slot)| {
        json!({
            "jsonrpc": "2.0",
            "id": i,
            "method": "getBlock",
            "params": [
                slot,
                {
                    "encoding": "json",
                    "transactionDetails": "full",
                    "maxSupportedTransactionVersion": 0,
                    "rewards": false
                }
            ]
        })
    })
    .collect();

    // Serialise once; clone bytes cheaply via Arc/Bytes for retries.
    let body_bytes: bytes::Bytes = match serde_json::to_vec(&batch) {
        Ok(b) => b.into(),
        Err(_) => return (0, slots.len(), 0, 0),
    };

    let count = slots.len();
    let mut attempt = 0usize;

    loop {
        attempt += 1;
        let req = add_auth(
            client.post(url).header("content-type", "application/json"),
                           api_key,
        );
        let result = req.body(body_bytes.clone()).send().await;

        match result {
            Err(_) if attempt <= retries => {
                tokio::time::sleep(Duration::from_millis(200 * attempt as u64)).await;
                continue;
            }
            Err(_) => return (0, count, 0, 0),
            Ok(resp) => {
                let status = resp.status();
                // Read body regardless so the TCP connection is returned to the pool.
                let buf: bytes::Bytes = match resp.bytes().await {
                    Ok(b) => b,
                    Err(_) if attempt <= retries => {
                        tokio::time::sleep(Duration::from_millis(200 * attempt as u64)).await;
                        continue;
                    }
                    Err(_) => return (0, count, 0, 0),
                };
                let resp_bytes = buf.len() as u64;

                if !status.is_success() {
                    if attempt <= retries {
                        tokio::time::sleep(Duration::from_millis(300 * attempt as u64)).await;
                        continue;
                    }
                    return (0, count, 0, resp_bytes);
                }

                let arr: Vec<Value> = match serde_json::from_slice(&buf) {
                    Ok(Value::Array(a)) => a,
                    _ if attempt <= retries => {
                        tokio::time::sleep(Duration::from_millis(200 * attempt as u64)).await;
                        continue;
                    }
                    _ => return (0, count, 0, resp_bytes),
                };

                let mut ok = 0usize;
                let mut err = 0usize;
                let mut skipped = 0usize;
                for item in &arr {
                    if item.get("result").map(|r| !r.is_null()).unwrap_or(false) {
                        ok += 1;
                    } else if item
                        .get("error")
                        .and_then(|e| e.get("code"))
                        .and_then(Value::as_i64)
                        == Some(-32009)      // SlotSkipped — not a real error
                        {
                            skipped += 1;
                        } else if item.get("result").map(Value::is_null).unwrap_or(false) {
                            skipped += 1;       // null result also means skipped slot
                        } else {
                            err += 1;
                        }
                }

                return (ok, err, skipped, resp_bytes);
            }
        }
    }
}

// ── Core download loop ────────────────────────────────────────────────────────

async fn download(args: &Args, run_index: usize) -> Result<RunSummary> {
    let client = build_client(args)?;

    let (start_slot, end_slot) = if let Some(n) = args.from_latest {
        let tip = get_slot(&client, &args.rpc, &args.api_key).await?;
        (tip.saturating_sub(n as u64 - 1), tip)
    } else {
        let s = args.start_slot.unwrap_or(0);
        let e = args.end_slot.unwrap_or(s);
        (s, e)
    };

    anyhow::ensure!(end_slot >= start_slot, "end_slot must be >= start_slot");

    let total = (end_slot - start_slot + 1) as usize;
    let batch_size = args.batch_size;
    let output_dir = PathBuf::from(&args.output);
    fs::create_dir_all(&output_dir)
    .with_context(|| format!("cannot create output dir {:?}", output_dir))?;

    eprintln!(
        "[run {}] slots {}..={} ({} blocks) batch={} concurrency={}",
              run_index + 1,
              start_slot,
              end_slot,
              total,
              batch_size,
              args.max_concurrent
    );

    // Shared atomic counters — avoids Mutex for hot-path aggregation.
    let total_ok = Arc::new(AtomicUsize::new(0));
    let total_err = Arc::new(AtomicUsize::new(0));
    let total_skipped = Arc::new(AtomicUsize::new(0));
    let total_bytes = Arc::new(AtomicU64::new(0));
    let total_done = Arc::new(AtomicUsize::new(0));

    let sem = Arc::new(Semaphore::new(args.max_concurrent));
    let num_batches = total.div_ceil(batch_size);
    let mut handles = Vec::with_capacity(num_batches);
    let wall = Instant::now();
    let quiet = args.quiet;

    for (batch_idx, start_idx) in (0..total).step_by(batch_size).enumerate() {
        let end_idx = (start_idx + batch_size).min(total);
        let chunk: Vec<u64> = ((start_slot + start_idx as u64)
        ..(start_slot + end_idx as u64))
        .collect();

        let url = args.rpc.clone();
        let api_key = args.api_key.clone();
        let client = client.clone();
        let retries = args.retries;
        let run_no = run_index + 1;

        // Counters
        let c_ok = Arc::clone(&total_ok);
        let c_err = Arc::clone(&total_err);
        let c_skip = Arc::clone(&total_skipped);
        let c_bytes = Arc::clone(&total_bytes);
        let c_done = Arc::clone(&total_done);

        // Acquire permit *inside* the spawned task so the main loop stays
        // non-blocking and all spawns complete immediately.
        let sem = Arc::clone(&sem);

        handles.push(tokio::spawn(async move {
            let _permit = sem.acquire_owned().await.expect("semaphore closed");
            let (ok, err, skip, bytes) =
            send_batch(&client, &url, &api_key, &chunk, retries).await;

            c_ok.fetch_add(ok, Ordering::Relaxed);
            c_err.fetch_add(err, Ordering::Relaxed);
            c_skip.fetch_add(skip, Ordering::Relaxed);
            c_bytes.fetch_add(bytes, Ordering::Relaxed);
            let done = c_done.fetch_add(ok + err + skip, Ordering::Relaxed) + ok + err + skip;

            if !quiet {
                eprintln!(
                    "[{}] batch {:>4} slots {}-{} ok={} err={} skip={} bytes={} done={}",
                    run_no,
                    batch_idx,
                    chunk.first().unwrap_or(&0),
                          chunk.last().unwrap_or(&0),
                          ok,
                          err,
                          skip,
                          bytes,
                          done,
                );
            }
        }));
    }

    // Await all tasks (futures are already spawned — no blocking here).
    for h in handles {
        // Propagate panics; ignore individual task errors (already counted).
        let _ = h.await;
    }

    let elapsed = wall.elapsed().as_secs_f64();
    Ok(RunSummary::new(
        batch_size,
        args.max_concurrent,
        total_ok.load(Ordering::Relaxed),
                       total_err.load(Ordering::Relaxed),
                       total_skipped.load(Ordering::Relaxed),
                       total_bytes.load(Ordering::Relaxed),
                       elapsed,
                       total,
    ))
}

// ── Entry point ───────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    if let Some(runs) = args.runs {
        let mut summaries = Vec::with_capacity(runs);
        for i in 0..runs {
            let mut run_args = args.clone();
            run_args.output = format!("{}/run_{}", args.output, i + 1);
            let s = download(&run_args, i).await?;
            eprintln!(
                "[run {}] ok={} err={} skip={} {:.2} MB/s  {:.1} blk/s  {:.2}s",
                i + 1,
                s.ok,
                s.err,
                s.skipped,
                s.mb_per_sec,
                s.blocks_per_sec,
                s.elapsed
            );
            summaries.push(s);
        }

        println!(
            "\n=== benchmark ({runs} runs) ===\n{:<6} {:<12} {:<8} {:<8} {:<8} {:<10} {:<14} {:<12}",
                 "batch", "concurr", "ok", "err", "skip", "MB/s", "blocks/s", "elapsed(s)"
        );
        for s in &summaries {
            println!(
                "{:<6} {:<12} {:<8} {:<8} {:<8} {:<10.2} {:<14.2} {:<12.2}",
                s.batch,
                s.concurrency,
                s.ok,
                s.err,
                s.skipped,
                s.mb_per_sec,
                s.blocks_per_sec,
                s.elapsed
            );
        }
    } else {
        let s = download(&args, 0).await?;
        println!(
            "done  ok={}  err={}  skip={}  {:.2} MB/s  {:.1} blk/s  {:.2}s",
            s.ok, s.err, s.skipped, s.mb_per_sec, s.blocks_per_sec, s.elapsed
        );
    }

    Ok(())
}
