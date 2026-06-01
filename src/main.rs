//! solana-backfill — diagnostic + max-throughput Solana block backfiller
//!
//! Use --bench to sweep batch_size × concurrency × endpoints automatically.
//! Per-batch timing shows exactly where time goes: connect / ttfb / body / parse / write.

use anyhow::{Context, Result};
use bytes::Bytes;
use clap::Parser;
use flate2::{write::GzEncoder, Compression};
use governor::{DefaultDirectRateLimiter, Quota, RateLimiter};
use reqwest::Client;
use serde_json::{json, Value};
use std::io::Write as IoWrite;
use std::num::NonZeroU32;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, Semaphore};

// ── CLI ───────────────────────────────────────────────────────────────────────

#[derive(Parser, Debug, Clone)]
#[command(author, version, about = "Max-throughput Solana block backfiller with diagnostics")]
struct Args {
    /// RPC endpoint(s). Repeat for multi-endpoint pooling: --rpc URL1 --rpc URL2
    #[arg(short, long, required = true)]
    rpc: Vec<String>,

    /// API key(s) — same index order as --rpc.
    #[arg(short, long)]
    api_key: Vec<String>,

    /// Start slot (inclusive)
    #[arg(short, long)]
    start_slot: Option<u64>,

    /// End slot (inclusive)
    #[arg(short, long)]
    end_slot: Option<u64>,

    /// Slots per JSON-RPC batch request
    #[arg(short, long, default_value = "10")]
    batch_size: usize,

    /// Max in-flight batches per endpoint
    #[arg(short, long, default_value = "20")]
    max_concurrent: usize,

    /// Output directory
    #[arg(short, long, default_value = "./blocks")]
    output: String,

    /// Per-request timeout in seconds
    #[arg(long, default_value = "120")]
    timeout: u64,

    /// Backfill the last N slots from current tip
    #[arg(long)]
    from_latest: Option<usize>,

    /// Retries per batch on transient failure
    #[arg(long, default_value = "4")]
    retries: usize,

    /// Rate limit per endpoint (requests/sec)
    #[arg(long, default_value = "10")]
    rps: u32,

    /// Transaction detail: full | accounts | signatures | none
    #[arg(long, default_value = "full")]
    tx_detail: String,

    /// Gzip output files (.ndjson.gz)
    #[arg(long, default_value = "true")]
    compress: bool,

    /// Writer queue depth
    #[arg(long, default_value = "512")]
    write_queue: usize,

    /// Suppress per-batch lines
    #[arg(long)]
    quiet: bool,

    /// BENCHMARK MODE: sweep batch_size=[5,10,20,50,100] × concurrency=[5,10,20,40]
    /// Uses --from-latest 200 for each cell. Prints comparison table at end.
    /// Add --bench-slots N to change sample size (default 200).
    #[arg(long)]
    bench: bool,

    /// Slots to fetch per benchmark cell (default 200)
    #[arg(long, default_value = "200")]
    bench_slots: usize,

    /// Print per-batch timing breakdown (ttfb / body_read / parse / write_queue)
    #[arg(long)]
    timing: bool,
}

// ── Endpoint ──────────────────────────────────────────────────────────────────

struct Endpoint {
    url:     String,
    api_key: Option<String>,
    client:  Client,
    limiter: Arc<DefaultDirectRateLimiter>,
}

impl Endpoint {
    fn new(url: String, api_key: Option<String>, args: &Args) -> Result<Self> {
        let pool_size = (args.max_concurrent * 2).min(256);
        let client = Client::builder()
        .user_agent("solana-backfill/0.5")
        .timeout(Duration::from_secs(args.timeout))
        .tcp_keepalive(Duration::from_secs(60))
        .tcp_nodelay(true)
        .pool_max_idle_per_host(pool_size)
        .pool_idle_timeout(Duration::from_secs(30))
        .build()
        .context("HTTP client build failed")?;

        let rps = NonZeroU32::new(args.rps).unwrap_or(NonZeroU32::new(10).unwrap());
        let limiter = Arc::new(RateLimiter::direct(Quota::per_second(rps)));
        Ok(Self { url, api_key, client, limiter })
    }

    fn new_with_concurrency(url: String, api_key: Option<String>, args: &Args, concurrency: usize) -> Result<Self> {
        let mut a = args.clone();
        a.max_concurrent = concurrency;
        Self::new(url, api_key, idx, &a)
    }
}

// ── Writer ────────────────────────────────────────────────────────────────────

struct WriteJob {
    path:     PathBuf,
    lines:    Vec<Bytes>,
    compress: bool,
}

async fn writer_task(mut rx: mpsc::Receiver<WriteJob>) {
    while let Some(job) = rx.recv().await {
        let _ = tokio::task::spawn_blocking(move || -> Result<()> {
            if job.compress {
                let f = std::fs::File::create(&job.path)?;
                let mut gz = GzEncoder::new(f, Compression::new(1));
                for line in &job.lines {
                    gz.write_all(line)?;
                    gz.write_all(b"\n")?;
                }
                gz.finish()?;
            } else {
                let cap: usize = job.lines.iter().map(|l| l.len() + 1).sum();
                let mut buf = Vec::with_capacity(cap);
                for line in &job.lines {
                    buf.extend_from_slice(line);
                    buf.push(b'\n');
                }
                std::fs::write(&job.path, &buf)?;
            }
            Ok(())
        })
        .await;
    }
}

// ── Per-batch timing ──────────────────────────────────────────────────────────

#[derive(Default, Debug, Clone)]
struct BatchTiming {
    wait_ms:   f64,  // time waiting for rate limiter + semaphore
    ttfb_ms:   f64,  // time to first byte (send → response headers)
    body_ms:   f64,  // time to read full response body
    parse_ms:  f64,  // serde_json parse time
    encode_ms: f64,  // re-serialise records for ndjson
    wire_kb:   f64,
}

// Global timing accumulators (relaxed — approximate is fine for diagnostics)
static WAIT_US:   AtomicU64 = AtomicU64::new(0);
static TTFB_US:   AtomicU64 = AtomicU64::new(0);
static BODY_US:   AtomicU64 = AtomicU64::new(0);
static PARSE_US:  AtomicU64 = AtomicU64::new(0);
static ENCODE_US: AtomicU64 = AtomicU64::new(0);
static BATCH_COUNT: AtomicU64 = AtomicU64::new(0);

// ── RPC ───────────────────────────────────────────────────────────────────────

async fn get_slot(ep: &Endpoint) -> Result<u64> {
    ep.limiter.until_ready().await;
    let mut req = ep.client.post(&ep.url).header("content-type", "application/json");
    if let Some(k) = &ep.api_key { req = req.header("x-api-key", k); }
    let v: Value = req
    .body(r#"{"jsonrpc":"2.0","id":1,"method":"getSlot","params":[]}"#)
    .send().await?.json().await?;
    if let Some(e) = v.get("error") { anyhow::bail!("getSlot error: {e}"); }
    v.get("result").and_then(Value::as_u64).filter(|&s| s > 0)
    .context("invalid slot in getSlot response")
}

async fn send_batch(
    ep:        &Endpoint,
    slots:     &[u64],
    tx_detail: &str,
    retries:   usize,
    timing:    bool,
) -> (usize, usize, usize, u64, Vec<Bytes>, BatchTiming) {
    let count = slots.len();
    let mut t = BatchTiming::default();

    let batch: Vec<Value> = slots.iter().enumerate().map(|(i, &slot)| {
        json!({
            "jsonrpc": "2.0", "id": i,
            "method": "getBlock",
            "params": [slot, {
                "encoding": "json",
                "transactionDetails": tx_detail,
                "maxSupportedTransactionVersion": 0,
                "rewards": false,
            }]
        })
    }).collect();

    let body: Bytes = match serde_json::to_vec(&batch) {
        Ok(b) => b.into(),
        Err(_) => return (0, count, 0, 0, vec![], t),
    };

    let mut attempt = 0usize;
    loop {
        attempt += 1;

        let t0 = Instant::now();
        ep.limiter.until_ready().await;
        let wait_ms = t0.elapsed().as_secs_f64() * 1000.0;

        let mut req = ep.client.post(&ep.url)
        .header("content-type", "application/json")
        .header("accept-encoding", "br, gzip, deflate");
        if let Some(k) = &ep.api_key { req = req.header("x-api-key", k); }

        let t_send = Instant::now();
        let resp = match req.body(body.clone()).send().await {
            Err(_) if attempt <= retries => {
                tokio::time::sleep(backoff(attempt)).await; continue;
            }
            Err(_) => return (0, count, 0, 0, vec![], t),
            Ok(r) => r,
        };
        let ttfb_ms = t_send.elapsed().as_secs_f64() * 1000.0;

        let status = resp.status();
        let t_body = Instant::now();
        let buf: Bytes = match resp.bytes().await {
            Err(_) if attempt <= retries => {
                tokio::time::sleep(backoff(attempt)).await; continue;
            }
            Err(_) => return (0, count, 0, 0, vec![], t),
            Ok(b) => b,
        };
        let body_ms = t_body.elapsed().as_secs_f64() * 1000.0;
        let wire_bytes = buf.len() as u64;

        if !status.is_success() {
            let delay = if status.as_u16() == 429 { Duration::from_secs(2) } else { backoff(attempt) };
            if attempt <= retries { tokio::time::sleep(delay).await; continue; }
            return (0, count, 0, wire_bytes, vec![], t);
        }

        let t_parse = Instant::now();
        let arr: Vec<Value> = match serde_json::from_slice(&buf) {
            Ok(Value::Array(a)) => a,
            _ if attempt <= retries => {
                tokio::time::sleep(backoff(attempt)).await; continue;
            }
            _ => return (0, count, 0, wire_bytes, vec![], t),
        };
        let parse_ms = t_parse.elapsed().as_secs_f64() * 1000.0;

        let t_enc = Instant::now();
        let mut ok = 0usize;
        let mut err = 0usize;
        let mut skipped = 0usize;
        let mut lines: Vec<Bytes> = Vec::with_capacity(arr.len());
        for (item, &slot) in arr.iter().zip(slots.iter()) {
            let result = item.get("result");
            if result.map(|r| !r.is_null()).unwrap_or(false) {
                if let Ok(b) = serde_json::to_vec(&json!({ "slot": slot, "block": result.unwrap() })) {
                    lines.push(b.into());
                }
                ok += 1;
            } else if is_skipped(item) {
                skipped += 1;
            } else {
                err += 1;
            }
        }
        let encode_ms = t_enc.elapsed().as_secs_f64() * 1000.0;

        t = BatchTiming { wait_ms, ttfb_ms, body_ms, parse_ms, encode_ms, wire_kb: wire_bytes as f64 / 1024.0 };

        if timing {
            // Accumulate into globals
            WAIT_US.fetch_add((wait_ms * 1000.0) as u64,   Ordering::Relaxed);
            TTFB_US.fetch_add((ttfb_ms * 1000.0) as u64,   Ordering::Relaxed);
            BODY_US.fetch_add((body_ms * 1000.0) as u64,   Ordering::Relaxed);
            PARSE_US.fetch_add((parse_ms * 1000.0) as u64, Ordering::Relaxed);
            ENCODE_US.fetch_add((encode_ms * 1000.0) as u64, Ordering::Relaxed);
            BATCH_COUNT.fetch_add(1, Ordering::Relaxed);
        }

        return (ok, err, skipped, wire_bytes, lines, t);
    }
}

#[inline] fn backoff(a: usize) -> Duration { Duration::from_millis((250 * a as u64).min(4000)) }
#[inline] fn is_skipped(item: &Value) -> bool {
item.get("result").map(Value::is_null).unwrap_or(false)
|| item.get("error").and_then(|e| e.get("code")).and_then(Value::as_i64) == Some(-32009)
}

// ── Download ──────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
struct RunResult {
    batch_size:  usize,
    concurrency: usize,
    n_endpoints: usize,
    ok:          usize,
    err:         usize,
    skip:        usize,
    mbps:        f64,
    blkps:       f64,
    elapsed:     f64,
    wire_mb:     f64,
}

async fn run_download(
    args:        &Args,
    endpoints:   &[Arc<Endpoint>],
    start_slot:  u64,
    end_slot:    u64,
    batch_size:  usize,
    concurrency: usize,
    output_dir:  &PathBuf,
    write_tx:    &mpsc::Sender<WriteJob>,
    label:       &str,
) -> Result<RunResult> {
    let total    = (end_slot - start_slot + 1) as usize;
    let n_ep     = endpoints.len();
    let compress = args.compress;
    let quiet    = args.quiet;
    let timing   = args.timing;
    let ext      = if compress { "ndjson.gz" } else { "ndjson" };

    let c_ok    = Arc::new(AtomicUsize::new(0));
    let c_err   = Arc::new(AtomicUsize::new(0));
    let c_skip  = Arc::new(AtomicUsize::new(0));
    let c_bytes = Arc::new(AtomicU64::new(0));
    let c_done  = Arc::new(AtomicUsize::new(0));

    // Per-endpoint semaphores (concurrency per endpoint, not global)
    let sems: Vec<Arc<Semaphore>> = (0..n_ep)
    .map(|_| Arc::new(Semaphore::new(concurrency)))
    .collect();

    let wall      = Instant::now();
    let retries   = args.retries;
    let tx_detail = args.tx_detail.clone();

    let num_batches = total.div_ceil(batch_size);
    let mut handles = Vec::with_capacity(num_batches);

    for (batch_idx, start_idx) in (0..total).step_by(batch_size).enumerate() {
        let end_idx = (start_idx + batch_size).min(total);
        let chunk: Vec<u64> = ((start_slot + start_idx as u64)..(start_slot + end_idx as u64)).collect();

        let ep_idx   = batch_idx % n_ep;
        let ep       = Arc::clone(&endpoints[ep_idx]);
        let sem      = Arc::clone(&sems[ep_idx]);
        let write_tx = write_tx.clone();
        let tx_det   = tx_detail.clone();
        let lbl      = label.to_string();

        let out_path = output_dir.join(format!(
            "{:010}-{:010}.{}", chunk[0], *chunk.last().unwrap(), ext
        ));

        let (c_ok, c_err, c_skip, c_bytes, c_done) = (
            Arc::clone(&c_ok), Arc::clone(&c_err), Arc::clone(&c_skip),
                                                      Arc::clone(&c_bytes), Arc::clone(&c_done),
        );

        handles.push(tokio::spawn(async move {
            let t_wait = Instant::now();
            let _permit = sem.acquire_owned().await.expect("sem closed");
            let sem_wait_ms = t_wait.elapsed().as_secs_f64() * 1000.0;

            let (ok, err, skip, bytes, lines, bt) =
            send_batch(&ep, &chunk, &tx_det, retries, timing).await;

            c_ok.fetch_add(ok,     Ordering::Relaxed);
            c_err.fetch_add(err,   Ordering::Relaxed);
            c_skip.fetch_add(skip, Ordering::Relaxed);
            c_bytes.fetch_add(bytes, Ordering::Relaxed);
            let done = c_done.fetch_add(ok + err + skip, Ordering::Relaxed) + ok + err + skip;

            if !lines.is_empty() {
                let _ = write_tx.try_send(WriteJob { path: out_path, lines, compress });
            }

            if !quiet {
                if timing {
                    eprintln!(
                        "[{lbl}][ep{ep_idx}] #{batch_idx:<4} \
sem_wait={sem_wait_ms:.0}ms ratelim_wait={:.0}ms \
ttfb={:.0}ms body={:.0}ms parse={:.0}ms enc={:.0}ms \
wire={:.1}KB  ok={ok} done={done}",
bt.wait_ms, bt.ttfb_ms, bt.body_ms, bt.parse_ms, bt.encode_ms, bt.wire_kb,
                    );
                } else {
                    eprintln!(
                        "[{lbl}][ep{ep_idx}] #{batch_idx:<4} slots {}-{}  \
ok={ok} err={err} skip={skip} wire={:.1}KB done={done}/{}",
chunk.first().unwrap_or(&0), chunk.last().unwrap_or(&0),
                              bytes as f64 / 1024.0, total,
                    );
                }
            }
        }));
    }

    for h in handles { let _ = h.await; }

    let elapsed = wall.elapsed().as_secs_f64();
    let ok      = c_ok.load(Ordering::Relaxed);
    let err     = c_err.load(Ordering::Relaxed);
    let skip    = c_skip.load(Ordering::Relaxed);
    let bytes   = c_bytes.load(Ordering::Relaxed);

    Ok(RunResult {
        batch_size,
       concurrency,
       n_endpoints: n_ep,
       ok, err, skip,
       mbps:    (bytes as f64 / 1_048_576.0) / elapsed,
       blkps:   ok as f64 / elapsed,
       elapsed,
       wire_mb: bytes as f64 / 1_048_576.0,
    })
}

// ── Benchmark sweep ───────────────────────────────────────────────────────────

async fn run_bench(args: &Args) -> Result<()> {
    eprintln!("═══ BENCHMARK MODE ═══");
    eprintln!("Will sweep batch_size × concurrency. Each cell uses {} slots.", args.bench_slots);
    eprintln!("Endpoints: {}", args.rpc.len());

    // Resolve tip once
    let probe_ep = Arc::new(Endpoint::new(
        args.rpc[0].clone(),
                                          args.api_key.first().cloned().filter(|s: &String| !s.is_empty()),
                                          &args,
    )?);
    let tip = get_slot(&probe_ep).await?;
    let start_slot = tip.saturating_sub(args.bench_slots as u64 - 1);
    let end_slot = tip;
    eprintln!("Slots {}..={}", start_slot, end_slot);

    let output_dir = PathBuf::from(&args.output);
    std::fs::create_dir_all(&output_dir)?;
    let (write_tx, write_rx) = mpsc::channel::<WriteJob>(args.write_queue);
    tokio::spawn(writer_task(write_rx));

    let batch_sizes  = [5usize, 10, 20, 50, 100];
    let concurrencies = [5usize, 10, 20, 40];
    let mut results: Vec<RunResult> = Vec::new();

    for &bs in &batch_sizes {
        for &cc in &concurrencies {
            // Rebuild endpoints with correct pool size for this concurrency
            let endpoints: Vec<Arc<Endpoint>> = args.rpc.iter().enumerate()
            .map(|(i, url)| {
                let key = args.api_key.get(i).cloned().filter(|s: &String| !s.is_empty());
                Endpoint::new_with_concurrency(url.clone(), key, args, cc).map(Arc::new)
            })
            .collect::<Result<_>>()?;

            let label = format!("b{bs}c{cc}ep{}", endpoints.len());
            eprintln!("\n── {label} ──");

            // Small warm-up sleep between cells so rate limiter token buckets reset
            tokio::time::sleep(Duration::from_millis(500)).await;

            let r = run_download(
                args, &endpoints,
                start_slot, end_slot,
                bs, cc,
                &output_dir.join(&label),
                                 &write_tx,
                                 &label,
            ).await?;

            eprintln!("  → {:.1} blk/s  {:.2} MB/s  {:.1}s  ok={}", r.blkps, r.mbps, r.elapsed, r.ok);
            results.push(r);
        }
    }

    drop(write_tx);

    // Print comparison table
    println!("\n╔═══════════════════════════════════════════════════════════════════════════╗");
    println!("║                      BENCHMARK RESULTS                                   ║");
    println!("╠═════════╦════════════╦═════════╦══════════╦══════════╦══════╦═══════════╣");
    println!("║ batch   ║ concurr    ║ ep      ║ blk/s    ║ MB/s     ║ ok   ║ elapsed   ║");
    println!("╠═════════╬════════════╬═════════╬══════════╬══════════╬══════╬═══════════╣");
    for r in &results {
        println!("║ {:<7} ║ {:<10} ║ {:<7} ║ {:<8.1} ║ {:<8.2} ║ {:<4} ║ {:<9.2}  ║",
                 r.batch_size, r.concurrency, r.n_endpoints, r.blkps, r.mbps, r.ok, r.elapsed);
    }
    println!("╚═════════╩════════════╩═════════╩══════════╩══════════╩══════╩═══════════╝");

    // Find winner
    if let Some(best) = results.iter().max_by(|a, b| a.blkps.partial_cmp(&b.blkps).unwrap()) {
        println!("\n★  Best config: batch={} concurrency={} endpoints={} → {:.1} blk/s",
                 best.batch_size, best.concurrency, best.n_endpoints, best.blkps);
        println!("   Recommended command:");
        println!("   solana-backfill {} --batch-size {} --max-concurrent {} --rps {}",
                 args.rpc.iter().map(|u| format!("--rpc {u}")).collect::<Vec<_>>().join(" "),
                 best.batch_size, best.concurrency, args.rps);
    }

    Ok(())
}

// ── Entry point ───────────────────────────────────────────────────────────────

async fn download(args: &Args) -> Result<()> {
    if args.bench {
        return run_bench(args).await;
    }

    let endpoints: Vec<Arc<Endpoint>> = args.rpc.iter().enumerate()
    .map(|(i, url)| {
        let key = args.api_key.get(i).cloned().filter(|s: &String| !s.is_empty());
        Endpoint::new(url.clone(), key, args).map(Arc::new)
    })
    .collect::<Result<_>>()?;

    let n_ep = endpoints.len();
    let eff_blkps = args.rps as usize * args.batch_size * n_ep;
    eprintln!("endpoints={n_ep} rps={} batch={} concurrency={} → theoretical_max=~{eff_blkps} blk/s",
              args.rps, args.batch_size, args.max_concurrent);
    if args.timing {
        eprintln!("Timing mode ON — per-batch breakdown will be printed");
    }

    let (start_slot, end_slot) = if let Some(n) = args.from_latest {
        let tip = get_slot(&endpoints[0]).await?;
        (tip.saturating_sub(n as u64 - 1), tip)
    } else {
        let s = args.start_slot.unwrap_or(0);
        let e = args.end_slot.unwrap_or(s);
        (s, e)
    };
    anyhow::ensure!(end_slot >= start_slot, "end_slot must be >= start_slot");

    let total = (end_slot - start_slot + 1) as usize;
    eprintln!("slots {}..={} ({} blocks)", start_slot, end_slot, total);

    let output_dir = PathBuf::from(&args.output);
    std::fs::create_dir_all(&output_dir)?;
    let (write_tx, write_rx) = mpsc::channel::<WriteJob>(args.write_queue);
    tokio::spawn(writer_task(write_rx));

    let r = run_download(
        args, &endpoints,
        start_slot, end_slot,
        args.batch_size, args.max_concurrent,
        &output_dir, &write_tx, "run",
    ).await?;

    drop(write_tx);

    eprintln!("\n═══ done ═══");
    eprintln!("ok={} err={} skip={}", r.ok, r.err, r.skip);
    eprintln!("{:.2} MB/s  {:.1} blk/s  {:.1}s", r.mbps, r.blkps, r.elapsed);
    eprintln!("wire total: {:.1} MB", r.wire_mb);

    if args.timing {
        let n = BATCH_COUNT.load(Ordering::Relaxed).max(1);
        let to_ms = |us: u64| us as f64 / 1000.0 / n as f64;
        eprintln!("\n── avg per-batch timing (n={n} batches) ──");
        eprintln!("  rate_limiter_wait : {:.1}ms", to_ms(WAIT_US.load(Ordering::Relaxed)));
        eprintln!("  ttfb              : {:.1}ms", to_ms(TTFB_US.load(Ordering::Relaxed)));
        eprintln!("  body_read         : {:.1}ms", to_ms(BODY_US.load(Ordering::Relaxed)));
        eprintln!("  json_parse        : {:.1}ms", to_ms(PARSE_US.load(Ordering::Relaxed)));
        eprintln!("  ndjson_encode     : {:.1}ms", to_ms(ENCODE_US.load(Ordering::Relaxed)));

        let total_accounted = to_ms(WAIT_US.load(Ordering::Relaxed))
        + to_ms(TTFB_US.load(Ordering::Relaxed))
        + to_ms(BODY_US.load(Ordering::Relaxed))
        + to_ms(PARSE_US.load(Ordering::Relaxed))
        + to_ms(ENCODE_US.load(Ordering::Relaxed));
        eprintln!("  ─────────────────────────────");
        eprintln!("  accounted for     : {total_accounted:.1}ms");
        eprintln!("  (remainder = semaphore contention + tokio scheduling overhead)");
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    download(&Args::parse()).await
}
