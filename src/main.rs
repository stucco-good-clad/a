use reqwest::Client;
use simd_json::prelude::ValueAsContainer;
use simd_json::prelude::ValueObjectAccess;
use simd_json::OwnedValue;
use std::env;
use std::fmt;
use std::time::{Duration, Instant};
use tokio::task::JoinHandle;

use reqwest::ClientBuilder;
use serde_json::json;
use serde_json::Value;

#[derive(Clone, Copy)]
enum BatchErrorKind {
    Timeout,
    Connection,
    HttpStatus,
    JsonParse,
    Empty,
    Io,
    Other,
}

impl fmt::Display for BatchErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BatchErrorKind::Timeout => write!(f, "timeout"),
            BatchErrorKind::Connection => write!(f, "conn"),
            BatchErrorKind::HttpStatus => write!(f, "http"),
            BatchErrorKind::JsonParse => write!(f, "json"),
            BatchErrorKind::Empty => write!(f, "empty"),
            BatchErrorKind::Io => write!(f, "io"),
            BatchErrorKind::Other => write!(f, "other"),
        }
    }
}

struct BatchResult {
    ok: usize,
    err: usize,
    bytes: usize,
    elapsed: Duration,
    error_kind: Option<BatchErrorKind>,
}

fn classify_error(msg: &str) -> BatchErrorKind {
    let m = msg.to_lowercase();
    if m.contains("timeout") || m.contains("timed out") {
        BatchErrorKind::Timeout
    } else if m.contains("connection") || m.contains("dns") || m.contains("unreachable") {
        BatchErrorKind::Connection
    } else if m.contains("status") || m.contains("http") {
        BatchErrorKind::HttpStatus
    } else if m.contains("json") || m.contains("parse") {
        BatchErrorKind::JsonParse
    } else if m.contains("empty") {
        BatchErrorKind::Empty
    } else if m.contains("io") || m.contains("write") || m.contains("read") {
        BatchErrorKind::Io
    } else {
        BatchErrorKind::Other
    }
}

fn classify_parse_bytes_error(e: &reqwest::Error) -> BatchErrorKind {
    if e.is_timeout() {
        BatchErrorKind::Timeout
    } else if e.is_connect() || e.is_request() {
        BatchErrorKind::Connection
    } else if e.is_decode() || e.is_status() {
        BatchErrorKind::JsonParse
    } else {
        BatchErrorKind::Io
    }
}

async fn do_request(client: &Client, url: &str, api_key: &Option<String>, body: Vec<u8>) -> Result<Vec<u8>, reqwest::Error> {
    let mut req = client.post(url).header("content-type", "application/json");
    if let Some(k) = api_key {
        req = req.header("x-api-key", k);
    }
    let resp = req.body(body).send().await?;
    let bytes = resp.bytes().await?;
    Ok(bytes.to_vec())
}

fn parse_result_count(buf: &[u8], count: usize) -> (usize, usize) {
    match simd_json::from_slice::<OwnedValue>(&mut buf.to_vec()) {
        Ok(v) => {
            if let Some(arr) = v.as_array() {
                let (o, e) = arr.iter().fold((0usize, 0usize), |(o, e), item| {
                    if item.get("result").is_some() { (o + 1, e) } else { (o, e + 1) }
                });
                return (o, e);
            }
            (0, count)
        }
        Err(_) => {
            match serde_json::from_slice::<Value>(&buf) {
                Ok(v) => {
                    if let Some(arr) = v.as_array() {
                        let (o, e) = arr.iter().fold((0usize, 0usize), |(o, e), item| {
                            if item.get("result").is_some() { (o + 1, e) } else { (o, e + 1) }
                        });
                        return (o, e);
                    }
                    (0, count)
                }
                Err(e) => {
                    eprintln!("  batch parse error: {e}");
                    (0, count)
                }
            }
        }
    }
}

async fn get_first_block(client: &Client, url: &str, api_key: &Option<String>) -> Option<u64> {
    let body = br#"{"jsonrpc":"2.0","id":1,"method":"getFirstAvailableBlock","params":[]}"#.to_vec();
    match do_request(client, url, api_key, body).await {
        Ok(buf) => {
            let v: Value = serde_json::from_slice(&buf).ok()?;
            v.get("result")?.as_u64()
        }
        Err(e) => {
            eprintln!("  first block request failed: {e}");
            None
        }
    }
}

async fn get_slot(client: &Client, url: &str, api_key: &Option<String>) -> Option<u64> {
    let body = br#"{"jsonrpc":"2.0","id":1,"method":"getSlot","params":[]}"#.to_vec();
    match do_request(client, url, api_key, body).await {
        Ok(buf) => {
            let v: Value = serde_json::from_slice(&buf).ok()?;
            v.get("result")?.as_u64()
        }
        Err(e) => {
            eprintln!("  getSlot request failed: {e}");
            None
        }
    }
}

async fn test_batch(
    client: &Client,
    url: &str,
    api_key: &Option<String>,
    slot: u64,
    count: usize,
    full_tx: bool,
) -> BatchResult {
    let mut batch = Vec::with_capacity(count);
    for i in 0..count {
        let b = slot - i as u64;
        let params = if full_tx {
            json!([b, {"encoding": "json", "transactionDetails": "full", "maxSupportedTransactionVersion": 0, "rewards": false}])
        } else {
            json!([b, {"encoding": "json", "transactionDetails": "none", "rewards": false}])
        };
        batch.push(json!({"jsonrpc":"2.0","id":i,"method":"getBlock","params":params}));
    }
    let body = match serde_json::to_vec(&batch) {
        Ok(b) => b,
        Err(e) => {
            return BatchResult {
                ok: 0,
                err: count,
                bytes: 0,
                elapsed: Duration::ZERO,
                error_kind: Some(BatchErrorKind::Io),
            };
        }
    };

    let start = Instant::now();
    match do_request(client, url, api_key, body).await {
        Ok(buf) => {
            let elapsed = start.elapsed();
            let size = buf.len();

            if buf.is_empty() {
                eprintln!("  batch: empty response");
                return BatchResult {
                    ok: 0,
                    err: count,
                    bytes: 0,
                    elapsed,
                    error_kind: Some(BatchErrorKind::Empty),
                };
            }

            let (ok, err) = parse_result_count(&buf, count);

            if err > 0 {
                let text = unsafe { std::str::from_utf8_unchecked(&buf) };
                let sample = if text.len() > 500 { &text[..500] } else { text };
                eprintln!("  batch errors: {} (body head: {})", err, sample);
            }

            BatchResult {
                ok,
                err,
                bytes: size,
                elapsed,
                error_kind: if err > 0 { Some(BatchErrorKind::JsonParse) } else { None },
            }
        }
        Err(e) => {
            let kind = classify_parse_bytes_error(e);
            eprintln!("  batch request failed: {} [{}]", e, kind);
            BatchResult {
                ok: 0,
                err: count,
                bytes: 0,
                elapsed: start.elapsed(),
                error_kind: Some(kind),
            }
        }
    }
}

async fn run_batch_test(client: Client, url: String, api_key: Option<String>) {
    let fb = get_first_block(&client, &url, &api_key).await;
    println!("first available block: {}", fb.unwrap_or_default());

    let slot = get_slot(&client, &url, &api_key).await.unwrap_or(0);
    println!("{slot}\n");

    let args: Vec<String> = env::args().collect();
    let batch_size = if args.len() > 3 {
        args[3].parse().unwrap_or(100)
    } else {
        100
    };
    let max_conc = if args.len() > 4 {
        args[4].parse().unwrap_or(20)
    } else {
        20
    };

    println!("=== {}x batch concurrency sweep up to {} ===", batch_size, max_conc);

    let mut concurrency_levels = Vec::new();
    let mut c = 1;
    while c <= max_conc {
        concurrency_levels.push(c);
        c *= 2;
    }
    if !concurrency_levels.contains(&max_conc) {
        concurrency_levels.push(max_conc);
    }

    for &conc in &concurrency_levels {
        let start = Instant::now();

        let mut handles: Vec<JoinHandle<BatchResult>> = Vec::with_capacity(conc);
        for job in 0..conc {
            let c = client.clone();
            let url = url.clone();
            let api_key = api_key.clone();
            handles.push(tokio::spawn(async move {
                test_batch(&c, &url, &api_key, slot - job as u64, batch_size, true).await
            }));
        }

        let mut results = Vec::with_capacity(conc);
        for h in handles {
            results.push(h.await);
        }

        let ok = Instant::now();

        let elapsed = ok.elapsed();
        let mut total_ok = 0;
        let mut total_err = 0;
        let mut total_bytes: u64 = 0;
        let mut err_kinds: [usize; 7] = [0; 7];
        let mut worst_elapsed = Duration::ZERO;

        for r in results {
            let r = match r {
                Ok(r) => r,
                Err(e) => {
                    eprintln!("  task join error: {e}");
                    continue;
                }
            };
            total_ok += r.ok;
            total_err += r.err;
            total_bytes += r.bytes as u64;
            if r.elapsed > worst_elapsed {
                worst_elapsed = r.elapsed;
            }
            if let Some(kind) = r.error_kind {
                let idx = match kind {
                    BatchErrorKind::Timeout => 0,
                    BatchErrorKind::Connection => 1,
                    BatchErrorKind::HttpStatus => 2,
                    BatchErrorKind::JsonParse => 3,
                    BatchErrorKind::Empty => 4,
                    BatchErrorKind::Io => 5,
                    BatchErrorKind::Other => 6,
                };
                err_kinds[idx] += 1;
            }
        }

        let total_blocks = total_ok + total_err;
        let blk_per_sec = if elapsed.as_secs_f64() > 0.0 {
            total_blocks as f64 / elapsed.as_secs_f64()
        } else {
            0.0
        };
        let mb_per_sec = if elapsed.as_secs_f64() > 0.0 {
            (total_bytes as f64 / 1024.0 / 1024.0) / elapsed.as_secs_f64()
        } else {
            0.0
        };

        println!(
            "  concurrency {:>3}: {:>3} ok, {:>3} err, {:>6.1} blk/s, {:.2}s, {:.1} MB/s",
            conc, total_ok, total_err, blk_per_sec, elapsed.as_secs_f64(), mb_per_sec,
        );
        if total_err > 0 {
            let kinds = vec![
                ("timeout", err_kinds[0]),
                ("conn", err_kinds[1]),
                ("http", err_kinds[2]),
                ("json", err_kinds[3]),
                ("empty", err_kinds[4]),
                ("io", err_kinds[5]),
                ("other", err_kinds[6]),
            ]
            .into_iter()
            .filter(|(_, c)| *c > 0)
            .map(|(k, c)| format!("{}={}", k, c))
            .collect::<Vec<_>>()
            .join(" ");
            println!("    errors: {}", kinds);
            println!("    worst thread: {:.2}s", worst_elapsed.as_secs_f64());
        }
    }
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 3 {
        eprintln!(
            "usage: {} batchtest <url> [api_key] [batch_size=100] [max_concurrency=20]",
            args[0]
        );
        std::process::exit(1);
    }
    let mode = &args[1];
    let url = args[2].clone();
    let api_key = if args.len() > 3 { Some(args[3].clone()) } else { None };

    match mode.as_str() {
        "batchtest" => {
            println!("batch test: {url}");
            let client = ClientBuilder::new()
                .user_agent("block-bench/1.0")
                .timeout(Duration::from_secs(60))
                .build()
                .expect("build reqwest client");
            run_batch_test(client, url, api_key).await;
        }
        _ => {
            eprintln!("unknown mode: {mode}");
            std::process::exit(1);
        }
    }
}