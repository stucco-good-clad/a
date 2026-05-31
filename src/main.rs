use std::env;
use std::fmt;
use std::fs::File;
use std::io::copy;
use std::sync::Arc;
use std::time::{Duration, Instant};
use rayon::iter::IntoParallelIterator;
use rayon::prelude::*;
use rayon::ThreadPoolBuilder;
use serde_json::json;
use ureq::{Agent, AgentBuilder};

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

fn read_response(r: ureq::Response, label: &str) -> Option<String> {
    match r.into_string() {
        Ok(s) => Some(s),
        Err(e) => {
            eprintln!("  {}: body read error: {}", label, e);
            None
        }
    }
}

fn get_first_block(agent: &Agent, url: &str, api_key: &Option<String>) -> Option<u64> {
    let body = br#"{"jsonrpc":"2.0","id":1,"method":"getFirstAvailableBlock","params":[]}"#;
    let mut req = agent.post(url).set("Content-Type", "application/json");
    if let Some(k) = api_key {
        req = req.set("x-api-key", k);
    }
    match req.send_bytes(body) {
        Ok(r) => read_response(r, "first_block").and_then(|text| {
            serde_json::from_str::<serde_json::Value>(&text)
                .ok()
                .and_then(|v| v["result"].as_u64())
        }),
        Err(e) => {
            eprintln!("  first block request failed: {e}");
            None
        }
    }
}

fn get_slot(agent: &Agent, url: &str, api_key: &Option<String>) -> Option<u64> {
    let body = br#"{"jsonrpc":"2.0","id":1,"method":"getSlot","params":[]}"#;
    let mut req = agent.post(url).set("Content-Type", "application/json");
    if let Some(k) = api_key {
        req = req.set("x-api-key", k);
    }
    match req.send_bytes(body) {
        Ok(r) => read_response(r, "getSlot").and_then(|text| {
            serde_json::from_str::<serde_json::Value>(&text)
                .ok()
                .and_then(|v| v["result"].as_u64())
        }),
        Err(e) => {
            eprintln!("  getSlot request failed: {e}");
            None
        }
    }
}

fn write_response_to_file(r: ureq::Response, path: &str) -> std::io::Result<u64> {
    let mut f = File::create(path)?;
    let mut reader = r.into_reader();
    let size = copy(&mut reader, &mut f)?;
    Ok(size)
}

fn test_batch(
    agent: &Agent,
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
    let body = serde_json::to_string(&batch).unwrap();

    let mut req = agent.post(url).set("Content-Type", "application/json");
    if let Some(k) = api_key {
        req = req.set("x-api-key", k);
    }

    let start = Instant::now();
    let tmp_path = format!("/tmp/batch_{}_{}.json", slot, count);

    match req.send_bytes(body.as_bytes()) {
        Ok(r) => {
            let write_size = match write_response_to_file(r, &tmp_path) {
                Ok(s) => s,
                Err(e) => {
                    eprintln!("  batch: file write error: {}", e);
                    return BatchResult {
                        ok: 0,
                        err: count,
                        bytes: 0,
                        elapsed: start.elapsed(),
                        error_kind: Some(BatchErrorKind::Io),
                    };
                }
            };
            let elapsed = start.elapsed();

            let text = match std::fs::read_to_string(&tmp_path) {
                Ok(s) => s,
                Err(e) => {
                    eprintln!("  batch: file read error: {}", e);
                    return BatchResult {
                        ok: 0,
                        err: count,
                        bytes: 0,
                        elapsed,
                        error_kind: Some(BatchErrorKind::Io),
                    };
                }
            };

            let size = text.len();
            if text.is_empty() {
                eprintln!("  batch: empty response from {} bytes", write_size);
                return BatchResult {
                    ok: 0,
                    err: count,
                    bytes: 0,
                    elapsed,
                    error_kind: Some(BatchErrorKind::Empty),
                };
            }
            match serde_json::from_str::<serde_json::Value>(&text) {
                Ok(v) => {
                    if let Some(arr) = v.as_array() {
                        let (ok, err) = arr.iter().fold((0, 0), |(o, e), item| {
                            if item.get("result").is_some() {
                                (o + 1, e)
                            } else {
                                (o, e + 1)
                            }
                        });
                        if err > 0 {
                            let first_err = arr
                                .iter()
                                .find(|item| item.get("result").is_none())
                                .and_then(|item| item.get("error"))
                                .and_then(|e| e.get("message"))
                                .and_then(|m| m.as_str())
                                .unwrap_or("unknown");
                            eprintln!("  batch errors: {} (sample: {})", err, first_err);
                        }
                        return BatchResult {
                            ok,
                            err,
                            bytes: size,
                            elapsed,
                            error_kind: if err > 0 {
                                Some(classify_error("error in response"))
                            } else {
                                None
                            },
                        };
                    }
                    eprintln!("  batch: response not an array (wrote {} bytes)", size);
                    BatchResult {
                        ok: 0,
                        err: count,
                        bytes: size,
                        elapsed,
                        error_kind: Some(BatchErrorKind::JsonParse),
                    }
                }
                Err(e) => {
                    eprintln!("  batch JSON parse error: {}", e);
                    eprintln!(
                        "  response head (first 500 chars):\n{}",
                        &text[..text.len().min(500)]
                    );
                    BatchResult {
                        ok: 0,
                        err: count,
                        bytes: size,
                        elapsed,
                        error_kind: Some(BatchErrorKind::JsonParse),
                    }
                }
            }
        }
        Err(e) => {
            let kind = classify_error(&e.to_string());
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

fn run_batch_test(url: &str, api_key: &Option<String>) {
    let pool = ThreadPoolBuilder::new()
        .num_threads(4)
        .build()
        .expect("build thread pool");

    let agent = Arc::new(
        AgentBuilder::new()
            .timeout_connect(Duration::from_secs(10))
            .timeout_read(Duration::from_secs(300))
            .timeout_write(Duration::from_secs(10))
            .build(),
    );

    print!("first available block: ");
    let fb = get_first_block(&agent, url, api_key);
    println!("{}", fb.unwrap_or_default());

    print!("current slot: ");
    let slot = get_slot(&agent, url, api_key).unwrap_or(0);
    println!("{slot}\n");

    println!("=== 50x batch concurrency sweep ===");
    let concurrency_levels = [1, 2, 5, 10, 20];

    for &conc in &concurrency_levels {
        let start = Instant::now();

        let results: Vec<BatchResult> = pool.install(|| {
            (0..conc)
                .into_par_iter()
                .map(|c| {
                    let agent = agent.clone();
                    let url = url.to_string();
                    let api_key = api_key.clone();
                    test_batch(&agent, &url, &api_key, slot - c as u64, 50, true)
                })
                .collect()
        });

        let elapsed = start.elapsed();
        let mut total_ok = 0;
        let mut total_err = 0;
        let mut total_bytes: u64 = 0;
        let mut err_kinds: [usize; 7] = [0; 7];
        let mut worst_elapsed = Duration::ZERO;

        for r in results {
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

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 3 {
        eprintln!("usage: {} batchtest <url> [api_key]", args[0]);
        std::process::exit(1);
    }
    let mode = &args[1];
    let url = &args[2];
    let api_key = if args.len() > 3 {
        Some(args[3].clone())
    } else {
        None
    };

    match mode.as_str() {
        "batchtest" => {
            println!("batch test: {url}");
            run_batch_test(url, &api_key);
        }
        _ => {
            eprintln!("unknown mode: {mode}");
            std::process::exit(1);
        }
    }
}
