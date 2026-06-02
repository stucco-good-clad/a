use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::Path;

use serde_json::Value;

const ORCA_WHIRLPOOL_PROGRAM: &str = "whirLb6i6ZP8EhUpgqu6eEt9rKkWxUNh1cUN8eCq9mB";
const POOL_ADDRESS: &str = "Czfq3xZZDmsdGdUyrNLtRhGc47cXcZtLG4crryfu44zE";

fn main() {
    let raw_dir = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "raw".to_string());
    let out_csv = std::env::args()
        .nth(2)
        .unwrap_or_else(|| "ohlcv.csv".to_string());

    let mut files: Vec<_> = fs::read_dir(&raw_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().map(|x| x == "txt").unwrap_or(false))
        .map(|e| e.path())
        .collect();
    files.sort();

    let mut slot_rows: Vec<SlotRow> = Vec::new();

    for path in files {
        let data = fs::read_to_string(&path).unwrap_or_default();
        let v: Value = serde_json::from_str(&data).unwrap_or(Value::Null);
        let slot = path.file_stem().and_then(|s| s.to_str()).and_then(|s| s.parse().ok());
        let block_time = v.get("blockTime").and_then(|x| x.as_u64());
        if v.is_null() || slot.is_none() || block_time.is_none() {
            continue;
        }
        let slot = slot.unwrap();
        let block_time = block_time.unwrap();

        let mut trades: Vec<Trade> = Vec::new();
        let txs = v.get("transactions").and_then(|x| x.as_array()).unwrap_or(&[]);

        for tx in txs {
            let msg = tx.get("transaction").and_then(|x| x.get("message"));
            let account_keys = msg
                .and_then(|x| x.get("accountKeys"))
                .and_then(|x| x.as_array())
                .map(|arr| {
                    arr.iter()
                        .filter_map(|k| k.as_str().map(String::from))
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();

            let pos = account_keys
                .iter()
                .position(|k| k == ORCA_WHIRLPOOL_PROGRAM);
            if pos.is_none() {
                continue;
            }

            let pre = tx.get("meta").and_then(|m| m.get("preTokenBalances")).and_then(|x| x.as_array()).unwrap_or(&[]);
            let post = tx.get("meta").and_then(|m| m.get("postTokenBalances")).and_then(|x| x.as_array()).unwrap_or(&[]);

            let mut sol_pre: u64 = 0;
            let mut sol_post: u64 = 0;
            let mut usdc_pre: u64 = 0;
            let mut usdc_post: u64 = 0;
            let mut usdc_mint = false;
            let mut usdc_index: usize = 0;

            for pb in pre {
                let account_index = pb.get("accountIndex").and_then(|x| x.as_u64()).unwrap_or(0) as usize;
                if account_index >= account_keys.len() {
                    continue;
                }
                let mint = pb.get("mint").and_then(|x| x.as_str()).unwrap_or("");
                let ui = pb.get("uiTokenAmount").and_then(|x| x.as_object());
                let amount = ui.and_then(|u| u.get("amount")).and_then(|x| x.as_str()).and_then(|s| s.parse().ok()).unwrap_or(0);
                if mint == "So11111111111111111111111111111111111111112" {
                    sol_pre = amount;
                } else if mint == "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v" {
                    usdc_pre = amount;
                    usdc_mint = true;
                    usdc_index = account_index;
                }
            }

            for pb in post {
                let account_index = pb.get("accountIndex").and_then(|x| x.as_u64()).unwrap_or(0) as usize;
                if account_index >= account_keys.len() {
                    continue;
                }
                let mint = pb.get("mint").and_then(|x| x.as_str()).unwrap_or("");
                let ui = pb.get("uiTokenAmount").and_then(|x| x.as_object());
                let amount = ui.and_then(|u| u.get("amount")).and_then(|x| x.as_str()).and_then(|s| s.parse().ok()).unwrap_or(0);
                if mint == "So11111111111111111111111111111111111111112" {
                    sol_post = amount;
                } else if mint == "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v" {
                    usdc_post = amount;
                }
            }

            if !usdc_mint {
                continue;
            }

            let sol_delta = if sol_post > sol_pre { sol_post - sol_pre } else { sol_pre - sol_post };
            let usdc_delta = if usdc_post > usdc_pre { usdc_post - usdc_pre } else { usdc_pre - usdc_post };

            if sol_delta == 0 || usdc_delta == 0 {
                continue;
            }

            let price = usdc_delta as f64 / sol_delta as f64;
            let amount = sol_delta as f64;
            trades.push(Trade { price, amount });
        }

        if trades.is_empty() {
            continue;
        }

        let open = trades[0].price;
        let close = trades[trades.len() - 1].price;
        let high = trades.iter().map(|t| t.price).fold(open, f64::max);
        let low = trades.iter().map(|t| t.price).fold(open, f64::min);
        let volume: f64 = trades.iter().map(|t| t.amount).sum();

        slot_rows.push(SlotRow {
            slot,
            block_time,
            open,
            high,
            low,
            close,
            volume,
        });
    }

    let mut wtr = csv::WriterBuilder::new().has_headers(true).from_path(&out_csv).unwrap();
    let _ = wtr.write_record(&["slot", "blockTime", "open", "high", "low", "close", "volume"]);
    for row in &slot_rows {
        let _ = wtr.write_record(&[
            row.slot.to_string(),
            row.block_time.to_string(),
            format!("{:.9}", row.open),
            format!("{:.9}", row.high),
            format!("{:.9}", row.low),
            format!("{:.9}", row.close),
            format!("{:.9}", row.volume),
        ]);
    }
    let _ = wtr.flush();
    println!("Wrote {} rows to {}", slot_rows.len(), out_csv);
}

struct Trade {
    price: f64,
    amount: f64,
}

struct SlotRow {
    slot: u64,
    block_time: u64,
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    volume: f64,
}
