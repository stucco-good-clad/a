use base64::Engine;
use futures::stream::{self, StreamExt};
use serde_json::{json, Value};
use solana_sdk::transaction::VersionedTransaction;
use std::collections::HashMap;

use std::sync::Arc;

const DEX_PROGRAMS: &[&str] = &[
    "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8",
    "CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK",
    "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4",
    "JUP4Fb2cqiRUcaTHdrPC8h2gNsA2ETXiPDD33WcGuJB",
    "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc",
    "LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo",
    "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P",
    "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA",
];

const SOL_MINT: &str = "So11111111111111111111111111111111111111112";
const USDC_MINT: &str = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v";
const USDT_MINT: &str = "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB";

fn bs58_encode(bytes: &[u8]) -> String {
    bs58::encode(bytes).into_string()
}

fn is_dex_program(key: &str) -> bool {
    DEX_PROGRAMS.iter().any(|p| *p == key)
}

#[derive(Debug, Default)]
struct SlotOhlcv {
    block_time: Option<i64>,
    volume: f64,
    buy_volume: f64,
    sell_volume: f64,
    num_trades: usize,
    sol_usd_prices: Vec<f64>,
}

fn process_rpc_block(slot: u64, block: &Value) -> Vec<(u64, Option<i64>, f64, f64, bool)> {
    let block_time = block.get("blockTime").and_then(|v| v.as_i64());
    let transactions = match block.get("transactions").and_then(|v| v.as_array()) {
        Some(t) => t,
        None => return Vec::new(),
    };

    let mut trades = Vec::new();

    for tx in transactions {
        let meta = match tx.get("meta") {
            Some(m) => m,
            None => continue,
        };

        if meta.get("err").is_some() && !meta.get("err").unwrap().is_null() {
            continue;
        }

        let tx_data = match tx.get("transaction").and_then(|v| v.as_array()) {
            Some(d) => d,
            None => continue,
        };

        if tx_data.is_empty() {
            continue;
        }

        let msg_b64 = match tx_data[0].as_str() {
            Some(s) => s,
            None => continue,
        };
        let msg_bytes = match base64::engine::general_purpose::STANDARD.decode(msg_b64) {
            Ok(b) => b,
            Err(_) => continue,
        };

        let num_sigs = tx_data.len() - 1;
        let mut raw_tx = Vec::with_capacity(8 + num_sigs * 64 + msg_bytes.len());
        raw_tx.extend_from_slice(&(num_sigs as u64).to_le_bytes());
        for i in 1..tx_data.len() {
            if let Some(sig_b64) = tx_data[i].as_str() {
                if let Ok(sig) = base64::engine::general_purpose::STANDARD.decode(sig_b64) {
                    raw_tx.extend_from_slice(&sig);
                }
            }
        }
        raw_tx.extend_from_slice(&msg_bytes);

        let versioned_tx = match bincode::deserialize::<VersionedTransaction>(&raw_tx) {
            Ok(v) => v,
            Err(_) => continue,
        };

        use solana_sdk::message::VersionedMessage;
        let account_keys: Vec<String> = match &versioned_tx.message {
            VersionedMessage::Legacy(m) => {
                m.account_keys.iter().map(|k| bs58_encode(&k.to_bytes())).collect()
            }
            VersionedMessage::V0(m) => {
                m.account_keys.iter().map(|k| bs58_encode(&k.to_bytes())).collect()
            }
        };

        let outer_dex = match &versioned_tx.message {
            VersionedMessage::Legacy(m) => m.instructions.iter().any(|i| {
                account_keys
                    .get(i.program_id_index as usize)
                    .map(|k| is_dex_program(k))
                    .unwrap_or(false)
            }),
            VersionedMessage::V0(m) => m.instructions.iter().any(|i| {
                account_keys
                    .get(i.program_id_index as usize)
                    .map(|k| is_dex_program(k))
                    .unwrap_or(false)
            }),
        };
        if !outer_dex {
            continue;
        }

        let signer = match account_keys.first() {
            Some(k) => k.clone(),
            None => continue,
        };

        let fee = meta.get("fee").and_then(|v| v.as_u64()).unwrap_or(0);
        let pre_balances = meta.get("preBalances").and_then(|v| v.as_array());
        let post_balances = meta.get("postBalances").and_then(|v| v.as_array());

        let pre_sol = pre_balances
            .and_then(|b| b.first())
            .and_then(|v| v.as_u64())
            .unwrap_or(0);
        let post_sol = post_balances
            .and_then(|b| b.first())
            .and_then(|v| v.as_u64())
            .unwrap_or(0);
        let native_sol_change: i128 = post_sol as i128 - pre_sol as i128;

        let pre_token = meta.get("preTokenBalances").and_then(|v| v.as_array());
        let post_token = meta.get("postTokenBalances").and_then(|v| v.as_array());

        let mut wsol_change: i128 = 0;
        let mut usdc_change: i128 = 0;
        let mut usdt_change: i128 = 0;

        if let Some(post_balances) = post_token {
            for post_bal in post_balances {
                let owner = post_bal.get("owner").and_then(|v| v.as_str()).unwrap_or("");
                if owner != signer {
                    continue;
                }
                let account_index = post_bal.get("accountIndex").and_then(|v| v.as_u64()).unwrap_or(0);
                let mint = post_bal.get("mint").and_then(|v| v.as_str()).unwrap_or("");

                let post_amount: i128 = post_bal
                    .get("uiTokenAmount")
                    .and_then(|u| u.get("amount"))
                    .and_then(|v| {
                        v.as_str().and_then(|s| s.parse().ok())
                            .or_else(|| v.as_u64().map(|n| n as i128))
                    })
                    .unwrap_or(0);

                let pre_amount: i128 = pre_token
                    .and_then(|pre| {
                        pre.iter().find(|p| {
                            p.get("accountIndex").and_then(|v| v.as_u64()) == Some(account_index)
                        })
                    })
                    .and_then(|p| {
                        p.get("uiTokenAmount")
                            .and_then(|u| u.get("amount"))
                            .and_then(|v| {
                                v.as_str().and_then(|s| s.parse().ok())
                                    .or_else(|| v.as_u64().map(|n| n as i128))
                            })
                    })
                    .unwrap_or(0);

                let change = post_amount - pre_amount;
                if change == 0 {
                    continue;
                }
                match mint {
                    SOL_MINT => wsol_change += change,
                    USDC_MINT => usdc_change += change,
                    USDT_MINT => usdt_change += change,
                    _ => {}
                }
            }
        }

        let stablecoin_change = usdc_change + usdt_change;
        let total_sol_spent: i128 = -(native_sol_change + fee as i128) - wsol_change;

        if total_sol_spent != 0 && stablecoin_change != 0 {
            let sol_amount = (total_sol_spent.unsigned_abs() as f64) / 1e9;
            let usd_amount = (stablecoin_change.unsigned_abs() as f64) / 1e6;
            let sol_price = usd_amount / sol_amount;
            let is_buy = total_sol_spent < 0;

            if sol_price > 0.0 && sol_amount > 0.0 && sol_price < 10000.0 {
                trades.push((slot, block_time, sol_price, sol_amount, is_buy));
            }
        }
    }
    trades
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let rpc_url = std::env::var("FAITHFUL_ENDPOINT")
        .unwrap_or_else(|_| "http://127.0.0.1:8888".to_string());
    let start_slot: u64 = std::env::var("START_SLOT")
        .unwrap_or_else(|_| "345600000".to_string())
        .parse()?;
    let end_slot: u64 = std::env::var("END_SLOT")
        .unwrap_or_else(|_| "346032000".to_string())
        .parse()?;
    let output_dir = std::env::var("OUTPUT_DIR").unwrap_or_else(|_| "./output".to_string());
    let concurrency: usize = std::env::var("CONCURRENCY")
        .unwrap_or_else(|_| "50".to_string())
        .parse()?;

    std::fs::create_dir_all(&output_dir)?;

    let http = reqwest::Client::builder()
        .pool_max_idle_per_host(concurrency)
        .timeout(std::time::Duration::from_secs(30))
        .build()?;

    eprintln!(
        "Fetching blocks {}..{} via JSON-RPC GetBlock with {} concurrent requests",
        start_slot, end_slot, concurrency
    );

    let total_slots = end_slot - start_slot;
    let mut slot_ohlcv: HashMap<u64, SlotOhlcv> = HashMap::new();
    let mut processed = 0u64;
    let mut found_blocks = 0u64;
    let mut found_txns = 0u64;
    let mut found_trades = 0u64;
    let mut not_found = 0u64;
    let mut meta_errs = 0u64;
    let mut has_token_balances = 0u64;
    let mut no_token_balances = 0u64;
    let started = std::time::Instant::now();

    let rpc_url = Arc::new(rpc_url);
    let http = Arc::new(http);

    let slots: Vec<u64> = (start_slot..end_slot).collect();

    let mut results = stream::iter(slots)
        .map(|slot| {
            let url = rpc_url.clone();
            let client = http.clone();
            async move {
                let body = json!({
                    "jsonrpc": "2.0",
                    "id": slot,
                    "method": "getBlock",
                    "params": [
                        slot,
                        {
                            "encoding": "base64",
                            "transactionDetails": "full",
                            "maxSupportedTransactionVersion": 0,
                            "rewards": false
                        }
                    ]
                });

                let resp = client.post(url.as_ref()).json(&body).send().await;
                (slot, resp)
            }
        })
        .buffer_unordered(concurrency);

    while let Some((slot, resp)) = results.next().await {
        processed += 1;

        match resp {
            Ok(r) => {
                let json: Value = r.json().await.unwrap_or(Value::Null);
                if let Some(error) = json.get("error") {
                    not_found += 1;
                    let _ = error;
                } else if let Some(result) = json.get("result") {
                    if result.is_null() {
                        not_found += 1;
                    } else {
                        found_blocks += 1;
                        let trades = process_rpc_block(slot, result);
                        let tx_count = result
                            .get("transactions")
                            .and_then(|v| v.as_array())
                            .map(|a| a.len() as u64)
                            .unwrap_or(0);
                        found_txns += tx_count;
                        found_trades += trades.len() as u64;
                        for (s, bt, price, sol_amount, is_buy) in &trades {
                            let entry = slot_ohlcv.entry(*s).or_insert_with(|| SlotOhlcv {
                                block_time: *bt,
                                ..Default::default()
                            });
                            entry.sol_usd_prices.push(*price);
                            entry.volume += sol_amount;
                            entry.num_trades += 1;
                            if *is_buy {
                                entry.buy_volume += sol_amount;
                            } else {
                                entry.sell_volume += sol_amount;
                            }
                        }
                    }
                } else {
                    meta_errs += 1;
                }
            }
            Err(_) => {
                not_found += 1;
            }
        }

        if processed % 1000 == 0 || processed == total_slots {
            let elapsed = started.elapsed().as_secs_f64();
            eprintln!(
                "[rpc] {}/{} ({:.0}%) blocks={} txns={} trades={} ohlcv={} not_found={} slots/s={:.0} elapsed={:.0}s",
                processed, total_slots, processed as f64 / total_slots as f64 * 100.0,
                found_blocks, found_txns, found_trades, slot_ohlcv.len(), not_found,
                processed as f64 / elapsed, elapsed
            );
        }
    }

    let elapsed = started.elapsed().as_secs_f64();
    eprintln!(
        "Done. slots={} blocks={} txns={} trades={} ohlcv={} not_found={} err={} slots/s={:.0} elapsed={:.0}s",
        total_slots, found_blocks, found_txns, found_trades, slot_ohlcv.len(),
        not_found, meta_errs, total_slots as f64 / elapsed, elapsed
    );

    let mut csv = vec![
        "slot,block_time,open,high,low,close,volume,num_trades,buy_volume,sell_volume".to_string(),
    ];
    let mut out_slots: Vec<u64> = slot_ohlcv.keys().copied().collect();
    out_slots.sort();
    for slot in &out_slots {
        let data = &slot_ohlcv[slot];
        if data.sol_usd_prices.is_empty() {
            continue;
        }
        let open = data.sol_usd_prices[0];
        let close = data.sol_usd_prices[data.sol_usd_prices.len() - 1];
        let high = data
            .sol_usd_prices
            .iter()
            .cloned()
            .fold(f64::NEG_INFINITY, f64::max);
        let low = data
            .sol_usd_prices
            .iter()
            .cloned()
            .fold(f64::INFINITY, f64::min);
        csv.push(format!(
            "{},{},{},{},{},{},{},{},{},{}",
            slot,
            data.block_time.unwrap_or(0),
            open,
            high,
            low,
            close,
            data.volume,
            data.num_trades,
            data.buy_volume,
            data.sell_volume
        ));
    }
    let csv_path = format!("{}/ohlcv.csv", output_dir);
    std::fs::write(&csv_path, csv.join("\n"))?;
    eprintln!("Written {} rows to {}", csv.len() - 1, csv_path);
    Ok(())
}
