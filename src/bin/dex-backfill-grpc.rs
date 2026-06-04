use futures::stream::{self, StreamExt};
use prost::Message as ProstMessage;
use solana_sdk::transaction::VersionedTransaction;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tonic::transport::Endpoint;

pub mod old_faithful {
    tonic::include_proto!("old_faithful");
}

pub mod solana_meta {
    include!(concat!(env!("OUT_DIR"), "/solana_storage.rs"));
}

use old_faithful::old_faithful_client::OldFaithfulClient;
use old_faithful::BlockRequest;
use solana_meta::TransactionStatusMeta;

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

#[derive(Debug, Default)]
struct SlotOhlcv {
    block_time: Option<i64>,
    volume: f64,
    buy_volume: f64,
    sell_volume: f64,
    num_trades: usize,
    sol_usd_prices: Vec<f64>,
}

fn process_block(block: &old_faithful::BlockResponse) -> Vec<(u64, Option<i64>, f64, f64, bool)> {
    let slot = block.slot;
    let block_time = if block.block_time != 0 {
        Some(block.block_time)
    } else {
        None
    };
    let mut trades = Vec::new();

    for tx in &block.transactions {
        let versioned_tx = match bincode::deserialize::<VersionedTransaction>(&tx.transaction) {
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
                    .map(|k| DEX_PROGRAMS.iter().any(|p| *p == k.as_str()))
                    .unwrap_or(false)
            }),
            VersionedMessage::V0(m) => m.instructions.iter().any(|i| {
                account_keys
                    .get(i.program_id_index as usize)
                    .map(|k| DEX_PROGRAMS.iter().any(|p| *p == k.as_str()))
                    .unwrap_or(false)
            }),
        };
        if !outer_dex {
            continue;
        }

        if tx.meta.is_empty() {
            continue;
        }
        let meta = match TransactionStatusMeta::decode(&tx.meta[..]) {
            Ok(m) => m,
            Err(_) => continue,
        };

        if meta.err.is_some() {
            continue;
        }

        let signer = match account_keys.first() {
            Some(k) => k.clone(),
            None => continue,
        };
        let fee = meta.fee;
        let pre_sol = meta.pre_balances.first().copied().unwrap_or(0);
        let post_sol = meta.post_balances.first().copied().unwrap_or(0);
        let native_sol_change: i128 = post_sol as i128 - pre_sol as i128;

        let mut wsol_change: i128 = 0;
        let mut usdc_change: i128 = 0;
        let mut usdt_change: i128 = 0;

        for post_bal in &meta.post_token_balances {
            if post_bal.owner != signer {
                continue;
            }
            let pre_amount: i128 = meta
                .pre_token_balances
                .iter()
                .find(|p| p.account_index == post_bal.account_index)
                .and_then(|p| p.ui_token_amount.as_ref())
                .map(|a| a.amount as i128)
                .unwrap_or(0);
            let post_amount: i128 = post_bal
                .ui_token_amount
                .as_ref()
                .map(|a| a.amount as i128)
                .unwrap_or(0);
            let change = post_amount - pre_amount;
            if change == 0 {
                continue;
            }
            match post_bal.mint.as_str() {
                SOL_MINT => wsol_change += change,
                USDC_MINT => usdc_change += change,
                USDT_MINT => usdt_change += change,
                _ => {}
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
    let endpoint = std::env::var("FAITHFUL_ENDPOINT")
        .unwrap_or_else(|_| "http://127.0.0.1:8889".to_string());
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

    let mut clients = Vec::new();
    for _ in 0..concurrency {
        let ch = Endpoint::from_shared(endpoint.clone())?
            .max_frame_size(16 * 1024 * 1024 - 1)
            .connect()
            .await?;
        clients.push(
            OldFaithfulClient::new(ch).max_decoding_message_size(64 * 1024 * 1024),
        );
    }

    eprintln!(
        "Fetching blocks {}..{} with {} concurrent GetBlock calls",
        start_slot, end_slot, concurrency
    );

    let total_slots = end_slot - start_slot;
    let mut slot_ohlcv: HashMap<u64, SlotOhlcv> = HashMap::new();
    let mut processed = 0u64;
    let mut found_blocks = 0u64;
    let mut found_txns = 0u64;
    let mut found_trades = 0u64;
    let mut not_found = 0u64;
    let started = std::time::Instant::now();

    let slots: Vec<u64> = (start_slot..end_slot).collect();

    let mut results = stream::iter(slots)
        .enumerate()
        .map(|(i, slot)| {
            let client_ref = &clients[i % clients.len()];
            let mut client = client_ref.clone();
            async move {
                let result = client.get_block(BlockRequest { slot }).await;
                (slot, result)
            }
        })
        .buffer_unordered(concurrency);

    while let Some((slot, result)) = results.next().await {
        processed += 1;

        match result {
            Ok(resp) => {
                let block = resp.into_inner();
                if block.transactions.is_empty() {
                    not_found += 1;
                } else {
                    found_blocks += 1;
                    found_txns += block.transactions.len() as u64;
                    let trades = process_block(&block);
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
            }
            Err(_) => {
                not_found += 1;
            }
        }

        if processed % 5000 == 0 || processed == total_slots {
            let elapsed = started.elapsed().as_secs_f64();
            eprintln!(
                "[getblock] {}/{} slots ({:.0}%) blocks={} txns={} trades={} ohlcv={} not_found={} slots_per_s={:.0} elapsed={:.0}s",
                processed, total_slots, processed as f64 / total_slots as f64 * 100.0,
                found_blocks, found_txns, found_trades, slot_ohlcv.len(), not_found,
                processed as f64 / elapsed, elapsed
            );
        }
    }

    let elapsed = started.elapsed().as_secs_f64();
    eprintln!(
        "Done. slots={} blocks={} txns={} trades={} ohlcv={} not_found={} slots_per_s={:.0} elapsed={:.0}s",
        total_slots, found_blocks, found_txns, found_trades, slot_ohlcv.len(),
        not_found, total_slots as f64 / elapsed, elapsed
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
