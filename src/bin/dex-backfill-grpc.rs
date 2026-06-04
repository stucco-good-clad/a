use futures::stream::StreamExt;
use prost::Message as ProstMessage;
use solana_sdk::transaction::VersionedTransaction;
use std::collections::HashMap;
use tonic::transport::Endpoint;

pub mod old_faithful {
    tonic::include_proto!("old_faithful");
}

pub mod solana_meta {
    include!(concat!(env!("OUT_DIR"), "/solana_storage.rs"));
}

use old_faithful::old_faithful_client::OldFaithfulClient;
use old_faithful::StreamBlocksRequest;
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

    std::fs::create_dir_all(&output_dir)?;

    eprintln!("Connecting to {}", endpoint);
    let channel = Endpoint::from_shared(endpoint)?
        .max_frame_size(16 * 1024 * 1024 - 1)
        .connect()
        .await?;
    let mut client = OldFaithfulClient::new(channel)
        .max_decoding_message_size(64 * 1024 * 1024);

    eprintln!("Streaming blocks {}..{} (no filter, DEX via meta balance changes)", start_slot, end_slot);

    let response = client
        .stream_blocks(StreamBlocksRequest {
            start_slot,
            end_slot,
            filter: None,
        })
        .await?
        .into_inner();

    let mut slot_ohlcv: HashMap<u64, SlotOhlcv> = HashMap::new();
    let mut total_blocks = 0u64;
    let mut total_tx = 0u64;
    let mut total_trades = 0u64;
    let mut meta_decoded = 0u64;
    let mut meta_errors = 0u64;
    let mut dex_hit = 0u64;
    let mut parse_errors = 0u64;
    let mut grpc_errors = 0u64;
    let started = std::time::Instant::now();
    let mut last_report = std::time::Instant::now();

    let mut stream = response;
    while let Some(result) = stream.next().await {
        let block = match result {
            Ok(b) => b,
            Err(e) => {
                grpc_errors += 1;
                if grpc_errors <= 10 {
                    eprintln!("[grpc] block error: {}", e);
                }
                continue;
            }
        };
        let slot = block.slot;
        let block_time = if block.block_time != 0 {
            Some(block.block_time)
        } else {
            None
        };

        for tx in &block.transactions {
            total_tx += 1;

            let versioned_tx = match bincode::deserialize::<VersionedTransaction>(&tx.transaction) {
                Ok(v) => v,
                Err(e) => {
                    parse_errors += 1;
                    if parse_errors <= 5 {
                        eprintln!("[grpc] slot={} bincode error: {}", slot, e);
                    }
                    continue;
                }
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
                    account_keys.get(i.program_id_index as usize)
                        .map(|k| DEX_PROGRAMS.iter().any(|p| *p == k.as_str()))
                        .unwrap_or(false)
                }),
                VersionedMessage::V0(m) => m.instructions.iter().any(|i| {
                    account_keys.get(i.program_id_index as usize)
                        .map(|k| DEX_PROGRAMS.iter().any(|p| *p == k.as_str()))
                        .unwrap_or(false)
                }),
            };
            if !outer_dex {
                continue;
            }
            dex_hit += 1;

            if tx.meta.is_empty() {
                continue;
            }
            let meta = match TransactionStatusMeta::decode(&tx.meta[..]) {
                Ok(m) => {
                    meta_decoded += 1;
                    m
                }
                Err(e) => {
                    meta_errors += 1;
                    if meta_errors <= 5 {
                        eprintln!("[grpc] slot={} meta decode error: {}", slot, e);
                    }
                    continue;
                }
            };

            if meta.err.is_some() {
                continue;
            }

            let signer = match account_keys.first() {
                Some(k) => k.clone(),
                None => continue,
            };

            let signer_idx = 0usize;

            let fee = meta.fee;
            let pre_sol = meta.pre_balances.get(signer_idx).copied().unwrap_or(0);
            let post_sol = meta.post_balances.get(signer_idx).copied().unwrap_or(0);
            let native_sol_change: i128 = post_sol as i128 - pre_sol as i128;

            let mut wsol_change: i128 = 0;
            let mut usdc_change: i128 = 0;
            let mut usdt_change: i128 = 0;

            for post_bal in &meta.post_token_balances {
                if post_bal.owner != signer {
                    continue;
                }
                let pre_amount: i128 = meta.pre_token_balances.iter()
                    .find(|p| p.account_index == post_bal.account_index)
                    .and_then(|p| p.ui_token_amount.as_ref())
                    .map(|a| a.amount as i128)
                    .unwrap_or(0);
                let post_amount: i128 = post_bal.ui_token_amount.as_ref()
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
                    total_trades += 1;
                    if dex_hit <= 5 {
                        eprintln!("[dex] slot={} TRADE sol={:.4} usd={:.2} price={:.4} buy={}",
                            slot, sol_amount, usd_amount, sol_price, is_buy);
                    }
                    let entry = slot_ohlcv.entry(slot).or_insert_with(|| SlotOhlcv {
                        block_time,
                        ..Default::default()
                    });
                    entry.sol_usd_prices.push(sol_price);
                    entry.volume += sol_amount;
                    entry.num_trades += 1;
                    if is_buy {
                        entry.buy_volume += sol_amount;
                    } else {
                        entry.sell_volume += sol_amount;
                    }
                }
            }
        }

        total_blocks += 1;
        if last_report.elapsed().as_secs() >= 5 {
            let elapsed = started.elapsed().as_secs_f64();
            let bps = total_blocks as f64 / elapsed;
            eprintln!(
                "[grpc] blocks={} txns={} dex_hit={} trades={} ohlcv={} meta_ok={} meta_err={} parse_errs={} grpc_errs={} bps={:.1} elapsed={:.1}s",
                total_blocks, total_tx, dex_hit, total_trades, slot_ohlcv.len(), meta_decoded, meta_errors, parse_errors, grpc_errors, bps, elapsed
            );
            last_report = std::time::Instant::now();
        }
    }

    let elapsed = started.elapsed().as_secs_f64();
    eprintln!(
        "Done. blocks={} txns={} dex_hit={} trades={} meta_ok={} meta_err={} parse_errs={} grpc_errs={} bps={:.1} elapsed={:.1}s",
        total_blocks, total_tx, dex_hit, total_trades, meta_decoded, meta_errors, parse_errors, grpc_errors, total_blocks as f64 / elapsed, elapsed
    );

    eprintln!("Writing OHLCV for {} slots...", slot_ohlcv.len());
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
            slot, data.block_time.unwrap_or(0), open, high, low, close, data.volume, data.num_trades, data.buy_volume,
            data.sell_volume
        ));
    }
    let csv_path = format!("{}/ohlcv.csv", output_dir);
    std::fs::write(&csv_path, csv.join("\n"))?;
    eprintln!("Written {} rows to {}", csv.len() - 1, csv_path);
    Ok(())
}
