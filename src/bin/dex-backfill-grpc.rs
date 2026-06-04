use futures::stream::StreamExt;
use solana_sdk::transaction::VersionedTransaction;
use solana_tx_parser::types::LoadedAddressesInput;
use solana_tx_parser::{DexParser, ParseConfig, RawInstruction, SolanaTransactionInput, TransactionMetaInput};
use std::collections::HashMap;
use tonic::transport::Endpoint;

pub mod old_faithful {
    tonic::include_proto!("old_faithful");
}

use old_faithful::old_faithful_client::OldFaithfulClient;
use old_faithful::StreamBlocksRequest;

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

fn convert_to_solana_input(
    slot: u64,
    block_time: Option<i64>,
    tx: &VersionedTransaction,
) -> Option<(SolanaTransactionInput, Vec<String>)> {
    use solana_sdk::message::VersionedMessage;

    let (account_keys, instructions, loaded_addresses) = match &tx.message {
        VersionedMessage::Legacy(m) => {
            let keys: Vec<String> = m.account_keys.iter().map(|k| bs58_encode(&k.to_bytes())).collect();
            let instrs: Vec<RawInstruction> = m.instructions.iter().map(|i| {
                RawInstruction {
                    program_id_index: i.program_id_index,
                    account_key_indexes: i.accounts.clone(),
                    data: i.data.clone(),
                }
            }).collect();
            (keys, instrs, None)
        }
        VersionedMessage::V0(m) => {
            let keys: Vec<String> = m.account_keys.iter().map(|k| bs58_encode(&k.to_bytes())).collect();
            let instrs: Vec<RawInstruction> = m.instructions.iter().map(|i| {
                RawInstruction {
                    program_id_index: i.program_id_index,
                    account_key_indexes: i.accounts.clone(),
                    data: i.data.clone(),
                }
            }).collect();
            let loaded = if !m.address_table_lookups.is_empty() {
                let mut writable = Vec::new();
                let mut readonly = Vec::new();
                for lookup in &m.address_table_lookups {
                    let table_key = bs58_encode(&lookup.account_key.to_bytes());
                    for &idx in &lookup.writable_indexes {
                        writable.push(format!("{}:{}", table_key, idx));
                    }
                    for &idx in &lookup.readonly_indexes {
                        readonly.push(format!("{}:{}", table_key, idx));
                    }
                }
                Some(LoadedAddressesInput { writable, readonly })
            } else {
                None
            };
            (keys, instrs, loaded)
        }
    };

    let signatures: Vec<Vec<u8>> = tx.signatures.iter().map(|s| s.as_ref().to_vec()).collect();

    Some((
        SolanaTransactionInput {
            slot,
            block_time,
            version: Some(0),
            signatures,
            account_keys: account_keys.clone(),
            instructions,
            inner_instructions: None,
            meta: Some(TransactionMetaInput {
                err: None,
                fee: None,
                pre_balances: None,
                post_balances: None,
                pre_token_balances: None,
                post_token_balances: None,
                inner_instructions: None,
                loaded_addresses,
                compute_units_consumed: None,
            }),
        },
        account_keys,
    ))
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

fn is_sol(mint: &str) -> bool {
    mint == SOL_MINT
}

fn is_stablecoin(mint: &str) -> bool {
    mint == USDC_MINT || mint == USDT_MINT
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
    let mut client = OldFaithfulClient::new(channel);

    eprintln!("Streaming ALL blocks {}..{} (no filter, DEX filtered client-side)", start_slot, end_slot);

    let response = client
        .stream_blocks(StreamBlocksRequest {
            start_slot,
            end_slot,
            filter: None,
        })
        .await?
        .into_inner();

    let dex_parser = DexParser::new();
    let mut slot_ohlcv: HashMap<u64, SlotOhlcv> = HashMap::new();
    let mut total_blocks = 0u64;
    let mut total_tx = 0u64;
    let mut total_trades = 0u64;
    let mut parse_errors = 0u64;
    let started = std::time::Instant::now();
    let mut last_report = std::time::Instant::now();

    let mut stream = response;
    while let Some(result) = stream.next().await {
        let block = match result {
            Ok(b) => b,
            Err(e) => {
                eprintln!("[grpc] block error: {}", e);
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
            match bincode::deserialize::<VersionedTransaction>(&tx.transaction) {
                Ok(versioned_tx) => {
                    if let Some((input, account_keys)) = convert_to_solana_input(slot, block_time, &versioned_tx) {
                        let has_dex = account_keys.iter().any(|k| DEX_PROGRAMS.iter().any(|p| *p == k.as_str()));
                        if !has_dex {
                            continue;
                        }
                        let mut cfg = ParseConfig::default();
                        cfg.program_ids = Some(DEX_PROGRAMS.iter().map(|p| p.to_string()).collect());
                        let trades = dex_parser.parse_trades(&input, Some(cfg));
                        for trade in &trades {
                            let im = &trade.input_token.mint;
                            let om = &trade.output_token.mint;
                            let (sol_price, sol_amount, is_buy) = if is_sol(im) && is_stablecoin(om) {
                                let p = if trade.input_token.amount > 0.0 {
                                    trade.output_token.amount / trade.input_token.amount
                                } else {
                                    0.0
                                };
                                (p, trade.input_token.amount, false)
                            } else if is_stablecoin(im) && is_sol(om) {
                                let p = if trade.output_token.amount > 0.0 {
                                    trade.input_token.amount / trade.output_token.amount
                                } else {
                                    0.0
                                };
                                (p, trade.output_token.amount, true)
                            } else {
                                continue;
                            };
                            if sol_price > 0.0 && sol_amount > 0.0 {
                                total_trades += 1;
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
                }
                Err(e) => {
                    parse_errors += 1;
                    if parse_errors <= 5 {
                        eprintln!("[grpc] slot={} bincode deserialize error: {}", slot, e);
                    }
                }
            }
        }

        total_blocks += 1;
        if last_report.elapsed().as_secs() >= 5 {
            let elapsed = started.elapsed().as_secs_f64();
            let rps = total_blocks as f64 / elapsed;
            eprintln!(
                "[grpc] blocks={} txns={} trades={} parse_errs={} ohlcv={} rps={:.1} elapsed={:.1}s",
                total_blocks, total_tx, total_trades, parse_errors, slot_ohlcv.len(), rps, elapsed
            );
            last_report = std::time::Instant::now();
        }
    }

    let elapsed = started.elapsed().as_secs_f64();
    eprintln!(
        "Done. blocks={} txns={} trades={} parse_errs={} rps={:.1} elapsed={:.1}s",
        total_blocks, total_tx, total_trades, parse_errors, total_blocks as f64 / elapsed, elapsed
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
