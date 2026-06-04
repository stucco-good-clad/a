use futures::stream::StreamExt;
use solana_tx_parser::{DexParser, ParseConfig, SolanaTransactionInput, TransactionMetaInput, RawInstruction};
use solana_tx_parser::types::LoadedAddressesInput;
use dashmap::DashMap;
use tonic::transport::Endpoint;

pub mod old_faithful {
    tonic::include_proto!("old_faithful");
}

use old_faithful::old_faithful_client::OldFaithfulClient;
use old_faithful::BlockRequest;

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

fn read_compact_u16(bytes: &[u8], offset: &mut usize) -> u16 {
    let mut result: u16 = 0;
    let mut shift = 0;
    loop {
        if *offset >= bytes.len() {
            break;
        }
        let byte = bytes[*offset];
        *offset += 1;
        result |= ((byte & 0x7f) as u16) << shift;
        if byte & 0x80 == 0 {
            break;
        }
        shift += 7;
    }
    result
}

struct ParsedTx {
    signatures: Vec<Vec<u8>>,
    account_keys: Vec<Vec<u8>>,
    instructions: Vec<(u8, Vec<u8>, Vec<u8>)>,
    loaded_writable: Vec<Vec<u8>>,
    loaded_readonly: Vec<Vec<u8>>,
}

fn parse_transaction(tx_bytes: &[u8]) -> Option<ParsedTx> {
    if tx_bytes.is_empty() {
        return None;
    }

    let (msg_bytes, is_versioned) = if tx_bytes[0] == 0x80 {
        (&tx_bytes[1..], true)
    } else {
        (tx_bytes, false)
    };

    let mut off = 0;

    let num_sigs = read_compact_u16(msg_bytes, &mut off) as usize;
    let end_of_sigs = off + num_sigs * 64;
    if end_of_sigs > msg_bytes.len() {
        return None;
    }
    let signatures: Vec<Vec<u8>> = (0..num_sigs)
        .map(|_| {
            let s = msg_bytes[off..off + 64].to_vec();
            off += 64;
            s
        })
        .collect();

    if !is_versioned {
        off += 3;
    }

    let num_accounts = read_compact_u16(msg_bytes, &mut off) as usize;
    let accounts_end = off + num_accounts * 32;
    if accounts_end > msg_bytes.len() {
        return None;
    }
    let account_keys: Vec<Vec<u8>> = (0..num_accounts)
        .map(|_| {
            let k = msg_bytes[off..off + 32].to_vec();
            off += 32;
            k
        })
        .collect();

    off += 32; // recent_blockhash

    let num_instrs = read_compact_u16(msg_bytes, &mut off) as usize;
    let mut instructions = Vec::with_capacity(num_instrs);
    for _ in 0..num_instrs {
        if off >= msg_bytes.len() {
            break;
        }
        let pid = msg_bytes[off];
        off += 1;
        let accts_len = read_compact_u16(msg_bytes, &mut off) as usize;
        if off + accts_len > msg_bytes.len() {
            break;
        }
        let accts = msg_bytes[off..off + accts_len].to_vec();
        off += accts_len;
        let data_len = read_compact_u16(msg_bytes, &mut off) as usize;
        if off + data_len > msg_bytes.len() {
            break;
        }
        let data = msg_bytes[off..off + data_len].to_vec();
        off += data_len;
        instructions.push((pid, accts, data));
    }

    let mut loaded_writable = Vec::new();
    let mut loaded_readonly = Vec::new();

    if is_versioned && off < msg_bytes.len() {
        let num_lookups = read_compact_u16(msg_bytes, &mut off) as usize;
        for _ in 0..num_lookups {
            if off + 32 > msg_bytes.len() {
                break;
            }
            off += 32; // account_key (lookup table address)
            let nw = read_compact_u16(msg_bytes, &mut off) as usize;
            for _ in 0..nw {
                if off >= msg_bytes.len() {
                    break;
                }
                let idx = msg_bytes[off] as usize;
                off += 1;
                if idx < account_keys.len() {
                    loaded_writable.push(account_keys[idx].clone());
                }
            }
            let nr = read_compact_u16(msg_bytes, &mut off) as usize;
            for _ in 0..nr {
                if off >= msg_bytes.len() {
                    break;
                }
                let idx = msg_bytes[off] as usize;
                off += 1;
                if idx < account_keys.len() {
                    loaded_readonly.push(account_keys[idx].clone());
                }
            }
        }
    }

    Some(ParsedTx {
        signatures,
        account_keys,
        instructions,
        loaded_writable,
        loaded_readonly,
    })
}

fn build_solana_input(
    slot: u64,
    block_time: Option<i64>,
    tx_bytes: &[u8],
) -> Option<(SolanaTransactionInput, Vec<String>)> {
    let parsed = parse_transaction(tx_bytes)?;

    let mut all_keys = parsed.account_keys.clone();
    all_keys.extend(parsed.loaded_writable.iter().cloned());
    all_keys.extend(parsed.loaded_readonly.iter().cloned());

    let account_keys_str: Vec<String> = all_keys.iter().map(|k| bs58_encode(k)).collect();

    let instructions: Vec<RawInstruction> = parsed
        .instructions
        .into_iter()
        .map(|(pid, accts, data)| RawInstruction {
            program_id_index: pid,
            account_key_indexes: accts,
            data,
        })
        .collect();

    let loaded_addresses = if !parsed.loaded_writable.is_empty() || !parsed.loaded_readonly.is_empty() {
        Some(LoadedAddressesInput {
            writable: parsed
                .loaded_writable
                .iter()
                .map(|k| bs58_encode(k))
                .collect(),
            readonly: parsed
                .loaded_readonly
                .iter()
                .map(|k| bs58_encode(k))
                .collect(),
        })
    } else {
        None
    };

    Some((SolanaTransactionInput {
        slot,
        block_time,
        version: Some(0),
        signatures: parsed.signatures,
        account_keys: account_keys_str.clone(),
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
    }, account_keys_str))
}

#[derive(Debug, Default)]
struct SlotOhlcv {
    volume: f64,
    buy_volume: f64,
    sell_volume: f64,
    num_trades: usize,
    sol_usd_prices: Vec<f64>,
}

fn is_sol(mint: &str) -> bool {
    mint == SOL_MINT
}

fn has_dex_program(account_keys: &[String]) -> bool {
    for k in account_keys {
        if DEX_PROGRAMS.iter().any(|p| p == k) {
            return true;
        }
    }
    false
}

fn is_stablecoin(mint: &str) -> bool {
    mint == USDC_MINT || mint == USDT_MINT
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let endpoint = std::env::var("FAITHFUL_ENDPOINT")
        .unwrap_or_else(|_| "http://127.0.0.1:8888".to_string());
    let start_slot: u64 = std::env::var("START_SLOT")
        .unwrap_or_else(|_| "345600000".to_string())
        .parse()?;
    let end_slot: u64 = std::env::var("END_SLOT")
        .unwrap_or_else(|_| "346032000".to_string())
        .parse()?;
    let output_dir = std::env::var("OUTPUT_DIR").unwrap_or_else(|_| "./output".to_string());

    std::fs::create_dir_all(&output_dir)?;

    let concurrency: usize = std::env::var("CONCURRENCY")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(500);

    eprintln!("Connecting to {}", endpoint);
    let channel = Endpoint::from_shared(endpoint)?
        .max_frame_size(16 * 1024 * 1024 - 1)
        .connect()
        .await?;
    let client = OldFaithfulClient::new(channel);

    eprintln!(
        "Fetching blocks {}..{} with concurrency={}",
        start_slot, end_slot, concurrency
    );

    let dex_parser = std::sync::Arc::new(DexParser::new());
    let slot_ohlcv = std::sync::Arc::new(DashMap::<u64, SlotOhlcv>::new());
    let counters = std::sync::Arc::new(std::sync::Mutex::new((0u64, 0u64, 0u64))); // (slots_done, total_tx, total_trades)
    let started = std::time::Instant::now();
    let last_report = std::sync::Arc::new(std::sync::Mutex::new(std::time::Instant::now()));

    let slots: Vec<u64> = (start_slot..end_slot).collect();
    let total = slots.len();
    eprintln!("Total slots: {}", total);

    let stream = futures::stream::iter(slots)
        .map(|slot| {
            let mut client = client.clone();
            let dex_parser = std::sync::Arc::clone(&dex_parser);
            let slot_ohlcv = std::sync::Arc::clone(&slot_ohlcv);
            let counters = std::sync::Arc::clone(&counters);
            let last_report = std::sync::Arc::clone(&last_report);
            async move {
                let resp = client.get_block(BlockRequest { slot }).await;
                match resp {
                    Ok(r) => {
                        let block = r.into_inner();
                        let block_time = if block.block_time != 0 {
                            Some(block.block_time)
                        } else {
                            None
                        };
                        let s = block.slot;

                        let mut local_tx = 0u64;
                        let mut local_trades = 0u64;
                        for tx in &block.transactions {
                            local_tx += 1;
                            if let Some((input, keys)) = build_solana_input(s, block_time, &tx.transaction) {
                                if !has_dex_program(&keys) {
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
                                        } else { 0.0 };
                                        (p, trade.input_token.amount, false)
                                    } else if is_stablecoin(im) && is_sol(om) {
                                        let p = if trade.output_token.amount > 0.0 {
                                            trade.input_token.amount / trade.output_token.amount
                                        } else { 0.0 };
                                        (p, trade.output_token.amount, true)
                                    } else {
                                        continue;
                                    };
                                    if sol_price > 0.0 && sol_amount > 0.0 {
                                        local_trades += 1;
                                        let mut entry = slot_ohlcv.entry(s).or_default();
                                        let data = entry.value_mut();
                                        data.sol_usd_prices.push(sol_price);
                                        data.volume += sol_amount;
                                        data.num_trades += 1;
                                        if is_buy { data.buy_volume += sol_amount; }
                                        else { data.sell_volume += sol_amount; }
                                    }
                                }
                            }
                        }

                        let mut c = counters.lock().unwrap();
                        c.0 += 1;
                        c.1 += local_tx;
                        c.2 += local_trades;
                        if c.0 <= 3 {
                            eprintln!("[grpc] slot={} txns={} block_time={:?}", s, local_tx, block_time);
                        }

                        let mut lr = last_report.lock().unwrap();
                        if lr.elapsed().as_secs() >= 5 {
                            let elapsed = started.elapsed().as_secs_f64();
                            let rps = c.0 as f64 / elapsed;
                            eprintln!(
                                "[grpc] slots={}/{} txns={} trades={} ohlcv_slots={} rps={:.1} elapsed={:.1}s",
                                c.0, total, c.1, c.2, slot_ohlcv.len(), rps, elapsed
                            );
                            *lr = std::time::Instant::now();
                        }
                    }
                    Err(e) => {
                        let mut c = counters.lock().unwrap();
                        c.0 += 1;
                        if c.0 <= 5 {
                            eprintln!("[grpc] slot={} error: {}", slot, e);
                        }
                    }
                }
            }
        })
        .buffer_unordered(concurrency);

    let (slots_done, total_tx, total_trades) = {
        let mut stream = std::pin::pin!(stream);
        while stream.next().await.is_some() {}
        let c = counters.lock().unwrap();
        (c.0, c.1, c.2)
    };
    eprintln!(
        "Done. slots_done={}, txns={}, trades={}, elapsed={:.1}s",
        slots_done,
        total_tx,
        total_trades,
        started.elapsed().as_secs_f64()
    );

    eprintln!(
        "Done. Writing OHLCV for {} slots...",
        slot_ohlcv.len()
    );

    let mut csv = vec![
        "slot,block_time,open,high,low,close,volume,num_trades,buy_volume,sell_volume".to_string(),
    ];

    let mut slots: Vec<u64> = slot_ohlcv.iter().map(|r| *r.key()).collect();
    slots.sort();

    for slot in &slots {
        let data = slot_ohlcv.get(slot).unwrap();
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
            slot, 0, open, high, low, close, data.volume, data.num_trades, data.buy_volume,
            data.sell_volume
        ));
    }

    let csv_path = format!("{}/ohlcv.csv", output_dir);
    std::fs::write(&csv_path, csv.join("\n"))?;
    eprintln!("Written {} rows to {}", csv.len() - 1, csv_path);

    Ok(())
}
