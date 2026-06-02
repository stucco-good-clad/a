use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::process;

use clap::Parser as ClapParser;
use serde_json::Value;
use solana_tx_parser::types::LoadedAddressesInput;
use solana_tx_parser::{
    DexParser, InnerInstructionSet, RawInstruction, SolanaTransactionInput, TokenBalanceInput,
    TradeInfo, TransactionMetaInput, UiTokenAmountInput,
};

const SOL_MINT: &str = "So11111111111111111111111111111111111111112";
const USDC_MINT: &str = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v";
const USDT_MINT: &str = "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB";
const OUTLIER_THRESHOLD: f64 = 0.5;

#[derive(ClapParser)]
#[command(
    name = "sol-swap-parser",
    about = "Parse Solana DEX swaps and export per-block SOL/USD OHLCV candles"
)]
struct Args {
    #[arg(long, default_value = "raw")]
    raw_dir: PathBuf,

    #[arg(long, default_value = "sol_usd_ohlcv.csv")]
    output: PathBuf,

    #[arg(long, default_value = "1.0")]
    min_volume: f64,
}

#[derive(Debug, Clone)]
struct BlockTrade {
    price: f64,
    volume: f64,
    is_buy: bool,
}

#[derive(Debug, Clone)]
struct Candle {
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    volume_usd: f64,
    buy_volume_usd: f64,
    sell_volume_usd: f64,
    price_volume_sum: f64,
    trades: u64,
    buy_count: u64,
    sell_count: u64,
}

impl Candle {
    fn new() -> Self {
        Self {
            open: 0.0,
            high: f64::MIN,
            low: f64::MAX,
            close: 0.0,
            volume_usd: 0.0,
            buy_volume_usd: 0.0,
            sell_volume_usd: 0.0,
            price_volume_sum: 0.0,
            trades: 0,
            buy_count: 0,
            sell_count: 0,
        }
    }

    fn update(&mut self, price: f64, volume: f64, is_buy: bool) {
        if self.trades == 0 {
            self.open = price;
        }
        self.high = self.high.max(price);
        self.low = self.low.min(price);
        self.close = price;
        self.volume_usd += volume;
        self.price_volume_sum += price * volume;
        if is_buy {
            self.buy_volume_usd += volume;
            self.buy_count += 1;
        } else {
            self.sell_volume_usd += volume;
            self.sell_count += 1;
        }
        self.trades += 1;
    }

    fn vwap(&self) -> f64 {
        if self.volume_usd > 0.0 {
            self.price_volume_sum / self.volume_usd
        } else {
            self.close
        }
    }
}

fn is_sol_usd_trade(trade: &TradeInfo) -> bool {
    let input = &trade.input_token.mint;
    let output = &trade.output_token.mint;
    (input == SOL_MINT && (output == USDC_MINT || output == USDT_MINT))
        || (output == SOL_MINT && (input == USDC_MINT || input == USDT_MINT))
}

fn get_sol_usd_price(trade: &TradeInfo) -> Option<f64> {
    let (sol_amount, usd_amount) = if trade.input_token.mint == SOL_MINT {
        (trade.input_token.amount, trade.output_token.amount)
    } else if trade.output_token.mint == SOL_MINT {
        (trade.output_token.amount, trade.input_token.amount)
    } else {
        return None;
    };
    if sol_amount > 0.0 {
        Some(usd_amount / sol_amount)
    } else {
        None
    }
}

fn get_usd_amount(trade: &TradeInfo) -> f64 {
    if trade.input_token.mint == USDC_MINT || trade.input_token.mint == USDT_MINT {
        trade.input_token.amount
    } else if trade.output_token.mint == USDC_MINT || trade.output_token.mint == USDT_MINT {
        trade.output_token.amount
    } else {
        0.0
    }
}

fn is_buy_trade(trade: &TradeInfo) -> bool {
    trade.output_token.mint == SOL_MINT
}

fn median(values: &mut [f64]) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let n = values.len();
    if n.is_multiple_of(2) {
        (values[n / 2 - 1] + values[n / 2]) / 2.0
    } else {
        values[n / 2]
    }
}

fn filter_outliers(trades: &[BlockTrade]) -> Vec<&BlockTrade> {
    if trades.len() < 3 {
        return trades.iter().collect();
    }
    let mut prices: Vec<f64> = trades.iter().map(|t| t.price).collect();
    let med = median(&mut prices);
    if med <= 0.0 {
        return trades.iter().collect();
    }
    let lo = med * (1.0 - OUTLIER_THRESHOLD);
    let hi = med * (1.0 + OUTLIER_THRESHOLD);
    trades
        .iter()
        .filter(|t| t.price >= lo && t.price <= hi)
        .collect()
}

fn convert_to_solana_input(
    slot: u64,
    block_time: Option<i64>,
    tx: &Value,
) -> Option<SolanaTransactionInput> {
    let transaction = tx.get("transaction")?;
    let msg = transaction.get("message")?;
    let meta = tx.get("meta");

    let signatures: Vec<Vec<u8>> = transaction
        .get("signatures")?
        .as_array()?
        .iter()
        .filter_map(|s| s.as_str())
        .filter_map(|s| bs58::decode(s).into_vec().ok())
        .collect();

    let mut account_keys: Vec<String> = msg
        .get("accountKeys")?
        .as_array()?
        .iter()
        .filter_map(|k| k.as_str().map(String::from))
        .collect();

    if let Some(loaded) = meta.and_then(|m| m.get("loadedAddresses")) {
        if let Some(writable) = loaded.get("writable").and_then(|arr| arr.as_array()) {
            for key in writable {
                if let Some(s) = key.as_str() {
                    account_keys.push(s.to_string());
                }
            }
        }
        if let Some(readonly) = loaded.get("readonly").and_then(|arr| arr.as_array()) {
            for key in readonly {
                if let Some(s) = key.as_str() {
                    account_keys.push(s.to_string());
                }
            }
        }
    }

    let instructions = msg
        .get("instructions")?
        .as_array()?
        .iter()
        .filter_map(|inst| {
            let program_id_index = inst.get("programIdIndex")?.as_u64()? as u8;
            let data_str = inst.get("data")?.as_str()?;
            let data = bs58::decode(data_str).into_vec().ok()?;
            let account_key_indexes: Vec<u8> = inst
                .get("accounts")?
                .as_array()?
                .iter()
                .filter_map(|a| a.as_u64().map(|v| v as u8))
                .collect();
            Some(RawInstruction {
                program_id_index,
                data,
                account_key_indexes,
            })
        })
        .collect();

    let inner_instructions = meta.and_then(|m| {
        m.get("innerInstructions")
            .and_then(|arr| arr.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|iis| {
                        let index = iis.get("index")?.as_u64()? as u32;
                        let instructions = iis
                            .get("instructions")?
                            .as_array()?
                            .iter()
                            .filter_map(|inst| {
                                let program_id_index =
                                    inst.get("programIdIndex")?.as_u64()? as u8;
                                let data_str = inst.get("data")?.as_str()?;
                                let data = bs58::decode(data_str).into_vec().ok()?;
                                let account_key_indexes: Vec<u8> = inst
                                    .get("accounts")?
                                    .as_array()?
                                    .iter()
                                    .filter_map(|a| a.as_u64().map(|v| v as u8))
                                    .collect();
                                Some(RawInstruction {
                                    program_id_index,
                                    data,
                                    account_key_indexes,
                                })
                            })
                            .collect();
                        Some(InnerInstructionSet {
                            index,
                            instructions,
                        })
                    })
                    .collect::<Vec<_>>()
            })
    });

    let meta_input = meta.map(|m| {
        let parse_token_balances = |key: &str| -> Option<Vec<TokenBalanceInput>> {
            m.get(key)
                .and_then(|arr| arr.as_array())
                .map(|arr| {
                    arr.iter()
                        .filter_map(|b| {
                            let account_index = b.get("accountIndex")?.as_u64()? as u32;
                            let mint =
                                b.get("mint").and_then(|x| x.as_str()).map(String::from);
                            let owner =
                                b.get("owner").and_then(|x| x.as_str()).map(String::from);
                            let ui = b.get("uiTokenAmount")?;
                            let amount = ui.get("amount")?.as_str()?.to_string();
                            let decimals = ui.get("decimals")?.as_u64()? as u8;
                            let ui_amount = ui.get("uiAmount").and_then(|x| x.as_f64());
                            let ui_amount_string = ui
                                .get("uiAmountString")
                                .and_then(|x| x.as_str())
                                .map(String::from);
                            Some(TokenBalanceInput {
                                account_index,
                                mint,
                                owner,
                                ui_token_amount: UiTokenAmountInput {
                                    amount,
                                    decimals,
                                    ui_amount,
                                    ui_amount_string,
                                },
                            })
                        })
                        .collect()
                })
        };

        TransactionMetaInput {
            err: m.get("err").cloned(),
            fee: m.get("fee").and_then(|x| x.as_u64()),
            pre_balances: m.get("preBalances").and_then(|arr| {
                arr.as_array()
                    .map(|a| a.iter().filter_map(|v| v.as_u64()).collect())
            }),
            post_balances: m.get("postBalances").and_then(|arr| {
                arr.as_array()
                    .map(|a| a.iter().filter_map(|v| v.as_u64()).collect())
            }),
            pre_token_balances: parse_token_balances("preTokenBalances"),
            post_token_balances: parse_token_balances("postTokenBalances"),
            inner_instructions: None,
            loaded_addresses: m.get("loadedAddresses").map(|la| {
                let parse_arr = |key: &str| -> Vec<String> {
                    la.get(key)
                        .and_then(|arr| arr.as_array())
                        .map(|a| {
                            a.iter()
                                .filter_map(|k| k.as_str().map(String::from))
                                .collect()
                        })
                        .unwrap_or_default()
                };
                LoadedAddressesInput {
                    writable: parse_arr("writable"),
                    readonly: parse_arr("readonly"),
                }
            }),
            compute_units_consumed: m.get("computeUnitsConsumed").and_then(|x| x.as_u64()),
        }
    });

    let version = transaction
        .get("version")
        .and_then(|v| match v.as_str()? {
            "legacy" => Some(None),
            v => v.parse::<u8>().ok().map(Some),
        })
        .flatten();

    Some(SolanaTransactionInput {
        slot,
        block_time,
        version,
        signatures,
        account_keys,
        instructions,
        inner_instructions,
        meta: meta_input,
    })
}

fn main() {
    let args = Args::parse();

    let mut files: Vec<PathBuf> = fs::read_dir(&args.raw_dir)
        .unwrap_or_else(|e| {
            eprintln!(
                "Error: cannot read directory '{}': {}",
                args.raw_dir.display(),
                e
            );
            process::exit(1);
        })
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.path()
                .extension()
                .map(|x| x == "txt")
                .unwrap_or(false)
        })
        .map(|e| e.path())
        .collect();
    files.sort();

    if files.is_empty() {
        eprintln!("No .txt files found in '{}'", args.raw_dir.display());
        process::exit(1);
    }

    eprintln!("Processing {} raw block files...", files.len());

    let parser = DexParser::new();
    let mut block_trades: HashMap<u64, Vec<BlockTrade>> = HashMap::new();
    let mut candles: HashMap<u64, Candle> = HashMap::new();
    let mut slot_times: HashMap<u64, i64> = HashMap::new();

    let mut total_txs = 0u64;
    let mut total_trades = 0u64;
    let mut sol_usd_trades = 0u64;
    let mut outliers_filtered = 0u64;
    let mut parse_errors = 0usize;

    for (file_idx, path) in files.iter().enumerate() {
        let slot = match path
            .file_stem()
            .and_then(|s| s.to_str())
            .and_then(|s| s.parse::<u64>().ok())
        {
            Some(s) => s,
            None => continue,
        };

        let data = match fs::read_to_string(path) {
            Ok(d) => d,
            Err(e) => {
                eprintln!("Warning: failed to read '{}': {}", path.display(), e);
                parse_errors += 1;
                continue;
            }
        };

        let block: Value = match serde_json::from_str(&data) {
            Ok(v) => v,
            Err(e) => {
                eprintln!("Warning: invalid JSON in '{}': {}", path.display(), e);
                parse_errors += 1;
                continue;
            }
        };

        let block_time = block.get("blockTime").and_then(|x| x.as_i64());
        if let Some(bt) = block_time {
            slot_times.insert(slot, bt);
        }

        let txs = block
            .get("transactions")
            .and_then(|x| x.as_array())
            .map(|v| v.as_slice())
            .unwrap_or(&[]);

        for tx in txs {
            total_txs += 1;

            let solana_input = match convert_to_solana_input(slot, block_time, tx) {
                Some(i) => i,
                None => continue,
            };

            let trades = parser.parse_trades(&solana_input, None);
            total_trades += trades.len() as u64;

            for trade in &trades {
                if !is_sol_usd_trade(trade) {
                    continue;
                }
                sol_usd_trades += 1;

                let price = match get_sol_usd_price(trade) {
                    Some(p) => p,
                    None => continue,
                };

                let volume = get_usd_amount(trade);
                let is_buy = is_buy_trade(trade);

                if volume < args.min_volume {
                    continue;
                }

                block_trades
                    .entry(slot)
                    .or_default()
                    .push(BlockTrade {
                        price,
                        volume,
                        is_buy,
                    });
            }
        }

        if let Some(trades) = block_trades.get(&slot) {
            let filtered = filter_outliers(trades);
            let dropped = trades.len() - filtered.len();
            outliers_filtered += dropped as u64;
            let mut candle = Candle::new();
            for t in filtered {
                candle.update(t.price, t.volume, t.is_buy);
            }
            if candle.trades > 0 {
                candles.insert(slot, candle);
            }
        }

        let processed = candles.len();
        if processed > 0 && processed.is_multiple_of(500) {
            eprintln!(
                "  {}/{} files, {} SOL/USD trades, {} candles...",
                file_idx + 1,
                files.len(),
                sol_usd_trades,
                processed
            );
        }
    }

    let mut rows: Vec<(u64, i64, Candle)> = candles
        .into_iter()
        .map(|(slot, candle)| {
            let bt = slot_times.get(&slot).copied().unwrap_or(0);
            (slot, bt, candle)
        })
        .collect();
    rows.sort_by_key(|r| r.0);

    let mut wtr = csv::WriterBuilder::new()
        .has_headers(true)
        .from_path(&args.output)
        .unwrap_or_else(|e| {
            eprintln!("Error: cannot create CSV '{}': {}", args.output.display(), e);
            process::exit(1);
        });

    wtr.write_record([
        "slot",
        "block_time",
        "open",
        "high",
        "low",
        "close",
        "vwap",
        "volume_usd",
        "buy_volume_usd",
        "sell_volume_usd",
        "trades",
        "buy_count",
        "sell_count",
    ])
    .unwrap();

    for (slot, block_time, candle) in &rows {
        wtr.write_record([
            slot.to_string(),
            block_time.to_string(),
            format!("{:.9}", candle.open),
            format!("{:.9}", candle.high),
            format!("{:.9}", candle.low),
            format!("{:.9}", candle.close),
            format!("{:.9}", candle.vwap()),
            format!("{:.9}", candle.volume_usd),
            format!("{:.9}", candle.buy_volume_usd),
            format!("{:.9}", candle.sell_volume_usd),
            candle.trades.to_string(),
            candle.buy_count.to_string(),
            candle.sell_count.to_string(),
        ])
        .unwrap();
    }

    wtr.flush().unwrap();

    eprintln!();
    eprintln!("=== Summary ===");
    eprintln!("  Files processed: {}", rows.len());
    eprintln!("  Parse errors: {}", parse_errors);
    eprintln!("  Total transactions: {}", total_txs);
    eprintln!("  Total trades parsed: {}", total_trades);
    eprintln!("  SOL/USD trades: {}", sol_usd_trades);
    if outliers_filtered > 0 {
        eprintln!("  Outliers filtered: {}", outliers_filtered);
    }
    eprintln!("  Output: {}", args.output.display());
}

#[cfg(test)]
mod tests {
    use super::*;
    use solana_tx_parser::{TokenInfo, TradeType};

    fn make_trade(
        input_mint: &str,
        output_mint: &str,
        input_amount: f64,
        output_amount: f64,
    ) -> TradeInfo {
        TradeInfo {
            user: "user1".to_string(),
            trade_type: TradeType::Swap,
            pool: vec!["pool1".to_string()],
            input_token: TokenInfo {
                mint: input_mint.to_string(),
                amount: input_amount,
                amount_raw: "0".to_string(),
                decimals: 9,
                authority: None,
                destination: None,
                destination_owner: None,
                source: None,
            },
            output_token: TokenInfo {
                mint: output_mint.to_string(),
                amount: output_amount,
                amount_raw: "0".to_string(),
                decimals: 6,
                authority: None,
                destination: None,
                destination_owner: None,
                source: None,
            },
            slippage_bps: None,
            fee: None,
            fees: None,
            program_id: None,
            amm: Some("Raydium".to_string()),
            amms: None,
            route: None,
            slot: 100,
            timestamp: 1234567890,
            signature: "sig1".to_string(),
            idx: "0".to_string(),
            signer: None,
        }
    }

    #[test]
    fn test_is_sol_usd_trade() {
        assert!(is_sol_usd_trade(&make_trade(SOL_MINT, USDC_MINT, 1.0, 150.0)));
        assert!(is_sol_usd_trade(&make_trade(USDC_MINT, SOL_MINT, 150.0, 1.0)));
        assert!(!is_sol_usd_trade(&make_trade(
            SOL_MINT,
            "RandomMint111111111111111111111111111111",
            1.0,
            100.0
        )));
    }

    #[test]
    fn test_get_sol_usd_price() {
        let price = get_sol_usd_price(&make_trade(SOL_MINT, USDC_MINT, 1.0, 150.0)).unwrap();
        assert!((price - 150.0).abs() < 0.0001);

        let price = get_sol_usd_price(&make_trade(USDC_MINT, SOL_MINT, 150.0, 1.0)).unwrap();
        assert!((price - 150.0).abs() < 0.0001);
    }

    #[test]
    fn test_get_usd_amount() {
        assert!(
            (get_usd_amount(&make_trade(SOL_MINT, USDC_MINT, 1.0, 150.0)) - 150.0).abs()
                < 0.0001
        );
        assert!(
            (get_usd_amount(&make_trade(USDC_MINT, SOL_MINT, 150.0, 1.0)) - 150.0).abs()
                < 0.0001
        );
    }

    #[test]
    fn test_is_buy_trade() {
        assert!(is_buy_trade(&make_trade(USDC_MINT, SOL_MINT, 150.0, 1.0)));
        assert!(!is_buy_trade(&make_trade(SOL_MINT, USDC_MINT, 1.0, 150.0)));
    }

    #[test]
    fn test_candle_update() {
        let mut c = Candle::new();
        c.update(100.0, 50.0, true);
        c.update(105.0, 60.0, false);
        c.update(95.0, 40.0, true);

        assert!((c.open - 100.0).abs() < 0.0001);
        assert!((c.high - 105.0).abs() < 0.0001);
        assert!((c.low - 95.0).abs() < 0.0001);
        assert!((c.close - 95.0).abs() < 0.0001);
        assert!((c.volume_usd - 150.0).abs() < 0.0001);
        assert!((c.buy_volume_usd - 90.0).abs() < 0.0001);
        assert!((c.sell_volume_usd - 60.0).abs() < 0.0001);
        assert_eq!(c.trades, 3);
        assert_eq!(c.buy_count, 2);
        assert_eq!(c.sell_count, 1);
    }

    #[test]
    fn test_vwap() {
        let mut c = Candle::new();
        c.update(100.0, 50.0, true);
        c.update(200.0, 50.0, false);

        // VWAP = (100*50 + 200*50) / 100 = 15000/100 = 150.0
        assert!((c.vwap() - 150.0).abs() < 0.0001);
    }

    #[test]
    fn test_median() {
        assert!((median(&mut vec![100.0]) - 100.0).abs() < 0.0001);
        assert!((median(&mut vec![100.0, 200.0]) - 150.0).abs() < 0.0001);
        assert!((median(&mut vec![100.0, 150.0, 200.0]) - 150.0).abs() < 0.0001);
        assert!((median(&mut vec![100.0, 100.0, 100.0, 28962.0]) - 100.0).abs() < 0.0001);
    }

    #[test]
    fn test_filter_outliers() {
        let trades = vec![
            BlockTrade { price: 76.0, volume: 100.0, is_buy: true },
            BlockTrade { price: 76.5, volume: 200.0, is_buy: false },
            BlockTrade { price: 76.2, volume: 150.0, is_buy: true },
            BlockTrade { price: 28962.0, volume: 30.0, is_buy: true },
        ];
        let filtered = filter_outliers(&trades);
        assert_eq!(filtered.len(), 3);
        assert!(filtered.iter().all(|t| t.price < 200.0));
    }

    #[test]
    fn test_filter_outliers_small_batch() {
        let trades = vec![
            BlockTrade { price: 76.0, volume: 100.0, is_buy: true },
            BlockTrade { price: 76.5, volume: 200.0, is_buy: false },
        ];
        let filtered = filter_outliers(&trades);
        assert_eq!(filtered.len(), 2);
    }
}
