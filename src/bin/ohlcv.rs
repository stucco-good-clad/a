use std::collections::HashMap;
use std::fs;

use serde_json::Value;

const USDC: &str = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v";
const USDT: &str = "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB";

#[derive(Debug)]
struct UsdSwap {
    price: f64,
    amount_usd: f64,
}

fn parse_amount(ui: Option<&Value>) -> u64 {
    ui.and_then(|u| u.get("amount"))
        .and_then(|x| x.as_str())
        .and_then(|s| s.parse().ok())
        .unwrap_or(0)
}

fn parse_decimals(ui: Option<&Value>) -> u8 {
    ui.and_then(|u| u.get("decimals"))
        .and_then(|x| x.as_u64())
        .unwrap_or(6) as u8
}

fn account_changes(
    pre: &[Value],
    post: &[Value],
    accounts: &[String],
) -> HashMap<String, (Vec<(String, i128, u8)>, Vec<(String, i128, u8)>)> {
    let mut map: HashMap<String, (Vec<(String, i128, u8)>, Vec<(String, i128, u8)>)> = HashMap::new();

    for pb in pre {
        let idx = pb.get("accountIndex").and_then(|x| x.as_u64()).unwrap_or(0) as usize;
        if idx >= accounts.len() {
            continue;
        }
        let mint = pb.get("mint").and_then(|x| x.as_str()).unwrap_or("");
        if mint.is_empty() {
            continue;
        }
        let amount = parse_amount(pb.get("uiTokenAmount")) as i128;
        let decimals = parse_decimals(pb.get("uiTokenAmount"));
        let acc = accounts[idx].clone();
        let key = acc.clone();
        map.entry(key).or_default().0.push((mint.to_string(), amount, decimals));
    }

    for pb in post {
        let idx = pb.get("accountIndex").and_then(|x| x.as_u64()).unwrap_or(0) as usize;
        if idx >= accounts.len() {
            continue;
        }
        let mint = pb.get("mint").and_then(|x| x.as_str()).unwrap_or("");
        if mint.is_empty() {
            continue;
        }
        let amount = parse_amount(pb.get("uiTokenAmount")) as i128;
        let decimals = parse_decimals(pb.get("uiTokenAmount"));
        let acc = accounts[idx].clone();
        let key = acc.clone();
        map.entry(key).or_default().1.push((mint.to_string(), amount, decimals));
    }

    map
}

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

    let mut slot_prices: HashMap<u64, Vec<UsdSwap>> = HashMap::new();
    let mut slot_times: HashMap<u64, u64> = HashMap::new();

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

        let txs = v.get("transactions")
            .and_then(|x| x.as_array())
            .map(|v| v.as_slice())
            .unwrap_or(&[]);

        for tx in txs {
            let msg = tx.get("transaction").and_then(|x| x.get("message"));
            let meta = tx.get("meta");
            let meta_obj = meta.and_then(|m| m.as_object());

            let account_keys = msg
                .and_then(|m| m.get("accountKeys"))
                .and_then(|x| x.as_array())
                .map(|arr| {
                    arr.iter()
                        .filter_map(|k| k.as_str().map(String::from))
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();

            let pre = meta_obj
                .and_then(|m| m.get("preTokenBalances"))
                .and_then(|x| x.as_array())
                .map(|v| v.as_slice())
                .unwrap_or(&[]);
            let post = meta_obj
                .and_then(|m| m.get("postTokenBalances"))
                .and_then(|x| x.as_array())
                .map(|v| v.as_slice())
                .unwrap_or(&[]);

            let changes = account_changes(pre, post, &account_keys);

            for (_, data) in changes {
                let (post_changes, _) = data;
                let mut positives: Vec<(String, i128, u8)> = Vec::new();
                let mut negatives: Vec<(String, i128, u8)> = Vec::new();
                let mut usd_pos: Option<(String, i128, u8)> = None;
                let mut usd_neg: Option<(String, i128, u8)> = None;

                for (mint, amount, decimals) in post_changes {
                    if amount > 0 {
                        positives.push((mint.clone(), amount, decimals));
                        if mint == USDC || mint == USDT {
                            usd_pos = Some((mint.to_string(), amount, decimals));
                        }
                    } else if amount < 0 {
                        negatives.push((mint.clone(), -amount, decimals));
                        if mint == USDC || mint == USDT {
                            usd_neg = Some((mint.to_string(), -amount, decimals));
                        }
                    }
                }

                if positives.len() != 1 || negatives.len() != 1 {
                    continue;
                }

                let usd_amount = if let Some((_, amt, _dec)) = usd_pos {
                    amt
                } else if let Some((_, amt, _dec)) = usd_neg {
                    amt
                } else {
                    continue;
                };

                let other = if usd_pos.is_some() { negatives[0].clone() } else { positives[0].clone() };
                let other_amount = other.1;
                let other_decimals = other.2;

                if usd_amount == 0 || other_amount == 0 {
                    continue;
                }

                let price = (usd_amount as f64 / 10_f64.powi(other.2 as i32))
                    / (other_amount as f64 / 10_f64.powi(other_decimals as i32));

                slot_prices.entry(slot).or_default().push(UsdSwap {
                    price,
                    amount_usd: usd_amount as f64 / 10_f64.powi(other.2 as i32),
                });
                slot_times.entry(slot).or_insert(block_time);
            }
        }
    }

    let mut rows: Vec<(u64, u64, f64, f64, f64, f64, f64)> = slot_prices
        .iter()
        .filter_map(|(slot, swaps)| {
            if swaps.is_empty() {
                return None;
            }
            let block_time = *slot_times.get(slot)?;
            let open = swaps[0].price;
            let close = swaps[swaps.len() - 1].price;
            let high = swaps.iter().map(|s| s.price).fold(open, f64::max);
            let low = swaps.iter().map(|s| s.price).fold(open, f64::min);
            let volume: f64 = swaps.iter().map(|s| s.amount_usd).sum();
            Some((*slot, block_time, open, high, low, close, volume))
        })
        .collect();

    rows.sort_by_key(|r| r.0);

    let mut wtr = csv::WriterBuilder::new().has_headers(true).from_path(&out_csv).unwrap();
    let _ = wtr.write_record(&["slot", "blockTime", "open", "high", "low", "close", "volume"]);
    for row in &rows {
        let _ = wtr.write_record(&[
            row.0.to_string(),
            row.1.to_string(),
            format!("{:.9}", row.2),
            format!("{:.9}", row.3),
            format!("{:.9}", row.4),
            format!("{:.9}", row.5),
            format!("{:.9}", row.6),
        ]);
    }
    let _ = wtr.flush();
    println!("Wrote {} rows to {}", rows.len(), out_csv);
}
