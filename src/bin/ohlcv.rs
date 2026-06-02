use std::collections::HashMap;
use std::fs;
use std::process;

use serde_json::Value;

/// USDC and USDT mint addresses on Solana mainnet.
const USDC_MINT: &str = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v";
const USDT_MINT: &str = "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB";
const USDC_DECIMALS: f64 = 1e6;

#[derive(Debug)]
struct UsdSwap {
    price: f64,
    amount_usd: f64,
}

fn parse_amount_raw(ui: Option<&Value>) -> u64 {
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

/// Compute per-account token balance deltas (post - pre) from token balance arrays.
/// Returns a map of account address -> Vec<(mint, delta, decimals)>.
fn compute_deltas(
    pre: &[Value],
    post: &[Value],
    accounts: &[String],
) -> HashMap<String, Vec<(String, i128, u8)>> {
    let mut pre_map: HashMap<(usize, String), (i128, u8)> = HashMap::new();
    let mut post_map: HashMap<(usize, String), (i128, u8)> = HashMap::new();

    for pb in pre {
        let idx = pb.get("accountIndex").and_then(|x| x.as_u64()).unwrap_or(0) as usize;
        if idx >= accounts.len() {
            continue;
        }
        let mint = pb.get("mint").and_then(|x| x.as_str()).unwrap_or("");
        if mint.is_empty() {
            continue;
        }
        let amount = parse_amount_raw(pb.get("uiTokenAmount")) as i128;
        let decimals = parse_decimals(pb.get("uiTokenAmount"));
        pre_map.insert((idx, mint.to_string()), (amount, decimals));
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
        let amount = parse_amount_raw(pb.get("uiTokenAmount")) as i128;
        let decimals = parse_decimals(pb.get("uiTokenAmount"));
        post_map.insert((idx, mint.to_string()), (amount, decimals));
    }

    let mut deltas: HashMap<String, Vec<(String, i128, u8)>> = HashMap::new();

    // Collect all mints seen in either pre or post for each account index
    let mut all_keys: Vec<(usize, String)> = Vec::new();
    for key in pre_map.keys().chain(post_map.keys()) {
        if !all_keys.contains(key) {
            all_keys.push(key.clone());
        }
    }

    for (idx, mint) in all_keys {
        if idx >= accounts.len() {
            continue;
        }
        let pre_amt = pre_map.get(&(idx, mint.clone())).map(|(a, _)| *a).unwrap_or(0);
        let post_amt = post_map.get(&(idx, mint.clone())).map(|(a, _)| *a).unwrap_or(0);
        let delta = post_amt - pre_amt;
        if delta == 0 {
            continue;
        }
        let decimals = post_map
            .get(&(idx, mint.clone()))
            .or_else(|| pre_map.get(&(idx, mint.clone())))
            .map(|(_, d)| *d)
            .unwrap_or(6);
        deltas
            .entry(accounts[idx].clone())
            .or_default()
            .push((mint, delta, decimals));
    }

    deltas
}

fn is_usd_mint(mint: &str) -> bool {
    mint == USDC_MINT || mint == USDT_MINT
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let raw_dir = args.get(1).cloned().unwrap_or_else(|| "raw".to_string());
    let out_csv = args.get(2).cloned().unwrap_or_else(|| "ohlcv.csv".to_string());

    let mut files: Vec<_> = fs::read_dir(&raw_dir)
        .unwrap_or_else(|e| {
            eprintln!("Error: cannot read directory '{}': {}", raw_dir, e);
            process::exit(1);
        })
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().map(|x| x == "txt").unwrap_or(false))
        .map(|e| e.path())
        .collect();
    files.sort();

    if files.is_empty() {
        eprintln!("No .txt files found in '{}'", raw_dir);
        process::exit(1);
    }

    let mut slot_prices: HashMap<u64, Vec<UsdSwap>> = HashMap::new();
    let mut slot_times: HashMap<u64, u64> = HashMap::new();
    let mut parse_errors: usize = 0;

    for path in &files {
        let data = match fs::read_to_string(path) {
            Ok(d) => d,
            Err(e) => {
                eprintln!("Warning: failed to read '{}': {}", path.display(), e);
                parse_errors += 1;
                continue;
            }
        };
        let v: Value = match serde_json::from_str(&data) {
            Ok(v) => v,
            Err(e) => {
                eprintln!("Warning: invalid JSON in '{}': {}", path.display(), e);
                parse_errors += 1;
                continue;
            }
        };

        let slot = path
            .file_stem()
            .and_then(|s| s.to_str())
            .and_then(|s| s.parse::<u64>().ok());
        let block_time = v.get("blockTime").and_then(|x| x.as_u64());

        if v.is_null() || slot.is_none() || block_time.is_none() {
            continue;
        }
        let slot = slot.unwrap();
        let block_time = block_time.unwrap();

        let txs = v
            .get("transactions")
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

            let deltas = compute_deltas(pre, post, &account_keys);

            for account_deltas in deltas.values() {
                let mut positives: Vec<(String, i128, u8)> = Vec::new();
                let mut negatives: Vec<(String, i128, u8)> = Vec::new();

                for (mint, delta, decimals) in account_deltas {
                    if *delta > 0 {
                        positives.push((mint.clone(), *delta, *decimals));
                    } else if *delta < 0 {
                        negatives.push((mint.clone(), -*delta, *decimals));
                    }
                }

                // A simple swap: exactly 1 token received, 1 token sent
                if positives.len() != 1 || negatives.len() != 1 {
                    continue;
                }

                // One side must be a USD stablecoin
                let (usd_side, token_side) = if is_usd_mint(&positives[0].0) {
                    (&positives[0], &negatives[0])
                } else if is_usd_mint(&negatives[0].0) {
                    (&negatives[0], &positives[0])
                } else {
                    continue;
                };

                let usd_raw = usd_side.1; // raw USDC/USDT amount (6 decimals)
                let token_raw = token_side.1.abs();
                let token_decimals = token_side.2;

                if usd_raw == 0 || token_raw == 0 {
                    continue;
                }

                let usd_human = usd_raw as f64 / USDC_DECIMALS;
                let token_human = token_raw as f64 / 10_f64.powi(token_decimals as i32);

                if token_human == 0.0 {
                    continue;
                }

                let price = usd_human / token_human;

                slot_prices
                    .entry(slot)
                    .or_default()
                    .push(UsdSwap {
                        price,
                        amount_usd: usd_human,
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
            let high = swaps.iter().map(|s| s.price).fold(f64::MIN, f64::max);
            let low = swaps.iter().map(|s| s.price).fold(f64::MAX, f64::min);
            let volume: f64 = swaps.iter().map(|s| s.amount_usd).sum();
            Some((*slot, block_time, open, high, low, close, volume))
        })
        .collect();

    rows.sort_by_key(|r| r.0);

    let mut wtr = csv::WriterBuilder::new()
        .has_headers(true)
        .from_path(&out_csv)
        .unwrap_or_else(|e| {
            eprintln!("Error: cannot create CSV '{}': {}", out_csv, e);
            process::exit(1);
        });

    if let Err(e) = wtr.write_record(["slot", "blockTime", "open", "high", "low", "close", "volume"]) {
        eprintln!("Error writing CSV header: {}", e);
        process::exit(1);
    }

    for row in &rows {
        if let Err(e) = wtr.write_record(&[
            row.0.to_string(),
            row.1.to_string(),
            format!("{:.9}", row.2),
            format!("{:.9}", row.3),
            format!("{:.9}", row.4),
            format!("{:.9}", row.5),
            format!("{:.9}", row.6),
        ]) {
            eprintln!("Error writing CSV row: {}", e);
            process::exit(1);
        }
    }

    if let Err(e) = wtr.flush() {
        eprintln!("Error flushing CSV: {}", e);
        process::exit(1);
    }

    println!("Wrote {} rows to {}", rows.len(), out_csv);
    if parse_errors > 0 {
        eprintln!("Warning: {} files had parse errors", parse_errors);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_is_usd_mint() {
        assert!(is_usd_mint(USDC_MINT));
        assert!(is_usd_mint(USDT_MINT));
        assert!(!is_usd_mint("So11111111111111111111111111111111111111112"));
    }

    #[test]
    fn test_compute_deltas_simple_swap() {
        // Account 0: had 100 USDC, now has 50 USDC (sent 50 USDC)
        // Account 0: had 0 SOL, now has 0.5 SOL (received 0.5 SOL)
        let accounts = vec!["account0".to_string()];

        let pre = vec![
            json!({
                "accountIndex": 0,
                "mint": USDC_MINT,
                "uiTokenAmount": { "amount": "100000000", "decimals": 6 }
            }),
        ];

        let post = vec![
            json!({
                "accountIndex": 0,
                "mint": USDC_MINT,
                "uiTokenAmount": { "amount": "50000000", "decimals": 6 }
            }),
            json!({
                "accountIndex": 0,
                "mint": "So11111111111111111111111111111111111111112",
                "uiTokenAmount": { "amount": "500000000", "decimals": 9 }
            }),
        ];

        let deltas = compute_deltas(&pre, &post, &accounts);
        let account_deltas = deltas.get("account0").unwrap();

        // Should have 2 deltas: USDC -50000000, SOL +500000000
        assert_eq!(account_deltas.len(), 2);

        let usdc_delta = account_deltas.iter().find(|(m, _, _)| m == USDC_MINT).unwrap();
        assert_eq!(usdc_delta.1, -50000000); // sent 50 USDC

        let sol_delta = account_deltas.iter().find(|(m, _, _)| m != USDC_MINT).unwrap();
        assert_eq!(sol_delta.1, 500000000); // received 0.5 SOL
    }

    #[test]
    fn test_compute_deltas_no_change() {
        let accounts = vec!["account0".to_string()];
        let pre = vec![
            json!({
                "accountIndex": 0,
                "mint": USDC_MINT,
                "uiTokenAmount": { "amount": "100000000", "decimals": 6 }
            }),
        ];
        let post = vec![
            json!({
                "accountIndex": 0,
                "mint": USDC_MINT,
                "uiTokenAmount": { "amount": "100000000", "decimals": 6 }
            }),
        ];

        let deltas = compute_deltas(&pre, &post, &accounts);
        // When there's no change, the account should either not be in the map
        // or have an empty delta list
        match deltas.get("account0") {
            Some(account_deltas) => assert_eq!(account_deltas.len(), 0),
            None => {} // Account not in map is also acceptable for no changes
        }
    }

    #[test]
    fn test_price_calculation() {
        // Swap: 50 USDC for 0.5 SOL
        // Price should be 50 / 0.5 = 100 USD per SOL
        let usd_raw: i128 = 50_000_000; // 50 USDC (6 decimals)
        let token_raw: i128 = 500_000_000; // 0.5 SOL (9 decimals)
        let token_decimals: u8 = 9;

        let usd_human = usd_raw as f64 / USDC_DECIMALS;
        let token_human = token_raw as f64 / 10_f64.powi(token_decimals as i32);
        let price = usd_human / token_human;

        assert!((price - 100.0).abs() < 0.0001);
    }

    #[test]
    fn test_volume_calculation() {
        // 3 swaps: 10 USDC, 20 USDC, 30 USDC
        let volumes = vec![10.0, 20.0, 30.0];
        let total: f64 = volumes.iter().sum();
        assert!((total - 60.0).abs() < 0.0001);
    }
}
