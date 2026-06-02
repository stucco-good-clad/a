use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::process;

use clap::Parser as ClapParser;
use serde_json::Value;

#[derive(ClapParser)]
#[command(
    name = "enrich-with-cex",
    about = "Merge CEX 1s klines into enriched Solana blocks"
)]
struct Args {
    #[arg(long, default_value = "enriched")]
    blocks_dir: PathBuf,

    #[arg(long, default_value = "cex")]
    cex_dir: PathBuf,

    #[arg(long, value_delimiter = ',')]
    symbols: Vec<String>,

    #[arg(long, default_value = "cex_enriched")]
    output_dir: PathBuf,
}

#[derive(Debug, Clone)]
struct KlineData {
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    volume: f64,
    quote_volume: f64,
    trades: u64,
    taker_buy_volume: f64,
    taker_buy_quote_volume: f64,
}

fn load_cex_csv(path: &Path) -> Result<HashMap<i64, KlineData>, Box<dyn std::error::Error>> {
    let mut map = HashMap::new();
    let contents = fs::read_to_string(path)?;
    let mut lines = contents.lines();

    lines.next();

    for line in lines {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let fields: Vec<&str> = line.split(',').collect();
        if fields.len() < 11 {
            continue;
        }
        let open_time_us: i64 = fields[0].parse()?;
        let open: f64 = fields[1].parse()?;
        let high: f64 = fields[2].parse()?;
        let low: f64 = fields[3].parse()?;
        let close: f64 = fields[4].parse()?;
        let volume: f64 = fields[5].parse()?;
        let quote_volume: f64 = fields[7].parse()?;
        let trades: u64 = fields[8].parse()?;
        let taker_buy_volume: f64 = fields[9].parse()?;
        let taker_buy_quote_volume: f64 = fields[10].parse()?;

        map.insert(
            open_time_us,
            KlineData {
                open,
                high,
                low,
                close,
                volume,
                quote_volume,
                trades,
                taker_buy_volume,
                taker_buy_quote_volume,
            },
        );
    }
    Ok(map)
}

fn kline_to_json(k: &KlineData) -> Value {
    serde_json::json!({
        "open": k.open,
        "high": k.high,
        "low": k.low,
        "close": k.close,
        "volume": k.volume,
        "quote_volume": k.quote_volume,
        "trades": k.trades,
        "taker_buy_volume": k.taker_buy_volume,
        "taker_buy_quote_volume": k.taker_buy_quote_volume,
    })
}

fn main() {
    let args = Args::parse();

    fs::create_dir_all(&args.output_dir).unwrap_or_else(|e| {
        eprintln!(
            "Error: cannot create directory '{}': {}",
            args.output_dir.display(),
            e
        );
        process::exit(1);
    });

    eprintln!("Loading CEX data from '{}'...", args.cex_dir.display());
    let mut cex_data: HashMap<String, HashMap<i64, KlineData>> = HashMap::new();

    for symbol in &args.symbols {
        let csv_path = args.cex_dir.join(format!("{}.csv", symbol));
        if !csv_path.exists() {
            eprintln!("  Warning: '{}' not found, skipping", csv_path.display());
            continue;
        }
        match load_cex_csv(&csv_path) {
            Ok(data) => {
                eprintln!("  {} — {} klines", symbol, data.len());
                cex_data.insert(symbol.clone(), data);
            }
            Err(e) => {
                eprintln!("  Warning: failed to load '{}': {}", csv_path.display(), e);
            }
        }
    }

    if cex_data.is_empty() {
        eprintln!("Error: no CEX data loaded");
        process::exit(1);
    }

    let block_files: Vec<PathBuf> = fs::read_dir(&args.blocks_dir)
        .unwrap_or_else(|e| {
            eprintln!(
                "Error: cannot read directory '{}': {}",
                args.blocks_dir.display(),
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

    if block_files.is_empty() {
        eprintln!(
            "No .txt files found in '{}'",
            args.blocks_dir.display()
        );
        process::exit(1);
    }

    eprintln!(
        "Enriching {} blocks with CEX data...",
        block_files.len()
    );

    let mut enriched_count = 0u64;
    let mut skipped_count = 0u64;

    for (file_idx, path) in block_files.iter().enumerate() {
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
                continue;
            }
        };

        let mut block: Value = match serde_json::from_str(&data) {
            Ok(v) => v,
            Err(e) => {
                eprintln!("Warning: invalid JSON in '{}': {}", path.display(), e);
                continue;
            }
        };

        let block_time = match block.get("blockTime").and_then(|x| x.as_i64()) {
            Some(t) => t,
            None => {
                skipped_count += 1;
                continue;
            }
        };

        let kline_key_us = block_time * 1_000_000;

        let mut cex_map = serde_json::Map::new();
        for (symbol, data) in &cex_data {
            if let Some(kline) = data.get(&kline_key_us) {
                cex_map.insert(symbol.clone(), kline_to_json(kline));
            }
        }

        if cex_map.is_empty() {
            block.as_object_mut().unwrap().insert(
                "cex".to_string(),
                Value::Null,
            );
        } else {
            block
                .as_object_mut()
                .unwrap()
                .insert("cex".to_string(), Value::Object(cex_map));
        }

        let output_path = args.output_dir.join(format!("{}.txt", slot));
        if let Err(e) = fs::write(&output_path, serde_json::to_string(&block).unwrap()) {
            eprintln!(
                "Warning: failed to write '{}': {}",
                output_path.display(),
                e
            );
            continue;
        }

        enriched_count += 1;

        let processed = enriched_count;
        if processed > 0 && processed.is_multiple_of(500) {
            eprintln!(
                "  {}/{} blocks enriched...",
                file_idx + 1,
                block_files.len()
            );
        }
    }

    eprintln!();
    eprintln!("=== Summary ===");
    eprintln!("  Blocks enriched: {}", enriched_count);
    if skipped_count > 0 {
        eprintln!("  Blocks skipped: {}", skipped_count);
    }
    eprintln!("  Output: {}", args.output_dir.display());
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_kline_to_json() {
        let k = KlineData {
            open: 80.5,
            high: 81.0,
            low: 80.0,
            close: 80.75,
            volume: 1000.0,
            quote_volume: 80500.0,
            trades: 500,
            taker_buy_volume: 600.0,
            taker_buy_quote_volume: 48300.0,
        };
        let json = kline_to_json(&k);
        assert_eq!(json["open"], 80.5);
        assert_eq!(json["close"], 80.75);
        assert_eq!(json["trades"], 500);
    }

    #[test]
    fn test_load_cex_csv() {
        let dir = std::env::temp_dir().join("test_cex_csv");
        fs::create_dir_all(&dir).unwrap();
        let csv_path = dir.join("TEST.csv");
        fs::write(
            &csv_path,
            "open_time_us,open,high,low,close,base_volume,close_time_us,quote_volume,num_trades,taker_buy_base_volume,taker_buy_quote_volume\n1780272000000000,80.5,81.0,80.0,80.75,1000.0,1780272000999999,80500.0,500,600.0,48300.0\n",
        )
        .unwrap();

        let data = load_cex_csv(&csv_path).unwrap();
        assert_eq!(data.len(), 1);
        let kline = data.get(&1780272000000000).unwrap();
        assert!((kline.open - 80.5).abs() < 0.0001);
        assert!((kline.close - 80.75).abs() < 0.0001);
        assert_eq!(kline.trades, 500);

        fs::remove_dir_all(&dir).unwrap();
    }
}
