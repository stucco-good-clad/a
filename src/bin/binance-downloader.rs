use std::fs;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::process;

use chrono::{Duration, NaiveDate};
use clap::Parser as ClapParser;

#[derive(ClapParser)]
#[command(
    name = "binance-downloader",
    about = "Download 1s klines from Binance.vision and consolidate into per-symbol CSVs"
)]
struct Args {
    #[arg(long, value_delimiter = ',')]
    symbols: Vec<String>,

    #[arg(long, default_value = "1s")]
    interval: String,

    #[arg(long)]
    start_date: String,

    #[arg(long)]
    end_date: String,

    #[arg(long, default_value = "cex")]
    output_dir: PathBuf,
}

struct KlineRow {
    open_time_us: i64,
    open: String,
    high: String,
    low: String,
    close: String,
    base_volume: String,
    close_time_us: i64,
    quote_volume: String,
    num_trades: String,
    taker_buy_base_volume: String,
    taker_buy_quote_volume: String,
}

fn download_zip(url: &str) -> Result<Vec<u8>, reqwest::Error> {
    let client = reqwest::blocking::Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .build()
        .unwrap();
    let bytes = client.get(url).send()?.bytes()?.to_vec();
    Ok(bytes)
}

fn extract_csv_from_zip(zip_bytes: &[u8]) -> Result<String, Box<dyn std::error::Error>> {
    let reader = std::io::Cursor::new(zip_bytes);
    let mut archive = zip::ZipArchive::new(reader)?;

    if archive.is_empty() {
        return Err("ZIP archive is empty".into());
    }

    let mut csv_file = archive.by_index(0)?;
    let mut contents = String::new();
    csv_file.read_to_string(&mut contents)?;
    Ok(contents)
}

fn parse_csv_rows(csv_contents: &str) -> Vec<KlineRow> {
    let mut rows = Vec::new();
    for line in csv_contents.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let fields: Vec<&str> = line.split(',').collect();
        if fields.len() < 11 {
            continue;
        }
        rows.push(KlineRow {
            open_time_us: fields[0].parse().unwrap_or(0),
            open: fields[1].to_string(),
            high: fields[2].to_string(),
            low: fields[3].to_string(),
            close: fields[4].to_string(),
            base_volume: fields[5].to_string(),
            close_time_us: fields[6].parse().unwrap_or(0),
            quote_volume: fields[7].to_string(),
            num_trades: fields[8].to_string(),
            taker_buy_base_volume: fields[9].to_string(),
            taker_buy_quote_volume: fields[10].to_string(),
        });
    }
    rows
}

fn write_consolidated_csv(path: &Path, rows: &[KlineRow]) -> Result<(), std::io::Error> {
    let mut file = fs::File::create(path)?;
    writeln!(
        file,
        "open_time_us,open,high,low,close,base_volume,close_time_us,quote_volume,num_trades,taker_buy_base_volume,taker_buy_quote_volume"
    )?;
    for row in rows {
        writeln!(
            file,
            "{},{},{},{},{},{},{},{},{},{},{}",
            row.open_time_us,
            row.open,
            row.high,
            row.low,
            row.close,
            row.base_volume,
            row.close_time_us,
            row.quote_volume,
            row.num_trades,
            row.taker_buy_base_volume,
            row.taker_buy_quote_volume,
        )?;
    }
    Ok(())
}

fn main() {
    let args = Args::parse();

    let start = NaiveDate::parse_from_str(&args.start_date, "%Y-%m-%d").unwrap_or_else(|e| {
        eprintln!(
            "Error: invalid start date '{}': {}",
            args.start_date, e
        );
        process::exit(1);
    });
    let end = NaiveDate::parse_from_str(&args.end_date, "%Y-%m-%d").unwrap_or_else(|e| {
        eprintln!("Error: invalid end date '{}': {}", args.end_date, e);
        process::exit(1);
    });

    fs::create_dir_all(&args.output_dir).unwrap_or_else(|e| {
        eprintln!(
            "Error: cannot create directory '{}': {}",
            args.output_dir.display(),
            e
        );
        process::exit(1);
    });

    let days = (end - start).num_days() + 1;
    eprintln!(
        "Downloading {}s klines for {} symbols, {} days ({} to {})...",
        args.interval,
        args.symbols.len(),
        days,
        args.start_date,
        args.end_date
    );

    let mut total_rows = 0u64;
    let mut failed_downloads = 0u64;

    for symbol in &args.symbols {
        eprintln!("  Downloading {}...", symbol);
        let mut all_rows: Vec<KlineRow> = Vec::new();

        let mut current = start;
        while current <= end {
            let date_str = current.format("%Y-%m-%d").to_string();
            let url = format!(
                "https://data.binance.vision/data/spot/daily/klines/{}/{}/{}-{}-{}.zip",
                symbol, args.interval, symbol, args.interval, date_str
            );

            match download_zip(&url) {
                Ok(zip_bytes) => match extract_csv_from_zip(&zip_bytes) {
                    Ok(csv) => {
                        let rows = parse_csv_rows(&csv);
                        let count = rows.len();
                        all_rows.extend(rows);
                        eprintln!("    {} — {} rows", date_str, count);
                    }
                    Err(e) => {
                        eprintln!("    {} — failed to extract CSV: {}", date_str, e);
                        failed_downloads += 1;
                    }
                },
                Err(e) => {
                    eprintln!("    {} — download failed: {}", date_str, e);
                    failed_downloads += 1;
                }
            }

            current += Duration::days(1);
        }

        all_rows.sort_by_key(|r| r.open_time_us);

        let output_path = args.output_dir.join(format!("{}.csv", symbol));
        write_consolidated_csv(&output_path, &all_rows).unwrap_or_else(|e| {
            eprintln!(
                "Error: cannot write '{}': {}",
                output_path.display(),
                e
            );
            process::exit(1);
        });

        total_rows += all_rows.len() as u64;
        eprintln!(
            "    Total: {} rows → {}",
            all_rows.len(),
            output_path.display()
        );
    }

    eprintln!();
    eprintln!("=== Summary ===");
    eprintln!("  Symbols: {}", args.symbols.len());
    eprintln!("  Total rows: {}", total_rows);
    if failed_downloads > 0 {
        eprintln!("  Failed downloads: {}", failed_downloads);
    }
    eprintln!("  Output: {}", args.output_dir.display());
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_csv_rows() {
        let csv = "1780272000000000,73674.39000000,73685.50000000,73674.39000000,73685.49000000,0.85507000,1780272000999999,62998.33664560,265,0.85469000,62970.33615940,0";
        let rows = parse_csv_rows(csv);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].open_time_us, 1780272000000000);
        assert_eq!(rows[0].open, "73674.39000000");
        assert_eq!(rows[0].close, "73685.49000000");
        assert_eq!(rows[0].num_trades, "265");
    }

    #[test]
    fn test_parse_csv_rows_empty() {
        let rows = parse_csv_rows("");
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn test_parse_csv_rows_multiple() {
        let csv = "1000,1.0,2.0,0.5,1.5,100.0,1999,200.0,50,60.0,120.0,0\n2000,1.5,2.5,1.0,2.0,150.0,2999,300.0,75,80.0,160.0,0";
        let rows = parse_csv_rows(csv);
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].open_time_us, 1000);
        assert_eq!(rows[1].open_time_us, 2000);
    }
}
