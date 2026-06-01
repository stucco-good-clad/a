use anyhow::{Context, Result};
use clap::Parser;
use solana_backfill::Backfill;
use tracing::{error, info};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Solana RPC endpoint URL
    #[arg(short, long)]
    rpc: String,

    /// API key for RPC (optional)
    #[arg(short, long)]
    api_key: Option<String>,

    /// Start slot (inclusive)
    #[arg(short, long)]
    start_slot: u64,

    /// End slot (inclusive)
    #[arg(short, long)]
    end_slot: u64,

    /// Number of blocks per batch
    #[arg(short, long, default_value = "100")]
    batch_size: usize,

    /// Maximum concurrent requests
    #[arg(short, long, default_value = "20")]
    max_concurrent: usize,

    /// Output directory for blocks
    #[arg(short, long, default_value = "./blocks")]
    output: String,

    /// Request timeout in seconds
    #[arg(long, default_value = "60")]
    timeout: u64,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let args = Args::parse();

    if args.end_slot < args.start_slot {
        anyhow::bail!("end_slot must be >= start_slot");
    }

    let total_blocks = (args.end_slot - args.start_slot + 1) as usize;
    info!(
        "Starting backfill: slots {} -> {} ({} blocks)",
        args.start_slot, args.end_slot, total_blocks
    );
    info!("RPC: {}", args.rpc);
    info!("Batch size: {}, Max concurrent: {}", args.batch_size, args.max_concurrent);
    info!("Output: {}", args.output);

    let mut backfill = Backfill::new(args)?;
    let result = backfill.run().await?;

    info!(
        "Done: {} ok, {} err, {:.2} MB/s",
        result.ok, result.err, result.mb_per_sec
    );

    Ok(())
}
