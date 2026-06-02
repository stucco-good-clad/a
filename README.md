# batch-blocks

Solana block data pipeline that fetches raw block data via JSON-RPC, parses DEX swaps using [solana-tx-parser](https://crates.io/crates/solana-tx-parser), and exports per-block SOL/USD OHLCV candlestick data for ML training.

## Architecture

```
┌─────────────────┐     ┌──────────────┐     ┌──────────────────┐     ┌───────────┐
│  batch-blocks   │────>│  raw/*.txt   │────>│ sol-swap-parser  │────>│ SOL_USD   │
│  (fetcher)      │     │ (filtered)   │     │  (DEX parser)    │     │ _ohlcv.csv│
└─────────────────┘     └──────────────┘     └──────────────────┘     └───────────┘
        │                                              │
        ├── Multi-RPC (10 API keys)                    ├── Jupiter, Raydium, Orca, Meteora, Pumpfun
        ├── Concurrency limit (20)                     ├── SOL/USDC & SOL/USDT swap filtering
        ├── Retry with backoff                         ├── Per-block candles (~400ms resolution)
        ├── Null inner-tx filtering                    ├── Real VWAP (trade-weighted)
        └── Batch processing                           └── CSV export
```

## Binaries

### `batch-blocks`

Fetches Solana block data in parallel batches across multiple API keys.

**Filtering:** Strips transactions with null/empty inner instructions before saving. Only DEX-active transactions are persisted.

**Modes:**
- **Coordinator** (default): Discovers valid block slots via `getBlocks` RPC method
- **Worker** (`--slots-file <path>`): Reads slot numbers from a file

**Environment Variables:**
| Variable | Default | Description |
|----------|---------|-------------|
| `RPC_URL` | `http://slc.rpc.orbitflare.com` | Solana RPC endpoint |
| `KEY_1..N` | (required) | API keys for round-robin distribution |
| `BATCH_SIZE` | `10` | Slots per JSON-RPC batch request |
| `NUM_BLOCKS` | `1000` | Number of blocks to fetch (coordinator mode) |
| `RANGE_START` | (auto) | Start slot for block discovery |
| `RANGE_END` | (auto) | End slot for block discovery |

**Output:** `raw/{slot}.txt` files containing filtered block JSON

### `sol-swap-parser`

Parses raw block files using `solana-tx-parser` and exports per-block SOL/USD OHLCV candles.

**Usage:**
```bash
sol-swap-parser [OPTIONS]

OPTIONS:
    --raw-dir <DIR>       Directory with raw block JSON files [default: raw]
    --output <FILE>       Output CSV file [default: sol_usd_ohlcv.csv]
    --min-volume <USD>    Minimum USD volume per trade [default: 1.0]
```

**Supported DEXes:** Jupiter, Raydium, Orca, Meteora, Pumpfun, Pumpswap

**Output CSV Columns:**
| Column | Type | Description |
|--------|------|-------------|
| `slot` | u64 | Solana block number |
| `block_time` | i64 | Unix timestamp |
| `open` | f64 | First SOL/USD price in block |
| `high` | f64 | Highest SOL/USD price |
| `low` | f64 | Lowest SOL/USD price |
| `close` | f64 | Last SOL/USD price |
| `vwap` | f64 | Volume-weighted average price |
| `volume_usd` | f64 | Total USD volume |
| `buy_volume_usd` | f64 | USD volume from buy trades |
| `sell_volume_usd` | f64 | USD volume from sell trades |
| `trades` | u64 | Number of swaps |
| `buy_count` | u64 | Number of buy trades |
| `sell_count` | u64 | Number of sell trades |

### `ohlcv`

Legacy parser using balance-delta approach (kept for backward compatibility).

## CI/CD

The `backfill.yml` GitHub Actions workflow runs the full pipeline:
1. Selects fastest RPC endpoint across 9 regions
2. Builds all binaries (cached via `rust-cache`)
3. Fetches raw blocks using 10 API keys (null inner-tx filtered)
4. Parses DEX swaps and exports per-block SOL/USD OHLCV
5. Uploads CSV as artifact
6. Archives filtered raw blocks + CSV to GitHub Release
7. Commits state for next run

Trigger manually via Actions tab.

## State Tracking

`state.json` tracks pipeline progress:
```json
{
  "next_start_slot": 423720666,
  "last_processed_slot": 423720665,
  "block_count": 1001,
  "updated_at": "2026-06-02T11:44:31Z"
}
```

## Development

```bash
# Build all binaries
cargo build --release

# Run fetcher
RPC_URL=http://localhost:8899 KEY_1=test ./target/release/batch-blocks

# Parse SOL/USD OHLCV from raw blocks
./target/release/sol-swap-parser --raw-dir raw --output sol_usd_ohlcv.csv

# Run tests
cargo test
```

## License

MIT
