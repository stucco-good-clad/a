# batch-blocks

Solana block data pipeline that fetches raw block data via JSON-RPC and converts it into OHLCV (Open/High/Low/Close/Volume) candlestick data for token swaps.

## Architecture

```
┌─────────────┐     ┌──────────────┐     ┌─────────┐
│ batch-blocks│────>│  raw/*.txt   │────>│  ohlcv  │
│ (fetcher)   │     │ (block JSON) │     │ (parser)│
└─────────────┘     └──────────────┘     └─────────┘
       │                                        │
       v                                        v
   RPC nodes                              ohlcv.csv
```

## Binaries

### `batch-blocks`

Fetches Solana block data in parallel batches across multiple API keys.

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

**Output:** `raw/{slot}.txt` files containing block JSON

### `ohlcv`

Parses raw block files and extracts USDC/USDT swap prices into OHLCV candles.

**Usage:**
```bash
ohlcv [raw_dir] [output_csv]
# Defaults: raw_dir=raw, output_csv=ohlcv.csv
```

**Output:** CSV with columns: `slot, blockTime, open, high, low, close, volume`

## CI/CD

The `backfill.yml` GitHub Actions workflow runs the full pipeline:
1. Selects fastest RPC endpoint across 9 regions
2. Fetches 1000-block windows
3. Processes blocks into OHLCV data
4. Uploads CSV as artifact
5. Commits state for next run

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
# Build
cargo build --release

# Run fetcher
RPC_URL=http://localhost:8899 KEY_1=test ./target/release/batch-blocks

# Run OHLCV parser
./target/release/ohlcv raw output.csv
```

## License

MIT
