#!/usr/bin/env bash
set -euo pipefail

: "${SYMBOLS:?SYMBOLS is required}"
: "${BINANCE_DOWNLOADER:?BINANCE_DOWNLOADER is required}"

set +o pipefail
FIRST=$(ls raw/*.txt | sort -n | head -1)
set -o pipefail
LAST=$(ls raw/*.txt | sort -n | tail -1)
MIN_TIME=$(jq -r '.blockTime' "$FIRST")
MAX_TIME=$(jq -r '.blockTime' "$LAST")
MIN_DATE=$(date -u -d "@$MIN_TIME" '+%Y-%m-%d')
MAX_DATE=$(date -u -d "@$MAX_TIME" '+%Y-%m-%d')

YESTERDAY=$(date -u -d 'yesterday' '+%Y-%m-%d')
if [[ "$MAX_DATE" > "$YESTERDAY" || "$MAX_DATE" == "$YESTERDAY" ]]; then
  MAX_DATE=$YESTERDAY
fi
if [[ "$MIN_DATE" > "$MAX_DATE" ]]; then
  echo "All blocks are too recent — no Binance data available yet. Skipping."
  exit 0
fi

echo "Downloading Binance 1s klines: $MIN_DATE to $MAX_DATE"
"$BINANCE_DOWNLOADER" \
  --symbols "$SYMBOLS" \
  --interval 1s \
  --start-date "$MIN_DATE" \
  --end-date "$MAX_DATE" \
  --output-dir cex
