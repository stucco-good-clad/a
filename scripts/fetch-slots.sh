#!/usr/bin/env bash
set -euo pipefail

: "${ONFINALITY_KEYS:?ONFINALITY_KEYS is required}"
: "${START:?START is required}"
: "${END:?END is required}"

KEY=$(echo "$ONFINALITY_KEYS" | cut -d',' -f1)
RESP=$(curl -s -X POST "https://solana.api.onfinality.io/rpc?apikey=${KEY}" \
  -H "Content-Type: application/json" \
  -d "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"getBlocks\",\"params\":[$START,$END]}")
echo "$RESP" | jq -r '.result[]' > slots.txt
COUNT=$(wc -l < slots.txt)
echo "Valid blocks: $COUNT"
if [ -n "${GITHUB_OUTPUT:-}" ]; then
  echo "count=$COUNT" >> "$GITHUB_OUTPUT"
fi
