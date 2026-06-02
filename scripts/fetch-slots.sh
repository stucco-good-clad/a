#!/usr/bin/env bash
set -euo pipefail

: "${KEY_1:?KEY_1 is required}"
: "${RPC_URL:?RPC_URL is required}"
: "${START:?START is required}"
: "${END:?END is required}"

RESP=$(curl -s -X POST "$RPC_URL?api_key=$KEY_1" \
  -H "Content-Type: application/json" \
  -d "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"getBlocks\",\"params\":[$START,$END]}")
echo "$RESP" | jq -r '.result[]' > slots.txt
COUNT=$(wc -l < slots.txt)
echo "Valid blocks: $COUNT"
if [ -n "${GITHUB_OUTPUT:-}" ]; then
  echo "count=$COUNT" >> "$GITHUB_OUTPUT"
fi
