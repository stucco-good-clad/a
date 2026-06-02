#!/usr/bin/env bash
set -euo pipefail

: "${KEY_1:?KEY_1 is required}"

REGIONS=(ams fra lon ny slc la va jp sg)
BEST_REGION=""
BEST_TIME="999"

for r in "${REGIONS[@]}"; do
  RESP=$(curl -s -w "\n%{time_total}" -X POST "http://${r}.rpc.orbitflare.com?api_key=$KEY_1" \
    -H "Content-Type: application/json" \
    -d '{"jsonrpc":"2.0","id":1,"method":"getSlot","params":[]}' 2>/dev/null)
  BODY=$(echo "$RESP" | head -n -1)
  TOTAL=$(echo "$RESP" | tail -n 1)
  SLOT=$(echo "$BODY" | jq -r '.result // empty' 2>/dev/null || echo "")
  if [ -n "$SLOT" ] && [ "$TOTAL" != "000" ]; then
    echo "${r}: ${TOTAL}s (slot ${SLOT})"
    if [ "$(printf '%s\n' "$TOTAL" "$BEST_TIME" | sort -V | head -n1)" = "$TOTAL" ]; then
      BEST_REGION="$r"
      BEST_TIME="$TOTAL"
    fi
  else
    echo "${r}: FAILED (${BODY})"
  fi
done

if [ -z "$BEST_REGION" ]; then
  echo "ERROR: No RPC endpoint responded successfully" >&2
  exit 1
fi

echo "Selected: ${BEST_REGION} (${BEST_TIME}s)"
if [ -n "${GITHUB_OUTPUT:-}" ]; then
  echo "rpc_url=http://${BEST_REGION}.rpc.orbitflare.com" >> "$GITHUB_OUTPUT"
fi
