#!/usr/bin/env bash
set -euo pipefail

: "${KEY_1:?KEY_1 is required}"

REGIONS=(ams fra lon ny slc la va jp sg)
declare -A TIMES
ALL_OK=true

for r in "${REGIONS[@]}"; do
  RESP=$(curl -s -w "\n%{time_total}" --max-time 10 -X POST "http://${r}.rpc.orbitflare.com?api_key=$KEY_1" \
    -H "Content-Type: application/json" \
    -d '{"jsonrpc":"2.0","id":1,"method":"getSlot","params":[]}' 2>/dev/null)
  BODY=$(echo "$RESP" | head -n -1)
  TOTAL=$(echo "$RESP" | tail -n 1)
  SLOT=$(echo "$BODY" | jq -r '.result // empty' 2>/dev/null || echo "")
  if [ -n "$SLOT" ] && [ "$TOTAL" != "000" ]; then
    TIMES[$r]="$TOTAL"
    echo "${r}: ${TOTAL}s (slot ${SLOT})"
  else
    echo "${r}: FAILED (${BODY})"
    ALL_OK=false
  fi
done

if [ "${#TIMES[@]}" -eq 0 ]; then
  echo "ERROR: No RPC endpoint responded successfully" >&2
  exit 1
fi

# Sort servers by latency, output comma-separated list (fastest first)
SORTED=$(for r in "${!TIMES[@]}"; do echo "${TIMES[$r]} ${r}"; done | sort -n | awk '{print $2}')
RPC_URLS=$(echo "$SORTED" | tr '\n' ',' | sed 's/,$//')
BEST_REGION=$(echo "$SORTED" | head -n1)
BEST_TIME="${TIMES[$BEST_REGION]}"

echo ""
echo "Fastest: ${BEST_REGION} (${BEST_TIME}s)"
echo "All servers (sorted): ${RPC_URLS}"

if [ -n "${GITHUB_OUTPUT:-}" ]; then
  echo "rpc_url=http://${BEST_REGION}.rpc.orbitflare.com" >> "$GITHUB_OUTPUT"
  echo "rpc_urls=$(echo "$SORTED" | sed 's/^/http:\/\//;s/$/.rpc.orbitflare.com/' | tr '\n' ',' | sed 's/,$//')" >> "$GITHUB_OUTPUT"
fi
