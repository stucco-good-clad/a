#!/usr/bin/env bash
set -euo pipefail

: "${KEY_1:?KEY_1 is required}"
: "${RPC_URL:?RPC_URL is required}"

get_current_slot() {
  local resp slot
  resp=$(curl -s -X POST "$RPC_URL?api_key=$KEY_1" \
    -H "Content-Type: application/json" \
    -d '{"jsonrpc":"2.0","id":1,"method":"getSlot","params":[]}')
  slot=$(echo "$resp" | jq -r '.result // empty')
  if [ -z "$slot" ] || [ "$slot" = "null" ]; then
    echo "ERROR: Failed to get current slot. Response: $resp" >&2
    return 1
  fi
  echo "$slot"
}

if [ -f state.json ]; then
  NEXT_START=$(jq -r '.next_start_slot' state.json)
  START=$NEXT_START
  CURRENT=$(get_current_slot)
  if [ "$START" -ge "$CURRENT" ]; then
    echo "Already caught up (next_start=$START >= current=$CURRENT). Using last 1000 slots."
    START=$((CURRENT - 9999))
    END=$CURRENT
  else
    END=$((START + 9999))
    if [ "$END" -gt "$CURRENT" ]; then
      END=$CURRENT
    fi
  fi
  echo "Resuming from state: window $START to $END"
else
  CURRENT=$(get_current_slot)
  END=$CURRENT
  START=$((CURRENT - 9999))
  echo "Fresh start at slot $CURRENT: window $START to $END"
fi

if [ -n "${GITHUB_OUTPUT:-}" ]; then
  echo "start=$START" >> "$GITHUB_OUTPUT"
  echo "end=$END" >> "$GITHUB_OUTPUT"
fi
