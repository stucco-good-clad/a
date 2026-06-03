#!/usr/bin/env bash
set -euo pipefail

: "${GIST_ID:?GIST_ID is required}"
: "${ONFINALITY_KEYS:?ONFINALITY_KEYS is required}"

get_current_slot() {
  local key resp slot
  key=$(echo "$ONFINALITY_KEYS" | cut -d',' -f1)
  resp=$(curl -s -X POST "https://solana.api.onfinality.io/rpc?apikey=${key}" \
    -H "Content-Type: application/json" \
    -d '{"jsonrpc":"2.0","id":1,"method":"getSlot","params":[]}')
  slot=$(echo "$resp" | jq -r '.result // empty')
  if [ -z "$slot" ] || [ "$slot" = "null" ]; then
    echo "ERROR: Failed to get current slot. Response: $resp" >&2
    return 1
  fi
  echo "$slot"
}

STATE=$(gh gist view "$GIST_ID" -f state.json 2>/dev/null || echo "")

if [ -n "$STATE" ] && echo "$STATE" | jq -e '.next_start_slot' >/dev/null 2>&1; then
  NEXT_START=$(echo "$STATE" | jq -r '.next_start_slot')
  START=$NEXT_START
  CURRENT=$(get_current_slot)
  if [ "$START" -ge "$CURRENT" ]; then
    echo "Already caught up (next_start=$START >= current=$CURRENT). Using last 1000 slots."
    START=$((CURRENT - 999))
    END=$CURRENT
  else
    END=$((START + 999))
    if [ "$END" -gt "$CURRENT" ]; then
      END=$CURRENT
    fi
  fi
  echo "Resuming from Gist state: window $START to $END"
else
  CURRENT=$(get_current_slot)
  END=$CURRENT
  START=$((CURRENT - 999))
  echo "Fresh start at slot $CURRENT: window $START to $END"
fi

if [ -n "${GITHUB_OUTPUT:-}" ]; then
  echo "start=$START" >> "$GITHUB_OUTPUT"
  echo "end=$END" >> "$GITHUB_OUTPUT"
fi
