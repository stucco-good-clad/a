#!/usr/bin/env bash
set -euo pipefail

: "${KEY_1:?KEY_1 is required}"
: "${RPC_URL:?RPC_URL is required}"
: "${GIST_ID:?GIST_ID is required}"

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
