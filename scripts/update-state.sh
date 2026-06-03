#!/usr/bin/env bash
set -euo pipefail

: "${END:?END is required}"
: "${COUNT:?COUNT is required}"
: "${GIST_ID:?GIST_ID is required}"

STATE=$(cat <<EOF
{
  "next_start_slot": $((END + 1)),
  "last_processed_slot": $END,
  "block_count": $COUNT,
  "updated_at": "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
}
EOF
)

echo "$STATE" | gh gist edit "$GIST_ID" -f state.json

echo "State updated in Gist:"
echo "$STATE"
