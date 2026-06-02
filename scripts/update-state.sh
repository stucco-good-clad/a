#!/usr/bin/env bash
set -euo pipefail

: "${END:?END is required}"
: "${COUNT:?COUNT is required}"

cat > state.json <<EOF
{
  "next_start_slot": $((END + 1)),
  "last_processed_slot": $END,
  "block_count": $COUNT,
  "updated_at": "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
}
EOF

echo "State updated:"
cat state.json
