#!/usr/bin/env bash
set -euo pipefail

: "${START:?START is required}"
: "${END:?END is required}"
: "${GH_TOKEN:?GH_TOKEN is required}"

CHUNK_SIZE=1000
TAG="backfill-${START}-${END}"
ALL_ZIPS=()

zip_chunk() {
  local label="$1" chunk_num="$2"
  shift 2
  local zip_name="${label}-${START}-${END}-chunk${chunk_num}.zip"
  echo "  $zip_name ($# blocks)"
  zip -jq "$zip_name" "$@"
  ALL_ZIPS+=("$zip_name")
}

zip_dir() {
  local dir="$1" label="$2"
  if ! ls "$dir"/*.txt >/dev/null 2>&1; then
    echo "No $dir blocks found"
    return
  fi
  local total
  total=$(ls "$dir"/*.txt | wc -l)
  echo "Zipping $total $dir blocks in chunks of $CHUNK_SIZE..."
  local chunk_num=0 i=0
  while true; do
    local batch
    batch=$(ls "$dir"/*.txt | sort | sed -n "$((i + 1)),$((i + CHUNK_SIZE))p")
    [ -z "$batch" ] && break
    chunk_num=$((chunk_num + 1))
    zip_chunk "$label" "$chunk_num" $batch
    i=$((i + CHUNK_SIZE))
  done
}

echo "=== Enriched ==="
zip_dir "enriched" "enriched"

echo "=== CEX Enriched ==="
zip_dir "cex_enriched" "cex_enriched"

echo ""
echo "Created ${#ALL_ZIPS[@]} total zips"

if [ ${#ALL_ZIPS[@]} -eq 0 ]; then
  echo "No zips to upload"
  exit 0
fi

if gh release view "$TAG" >/dev/null 2>&1; then
  echo "Release $TAG already exists, updating assets..."
  gh release upload "$TAG" "${ALL_ZIPS[@]}" sol_usd_ohlcv.csv --clobber
else
  gh release create "$TAG" "${ALL_ZIPS[@]}" sol_usd_ohlcv.csv \
    --title "Backfill ${START} to ${END}" \
    --generate-notes
fi

echo "Done."
