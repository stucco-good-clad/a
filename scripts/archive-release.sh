#!/usr/bin/env bash
set -euo pipefail

: "${START:?START is required}"
: "${END:?END is required}"
: "${GH_TOKEN:?GH_TOKEN is required}"

TAG="backfill-${START}-${END}"
BLOCKS=$(ls enriched/*.txt 2>/dev/null | wc -l)
CEX_BLOCKS=$(ls cex_enriched/*.txt 2>/dev/null | wc -l)
echo "Archiving ${BLOCKS} enriched + ${CEX_BLOCKS} cex_enriched blocks..."

zip -j "enriched-${START}-${END}.zip" enriched/*.txt
zip -j "cex_enriched-${START}-${END}.zip" cex_enriched/*.txt

if gh release view "$TAG" >/dev/null 2>&1; then
  echo "Release $TAG already exists, updating assets..."
  gh release upload "$TAG" \
    "enriched-${START}-${END}.zip" \
    "cex_enriched-${START}-${END}.zip" \
    sol_usd_ohlcv.csv --clobber
else
  gh release create "$TAG" \
    "enriched-${START}-${END}.zip" \
    "cex_enriched-${START}-${END}.zip" \
    sol_usd_ohlcv.csv \
    --title "Backfill ${START} to ${END}" \
    --generate-notes
fi
