#!/usr/bin/env bash
set -euo pipefail

BIN="${1:-./target/release/solana-backfill}"
RPC="${2:-http://slc.rpc.orbitflare.com}"
KEY="${3:-}"
OUTDIR="./blocks_bench"
BLOCKS=1000
BATCH_SIZES="5 10 20 50 100"
CONCURRENCIES="10 20 40 80 100"

mkdir -p "$OUTDIR"

printf "%-8s %-12s %10s %12s %10s %10s\n" "batch" "concurr" "blocks/s" "MB/s" "ok" "err"
printf "%-8s %-12s %10s %12s %10s %10s\n" "--------" "------------" "----------" "------------" "----------" "----------"

for bs in $BATCH_SIZES; do
  for mc in $CONCURRENCIES; do
    run_dir="$OUTDIR/b${bs}_c${mc}"
    mkdir -p "$run_dir"
    set +e
    output=$("$BIN" --rpc "$RPC" ${KEY:+--api-key "$KEY"} --from-latest "$BLOCKS" --batch-size "$bs" --max-concurrent "$mc" --output "$run_dir" 2>&1)
    rc=$?
    set -e
    if [[ $rc -ne 0 ]]; then
      printf "%-8s %-12s %10s %12s %10s %10s\n" "$bs" "$mc" "FAIL" "-" "-" "-" | tee -a bench_results.txt
      continue
    fi
    ok=$(echo "$output" | grep -oP 'Done: \K[0-9]+' | head -1 || echo 0)
    err=$(echo "$output" | grep -oP 'Done: .*? \K[0-9]+' | head -1 || echo 0)
    mbs=$(echo "$output" | grep -oP '\d+\.\d+ MB/s' | grep -oP '[\d.]+' | head -1 || echo 0)
    elapsed=$(echo "$output" | grep -oP '\d+ seconds' | grep -oP '[\d]+' | head -1 || echo 1)

    if [[ "$elapsed" == "0" || -z "$elapsed" ]]; then elapsed=1; fi
    bps=$(awk "BEGIN {printf \"%.1f\", $BLOCKS/$elapsed}")

    printf "%-8s %-12s %10s %12s %10s %10s\n" "$bs" "$mc" "$bps" "$mbs" "$ok" "$err"
  done
done
