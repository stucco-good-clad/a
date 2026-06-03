#!/bin/bash
set -euo pipefail

ENDPOINT="${ENDPOINT:-http://localhost:8899}"
EPOCH="${EPOCH:-979}"
SLOTS_PER_EPOCH=432000
OUTPUT_DIR="${OUTPUT_DIR:-$RUNNER_TEMP/test-results}"

mkdir -p "$OUTPUT_DIR"

echo "============================================"
echo "  Faithful-CLI Benchmark Suite"
echo "============================================"
echo "Endpoint: $ENDPOINT"
echo "Epoch: $EPOCH"
echo "Output: $OUTPUT_DIR"
echo ""

# --- Helpers ---
rpc_call() {
    local method=$1
    local params=$2
    local timeout=${3:-60}
    curl -sf --max-time "$timeout" -X POST "$ENDPOINT" \
        -H "Content-Type: application/json" \
        -d "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"$method\",\"params\":$params}"
}

time_rpc() {
    local method=$1
    local params=$2
    local timeout=${3:-60}
    local start_ns
    start_ns=$(date +%s%N)
    local result
    result=$(rpc_call "$method" "$params" "$timeout" 2>/dev/null) || { echo "ERROR"; return 1; }
    local end_ns
    end_ns=$(date +%s%N)
    local elapsed_ms=$(( (end_ns - start_ns) / 1000000 ))
    echo "$elapsed_ms"
}

get_block() {
    local slot=$1
    local timeout=${2:-60}
    curl -sf --max-time "$timeout" -X POST "$ENDPOINT" \
        -H "Content-Type: application/json" \
        -d "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"getBlock\",\"params\":[$slot,{\"encoding\":\"json\",\"transactionDetails\":\"none\",\"rewards\":false,\"maxSupportedTransactionVersion\":0}]}"
}

# ============================================
echo "=== Test 1: Basic RPC Methods ==="
echo ""

echo -n "getVersion: "
time_rpc "getVersion" "[]" > /dev/null
echo "ms"

echo -n "getSlot: "
SLOT_RESULT=$(rpc_call "getSlot" "[]")
echo "$SLOT_RESULT" > "$OUTPUT_DIR/getSlot.json"
CURRENT_SLOT=$(echo "$SLOT_RESULT" | jq -r '.result // "null"')
echo "$CURRENT_SLOT"

echo -n "getFirstAvailableBlock: "
time_rpc "getFirstAvailableBlock" "[]"
echo "ms"

echo -n "getEpochInfo: "
EPOCH_RESULT=$(rpc_call "getEpochInfo" "[]")
echo "$EPOCH_RESULT" > "$OUTPUT_DIR/getEpochInfo.json"
echo "$(echo "$EPOCH_RESULT" | jq -r '.result // "null"' | head -c 200)"
echo ""

# ============================================
echo ""
echo "=== Test 2: Warmup (10 cold getBlock calls) ==="
echo ""

EPOCH_START=$(( EPOCH * SLOTS_PER_EPOCH ))
WARMUP_SLOTS=()
for i in $(seq 0 9); do
    WARMUP_SLOTS+=( $(( EPOCH_START + 50000 + i * 100 )) )
done

printf "%-15s %10s %10s\n" "SLOT" "LATENCY" "STATUS"
printf "%-15s %10s %10s\n" "----" "-------" "------"

for slot in "${WARMUP_SLOTS[@]}"; do
    start_ns=$(date +%s%N)
    result=$(get_block "$slot" 120 2>/dev/null) || result='{"error":"timeout"}'
    end_ns=$(date +%s%N)
    elapsed_ms=$(( (end_ns - start_ns) / 1000000 ))

    if echo "$result" | jq -e '.result' >/dev/null 2>&1; then
        printf "%-15s %7dms %10s\n" "$slot" "$elapsed_ms" "OK"
    else
        err=$(echo "$result" | jq -r '.error.message // .error // "unknown"' 2>/dev/null)
        printf "%-15s %7dms %10s\n" "$slot" "$elapsed_ms" "FAIL: $err"
    fi
done

echo ""
echo "(These calls load indexes into memory — subsequent calls should be faster)"
echo ""

# ============================================
echo ""
echo "=== Test 3: getBlock Latency (post-warmup, 5 calls) ==="
echo ""

POST_WARMUP_SLOTS=(
    $(( EPOCH_START + 10000 ))
    $(( EPOCH_START + 100000 ))
    $(( EPOCH_START + 200000 ))
    $(( EPOCH_START + 300000 ))
    $(( EPOCH_START + 400000 ))
)

printf "%-15s %10s %10s\n" "SLOT" "LATENCY" "STATUS"
printf "%-15s %10s %10s\n" "----" "-------" "------"

TOTAL_MS=0
SUCCESS=0
FAIL=0

for slot in "${POST_WARMUP_SLOTS[@]}"; do
    start_ns=$(date +%s%N)
    result=$(get_block "$slot" 120 2>/dev/null) || result='{"error":"timeout"}'
    end_ns=$(date +%s%N)
    elapsed_ms=$(( (end_ns - start_ns) / 1000000 ))

    if echo "$result" | jq -e '.result' >/dev/null 2>&1; then
        printf "%-15s %7dms %10s\n" "$slot" "$elapsed_ms" "OK"
        TOTAL_MS=$(( TOTAL_MS + elapsed_ms ))
        SUCCESS=$(( SUCCESS + 1 ))
        echo "$result" > "$OUTPUT_DIR/block-${slot}.json"
    else
        err=$(echo "$result" | jq -r '.error.message // .error // "unknown"' 2>/dev/null)
        printf "%-15s %7dms %10s\n" "$slot" "$elapsed_ms" "FAIL: $err"
        FAIL=$(( FAIL + 1 ))
    fi
done

echo ""
if [ "$SUCCESS" -gt 0 ]; then
    AVG_MS=$(( TOTAL_MS / SUCCESS ))
    echo "Average getBlock latency (post-warmup): ${AVG_MS}ms (${SUCCESS}/${#POST_WARMUP_SLOTS[@]} succeeded)"
else
    echo "All getBlock calls failed"
fi

# ============================================
echo ""
echo "=== Test 4: getBlock Throughput (sequential, 30 blocks) ==="
echo ""

TEST_COUNT=30
START_SLOT=$(( EPOCH_START + 50000 ))
echo "Fetching $TEST_COUNT blocks sequentially starting from slot $START_SLOT..."

SEQ_SUCCESS=0
SEQ_FAIL=0
SEQ_TOTAL_MS=0
SEQ_LATENCIES=""

start_total=$(date +%s%N)
for i in $(seq 0 $(( TEST_COUNT - 1 ))); do
    slot=$(( START_SLOT + i * 100 ))
    start_ns=$(date +%s%N)
    result=$(get_block "$slot" 60 2>/dev/null) || result='{"error":"timeout"}'
    end_ns=$(date +%s%N)
    elapsed_ms=$(( (end_ns - start_ns) / 1000000 ))

    if echo "$result" | jq -e '.result' >/dev/null 2>&1; then
        SEQ_SUCCESS=$(( SEQ_SUCCESS + 1 ))
        SEQ_TOTAL_MS=$(( SEQ_TOTAL_MS + elapsed_ms ))
        SEQ_LATENCIES="${SEQ_LATENCIES}${elapsed_ms} "
    else
        SEQ_FAIL=$(( SEQ_FAIL + 1 ))
    fi
done
end_total=$(date +%s%N)

total_elapsed_ms=$(( (end_total - start_total) / 1000000 ))
echo ""
echo "  Results:"
echo "    Total time: ${total_elapsed_ms}ms"
echo "    Success: $SEQ_SUCCESS / $TEST_COUNT"
echo "    Failed: $SEQ_FAIL / $TEST_COUNT"
if [ "$SEQ_SUCCESS" -gt 0 ]; then
    AVG_SEQ=$(( SEQ_TOTAL_MS / SEQ_SUCCESS ))
    RPS=$(( SEQ_SUCCESS * 1000 / total_elapsed_ms ))
    echo "    Avg latency: ${AVG_SEQ}ms"
    echo "    Throughput: ${RPS} blocks/sec"
    echo "    Per-block latencies: $SEQ_LATENCIES"
fi

# ============================================
echo ""
echo "=== Test 5: Concurrent Throughput (5 streams × 10 blocks) ==="
echo ""

CONCURRENT=5
BLOCKS_PER=10
echo "Running $CONCURRENT concurrent streams, $BLOCKS_PER blocks each..."

CONC_DIR=$(mktemp -d)
CONC_RESULTS=()

start_total=$(date +%s%N)
for c in $(seq 1 $CONCURRENT); do
    (
        stream_success=0
        stream_fail=0
        stream_latencies=""
        for i in $(seq 0 $(( BLOCKS_PER - 1 ))); do
            slot=$(( EPOCH_START + 60000 + c * 10000 + i * 100 ))
            start_ns=$(date +%s%N)
            result=$(get_block "$slot" 60 2>/dev/null) || result='{"error":"timeout"}'
            end_ns=$(date +%s%N)
            elapsed_ms=$(( (end_ns - start_ns) / 1000000 ))

            if echo "$result" | jq -e '.result' >/dev/null 2>&1; then
                stream_success=$(( stream_success + 1 ))
                stream_latencies="${stream_latencies}${elapsed_ms} "
            else
                stream_fail=$(( stream_fail + 1 ))
            fi
        done
        echo "${stream_success} ${stream_fail} ${stream_latencies}" > "$CONC_DIR/stream-${c}.txt"
    ) &
done
wait
end_total=$(date +%s%N)

total_elapsed_ms=$(( (end_total - start_total) / 1000000 ))
total_success=0
total_fail=0
all_latencies=""

for c in $(seq 1 $CONCURRENT); do
    read s f lats < "$CONC_DIR/stream-${c}.txt"
    total_success=$(( total_success + s ))
    total_fail=$(( total_fail + f ))
    all_latencies="${all_latencies}${lats}"
done
rm -rf "$CONC_DIR"

total_blocks=$(( CONCURRENT * BLOCKS_PER ))
echo ""
echo "  Results:"
echo "    Total time: ${total_elapsed_ms}ms"
echo "    Success: $total_success / $total_blocks"
echo "    Failed: $total_fail / $total_blocks"
if [ "$total_success" -gt 0 ]; then
    RPS=$(( total_success * 1000 / total_elapsed_ms ))
    echo "    Throughput: ${RPS} blocks/sec (concurrent=$CONCURRENT)"
    echo "    Per-block latencies: $all_latencies"
fi

# ============================================
echo ""
echo "=== Test 6: getTransaction ==="
echo ""

BLOCK_FILE=$(ls "$OUTPUT_DIR"/block-*.json 2>/dev/null | head -1)
if [ -n "$BLOCK_FILE" ]; then
    FIRST_TX=$(jq -r '.result.transactions[0].transaction.signatures[0] // empty' "$BLOCK_FILE" 2>/dev/null)
    if [ -n "$FIRST_TX" ]; then
        echo "Testing getTransaction with signature: $FIRST_TX"
        TX_RESULT=$(time_rpc "getTransaction" "[\"$FIRST_TX\",{\"encoding\":\"json\"}]" 60)
        echo "  Latency: ${TX_RESULT}ms"
    else
        echo "  No transactions found in test block"
    fi
else
    echo "  No block data available"
fi

# ============================================
echo ""
echo "=== Test 7: getSignaturesForAddress ==="
echo ""

SOL_MINT="So11111111111111111111111111111111111111112"
echo "Testing getSignaturesForAddress for SOL mint (limit=10)..."
SIG_RESULT=$(time_rpc "getSignaturesForAddress" "[\"$SOL_MINT\",{\"limit\":10}]" 60)
echo "  Latency: ${SIG_RESULT}ms"

# ============================================
echo ""
echo "============================================"
echo "  SUMMARY"
echo "============================================"
echo "Mode: $([[ "$ENDPOINT" == *"localhost"* ]] && echo "LOCAL" || echo "REMOTE")"
echo "Endpoint: $ENDPOINT"
echo "Epoch: $EPOCH"
echo ""
echo "Results saved to: $OUTPUT_DIR"
echo "============================================"
