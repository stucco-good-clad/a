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

# --- Helper: time a curl call ---
rpc_call() {
    local method=$1
    local params=$2
    local timeout=${3:-30}
    curl -sf --max-time "$timeout" -X POST "$ENDPOINT" \
        -H "Content-Type: application/json" \
        -d "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"$method\",\"params\":$params}"
}

time_rpc() {
    local method=$1
    local params=$2
    local timeout=${3:-30}
    local start_ns
    start_ns=$(date +%s%N)
    local result
    result=$(rpc_call "$method" "$params" "$timeout" 2>/dev/null) || { echo "ERROR"; return 1; }
    local end_ns
    end_ns=$(date +%s%N)
    local elapsed_ms=$(( (end_ns - start_ns) / 1000000 ))
    echo "$elapsed_ms"
}

# ============================================
echo "=== Test 1: Basic RPC Methods ==="
echo ""

echo -n "getVersion: "
time_rpc "getVersion" "[]" > /dev/null
echo ""

echo -n "getSlot: "
SLOT_RESULT=$(rpc_call "getSlot" "[]")
echo "$SLOT_RESULT" > "$OUTPUT_DIR/getSlot.json"
CURRENT_SLOT=$(echo "$SLOT_RESULT" | jq -r '.result // "null"')
echo "  result: $CURRENT_SLOT"
echo ""

echo -n "getFirstAvailableBlock: "
time_rpc "getFirstAvailableBlock" "[]" > /dev/null
echo ""

echo -n "getEpochInfo: "
EPOCH_RESULT=$(rpc_call "getEpochInfo" "[]")
echo "$EPOCH_RESULT" > "$OUTPUT_DIR/getEpochInfo.json"
echo "  result: $(echo "$EPOCH_RESULT" | jq -r '.result // "null"' | head -c 200)"
echo ""

# ============================================
echo ""
echo "=== Test 2: getBlock Latency (single calls) ==="
echo ""

# Pick 5 slots spread across the epoch
EPOCH_START=$(( EPOCH * SLOTS_PER_EPOCH ))
SLOTS_TO_TEST=(
    $(( EPOCH_START + 10000 ))
    $(( EPOCH_START + 100000 ))
    $(( EPOCH_START + 200000 ))
    $(( EPOCH_START + 300000 ))
    $(( EPOCH_START + 400000 ))
)

echo "Testing slots: ${SLOTS_TO_TEST[*]}"
echo ""
printf "%-15s %10s %10s\n" "SLOT" "LATENCY" "STATUS"
printf "%-15s %10s %10s\n" "----" "-------" "------"

TOTAL_MS=0
SUCCESS=0
FAIL=0

for slot in "${SLOTS_TO_TEST[@]}"; do
    start_ns=$(date +%s%N)
    result=$(curl -sf --max-time 60 -X POST "$ENDPOINT" \
        -H "Content-Type: application/json" \
        -d "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"getBlock\",\"params\":[$slot,{\"encoding\":\"json\",\"transactionDetails\":\"none\",\"rewards\":false,\"maxSupportedTransactionVersion\":0}]}" 2>/dev/null) || result='{"error":"timeout"}'
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
    echo "Average getBlock latency: ${AVG_MS}ms (${SUCCESS}/${#SLOTS_TO_TEST[@]} succeeded)"
else
    echo "All getBlock calls failed"
fi

# ============================================
echo ""
echo "=== Test 3: getBlock Throughput (sequential) ==="
echo ""

TEST_COUNT=20
START_SLOT=$(( EPOCH_START + 50000 ))
echo "Fetching $TEST_COUNT blocks sequentially starting from slot $START_SLOT..."

start_total=$(date +%s%N)
for i in $(seq 0 $(( TEST_COUNT - 1 ))); do
    slot=$(( START_SLOT + i * 1000 ))
    curl -sf --max-time 60 -X POST "$ENDPOINT" \
        -H "Content-Type: application/json" \
        -d "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"getBlock\",\"params\":[$slot,{\"encoding\":\"json\",\"transactionDetails\":\"none\",\"rewards\":false,\"maxSupportedTransactionVersion\":0}]" \
        -o /dev/null 2>/dev/null || true
done
end_total=$(date +%s%N)

total_elapsed_ms=$(( (end_total - start_total) / 1000000 ))
if [ "$total_elapsed_ms" -gt 0 ]; then
    rps=$(( TEST_COUNT * 1000 / total_elapsed_ms ))
    echo "Result: $TEST_COUNT blocks in ${total_elapsed_ms}ms = ${rps} blocks/sec"
else
    echo "Result: Measurement too fast to capture"
fi

# ============================================
echo ""
echo "=== Test 4: Concurrent Throughput ==="
echo ""

CONCURRENT=5
BLOCKS_PER=$(( TEST_COUNT / CONCURRENT ))
echo "Running $CONCURRENT concurrent streams, $BLOCKS_PER blocks each..."

start_total=$(date +%s%N)
for c in $(seq 1 $CONCURRENT); do
    (
        for i in $(seq 0 $(( BLOCKS_PER - 1 ))); do
            slot=$(( START_SLOT + c * 50000 + i * 1000 ))
            curl -sf --max-time 60 -X POST "$ENDPOINT" \
                -H "Content-Type: application/json" \
                -d "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"getBlock\",\"params\":[$slot,{\"encoding\":\"json\",\"transactionDetails\":\"none\",\"rewards\":false,\"maxSupportedTransactionVersion\":0}]" \
                -o /dev/null 2>/dev/null || true
        done
    ) &
done
wait
end_total=$(date +%s%N)

total_elapsed_ms=$(( (end_total - start_total) / 1000000 ))
total_blocks=$(( CONCURRENT * BLOCKS_PER ))
if [ "$total_elapsed_ms" -gt 0 ]; then
    rps=$(( total_blocks * 1000 / total_elapsed_ms ))
    echo "Result: $total_blocks blocks in ${total_elapsed_ms}ms = ${rps} blocks/sec (concurrent=$CONCURRENT)"
fi

# ============================================
echo ""
echo "=== Test 5: getTransaction ==="
echo ""

# Find a slot with transactions from the first block test
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
echo "=== Test 6: getSignaturesForAddress ==="
echo ""

# SOL mint
SOL_MINT="So11111111111111111111111111111111111111112"
echo "Testing getSignaturesForAddress for SOL mint..."
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
