#!/bin/bash
set -euo pipefail

DATA_DIR="${DATA_DIR:-$RUNNER_TEMP/faithful-data}"
CONFIG_DIR="${CONFIG_DIR:-$RUNNER_TEMP/faithful-configs}"
LISTEN="${LISTEN:-:8899}"
OF1_BASE="https://files.old-faithful.net"
EPOCH="${EPOCH:-979}"

mkdir -p "$DATA_DIR" "$CONFIG_DIR"

echo "=== Faithful-CLI HTTP Mode (Hybrid) ==="
echo "Data dir: $DATA_DIR"
echo "Config dir: $CONFIG_DIR"
echo "Epoch: $EPOCH"
echo "Listen: $LISTEN"
echo ""
echo "Strategy:"
echo "  - Small indexes (slot-to-cid, slot-to-blocktime) → downloaded locally (~18MB)"
echo "  - Large indexes (cid-to-offset, sig-to-cid, sig-exists) → HTTP from OF1"
echo "  - CAR file → HTTP range requests from OF1"
echo ""

# --- Download faithful-cli binary ---
if [ ! -f "$RUNNER_TEMP/faithful-cli" ]; then
    echo "[1/4] Downloading faithful-cli v0.7.24..."
    curl -sL -o "$RUNNER_TEMP/faithful-cli" \
        https://github.com/rpcpool/yellowstone-faithful/releases/download/v0.7.24/faithful-cli_linux_amd64
    chmod +x "$RUNNER_TEMP/faithful-cli"
else
    echo "[1/4] faithful-cli already downloaded"
fi

# --- Get CID and download small indexes ---
echo "[2/4] Fetching epoch metadata and small indexes..."
epoch_dir="$DATA_DIR/$EPOCH"
mkdir -p "$epoch_dir"
cd "$epoch_dir"

# CID
curl -sL --max-time 30 -o "epoch-${EPOCH}.cid" \
    "$OF1_BASE/$EPOCH/epoch-${EPOCH}.cid" || { echo "ERROR: Could not fetch CID"; exit 1; }
CID=$(cat "epoch-${EPOCH}.cid" | tr -d '[:space:]')
echo "  CID: $CID"

# slot-to-cid (~16MB) — download locally
S2C_FILE="epoch-${EPOCH}-${CID}-mainnet-slot-to-cid.index"
if [ ! -f "$S2C_FILE" ] || [ "$(stat -c%s "$S2C_FILE" 2>/dev/null || echo 0)" -lt 100 ]; then
    echo -n "  slot-to-cid (~16MB): "
    start_time=$(date +%s)
    curl -sL --max-time 120 -o "$S2C_FILE" "$OF1_BASE/$EPOCH/$S2C_FILE"
    end_time=$(date +%s)
    elapsed=$(( end_time - start_time ))
    fsize=$(stat -c%s "$S2C_FILE" 2>/dev/null || echo 0)
    echo "done ($(( fsize / 1048576 ))MB in ${elapsed}s)"
else
    echo "  slot-to-cid: already downloaded"
fi

# slot-to-blocktime (~1.7MB) — download locally
S2B_FILE="epoch-${EPOCH}-${CID}-mainnet-slot-to-blocktime.index"
if [ ! -f "$S2B_FILE" ] || [ "$(stat -c%s "$S2B_FILE" 2>/dev/null || echo 0)" -lt 100 ]; then
    echo -n "  slot-to-blocktime (~2MB): "
    curl -sL --max-time 60 -o "$S2B_FILE" "$OF1_BASE/$EPOCH/$S2B_FILE"
    echo "done"
else
    echo "  slot-to-blocktime: already downloaded"
fi

# slots.txt (~2MB) — needed for getBlocks
if [ ! -f "${EPOCH}.slots.txt" ]; then
    curl -sL --max-time 60 -o "${EPOCH}.slots.txt" \
        "$OF1_BASE/$EPOCH/${EPOCH}.slots.txt" || true
fi

echo ""
echo "  Local indexes total: $(( $(stat -c%s "$S2C_FILE" 2>/dev/null || echo 0) + $(stat -c%s "$S2B_FILE" 2>/dev/null || echo 0) )) bytes"
echo ""

# --- Generate config with hybrid URIs ---
echo "[3/4] Generating config (hybrid: local small + HTTP large)..."

cat > "$CONFIG_DIR/epoch-${EPOCH}.yaml" << EOF
epoch: ${EPOCH}
version: 1
data:
  car:
    uri: ${OF1_BASE}/${EPOCH}/epoch-${EPOCH}.car
indexes:
  cid_to_offset_and_size:
    uri: ${OF1_BASE}/${EPOCH}/epoch-${EPOCH}-${CID}-mainnet-cid-to-offset-and-size.index
  slot_to_cid:
    uri: ${epoch_dir}/epoch-${EPOCH}-${CID}-mainnet-slot-to-cid.index
  sig_to_cid:
    uri: ${OF1_BASE}/${EPOCH}/epoch-${EPOCH}-${CID}-mainnet-sig-to-cid.index
  sig_exists:
    uri: ${OF1_BASE}/${EPOCH}/epoch-${EPOCH}-${CID}-mainnet-sig-exists.index
  slot_to_blocktime:
    uri: ${epoch_dir}/epoch-${EPOCH}-${CID}-mainnet-slot-to-blocktime.index
  blocks:
    uri: ${epoch_dir}/${EPOCH}.slots.txt
EOF
echo "  Config written to $CONFIG_DIR/epoch-${EPOCH}.yaml"
echo "  slot_to_cid → local (${epoch_dir})"
echo "  slot_to_blocktime → local (${epoch_dir})"
echo "  cid_to_offset_and_size → HTTP (OF1)"
echo "  sig_to_cid → HTTP (OF1)"
echo "  sig_exists → HTTP (OF1)"

# --- Start faithful-cli ---
echo ""
echo "[4/4] Starting faithful-cli on ${LISTEN}..."
"$RUNNER_TEMP/faithful-cli" rpc \
    --listen="${LISTEN}" \
    --max-cache=512 \
    --epoch-load-concurrency=2 \
    --watch \
    "$CONFIG_DIR" &
FAITHFUL_PID=$!

echo "$FAITHFUL_PID" > "$RUNNER_TEMP/faithful.pid"
echo "  PID: $FAITHFUL_PID"

# Wait for port to be ready
echo "  Waiting for port to be ready..."
for i in $(seq 1 30); do
    if curl -sf -o /dev/null -X POST "http://localhost${LISTEN}" \
        -H "Content-Type: application/json" \
        -d '{"jsonrpc":"2.0","id":1,"method":"getVersion"}' 2>/dev/null; then
        echo "  faithful-cli is READY"
        break
    fi
    sleep 1
done

# --- Disk usage ---
echo ""
echo "=== Disk Usage ==="
du -sh "$DATA_DIR"/* 2>/dev/null || true
echo ""
df -h / /tmp 2>/dev/null || true
echo ""
echo "=== Done ==="
echo "PID: $FAITHFUL_PID"
echo "Endpoint: http://localhost${LISTEN}"
echo ""
echo "NOTE: Large indexes (cid-to-offset ~8.5GB, sig-to-cid ~18GB) are fetched"
echo "via HTTP on first access, then cached in memory. First few getBlock"
echo "calls will be slow (~10-15s) while indexes load. Subsequent calls"
echo "should be fast (~100-500ms)."
