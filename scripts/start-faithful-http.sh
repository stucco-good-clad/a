#!/bin/bash
set -euo pipefail

CONFIG_DIR="${CONFIG_DIR:-$RUNNER_TEMP/faithful-configs}"
LISTEN="${LISTEN:-:8899}"
OF1_BASE="https://files.old-faithful.net"
EPOCH="${EPOCH:-979}"

mkdir -p "$CONFIG_DIR"

echo "=== Faithful-CLI HTTP Mode ==="
echo "Config dir: $CONFIG_DIR"
echo "Epoch: $EPOCH"
echo "Listen: $LISTEN"
echo ""

# --- Download faithful-cli binary ---
if [ ! -f "$RUNNER_TEMP/faithful-cli" ]; then
    echo "[1/3] Downloading faithful-cli v0.7.24..."
    curl -sL -o "$RUNNER_TEMP/faithful-cli" \
        https://github.com/rpcpool/yellowstone-faithful/releases/download/v0.7.24/faithful-cli_linux_amd64
    chmod +x "$RUNNER_TEMP/faithful-cli"
else
    echo "[1/3] faithful-cli already downloaded"
fi

# --- Get CID (only small file we download) ---
echo "[2/3] Fetching epoch CID..."
CID=$(curl -sL --max-time 30 "$OF1_BASE/$EPOCH/epoch-${EPOCH}.cid" | tr -d '[:space:]')
if [ -z "$CID" ]; then
    echo "ERROR: Could not fetch CID"
    exit 1
fi
echo "  CID: $CID"

# --- Generate config with HTTP URIs ---
echo "[3/3] Generating HTTP config..."

# Also download slots.txt (small, needed for getBlocks)
curl -sL --max-time 60 -o "$CONFIG_DIR/${EPOCH}.slots.txt" \
    "$OF1_BASE/$EPOCH/${EPOCH}.slots.txt" || true

# Download .cid file locally (needed for config generation)
echo "$CID" > "$CONFIG_DIR/epoch-${EPOCH}.cid"

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
    uri: ${OF1_BASE}/${EPOCH}/epoch-${EPOCH}-${CID}-mainnet-slot-to-cid.index
  sig_to_cid:
    uri: ${OF1_BASE}/${EPOCH}/epoch-${EPOCH}-${CID}-mainnet-sig-to-cid.index
  sig_exists:
    uri: ${OF1_BASE}/${EPOCH}/epoch-${EPOCH}-${CID}-mainnet-sig-exists.index
  slot_to_blocktime:
    uri: ${OF1_BASE}/${EPOCH}/epoch-${EPOCH}-${CID}-mainnet-slot-to-blocktime.index
  blocks:
    uri: ${CONFIG_DIR}/${EPOCH}.slots.txt
EOF
echo "  Config written to $CONFIG_DIR/epoch-${EPOCH}.yaml"
echo "  All index URIs point to files.old-faithful.net (no local storage)"

# --- Start faithful-cli ---
echo ""
echo "Starting faithful-cli on ${LISTEN}..."
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
for i in $(seq 1 60); do
    if curl -sf -o /dev/null -X POST "http://localhost${LISTEN}" \
        -H "Content-Type: application/json" \
        -d '{"jsonrpc":"2.0","id":1,"method":"getVersion"}' 2>/dev/null; then
        echo "  faithful-cli is READY"
        break
    fi
    if [ "$i" -eq 60 ]; then
        echo "  WARNING: Timed out waiting for port"
    fi
    sleep 1
done

echo ""
echo "=== Done ==="
echo "PID: $FAITHFUL_PID"
echo "Endpoint: http://localhost${LISTEN}"
echo "NOTE: Epochs may take time to load from HTTP (no local indexes)"
