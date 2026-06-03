#!/bin/bash
set -euo pipefail

DATA_DIR="${DATA_DIR:-$RUNNER_TEMP/faithful-data}"
CONFIG_DIR="${CONFIG_DIR:-$RUNNER_TEMP/faithful-configs}"
LISTEN="${LISTEN:-:8899}"
OF1_BASE="https://files.old-faithful.net"
EPOCH="${EPOCH:-979}"

mkdir -p "$DATA_DIR" "$CONFIG_DIR"

echo "=== Faithful-CLI Hosted Mode ==="
echo "Data dir: $DATA_DIR"
echo "Config dir: $CONFIG_DIR"
echo "Epoch: $EPOCH"
echo "Listen: $LISTEN"
echo ""

# --- Download faithful-cli binary ---
if [ ! -f "$RUNNER_TEMP/faithful-cli" ]; then
    echo "[1/5] Downloading faithful-cli v0.7.24..."
    curl -sL -o "$RUNNER_TEMP/faithful-cli" \
        https://github.com/rpcpool/yellowstone-faithful/releases/download/v0.7.24/faithful-cli_linux_amd64
    chmod +x "$RUNNER_TEMP/faithful-cli"
else
    echo "[1/5] faithful-cli already downloaded"
fi

# --- Download CID ---
echo "[2/5] Downloading epoch metadata..."
epoch_dir="$DATA_DIR/$EPOCH"
mkdir -p "$epoch_dir"
cd "$epoch_dir"

curl -sL --max-time 30 -o "epoch-${EPOCH}.cid" \
    "$OF1_BASE/$EPOCH/epoch-${EPOCH}.cid" || { echo "ERROR: Could not download CID"; exit 1; }
CID=$(cat "epoch-${EPOCH}.cid" | tr -d '[:space:]')
echo "  CID: $CID"

# --- Download indexes with progress ---
echo "[3/5] Downloading indexes..."

download_index() {
    local name=$1
    local file=$2
    local min_size=${3:-100}
    local threshold=${4:-1048576}

    if [ -f "$file" ]; then
        local fsize
        fsize=$(stat -c%s "$file" 2>/dev/null || echo 0)
        if [ "$fsize" -ge "$min_size" ]; then
            echo "  $name: already downloaded ($(( fsize / 1048576 ))MB)"
            return 0
        fi
    fi

    local start_time
    start_time=$(date +%s)

    # Use progress bar for large files, simple output for small files
    if [ "$threshold" -ge 1048576 ]; then
        echo "  $name: downloading (progress bar)..."
        curl -sL --max-time 3600 --progress-bar -o "$file" "$OF1_BASE/$EPOCH/$file" 2>&2
    else
        echo -n "  $name: downloading..."
        curl -sL --max-time 300 -o "$file" "$OF1_BASE/$EPOCH/$file"
    fi

    local end_time
    end_time=$(date +%s)
    local elapsed=$(( end_time - start_time ))
    local fsize
    fsize=$(stat -c%s "$file" 2>/dev/null || echo 0)

    if [ "$fsize" -lt "$min_size" ]; then
        echo " FAILED (size=$fsize)"
        rm -f "$file"
        return 1
    fi

    local speed="N/A"
    if [ "$elapsed" -gt 0 ]; then
        speed="$(( fsize / elapsed / 1048576 ))MB/s"
    fi
    echo "  $name: done ($(( fsize / 1048576 ))MB in ${elapsed}s, ${speed})"
}

download_index "slot-to-cid" "epoch-${EPOCH}-${CID}-mainnet-slot-to-cid.index" 100 100
download_index "cid-to-offset-and-size" "epoch-${EPOCH}-${CID}-mainnet-cid-to-offset-and-size.index" 1048576 1048576
download_index "sig-to-cid" "epoch-${EPOCH}-${CID}-mainnet-sig-to-cid.index" 1048576 1048576
download_index "sig-exists" "epoch-${EPOCH}-${CID}-mainnet-sig-exists.index" 1048576 1048576
download_index "slot-to-blocktime" "epoch-${EPOCH}-${CID}-mainnet-slot-to-blocktime.index" 100 100

if [ ! -f "${EPOCH}.slots.txt" ]; then
    echo -n "  slots.txt: downloading..."
    curl -sL --max-time 60 -o "${EPOCH}.slots.txt" \
        "$OF1_BASE/$EPOCH/${EPOCH}.slots.txt" || true
    echo "done"
fi

# --- Generate config ---
echo "[4/5] Generating epoch config..."
cat > "$CONFIG_DIR/epoch-${EPOCH}.yaml" << EOF
epoch: ${EPOCH}
version: 1
data:
  car:
    uri: ${OF1_BASE}/${EPOCH}/epoch-${EPOCH}.car
indexes:
  cid_to_offset_and_size:
    uri: ${epoch_dir}/epoch-${EPOCH}-${CID}-mainnet-cid-to-offset-and-size.index
  slot_to_cid:
    uri: ${epoch_dir}/epoch-${EPOCH}-${CID}-mainnet-slot-to-cid.index
  sig_to_cid:
    uri: ${epoch_dir}/epoch-${EPOCH}-${CID}-mainnet-sig-to-cid.index
  sig_exists:
    uri: ${epoch_dir}/epoch-${EPOCH}-${CID}-mainnet-sig-exists.index
  slot_to_blocktime:
    uri: ${epoch_dir}/epoch-${EPOCH}-${CID}-mainnet-slot-to-blocktime.index
  blocks:
    uri: ${epoch_dir}/${EPOCH}.slots.txt
EOF
echo "  Config written to $CONFIG_DIR/epoch-${EPOCH}.yaml"

# --- Start faithful-cli ---
echo "[5/5] Starting faithful-cli on ${LISTEN}..."
"$RUNNER_TEMP/faithful-cli" rpc \
    --listen="${LISTEN}" \
    --max-cache=512 \
    --epoch-load-concurrency=2 \
    --use-mmap-for-local-indexes \
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
