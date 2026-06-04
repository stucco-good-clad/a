#!/usr/bin/env bash
set -euo pipefail

# Generate faithful-cli config YAML for given epochs
# Usage: ./generate-configs.sh 800 801 802

OUTPUT_DIR="${CONFIGS_DIR:-./configs}"
mkdir -p "$OUTPUT_DIR"

for EPOCH in "$@"; do
  echo "Generating config for epoch $EPOCH..."

  CID=$(curl -sf "https://files.old-faithful.net/${EPOCH}/epoch-${EPOCH}.cid" | tr -d '[:space:]')
  if [ -z "$CID" ]; then
    echo "ERROR: Could not fetch CID for epoch $EPOCH" >&2
    exit 1
  fi

  BASE="https://files.old-faithful.net/${EPOCH}/epoch-${EPOCH}"

  cat > "$OUTPUT_DIR/epoch-${EPOCH}.yml" <<EOF
version: 1
epoch: ${EPOCH}
data:
  car:
    uri: ${BASE}.car
indexes:
  cid_to_offset_and_size:
    uri: ${BASE}-${CID}-mainnet-cid-to-offset-and-size.index
  slot_to_cid:
    uri: ${BASE}-${CID}-mainnet-slot-to-cid.index
  sig_to_cid:
    uri: ${BASE}-${CID}-mainnet-sig-to-cid.index
  sig_exists:
    uri: ${BASE}-${CID}-mainnet-sig-exists.index
EOF

  echo "  CID: $CID"
  echo "  Written: $OUTPUT_DIR/epoch-${EPOCH}.yml"
done

echo "Done. Generated $(ls "$OUTPUT_DIR"/epoch-*.yml 2>/dev/null | wc -l) config(s)."
