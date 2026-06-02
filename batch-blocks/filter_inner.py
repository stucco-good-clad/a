import json
import sys

def main():
    if len(sys.argv) < 3:
        print("Usage: filter_inner.py <src.json> <dst.json>")
        sys.exit(1)
    src, dst = sys.argv[1], sys.argv[2]
    with open(src, "r", encoding="utf-8") as f:
        data = json.load(f)
    txs = data.get("transactions") or []
    filtered = []
    for tx in txs:
        meta = tx.get("meta") or {}
        inner = meta.get("innerInstructions")
        if inner is not None and len(inner) == 0:
            continue
        filtered.append(tx)
    data["transactions"] = filtered
    with open(dst, "w", encoding="utf-8") as f:
        json.dump(data, f, separators=(",", ":"))

if __name__ == "__main__":
    main()
