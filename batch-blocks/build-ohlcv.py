import glob
import json
import csv
import os
import sys

HEADER = ["slot", "blockTime", "tx_count", "first_tx_sig", "last_tx_sig"]

def main():
    root = os.path.dirname(os.path.abspath(__file__))
    files = sorted(glob.glob(os.path.join(root, "filtered", "*.txt")))
    if not files:
        print("No filtered block files found in", os.path.join(root, "filtered"))
        sys.exit(0)

    out_csv = os.path.join(root, "ohlcv.csv")
    rows = []
    for path in files:
        slot = int(os.path.splitext(os.path.basename(path))[0])
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        block_time = data.get("blockTime")
        transactions = data.get("transactions") or []
        tx_count = len(transactions)
        first_sig = ""
        last_sig = ""
        if transactions:
            first_sig = transactions[0].get("transaction", {}).get("signatures", [""])[0]
            last_sig = transactions[-1].get("transaction", {}).get("signatures", [""])[0]
        rows.append({
            "slot": slot,
            "blockTime": block_time,
            "tx_count": tx_count,
            "first_tx_sig": first_sig,
            "last_tx_sig": last_sig,
        })

    write_header = not os.path.exists(out_csv) or os.path.getsize(out_csv) == 0
    with open(out_csv, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=HEADER)
        if write_header:
            writer.writeheader()
        writer.writerows(rows)

    print(f"Appended {len(rows)} rows to {out_csv}")

if __name__ == "__main__":
    main()
