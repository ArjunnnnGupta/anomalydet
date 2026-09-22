#!/usr/bin/env python3
"""
src/storage/write_alerts.py — Phase 5: Write Scored Results to SQLite

Reads the IF scores/flags and RF predictions from Phase 4 and writes them
into alerts.db (SQLite). Creates the schema if it doesn't exist yet.

After running, verify with:
    sqlite3 alerts.db "SELECT COUNT(*) FROM flow_scores;"
    sqlite3 alerts.db "SELECT COUNT(*) FROM alerts;"
    sqlite3 alerts.db "SELECT * FROM alerts LIMIT 5;"

Run: python src/storage/write_alerts.py
"""

import os
import sys
import sqlite3
import numpy as np
import joblib
from datetime import datetime, timezone

# ── Config ────────────────────────────────────────────────────────────────────
DB_PATH         = "alerts.db"
SCHEMA_PATH     = "src/storage/db_schema.sql"
Y_LABELS_PATH   = "data/y_labels.npy"
IF_SCORES_PATH  = "data/if_scores.npy"
IF_FLAGS_PATH   = "data/if_flags.npy"
RF_PREDS_PATH   = "data/rf_predictions.npy"
ENCODER_PATH    = "models/label_encoder.joblib"

# ── Helpers ───────────────────────────────────────────────────────────────────
def check(path):
    if not os.path.exists(path):
        print(f"\nERROR: {path} not found.")
        sys.exit(1)

def get_conn():
    conn = sqlite3.connect(DB_PATH)
    with open(SCHEMA_PATH) as f:
        conn.executescript(f.read())
    conn.commit()
    return conn

# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    for p in [Y_LABELS_PATH, IF_SCORES_PATH, IF_FLAGS_PATH, RF_PREDS_PATH, ENCODER_PATH]:
        check(p)

    print(f"\n{'='*60}")
    print("Phase 5 — Writing Alerts to SQLite")
    print(f"{'='*60}")

    print(f"\n[1/4] Loading scored data ...")
    y_raw      = np.load(Y_LABELS_PATH, allow_pickle=True)
    if_scores  = np.load(IF_SCORES_PATH)
    if_flags   = np.load(IF_FLAGS_PATH)
    rf_preds   = np.load(RF_PREDS_PATH)
    le         = joblib.load(ENCODER_PATH)

    # RF predictions cover the test split only — pad the rest with None
    # so indices align with the full IF-scored sample.
    n_total = len(if_scores)
    n_rf    = len(rf_preds)
    rf_labels_full = [None] * n_total
    # RF preds correspond to the last n_rf rows of the dataset (test split)
    # This is an approximation — in production you'd save test indices.
    rf_labels_full[-n_rf:] = [le.inverse_transform([p])[0] for p in rf_preds]

    print(f"      Total flows: {n_total:,}  |  RF predictions: {n_rf:,}")

    print(f"\n[2/4] Connecting to SQLite at {DB_PATH} ...")
    conn = get_conn()
    cur  = conn.cursor()

    # Clear old data so re-runs don't duplicate rows
    cur.execute("DELETE FROM flow_scores")
    cur.execute("DELETE FROM alerts")
    conn.commit()

    scored_at = datetime.now(timezone.utc).isoformat()
    # Convert IF flag: sklearn -1/1 → 1/0 (1=anomaly for readability)
    is_anomaly = (if_flags == -1).astype(int)

    print(f"\n[3/4] Inserting {n_total:,} rows into flow_scores ...")
    flow_rows = [
        (
            int(i),
            float(if_scores[i]),
            int(is_anomaly[i]),
            rf_labels_full[i],
            str(y_raw[i]),
            scored_at,
        )
        for i in range(n_total)
    ]
    cur.executemany(
        "INSERT INTO flow_scores (flow_index, anomaly_score, if_flag, rf_label, ground_truth, scored_at) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        flow_rows,
    )

    print(f"[4/4] Inserting anomalous flows into alerts ...")
    alert_rows = [row for row in flow_rows if row[2] == 1]   # if_flag == 1
    cur.executemany(
        "INSERT INTO alerts (flow_index, anomaly_score, rf_label, ground_truth, scored_at) "
        "VALUES (?, ?, ?, ?, ?)",
        [(r[0], r[1], r[3], r[4], r[5]) for r in alert_rows],
    )
    conn.commit()
    conn.close()

    print(f"\n{'='*60}")
    print("Phase 5 complete.")
    print(f"  flow_scores rows: {len(flow_rows):,}")
    print(f"  alerts rows     : {len(alert_rows):,}  ({len(alert_rows)/n_total:.2%} flagged)")
    print(f"  Database        : {DB_PATH}")
    print(f"\nVerify with:")
    print(f"  sqlite3 {DB_PATH} \"SELECT ground_truth, COUNT(*) FROM alerts GROUP BY ground_truth ORDER BY 2 DESC;\"")
    print(f"{'='*60}\n")

if __name__ == "__main__":
    main()

