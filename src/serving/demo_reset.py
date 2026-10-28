#!/usr/bin/env python3
"""
src/serving/demo_reset.py — clear the results of previous live runs, for a fresh demo.

KEEPS:   the trained model (registry), the backfill data in bronze/silver, the gold windows of the
         backfill split, the evaluation metrics/plots, and every streaming checkpoint except bronze's.
CLEARS:  - live-split rows in the gold Delta tables (scored flows, attack timeline, host windows,
           port-scan candidates)
         - the dashboard's SQLite serving tables (alerts, host windows, port scans, metrics, events)
         - the bronze query's checkpoint, because `make demo-reset` also recreates the (now empty)
           Kafka topics, whose offsets start again at 0

Run with the stream stopped:   make demo-reset        (dry run: python -m src.serving.demo_reset --dry-run)
"""

import argparse
import os
import shutil
import sqlite3

from deltalake import DeltaTable

from src.common.config import checkpoint, lake
from src.serving.sqlite_sink import db_path

GOLD_LIVE = ["gold_scored", "gold_attack_timeline", "gold_host_profile", "gold_port_scan"]
SQLITE_TABLES = ["alerts", "host_window_stats", "port_scan_candidates", "stream_metrics", "pipeline_events"]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true", help="only show what would be cleared")
    args = ap.parse_args()
    verb = "would clear" if args.dry_run else "cleared"

    for name in GOLD_LIVE:
        p = lake(name)
        if not os.path.exists(p):
            continue
        dt = DeltaTable(p)
        n = sum(1 for s in dt.to_pyarrow_table(columns=["split"]).column("split").to_pylist() if s == "live")
        if n and not args.dry_run:
            dt.delete("split = 'live'")
        print(f"  Delta {name:<22} {verb} {n:>7,} live rows")

    db = db_path()
    if os.path.exists(db):
        with sqlite3.connect(db, timeout=30) as c:
            for t in SQLITE_TABLES:
                n = c.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
                if not args.dry_run:
                    c.execute(f"DELETE FROM {t}")
                print(f"  SQLite {t:<21} {verb} {n:>7,} rows")
            if not args.dry_run:
                c.execute("INSERT INTO pipeline_events VALUES (datetime('now'), 'ops', 'demo reset')")

    cp = checkpoint("q1_bronze")
    if os.path.exists(cp):
        if not args.dry_run:
            shutil.rmtree(cp)
        print(f"  checkpoint q1_bronze {verb} (Kafka topics are recreated empty)")
    print("Dry run only; nothing changed." if args.dry_run else "Demo reset done.")


if __name__ == "__main__":
    main()
