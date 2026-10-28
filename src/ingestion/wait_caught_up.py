#!/usr/bin/env python3
"""
src/ingestion/wait_caught_up.py — block until the stream has drained Kafka.

"Caught up" means:
  • Kafka lag on flows.raw is 0: the bronze query's end offsets (recorded by the
    metrics listener) equal every partition's high-watermark, and
  • silver processed data after bronze's last data batch, and neither has seen
    new rows for a quiet period (Spark 3.5 reports idle triggers via onQueryIdle,
    not as empty progress events, so "quiet" is measured in wall-clock time).

Used between `make backfill` and `make train` so the model trains on the complete backfill.
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
import time
from datetime import datetime, timedelta, timezone

from confluent_kafka import Consumer, TopicPartition

from src.common.config import kafka_bootstrap, topic
from src.serving.sqlite_sink import db_path


def high_watermarks() -> dict[int, int]:
    c = Consumer({"bootstrap.servers": kafka_bootstrap(), "group.id": "wait-caught-up",
                  "enable.auto.commit": False})
    try:
        parts = c.list_topics(topic("raw"), timeout=10).topics[topic("raw")].partitions
        return {p: c.get_watermark_offsets(TopicPartition(topic("raw"), p), timeout=10)[1] for p in parts}
    finally:
        c.close()


def last_data_batch(conn, query: str) -> tuple | None:
    """(ts, end_offsets) of the query's most recent micro-batch that had input rows."""
    return conn.execute("SELECT ts, end_offsets FROM stream_metrics WHERE query_name = ? AND num_input_rows > 0 "
                        "ORDER BY ts DESC LIMIT 1", (query,)).fetchone()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--timeout", type=int, default=3600)
    ap.add_argument("--quiet-seconds", type=int, default=30)
    args = ap.parse_args()

    deadline = time.time() + args.timeout
    while time.time() < deadline:
        hw = high_watermarks()
        try:
            with sqlite3.connect(f"file:{db_path()}?mode=ro", uri=True, timeout=5) as conn:
                bronze = last_data_batch(conn, "q1_bronze")
                silver = last_data_batch(conn, "q2_silver")
        except sqlite3.OperationalError:
            bronze = silver = None
        consumed = json.loads(bronze[1]).get(topic("raw"), {}) if bronze and bronze[1] else {}
        lag = sum(h - int(consumed.get(str(p), 0)) for p, h in hw.items())
        now = datetime.now(timezone.utc)
        quiet = now - timedelta(seconds=args.quiet_seconds)
        idle = (bronze is not None and silver is not None
                and silver[0] >= bronze[0]
                and datetime.fromisoformat(silver[0].replace("Z", "+00:00")) < quiet)
        print(f"  lag={lag:>10,}   silver caught up & quiet={idle}", end="\r", flush=True)
        if lag == 0 and idle:
            print(f"\nCaught up: {sum(hw.values()):,} messages consumed, silver idle.")
            return
        time.sleep(5)
    print("\nTimed out waiting for the stream to catch up.")
    sys.exit(1)


if __name__ == "__main__":
    main()
