#!/usr/bin/env python3
"""
src/ingestion/create_topics.py — create (or recreate) the pipeline's Kafka topics.

Partitions, replication factor, min.insync.replicas and retention come from
conf/pipeline.yaml. After creation it prints each partition's leader, replicas
and ISR so you can see the replication layout.

Usage:
    python -m src.ingestion.create_topics              # create missing topics
    python -m src.ingestion.create_topics --recreate   # delete + recreate (wipes data)
    python -m src.ingestion.create_topics --describe   # only print the layout
"""

from __future__ import annotations

import argparse
import sys
import time

from confluent_kafka.admin import AdminClient, NewTopic

from src.common.config import cfg, kafka_bootstrap, replication_factor


def describe(admin: AdminClient, names: list[str]) -> None:
    # Right after creation, metadata can lag behind on some brokers: retry for a few seconds
    for _ in range(15):
        md = admin.list_topics(timeout=10)
        if all(n in md.topics and not md.topics[n].error and md.topics[n].partitions for n in names):
            break
        time.sleep(1)
    print(f"\nBrokers: {sorted(b.id for b in md.brokers.values())}")
    for name in names:
        t = md.topics.get(name)
        if t is None or t.error:
            print(f"  {name}: MISSING")
            continue
        print(f"  {name}  ({len(t.partitions)} partitions)")
        for pid in sorted(t.partitions):
            p = t.partitions[pid]
            print(f"    p{pid:<3} leader={p.leader}  replicas={p.replicas}  isr={p.isrs}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--recreate", action="store_true")
    ap.add_argument("--describe", action="store_true")
    args = ap.parse_args()

    k = cfg()["kafka"]
    rf = replication_factor()
    admin = AdminClient({"bootstrap.servers": kafka_bootstrap()})
    names = [t["name"] for t in k["topics"].values()]

    if args.describe:
        describe(admin, names)
        return

    existing = set(admin.list_topics(timeout=10).topics)

    if args.recreate:
        doomed = [n for n in names if n in existing]
        if doomed:
            for n, f in admin.delete_topics(doomed, operation_timeout=30).items():
                f.result()
                print(f"Deleted {n}")
            time.sleep(3)  # topic deletion is asynchronous on the brokers
            existing -= set(doomed)

    new = [
        NewTopic(
            t["name"],
            num_partitions=t["partitions"],
            replication_factor=rf,
            config={
                "compression.type": "lz4",
                "retention.bytes": str(t["retention_bytes"]),
                "min.insync.replicas": str(min(k["min_insync_replicas"], rf)),
            },
        )
        for t in k["topics"].values()
        if t["name"] not in existing
    ]
    if new:
        for n, f in admin.create_topics(new, operation_timeout=30).items():
            try:
                f.result()
                print(f"Created {n}")
            except Exception as e:  # noqa: BLE001 — report and keep going
                print(f"FAILED {n}: {e}", file=sys.stderr)
    else:
        print("All topics already exist.")

    describe(admin, names)


if __name__ == "__main__":
    main()
