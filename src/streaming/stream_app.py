#!/usr/bin/env python3
"""
src/streaming/stream_app.py — the always-on Kappa pipeline: one Spark application, five queries.

    Q1  q1_bronze           Kafka flows.raw   → Delta bronze_flows
    Q2  q2_silver           bronze (Delta)    → parse/DQ/enrich → append silver_flows (+ quarantine, DLQ)
    Q3a q3a_gold_timeline   silver (Delta)    → 1-min tumbling windows → gold_attack_timeline (Delta)
    Q3b q3b_gold_hosts      silver (Delta)    → 1-min per-host windows → gold_host_profile/port_scan + SQLite
    Q4  q4_scoring          silver live split → MLlib RF + KMeans (hot reload) → gold_scored, Kafka, SQLite

Run inside the cluster:  make stream
Restart-safe: every query resumes from its own checkpoint under data/checkpoints/.
"""

import argparse

from src.common.spark import get_spark
from src.serving import sqlite_sink
from src.streaming import bronze, gold, scoring, silver
from src.streaming.metrics_listener import MetricsListener

STAGES = ["bronze", "silver", "gold", "scoring"]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--only", nargs="*", choices=STAGES, help="start a subset of the queries")
    args = ap.parse_args()
    stages = [s for s in STAGES if s in (args.only or STAGES)]   # always upstream → downstream

    spark = get_spark("kappa-stream-app", streaming=True)
    spark.streams.addListener(MetricsListener())
    sqlite_sink.connect().close()       # create serving schema up front

    # Create empty Delta tables first, so downstream Delta-stream readers can start before data arrives
    silver.ensure_tables(spark)
    gold.ensure_tables(spark)
    scoring.ensure_tables(spark)

    starters = {"bronze": bronze.start, "silver": silver.start, "gold": gold.start,
                "scoring": scoring.start}

    queries = []
    for name in stages:
        started = starters[name](spark)
        queries.extend(started if isinstance(started, list) else [started])

    print("\nStreaming queries running:")
    for q in queries:
        print(f"  • {q.name:<22} id={q.id}")
    print("\nSpark UI → http://localhost:4040/StructuredStreaming/   (Ctrl+C to stop)\n", flush=True)
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
