#!/usr/bin/env python3
"""
src/ingestion/kafka_producer.py — Phase 2: Simulated Streaming Ingestion

Reads the cleaned Parquet data and publishes rows to the 'network-flows'
Kafka topic as JSON messages, simulating a live network flow feed.

Usage:
    python src/ingestion/kafka_producer.py              # stream all rows
    python src/ingestion/kafka_producer.py --sample 5000  # stream only 5000 rows (for testing)

Each "batch" of BATCH_SIZE rows is sent, then the producer sleeps for SLEEP_SECONDS.
This simulates new flows arriving from a network tap or sensor.
"""

import sys
import os
import json
import time
import argparse

import pandas as pd
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# ── Config ────────────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP     = "localhost:9092"
TOPIC               = "network-flows"
CLEAN_PARQUET_PATH  = "data/clean_parquet"
BATCH_SIZE          = 50       # rows per send batch
SLEEP_SECONDS       = 0.5      # pause between batches (adjust to taste)
LABEL_COLUMN        = "Attack Type"

# ── Helpers ───────────────────────────────────────────────────────────────────
def check_input(path):
    if not os.path.exists(path):
        print(f"\nERROR: Cleaned Parquet not found at '{path}'.")
        print("Run Phase 1 first: python src/processing/clean_data.py")
        sys.exit(1)

def make_producer():
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            acks="all",
            retries=3,
        )
        return producer
    except NoBrokersAvailable:
        print(f"\nERROR: Cannot connect to Kafka at {KAFKA_BOOTSTRAP}.")
        print("Start Kafka first: bash scripts/start_kafka.sh && bash scripts/create_topic.sh")
        sys.exit(1)

# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Kafka producer: stream flow rows to Kafka")
    parser.add_argument("--sample", type=int, default=None,
                        help="If set, only stream this many rows (useful for quick testing)")
    args = parser.parse_args()

    check_input(CLEAN_PARQUET_PATH)

    print(f"\n{'='*60}")
    print("Phase 2 — Kafka Producer")
    print(f"  Topic   : {TOPIC}")
    print(f"  Broker  : {KAFKA_BOOTSTRAP}")
    print(f"  Batch   : {BATCH_SIZE} rows / {SLEEP_SECONDS}s")
    print(f"{'='*60}")

    # Load cleaned data into Pandas (Parquet is already small enough after cleaning)
    print(f"\n[1/3] Loading cleaned Parquet from {CLEAN_PARQUET_PATH} ...")
    df = pd.read_parquet(CLEAN_PARQUET_PATH)

    if args.sample:
        df = df.sample(n=min(args.sample, len(df)), random_state=42).reset_index(drop=True)
        print(f"      Sampling {len(df):,} rows (--sample {args.sample})")
    else:
        print(f"      Loaded {len(df):,} rows")

    print(f"\n[2/3] Connecting to Kafka ...")
    producer = make_producer()
    print(f"      Connected.")

    print(f"\n[3/3] Publishing to topic '{TOPIC}' ...")
    total_sent = 0
    start_time = time.time()

    for batch_start in range(0, len(df), BATCH_SIZE):
        batch = df.iloc[batch_start : batch_start + BATCH_SIZE]
        for _, row in batch.iterrows():
            # Convert row to dict; handle NaN → None for JSON serialization
            record = {k: (None if pd.isna(v) else v) for k, v in row.items()}
            producer.send(TOPIC, value=record)
        producer.flush()
        total_sent += len(batch)

        elapsed = time.time() - start_time
        rate = total_sent / elapsed if elapsed > 0 else 0
        print(f"  Sent {total_sent:>8,} / {len(df):,} rows  ({rate:.0f} rows/s)", end="\r")

        time.sleep(SLEEP_SECONDS)

    producer.close()
    elapsed = time.time() - start_time
    print(f"\n\nDone. Published {total_sent:,} rows in {elapsed:.1f}s.")

if __name__ == "__main__":
    main()

