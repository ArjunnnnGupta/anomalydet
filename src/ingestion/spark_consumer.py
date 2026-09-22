#!/usr/bin/env python3
"""
src/ingestion/spark_consumer.py — Phase 2: Spark Structured Streaming Consumer

Reads JSON messages from the 'network-flows' Kafka topic in near-real-time and
writes them to a Parquet sink (with checkpoint for exactly-once semantics).

Toggle DEBUG_CONSOLE_SINK = True to print rows to the console instead of Parquet
(useful for verifying end-to-end message flow without writing files).

Run:
    python src/ingestion/spark_consumer.py

Confirm it works: you should see micro-batch progress logs every few seconds as
the producer sends data. Then check data/streaming_parquet/ for output files.
"""

import os
import sys

if "JAVA_HOME" not in os.environ and os.path.exists("/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home"):
    os.environ["JAVA_HOME"] = "/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home"
    os.environ["PATH"] = f"/opt/homebrew/opt/openjdk@17/bin:{os.environ.get('PATH', '')}"

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, DoubleType, StringType
)

# ── Config ────────────────────────────────────────────────────────────────────
SPARK_DRIVER_MEMORY  = "2g"
KAFKA_BOOTSTRAP      = "localhost:9092"
TOPIC                = "network-flows"
OUTPUT_PARQUET_PATH  = "data/streaming_parquet"
CHECKPOINT_PATH      = "data/checkpoint"
TRIGGER_SECS         = 5          # micro-batch interval in seconds
DEBUG_CONSOLE_SINK   = False      # set True to print to console instead of Parquet

# ── Schema ────────────────────────────────────────────────────────────────────
# This must match the columns written by clean_data.py.
# All feature columns are Double; Attack Type is String.
FEATURE_COLS = [
    "Destination Port", "Flow Duration", "Total Fwd Packets",
    "Total Length of Fwd Packets", "Fwd Packet Length Max",
    "Fwd Packet Length Min", "Fwd Packet Length Mean", "Fwd Packet Length Std",
    "Bwd Packet Length Max", "Bwd Packet Length Min", "Bwd Packet Length Mean",
    "Bwd Packet Length Std", "Flow Bytes/s", "Flow Packets/s",
    "Flow IAT Mean", "Flow IAT Std", "Flow IAT Max", "Flow IAT Min",
    "Fwd IAT Total", "Fwd IAT Mean", "Fwd IAT Std", "Fwd IAT Max", "Fwd IAT Min",
    "Bwd IAT Total", "Bwd IAT Mean", "Bwd IAT Std", "Bwd IAT Max", "Bwd IAT Min",
    "Fwd Header Length", "Bwd Header Length", "Fwd Packets/s", "Bwd Packets/s",
    "Min Packet Length", "Max Packet Length", "Packet Length Mean",
    "Packet Length Std", "Packet Length Variance", "FIN Flag Count",
    "PSH Flag Count", "ACK Flag Count", "Average Packet Size",
    "Subflow Fwd Bytes", "Init_Win_bytes_forward", "Init_Win_bytes_backward",
    "act_data_pkt_fwd", "min_seg_size_forward", "Active Mean", "Active Max",
    "Active Min", "Idle Mean", "Idle Max", "Idle Min",
]
LABEL_COL = "Attack Type"

SCHEMA = StructType(
    [StructField(col, DoubleType(), True) for col in FEATURE_COLS]
    + [StructField(LABEL_COL, StringType(), True)]
)

# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("spark_consumer")
        .config("spark.driver.memory", SPARK_DRIVER_MEMORY)
        .config("spark.sql.shuffle.partitions", "4")
        # Kafka connector (bundled with PySpark 3.5)
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    print(f"\n{'='*60}")
    print("Phase 2 — Spark Structured Streaming Consumer")
    print(f"  Kafka topic : {TOPIC}")
    print(f"  Output      : {OUTPUT_PARQUET_PATH}")
    print(f"  Debug mode  : {'CONSOLE' if DEBUG_CONSOLE_SINK else 'PARQUET'}")
    print(f"{'='*60}\n")

    # Read raw bytes from Kafka
    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "earliest")
        .load()
    )

    # Kafka delivers messages as binary; cast value to string then parse JSON
    json_stream = raw_stream.select(
        F.from_json(F.col("value").cast("string"), SCHEMA).alias("data")
    ).select("data.*")

    if DEBUG_CONSOLE_SINK:
        # Print first N rows of each micro-batch to console (for debugging only)
        query = (
            json_stream.writeStream
            .format("console")
            .option("numRows", 5)
            .option("truncate", False)
            .trigger(processingTime=f"{TRIGGER_SECS} seconds")
            .start()
        )
    else:
        os.makedirs(CHECKPOINT_PATH, exist_ok=True)
        query = (
            json_stream.writeStream
            .format("parquet")
            .option("path", OUTPUT_PARQUET_PATH)
            .option("checkpointLocation", CHECKPOINT_PATH)
            .trigger(processingTime=f"{TRIGGER_SECS} seconds")
            .start()
        )

    print("Streaming query started. Waiting for data from Kafka producer ...")
    print("(Press Ctrl+C to stop)\n")
    query.awaitTermination()

if __name__ == "__main__":
    main()

