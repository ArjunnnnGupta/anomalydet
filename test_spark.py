#!/usr/bin/env python3
"""
test_spark.py — Phase 0 sanity check.
Confirms PySpark starts correctly in local mode with a 2GB driver memory cap.
Expected output: a row count of 10, then "Spark OK."
"""

import os
import sys

if "JAVA_HOME" not in os.environ and os.path.exists("/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home"):
    os.environ["JAVA_HOME"] = "/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home"
    os.environ["PATH"] = f"/opt/homebrew/opt/openjdk@17/bin:{os.environ.get('PATH', '')}"

from pyspark.sql import SparkSession

# Memory cap: keep this at 2g on an 8GB machine.
SPARK_DRIVER_MEMORY = "2g"

spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("test_spark")
    .config("spark.driver.memory", SPARK_DRIVER_MEMORY)
    .config("spark.sql.shuffle.partitions", "4")   # Reduce default 200 → 4 for local mode
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

count = spark.range(10).count()
assert count == 10, f"Expected 10, got {count}"
print(f"Row count: {count}")
print("Spark OK.")

spark.stop()

