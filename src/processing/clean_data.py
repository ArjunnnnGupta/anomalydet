#!/usr/bin/env python3
"""
src/processing/clean_data.py — Phase 1: Data Cleaning

Loads cicds.csv from data/raw/, cleans it in Spark, and writes clean Parquet
to data/clean_parquet/.

What this does:
  - Strips whitespace from column names (CICIDS CSVs sometimes have leading spaces)
  - Casts all feature columns to DoubleType
  - Replaces Infinity / -Infinity values with NaN, then drops those rows
  - Drops rows with any remaining null values
  - Drops zero-variance columns (constant across all rows — useless for ML)
  - Prints label distribution and row counts before/after cleaning
  - Writes result to Parquet

Run: python src/processing/clean_data.py
"""

import sys
import os

if "JAVA_HOME" not in os.environ and os.path.exists("/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home"):
    os.environ["JAVA_HOME"] = "/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home"
    os.environ["PATH"] = f"/opt/homebrew/opt/openjdk@17/bin:{os.environ.get('PATH', '')}"

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

# ── Config ────────────────────────────────────────────────────────────────────
SPARK_DRIVER_MEMORY = "2g"
RAW_CSV_PATH        = "data/raw/cicds.csv"
OUTPUT_PARQUET_PATH = "data/clean_parquet"
LABEL_COLUMN        = "Attack Type"

# ── Helpers ───────────────────────────────────────────────────────────────────
def check_input(path):
    if not os.path.exists(path):
        print(f"\nERROR: Input file not found: '{path}'")
        print("Please place cicds.csv in the data/raw/ directory and try again.")
        sys.exit(1)

def get_spark():
    return (
        SparkSession.builder
        .master("local[*]")
        .appName("clean_data")
        .config("spark.driver.memory", SPARK_DRIVER_MEMORY)
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )

# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    check_input(RAW_CSV_PATH)

    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")

    print(f"\n{'='*60}")
    print("Phase 1 — Data Cleaning")
    print(f"{'='*60}")

    # 1. Load raw CSV
    print(f"\n[1/6] Loading {RAW_CSV_PATH} ...")
    df = spark.read.csv(RAW_CSV_PATH, header=True, inferSchema=True)

    # Strip leading/trailing whitespace from column names
    df = df.toDF(*[c.strip() for c in df.columns])

    raw_rows = df.count()
    raw_cols = len(df.columns)
    print(f"      Raw shape: {raw_rows:,} rows × {raw_cols} columns")

    # 2. Show label distribution (before cleaning)
    print(f"\n[2/6] Label distribution (raw):")
    df.groupBy(LABEL_COLUMN).count().orderBy("count", ascending=False).show(30, truncate=False)

    # 3. Identify feature columns (everything except the label)
    feature_cols = [c for c in df.columns if c != LABEL_COLUMN]

    # 4. Cast all feature columns to Double
    print(f"[3/6] Casting {len(feature_cols)} feature columns to DoubleType ...")
    for col in feature_cols:
        df = df.withColumn(col, F.col(col).cast(DoubleType()))

    # 5. Replace Infinity / -Infinity with null, then drop null rows
    print(f"[4/6] Replacing Inf/-Inf with null and dropping null rows ...")
    for col in feature_cols:
        df = df.withColumn(
            col,
            F.when(F.col(col).isin([float("inf"), float("-inf")]), None)
             .otherwise(F.col(col))
        )
    df_clean = df.dropna()
    after_null_drop = df_clean.count()
    print(f"      Rows after null/inf drop: {after_null_drop:,} "
          f"(dropped {raw_rows - after_null_drop:,})")

    # 6. Drop zero-variance columns (same value in every row)
    print(f"[5/6] Detecting and dropping zero-variance columns ...")
    stats = df_clean.select(
        [F.stddev(F.col(c)).alias(c) for c in feature_cols]
    ).collect()[0].asDict()
    zero_var_cols = [c for c, std in stats.items() if std is None or std == 0.0]
    if zero_var_cols:
        print(f"      Dropping {len(zero_var_cols)} zero-variance columns: {zero_var_cols}")
        df_clean = df_clean.drop(*zero_var_cols)
    else:
        print(f"      No zero-variance columns found.")

    final_rows = df_clean.count()
    final_cols = len(df_clean.columns)
    print(f"\n      Final shape: {final_rows:,} rows × {final_cols} columns")

    # 7. Write to Parquet
    print(f"\n[6/6] Writing clean data to {OUTPUT_PARQUET_PATH} ...")
    df_clean.write.mode("overwrite").parquet(OUTPUT_PARQUET_PATH)
    print(f"      Done.")

    print(f"\n{'='*60}")
    print("Phase 1 complete.")
    print(f"  Input rows : {raw_rows:,}")
    print(f"  Output rows: {final_rows:,}  (dropped {raw_rows - final_rows:,})")
    print(f"  Output cols: {final_cols}  (dropped {raw_cols - final_cols} zero-variance)")
    print(f"  Output path: {OUTPUT_PARQUET_PATH}/")
    print(f"{'='*60}\n")

    spark.stop()

if __name__ == "__main__":
    main()

