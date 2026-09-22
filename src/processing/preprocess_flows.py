#!/usr/bin/env python3
"""
src/processing/preprocess_flows.py — Phase 3: Per-Flow Feature Preprocessing

Loads the cleaned Parquet, handles any residual Inf values in Spark,
then exports the feature matrix. In pandas, applies StandardScaler and
saves the scaled features + scaler to disk for use in Phase 4 ML training.

NOTE: No windowing, no groupBy source IP. Each row is a flow; each row is
      preprocessed and scored independently. This is by design — the dataset
      (cicds.csv) has no Source IP or Timestamp columns.

      If the full dataset exceeds ~1.5GB in Pandas (system-dependent), this
      script samples 200K rows. The sample flag is printed clearly so you know
      if sampling occurred.

Run: python src/processing/preprocess_flows.py
"""

import os
import sys

if "JAVA_HOME" not in os.environ and os.path.exists("/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home"):
    os.environ["JAVA_HOME"] = "/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home"
    os.environ["PATH"] = f"/opt/homebrew/opt/openjdk@17/bin:{os.environ.get('PATH', '')}"

import joblib
import numpy as np
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from sklearn.preprocessing import StandardScaler

# ── Config ────────────────────────────────────────────────────────────────────
SPARK_DRIVER_MEMORY     = "2g"
CLEAN_PARQUET_PATH      = "data/clean_parquet"
OUTPUT_PARQUET_PATH     = "data/preprocessed_parquet"
SCALER_PATH             = "models/scaler.joblib"
LABEL_COLUMN            = "Attack Type"
MAX_PANDAS_ROWS         = 500_000   # sample if dataset exceeds this in Pandas
SAMPLE_SEED             = 42

# ── Helpers ───────────────────────────────────────────────────────────────────
def check_input(path):
    if not os.path.exists(path):
        print(f"\nERROR: Cleaned Parquet not found at '{path}'.")
        print("Run Phase 1 first: python src/processing/clean_data.py")
        sys.exit(1)

def get_spark():
    return (
        SparkSession.builder
        .master("local[*]")
        .appName("preprocess_flows")
        .config("spark.driver.memory", SPARK_DRIVER_MEMORY)
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )

# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    check_input(CLEAN_PARQUET_PATH)
    os.makedirs("models", exist_ok=True)

    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")

    print(f"\n{'='*60}")
    print("Phase 3 — Per-Flow Feature Preprocessing")
    print(f"{'='*60}")

    # 1. Load cleaned Parquet
    print(f"\n[1/5] Loading cleaned Parquet from {CLEAN_PARQUET_PATH} ...")
    df = spark.read.parquet(CLEAN_PARQUET_PATH)
    total_rows = df.count()
    feature_cols = [c for c in df.columns if c != LABEL_COLUMN]
    print(f"      Rows: {total_rows:,}  |  Feature columns: {len(feature_cols)}")

    # 2. Replace any residual Inf / -Inf (can survive cast if data has edge cases)
    print(f"\n[2/5] Replacing residual Inf/-Inf values in feature columns ...")
    for col in feature_cols:
        df = df.withColumn(
            col,
            F.when(F.col(col) == float("inf"),  1e15).otherwise(
            F.when(F.col(col) == float("-inf"), -1e15).otherwise(
            F.col(col)))
        )

    # 3. Write Spark-processed feature matrix to Parquet (preserves label for Phase 4)
    print(f"\n[3/5] Writing preprocessed Parquet to {OUTPUT_PARQUET_PATH} ...")
    df.write.mode("overwrite").parquet(OUTPUT_PARQUET_PATH)
    print(f"      Done.")

    # 4. Pull into Pandas for scaling (sample if too large)
    print(f"\n[4/5] Pulling feature matrix into Pandas for scaling ...")
    if total_rows > MAX_PANDAS_ROWS:
        fraction = MAX_PANDAS_ROWS / total_rows
        print(f"      ⚠  Dataset has {total_rows:,} rows — sampling {MAX_PANDAS_ROWS:,} "
              f"(fraction={fraction:.3f}) to stay within 8GB RAM.")
        print(f"      (Set MAX_PANDAS_ROWS higher only if your machine has >8GB free RAM)")
        pdf = df.sample(fraction=fraction, seed=SAMPLE_SEED).toPandas()
    else:
        pdf = df.toPandas()

    print(f"      Pandas shape: {pdf.shape[0]:,} rows × {pdf.shape[1]} columns")

    # Separate features from label
    X = pdf[feature_cols].values
    y = pdf[LABEL_COLUMN].values  # kept for Phase 4 evaluation

    # 5. Fit and apply StandardScaler
    print(f"\n[5/5] Fitting StandardScaler and scaling features ...")
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    # Save scaler for use at inference time
    joblib.dump(scaler, SCALER_PATH)
    print(f"      Scaler saved to {SCALER_PATH}")

    # Save scaled feature matrix + labels as numpy arrays
    np.save("data/X_scaled.npy", X_scaled)
    np.save("data/y_labels.npy", y)
    # Also save feature column names for reference
    import json
    with open("data/feature_cols.json", "w") as f:
        json.dump(feature_cols, f)

    print(f"\n      Saved:")
    print(f"        data/X_scaled.npy    — scaled feature matrix ({X_scaled.shape})")
    print(f"        data/y_labels.npy    — label array ({y.shape})")
    print(f"        data/feature_cols.json — feature column names")
    print(f"        models/scaler.joblib — fitted StandardScaler")

    print(f"\n      Feature stats (first 5 columns after scaling):")
    print(f"        mean: {X_scaled[:, :5].mean(axis=0).round(4)}")
    print(f"        std : {X_scaled[:, :5].std(axis=0).round(4)}")

    print(f"\n{'='*60}")
    print("Phase 3 complete.")
    print(f"{'='*60}\n")

    spark.stop()

if __name__ == "__main__":
    main()

