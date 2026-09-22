#!/usr/bin/env python3
"""
tests/test_pipeline.py — End-to-end sanity checks for all pipeline stages.

Checks that expected output files exist and contain non-zero data.
Run after completing all phases: python tests/test_pipeline.py
"""

import os
import sys
import sqlite3
import numpy as np

PASS  = "✅"
FAIL  = "❌"
results = []

def check(condition, label):
    status = PASS if condition else FAIL
    results.append((status, label))
    print(f"  {status}  {label}")
    return condition

print(f"\n{'='*60}")
print("Pipeline Sanity Checks")
print(f"{'='*60}\n")

# Phase 1
print("Phase 1 — Data Cleaning")
check(os.path.exists("data/clean_parquet"), "data/clean_parquet/ exists")
check(len(os.listdir("data/clean_parquet")) > 0, "clean_parquet/ contains files")

# Phase 3
print("\nPhase 3 — Preprocessing")
check(os.path.exists("data/X_scaled.npy"), "data/X_scaled.npy exists")
check(os.path.exists("data/y_labels.npy"), "data/y_labels.npy exists")
check(os.path.exists("models/scaler.joblib"), "models/scaler.joblib exists")
if os.path.exists("data/X_scaled.npy"):
    X = np.load("data/X_scaled.npy")
    check(X.shape[0] > 0, f"X_scaled has {X.shape[0]:,} rows")
    check(X.shape[1] > 0, f"X_scaled has {X.shape[1]} feature columns")

# Phase 4
print("\nPhase 4 — ML Models")
check(os.path.exists("models/isolation_forest.joblib"), "isolation_forest.joblib exists")
check(os.path.exists("models/random_forest.joblib"),    "random_forest.joblib exists")
check(os.path.exists("models/label_encoder.joblib"),    "label_encoder.joblib exists")
check(os.path.exists("data/if_scores.npy"),  "if_scores.npy exists")
check(os.path.exists("data/if_flags.npy"),   "if_flags.npy exists")
check(os.path.exists("data/rf_predictions.npy"), "rf_predictions.npy exists")
if os.path.exists("data/if_flags.npy"):
    flags = np.load("data/if_flags.npy")
    n_anomaly = (flags == -1).sum()
    check(n_anomaly > 0, f"IF flagged {n_anomaly:,} anomalous flows (non-zero)")
check(os.path.exists("models/plots/if_confusion_matrix.png"), "IF confusion matrix plot saved")
check(os.path.exists("models/plots/rf_confusion_matrix.png"), "RF confusion matrix plot saved")

# Phase 5
print("\nPhase 5 — Storage")
check(os.path.exists("alerts.db"), "alerts.db exists")
if os.path.exists("alerts.db"):
    conn = sqlite3.connect("alerts.db")
    fs_count = conn.execute("SELECT COUNT(*) FROM flow_scores").fetchone()[0]
    al_count = conn.execute("SELECT COUNT(*) FROM alerts").fetchone()[0]
    conn.close()
    check(fs_count > 0, f"flow_scores has {fs_count:,} rows")
    check(al_count > 0, f"alerts has {al_count:,} rows")

# Summary
print(f"\n{'='*60}")
passed = sum(1 for s, _ in results if s == PASS)
total  = len(results)
print(f"Result: {passed}/{total} checks passed")
if passed < total:
    print("Failed checks:")
    for s, label in results:
        if s == FAIL:
            print(f"  {FAIL}  {label}")
    sys.exit(1)
else:
    print("All checks passed. Pipeline is healthy.")
print(f"{'='*60}\n")

