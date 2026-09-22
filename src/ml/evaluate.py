#!/usr/bin/env python3
"""
src/ml/evaluate.py — Phase 4c: Model Evaluation

Evaluates both trained models against ground-truth labels and generates plots:
  1. Isolation Forest: precision/recall/F1 (binary: attack vs benign),
     confusion matrix, anomaly score distribution plot
  2. Random Forest: per-class precision/recall/F1, confusion matrix heatmap

Plots are saved to models/plots/.

Run: python src/ml/evaluate.py
"""

import os
import sys
import json
import numpy as np
import joblib
import matplotlib
matplotlib.use("Agg")   # non-interactive backend (no display needed)
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
from sklearn.metrics import (
    classification_report, confusion_matrix, ConfusionMatrixDisplay
)

# ── Config ────────────────────────────────────────────────────────────────────
Y_LABELS_PATH   = "data/y_labels.npy"
IF_SCORES_PATH  = "data/if_scores.npy"
IF_FLAGS_PATH   = "data/if_flags.npy"
RF_PREDS_PATH   = "data/rf_predictions.npy"
RF_TEST_Y_PATH  = "data/rf_test_y.npy"
ENCODER_PATH    = "models/label_encoder.joblib"
PLOTS_DIR       = "models/plots"

# ── Helpers ───────────────────────────────────────────────────────────────────
def check(path):
    if not os.path.exists(path):
        print(f"\nERROR: {path} not found. Run the training scripts first.")
        sys.exit(1)

# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    for p in [Y_LABELS_PATH, IF_SCORES_PATH, IF_FLAGS_PATH,
              RF_PREDS_PATH, RF_TEST_Y_PATH, ENCODER_PATH]:
        check(p)

    os.makedirs(PLOTS_DIR, exist_ok=True)

    y_raw      = np.load(Y_LABELS_PATH, allow_pickle=True)
    if_scores  = np.load(IF_SCORES_PATH)
    if_flags   = np.load(IF_FLAGS_PATH)
    rf_preds   = np.load(RF_PREDS_PATH)
    rf_test_y  = np.load(RF_TEST_Y_PATH)
    le         = joblib.load(ENCODER_PATH)

    # Binary ground truth aligned with IF (same sample, same order): Normal -> 0, Attack -> 1
    y_binary = (~np.isin(y_raw, ["Normal Traffic", "BENIGN"])).astype(int)
    # IF flags: sklearn returns -1=anomaly, 1=normal → convert to 0/1
    if_pred_binary = (if_flags == -1).astype(int)

    print(f"\n{'='*60}")
    print("Phase 4c — Model Evaluation")
    print(f"{'='*60}")

    # ── Isolation Forest ──────────────────────────────────────────────────────
    print(f"\n{'─'*40}")
    print("Isolation Forest — Binary Classification (attack vs benign)")
    print(f"{'─'*40}")
    print(classification_report(
        y_binary, if_pred_binary,
        target_names=["Benign (0)", "Attack (1)"]
    ))

    # Confusion matrix
    cm_if = confusion_matrix(y_binary, if_pred_binary)
    fig, ax = plt.subplots(figsize=(5, 4))
    disp = ConfusionMatrixDisplay(cm_if, display_labels=["Benign", "Attack"])
    disp.plot(ax=ax, colorbar=False, cmap="Blues")
    ax.set_title("Isolation Forest — Confusion Matrix")
    plt.tight_layout()
    plt.savefig(f"{PLOTS_DIR}/if_confusion_matrix.png", dpi=120)
    plt.close()
    print(f"  → Saved: {PLOTS_DIR}/if_confusion_matrix.png")

    # Anomaly score distribution
    fig, ax = plt.subplots(figsize=(8, 4))
    ax.hist(if_scores[y_binary == 0], bins=80, alpha=0.6, label="Benign",  color="steelblue")
    ax.hist(if_scores[y_binary == 1], bins=80, alpha=0.6, label="Attack",  color="crimson")
    ax.set_xlabel("Anomaly Score (higher = more anomalous)")
    ax.set_ylabel("Flow count")
    ax.set_title("Isolation Forest — Anomaly Score Distribution")
    ax.legend()
    ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, _: f"{int(x):,}"))
    plt.tight_layout()
    plt.savefig(f"{PLOTS_DIR}/if_score_distribution.png", dpi=120)
    plt.close()
    print(f"  → Saved: {PLOTS_DIR}/if_score_distribution.png")

    # ── Random Forest ─────────────────────────────────────────────────────────
    print(f"\n{'─'*40}")
    print("Random Forest — Multi-class Attack Type Classification")
    print(f"{'─'*40}")
    class_names = le.classes_
    print(classification_report(
        rf_test_y, rf_preds,
        target_names=class_names,
        zero_division=0
    ))

    # Confusion matrix heatmap
    cm_rf = confusion_matrix(rf_test_y, rf_preds)
    # If too many classes, abbreviate for readability
    short_names = [n[:12] for n in class_names]
    fig_size = max(8, len(class_names))
    fig, ax = plt.subplots(figsize=(fig_size, fig_size - 1))
    disp = ConfusionMatrixDisplay(cm_rf, display_labels=short_names)
    disp.plot(ax=ax, colorbar=True, cmap="Blues", xticks_rotation=45)
    ax.set_title("Random Forest — Confusion Matrix (multi-class)")
    plt.tight_layout()
    plt.savefig(f"{PLOTS_DIR}/rf_confusion_matrix.png", dpi=120)
    plt.close()
    print(f"  → Saved: {PLOTS_DIR}/rf_confusion_matrix.png")

    print(f"\n{'='*60}")
    print("Evaluation complete. All plots saved to models/plots/")
    print(f"{'='*60}\n")

if __name__ == "__main__":
    main()

