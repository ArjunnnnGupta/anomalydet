#!/usr/bin/env python3
"""
src/ml/train_random_forest.py — Phase 4b: Random Forest Classifier (Supervised)

Trains a RandomForestClassifier on the labeled data from Phase 3 to:
  1. Classify flows into attack types (multi-class)
  2. Provide a supervised cross-check against the unsupervised IF predictions

Uses a stratified 80/20 train/test split. Saves the trained model and the
test-set predictions for evaluation in evaluate.py.

Run: python src/ml/train_random_forest.py
"""

import os
import sys
import numpy as np
import joblib
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder

# ── Config ────────────────────────────────────────────────────────────────────
X_SCALED_PATH   = "data/X_scaled.npy"
Y_LABELS_PATH   = "data/y_labels.npy"
MODEL_OUTPUT    = "models/random_forest.joblib"
ENCODER_OUTPUT  = "models/label_encoder.joblib"
RF_PREDS_OUTPUT = "data/rf_predictions.npy"
RF_TEST_Y       = "data/rf_test_y.npy"

N_ESTIMATORS    = 100
MAX_DEPTH       = 20     # cap depth to reduce RAM; increase if accuracy is poor
N_JOBS          = -1
RANDOM_STATE    = 42
TEST_SIZE       = 0.2

# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    for path in [X_SCALED_PATH, Y_LABELS_PATH]:
        if not os.path.exists(path):
            print(f"\nERROR: {path} not found. Run Phase 3 first.")
            sys.exit(1)

    os.makedirs("models", exist_ok=True)

    print(f"\n{'='*60}")
    print("Phase 4b — Random Forest Training")
    print(f"{'='*60}")

    print(f"\n[1/5] Loading features and labels ...")
    X = np.load(X_SCALED_PATH)
    y_raw = np.load(Y_LABELS_PATH, allow_pickle=True)
    print(f"      X shape: {X.shape}  |  y shape: {y_raw.shape}")

    # Encode string labels to integers
    print(f"\n[2/5] Encoding labels ...")
    le = LabelEncoder()
    y = le.fit_transform(y_raw)
    print(f"      Classes ({len(le.classes_)}): {list(le.classes_)}")

    # Stratified split
    print(f"\n[3/5] Stratified 80/20 train/test split ...")
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=TEST_SIZE, random_state=RANDOM_STATE, stratify=y
    )
    print(f"      Train: {X_train.shape[0]:,} rows  |  Test: {X_test.shape[0]:,} rows")

    print(f"\n[4/5] Training RandomForestClassifier "
          f"(n_estimators={N_ESTIMATORS}, max_depth={MAX_DEPTH}) ...")
    print(f"      This may take 1–5 minutes on the full dataset ...")
    clf = RandomForestClassifier(
        n_estimators=N_ESTIMATORS,
        max_depth=MAX_DEPTH,
        n_jobs=N_JOBS,
        random_state=RANDOM_STATE,
        class_weight="balanced",   # handle class imbalance (many BENIGN rows)
    )
    clf.fit(X_train, y_train)
    print(f"      Training complete.")

    print(f"\n[5/5] Generating test-set predictions and saving ...")
    y_pred = clf.predict(X_test)
    train_acc = clf.score(X_train, y_train)
    test_acc  = clf.score(X_test, y_test)
    print(f"      Train accuracy: {train_acc:.4f}  |  Test accuracy: {test_acc:.4f}")

    joblib.dump(clf, MODEL_OUTPUT)
    joblib.dump(le, ENCODER_OUTPUT)
    np.save(RF_PREDS_OUTPUT, y_pred)
    np.save(RF_TEST_Y, y_test)

    print(f"      Model   → {MODEL_OUTPUT}")
    print(f"      Encoder → {ENCODER_OUTPUT}")
    print(f"      Preds   → {RF_PREDS_OUTPUT}")
    print(f"      Test y  → {RF_TEST_Y}")

    print(f"\n{'='*60}")
    print("Phase 4b complete. Run evaluate.py for full metrics.")
    print(f"{'='*60}\n")

if __name__ == "__main__":
    main()

