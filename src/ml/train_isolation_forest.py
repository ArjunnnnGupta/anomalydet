#!/usr/bin/env python3
"""
src/ml/train_isolation_forest.py — Phase 4a: Isolation Forest (Unsupervised)

Trains an IsolationForest on the scaled feature matrix from Phase 3.
Each row (network flow) is scored independently — no entity grouping,
no time windows. This is the correct approach for per-flow anomaly detection.

Outputs:
  - models/isolation_forest.joblib    — trained model
  - data/if_scores.npy                — anomaly score per flow (higher = more anomalous)
  - data/if_flags.npy                 — binary flag: -1 = anomaly, 1 = normal

Run: python src/ml/train_isolation_forest.py
"""

import os
import sys
import numpy as np
import joblib
from sklearn.ensemble import IsolationForest

# ── Config ────────────────────────────────────────────────────────────────────
X_SCALED_PATH   = "data/X_scaled.npy"
Y_LABELS_PATH   = "data/y_labels.npy"
MODEL_OUTPUT    = "models/isolation_forest.joblib"
SCORES_OUTPUT   = "data/if_scores.npy"
FLAGS_OUTPUT    = "data/if_flags.npy"

# contamination = expected fraction of anomalies in the dataset.
# Dataset has 2,095,057 Normal Traffic (83.1%) and 425,694 attacks (16.9%) -> contamination = 0.17
CONTAMINATION   = 0.17

N_ESTIMATORS    = 100    # number of trees; 100 is standard
N_JOBS          = -1     # use all CPU cores
RANDOM_STATE    = 42

# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    for path in [X_SCALED_PATH, Y_LABELS_PATH]:
        if not os.path.exists(path):
            print(f"\nERROR: {path} not found. Run Phase 3 first.")
            sys.exit(1)

    os.makedirs("models", exist_ok=True)

    print(f"\n{'='*60}")
    print("Phase 4a — Isolation Forest Training")
    print(f"{'='*60}")

    print(f"\n[1/4] Loading scaled features ...")
    X = np.load(X_SCALED_PATH)
    y = np.load(Y_LABELS_PATH, allow_pickle=True)
    print(f"      X shape: {X.shape}  |  y shape: {y.shape}")

    # Derive binary ground-truth labels for evaluation: Normal Traffic/BENIGN -> 0, Attack -> 1
    y_binary = (~np.isin(y, ["Normal Traffic", "BENIGN"])).astype(int)
    attack_fraction = y_binary.mean()
    print(f"      Attack fraction in sample: {attack_fraction:.2%}  "
          f"(contamination set to {CONTAMINATION})")

    print(f"\n[2/4] Training IsolationForest "
          f"(n_estimators={N_ESTIMATORS}, contamination={CONTAMINATION}) ...")
    print(f"      This may take 30–90 seconds on the full dataset ...")

    clf = IsolationForest(
        n_estimators=N_ESTIMATORS,
        contamination=CONTAMINATION,
        n_jobs=N_JOBS,
        random_state=RANDOM_STATE,
    )
    clf.fit(X)
    print(f"      Training complete.")

    print(f"\n[3/4] Generating anomaly scores and flags ...")
    # decision_function: negative = more anomalous, positive = more normal
    # We negate so that HIGHER score = MORE anomalous (intuitive direction)
    scores = -clf.decision_function(X)
    flags  = clf.predict(X)           # -1 = anomaly, 1 = normal (sklearn convention)

    n_flagged = (flags == -1).sum()
    print(f"      Flagged as anomalous: {n_flagged:,} / {len(flags):,} "
          f"({n_flagged/len(flags):.2%})")

    print(f"\n[4/4] Saving model and outputs ...")
    joblib.dump(clf, MODEL_OUTPUT)
    np.save(SCORES_OUTPUT, scores)
    np.save(FLAGS_OUTPUT, flags)
    print(f"      Model  → {MODEL_OUTPUT}")
    print(f"      Scores → {SCORES_OUTPUT}")
    print(f"      Flags  → {FLAGS_OUTPUT}")

    print(f"\n{'='*60}")
    print("Phase 4a complete. Run evaluate.py to see precision/recall.")
    print(f"{'='*60}\n")

if __name__ == "__main__":
    main()

