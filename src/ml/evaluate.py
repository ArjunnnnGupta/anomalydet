#!/usr/bin/env python3
"""
src/ml/evaluate.py — evaluation report + plots for a registered model version.

    make evaluate               held-out test split   (Delta gold_model_predictions)
    make evaluate ARGS=--live   live stream output    (Delta gold_scored, the 20% never trained on)

All counting happens in Spark (groupBy confusion matrix, histogram buckets,
BinaryClassificationEvaluator). Only small aggregates reach the driver for plotting.
Output: data/models/registry/vN/metrics[_live].json + plots/*.png
"""

from __future__ import annotations

import argparse
import json
import os

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402
from pyspark.ml.evaluation import BinaryClassificationEvaluator  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from src.common.config import cfg, lake  # noqa: E402
from src.common.spark import get_spark  # noqa: E402
from src.ml import registry  # noqa: E402
from src.ml.train_pipeline import per_class_report  # noqa: E402


def plot_confusion(cm: dict, title: str, out: str) -> None:
    labels, counts = cm["labels"], np.array(cm["counts"], dtype=float)
    norm = counts / np.maximum(counts.sum(axis=1, keepdims=True), 1)
    fig, ax = plt.subplots(figsize=(8, 7))
    im = ax.imshow(norm, cmap="Blues", vmin=0, vmax=1)
    ax.set_xticks(range(len(labels)), labels, rotation=45, ha="right")
    ax.set_yticks(range(len(labels)), labels)
    for i in range(len(labels)):
        for j in range(len(labels)):
            ax.text(j, i, f"{int(counts[i, j]):,}", ha="center", va="center", fontsize=7,
                    color="white" if norm[i, j] > 0.5 else "black")
    ax.set_xlabel("Predicted family")
    ax.set_ylabel("True family")
    ax.set_title(title)
    fig.colorbar(im, ax=ax, fraction=0.046, label="row-normalised")
    fig.tight_layout()
    fig.savefig(out, dpi=120)
    plt.close(fig)


def plot_score_hist(df, threshold: float, out: str) -> None:
    """Histogram computed in Spark: bucket ids per class, counted with groupBy."""
    hi = df.approxQuantile("anomaly_score", [0.999], 0.001)[0] or 1.0
    width = hi / 60
    h = (df.withColumn("b", F.least(F.floor(F.col("anomaly_score") / width), F.lit(60)))
         .groupBy("is_attack", "b").count().collect())
    fig, ax = plt.subplots(figsize=(9, 4))
    for cls, name, color in [(0, "Benign", "#4C78A8"), (1, "Attack", "#E45756")]:
        pts = sorted((r["b"] * width, r["count"]) for r in h if r["is_attack"] == cls)
        if pts:
            ax.bar([p[0] for p in pts], [p[1] for p in pts], width=width, alpha=0.6, label=name, color=color)
    ax.axvline(threshold, color="black", ls="--", label=f"threshold {threshold:.2f}")
    ax.set_yscale("log")
    ax.set_xlabel("KMeans centroid distance (anomaly score)")
    ax.set_ylabel("flows (log)")
    ax.set_title("Anomaly score distribution")
    ax.legend()
    fig.tight_layout()
    fig.savefig(out, dpi=120)
    plt.close(fig)


def plot_importances(meta: dict, out: str) -> None:
    imp = meta.get("feature_importances") or []
    if not imp:
        return
    names, vals = zip(*imp[::-1])
    fig, ax = plt.subplots(figsize=(8, 6))
    ax.barh(names, vals, color="#4C78A8")
    ax.set_title("RandomForest feature importance (top 20)")
    fig.tight_layout()
    fig.savefig(out, dpi=120)
    plt.close(fig)


def evaluate(spark, version: int | None = None, live: bool = False) -> dict:
    version = version or registry.latest_version()
    if version is None:
        raise SystemExit("No registered model. Run `make train` first.")
    vdir = os.path.join(registry.root(), f"v{version}")
    with open(os.path.join(vdir, "meta.json")) as f:
        meta = json.load(f)

    label = cfg()["ml"]["label_col"]
    src = lake("gold_scored") if live else lake("gold_model_predictions")
    df = (spark.read.format("delta").load(src)
          .filter(F.col("model_version") == version)
          .withColumn("is_attack_d", F.col("is_attack").cast("double"))
          .cache())
    n = df.count()
    if n == 0:
        raise SystemExit(f"No rows for model v{version} in {src}.")

    report = per_class_report(df, label, "predicted_family")
    auc = lambda col: BinaryClassificationEvaluator(  # noqa: E731
        rawPredictionCol=col, labelCol="is_attack_d").evaluate(df)
    c = {(r["is_attack"], r["is_alert"]): r["count"] for r in df.groupBy("is_attack", "is_alert").count().collect()}
    tp, fp, fn = c.get((1, True), 0), c.get((0, True), 0), c.get((1, False), 0)
    reasons = {r["alert_reason"]: r["count"] for r in
               df.filter("is_alert").groupBy("alert_reason").count().collect()}
    metrics = {
        "model_version": version,
        "source": "live_stream" if live else "held_out_test",
        "rows": n,
        "macro_f1": report["macro_f1"],
        "rf_attack_prob_auc": auc("attack_prob"),
        "kmeans_anomaly_auc": auc("anomaly_score"),
        "alert_precision": tp / (tp + fp) if tp + fp else 0.0,
        "alert_recall": tp / (tp + fn) if tp + fn else 0.0,
        "alerts_by_reason": reasons,
        **report,
    }

    suffix = "_live" if live else ""
    plots = os.path.join(vdir, "plots")
    os.makedirs(plots, exist_ok=True)
    with open(os.path.join(vdir, f"metrics{suffix}.json"), "w") as f:
        json.dump(metrics, f, indent=2)
    plot_confusion(report["confusion_matrix"], f"RF confusion matrix — v{version} ({metrics['source']})",
                   os.path.join(plots, f"confusion_matrix{suffix}.png"))
    plot_score_hist(df, meta["anomaly_threshold"], os.path.join(plots, f"anomaly_scores{suffix}.png"))
    plot_importances(meta, os.path.join(plots, "feature_importance.png"))

    print(f"\n{'=' * 64}\nModel v{version} — {metrics['source']} ({n:,} flows)")
    print(f"  macro-F1 {metrics['macro_f1']:.4f}  |  AUC RF {metrics['rf_attack_prob_auc']:.4f}  "
          f"KMeans {metrics['kmeans_anomaly_auc']:.4f}")
    print(f"  alerts: precision {metrics['alert_precision']:.4f}  recall {metrics['alert_recall']:.4f}  "
          f"by reason {reasons}")
    print(f"  → {vdir}/metrics{suffix}.json and plots/\n{'=' * 64}")
    df.unpersist()
    return metrics


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--version", type=int, help="model version (default: latest)")
    ap.add_argument("--live", action="store_true", help="evaluate the live scored stream instead")
    args = ap.parse_args()
    spark = get_spark("mllib-evaluate")
    evaluate(spark, args.version, args.live)
    spark.stop()


if __name__ == "__main__":
    main()
