#!/usr/bin/env python3
"""
src/ml/train_pipeline.py — distributed MLlib training on the lake the stream built.

Input : Delta silver_flows (split = 'backfill'), pinned to one table version
        with time travel, so every model records exactly which data it saw.
        All rows are used: no toPandas(), no sampling.

  1. stratified train/test split per attack family (sampleBy + left_anti join)
  2. class weights √(N / (k·n_class)), broadcast-joined, against the 400:1 imbalance
  3. feature PipelineModel  : Imputer → VectorAssembler → StandardScaler
  4. supervised             : RandomForest (numTrees / maxDepth from pipeline.yaml)
  5. unsupervised           : KMeans on benign traffic only; anomaly threshold is the
                              99.5th percentile of benign centroid distances (approxQuantile)
  6. held-out evaluation    : macro-F1, per-class P/R, ROC-AUC of both scores
  7. register a new version : data/models/registry/vN  →  the streaming scorer hot-reloads it
  8. test predictions       → Delta gold_model_predictions

Run inside the cluster:  make train
"""

import time
from datetime import datetime, timezone

import numpy as np
from delta.tables import DeltaTable
from pyspark import StorageLevel
from pyspark.ml import PipelineModel
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from pyspark.ml.feature import IndexToString, StringIndexer
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.common.config import cfg, lake
from src.common.schemas import FEATURE_COLS
from src.common.spark import get_spark
from src.ml import model_utils, registry
from src.serving import sqlite_sink


def per_class_report(pred: DataFrame, label: str, predicted: str) -> dict:
    """Confusion matrix via a distributed groupBy, then precision/recall/F1 per class."""
    cm = {(r[label], r[predicted]): r["count"] for r in pred.groupBy(label, predicted).count().collect()}
    classes = sorted({k[0] for k in cm} | {k[1] for k in cm if k[1] is not None})
    report = {}
    for c in classes:
        tp = cm.get((c, c), 0)
        fp = sum(v for (t, p), v in cm.items() if p == c and t != c)
        fn = sum(v for (t, p), v in cm.items() if t == c and p != c)
        prec = tp / (tp + fp) if tp + fp else 0.0
        rec = tp / (tp + fn) if tp + fn else 0.0
        f1 = 2 * prec * rec / (prec + rec) if prec + rec else 0.0
        report[c] = {"precision": prec, "recall": rec, "f1": f1, "support": tp + fn}
    matrix = {"labels": classes,
              "counts": [[cm.get((t, p), 0) for p in classes] for t in classes]}
    return {"per_class": report, "confusion_matrix": matrix,
            "macro_f1": sum(r["f1"] for r in report.values()) / max(len(report), 1)}


def train(spark) -> int:
    """Train both models on silver's backfill split, register them, return the new version."""
    m = cfg()["ml"]
    label, normal, seed = m["label_col"], m["normal_label"], m["seed"]
    t0 = time.time()

    # ── 1. Load backfill split, pinned to a Delta version (time travel) ────────────
    silver_path = lake("silver_flows")
    silver_version = DeltaTable.forPath(spark, silver_path).history(1).collect()[0]["version"]
    df = (spark.read.format("delta").option("versionAsOf", silver_version).load(silver_path)
          .filter(F.col("split") == "backfill")
          .select("flow_uid", label, "is_attack", *FEATURE_COLS))
    counts = {r[label]: r["count"] for r in df.groupBy(label).count().collect()}
    n = sum(counts.values())
    if n == 0:
        raise SystemExit("silver_flows has no backfill rows yet — run the backfill replay first.")
    print(f"\nTraining on silver_flows@v{silver_version}: {n:,} backfill flows")
    for k, v in sorted(counts.items(), key=lambda kv: -kv[1]):
        print(f"  {k:<16} {v:>10,}")

    # ── 2. Stratified split + class weights ───────────────────────────────────────
    train_df = df.sampleBy(label, {k: m["train_fraction"] for k in counts}, seed=seed)
    test = df.join(train_df.select("flow_uid"), "flow_uid", "left_anti")
    k = len(counts)
    weights = spark.createDataFrame([(c, float((n / (k * v)) ** 0.5)) for c, v in counts.items()],
                                    f"{label} string, weight double")
    train_df = train_df.join(F.broadcast(weights), label).persist(StorageLevel.MEMORY_AND_DISK)

    # ── 3. Feature pipeline ───────────────────────────────────────────────────────
    features = model_utils.feature_pipeline(FEATURE_COLS).fit(train_df)
    train_f = (features.transform(train_df).select("flow_uid", label, "weight", model_utils.FEATURES)
               .persist(StorageLevel.MEMORY_AND_DISK))
    train_df.unpersist()
    indexer = StringIndexer(inputCol=label, outputCol="label_idx", stringOrderType="frequencyDesc").fit(train_f)
    labels = list(indexer.labels)
    train_i = indexer.transform(train_f)

    # ── 4. RandomForest ─────────────────────────────────────────────────────────────
    rf_cfg = m["rf"]
    rf = RandomForestClassifier(featuresCol=model_utils.FEATURES, labelCol="label_idx", weightCol="weight",
                                numTrees=rf_cfg["num_trees"], maxDepth=rf_cfg["max_depth"],
                                maxBins=rf_cfg["max_bins"], subsamplingRate=0.7, seed=seed)
    best_rf = rf.fit(train_i)
    rf_model = PipelineModel(stages=[
        best_rf, IndexToString(inputCol="prediction", outputCol="predicted_family", labels=labels)])

    # ── 5. KMeans anomaly model on benign traffic ─────────────────────────────────
    km_cfg = m["kmeans"]
    benign = train_f.filter(F.col(label) == normal)
    kmeans = KMeans(k=km_cfg["k"], maxIter=km_cfg["max_iter"], seed=seed,
                    featuresCol=model_utils.FEATURES, predictionCol=model_utils.CLUSTER).fit(benign)
    centers = np.array([c.tolist() for c in kmeans.clusterCenters()])
    benign_d = model_utils.add_anomaly_score(kmeans.transform(benign), centers)
    threshold = benign_d.approxQuantile("anomaly_score", [km_cfg["threshold_quantile"]], 0.001)[0]
    print(f"\nKMeans k={km_cfg['k']}  anomaly threshold (q={km_cfg['threshold_quantile']}) = {threshold:.4f}")

    meta = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "silver_version": silver_version,
        "train_rows": train_f.count(),
        "features": FEATURE_COLS,
        "labels": labels,
        "normal_label": normal,
        "normal_idx": labels.index(normal),
        "anomaly_threshold": threshold,
        "alert_prob_threshold": m["alert_prob_threshold"],
        "rf_params": {"numTrees": best_rf.getNumTrees, "maxDepth": best_rf.getOrDefault("maxDepth")},
        "feature_importances": sorted(
            zip(FEATURE_COLS, best_rf.featureImportances.toArray().tolist()), key=lambda x: -x[1])[:20],
    }

    # ── 6. Held-out evaluation ────────────────────────────────────────────────────
    tmp = registry.Bundle(0, features, rf_model, kmeans, meta, centers)
    scored = (model_utils.score(test, tmp)
              .withColumn("is_attack_d", F.col("is_attack").cast("double"))
              .persist(StorageLevel.MEMORY_AND_DISK))
    report = per_class_report(scored, label, "predicted_family")
    auc = lambda col: BinaryClassificationEvaluator(  # noqa: E731
        rawPredictionCol=col, labelCol="is_attack_d", metricName="areaUnderROC").evaluate(scored)
    alerts = scored.groupBy("is_attack", "is_alert").count().collect()
    c = {(r["is_attack"], r["is_alert"]): r["count"] for r in alerts}
    tp, fp, fn = c.get((1, True), 0), c.get((0, True), 0), c.get((1, False), 0)
    metrics = {
        "test_rows": scored.count(),
        "macro_f1": report["macro_f1"],
        "accuracy": MulticlassClassificationEvaluator(labelCol="label_idx", metricName="accuracy")
        .evaluate(indexer.setHandleInvalid("keep").transform(scored)),
        "rf_attack_prob_auc": auc("attack_prob"),
        "kmeans_anomaly_auc": auc("anomaly_score"),
        "alert_precision": tp / (tp + fp) if tp + fp else 0.0,
        "alert_recall": tp / (tp + fn) if tp + fn else 0.0,
        **report,
    }
    meta["metrics"] = {k_: v for k_, v in metrics.items() if k_ != "confusion_matrix"}

    # ── 7. Register + 8. persist predictions ──────────────────────────────────────
    version = registry.register(features, rf_model, kmeans, meta)
    (scored.select("flow_uid", label, "predicted_family", "attack_prob", "anomaly_score",
                   "is_attack", "is_alert", "alert_reason")
     .withColumn("model_version", F.lit(version))
     .write.format("delta").mode("append").partitionBy("model_version")
     .save(lake("gold_model_predictions")))
    sqlite_sink.log_event("train", f"registered model v{version} "
                                   f"(macro-F1 {metrics['macro_f1']:.3f}, silver@v{silver_version})")

    print(f"\n{'=' * 64}\nModel v{version} registered in {time.time() - t0:,.0f}s")
    print(f"  macro-F1 {metrics['macro_f1']:.4f}   accuracy {metrics['accuracy']:.4f}")
    print(f"  ROC-AUC  RF P(attack) {metrics['rf_attack_prob_auc']:.4f}   "
          f"KMeans distance {metrics['kmeans_anomaly_auc']:.4f}")
    print(f"  alerts   precision {metrics['alert_precision']:.4f}   recall {metrics['alert_recall']:.4f}")
    for cls, r in sorted(report["per_class"].items(), key=lambda kv: -kv[1]["support"]):
        print(f"    {cls:<16} P {r['precision']:.3f}  R {r['recall']:.3f}  F1 {r['f1']:.3f}  n={r['support']:,}")
    print("The running stream will hot-reload this version on its next micro-batch.\n" + "=" * 64)
    for d in (train_f, scored):
        d.unpersist()
    return version


def main():
    spark = get_spark("mllib-train")
    train(spark)
    spark.stop()


if __name__ == "__main__":
    main()
