"""
src/ml/model_utils.py — MLlib building blocks shared by training (batch) and scoring (streaming).

Feature pipeline : Imputer(median) → VectorAssembler → StandardScaler   → "features"
Supervised       : RandomForestClassifier → IndexToString               → "predicted_family"
Unsupervised     : KMeans fitted on benign traffic; anomaly score = distance to nearest centroid
"""

from __future__ import annotations

import numpy as np
import pandas as pd
from pyspark.ml import Pipeline
from pyspark.ml.feature import Imputer, StandardScaler, VectorAssembler
from pyspark.ml.functions import vector_to_array
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

FEATURES = "features"
CLUSTER = "cluster"


def feature_pipeline(feature_cols: list[str]) -> Pipeline:
    imputed = [f"{c}__imp" for c in feature_cols]
    return Pipeline(stages=[
        Imputer(inputCols=feature_cols, outputCols=imputed, strategy="median"),
        VectorAssembler(inputCols=imputed, outputCol="raw_features", handleInvalid="keep"),
        StandardScaler(inputCol="raw_features", outputCol=FEATURES, withMean=True, withStd=True),
    ])


def add_anomaly_score(df: DataFrame, centers: np.ndarray) -> DataFrame:
    """Vectorised (Arrow) pandas UDF: Euclidean distance from each flow to its KMeans centroid."""
    c = np.asarray(centers)

    @F.pandas_udf("double")
    def _dist(x: pd.Series, k: pd.Series) -> pd.Series:
        if len(x) == 0:
            return pd.Series([], dtype="float64")
        X = np.stack(x.to_numpy())
        return pd.Series(np.linalg.norm(X - c[k.to_numpy()], axis=1))

    return df.withColumn("anomaly_score", _dist(vector_to_array(FEATURES), F.col(CLUSTER)))


def add_attack_prob(df: DataFrame, normal_idx: int) -> DataFrame:
    """P(attack) = 1 − P(Normal Traffic) from the RF class-probability vector."""
    return df.withColumn("attack_prob", 1.0 - vector_to_array("probability")[normal_idx])


def score(df: DataFrame, bundle) -> DataFrame:
    """Run both models on a (batch or micro-batch) DataFrame and decide alerts."""
    m = bundle.meta
    out = bundle.features.transform(df)
    out = bundle.rf.transform(out)
    out = bundle.kmeans.transform(out)
    out = add_attack_prob(out, m["normal_idx"])
    out = add_anomaly_score(out, bundle.centers)

    supervised = (F.col("predicted_family") != m["normal_label"]) & (F.col("attack_prob") >= m["alert_prob_threshold"])
    anomalous = F.col("anomaly_score") > m["anomaly_threshold"]
    return (
        out.withColumn("is_alert", supervised | anomalous)
        .withColumn("alert_reason",
                    F.when(supervised & anomalous, "rf+anomaly")
                    .when(supervised, "rf")
                    .when(anomalous, "anomaly"))
        .withColumn("model_version", F.lit(int(bundle.version)))
    )
