"""
src/common/lake.py — small Delta Lake helpers shared by the streaming and ML jobs.
"""

from __future__ import annotations

import json
import os

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType

from src.common.config import cfg, checkpoint


def ensure_table(spark: SparkSession, path: str, schema: StructType, partition_by: list[str] | None = None):
    """Create an empty Delta table if missing, so downstream streams can start before data arrives."""
    if DeltaTable.isDeltaTable(spark, path):
        return
    w = spark.createDataFrame([], schema).write.format("delta").mode("ignore")
    if partition_by:
        w = w.partitionBy(*partition_by)
    w.save(path)


def merge_upsert(spark: SparkSession, df: DataFrame, path: str, keys: list[str],
                 prune_partitions: str | None = None, min_col: str | None = None) -> None:
    """MERGE INTO on natural keys: ACID, idempotent upsert of a micro-batch (used by the gold tables).

    A MERGE condition on keys alone makes Delta scan the whole target table every micro-batch.
    Literal predicates let it skip data: `prune_partitions` adds `t.<col> IN (<values in this batch>)`
    (partition pruning) and `min_col` adds `t.<col> >= <smallest value in this batch>` (file
    skipping via Delta's per-file min/max stats).
    """
    cond = " AND ".join(f"t.`{k}` = s.`{k}`" for k in keys)
    if prune_partitions:
        vals = [r[0] for r in df.select(prune_partitions).distinct().collect() if r[0] is not None]
        if vals:
            cond += f" AND t.`{prune_partitions}` IN ({', '.join(repr(str(v)) for v in vals)})"
    if min_col:
        lo = df.agg({min_col: "min"}).collect()[0][0]
        if lo is not None:
            cond += f" AND t.`{min_col}` >= TIMESTAMP'{lo:%Y-%m-%d %H:%M:%S}'"
    (DeltaTable.forPath(spark, path).alias("t").merge(df.alias("s"), cond)
     .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())


def query_app_id(query: str) -> str:
    """txnAppId for a streaming query's idempotent Delta writes: name + the query id from its checkpoint.

    The id changes whenever the checkpoint is new. Using the bare query name broke after a
    checkpoint reset: batch ids restart at 0, Delta still remembered the old run's last
    txnVersion, and silently skipped every batch up to it.
    """
    try:
        with open(os.path.join(checkpoint(query), "metadata")) as f:
            return f"{query}-{json.load(f)['id']}"
    except (OSError, ValueError, KeyError):
        return query


def idempotent_append(df: DataFrame, path: str, app_id: str, batch_id: int) -> None:
    """Delta idempotent write: a retried foreachBatch with the same (appId, version) is skipped."""
    (df.write.format("delta").mode("append")
       .option("txnAppId", app_id).option("txnVersion", batch_id)
       .save(path))


def stream_source(spark: SparkSession, path: str) -> DataFrame:
    """Read a Delta table as a stream (Delta transaction log = ordered, replayable source).

    Rate-limited like the Kafka source: without a cap, a stage that falls behind swallows its
    whole backlog in one micro-batch (276K rows in the first cluster run) and the executor is
    OOM-killed.
    """
    s = cfg()["streaming"]
    r = (spark.readStream.format("delta")
         .option("skipChangeCommits", "true")
         .option("maxFilesPerTrigger", s["max_files_per_trigger"])
         .option("maxBytesPerTrigger", s["max_bytes_per_trigger"]))
    if os.environ.get("STREAM_SKIP_BACKLOG") == "1":
        # Opt-in (make stream SKIP_BACKLOG=1): a query WITHOUT a checkpoint starts at the table's
        # current end instead of replaying it. Queries that have a checkpoint are unaffected.
        r = r.option("startingVersion", "latest")
    return r.load(path)
