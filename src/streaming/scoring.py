"""
src/streaming/scoring.py — Q4: real-time ML scoring of the held-out live split.

Reads silver as a Delta stream (split = 'live') and, per micro-batch (foreachBatch):
  1. hot-loads the newest model version from the registry (no restart needed);
     before the first `make train` it skips scoring and logs once
  2. scores with the MLlib RandomForest (attack family + P(attack)) and the
     KMeans anomaly model (distance to centroid)
  3. fans out to several sinks:
       Delta gold_scored        idempotent append (txnAppId/txnVersion)
       Kafka flows.scored       every scored flow, keyed by src_ip
       Kafka alerts             alerts only, keyed by flow_id
       SQLite alerts            serving cache for the dashboard (INSERT OR REPLACE)
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.types import (BooleanType, DateType, DoubleType, IntegerType, LongType, StringType,
                               StructField, StructType, TimestampType)

from src.common.config import checkpoint, kafka_bootstrap, kafka_sinks_enabled, lake, topic, trigger
from src.common.lake import ensure_table, idempotent_append, query_app_id, stream_source
from src.ml import model_utils
from src.ml.registry import HotLoader
from src.serving import sqlite_sink

QUERY = "q4_scoring"

_s, _i, _l, _d, _t = StringType(), IntegerType(), LongType(), DoubleType(), TimestampType()
SCORED_SCHEMA = StructType([StructField(n, t) for n, t in [
    ("flow_uid", _s), ("flow_id", _s), ("src_ip", _s), ("src_port", _i), ("dst_ip", _s),
    ("dst_port", _i), ("protocol", _i), ("service", _s), ("src_zone", _s), ("dst_zone", _s),
    ("orig_ts", _t), ("event_time", _t), ("day", DateType()), ("split", _s),
    ("label", _s), ("label_family", _s), ("is_attack", _i),
    ("predicted_family", _s), ("attack_prob", _d), ("anomaly_score", _d), ("cluster", _i),
    ("is_alert", BooleanType()), ("alert_reason", _s), ("model_version", _i),
    ("batch_id", _l), ("scored_at", _t)]])

ALERT_COLS = ["flow_uid", "event_time", "orig_ts", "src_ip", "src_port", "dst_ip", "dst_port", "service",
              "src_zone", "predicted_family", "attack_prob", "anomaly_score", "alert_reason",
              "label_family", "model_version", "batch_id", "scored_at"]

_loader = HotLoader()
_state = {"version": None, "waiting_logged": False}


def ensure_tables(spark: SparkSession) -> None:
    ensure_table(spark, lake("gold_scored"), SCORED_SCHEMA, ["day"])


def _to_kafka(df: DataFrame, key_col: str, tp: str) -> None:
    if not kafka_sinks_enabled():
        return
    (df.select(F.col(key_col).cast("string").alias("key"),
               F.to_json(F.struct(*df.columns)).alias("value"))
       .write.format("kafka")
       .option("kafka.bootstrap.servers", kafka_bootstrap())
       .option("topic", tp).save())


def write_batch(batch: DataFrame, batch_id: int) -> None:
    bundle = _loader.get()
    if bundle is None:
        if not _state["waiting_logged"]:
            msg = "no model registered yet — skipping scoring (run `make train` after the backfill)"
            print(f"[{QUERY}] {msg}", flush=True)
            sqlite_sink.log_event(QUERY, msg)
            _state["waiting_logged"] = True
        return
    if bundle.version != _state["version"]:
        msg = f"loaded model v{bundle.version} (hot reload, batch {batch_id})"
        print(f"[{QUERY}] {msg}", flush=True)
        sqlite_sink.log_event(QUERY, msg)
        _state["version"] = bundle.version

    scored = (
        model_utils.score(batch, bundle)
        .withColumn("cluster", F.col(model_utils.CLUSTER).cast("int"))
        .withColumn("batch_id", F.lit(batch_id).cast("long"))
        .withColumn("scored_at", F.current_timestamp())
        .select(*SCORED_SCHEMA.fieldNames())
        .persist()
    )
    try:
        idempotent_append(scored, lake("gold_scored"), query_app_id(QUERY), batch_id)
        _to_kafka(scored, "src_ip", topic("scored"))

        alerts = scored.filter("is_alert").select(*ALERT_COLS).persist()
        if not alerts.isEmpty():
            _to_kafka(alerts, "flow_uid", topic("alerts"))
            sqlite_sink.upsert_df("alerts", alerts)
        alerts.unpersist()
    finally:
        scored.unpersist()


def start(spark: SparkSession) -> StreamingQuery:
    live = stream_source(spark, lake("silver_flows")).filter(F.col("split") == "live")
    spark.sparkContext.setLocalProperty("spark.scheduler.pool", QUERY)
    return (
        live.writeStream.queryName(QUERY)
        .foreachBatch(write_batch)
        .option("checkpointLocation", checkpoint(QUERY))
        .trigger(**trigger())
        .start()
    )
