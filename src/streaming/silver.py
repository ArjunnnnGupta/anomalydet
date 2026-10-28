"""
src/streaming/silver.py — Q2: Delta bronze (as a stream) → parse, validate, enrich → Delta silver.

  • from_json with an explicit schema (every value arrives as a raw string)
  • event-time parsing for CICIDS2017's mixed timestamp formats + 12-hour clock
  • numeric casting, Infinity/NaN → NULL
  • label cleaning + attack-family mapping
  • data-quality rules → bad rows go to silver_quarantine (Delta) and flows.dlq (Kafka)
  • stream-static BROADCAST joins with dim_subnets / dim_ports
  • foreachBatch idempotent append into silver (txnAppId/txnVersion → exactly-once), deduped on flow_uid
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.types import StructType

from src.common.config import checkpoint, kafka_bootstrap, kafka_sinks_enabled, lake, topic, trigger
from src.common.dims import dim_ports, dim_subnets
from src.common.lake import ensure_table, idempotent_append, query_app_id, stream_source
from src.common.schemas import NUMERIC_COLS, PAYLOAD_SCHEMA, RAW_COLS, clean_label, label_family
from src.streaming.bronze import BRONZE_SCHEMA

QUERY = "q2_silver"

KEY_COLS = ["flow_uid", "flow_id", "src_ip", "src_port", "dst_ip", "dst_port", "protocol",
            "orig_ts", "event_time", "day"]
LABEL_COLS = ["label", "label_family", "is_attack"]
META_COLS = ["split", "source_file", "kafka_partition", "kafka_offset", "ingest_ts"]
ENRICH_COLS = ["src_zone", "dst_zone", "service"]


def parse_event_time(ts):
    """dd/MM/yyyy H:mm[:ss]; afternoon files use a 12-hour clock without AM/PM (1:00 → 13:00)."""
    t = F.coalesce(F.to_timestamp(ts, "d/M/yyyy H:mm:ss"), F.to_timestamp(ts, "d/M/yyyy H:mm"))
    return F.when(F.hour(t) < 8, t + F.expr("INTERVAL 12 HOURS")).otherwise(t)


def _num(c: str):
    v = F.col(f"p.{c}").cast("double")
    return F.when(F.isnan(v) | (F.abs(v) == float("inf")), None).otherwise(v).alias(c)


def parse(bronze: DataFrame) -> DataFrame:
    """Pure transformation: bronze rows → typed rows + `dq_reason` (NULL = good)."""
    p = bronze.select("*", F.from_json("value", PAYLOAD_SCHEMA).alias("p"))
    orig_ts = parse_event_time(F.col("p.timestamp"))
    replay_ts = F.expr("timestamp_millis(cast(p.replay_ts_ms as bigint))")
    label = clean_label(F.col("p.label"))
    # concat_ws skips NULLs, so backfill rows (no run_id) hash exactly as before; each live replay
    # run gets fresh uids (re-runnable demo), while retries within a run still dedupe.
    uid = F.sha2(F.concat_ws("|", *[F.coalesce(F.col(f"p.{c}"), F.lit("")) for c in RAW_COLS],
                             F.col("p.run_id")), 256)

    df = p.select(
        uid.alias("flow_uid"),
        F.col("p.flow_id").alias("flow_id"),
        F.col("p.source_ip").alias("src_ip"),
        F.col("p.source_port").cast("int").alias("src_port"),
        F.col("p.destination_ip").alias("dst_ip"),
        F.col("p.destination_port").cast("int").alias("dst_port"),
        F.col("p.protocol").cast("int").alias("protocol"),
        orig_ts.alias("orig_ts"),
        # Live replay is re-based onto the wall clock; backfill keeps the 2017 capture time
        F.coalesce(replay_ts, orig_ts).alias("event_time"),
        F.to_date(orig_ts).alias("day"),
        *[_num(c) for c in NUMERIC_COLS],
        label.alias("label"),
        label_family(label).alias("label_family"),
        F.col("p.split").alias("split"),
        F.col("p.source_file").alias("source_file"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        "ingest_ts",
        "value",
    )
    dq = (
        F.when(F.col("flow_id").isNull(), "unparseable_payload")
        .when(F.col("orig_ts").isNull(), "bad_timestamp")
        .when(F.col("flow_duration") < 0, "negative_duration")
        .when(F.col("label_family").isNull(), "unknown_label")
    )
    return (df.withColumn("is_attack", (F.col("label_family") != "Normal Traffic").cast("int"))
              .withColumn("dq_reason", dq))


def enrich(df: DataFrame, spark: SparkSession) -> DataFrame:
    """Stream-static broadcast joins: zone of both endpoints + destination service."""
    subnets = F.broadcast(dim_subnets(spark))
    ports = F.broadcast(dim_ports(spark))
    net = lambda c: F.regexp_extract(F.col(c), r"^(\d+\.\d+\.\d+)\.", 1)  # noqa: E731
    s = subnets.select(F.col("net_prefix").alias("_s_net"), F.col("zone").alias("src_zone"))
    d = subnets.select(F.col("net_prefix").alias("_d_net"), F.col("zone").alias("dst_zone"))
    return (
        df.withColumn("_s_net", net("src_ip")).withColumn("_d_net", net("dst_ip"))
        .join(s, "_s_net", "left").join(d, "_d_net", "left")
        .join(ports.withColumnRenamed("port", "dst_port"), "dst_port", "left")
        .withColumn("src_zone", F.coalesce("src_zone", F.lit("external")))
        .withColumn("dst_zone", F.coalesce("dst_zone", F.lit("external")))
        .withColumn("service", F.coalesce("service", F.lit("other")))
        .drop("_s_net", "_d_net")
    )


def silver_columns(df: DataFrame) -> DataFrame:
    return df.select(*KEY_COLS, *NUMERIC_COLS, *LABEL_COLS, *ENRICH_COLS, *META_COLS)


def quarantine_columns(df: DataFrame) -> DataFrame:
    return df.select("dq_reason", "value", "kafka_partition", "kafka_offset", "ingest_ts")


def silver_schema(spark: SparkSession) -> StructType:
    empty = spark.createDataFrame([], BRONZE_SCHEMA)
    return silver_columns(enrich(parse(empty), spark)).schema


def quarantine_schema(spark: SparkSession) -> StructType:
    return quarantine_columns(parse(spark.createDataFrame([], BRONZE_SCHEMA))).schema


def ensure_tables(spark: SparkSession) -> None:
    ensure_table(spark, lake("bronze_flows"), BRONZE_SCHEMA)
    ensure_table(spark, lake("silver_flows"), silver_schema(spark), ["day"])
    ensure_table(spark, lake("silver_quarantine"), quarantine_schema(spark))


def write_batch(batch: DataFrame, batch_id: int) -> None:
    batch.persist()
    try:
        bad = batch.filter(F.col("dq_reason").isNotNull())
        good = batch.filter(F.col("dq_reason").isNull())

        if not bad.isEmpty():
            q = quarantine_columns(bad)
            idempotent_append(q, lake("silver_quarantine"), query_app_id(QUERY), batch_id)
            if kafka_sinks_enabled():
                (q.select(F.col("dq_reason").alias("key"),
                          F.to_json(F.struct(*q.columns)).alias("value"))
                  .write.format("kafka")
                  .option("kafka.bootstrap.servers", kafka_bootstrap())
                  .option("topic", topic("dlq")).save())

        # Idempotent append (txnAppId/txnVersion): a retried batch is skipped, so the sink stays
        # exactly-once. It used to be an insert-only MERGE on flow_uid, which also caught duplicates
        # across batches, but it re-scanned the whole silver table every batch (~2.5 min per batch
        # at 800K rows). Duplicates within a batch are still dropped here.
        good = silver_columns(good).dropDuplicates(["flow_uid"])
        idempotent_append(good, lake("silver_flows"), query_app_id(QUERY), batch_id)
    finally:
        batch.unpersist()


def start(spark: SparkSession) -> StreamingQuery:
    stream = enrich(parse(stream_source(spark, lake("bronze_flows"))), spark)
    spark.sparkContext.setLocalProperty("spark.scheduler.pool", QUERY)
    return (
        stream.writeStream.queryName(QUERY)
        .foreachBatch(write_batch)
        .option("checkpointLocation", checkpoint(QUERY))
        .trigger(**trigger())
        .start()
    )
