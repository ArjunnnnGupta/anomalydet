"""
src/streaming/gold.py — Q3a/Q3b: event-time windowed aggregations over the silver stream.

  Q3a  attack timeline   1-minute TUMBLING windows × split × label_family
  Q3b  host profile      1-minute windows per source IP (length/slide set in pipeline.yaml;
                         equal = tumbling, a smaller slide = sliding windows):
                         flows, bytes, approx distinct destinations / ports (HyperLogLog),
                         SYN/RST ratios, attack flows → also derives port-scan candidates

Both use a watermark: Spark keeps window state only until the watermark passes the
window end, which bounds memory and tolerates out-of-order events. Output mode is
UPDATE (changed windows only), upserted into Delta gold tables (MERGE). The busiest host
windows and port-scan candidates are also mirrored into SQLite for the dashboard.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.types import (DoubleType, LongType, StringType, StructField, StructType,
                               TimestampType)

from src.common.config import cfg, checkpoint, lake, trigger
from src.common.lake import ensure_table, merge_upsert, stream_source
from src.serving import sqlite_sink

Q_TIMELINE = "q3a_gold_timeline"
Q_HOSTS = "q3b_gold_hosts"

_ts, _str, _long, _dbl = TimestampType(), StringType(), LongType(), DoubleType()

TIMELINE_SCHEMA = StructType([StructField(n, t) for n, t in [
    ("window_start", _ts), ("window_end", _ts), ("split", _str), ("label_family", _str),
    ("flows", _long), ("bytes", _dbl), ("distinct_src_ips", _long)]])

HOST_SCHEMA = StructType([StructField(n, t) for n, t in [
    ("window_start", _ts), ("window_end", _ts), ("split", _str), ("src_ip", _str), ("src_zone", _str),
    ("flows", _long), ("bytes", _dbl), ("distinct_dst_ips", _long), ("distinct_dst_ports", _long),
    ("syn_ratio", _dbl), ("rst_ratio", _dbl), ("attack_flows", _long)]])

PORTSCAN_SCHEMA = StructType([StructField(n, t) for n, t in [
    ("window_start", _ts), ("window_end", _ts), ("split", _str), ("src_ip", _str),
    ("distinct_dst_ports", _long), ("flows", _long)]])

def _bytes():
    return (F.coalesce(F.col("total_length_of_fwd_packets"), F.lit(0.0))
            + F.coalesce(F.col("total_length_of_bwd_packets"), F.lit(0.0)))


def timeline_agg(silver: DataFrame) -> DataFrame:
    s = cfg()["streaming"]
    return (
        silver.withWatermark("event_time", s["watermark"])
        .groupBy(F.window("event_time", s["timeline_window"]), "split", "label_family")
        .agg(F.count("*").alias("flows"),
             F.sum(_bytes()).alias("bytes"),
             F.approx_count_distinct("src_ip").alias("distinct_src_ips"))
        .select(F.col("window.start").alias("window_start"), F.col("window.end").alias("window_end"),
                "split", "label_family", "flows", "bytes", "distinct_src_ips")
    )


def host_agg(silver: DataFrame) -> DataFrame:
    s = cfg()["streaming"]
    return (
        silver.withWatermark("event_time", s["watermark"])
        .groupBy(F.window("event_time", s["host_window"], s["host_slide"]), "split", "src_ip")
        .agg(F.first("src_zone").alias("src_zone"),
             F.count("*").alias("flows"),
             F.sum(_bytes()).alias("bytes"),
             F.approx_count_distinct("dst_ip").alias("distinct_dst_ips"),
             F.approx_count_distinct("dst_port").alias("distinct_dst_ports"),
             (F.sum(F.when(F.col("syn_flag_count") > 0, 1).otherwise(0)) / F.count("*")).alias("syn_ratio"),
             (F.sum(F.when(F.col("rst_flag_count") > 0, 1).otherwise(0)) / F.count("*")).alias("rst_ratio"),
             F.sum("is_attack").cast("long").alias("attack_flows"))
        .select(F.col("window.start").alias("window_start"), F.col("window.end").alias("window_end"),
                "split", "src_ip", "src_zone", "flows", "bytes", "distinct_dst_ips",
                "distinct_dst_ports", "syn_ratio", "rst_ratio", "attack_flows")
    )


def port_scan_candidates(hosts: DataFrame) -> DataFrame:
    n = cfg()["streaming"]["port_scan_distinct_ports"]
    return hosts.filter(F.col("distinct_dst_ports") > n).select(
        "window_start", "window_end", "split", "src_ip", "distinct_dst_ports", "flows")


def ensure_tables(spark: SparkSession) -> None:
    ensure_table(spark, lake("gold_attack_timeline"), TIMELINE_SCHEMA, ["split"])
    ensure_table(spark, lake("gold_host_profile"), HOST_SCHEMA, ["split"])
    ensure_table(spark, lake("gold_port_scan"), PORTSCAN_SCHEMA)


def _write_timeline(batch: DataFrame, _batch_id: int) -> None:
    # The dashboard reads this table straight from Delta (delta-rs), so no SQLite mirror
    batch = batch.persist()   # merge_upsert reads the batch more than once
    try:
        merge_upsert(batch.sparkSession, batch, lake("gold_attack_timeline"),
                     ["window_start", "split", "label_family"],
                     prune_partitions="split", min_col="window_start")
    finally:
        batch.unpersist()


def _write_hosts(batch: DataFrame, _batch_id: int) -> None:
    batch = batch.persist()
    try:
        spark = batch.sparkSession
        merge_upsert(spark, batch, lake("gold_host_profile"), ["window_start", "split", "src_ip"],
                     prune_partitions="split", min_col="window_start")
        scans = port_scan_candidates(batch).persist()
        if not scans.isEmpty():
            merge_upsert(spark, scans, lake("gold_port_scan"), ["window_start", "split", "src_ip"],
                         min_col="window_start")
            sqlite_sink.upsert_df("port_scan_candidates", scans)
        scans.unpersist()
        # Only the busiest hosts go to the serving cache; the full profile stays in Delta
        sqlite_sink.upsert_df("host_window_stats", batch.orderBy(F.desc("flows")).limit(2000))
    finally:
        batch.unpersist()


def start(spark: SparkSession) -> list[StreamingQuery]:
    silver = stream_source(spark, lake("silver_flows"))
    queries = []
    for name, agg, sink in [(Q_TIMELINE, timeline_agg, _write_timeline), (Q_HOSTS, host_agg, _write_hosts)]:
        spark.sparkContext.setLocalProperty("spark.scheduler.pool", name)
        queries.append(
            agg(silver).writeStream.queryName(name)
            .outputMode("update")
            .foreachBatch(sink)
            .option("checkpointLocation", checkpoint(name))
            .trigger(**trigger())
            .start()
        )
    return queries
