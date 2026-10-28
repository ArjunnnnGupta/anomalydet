"""
src/common/spark.py — one SparkSession builder for every job.

Inside the cluster, jobs are launched with `spark-submit --master spark://spark-master:7077`
and the Kafka + Delta jars are baked into the image. On the host (tests), the
Delta jars come from delta-spark's pip helper and the master defaults to local[*].
"""

from __future__ import annotations

import os
import sys

from pyspark.sql import SparkSession

from src.common.config import cfg, in_docker, path

# Same JDK fix the old scripts used: PySpark 3.5 needs Java 17 on macOS hosts.
_BREW_JDK17 = "/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home"
if "JAVA_HOME" not in os.environ and os.path.exists(_BREW_JDK17):
    os.environ["JAVA_HOME"] = _BREW_JDK17
# Python UDF workers must use the same interpreter (and venv) as the driver
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)


def get_spark(app: str, master: str | None = None, streaming: bool = False) -> SparkSession:
    s = cfg()["spark"]
    event_dir = path(s["event_log_dir"])
    os.makedirs(event_dir, exist_ok=True)

    b = (
        SparkSession.builder.appName(app)
        # Delta Lake: ACID tables, MERGE, time travel
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        # Adaptive Query Execution: runtime partition coalescing, skew-join splitting
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        .config("spark.sql.shuffle.partitions", str(s["shuffle_partitions"]))
        # Delta rebuilds its transaction-log snapshot with 50 partitions by default (sized for big
        # clusters). On 2 cores every Delta-reading micro-batch queued 50 tiny tasks and scoring
        # starved, so a 15-row batch took minutes. Match it to the cluster instead.
        .config("spark.databricks.delta.snapshotPartitions", str(s["delta_snapshot_partitions"]))
        .config("spark.sql.autoBroadcastJoinThreshold", str(10 * 1024 * 1024))
        # Streaming state lives in RocksDB (off-heap), not the JVM heap
        .config("spark.sql.streaming.stateStore.providerClass",
                "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider")
        # ...and its native memory is capped, so it can't push the worker container past its limit
        .config("spark.sql.streaming.stateStore.rocksdb.boundedMemoryUsage", "true")
        .config("spark.sql.streaming.stateStore.rocksdb.maxMemoryUsageMB", "128")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.eventLog.enabled", "true")
        .config("spark.eventLog.dir", f"file://{event_dir}")
        .config("spark.sql.session.timeZone", "UTC")
        # Unparseable timestamps become NULL (→ quarantine) instead of raising
        .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
    )

    if streaming:
        # Several streaming queries share one app: FAIR scheduling stops one query starving the rest
        b = b.config("spark.scheduler.mode", "FAIR").config("spark.cores.max", str(s["stream_cores_max"]))

    master = master or os.environ.get("SPARK_MASTER")
    if master:
        b = b.master(master)

    if in_docker():
        b = (
            b.config("spark.executor.memory", s["executor_memory"])
            # streaming: two 1-core executors, one per worker (spreads memory); batch: 2-core executors
            .config("spark.executor.cores", str(s["stream_executor_cores"] if streaming else s["executor_cores"]))
            .config("spark.executor.extraJavaOptions",
                    f"-XX:MaxDirectMemorySize={s['executor_direct_memory']}")
        )
    else:
        # Host / tests: pull Delta (+ Kafka connector) jars through Ivy
        from delta import configure_spark_with_delta_pip
        if not master:
            b = b.master("local[*]")
        b = configure_spark_with_delta_pip(
            b, extra_packages=["org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3"]
        ).config("spark.driver.memory", s["driver_memory"])

    spark = b.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
