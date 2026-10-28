"""
src/streaming/bronze.py — Q1: Kafka flows.raw → Delta bronze (raw, append-only, replayable).

Bronze stores the payload exactly as received plus its Kafka coordinates
(topic, partition, offset). The Kafka source + checkpointed Delta sink gives
exactly-once ingestion: on restart Spark resumes from the offsets in the
checkpoint, and Delta's transaction log ignores half-written batches.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.types import (DateType, IntegerType, LongType, StringType, StructField,
                               StructType, TimestampType)

from src.common.config import cfg, checkpoint, kafka_bootstrap, lake, topic, trigger

QUERY = "q1_bronze"

BRONZE_SCHEMA = StructType([
    StructField("kafka_key", StringType()),
    StructField("value", StringType()),
    StructField("topic", StringType()),
    StructField("partition", IntegerType()),
    StructField("offset", LongType()),
    StructField("kafka_ts", TimestampType()),
    StructField("ingest_ts", TimestampType()),
    StructField("ingest_date", DateType()),
])


def to_bronze(kafka_df: DataFrame) -> DataFrame:
    return (
        kafka_df.select(
            F.col("key").cast("string").alias("kafka_key"),
            F.col("value").cast("string").alias("value"),
            "topic", "partition", "offset",
            F.col("timestamp").alias("kafka_ts"),
        )
        .withColumn("ingest_ts", F.current_timestamp())
        .withColumn("ingest_date", F.to_date("ingest_ts"))
    )


def start(spark: SparkSession) -> StreamingQuery:
    s = cfg()["streaming"]
    src = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap())
        .option("subscribe", topic("raw"))
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", s["max_offsets_per_trigger"])   # backpressure
        .option("failOnDataLoss", "true")       # never skip offsets silently: fail loudly instead
        .load()
    )
    spark.sparkContext.setLocalProperty("spark.scheduler.pool", QUERY)
    return (
        to_bronze(src).writeStream.format("delta")
        .queryName(QUERY)
        .outputMode("append")
        # Not partitioned: a partitioned write sorts every micro-batch by the partition column, and
        # sorting 50k raw JSON rows ran the 640 MB executor out of memory in the first cluster run.
        # Every row of a batch has the same ingest_date anyway, so partitioning bought nothing here.
        .option("checkpointLocation", checkpoint(QUERY))
        .trigger(**trigger())
        .start(lake("bronze_flows"))
    )
