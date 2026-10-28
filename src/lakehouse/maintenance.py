#!/usr/bin/env python3
"""
src/lakehouse/maintenance.py — Delta Lake operations on the tables the stream builds.

Not an ingestion path (Kappa: data only enters through Kafka). These are the
housekeeping and inspection jobs every streaming lakehouse needs:

  1. table inventory     row counts, file counts, size per table
  2. OPTIMIZE            bin-pack the many small files that 5-second micro-batches create
  3. ZORDER BY (src_ip)  co-locate a host's rows → data skipping on host lookups
  4. DESCRIBE HISTORY    the ACID transaction log (every streaming commit, MERGE, OPTIMIZE)
  5. time travel         query silver as of an older version, diff the row counts
  6. schema evolution    append a batch with a new column using mergeSchema
  7. VACUUM              delete files no longer referenced (retention-guarded)

Run inside the cluster:  make maintenance
"""

import argparse

from delta.tables import DeltaTable
from pyspark.sql import functions as F

from src.common.config import lake
from src.common.spark import get_spark

TABLES = ["bronze_flows", "silver_flows", "silver_quarantine", "gold_attack_timeline",
          "gold_host_profile", "gold_port_scan", "gold_scored", "gold_model_predictions"]

ZORDER = {"silver_flows": "src_ip", "gold_scored": "src_ip", "gold_host_profile": "src_ip"}


def banner(t):
    print(f"\n{'─' * 64}\n{t}\n{'─' * 64}")


def run(spark, vacuum_hours: int = 168, skip_optimize: bool = False) -> None:

    banner("1. Table inventory (DESCRIBE DETAIL)")
    for t in TABLES:
        p = lake(t)
        if not DeltaTable.isDeltaTable(spark, p):
            print(f"  {t:<24} (not created yet)")
            continue
        d = spark.sql(f"DESCRIBE DETAIL delta.`{p}`").collect()[0]
        rows = spark.read.format("delta").load(p).count()
        print(f"  {t:<24} rows={rows:>11,}  files={d['numFiles']:>6,}  "
              f"size={d['sizeInBytes'] / 1e6:>9,.1f} MB  partitions={d['partitionColumns']}")

    if not skip_optimize:
        banner("2–3. OPTIMIZE (+ ZORDER) — compact small streaming files")
        for t in TABLES:
            p = lake(t)
            if not DeltaTable.isDeltaTable(spark, p):
                continue
            opt = DeltaTable.forPath(spark, p).optimize()
            res = opt.executeZOrderBy(ZORDER[t]) if t in ZORDER else opt.executeCompaction()
            m = res.select("metrics.numFilesAdded", "metrics.numFilesRemoved").collect()[0]
            how = f"ZORDER BY ({ZORDER[t]})" if t in ZORDER else "compaction"
            print(f"  {t:<24} {how:<20} files removed={m[1]:>6,}  added={m[0]:>4,}")

    silver = lake("silver_flows")
    if DeltaTable.isDeltaTable(spark, silver):
        banner("4. DESCRIBE HISTORY silver_flows (last 10 commits)")
        (DeltaTable.forPath(spark, silver).history(10)
         .select("version", "timestamp", "operation",
                 F.col("operationMetrics.numTargetRowsInserted").alias("rows_inserted"),
                 F.col("operationMetrics.numOutputRows").alias("rows_out"))
         .show(truncate=False))

        banner("5. Time travel — silver row count per version")
        latest = DeltaTable.forPath(spark, silver).history(1).collect()[0]["version"]
        for v in sorted({0, latest // 4, latest // 2, latest}):
            n = spark.read.format("delta").option("versionAsOf", v).load(silver).count()
            print(f"  VERSION AS OF {v:<6} → {n:>11,} rows")
        print("\n  SQL form:  SELECT label_family, count(*) FROM delta.`<silver>` VERSION AS OF 0 GROUP BY 1")

    banner("6. Schema evolution — mergeSchema on a scratch copy of gold_attack_timeline")
    tl = lake("gold_attack_timeline")
    if DeltaTable.isDeltaTable(spark, tl):
        scratch = lake("root") + "/_scratch/timeline_evolved"
        base = spark.read.format("delta").load(tl).limit(100)
        base.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(scratch)
        (base.withColumn("attack_share", F.when(F.col("label_family") != "Normal Traffic", 1.0).otherwise(0.0))
         .write.format("delta").mode("append").option("mergeSchema", "true").save(scratch))
        print("  New column `attack_share` added without rewriting existing files:")
        spark.read.format("delta").load(scratch).printSchema()

    banner(f"7. VACUUM (retain {vacuum_hours} h)")
    if vacuum_hours < 168:
        spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
    for t in TABLES:
        p = lake(t)
        if DeltaTable.isDeltaTable(spark, p):
            DeltaTable.forPath(spark, p).vacuum(vacuum_hours)
            print(f"  vacuumed {t}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--vacuum-hours", type=int, default=168, help="VACUUM retention (default 7 days)")
    ap.add_argument("--skip-optimize", action="store_true")
    args = ap.parse_args()
    spark = get_spark("lakehouse-maintenance")
    run(spark, args.vacuum_hours, args.skip_optimize)
    spark.stop()


if __name__ == "__main__":
    main()
