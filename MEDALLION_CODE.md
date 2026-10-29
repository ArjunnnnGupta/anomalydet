# Bronze, Silver and Gold: the Code Line by Line

This guide explains the three medallion files line by line, in easy words:

- [src/streaming/bronze.py](src/streaming/bronze.py): **query 1**, saves the raw copy
- [src/streaming/silver.py](src/streaming/silver.py): **query 2**, cleans the data
- [src/streaming/gold.py](src/streaming/gold.py): **queries 3a and 3b**, makes summaries

Line numbers match the files, so you can keep the file open next to this guide. For the big
picture first, read [WORKFLOW.md](WORKFLOW.md).

```
Kafka flows.raw ──► bronze.py ──► Delta bronze_flows ──► silver.py ──► Delta silver_flows ──► gold.py ──► Delta gold_*
                    (raw copy)                           (cleaned)                            (per-minute summaries)
```

### Three ideas you need first

| Idea | In easy words |
|---|---|
| **DataFrame** | A table inside Spark (rows and columns). Spark splits it across workers so it can handle millions of rows. |
| **Streaming query** | A job that never ends. Every 5 seconds it takes the *new* rows, processes them as a small **micro-batch**, saves the result, and waits for more. |
| **Checkpoint** | A bookmark folder (`data/checkpoints/<query>/`) where the query notes how far it has got. After a crash it continues from the bookmark: nothing lost, nothing done twice. |

---

# Part 1: `bronze.py` (Kafka → raw copy)

**Job:** read every message from Kafka and save it **unchanged** in the Delta table `bronze_flows`.

### Lines 1–8: the docstring (description of the file)
```python
"""
src/streaming/bronze.py — Q1: Kafka flows.raw → Delta bronze (raw, append-only, replayable).
...
"""
```
A comment block explaining the file:
- Bronze keeps each message exactly as received, plus *where* it came from in Kafka (topic, partition, offset).
- Checkpoint + Delta together give **exactly-once**: each message is saved once, never lost and never duplicated.

### Lines 10–16: imports
```python
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.types import (DateType, IntegerType, LongType, StringType, StructField, StructType, TimestampType)
from src.common.config import cfg, checkpoint, kafka_bootstrap, lake, topic, trigger
```
- **`DataFrame`, `SparkSession`:** Spark's table type, and the connection to the Spark cluster.
- **`functions as F`:** Spark's built-in functions (`F.col`, `F.current_timestamp`…), shortened to `F`.
- **`StreamingQuery`:** the type of a running streaming job.
- **The `types`:** column data types (text, whole number, timestamp…), used to describe the table's columns.
- **The last line:** helpers from our own `config.py`:
  - `cfg()` gives the settings;
  - `checkpoint()` gives the bookmark folder;
  - `kafka_bootstrap()` gives the Kafka address;
  - `lake()` gives the table path;
  - `topic()` gives the topic name;
  - `trigger()` gives "every 5 seconds".

### Line 18: the query's name
```python
QUERY = "q1_bronze"
```
The name shown in the logs, on the dashboard, and used for the checkpoint folder.

### Lines 20–29: the bronze table's columns (schema)
```python
BRONZE_SCHEMA = StructType([
    StructField("kafka_key", StringType()),     # the message key = source IP
    StructField("value", StringType()),         # the whole flow as JSON text
    StructField("topic", StringType()),         # which topic it came from (flows.raw)
    StructField("partition", IntegerType()),    # which of the 12 partitions
    StructField("offset", LongType()),          # its position number inside that partition
    StructField("kafka_ts", TimestampType()),   # when Kafka received it
    StructField("ingest_ts", TimestampType()),  # when Spark saved it
    StructField("ingest_date", DateType()),     # the date part of ingest_ts
])
```
A list of the 8 columns of the bronze table and their types. Other files use it too: silver uses it to create empty tables of the right shape.

**Why keep partition + offset?** They are the message's exact "address" in Kafka, so any bronze row can be traced back to its original message.

### Lines 32–42: `to_bronze()`, turning a Kafka message into a bronze row
```python
def to_bronze(kafka_df):
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
```
- Kafka gives the key and value as raw **bytes**. `.cast("string")` turns them into readable text.
- **`.alias(...)`** renames a column, so `key` becomes `kafka_key`.
- `topic`, `partition` and `offset` are kept as they are. Kafka's `timestamp` is renamed `kafka_ts`.
- **`withColumn("ingest_ts", current_timestamp())`** adds "the time we saved this", and `ingest_date` takes just the date part.

The JSON is **not** opened or cleaned here. That's silver's job. Bronze stays a pure copy.

### Lines 45–55: `start()`, part 1: connecting to Kafka
```python
def start(spark):
    s = cfg()["streaming"]
    src = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap())
        .option("subscribe", topic("raw"))
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", s["max_offsets_per_trigger"])   # backpressure
        .option("failOnDataLoss", "true")
        .load()
    )
```
| Line | Meaning |
|---|---|
| `s = cfg()["streaming"]` | Load the streaming settings from `pipeline.yaml` |
| `readStream.format("kafka")` | "Read continuously, from Kafka" (`readStream` = never-ending; `read` would be one-off) |
| `kafka.bootstrap.servers` | The address of the Kafka broker |
| `subscribe` = `flows.raw` | Which topic to listen to |
| `startingOffsets = earliest` | On the very first run, start from the oldest message, so nothing is missed. After that, the checkpoint decides where to continue |
| `maxOffsetsPerTrigger = 20000` | **Backpressure:** take at most 20,000 messages per batch, so a big backlog never floods memory |
| `failOnDataLoss = true` | If messages Spark expected have disappeared from Kafka, **stop with an error** instead of silently skipping them. We added this after Kafka once deleted data because of old 2017 timestamps |
| `.load()` | Create the streaming DataFrame (no data moves yet) |

### Line 56: CPU sharing
```python
spark.sparkContext.setLocalProperty("spark.scheduler.pool", QUERY)
```
Puts this query in its own "pool" in Spark's **FAIR scheduler**, so the 5 queries share the CPU fairly and none of them starves the others.

### Lines 57–67: `start()`, part 2: writing to Delta
```python
    return (
        to_bronze(src).writeStream.format("delta")
        .queryName(QUERY)
        .outputMode("append")
        .option("checkpointLocation", checkpoint(QUERY))
        .trigger(**trigger())
        .start(lake("bronze_flows"))
    )
```
| Line | Meaning |
|---|---|
| `to_bronze(src)` | Apply the conversion from lines 32–42 to every incoming message |
| `writeStream.format("delta")` | Write continuously into a **Delta Lake** table |
| `queryName(QUERY)` | Name the query `q1_bronze` |
| `outputMode("append")` | Only add new rows; never change old ones |
| `checkpointLocation` | The bookmark folder `data/checkpoints/q1_bronze/`, which stores the last Kafka offsets saved |
| `trigger(**trigger())` | Run a micro-batch every 5 seconds |
| `.start(lake("bronze_flows"))` | **Start the query**, writing to `data/lake/bronze/flows` |

The comment in lines 61–63 explains why bronze is **not partitioned**: a partitioned write sorted each batch and ran out of memory on the laptop.

---

# Part 2: `silver.py` (raw copy → clean data)

**Job:** read bronze, open the JSON, clean and check every record, add extra information, and save the good rows to `silver_flows`. Bad rows go to quarantine.

### Lines 1–11: the docstring
The file's to-do list: parse JSON, fix timestamps, clean numbers, clean labels, apply data-quality rules, broadcast joins, and an idempotent (no-duplicate) write.

### Lines 13–22: imports
```python
from src.common.config import checkpoint, kafka_bootstrap, kafka_sinks_enabled, lake, topic, trigger
from src.common.dims import dim_ports, dim_subnets
from src.common.lake import ensure_table, idempotent_append, query_app_id, stream_source
from src.common.schemas import NUMERIC_COLS, PAYLOAD_SCHEMA, RAW_COLS, clean_label, label_family
from src.streaming.bronze import BRONZE_SCHEMA
```
- **`dims`:** the two small lookup tables (subnet → zone, port → service).
- **`lake`:** helpers to create tables, read a Delta table as a stream, and write without duplicates.
- **`schemas`:** the list of column names, the JSON layout (`PAYLOAD_SCHEMA`), and the label-cleaning functions.
- **`BRONZE_SCHEMA`:** bronze's column list, reused from bronze.py.

### Lines 24–29: names and column groups
```python
QUERY = "q2_silver"
KEY_COLS = ["flow_uid", "flow_id", "src_ip", "src_port", "dst_ip", "dst_port", "protocol", "orig_ts", "event_time", "day"]
LABEL_COLS = ["label", "label_family", "is_attack"]
META_COLS = ["split", "source_file", "kafka_partition", "kafka_offset", "ingest_ts"]
ENRICH_COLS = ["src_zone", "dst_zone", "service"]
```
The query name, and four lists of columns grouped by purpose. They are used later to put silver's columns in a fixed order:
- **who/when:** the IDs and times;
- **the answer:** the labels;
- **where it came from:** the metadata;
- **added information:** the enrichment columns.

### Lines 33–36: `parse_event_time()`, fixing the timestamps
```python
def parse_event_time(ts):
    t = F.coalesce(F.to_timestamp(ts, "d/M/yyyy H:mm:ss"), F.to_timestamp(ts, "d/M/yyyy H:mm"))
    return F.when(F.hour(t) < 8, t + F.expr("INTERVAL 12 HOURS")).otherwise(t)
```
- CICIDS2017 writes times in **two formats**: `3/7/2017 9:15:42` (with seconds) and `3/7/2017 9:15` (without). Each **`to_timestamp`** tries one format and gives NULL if it doesn't fit, and **`coalesce`** keeps the first one that worked.
- The afternoon files use a **12-hour clock without AM/PM**, so 1:30 PM is written as `1:30`. The office hours were 8:00–17:00, so **any hour before 8 must be afternoon**, and 12 hours are added (`1:30` → `13:30`).

### Lines 39–41: `_num()`, cleaning one number column
```python
def _num(c):
    v = F.col(f"p.{c}").cast("double")
    return F.when(F.isnan(v) | (F.abs(v) == float("inf")), None).otherwise(v).alias(c)
```
- `.cast("double")` turns the text `"123.5"` into the number 123.5.
- Some columns contain **`Infinity` or `NaN`**, from dividing by a flow duration of 0. `when(... , None)` replaces them with **NULL** (empty), because a fake infinite value would confuse the model. Training fills NULLs later with the Imputer.

### Lines 44–84: `parse()`, the main cleaning step
**Line 46: open the JSON.**
```python
p = bronze.select("*", F.from_json("value", PAYLOAD_SCHEMA).alias("p"))
```
`from_json` reads the JSON text in `value` using a **fixed layout** (`PAYLOAD_SCHEMA`) and puts all its fields under `p` (`p.source_ip`, `p.label`…). A fixed layout is faster and safer than letting Spark guess.

**Lines 47–49: prepare three values.**
```python
orig_ts = parse_event_time(F.col("p.timestamp"))
replay_ts = F.expr("timestamp_millis(cast(p.replay_ts_ms as bigint))")
label = clean_label(F.col("p.label"))
```
- `orig_ts`: the original 2017 time, fixed with the function above.
- `replay_ts`: for **live** replays, the producer sends a "now" time in milliseconds, which is turned into a timestamp here. Backfill rows don't have it.
- `label`: the label with the broken Windows dash replaced and spaces tidied (`clean_label`).

**Lines 50–53: the fingerprint (`flow_uid`).**
```python
uid = F.sha2(F.concat_ws("|", *[F.coalesce(F.col(f"p.{c}"), F.lit("")) for c in RAW_COLS], F.col("p.run_id")), 256)
```
- Join **all raw columns** into one long text, separated by `|` (`concat_ws`), and compute its **SHA-256 hash**, a unique 64-character code.
- **Identical rows give the same code**, which is how duplicates are found.
- `run_id` is added so each *new* live replay counts as new flows, while a retry within the same run still dedupes. (The comment in lines 50–51 explains this.)

**Lines 55–76: build the clean row.**
```python
df = p.select(
    uid.alias("flow_uid"),
    F.col("p.flow_id").alias("flow_id"),
    F.col("p.source_ip").alias("src_ip"),
    F.col("p.source_port").cast("int").alias("src_port"),
    ...
    orig_ts.alias("orig_ts"),
    F.coalesce(replay_ts, orig_ts).alias("event_time"),
    F.to_date(orig_ts).alias("day"),
    *[_num(c) for c in NUMERIC_COLS],
    label.alias("label"),
    label_family(label).alias("label_family"),
    ...
)
```
- Each line picks one field, **converts its type** (ports and protocol become whole numbers), and gives it a short name (`source_ip` → `src_ip`).
- **`event_time`** is the time used for windows: the live "now" time if there is one, otherwise the 2017 time.
- **`day`** is the date, used to partition the silver table.
- **`*[_num(c) for c in NUMERIC_COLS]`** cleans all ~78 numeric columns in one line, using `_num` from above.
- **`label_family(label)`** groups 15 labels into 8 families (e.g. `DoS Hulk` → `DoS`).
- The Kafka partition/offset and `value` are kept for traceability and the quarantine table.

**Lines 77–82: the 4 data-quality rules.**
```python
dq = (
    F.when(F.col("flow_id").isNull(), "unparseable_payload")
    .when(F.col("orig_ts").isNull(), "bad_timestamp")
    .when(F.col("flow_duration") < 0, "negative_duration")
    .when(F.col("label_family").isNull(), "unknown_label")
)
```
A chain of checks, like `if … elif …`:

| Problem | `dq_reason` it gets |
|---|---|
| JSON couldn't be read | `unparseable_payload` |
| Time couldn't be read | `bad_timestamp` |
| Duration below 0 (impossible) | `negative_duration` |
| Unknown label | `unknown_label` |

A good row matches none of these, so its `dq_reason` stays **NULL**.

**Lines 83–84: two last columns.**
```python
return (df.withColumn("is_attack", (F.col("label_family") != "Normal Traffic").cast("int"))
          .withColumn("dq_reason", dq))
```
- `is_attack` = 1 if the flow is any kind of attack, 0 if normal.
- `dq_reason` = the result of the checks above.

`parse()` is a "pure" function: it only transforms data and doesn't save anything. That makes it easy to test.

### Lines 87–102: `enrich()`, adding information with broadcast joins
```python
subnets = F.broadcast(dim_subnets(spark))
ports = F.broadcast(dim_ports(spark))
net = lambda c: F.regexp_extract(F.col(c), r"^(\d+\.\d+\.\d+)\.", 1)
...
.join(s, "_s_net", "left").join(d, "_d_net", "left")
.join(ports.withColumnRenamed("port", "dst_port"), "dst_port", "left")
.withColumn("src_zone", F.coalesce("src_zone", F.lit("external")))
.withColumn("service", F.coalesce("service", F.lit("other")))
```
- **`F.broadcast(...)`** marks the lookup tables as small. Spark sends a full copy to every worker, so the millions of flows don't have to be moved around (no shuffle). This is the **broadcast join**.
- `net(...)` uses a regex to take the first three parts of an IP (`192.168.10.50` → `192.168.10`), which are matched against the subnet table.
- **Three `left` joins** add:
  - `src_zone`: the source computer's zone (internal, attacker network…);
  - `dst_zone`: the destination's zone;
  - `service`: the port's service name (22 → SSH).

  "Left" means: keep every flow, even when there's no match.
- **`coalesce(..., "external")` / `"other"`:** no match means an outside IP or an unknown port.
- `.drop(...)` removes the temporary helper columns.

### Lines 105–119: small helper functions
- **`silver_columns()`:** picks the silver columns in a fixed order, using the lists from lines 26–29.
- **`quarantine_columns()`:** the columns kept for a bad row: the reason, the original text and its Kafka address.
- **`silver_schema()` / `quarantine_schema()`:** run the real cleaning on an **empty** table to get the exact column layout, so the empty tables can be created in advance.

### Lines 122–125: `ensure_tables()`
```python
ensure_table(spark, lake("bronze_flows"), BRONZE_SCHEMA)
ensure_table(spark, lake("silver_flows"), silver_schema(spark), ["day"])
ensure_table(spark, lake("silver_quarantine"), quarantine_schema(spark))
```
Creates the tables, empty, if they don't exist yet, so every query can start even before any data arrives. Silver is **partitioned by `day`**, so a query about one day reads only that day's folder.

### Lines 128–151: `write_batch()`, saving one micro-batch
This function runs **once for every micro-batch** (`foreachBatch`).

```python
batch.persist()
```
Keep the batch in memory, because it's used twice (good rows and bad rows). Without this, Spark would redo the cleaning twice.

```python
bad = batch.filter(F.col("dq_reason").isNotNull())
good = batch.filter(F.col("dq_reason").isNull())
```
Split the batch into **bad** rows (a reason was set) and **good** rows (no reason).

```python
if not bad.isEmpty():
    q = quarantine_columns(bad)
    idempotent_append(q, lake("silver_quarantine"), query_app_id(QUERY), batch_id)
    if kafka_sinks_enabled():
        (q.select(F.col("dq_reason").alias("key"), F.to_json(F.struct(*q.columns)).alias("value"))
          .write.format("kafka") ... .option("topic", topic("dlq")).save())
```
If there are bad rows:
1. Save them to the **quarantine** Delta table.
2. Also send them to the Kafka **dead-letter queue** (`flows.dlq`), as JSON, keyed by the reason.

Bad data is **kept for inspection, never silently deleted**. (Kafka writing is switched off in tests.)

```python
good = silver_columns(good).dropDuplicates(["flow_uid"])
idempotent_append(good, lake("silver_flows"), query_app_id(QUERY), batch_id)
```
- **`dropDuplicates(["flow_uid"])`** removes identical rows using the fingerprint.
- **`idempotent_append`** appends to silver with a "transaction ID" (the query's ID plus the batch number). If Spark retries the same batch after a failure, Delta sees that ID again and **skips it**, so there are no duplicates. This gives **exactly-once**.

The comment in lines 144–147 explains why this replaced an older MERGE: the MERGE re-read the whole silver table every batch, about 2.5 minutes per batch.

```python
finally:
    batch.unpersist()
```
Free the memory, even if something failed.

### Lines 154–163: `start()`, starting query 2
```python
stream = enrich(parse(stream_source(spark, lake("bronze_flows"))), spark)
spark.sparkContext.setLocalProperty("spark.scheduler.pool", QUERY)
return (stream.writeStream.queryName(QUERY)
        .foreachBatch(write_batch)
        .option("checkpointLocation", checkpoint(QUERY))
        .trigger(**trigger())
        .start())
```
- Read inside out: **read bronze as a stream** (`stream_source`) → **clean** (`parse`) → **add information** (`enrich`).
  - `stream_source` reads a Delta table as a stream: each new commit to bronze becomes new input.
  - It's rate-limited too (24 files / 16 MB per batch), so a backlog can't flood memory.
- Give the query its own FAIR pool.
- **`foreachBatch(write_batch)`:** for each micro-batch, call the function above, which writes to more than one place.
- Checkpoint and trigger, as in bronze. **`.start()`** sets it running.

---

# Part 3: `gold.py` (clean data → per-minute summaries)

**Job:** two queries that count the silver flows in **1-minute time windows**:
- **Q3a `q3a_gold_timeline`:** how many flows of each attack family, per minute.
- **Q3b `q3b_gold_hosts`:** for each computer (source IP) per minute: connections, data, distinct destinations and ports. This also finds **port scanners**.

### Lines 1–14: the docstring
Describes the two queries. It mentions three key ideas:
- **Watermark:** how long to wait for late data.
- **UPDATE mode:** output only the windows that changed.
- **MERGE:** update a row if it exists, insert it if not.

### Lines 16–26: imports
- `from __future__ import annotations` keeps the type hints working on the Spark image's older Python 3.8.
- From `lake`: `merge_upsert` (the MERGE helper) and `stream_source`.
- `sqlite_sink` writes to the dashboard's SQLite database.

### Lines 28–31: names and shortcuts
```python
Q_TIMELINE = "q3a_gold_timeline"
Q_HOSTS = "q3b_gold_hosts"
_ts, _str, _long, _dbl = TimestampType(), StringType(), LongType(), DoubleType()
```
The two query names, and short nicknames for column types so the schemas below are shorter.

### Lines 33–44: the three gold tables' columns
| Table | Columns | One row means… |
|---|---|---|
| `TIMELINE_SCHEMA` (`gold_attack_timeline`) | window start/end, split, family, flows, bytes, distinct source IPs | "Between 14:32 and 14:33, there were 1,860 Port Scanning flows from 1 IP" |
| `HOST_SCHEMA` (`gold_host_profile`) | window, split, src_ip, zone, flows, bytes, distinct dst IPs, **distinct dst ports**, SYN ratio, RST ratio, attack flows | "172.16.0.1, at 14:32, made 1,860 flows to 980 different ports" |
| `PORTSCAN_SCHEMA` (`gold_port_scan`) | window, split, src_ip, distinct ports, flows | Only the hosts above the port-scan limit |

### Lines 46–48: `_bytes()`
```python
return (F.coalesce(F.col("total_length_of_fwd_packets"), F.lit(0.0))
        + F.coalesce(F.col("total_length_of_bwd_packets"), F.lit(0.0)))
```
The total bytes of a flow = bytes sent forward + bytes sent back. `coalesce(..., 0)` treats an empty value as 0, so one NULL doesn't make the whole sum NULL.

### Lines 51–61: `timeline_agg()`, attacks per minute (Q3a)
```python
silver.withWatermark("event_time", s["watermark"])
.groupBy(F.window("event_time", s["timeline_window"]), "split", "label_family")
.agg(F.count("*").alias("flows"),
     F.sum(_bytes()).alias("bytes"),
     F.approx_count_distinct("src_ip").alias("distinct_src_ips"))
.select(F.col("window.start").alias("window_start"), F.col("window.end").alias("window_end"), ...)
```
| Line | Meaning |
|---|---|
| `withWatermark("event_time", "10 minutes")` | **Watermark:** "flows can arrive up to 10 minutes late". Spark keeps each minute's counter open until it's sure no more data for it will come, then closes it and frees the memory. This stops memory growing forever. |
| `F.window("event_time", "1 minute")` | Put each flow into its **1-minute bucket** by its *event time* (when it happened, not when it arrived). Buckets don't overlap: a **tumbling** window. |
| `groupBy(window, split, label_family)` | One counter per minute × backfill/live × attack family |
| `count("*")` | Number of flows |
| `sum(_bytes())` | Total data |
| `approx_count_distinct("src_ip")` | **About** how many different source IPs. It uses **HyperLogLog**, which estimates using very little memory instead of remembering every IP. This is a classic big data trick. |
| `.select(window.start, window.end, …)` | Turn the window into two plain columns: start and end |

### Lines 64–80: `host_agg()`, per-computer activity (Q3b)
```python
.groupBy(F.window("event_time", s["host_window"], s["host_slide"]), "split", "src_ip")
.agg(F.first("src_zone").alias("src_zone"),
     F.count("*").alias("flows"),
     F.sum(_bytes()).alias("bytes"),
     F.approx_count_distinct("dst_ip").alias("distinct_dst_ips"),
     F.approx_count_distinct("dst_port").alias("distinct_dst_ports"),
     (F.sum(F.when(F.col("syn_flag_count") > 0, 1).otherwise(0)) / F.count("*")).alias("syn_ratio"),
     (F.sum(F.when(F.col("rst_flag_count") > 0, 1).otherwise(0)) / F.count("*")).alias("rst_ratio"),
     F.sum("is_attack").cast("long").alias("attack_flows"))
```
- The same watermark and window idea, but grouped **per source IP**.
- `F.window(time, length, slide)`: length and slide are both 1 minute, so it's tumbling. A smaller slide would make overlapping **sliding** windows, but that's 5× the work, as the `pipeline.yaml` comment explains.
- `first("src_zone")`: the computer's zone (the same for all its flows).
- **`approx_count_distinct("dst_port")`: the key number.** How many **different ports** this computer touched in this minute. Normal computers touch a few, while a port scanner touches hundreds.
- `syn_ratio` / `rst_ratio`: the share of flows with SYN or RST flags. Many SYNs (connection starts) and RSTs (refused) is a sign of scanning.
- `sum("is_attack")`: how many of its flows were truly attacks (from the labels).

### Lines 83–86: `port_scan_candidates()`, the port-scan rule
```python
n = cfg()["streaming"]["port_scan_distinct_ports"]          # 50
return hosts.filter(F.col("distinct_dst_ports") > n).select(...)
```
Keep only the host-minutes where a computer touched **more than 50 different ports**. These are the port-scan suspects. It's a simple counting rule with **no ML**, which is why it's fast and easy to explain.

### Lines 89–92: `ensure_tables()`
Creates the three gold tables if they're missing. The timeline and host tables are **partitioned by `split`** (backfill vs live), so updates to live data don't touch the backfill files.

### Lines 95–103: `_write_timeline()`, saving Q3a's batch
```python
batch = batch.persist()
merge_upsert(batch.sparkSession, batch, lake("gold_attack_timeline"),
             ["window_start", "split", "label_family"],
             prune_partitions="split", min_col="window_start")
```
- The counts for a minute **grow** as more flows arrive, so Spark sends the *updated* row again. We can't just append, or the same minute would appear many times.
- **`merge_upsert`** runs a Delta **MERGE**:
  - if a row with the same key (window start + split + family) exists, **update** it;
  - if not, **insert** it.
- `prune_partitions` and `min_col` are speed-ups. They tell Delta "only look in this split's folder, and only at windows from this time onwards", so it skips old files instead of scanning the whole table. This made the MERGE about 10× faster.

### Lines 106–121: `_write_hosts()`, saving Q3b's batch
```python
merge_upsert(spark, batch, lake("gold_host_profile"), ["window_start", "split", "src_ip"], ...)
scans = port_scan_candidates(batch).persist()
if not scans.isEmpty():
    merge_upsert(spark, scans, lake("gold_port_scan"), [...], min_col="window_start")
    sqlite_sink.upsert_df("port_scan_candidates", scans)
scans.unpersist()
sqlite_sink.upsert_df("host_window_stats", batch.orderBy(F.desc("flows")).limit(2000))
```
1. MERGE all host-minutes into `gold_host_profile`.
2. Apply the port-scan rule. If any suspects are found, MERGE them into `gold_port_scan` **and** copy them to SQLite for the dashboard.
3. Copy only the **2,000 busiest** host-minutes to SQLite (`orderBy(desc("flows")).limit(2000)`). The dashboard only needs the top ones, and the full data stays in Delta.

### Lines 124–137: `start()`, starting both queries
```python
silver = stream_source(spark, lake("silver_flows"))
for name, agg, sink in [(Q_TIMELINE, timeline_agg, _write_timeline), (Q_HOSTS, host_agg, _write_hosts)]:
    spark.sparkContext.setLocalProperty("spark.scheduler.pool", name)
    queries.append(
        agg(silver).writeStream.queryName(name)
        .outputMode("update")
        .foreachBatch(sink)
        .option("checkpointLocation", checkpoint(name))
        .trigger(**trigger())
        .start())
```
- Read silver **as a stream**, once.
- A loop starts **two queries** from the same pattern: each pairs a counting function (`agg`) with a saving function (`sink`).
- **`outputMode("update")`:** each batch sends only the windows that **changed**, not the whole table. This is why MERGE is needed when saving.
- Each query gets its own FAIR pool and **its own checkpoint**. Windows that are still open are stored in the **RocksDB state store** inside the checkpoint, so a restart doesn't lose half-counted minutes.

---

## Summary table

| | bronze.py | silver.py | gold.py |
|---|---|---|---|
| **Reads** | Kafka `flows.raw` | bronze (Delta stream) | silver (Delta stream) |
| **Does** | Copies messages unchanged | Parses, cleans, checks, enriches, dedupes | Counts per 1-minute window; flags port scans |
| **Writes** | `bronze_flows` | `silver_flows` (+ `silver_quarantine`, Kafka `flows.dlq`) | `gold_attack_timeline`, `gold_host_profile`, `gold_port_scan` (+ SQLite) |
| **Write style** | append | idempotent append (no duplicates) | MERGE (update or insert) |
| **Big data ideas** | Kafka source, backpressure, checkpoint, exactly-once | `from_json` with a schema, data-quality rules + DLQ, broadcast join, SHA-256 dedup, `foreachBatch` | event-time windows, watermark, HyperLogLog, update mode, state store, MERGE with partition pruning |
