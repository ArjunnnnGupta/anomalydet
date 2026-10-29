# Architecture
## Kappa Network-Flow Anomaly Detection Platform

> **Audience:** teammates and evaluators who need to understand how the pipeline works,
> why it's built this way, and where each big data concept lives in the code.

---

## Contents
1. [Why Kappa (and not Lambda)](#1-why-kappa-and-not-lambda)
2. [Cluster topology](#2-cluster-topology)
3. [Kafka design](#3-kafka-design)
4. [The streaming application](#4-the-streaming-application)
5. [Delta Lake medallion tables](#5-delta-lake-medallion-tables)
6. [Machine learning with MLlib](#6-machine-learning-with-mllib)
7. [Serving and observability](#7-serving-and-observability)
8. [Fault tolerance and delivery guarantees](#8-fault-tolerance-and-delivery-guarantees)
9. [Big data concepts → where in the code](#9-big-data-concepts--where-in-the-code)
10. [Limitations and honest caveats](#10-limitations-and-honest-caveats)

---

## 1. Why Kappa (and not Lambda)

A **Lambda** architecture keeps two code paths: a batch layer that recomputes everything from
the raw files, and a speed layer that handles fresh events. Logic gets duplicated, and the two
paths drift apart. That was this project's earlier shape: Spark batch on a CSV, with Kafka as a side demo.

A **Kappa** architecture keeps one path. **The log is the source of truth.** Every record
enters through Kafka, and every derived table is built by stream processing. Reprocessing means
replaying the log (or the Delta tables, which are themselves replayable logs) through the same code.

```
            ┌────────────────────────── ONE code path ──────────────────────────┐
 CSVs ─► producer ─► Kafka ─► bronze ─► silver ─► gold / scoring ─► sinks
            └── replay = re-read the log (Kafka offsets or Delta versions) ─────┘
```

**Bootstrapping a model in a Kappa system: two passes through Kafka**

| Pass | What is replayed | What happens |
|---|---|---|
| 1. backfill | ~80% of flows (deterministic hash), full speed, original 2017 timestamps | The stream builds bronze, silver and gold. Scoring idles ("no model yet"). |
| train | — | MLlib reads silver (backfill split) and registers model **v1** |
| 2. live | the held-out ~20%, paced at N× real time, timestamps re-based to now | The stream hot-loads v1 and scores every flow in real time |

CICIDS2017 puts each attack type on a different day, so a time-based split would hide whole
attack families from training. The hash split keeps every family in both passes, and live
scoring doubles as a genuine held-out evaluation (`make evaluate-live`).

**Demo-sized data.** The full dataset is 2.83M flows, about 80% benign. To keep the demo fast on
a laptop, the producer keeps **every attack flow and 20% of the benign flows** (deterministic
hash, `dataset.benign_pct`), which is **1,012,310 flows**: about 810K backfill and 203K live.
This makes attacks about 55% of the replayed traffic instead of 20%, so the models train on a more
balanced set and precision looks better than it would on real-world traffic. Set `benign_pct: 100`
to replay everything.

---

## 2. Cluster topology

One small cluster, sized for an 8 GB Mac with Docker at 6 GB (`docker/docker-compose.yml`):

```
┌──────────────────────────── Docker network ────────────────────────────┐
│                                                                         │
│  kafka-1 :9092   (one KRaft broker = broker + controller, RF=1)         │
│     │                                                                   │
│  kafka-ui :8080                                                         │
│                                                                         │
│  spark-master :8081 ──── spark-worker-1 (2 cores, 2000m, container 3 GB)│
│  (+ drivers, client                                                     │
│   mode :4040/:4041)                                                     │
│        all mount the project at /opt/app  (shared FS for Delta tables)  │
└─────────────────────────────────────────────────────────────────────────┘
  host: replay producer · topic admin · Streamlit dashboard · pytest
```

| Service | Memory cap | Notes |
|---|---|---|
| Kafka | 512 MB (heap 320 MB) | internal listener `:19092`, host listener 9092 |
| Kafka UI | 320 MB | kafbat/kafka-ui |
| Spark master | 2.5 GB | also hosts the drivers (stream app 768 MB, jobs 1 GB) |
| Spark worker | 3 GB | executors: 800 MB heap + 256 MB direct memory (RocksDB state capped at 128 MB) |

The streaming app runs as **two 1-core executors** (`spark.cores.max=2`, `executor.cores=1`)
under FAIR scheduling. An earlier layout with one 2-core executor ran out of memory.

**Batch jobs (train, evaluate, maintenance) run only while the stream is stopped.** The drivers
live in the master container, and two at once (stream + evaluate) got it OOM-killed on 6 GB.
The Makefile refuses to start a batch job while the stream runs. On a larger cluster the two
can run side by side, and the scorer then swaps a newly trained model in mid-stream: it
re-reads the registry's `latest.json` on every micro-batch (covered by the test suite).

On a real multi-machine cluster, the same code would run with more brokers and workers; only the
Docker setup changes.

---

## 3. Kafka design

| Topic | Partitions | Key | Producer → consumer |
|---|---|---|---|
| `flows.raw` | 12 | Source IP | replay producer → Q1 bronze |
| `flows.scored` | 6 | Source IP | Q4 → any downstream consumer (SIEM, archiver) |
| `alerts` | 3 | flow_uid | Q4 → alerting / ticketing |
| `flows.dlq` | 1 | DQ reason | Q2 → dead-letter inspection |

- **Partitioning and keys:** keying by Source IP sends every flow of a host to one partition.
  That keeps each host's events in order and spreads load across 12 partitions, which caps
  consumer parallelism at 12.
- **Replication:** one broker, so replication factor 1. The producer still uses `acks=all`. With more
  brokers, a higher replication factor would let one broker fail without losing data.
- **Idempotent producer:** `enable.idempotence=true`, so librdkafka retries never create duplicates.
- **Throughput tuning:** `linger.ms=20`, `batch.size=256KB`, `compression.type=lz4`.
  When the local queue is full, the producer calls `poll()` until it drains, which gives natural backpressure.
- **Retention:** `retention.bytes` per partition protects the laptop's disk. Kafka is the
  replay buffer, and Delta bronze is the durable copy.
- **Replay:** the producer can send the dataset again at any speed. The streaming queries can
  also be reset to `startingOffsets=earliest` by deleting a checkpoint.

**Event-time replay** (`replay_producer.py`): rows are sorted by flow start time within each
file. A virtual clock advances by the event-time gap between rows (capped at 60s, so nights and
weekends don't stall the demo), and the producer sleeps whenever it gets ahead of
`virtual_time / speedup`. In the live pass, each record also carries `replay_ts_ms` (wall-clock
send time), which becomes its `event_time` in silver. The Kafka record timestamp is deliberately
**not** set to the event time: broker retention works on record timestamps, so stamping 2017
timestamps made Kafka delete the whole backfill within minutes (found in the first real cluster
run). The event time lives in the payload, and Kafka's own timestamp is the send time. The bronze
source also runs with `failOnDataLoss=true`, so skipped offsets fail the query instead of
silently losing data.

---

## 4. The streaming application

`src/streaming/stream_app.py` runs **five streaming queries in one Spark application**.
Each query has its own checkpoint and FAIR-scheduler pool.

```
Kafka flows.raw
   │ Q1 q1_bronze           readStream.format("kafka"), maxOffsetsPerTrigger=20k
   ▼
Delta bronze_flows          raw JSON + topic/partition/offset/kafka_ts (append-only)
   │ Q2 q2_silver           Delta-as-stream source
   │   from_json(explicit schema) → event-time parsing → casts (Inf/NaN→NULL)
   │   → label cleaning + 8 families → sha2 flow_uid → 4 DQ rules
   │   → stream-static BROADCAST joins (dim_subnets ×2, dim_ports)
   │   → foreachBatch: bad → quarantine (Delta) + flows.dlq (Kafka)
   │                   good → dropDuplicates → idempotent append into silver (txnVersion)
   ▼
Delta silver_flows ─────────┬───────────────────────────────┐
   │ Q3a q3a_gold_timeline  │ Q3b q3b_gold_hosts             │ Q4 q4_scoring (split = live)
   │ watermark 10 min       │ watermark 10 min               │ foreachBatch:
   │ 1-min TUMBLING window  │ 1-min TUMBLING window          │   HotLoader → newest model
   │ × split × family       │ × split × src_ip               │   features → RF → KMeans
   │ count, bytes,          │ approx_count_distinct(dst_ip,  │   P(attack), centroid distance
   │ approx distinct srcs   │ dst_port), SYN/RST ratios      │   is_alert / alert_reason
   │ update mode → MERGE    │ → MERGE + port-scan filter     │   → gold_scored (txn-idempotent)
   ▼                        ▼                                │   → Kafka flows.scored, alerts
gold_attack_timeline   gold_host_profile, gold_port_scan     │   → SQLite alerts
   (Delta only)            + SQLite                          ▼
                                                    Delta gold_scored
```

**Design notes**
- **Delta tables chain the queries together.** Each downstream query streams from the upstream
  Delta table's transaction log. Stages can be restarted, replayed or scaled on their own, and
  every intermediate state is a queryable table.
- **Silver is an idempotent append, not a MERGE.** A retried batch carries the same
  `txnVersion` and is skipped, so it's still exactly-once, and the log stays append-only, so
  silver can itself be read as a stream. It started as an insert-only MERGE on `flow_uid`, but
  that re-scanned the whole table every batch (~2.5 min per batch at 800K rows), too slow for a
  live demo. MERGE is still used for the gold window tables, where the targets are small.
- **Watermarks** bound the state for windowed aggregations. CICIDS rows aren't strictly
  time-ordered, so 10 minutes of disorder is tolerated, and later rows are dropped from windows
  (but they're still kept in silver).
- **Update output mode** gives the dashboard low-latency window values. Each foreachBatch
  MERGEs the changed windows into Delta (and, for host windows, upserts them into SQLite).
- **Stateful aggregation:** the open windows of Q3a/Q3b are streaming state, kept in the RocksDB
  state store and checkpointed, and dropped once the watermark passes the window end.
- **Host windows are tumbling (1 min), not sliding.** The window length and slide are config
  values; a 5-minute window sliding every minute puts each flow in 5 windows, which was too much
  work for an 8 GB laptop. Setting `host_slide` smaller than `host_window` turns sliding back on.
- **Tests** run each stage with `Trigger.AvailableNow` (drain what's there, then stop), via the
  `STREAM_AVAILABLE_NOW` switch in `config.trigger()`.

---

## 5. Delta Lake medallion tables

| Layer | Table | Partitioned by | Written by | Notes |
|---|---|---|---|---|
| Bronze | `bronze_flows` | — (a partitioned stream write sorts each batch; that ran the executor out of memory) | Q1 | Raw payload + Kafka coordinates. Replayable. |
| Silver | `silver_flows` | `day` (capture day) | Q2 (idempotent append) | Typed, cleaned, enriched, deduplicated on `flow_uid` within each batch |
| Silver | `silver_quarantine` | — | Q2 | Failed DQ rule + raw payload |
| Gold | `gold_attack_timeline` | `split` | Q3a | 1-min windows × family |
| Gold | `gold_host_profile` | `split` | Q3b | 1-min windows per host |
| Gold | `gold_port_scan_candidates` | — | Q3b | Hosts above 50 distinct ports per window |
| Gold | `gold_scored_flows` | `day` | Q4 | Every live flow + model output + model_version |
| Gold | `gold_model_predictions` | `model_version` | training | Held-out test predictions per version |

**Data-quality rules** (silver): `unparseable_payload`, `bad_timestamp`, `negative_duration`,
`unknown_label`. Rows that fail are kept, not dropped, in `silver_quarantine`, and published to `flows.dlq`.

**Dataset quirks handled in silver:**
- Two timestamp formats (`dd/MM/yyyy HH:mm:ss` and `d/M/yyyy H:mm`).
- A 12-hour clock with no AM/PM in the afternoon files (hours before 08:00 are shifted by 12h).
- A Windows-1252 dash in the web-attack labels.
- `Infinity` / `NaN` in the rate columns.
- A duplicated `Fwd Header Length` column (dropped in the producer).
- About 288K blank lines in the Thursday file (skipped in the producer).

**Lakehouse operations** (`make maintenance`): `DESCRIBE DETAIL` inventory, `OPTIMIZE`
compaction of the small files micro-batches create, `ZORDER BY (src_ip)` for data skipping on
host lookups, `DESCRIBE HISTORY`, `VERSION AS OF` time travel, `mergeSchema` schema evolution, and `VACUUM`.

---

## 6. Machine learning with MLlib

`src/ml/train_pipeline.py` runs as a distributed Spark job on the cluster. It uses **all**
backfill rows in silver, with no `toPandas()`. (The only sampling is the benign downsampling in
the producer, described in §1.)

```
silver_flows @ version V (time travel pins the training snapshot)
   │ filter split = 'backfill'
   ├─ sampleBy(label_family, 0.8)  → train          (stratified)
   └─ left_anti join on flow_uid   → test
train ─► broadcast-join class weights √(N / (k·n_c))
      ─► Feature PipelineModel: Imputer(median) → VectorAssembler → StandardScaler
      ─► StringIndexer ─► RandomForestClassifier(weightCol, 50 trees, depth 10) ─► IndexToString
      └─ benign rows only ─► KMeans(k=8) ─► threshold = approxQuantile(distance, 0.995)
test  ─► macro-F1, per-class P/R/F1 (groupBy confusion matrix), ROC-AUC of P(attack) and of
         the KMeans distance, alert precision/recall
      ─► registry/vN/{features, rf, kmeans, meta.json}  +  Delta gold_model_predictions
```

**Two detectors, one alert rule.**
- `rf`: the RandomForest predicts an attack family with P(attack) ≥ 0.6. This is the supervised path, for known attack families.
- `anomaly`: the distance to the nearest *benign* centroid is above the 99.5th percentile of benign distances. This is the unsupervised path, for traffic unlike anything normal, including unseen attacks.

The two can fire together (`rf+anomaly`), and `alert_reason` records which one fired.

**Model registry and hot reload** (`src/ml/registry.py`): each training run writes `vN/` and
then swaps `latest.json` atomically (write a temp file, then `os.replace`). On every micro-batch
the scoring query reads `latest.json`, and when the version changes it loads the new
PipelineModels without restarting the stream. Every scored row and alert carries `model_version`.

The distance computation is an **Arrow-vectorised pandas UDF** (`model_utils.add_anomaly_score`),
shared by training (to set the threshold) and by streaming (to score).

---

## 7. Serving and observability

- **SQLite serving cache** (`data/serving/alerts.db`) holds small, query-ready tables for the
  dashboard: alerts, host_window_stats, port_scan_candidates, stream_metrics, pipeline_events.
  (The attack timeline and scored flows are read straight from Delta with delta-rs.) Every table has a natural primary key and
  is written with `INSERT OR REPLACE`, so it's idempotent.
- **StreamingQueryListener** (`metrics_listener.py`) records each micro-batch's input and
  processed rows/s, duration, watermark, state-store rows and source end offsets. It also
  commits bronze's processed offsets to the Kafka consumer group `spark-bronze-ingest`. Spark
  normally keeps offsets only in its checkpoint, so without that commit, consumer lag would be
  invisible to Kafka UI.
- **Dashboard** (`src/dashboard/app.py`, no Spark) has 3 tabs:
  - pipeline: stream status, throughput per query, per-partition Kafka lag
  - alerts: headline numbers from Delta `gold_scored` (via delta-rs), alerts per minute, latest alerts with ✅/❌
  - model & hosts: model metrics, port-scan suspects, top alerting hosts
- **Spark UIs:** the master on :8081, the streaming tab on :4040, and event logs in `data/spark-events`.

---

## 8. Fault tolerance and delivery guarantees

| Hop | Guarantee | Mechanism |
|---|---|---|
| producer → Kafka | exactly-once per partition | idempotent producer, `acks=all` |
| Kafka → bronze | exactly-once | Kafka offsets in the checkpoint + Delta transactional sink |
| bronze → silver | exactly-once | Delta source versions in the checkpoint + Delta idempotent write (`txnAppId`/`txnVersion`) |
| silver → quarantine / gold_scored | exactly-once | Delta idempotent writes (`txnAppId` = query, `txnVersion` = batch id) |
| silver → gold windows | exactly-once | stateful aggregation in the checkpoint + MERGE on window keys |
| → SQLite | effectively once | `INSERT OR REPLACE` on natural keys |
| → Kafka flows.scored / alerts / dlq | at-least-once | the Kafka batch sink can re-send when a micro-batch is retried |

You can test this by killing the streaming app at any point and restarting it. There will be no
duplicate `flow_uid`s in silver, in `gold_scored` or in SQLite alerts. The test suite asserts the
same thing by replaying micro-batches.

---

## 9. Big data concepts → where in the code

| Concept | Where |
|---|---|
| **Volume:** 2.83M flows × 85 columns (1.0M replayed in the demo), all processed distributed | `replay_producer.py`, `silver.py`, `train_pipeline.py` |
| **Velocity:** continuous micro-batches, paced event-time replay | `stream_app.py`, `replay_producer.py --speedup` |
| **Variety / veracity:** raw strings, mixed timestamp formats, Inf/NaN, bad records | `silver.parse`, DQ rules, quarantine + DLQ |
| Distributed log, partitions, message keys | `create_topics.py`, producer `key=source_ip` |
| Idempotent producer, acks, batching, compression | `replay_producer.make_producer` |
| Consumer offsets, replay, consumer lag | bronze checkpoint, `metrics_listener._commit_offsets`, dashboard lag chart |
| Backpressure | `maxOffsetsPerTrigger` (bronze), producer queue `poll()` |
| Kappa architecture | the whole design; see §1 |
| Structured Streaming micro-batches, triggers, checkpoints | every module in `src/streaming/`, `config.trigger()` |
| Event time vs processing time | `silver.parse_event_time`, `event_time` vs `ingest_ts` |
| Watermarks and late data | `gold.timeline_agg`, `gold.host_agg` |
| Tumbling windows (sliding is one config value away) | `gold.timeline_agg`, `gold.host_agg` (1 min); `host_window`/`host_slide` in `pipeline.yaml` |
| Approximate distinct counting (HyperLogLog++) | `approx_count_distinct` in `gold.py` |
| RocksDB state store | `spark.py` (`stateStore.providerClass`) |
| Stream-static broadcast join | `silver.enrich` (`F.broadcast(dim_subnets / dim_ports)`) |
| foreachBatch multi-sink fan-out | `silver.write_batch`, `scoring.write_batch`, `gold._write_*` |
| Exactly-once / idempotent sinks | `lake.idempotent_append`, `lake.merge_upsert`, `sqlite_sink.upsert` |
| Medallion lakehouse | `bronze.py` → `silver.py` → `gold.py` / `scoring.py` |
| Delta: ACID, MERGE, idempotent writes, Delta-as-stream-source | `lake.py`, `gold._write_*` (MERGE), `silver.write_batch` (idempotent append) |
| Delta: time travel, history, schema evolution | `train_pipeline` (`versionAsOf`), `maintenance.py` |
| Partitioning, partition pruning, small-file compaction, Z-ordering | partitionBy in each writer, day-pruned MERGE, `maintenance.py` |
| Columnar storage (Parquet under Delta), predicate pushdown | all Delta reads, e.g. the `split = 'live'` filter in scoring and `split = 'backfill'` in training |
| Adaptive Query Execution, shuffle partitions | `spark.py` |
| Spark standalone cluster, executors, FAIR scheduling, cores.max | `docker-compose.yml`, `spark.py` |
| MLlib Pipelines, distributed training | `model_utils.feature_pipeline`, `train_pipeline.py` |
| Class imbalance handling at scale | broadcast class weights, `weightCol` |
| Stratified sampling | `sampleBy` + `left_anti` |
| Vectorised (Arrow) pandas UDFs | `model_utils.add_anomaly_score` |
| Model registry, versioning, hot swap | `registry.py`, `scoring.write_batch` |
| Stream monitoring | `metrics_listener.py`, Spark UI, Kafka UI, dashboard |

---

## 10. Limitations and honest caveats

- **This is one laptop.** The cluster is real (separate JVMs, network shuffle, replication),
  but every container shares one 8 GB machine. Throughput numbers show the design working, not
  production capacity.
- **The shared filesystem is a bind mount**, standing in for HDFS or S3. Delta's
  single-writer-per-table assumption holds because each table has exactly one writing query.
- **The Kafka batch sinks are at-least-once** (see §8). Consumers of `flows.scored` and `alerts`
  should dedupe on `flow_uid`.
- **SQLite** is a serving cache for a single dashboard, not a scalable serving store. In
  production it would be Cassandra, Elasticsearch or Postgres. The idempotent upsert pattern stays the same.
- **The test suite uses synthetic, CICIDS-shaped data** (`tests/synthetic.py`) and runs without
  Docker. Its metrics are perfect only because the synthetic families are separable. Real
  numbers come from `make train` / `make evaluate` on the real dataset.
- **CICIDS2017 is a lab capture.** Very rare classes (Infiltration ≈ 36 flows, Heartbleed ≈ 11)
  can't be learned reliably by any model, and that shows up in the per-class metrics.
