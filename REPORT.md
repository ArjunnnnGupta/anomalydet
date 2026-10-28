# BIG DATA ANALYTICS (BCSE402L)
## Digital Assignment 3 / Review 3 Report: Final Implementation
**Institution:** Vellore Institute of Technology (VIT Chennai), School of Computer Science and Engineering (SCOPE)  
**Project Title:** Kappa-Architecture Network-Flow Anomaly Detection Platform  
**Team Members:**  
- Arjun Gupta — [Registration Number]  
- [Team Member 2] — [Registration Number]  
**Submitted to:** Dr. Dharini R, SCOPE  

---

## 1. Summary of Changes Since Review 2

In Review 2 the pipeline was batch-oriented: Spark cleaned a CSV into Parquet, a 500K-row sample was pulled into pandas, scikit-learn trained the models, and Kafka was a side demo that streamed 500 rows. For Review 3 the project was rebuilt as a **streaming-first big data pipeline**:

| Area | Review 2 | Review 3 (final) |
|---|---|---|
| Architecture | Batch scripts, Kafka as a side demo | **Kappa architecture**: Kafka is the only way data enters |
| Dataset | Kaggle `cicds.csv` (no IPs, no timestamps) | **Original CICIDS2017 TrafficLabelling CSVs**: 2,830,743 flows with IPs, ports and timestamps |
| Processing | Spark batch cleaning, then pandas | **Spark Structured Streaming**: 4 continuous stages |
| Storage | Parquet files + `.npy` arrays | **Delta Lake** medallion lakehouse (bronze → silver → gold) |
| Machine learning | scikit-learn on a 500K sample | **Spark MLlib** on the full lake the stream built |
| Analytics | Per-flow only | **Event-time windows** per attack type and per source host, port-scan detection |
| Serving | Static SQLite tables | Live alerts, Kafka output topics, real-time dashboard |
| Deployment | Local Python scripts | **Docker Compose** cluster: Kafka (KRaft) + Spark standalone |

Review 2's planned items were Kafka streaming integration and real-time alerting. Both are delivered: flows stream through Kafka into Spark Structured Streaming, and alerts are produced continuously to a Kafka `alerts` topic and the dashboard. SHAP explainability was replaced by MLlib's feature importances (Section 5.2).

---

## 2. System Architecture

```
 CICIDS2017 CSVs (8 days, 2.83M flows)
        │  replay producer: event-time paced, keyed by Source IP,
        │  idempotent, lz4
        ▼
┌─────────── Apache Kafka (KRaft, no ZooKeeper) ───────────┐
│ flows.raw (12 partitions) · flows.scored · alerts · DLQ  │
└──────────────────────────────────────────────────────────┘
        │
        ▼
┌────────── Spark Structured Streaming (one app) ──────────┐
│ Q1 BRONZE  Kafka → Delta: raw records + offsets          │
│ Q2 SILVER  parse · validate · enrich → Delta             │
│            bad records → quarantine table + DLQ topic    │
│ Q3 GOLD    1-min windows per attack type and per host,   │
│            port-scan candidates → Delta (MERGE)          │
│ Q4 SCORE   MLlib RandomForest + KMeans → alerts          │
└──────────────────────────────────────────────────────────┘
        │                    │                     │
        ▼                    ▼                     ▼
 Delta Lake tables    Kafka alerts topic    SQLite serving cache
        │                                          │
        ▼                                          ▼
 MLlib training job                         Streamlit dashboard
 (model registry, hot reload)
```

**Kappa vs. Lambda.** A Lambda architecture keeps a separate batch path and a streaming path. Here there is a single path: every record enters through Kafka, and every table is built by stream processing. Reprocessing means replaying the log (Kafka or a Delta table) through the same code.

**Two passes through Kafka.** The model must be trained before live traffic can be scored, so the data is split with a deterministic hash:
1. **Backfill (80%)** is replayed at full speed. The stream builds the lakehouse, and MLlib trains on it.
2. **Live (20%, held out)** is replayed paced to real time, re-stamped to the current clock. The stream scores every flow as it arrives. Because these flows were never used in training, live scoring is also a genuine held-out evaluation.

---

## 3. Big Data Concepts Applied

| Concept | Where it is applied |
|---|---|
| **Distributed log, partitioning, keys** | `flows.raw` has 12 partitions. Keying by Source IP keeps each host's flows in order on one partition |
| **Replication and fault tolerance** | Full cluster: 3 brokers, replication factor 3, `min.insync.replicas=2`, producer `acks=all` (verified: every partition had all 3 replicas in sync) |
| **Idempotent producer, batching, compression** | `enable.idempotence`, `linger.ms`, 256 KB batches, lz4 |
| **Backpressure** | `maxOffsetsPerTrigger` (Kafka) and `maxFilesPerTrigger` / `maxBytesPerTrigger` (Delta) cap each micro-batch |
| **Micro-batch stream processing** | Spark Structured Streaming, 5-second triggers, one checkpoint per query |
| **Exactly-once processing** | Kafka offsets in checkpoints + Delta transactional sinks + idempotent writes (`txnAppId` / `txnVersion`) |
| **Event time and watermarks** | Windows use the flow's own timestamp. A 10-minute watermark tolerates out-of-order data and bounds state |
| **Windowed aggregation** | 1-minute tumbling windows per attack type and per source host, with `approx_count_distinct` (HyperLogLog) for distinct ports and destinations |
| **Stateful streaming** | Open windows are kept in the RocksDB state store and checkpointed |
| **Broadcast joins** | Small subnet and port→service dimension tables are broadcast-joined onto the stream |
| **Medallion lakehouse** | Delta Lake bronze (raw) → silver (clean) → gold (aggregates, scored flows) |
| **ACID, MERGE, time travel** | Gold tables are upserted with MERGE. Training pins the exact silver version it read (`versionAsOf`) |
| **Partition pruning and file skipping** | MERGE conditions include literal partition and min-timestamp predicates so Delta skips old data |
| **Lakehouse maintenance** | OPTIMIZE (small-file compaction), Z-ORDER by source IP, DESCRIBE HISTORY, schema evolution, VACUUM |
| **Distributed ML** | MLlib Pipelines on the full training set, distributed class weighting, optional CrossValidator |
| **Vectorised UDFs** | Arrow-based pandas UDF computes the KMeans anomaly distance |
| **Model registry and hot swap** | Versioned models. The scoring query reloads the newest version without a restart |
| **Data quality** | 4 validation rules. Failing records go to a quarantine table and a dead-letter topic instead of being dropped |

---

## 4. Implementation

### 4.1 Dataset

The original CICIDS2017 *GeneratedLabelledFlows* CSVs from UNB (8 files, 1.2 GB, 85 columns): **2,830,743 flows** from 3–7 July 2017, with 15 attack labels mapped to 8 families (Normal, DoS, DDoS, Port Scanning, Brute Force, Web Attacks, Bots, Infiltration).

Dataset issues handled in the silver layer:
- Two timestamp formats, and 12-hour times without AM/PM in the afternoon files.
- `Infinity` / `NaN` values in the rate columns.
- A duplicated column, Windows-1252 characters in labels, and about 288K blank lines.

For a laptop-sized demo, the producer keeps **every attack flow and 20% of benign flows**: **1,012,310 flows** (557,646 attacks + 454,664 benign), split into 809,632 backfill and 202,678 live.

### 4.2 Streaming application

One Spark application runs four stages (five queries):

```python
# Q1 bronze: Kafka → Delta, backpressure via maxOffsetsPerTrigger
spark.readStream.format("kafka").option("subscribe", "flows.raw") \
     .option("maxOffsetsPerTrigger", 20000).load()

# Q3 gold: event-time window per source host, with watermark
silver.withWatermark("event_time", "10 minutes") \
      .groupBy(F.window("event_time", "1 minute"), "split", "src_ip") \
      .agg(F.count("*").alias("flows"),
           F.approx_count_distinct("dst_port").alias("distinct_dst_ports"))
```

Hosts touching more than 50 distinct ports in one minute are flagged as **port-scan candidates**. On the real data, the attacker address `172.16.0.1` contacted 1,034 distinct ports within single minutes.

### 4.3 Machine learning (Spark MLlib)

- **Features:** 70 numeric flow features, put through Imputer → VectorAssembler → StandardScaler.
- **Supervised model:** RandomForest (50 trees, depth 10) with class weights against the imbalance. It predicts the attack family and P(attack).
- **Unsupervised model:** KMeans (k = 8) trained on benign traffic only. The anomaly score is the distance to the nearest centroid, with a threshold at the 99.5th percentile of benign distances.
- **Alert rule:** alert if the RandomForest predicts an attack with P ≥ 0.6, **or** the KMeans distance exceeds the threshold.
- Trained on **647,533 rows** (silver, backfill split, version 9) and tested on 162,029 held-out rows.

### 4.4 Deployment

A Docker Compose cluster: Kafka in KRaft mode, Kafka UI, a Spark standalone master and worker(s), all sharing the project folder as the lakehouse filesystem. Two profiles:
- **Lite (default, used for the demo):** 1 Kafka broker and 1 Spark worker, sized for an 8 GB laptop.
- **Full (`LITE=0`):** 3 Kafka brokers with replication and 2 Spark workers.

The code is identical in both; only the cluster size changes.

---

## 5. Results

### 5.1 Model performance

| Metric | Held-out test (162,029 flows) | Live stream (24,045 flows) |
|---|---|---|
| Alert precision (alerts that were real attacks) | **0.992** | **0.994** |
| Alert recall (real attacks that raised an alert) | **0.996** | **0.992** |
| RandomForest ROC-AUC | 0.9997 | 0.9998 |
| KMeans anomaly ROC-AUC | 0.709 | 0.772 |
| Accuracy (attack family) | 0.995 | — |
| Macro-F1 (attack family) | 0.907 | 0.853* |

\*The live flows scored so far contain only 8 Bot and 7 Web Attack flows (F1 0.35 and 0.64), which pull the equally-weighted macro average down; every other family scores F1 ≥ 0.994. The binary precision and recall are not affected.

**Per-family results (held-out test):**

| Family | Precision | Recall | F1 | Flows |
|---|---|---|---|---|
| Normal Traffic | 0.997 | 0.992 | 0.994 | 73,148 |
| DoS | 0.992 | 0.997 | 0.994 | 40,110 |
| Port Scanning | 0.998 | 0.999 | 0.999 | 25,466 |
| DDoS | 1.000 | 0.999 | 0.999 | 20,463 |
| Brute Force | 1.000 | 0.994 | 0.997 | 2,184 |
| Web Attacks | 0.812 | 0.893 | 0.851 | 335 |
| Bots | 0.621 | 0.956 | 0.753 | 315 |
| Infiltration | 1.000 | 0.500 | 0.667 | 8 |

The large families are detected almost perfectly. Macro-F1 (0.907) is pulled down by the three rare families. Infiltration has only 8 test flows, so one miss halves its recall.

**Which model fires.** Of the alerts on live traffic, 12,445 came from the RandomForest alone, 131 from both models and 48 from KMeans alone (held-out test: 88,601 / 249 / 393). The supervised model does most of the work, and KMeans adds a safety net for traffic that does not look like anything normal.

**Most important features** (RandomForest): destination port, backward packets/s, subflow forward bytes, maximum forward packet length, initial TCP window size, average backward segment size.

**Caveat.** Keeping only 20% of benign traffic makes attacks about 55% of the replayed data, against about 20% in the original capture. Precision on real-world traffic, where attacks are rare, would be lower.

### 5.2 Streaming run

On the lite cluster, a demo replay of **30,235 live flows** went through Kafka → bronze → silver → scoring with no memory failures, raising **16,872 alerts** to SQLite and the Kafka `alerts` topic. Kafka consumer lag returned to zero once the replay finished. That run also exposed a bug: after a checkpoint reset, its scored rows were silently skipped by the Delta `gold_scored` table (Section 8). After the fix, a second replay of **4,060 flows** was verified end to end: `gold_scored` grew from 19,985 to 24,045 rows (exactly 4,060).

Fixing the bottlenecks found on the cluster made the stream several times faster:

| Query | Micro-batch time before | After |
|---|---|---|
| Scoring | 48–138 s | 12–23 s |
| Gold per-host windows | 140–363 s | 7–33 s |
| Gold MERGE scan time | 76–248 s | 4–27 s |

### 5.3 Dashboard

The Streamlit dashboard has five tabs:
- **Live pipeline:** per-query throughput, micro-batch duration, Kafka consumer lag per partition, and stream status.
- **Threat timeline:** alerts per minute by attack family, with precision and recall against ground truth.
- **Hosts:** top alerting source hosts, port-scan candidates, and per-host traffic windows.
- **Alert feed:** filterable alerts, each marked correct or incorrect against ground truth.
- **Lakehouse & model:** Delta table versions and files, the gold attack timeline, model metrics (held-out vs. live), the confusion matrix and feature importance.

### 5.4 Testing

18 automated tests (pytest) run the real streaming queries on synthetic CICIDS-shaped data. They cover: parsing and data-quality quarantine, idempotent replay, windowed aggregation and port-scan detection, MLlib training and registration, live scoring into Delta and SQLite, model hot reload, evaluation, and Delta maintenance.

---

## 6. Team Participation

| Team Member | Contributions in Review 3 |
|---|---|
| **Arjun Gupta**<br>[Registration Number] | [fill in] |
| **[Team Member 2]**<br>[Registration Number] | [fill in] |

---

## 7. Project Status

| Component | Status |
|---|---|
| Kafka ingestion (replay producer, topics, partitioning, dead-letter topic) | **Completed** |
| Spark Structured Streaming: bronze, silver, gold, scoring | **Completed** |
| Delta Lake lakehouse and maintenance job | **Completed** |
| MLlib training, model registry, hot reload, evaluation | **Completed** |
| Streamlit dashboard | **Completed** |
| Docker Compose deployment (lite and full profiles) | **Completed** |
| Automated test suite (18 tests) | **Completed** |

---

## 8. Challenges Encountered

These problems all came up when running on the real cluster with the full dataset:

* **Kafka deleted the data minutes after it arrived.** The producer stamped records with their original 2017 timestamps, and Kafka's time-based retention (48 hours) treated them as years old. The record timestamp is now the send time, with the event time carried in the payload. The bronze source also now fails loudly on missing data instead of skipping it.
* **Memory on an 8 GB laptop.** Executors and the driver were repeatedly killed for exceeding container limits. Fixes included:
  - capping every micro-batch, not only the Kafka read
  - removing a sort caused by a partitioned stream write
  - spreading the stream over two small executors
  - never running a batch job alongside the stream
  - using the single-broker, single-worker profile for the demo
* **Slow deduplication.** A MERGE-based duplicate check re-scanned the whole silver table on every micro-batch (about 2.5 minutes per batch). It was replaced with Delta's idempotent append, and the gold MERGEs were given partition-pruning and file-skipping predicates.
* **Silently skipped writes after a checkpoint reset.** Delta's idempotent write used the query name as its ID. After a checkpoint reset the batch numbers restarted at 0, so Delta treated them as already written. The ID now includes the streaming query's unique ID, as Delta recommends.
* **CPU starvation from a Delta default.** Delta rebuilds its transaction-log snapshot with 50 tasks by default, sized for large clusters. On a 2-core worker every micro-batch of every Delta-reading query queued 50 tiny tasks, and scoring waited behind them. Setting `spark.databricks.delta.snapshotPartitions` to 4 removed the starvation.
* **Python 3.8 in the Spark image.** The official Spark 3.5.3 image ships Python 3.8. Library versions were pinned accordingly, and the code kept 3.8-compatible.

---

## 9. Conclusion and Future Work

The final system is an end-to-end Kappa-architecture big data pipeline. Network flows enter only through Kafka. Spark Structured Streaming builds a Delta Lake lakehouse continuously, Spark MLlib trains on it, and live traffic is scored in real time. On held-out data, 99.2% of alerts were real attacks and 99.6% of attacks raised an alert. Results appear on a live dashboard and a Kafka alerts topic.

**Future work:**
1. Run on a multi-node cluster with the full 2.83M flows and no benign downsampling, to measure precision on realistic traffic.
2. Improve the rare families (Bots, Web Attacks, Infiltration) with more training examples or oversampling.
3. Add per-alert explanations (for example SHAP) and an automated notification channel subscribed to the `alerts` topic.
