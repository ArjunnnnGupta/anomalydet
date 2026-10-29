---
course: BIG DATA ANALYTICS
code: COURSE CODE – BCSE402L
assignment: Digital Assignment 3
title: FLOW LEVEL NETWORK ANOMALY DETECTION PLATFORM
team: Arjun Gupta – 23BLC1177; Abhinav Shekhar – 23BLC1176; Manas Kukreti – 23BLC1218; Ashutosh Das – 23BLC1163
submitted: Dr. Dharini R; SCOPE
---

# Flow Level Network Anomaly Detection Platform

## Abstract

This project builds a real-time Big Data Analytics platform for network intrusion detection on the CICIDS2017 dataset. The original GeneratedLabelledFlows data (8 CSV files, 2,830,743 labelled network flows captured from 3 to 7 July 2017) is replayed through Apache Kafka. For the demonstration every attack flow and 20% of the benign flows are kept, giving 1,012,310 flows. The design follows a Kappa architecture: Kafka is the only way data enters, and one Spark Structured Streaming application with five queries builds a Delta Lake medallion lakehouse (bronze, silver, gold), computes 1-minute windowed aggregates and port-scan candidates, and scores live traffic with Spark MLlib models. A RandomForest classifier reached a macro-F1 of 0.907 and an accuracy of 99.48% on 162,029 held-out flows. On 27,099 live flows that were never used in training, 99.2% of alerts were real attacks and 99.2% of attacks raised an alert. The registered model is hot-swapped into the running stream without a restart, and the results are served through SQLite to a five-tab Streamlit dashboard. The platform runs on a single 8 GB laptop under Docker, so the results demonstrate a working pipeline rather than production-scale performance.

## 1. Problem Statement

Networks produce flow records continuously, and an intrusion is only useful to detect while it is still happening. The CICIDS2017 dataset alone contains about 2.83 million flows with more than 80 features each over five working days. Treated as a live stream, this volume is too large to handle comfortably with spreadsheets or a single-threaded pandas script. The problem addressed here is to ingest flow records as a continuous stream, clean and store them reliably, summarise traffic over time windows, detect attacks in near real time with a model trained on the same data, and show the results to an analyst.

The project replays the CICIDS2017 flows through Apache Kafka, processes them with Spark Structured Streaming into Delta Lake, classifies them with Spark MLlib and presents the results through a Streamlit dashboard. This report documents the completed work and the final outputs.

## 2. Completion of Proposed Work

| Component | Planned purpose | Final implementation / status |
|---|---|---|
| Data ingestion | Stream flow records into the platform | Completed: replay producer, 1,012,310 flows sent to Kafka (809,632 backfill + 202,678 live) |
| Message broker | Durable, partitioned transport | Completed: KRaft Kafka, 4 topics; 1 broker for the demo, 3 brokers with replication factor 3 in the full profile |
| Stream processing | Continuous parsing and cleaning | Completed: Spark Structured Streaming, 5 queries in one application |
| Lakehouse storage | Reliable storage with history | Completed: Delta Lake bronze / silver / gold, 8 tables, time travel |
| Data quality | Catch malformed records | Completed: DQ rules, quarantine table and Kafka dead-letter topic |
| Windowed analytics | Time-based traffic summaries | Completed: 1-minute windows, 10-minute watermark, port-scan detection |
| Machine learning | Classify flows into attack families | Completed: MLlib RandomForest + KMeans, macro-F1 0.907 |
| Real-time scoring | Score live traffic as it arrives | Completed: 27,099 live flows scored, 12,649 alerts, model hot reload |
| Dashboard | Present results interactively | Completed: Streamlit dashboard, five tabs |

## 3. Methodology

The implemented workflow has seven steps:

1. Replay the CICIDS2017 CSV files into Kafka in event-time order, split deterministically into 80% backfill and 20% live.
2. Ingest the Kafka stream into the Delta bronze table with Spark Structured Streaming.
3. Parse, validate, enrich and deduplicate the records into the silver table.
4. Compute 1-minute windowed aggregates and port-scan candidates into gold tables.
5. Train the MLlib RandomForest and KMeans models on the silver backfill and register them.
6. Score the live stream with the registered model and publish alerts to Kafka.
7. Serve the results through SQLite and display them in the Streamlit dashboard.

Figure 1 shows the main flow (CICIDS2017 CSVs, Replay Producer, Kafka, Spark Structured Streaming, Delta Lake, gold windows and live scoring, then the SQLite cache and Streamlit dashboard), the MLlib training branch that reads silver through Delta time travel, and the model registry from which the scoring query hot-swaps new model versions (dashed arrows).

![Final workflow of the Flow Level Network Anomaly Detection Platform, with the training and model hot-swap paths shown as dashed arrows.](data/report_figures/fig1_architecture.png)

## 4. Results and Discussion

### 4.1 Dataset Processing

The analysis uses the CICIDS2017 GeneratedLabelledFlows data for the five capture days, 3 to 7 July 2017.

| Item | Value |
|---|---|
| Source | CICIDS2017 GeneratedLabelledFlows |
| Capture period | 3–7 July 2017 (5 working days) |
| Input files | 8 CSV files (1.2 GB) |
| Raw flows | 2,830,743 |
| Flows replayed (all attacks + 20% benign) | 1,012,310 |
| Backfill / live messages | 809,632 / 202,678 |
| Silver rows from backfill | 809,562 |
| Quarantined by DQ rules (backfill) | 27 |
| Storage | Delta Lake (Parquet + transaction log) |

Every message in Kafka reached bronze. The backfill messages and the silver backfill rows differ by 70 records: 27 failed the data-quality rules (all for a negative flow duration) and were quarantined. The remaining 43 are identical rows that silver deduplicates through its content hash (`flow_uid`); CICIDS2017 contains duplicate rows.

### 4.2 Attack-Family Distribution

The 15 raw labels are mapped to 8 families. Normal traffic is the largest family, followed by DoS, Port Scanning and DDoS. Brute Force, Web Attacks, Bots and Infiltration are much rarer. Because benign flows were downsampled to 20%, attacks make up about 55% of the replayed traffic, far more than in a real network.

![Flows by family in the silver table, backfill split (percentage view).](data/report_figures/fig2_families.png)

### 4.3 Temporal Activity

Hourly flow counts follow the CICIDS2017 attack schedule. Normal traffic stays below about 21,000 flows per hour (after downsampling), while attack traffic shows sharp peaks: about 183,000 flows in one hour on Wednesday 5 July (DoS) and about 111,000 in one hour on Friday 7 July afternoon (port scan and DDoS). Windows use a 10-minute watermark, so rows that arrive more than 10 minutes late in event time are dropped from the gold windows; the windows therefore count slightly fewer flows than silver.

![Normal and attack flows per hour (event time), backfill split.](data/report_figures/fig3_hourly.png)

### 4.4 Kafka Ingestion and Streaming Throughput

All flows enter through Kafka. The producer is idempotent, waits for all in-sync replicas (acks=all), compresses with lz4 and keys each message by source IP, so all flows from one host land in the same partition and stay in order. The demo runs on the lite profile (one broker, replication factor 1); the full profile uses three brokers with replication factor 3 and `min.insync.replicas=2`.

| Topic | Partitions | Replication (lite / full) | Purpose |
|---|---|---|---|
| flows.raw | 12 | 1 / 3 | Raw flow records from the producer |
| flows.scored | 6 | 1 / 3 | Every live flow with its prediction |
| alerts | 3 | 1 / 3 | Flows that triggered an alert |
| flows.dlq | 1 | 1 / 3 | Dead-letter queue for malformed records |

The StreamingQueryListener records the progress of every micro-batch. The table below shows the batches that carried input during the live replays. The producer paces live traffic to a fixed speed-up of real time, so these rates follow the replay rate rather than the cluster's limit.

| Query | Batches | Rows | Avg rows/s | Peak rows/s |
|---|---|---|---|---|
| Q1 bronze (Kafka → Delta) | 59 | 64,530 | 36 | 185 |
| Q2 silver (parse, DQ, enrich) | 43 | 63,582 | 32 | 219 |
| Q3a gold timeline | 20 | 46,061 | 34 | 189 |
| Q3b gold hosts | 17 | 37,348 | 46 | 244 |
| Q4 scoring | 18 | 34,838 | 38 | 169 |

![Average and peak throughput per streaming query.](data/report_figures/fig4_throughput.png)

Fixing the bottlenecks found on the cluster (Section 6) made the slowest queries several times faster:

| Query | Micro-batch time before | After |
|---|---|---|
| Q4 scoring | 48–138 s | 12–23 s |
| Q3b gold per-host windows | 140–363 s | 7–33 s |
| Gold MERGE scan time | 76–248 s | 4–27 s |

### 4.5 Delta Lake Medallion Tables

Each layer is a Delta table, so every write is an ACID transaction with a version number. Training reads silver pinned to version 9 through Delta time travel, so the model can always be traced to the exact data it was trained on. Bronze and silver also hold every live replay of the demo (each replay carries its own `run_id`).

| Layer | Table | Rows | Delta version |
|---|---|---|---|
| Bronze | flows | 1,279,518 | 125 |
| Silver | flows | 1,274,985 | 65 |
| Silver | quarantine | 37 | 15 |
| Gold | attack_timeline | 2,641 | 38 |
| Gold | host_profile | 70,466 | 29 |
| Gold | port_scan_candidates | 102 | 13 |
| Gold | scored_flows | 27,099 | 11 |
| Gold | model_predictions | 162,029 | 0 |

### 4.6 Machine Learning with Spark MLlib

A Spark MLlib Pipeline (Imputer, VectorAssembler, StandardScaler) prepares 70 flow features. A RandomForest classifier predicts the attack family, and a KMeans model (k = 8) fitted on benign flows gives an anomaly score (distance to the nearest centroid, with the threshold at the 99.5th percentile of benign distances, taken from approxQuantile). A flow raises an alert when the attack probability is at least 0.6 or the anomaly score exceeds the threshold. The RandomForest is a classifier of known attack families; it is not a guarantee against new attack types.

| Metric | Value | Metric | Value |
|---|---|---|---|
| Training rows | 647,533 | Accuracy | 99.48% |
| Test rows (held out) | 162,029 | Macro-F1 | 0.907 |
| Features | 70 | RF attack-probability AUC | 0.9997 |
| RandomForest trees / depth | 50 / 10 | KMeans anomaly AUC | 0.709 |
| Training data | silver @ v9 | Alert precision / recall | 99.23% / 99.63% |

The large families score an F1 above 0.99. Macro-F1 is pulled down by the rare families: Infiltration has only 8 test flows (recall 0.50) and Bots has 315 (precision 0.62). The alert precision is optimistic because benign traffic was downsampled.

![Held-out F1 score per class.](data/report_figures/fig5_f1.png)

![Top 10 RandomForest feature importances.](data/report_figures/fig6_importance.png)

### 4.7 Live Scoring and Alerts

The scoring query skips batches until a model exists, then loads the newest registered version without a restart (model v1 was hot-loaded mid-stream in each demo run). Each scored flow is written to Delta, to the flows.scored topic and, if it raises an alert, to the alerts topic and SQLite.

| Measure | Value |
|---|---|
| Live flows scored | 27,099 |
| Real attacks among them | 12,647 |
| Alerts raised | 12,649 |
| Alert precision / recall | 99.19% / 99.21% |
| Predicted family = true family | 99.46% |
| Alert reason: RandomForest / both / KMeans only | 12,459 / 131 / 59 |
| False-positive alerts (true family Normal) | 102 (0.81%) |
| Model version | v1 (hot-loaded without a restart) |

Several live replays were run while the pipeline was being tuned; the scoring query skipped the backlog from those runs, so the scored table covers the 27,099 flows scored in the final runs. These flows were never used in training, so the live numbers are a genuine held-out evaluation.

![Live alerts by true family and predicted family (log scale).](data/report_figures/fig7_live_alerts.png)

### 4.8 Window Analytics and Port-Scan Detection

The gold host query counts, for each source IP and 1-minute window, the flows and the approximate number of distinct destination ports (approx_count_distinct, HyperLogLog). A window with more than 50 distinct ports is a port-scan candidate. This rule works by counting alone, without the ML model.

| Measure | Value |
|---|---|
| Attack-timeline windows | 2,641 |
| Per-host window rows | 70,466 |
| Port-scan candidate windows | 102 |
| Distinct source IPs flagged | 9 |

172.16.0.1, the address the attacker's traffic arrives from inside the CICIDS2017 testbed, touched up to 1,034 ports in one minute. Some public IPs with a single flagged window are likely false positives of the simple threshold rule.

![Port-scan candidate source IPs.](data/report_figures/fig8_port_scans.png)

## 5. Understanding and Application of Big Data Analytics Concepts

| Concept | Application in the Project |
|---|---|
| Distributed messaging | Kafka with 12 partitions keyed by source IP, idempotent producer, replication factor 3 in the full profile. |
| Kappa architecture | One streaming path; backfill and live data are both replayed through Kafka. |
| Stream processing | Spark Structured Streaming micro-batches with checkpoints and backpressure (maxOffsetsPerTrigger, maxFilesPerTrigger). |
| Exactly-once processing | Kafka offsets in checkpoints, Delta transactional sinks and idempotent writes (txnAppId / txnVersion). |
| Event-time windows | 1-minute tumbling windows with a 10-minute watermark for late data, kept in the RocksDB state store. |
| Lakehouse storage | Delta Lake MERGE, idempotent appends, time travel, partition pruning, OPTIMIZE / Z-ORDER and VACUUM. |
| Data quality | Validation rules, a quarantine table and a Kafka dead-letter topic. |
| Broadcast joins | Small subnet and port dimension tables joined to every flow in silver. |
| Distributed ML | MLlib Pipeline, RandomForest and KMeans trained on the Spark cluster. |
| Model serving | Versioned model registry, hot reload inside the streaming query. |
| Fault tolerance | Checkpoints plus idempotent writes; queries resume after a kill and restart. |
| Visualisation | Streamlit charts, tables and live pipeline metrics. |

What the team learned:

- Running Kafka and reading it reliably from Spark.
- Validating counts between Kafka, bronze, silver and gold.
- Fitting a distributed pipeline into the memory of one laptop.
- Making streaming writes idempotent so replays do not create duplicates.
- Interpreting model metrics carefully when classes are imbalanced or downsampled.

## 6. Challenges and Solutions

| Challenge | Solution |
|---|---|
| Kafka deleted data because records carried 2017 timestamps | Records use the send time; bronze fails loudly on data loss (failOnDataLoss=true). |
| Executor out of memory on the partitioned bronze write | Unpartitioned bronze, smaller batches and explicit memory caps. |
| Streaming app killed by Docker (out of memory) | Rate limits on every Delta stream source, a cap on direct memory, two small executors, and no batch job alongside the stream. |
| Slow, hot laptop on the full replay | All attacks kept, benign flows sampled to 20%, the single-broker lite profile and a sampled live replay. |
| Silver deduplication re-scanned the whole table each batch | Replaced the MERGE with Delta idempotent append; gold MERGEs got partition-pruning and file-skipping predicates. |
| Writes silently skipped after a checkpoint reset | The idempotent-write ID now includes the streaming query's unique ID. |
| CPU starvation from a Delta default (50 snapshot tasks) | spark.databricks.delta.snapshotPartitions set to 4. |
| Python 3.8 inside the Spark image | Library versions pinned to Python 3.8-compatible releases. |
| Late, out-of-order flow records | 10-minute watermark; late rows are dropped from gold windows. |

## 7. Conclusion and Future Scope

The platform streams about 1.01 million CICIDS2017 flows through Kafka into a Delta Lake lakehouse with Spark Structured Streaming, trains a RandomForest with macro-F1 0.907 using Spark MLlib, scores live traffic with a model that can be swapped without a restart, and presents the results through a Streamlit dashboard. On held-out live traffic, 99.2% of alerts were real attacks and 99.2% of attacks were caught. The results come from one laptop and a downsampled dataset, and are best read as a working demonstration of the pipeline.

Future Scope –

- Run on a multi-node cluster with the full, undownsampled dataset, to measure precision on realistic traffic.
- Better handling of rare classes (Bots, Infiltration, Web Attacks).
- Per-alert explanations (for example SHAP) and a notification service subscribed to the alerts topic.
- Automated data-quality and model-drift checks, and validation on other intrusion-detection datasets.
