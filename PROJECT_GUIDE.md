# Project Guide: What Every Part Does, Where It Lives, and Why

A plain-language walkthrough of the Kappa network-flow anomaly detection platform, for
explaining it end to end. For the deeper engineering detail, see [ARCHITECTURE.md](ARCHITECTURE.md).

---

## 1. The project in one paragraph

We detect network attacks in real time. The data is CICIDS2017: 2.83 million recorded network
flows (one flow = one connection between two computers, summarised as ~80 numbers such as duration,
bytes and packet counts), each labelled as normal or as one of 14 attack types. Every flow enters
the system through **Kafka**. A continuously running **Spark Structured Streaming** application
cleans the flows and builds a **Delta Lake** lakehouse. **Spark MLlib** trains a model on that
lakehouse, and the same streaming application then uses the model to score new flows as they
arrive. Alerts appear on a **Streamlit dashboard** and on a Kafka `alerts` topic.

---

## 2. The journey of one network flow

Follow a single flow from a CSV file to an alert on the dashboard:

```
1. CSV row              data/raw/cicids2017/Wednesday-workingHours.pcap_ISCX.csv
        │  replay_producer.py reads it, keeps it as raw text, turns it into JSON
        ▼
2. Kafka message        topic flows.raw, key = source IP → lands on one of 12 partitions
        │  q1_bronze reads it (Spark Structured Streaming)
        ▼
3. Bronze row           data/lake/bronze/flows  (raw JSON + Kafka partition/offset)
        │  q2_silver parses, validates, enriches
        ▼
4. Silver row           data/lake/silver/flows  (typed columns, clean label, zone, service)
        │                                   │
        │ q3a / q3b aggregate it            │ q4_scoring scores it (live split only)
        ▼                                   ▼
5. Gold windows         data/lake/gold/...  Scored row: data/lake/gold/scored_flows
   (attacks per minute, per-host counts)    + Kafka flows.scored
                                            + if it's an alert: Kafka alerts topic and
                                              SQLite data/serving/alerts.db
        ▼                                   ▼
6. Dashboard            src/dashboard/app.py reads SQLite + Delta → http://localhost:8501
```

The model used in step 5 was trained earlier by `make train`, from the silver rows of the
*backfill* split.

---

## 3. Component by component

Each entry covers **what** it does, **where** it is, **why** we need it, and the **big data concept** it shows.

### 3.1 The dataset
- **What:** 8 CSV files, one per capture period (Monday to Friday, 3–7 July 2017), 2,830,743 flows, 85 columns,
  with source/destination IP, ports and timestamps.
- **Where:** `data/raw/cicids2017/`.
- **Why this version:** the earlier Kaggle copy (`cicds.csv`) had no IPs or timestamps. Without those,
  per-host analysis and event-time windows are impossible.
- **Demo size:** the producer keeps every attack flow and 20% of normal flows, giving 1,012,310 flows.
  This keeps the laptop fast; the trade-off is that attacks are over-represented (about 55% instead of 20%).

### 3.2 Replay producer: the "sensor"
- **What:** reads the CSVs and sends each flow to Kafka as a JSON message, as if a network sensor were
  reporting it live.
- **Where:** `src/ingestion/replay_producer.py`. Run with `make backfill` and `make live`.
- **Why:** we don't have a live network, so we replay a recorded one. The pipeline can't tell the difference.
- **How it decides what to send:**
  - **Backfill vs live split:** a hash of each row puts about 80% in *backfill* (used to train the
    model) and 20% in *live* (held out; the model never sees it in training). The hash is deterministic,
    so every run splits the same way.
  - **Pacing:** `--speedup 600` replays 600× faster than the real capture. Long idle gaps (nights)
    are capped, so the demo doesn't stall.
  - **Re-stamping:** live flows get the current time as their event time, so the demo looks like
    traffic happening now.
  - **Demo sample:** `make live` sends only a fixed 15% sample of the live split (`LIVE_PCT`).
- **Big data concepts:**
  - **Message keys:** each flow is keyed by its source IP, so all of one host's flows go to the same
    partition, in order.
  - **Idempotent producer and `acks=all`:** retries never create duplicates, and a message counts
    as sent only once the broker has stored it.
  - **Batching and compression:** `linger.ms`, 256 KB batches and lz4.

### 3.3 Apache Kafka: the entry point and the log
- **What:** a distributed, append-only log that stores the messages until the stream reads them.
- **Where:** a Docker container (`kafka-1`). Topics are created by `src/ingestion/create_topics.py`
  (`make topics`). Kafka UI runs at http://localhost:8080.
- **Topics:**

  | Topic | Partitions | Contents |
  |---|---|---|
  | `flows.raw` | 12 | every incoming flow |
  | `flows.scored` | 6 | every flow after the model scored it |
  | `alerts` | 3 | only the flows that raised an alert |
  | `flows.dlq` | 1 | broken records (dead-letter queue) |

- **Why:** Kafka decouples the producer from the processing. If Spark is slow or restarts,
  messages wait safely in Kafka. And because Kafka keeps the log, data can be **replayed** at any time.
- **Big data concepts:**
  - **Partitions:** the unit of parallelism. Up to 12 tasks can read `flows.raw` at once.
  - **Consumer offsets and lag:** Kafka UI shows how far behind the reader is.
  - **Replication:** we run one broker (replication factor 1) to fit in 8 GB. On a real cluster,
    several brokers would each keep a copy of every partition, so one could fail without losing data.
  - **KRaft:** Kafka runs without ZooKeeper.

### 3.4 The streaming application: one app, five queries
- **What:** one always-running Spark program that runs five streaming queries side by side.
- **Where:** `src/streaming/stream_app.py`. Run with `make stream`. It runs inside the `spark-master`
  container on the Spark cluster.
- **Why one app:** the queries share the cluster's two cores through the FAIR scheduler, which uses
  much less memory than five separate apps.
- **Big data concepts:**
  - **Micro-batches:** every 5 seconds each query processes whatever new data has arrived.
  - **Checkpoints** (`data/checkpoints/<query>`): each query records how far it got. After a crash it
    resumes exactly where it stopped.
  - **Backpressure:** each micro-batch is capped (20,000 Kafka messages, or 24 files / 16 MB of a Delta
    table), so a backlog can never overwhelm memory.

#### Q1 Bronze: raw storage
- **Where:** `src/streaming/bronze.py` → `data/lake/bronze/flows`.
- **What:** copies every Kafka message into a Delta table exactly as received, plus its Kafka
  partition and offset.
- **Why:** a permanent, replayable raw record. If later logic changes, silver can be rebuilt from
  bronze without going back to Kafka.

#### Q2 Silver: clean and trustworthy data
- **Where:** `src/streaming/silver.py` → `data/lake/silver/flows` (good rows) and
  `data/lake/silver/quarantine` (bad rows).
- **What, in order:**
  1. **Parse** the JSON with a fixed schema.
  2. **Fix timestamps:** two date formats, and afternoon files written as "3:15" meaning 15:15.
  3. **Fix numbers:** `Infinity` and `NaN` become empty.
  4. **Clean labels:** 15 raw labels become 8 families (Normal, DoS, DDoS, Port Scanning, Brute Force,
     Web Attacks, Bots, Infiltration).
  5. **Data-quality rules:** unparseable message, bad timestamp, negative duration, unknown label.
     Failing rows go to the quarantine table **and** the `flows.dlq` topic instead of being dropped.
  6. **Enrich:** add the network zone of both IPs (internal / attacker network / external) and the
     service name of the port (80 → HTTP). These lookups are tiny tables, **broadcast-joined** onto the stream.
  7. **Give every flow a unique ID** (`flow_uid`, a SHA-256 hash), used to deduplicate.
- **Why:** everything downstream (analytics, ML, alerts) is only as good as this data.

#### Q3a / Q3b Gold: analytics
- **Where:** `src/streaming/gold.py` → `data/lake/gold/attack_timeline`, `gold/host_profile`,
  `gold/port_scan_candidates`.
- **What:**
  - **Q3a, attack timeline:** for every 1-minute window, how many flows of each attack family.
  - **Q3b, host profiles:** for every source IP in every 1-minute window: flows, bytes, distinct
    destinations and distinct ports contacted, and the share with SYN/RST flags.
  - **Port-scan candidates:** hosts that touched more than 50 different ports in one minute. In the
    real data, `172.16.0.1` touched 1,034.
- **Big data concepts:**
  - **Event time vs processing time:** windows use the time the flow happened, not when Spark saw it.
  - **Watermark (10 minutes):** how late data may arrive and still count. It also lets Spark discard
    old window state, so memory stays bounded.
  - **Tumbling windows:** fixed, non-overlapping 1-minute windows.
  - **HyperLogLog** (`approx_count_distinct`): counts distinct ports cheaply, with no list of every port.
  - **Stateful streaming:** open windows are kept in the RocksDB state store, saved in the checkpoint.
  - **MERGE (upsert)** with partition pruning: updated windows overwrite their old values, and Delta
    skips the files that can't match.

#### Q4 Scoring: real-time machine learning
- **Where:** `src/streaming/scoring.py` → `data/lake/gold/scored_flows`, Kafka `flows.scored` and
  `alerts`, SQLite `alerts`.
- **What:** for every *live* flow:
  1. Load the newest model from the registry. If a new version was registered, swap to it without
     restarting (**hot reload**).
  2. Run the RandomForest: predicted attack family plus P(attack).
  3. Run KMeans: the distance from the nearest "normal traffic" cluster centre (the anomaly score).
  4. **Raise an alert** if the RandomForest says attack with P ≥ 0.6, **or** the distance is above
     the threshold (26.85, the 99.5th percentile of normal traffic).
  5. Write the result to all four outputs.
- **Why two models:** the RandomForest recognises the attack types it was trained on. KMeans only
  knows what *normal* looks like, so it can flag unusual traffic, including attack types it never saw.

#### Metrics listener
- **Where:** `src/streaming/metrics_listener.py` → SQLite `stream_metrics`.
- **What:** after every micro-batch, records rows processed, rows per second, batch duration and
  watermark. It also reports Spark's Kafka position back to Kafka, so Kafka UI can show the lag.
- **Why:** this feeds the dashboard's Pipeline tab, which is how you see the system working.

### 3.5 Delta Lake: the lakehouse
- **What:** a table format on top of Parquet files that adds database guarantees.
- **Where:** `data/lake/` (bronze, silver, gold). The project folder is shared with every container,
  acting as the cluster's shared file system (the role HDFS or S3 plays on a real cluster).
- **Why not plain Parquet files:**
  - **ACID transactions:** every micro-batch is one commit, all or nothing. A crash never leaves half a batch.
  - **A table can be a streaming source:** silver reads bronze, and gold and scoring read silver, as
    streams. Every stage is a queryable table.
  - **Idempotent writes:** a retried batch is detected (`txnAppId` + `txnVersion`) and skipped, so no duplicates.
  - **Time travel:** training records exactly which version of silver it learned from.
- **Medallion layers:** **bronze** = raw, **silver** = clean, **gold** = aggregated or scored. Each
  layer is more refined and more useful.
- **Maintenance:** `src/lakehouse/maintenance.py` (`make maintenance`) compacts the many small files
  that streaming creates (OPTIMIZE), sorts data by source IP so lookups skip files (Z-ORDER), shows the
  commit history and time travel, and removes old files (VACUUM).

### 3.6 Machine learning (Spark MLlib)
- **Where:**
  - `src/ml/train_pipeline.py`: training (`make train`)
  - `src/ml/model_utils.py`: shared features and scoring logic
  - `src/ml/registry.py`: model versions
  - `src/ml/evaluate.py`: metrics and plots (`make evaluate`, `make evaluate-live`)
- **What training does:**
  1. Reads silver's backfill split, pinned to one Delta version (647,533 training rows).
  2. Splits 80/20 into train and test, stratified per attack family.
  3. Gives rare attack types more weight, so the model doesn't ignore them.
  4. Builds a feature pipeline: fill missing values (Imputer), combine 70 features into one vector
     (VectorAssembler), and scale them (StandardScaler).
  5. Trains a RandomForest (50 trees, depth 10) and a KMeans (8 clusters, on normal traffic only).
  6. Evaluates on the held-out test set, then saves everything as a new version in
     `data/models/registry/vN`.
- **Why MLlib and not scikit-learn:** MLlib trains on the cluster on the full dataset. There's no
  sampling into one machine's memory.
- **Results:**
  - **Held-out test:** 99.2% of alerts were real attacks, and 99.6% of attacks were caught.
  - **Live stream:** 99.4% and 99.2%.
  - **Weak spots:** rare families such as Bots and Infiltration, which have very few examples.

### 3.7 Serving layer: SQLite
- **What:** a small database of query-ready tables for the dashboard: alerts, host windows,
  port-scan candidates, stream metrics and pipeline events.
- **Where:** schema in `src/serving/schema.sql`, writer in `src/serving/sqlite_sink.py`, file at
  `data/serving/alerts.db`.
- **Why:** the dashboard needs fast, simple reads. Querying the lakehouse with Spark on every
  dashboard refresh would be slow and need its own Spark job. Every write is `INSERT OR REPLACE` on a
  unique key, so a retried batch never creates duplicates. In production this would be Cassandra,
  Elasticsearch or Postgres.

### 3.8 Dashboard (Streamlit)
- **Where:** `src/dashboard/app.py`. Run with `make dashboard`, then open http://localhost:8501.
- **What each tab shows:**

  | Tab | What it shows | Where the data comes from |
  |---|---|---|
  | 📡 Pipeline | stream status, rows/s per query, Kafka lag | SQLite `stream_metrics`, Kafka, Spark master |
  | 🚨 Alerts | the 4 headline numbers, alerts per minute, the latest alerts with ✅/❌ | Delta `gold_scored` + SQLite `alerts` |
  | 🧠 Model & hosts | model accuracy, port-scan suspects, top attacking IPs | model registry + SQLite |

- **Why it doesn't need Spark:** it reads Delta tables directly with the lightweight `deltalake`
  library, so the dashboard stays fast and uses almost no memory.

#### Reading the dashboard: every item explained in easy words

The page refreshes itself every few seconds, so the numbers move while the demo runs.

**Header and sidebar (always visible)**

| Item | What it means |
|---|---|
| **Kafka → Spark … → real-time alerts** | The whole pipeline in one line. |
| **Auto-refresh / Refresh every (s)** | How often the page re-reads the data (default 5 seconds). |
| **Streaming app: 🟢 running / 🔴 stopped** | Whether `make stream` is running. The dashboard asks the Spark master. If it's stopped, the numbers shown are from the last run. |
| **Serving model: v1** | Which trained model version is judging live traffic. |
| **Cluster UIs** | Links to Kafka UI (topics, partitions, lag), the Spark master and the running Spark app. |

**Tab 1: 📡 Pipeline (is data flowing, and is the stream keeping up?)**

| Item | What it means |
|---|---|
| **Query cards** (e.g. `q1_bronze: 23 rows/s`, "batch 3 · 51 s ago") | One card per streaming query: how many rows per second its **last** micro-batch processed, which batch number that was, and how long ago it finished. A card appears only after that query finishes its first batch, so `q3b_gold_hosts` and `q4_scoring` (the slowest) show up last. |
| **Kafka consumer lag (flows.raw)**, total + bar per partition | **Lag = messages waiting in Kafka that Spark hasn't read yet.** One bar for each of the 12 partitions. Lag goes up while the producer sends faster than Spark reads; Kafka holds the messages safely, so nothing is lost. **Lag 0 means bronze has read everything.** Give silver, gold and scoring another minute or two after that. |
| **Throughput per query** (line chart) | Rows per second for each query over time. Each dot is one micro-batch. Spikes are normal: a batch that picked up a big chunk of data. |

**Tab 2: 🚨 Alerts (what was detected, and was it right?)**

| Item | What it means |
|---|---|
| **Live flows scored** | How many live flows the model has judged so far. |
| **Alerts raised** (and "% of flows") | How many of those flows the model flagged as attacks. The share is high (about 50%) because the demo keeps every attack but only 20% of normal traffic. |
| **Alerts that were real attacks** | **Precision**: of all alerts, the share that really were attacks (every flow carries its true label, so we can check). About 99% means very few false alarms. |
| **Real attacks caught** | **Recall**: of all real attacks, the share that raised an alert. About 99% means very few attacks missed. |
| **Alerts per minute by predicted attack family** | A bar chart over time; colours = the attack type the model predicted (DoS, Brute Force, Port Scanning…). You can see attacks start and stop. |
| **Latest alerts** (table + family filter) | The newest alerts, one per row: when, who → whom, which port/service, the predicted type, `attack_prob` (the model's confidence, 0 to 1), `alert_reason` (`rf` = RandomForest recognised a known attack, `anomaly` = KMeans found it unusual, `rf+anomaly` = both) and `ground_truth` (the real label). |
| **correct?** ✅ / ❌ | ✅ = the true label is an attack, so the alert was right. ❌ = it was normal traffic (a false alarm). |
| "No alerts yet" | Scoring hasn't finished its first batch (it takes 1–2 minutes after `make live` starts), or no live data has been sent. |

**Tab 3: 🧠 Model & hosts (how good is the model, and who is attacking?)**

| Item | What it means |
|---|---|
| **Metrics on: Held-out test split / Live stream** | Switch between the 20% of history that training never saw, and the demo flows (also never seen in training). |
| **Macro-F1** | The average score across all 8 attack types, each counted equally, so rare types (Bots, Infiltration) pull it down. 0.907 on the test split. |
| **RF AUC / KMeans AUC** | How well each model's score separates attacks from normal traffic (1.0 = perfect, 0.5 = guessing). RF ≈ 1.0, KMeans ≈ 0.71: KMeans is weaker alone, which is why it's only the backup. |
| **Alert precision / Alert recall** | The same as on the Alerts tab: about 99% of alerts were real, and about 99% of attacks were caught. |
| **Per-family table** | Precision, recall, F1 and `support` (how many test flows) for each attack type. Big families score about 0.99; tiny ones score lower because there are few examples. |
| **Line under the table** | The silver version the model was trained on (time travel, v9), the number of training rows, the RandomForest settings and the anomaly threshold. |
| **Port-scan suspects** | IPs that touched **more than 50 different ports in one minute**, the classic sign of a port scan. `max_ports` = the most ports in a single minute, `windows` = in how many minutes. This comes from counting in the gold layer, **not** from the ML model. `172.16.0.1` (the attacker in the dataset) is at the top. |
| **Top alerting hosts** | The 15 IPs with the most alerts, their zone (internal / attacker network / external) and the attack types predicted for them. |

**Three things to point at during the demo**
1. **Pipeline:** Kafka lag goes up while `make live` sends, then falls back to 0. Kafka buffers, and Spark catches up.
2. **Alerts:** the alert bars appear minute by minute, with about 99% precision and recall against the true labels.
3. **Model & hosts:** `172.16.0.1` is flagged as a port scanner by simple counting in gold, without any ML.

### 3.9 Cluster and infrastructure (Docker)
- **Where:** `docker/docker-compose.yml`. The Spark image is `docker/spark/Dockerfile`.
- **The containers:**

  | Container | Role | UI |
  |---|---|---|
  | `kafka-1` | the Kafka broker | — |
  | `kafka-ui` | web view of topics and lag | http://localhost:8080 |
  | `spark-master` | schedules work and runs the drivers | http://localhost:8081 (and the app UI on 4040) |
  | `spark-worker-1` | runs the actual tasks: 2 cores, two 800 MB executors | — |

- **Why Docker:** the whole cluster starts with one command, and it's identical on any machine.
- **Spark standalone:** the master hands out work, and workers run it in **executors**. The streaming
  app runs as two 1-core executors. Batch jobs (train, evaluate) run only while the stream is stopped,
  because 6 GB of Docker memory can't hold two drivers.

### 3.10 Configuration, commands and tests
- **`conf/pipeline.yaml`:** the single source of truth for topics, partitions, file paths, window
  sizes, thresholds, memory settings and ML parameters. Change a number there, not in the code.
- **`Makefile`:** every operation is one command (`make help` lists them). Long commands keep the Mac
  awake with `caffeinate`, and batch jobs refuse to start while the stream runs.
- **`tests/`:** 18 automated tests (`make test`). They run the real streaming queries on synthetic data
  shaped like CICIDS2017, without Docker or Kafka, and cover data quality, duplicates, windows,
  training, scoring, hot reload, evaluation and lakehouse maintenance.

### 3.11 Where everything lives

```
anomaly-detection-platform/
├── conf/pipeline.yaml          all settings
├── docker/                     cluster definitions + Spark image
├── src/
│   ├── common/                 config, Spark session, schema, Delta helpers, lookup tables
│   ├── ingestion/              producer, topic creation, "wait until caught up"
│   ├── streaming/              stream_app + bronze, silver, gold, scoring, metrics listener
│   ├── ml/                     training, scoring logic, model registry, evaluation
│   ├── lakehouse/              Delta maintenance job
│   ├── serving/                SQLite schema + writer
│   └── dashboard/              Streamlit app
├── tests/                      18 tests + synthetic data generator
├── data/                       (not in git)
│   ├── raw/cicids2017/         the 8 CSVs
│   ├── lake/                   Delta tables: bronze/, silver/, gold/
│   ├── checkpoints/            one folder per streaming query
│   ├── models/registry/        v1/, latest.json, plots
│   ├── serving/alerts.db       SQLite cache for the dashboard
│   └── spark-events/, logs/    job history and logs
├── README.md, ARCHITECTURE.md  how to run / deep design
├── REPORT.md                   DA3 report (→ Digital_Assignment_3_Report.pdf via make report)
└── CHECKPOINT.md               build log: every change, problem and fix
```

---

## 4. Big data concepts at a glance

| Concept | One-line meaning | Where you can show it |
|---|---|---|
| **Kappa architecture** | One streaming path for everything. Reprocessing = replaying the log | The whole design. There's no separate batch pipeline |
| **Distributed log** | Kafka stores ordered, replayable messages | Kafka UI, topic `flows.raw` |
| **Partitioning** | Data split into parallel pieces | 12 partitions; key = source IP |
| **Micro-batch streaming** | Process new data every few seconds | Spark UI, Structured Streaming tab |
| **Exactly-once** | No loss, no duplicates, even after a crash | Checkpoints + Delta transactions + idempotent writes |
| **Backpressure** | Cap the work per batch | `maxOffsetsPerTrigger`, `maxFilesPerTrigger` |
| **Event time and watermarks** | Use when it happened; tolerate late data | Gold windows (10-minute watermark) |
| **Windowed aggregation** | Group by time window | 1-minute windows per attack type and per host |
| **Approximate counting** | HyperLogLog distinct counts | Distinct ports per host |
| **Broadcast join** | Ship a small table to every worker | Zone and service lookups in silver |
| **Data lakehouse / medallion** | Raw → clean → aggregated tables on cheap storage | `data/lake/bronze`, `silver`, `gold` |
| **ACID and time travel** | Transactions, plus reading old versions | Delta history; training pins its version |
| **Partition pruning and file skipping** | Read only what's needed | Gold MERGE predicates; Z-ORDER |
| **Small-file compaction** | Merge many tiny files | `make maintenance` (OPTIMIZE) |
| **Distributed ML** | Train on the cluster on all the data | MLlib Pipeline, RandomForest, KMeans |
| **Model serving and hot swap** | Use the newest model without downtime | Model registry; scoring reloads `latest.json` |
| **Data quality and DLQ** | Bad records are kept and inspected, not lost | Quarantine table, `flows.dlq` topic |
| **Observability** | Measure throughput and lag | Metrics listener, dashboard, Kafka UI |

---

## 5. Likely questions and short answers

- **Why Kafka in front of Spark?** It decouples the two. Data waits safely if Spark is slow or
  restarting, and it can be replayed.
- **Why not just read the CSVs with Spark?** That's a batch (Lambda-style) path. Here *everything* is
  a stream, which is how a real sensor feed works, and one code path is simpler to keep correct.
- **What happens if Spark crashes?** Each query restarts from its checkpoint. Delta's transactions
  and idempotent writes guarantee nothing is lost or written twice. (Tested: killed with `kill -9`,
  restarted, no duplicates.)
- **Why bronze, silver and gold?** Separation of concerns. Raw data is never lost, clean data is
  reused by both analytics and ML, and gold holds ready-to-use answers.
- **Why two models?** The RandomForest is very accurate on known attack types. KMeans flags
  anything unusual, as a safety net for unknown attacks.
- **Why is the precision so high?** The demo keeps every attack but only 20% of normal traffic, so
  attacks are about 55% of the data. On real traffic, where attacks are rare, precision would be lower.
- **Why only one Kafka broker and one worker?** It's an 8 GB laptop. On a real cluster the same code
  would run with more brokers (with replication) and more workers; only the Docker setup changes.
- **What was hardest?** Making it run reliably on one laptop. Kafka deleted data because of the 2017
  timestamps, executors ran out of memory, and a Delta default created 50 tasks per batch on 2 cores.
  All are documented in [CHECKPOINT.md](CHECKPOINT.md) and the report.
