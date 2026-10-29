# File-by-File Program Flow

This guide follows the program **in the order it actually runs**, from starting the cluster to
the dashboard, and describes each file as it comes into play: **what** it does and **why** it's
needed.

- For the simple story without code, see [WORKFLOW.md](WORKFLOW.md).
- For concepts and components, see [PROJECT_GUIDE.md](PROJECT_GUIDE.md).

---

## How to explain this to the professor (easy script)

If you're asked to "explain the flow file by file", follow the data, not the folders. Start where
the data comes in, end where it's shown, and say one or two simple lines per file. Have these
files open in the editor, in this order, and click through them as you talk.

### The one-breath summary (say this first)

> "The data starts as CSV files. A **producer** sends each row into **Kafka**. One always-running
> **Spark** program picks the rows up and passes them through **bronze** (raw copy), **silver**
> (cleaned) and **gold** (summaries). A **training** script teaches the models using silver, and
> a **scoring** step uses those models to raise alerts on new traffic. The **dashboard** shows the
> results."

### Then walk through the files (10 steps)

**1. `conf/pipeline.yaml`: the settings file**
> "All the settings are in one file: topic names, how many partitions, the window size, the
> model settings. Every other file reads from here, so nothing is hard-coded."

**2. `docker/docker-compose.yml`: starts the machines**
> "This starts Kafka, the Kafka UI, a Spark master and a Spark worker as containers, so one
> laptop behaves like a small cluster."

**3. `src/ingestion/create_topics.py`: creates the Kafka topics**
> "This creates the 4 Kafka topics. `flows.raw`, with 12 partitions, is where all incoming data
> goes."

**4. `src/ingestion/replay_producer.py`: sends the data into Kafka**
> "We don't have a real network, so this program pretends to be one. It reads the CSV rows in time
> order and sends each one to Kafka. It splits them 80% for training and 20% kept hidden for live
> testing. The key is the source IP, so one computer's traffic stays in order."

**5. `src/streaming/stream_app.py`: the main Spark program**
> "This is the heart of the project. It's one Spark program that runs all the time and starts 5
> queries, one for each stage of the pipeline."

**6. `src/streaming/bronze.py`: query 1, raw copy**
> "This reads from Kafka and saves every message exactly as it arrived, into the bronze Delta
> table. It's our backup of the original data."

**7. `src/streaming/silver.py`: query 2, cleaning**
> "This cleans the data: it fixes the timestamps, turns broken numbers like Infinity into empty
> values, tidies the labels into 8 attack families, removes duplicates, and moves bad records to
> a quarantine table instead of deleting them. The clean data goes into silver."
> *(It uses `schemas.py` for column names and labels, and `dims.py` for the zone and port lookup tables.)*

**8. `src/streaming/gold.py`: queries 3a and 3b, summaries**
> "This counts the traffic per minute: attacks per minute, and for each computer, how many
> connections and how many different ports. Any computer that touches more than 50 ports in a
> minute is flagged as a port scanner."

**9. `src/ml/train_pipeline.py` → `src/streaming/scoring.py`: the machine learning**
> "The training script learns from the 80% history in silver. A RandomForest learns the known
> attack types, and KMeans learns what normal traffic looks like. It saves the models as version 1
> in the model registry (`registry.py`). Then query 4, `scoring.py`, loads that model *while the
> stream is running*, checks every new live flow, and raises an alert if either model says
> 'attack'. Alerts go to Kafka, to Delta and to SQLite."

**10. `src/dashboard/app.py`: the dashboard**
> "This is the web page. It reads the alerts and stats from SQLite and the Delta tables, and
> shows live throughput, attacks per minute, the top attackers and the alert feed, with a tick or
> cross showing whether each alert was right."

### If the professor asks about the "helper" files

| File | One line in easy words |
|---|---|
| `Makefile` | "Shortcuts: each step is one command, like `make stream` or `make train`." |
| `src/common/config.py` | "Reads the settings file and gives every other file the right paths and addresses." |
| `src/common/spark.py` | "Starts Spark the same way everywhere, with Delta switched on and memory limits set." |
| `src/common/lake.py` | "Small helpers for saving to Delta safely, so a batch is never saved twice." |
| `src/ml/model_utils.py` | "The feature preparation and alert rule, shared by training and scoring, so both treat data exactly the same." |
| `src/streaming/metrics_listener.py` | "Records the speed of every batch, which the dashboard shows." |
| `src/serving/sqlite_sink.py` | "Saves results into a small, fast database that the dashboard reads." |
| `src/ml/evaluate.py` | "Measures how accurate the model is on data it never saw." |
| `src/lakehouse/maintenance.py` | "Delta housekeeping: compacts small files, shows table history and time travel." |
| `src/serving/demo_reset.py` | "Clears the live results so the dashboard starts fresh for a new demo." |
| `tests/` | "18 automatic tests that run the real pipeline on small fake data." |

### Tips

- **Follow the data:** CSV → producer → Kafka → bronze → silver → gold / scoring → dashboard. If you lose your place, go back to this line.
- **For each file, say three things:** what goes in, what it does, and what comes out.
- **Show, don't just say:** while explaining `silver.py`, point at `parse_event_time` or the data-quality rules; for `gold.py`, point at the `window(...)` and `approx_count_distinct`.
- **Know where each big data concept lives:**
  - partitions → producer and Kafka
  - exactly-once and checkpoints → bronze and `lake.py`
  - watermark and windows → gold
  - broadcast join → silver
  - Delta time travel → training
  - distributed ML → `train_pipeline.py`

---

## The whole flow at a glance

```
make cluster-up ─► docker/docker-compose.yml ─► docker/spark/Dockerfile
make topics ────► src/ingestion/create_topics.py ─► src/common/config.py ─► conf/pipeline.yaml
make stream ────► src/streaming/stream_app.py
                    ├─ src/common/spark.py              (builds the SparkSession)
                    ├─ src/streaming/metrics_listener.py (records every micro-batch)
                    ├─ Q1  src/streaming/bronze.py       Kafka → bronze
                    ├─ Q2  src/streaming/silver.py       bronze → silver  (schemas.py, dims.py, lake.py)
                    ├─ Q3  src/streaming/gold.py         silver → gold    (sqlite_sink.py, schema.sql)
                    └─ Q4  src/streaming/scoring.py      silver → alerts  (registry.py, model_utils.py)
make backfill ──► src/ingestion/replay_producer.py ─► Kafka flows.raw  (stream picks it up)
make wait ──────► src/ingestion/wait_caught_up.py
make train ─────► src/ml/train_pipeline.py ─► model_utils.py ─► registry.py (model v1)
make evaluate ──► src/ml/evaluate.py
make live ──────► src/ingestion/replay_producer.py  (scored live by Q4)
make dashboard ─► src/dashboard/app.py ◄─ SQLite + Delta + registry + Kafka
make maintenance► src/lakehouse/maintenance.py
make demo-reset ► src/serving/demo_reset.py
make test ──────► tests/*.py
make report ────► generate_report_da3_pdf.py ─► report_figures.py
```

---

## Stage 0: Control and configuration

### `Makefile`
- **What:** the "remote control". Every step is one command: `make cluster-up`, `make stream`, `make backfill`, `make train`, `make live`, `make dashboard` and so on. It calls `docker compose`, runs `spark-submit` inside the Spark master container for Spark jobs, and runs host Python for the producer and dashboard.
- **Why:**
  - Nobody needs to remember long `docker compose exec … spark-submit --master spark://…` commands.
  - It holds the safety rules, for example `train` refuses to run while the stream is running (the `NO_STREAM` guard), because two Spark drivers don't fit in 6 GB.
  - It uses `caffeinate` to keep the Mac awake during long runs.

### `conf/pipeline.yaml`
- **What:** the single settings file. It holds:
  - the dataset file list and split percentages (80% backfill, 20% benign kept);
  - topic names, partition counts and retention;
  - Delta table paths and checkpoint paths;
  - Spark memory and cores;
  - trigger interval, watermark, window size and the port-scan threshold (50);
  - ML parameters (trees, depth, k, alert thresholds).
- **Why:** every number lives in **one place**. Changing a setting doesn't mean editing code in five files.

### `src/common/config.py`
- **What:** loads `pipeline.yaml` once (`cfg()`) and offers helpers:
  - `lake("silver_flows")` gives the table path, and `checkpoint("q2_silver")` gives the checkpoint folder;
  - `topic("raw")` gives the topic name;
  - `kafka_bootstrap()` gives the right broker address for inside Docker or on the host;
  - `replication_factor()` gives 1 (one broker);
  - `trigger()` gives "every 5 seconds" (tests switch it to "drain and stop").
- **Why:** the same code runs in three places: inside Docker, on the Mac, and in tests. This file hides those differences.

### `requirements.txt`
- **What:** Python packages for the **host** venv: the producer, the dashboard, the tests and the report.
- **Why:** `make setup` installs them in one go. The Spark containers get their own packages from the Dockerfile.

---

## Stage 1: Start the cluster (`make cluster-up`)

### `docker/docker-compose.yml`
- **What:** defines the 4 containers:
  - **Kafka** in KRaft mode (1 broker);
  - **Kafka UI** on port 8080;
  - the **Spark master** on port 8081;
  - the **Spark worker**.

  The project folder is mounted at `/opt/app` in every Spark container. Memory limits are sized for an 8 GB Mac.
- **Why:** one command starts a small "cluster" on a laptop. The shared folder acts like the shared storage (HDFS/S3) of a real cluster: the Delta tables, checkpoints and models live there, and every container sees them.

### `docker/spark/Dockerfile`
- **What:** builds the Spark image. It starts from `apache/spark:3.5.3-python3`, adds the Kafka-connector and Delta Lake jars, and installs Python libraries (numpy, pandas, pyarrow, confluent-kafka…) pinned to versions that work on the image's Python 3.8.
- **Why:** with the jars baked in, jobs never download anything at start-up. They start fast and work offline.

---

## Stage 2: Create the Kafka topics (`make topics`)

### `src/ingestion/create_topics.py`
- **What:** uses Kafka's AdminClient to create 4 topics from `pipeline.yaml`:

  | Topic | Partitions | Holds |
  |---|---|---|
  | `flows.raw` | 12 | incoming flows |
  | `flows.scored` | 6 | every scored live flow |
  | `alerts` | 3 | alerts only |
  | `flows.dlq` | 1 | broken records |

  It then prints each partition's leader, replicas and ISR. `--recreate` empties the topics, which `make demo-reset` uses.
- **Why:** auto-creation of topics is turned off, so partitions and retention are chosen deliberately, not left to defaults.

---

## Stage 3: Start the always-on stream (`make stream`)

### `src/streaming/stream_app.py`: the entry point of the pipeline
- **What:** one Spark application that:
  1. builds a SparkSession (via `spark.py`);
  2. registers the metrics listener;
  3. creates any missing Delta tables;
  4. starts the **5 streaming queries**: `bronze.start()`, `silver.start()`, `gold.start()` (2 queries) and `scoring.start()`;
  5. waits forever (`awaitAnyTermination`).

  Option: `--only silver` runs a single stage.
- **Why:** this is the Kappa idea, *one* program that builds everything continuously. The queries share one set of executors, using a FAIR scheduler so no query starves the others.

### `src/common/spark.py`
- **What:** `get_spark()` builds every SparkSession in the project the same way:
  - Delta Lake extensions;
  - adaptive query execution (AQE);
  - 8 shuffle partitions;
  - RocksDB state store with a memory cap;
  - Delta snapshot partitions set to 4;
  - memory and core limits for the executors.
- **Why:** one place for all tuning. Several out-of-memory and CPU-starvation fixes live here.

### `src/streaming/metrics_listener.py`
- **What:** a `StreamingQueryListener`. After **every micro-batch of every query**, it writes rows in, rows/second, batch duration, watermark and Kafka offsets into SQLite `stream_metrics`. It also commits bronze's offsets to a Kafka consumer group, so Kafka UI can show **consumer lag**.
- **Why:** this is the pipeline's monitoring. The dashboard's "Pipeline" tab and `make wait` both rely on it.

### Q1: `src/streaming/bronze.py`
- **What:** reads Kafka `flows.raw` with `readStream.format("kafka")`, at most 20,000 records per batch (backpressure). It writes the **raw JSON exactly as received**, plus its partition, offset and Kafka timestamp, to Delta `bronze_flows`. It uses `failOnDataLoss=true`.
- **Why:**
  - Bronze is the untouched backup. If the cleaning logic is ever wrong, silver can be rebuilt from bronze without re-sending anything.
  - Checkpoint + Delta gives **exactly-once** ingestion: after a crash it resumes at the right offset.

### Q2: `src/streaming/silver.py`
- **What:** reads bronze **as a Delta stream** and cleans each record:
  - parses the JSON with a fixed schema;
  - fixes two timestamp formats and the 12-hour clock;
  - casts the numbers and turns `Infinity`/`NaN` into NULL;
  - cleans the labels and maps them into 8 families;
  - applies 4 data-quality rules: bad rows go to `silver_quarantine` and Kafka `flows.dlq`;
  - adds zone and service with **broadcast joins**;
  - removes duplicates with a SHA-256 fingerprint (`flow_uid`).

  Clean rows are appended to `silver_flows`.
- **Why:** everything downstream (gold, training, scoring) needs **clean, trusted** data. Bad records are kept for inspection, not silently lost.
- **Uses:**
  - **`src/common/schemas.py`:** the data "contract" shared by the producer and Spark. It holds the column-name normalisation (`" Flow Duration"` → `flow_duration`, and the duplicated column renamed), the list of raw and numeric columns, the JSON schema, `clean_label()` and `label_family()`.
  - **`src/common/dims.py`:** two tiny lookup tables, subnet → zone (internal / attacker_net) and port → service (22 → SSH). They're small, so Spark **broadcasts** them to every executor instead of shuffling millions of rows.
  - **`src/common/lake.py`:** Delta helpers used by every stage:
    - `ensure_table()` creates a table if it's missing;
    - `stream_source()` reads a Delta table as a stream, with batch-size limits;
    - `idempotent_append()` writes each batch at most once (`txnAppId` + batch id), so retries never duplicate;
    - `query_app_id()` makes that ID unique per checkpoint;
    - `merge_upsert()` runs a MERGE with partition-pruning and file-skipping conditions.

### Q3a / Q3b: `src/streaming/gold.py`
- **What:** two windowed queries on the silver stream, each with a **10-minute watermark** for late data:
  - **`q3a_gold_timeline`:** flows per **1-minute window** per attack family, into `gold_attack_timeline`.
  - **`q3b_gold_hosts`:** for each source IP and minute, it computes flows, bytes, distinct destinations and **distinct ports** (`approx_count_distinct`, i.e. HyperLogLog), plus SYN/RST ratios. Results go to `gold_host_profile`. Any host with more than 50 ports in a minute goes to `gold_port_scan`.

  Both **MERGE** (upsert) into Delta, because windows get updated as more data arrives. The busiest hosts and port scans are copied to SQLite for the dashboard.
- **Why:** analysts want answers ("who is scanning right now?"), not raw rows. The port-scan rule works **without ML**, by counting.
- **Uses:** `src/serving/sqlite_sink.py` (below).

### Q4: `src/streaming/scoring.py`
- **What:** reads silver rows with `split = 'live'`. In each micro-batch:
  1. checks the registry for a newer model (**hot reload**). If there's no model yet, it skips the batch.
  2. scores every flow with the RandomForest and KMeans.
  3. writes all scored flows to Delta `gold_scored` and Kafka `flows.scored`.
  4. writes alerts to Kafka `alerts` and SQLite `alerts`.
- **Why:** this is the actual real-time detection. Hot reload means a newly trained model goes live without restarting the stream.
- **Uses:**
  - **`src/ml/registry.py`:** the file-based model registry.
    - `register()` saves `vN/` and atomically updates `latest.json` (writes a temp file, then renames it).
    - `HotLoader.get()` re-reads `latest.json` each batch and loads a new version only when it changes.
  - **`src/ml/model_utils.py`:** the ML pieces shared by training and scoring:
    - `feature_pipeline()`: Imputer → VectorAssembler → StandardScaler;
    - `add_attack_prob()`;
    - `add_anomaly_score()`, a vectorised pandas UDF that computes the distance to the nearest KMeans centre;
    - `score()`, which applies the alert rule: (RF says attack with P ≥ 0.6) **or** (anomaly score > threshold).

  Sharing this code guarantees that scoring transforms data **exactly** as training did.

### `src/serving/sqlite_sink.py` + `src/serving/schema.sql`
- **What:**
  - `schema.sql` defines the 5 dashboard tables: `alerts`, `host_window_stats`, `port_scan_candidates`, `stream_metrics` and `pipeline_events`.
  - `sqlite_sink.py` writes into them with `INSERT OR REPLACE` on each table's key.
- **Why:**
  - The dashboard refreshes every few seconds, and reading a small SQLite file is instant, while asking Spark each time would be slow. This is the **serving layer**.
  - Because writes are "insert or replace", a retried batch overwrites its own rows instead of duplicating them.

---

## Stage 4: Feed the history (`make backfill`)

### `src/ingestion/replay_producer.py`
- **What:** reads the 8 CICIDS2017 CSVs (with pyarrow, values kept as raw text) in time order and sends each row to Kafka `flows.raw` as JSON. Along the way it:
  - normalises the column names and skips blank lines;
  - routes each row to **backfill (80%)** or **live (20%)** using a hash of its content, so the split is the same every time;
  - keeps all attacks but only 20% of benign rows; `--sample-pct` takes a smaller demo sample.

  Producer settings:
  - **key = source IP**, so one host's flows stay in order on one partition;
  - idempotent producer, `acks=all`, lz4 compression, batching.

  For `make live`, `--speedup` paces the replay, `--rebase` shifts the times to "now", and each run gets a `run_id`.
- **Why:** there's no real network to watch, so this program **pretends to be one**. It's the only way data enters (Kappa).

### `src/ingestion/wait_caught_up.py` (`make wait`)
- **What:** waits until Kafka lag is 0 (bronze's offsets match the topic's end) and silver has been quiet for a while. It reads `stream_metrics`.
- **Why:** training must start only after **all** backfill data has reached silver. Otherwise the model would learn from half the data.

---

## Stage 5: Train the model (`make train`, stream stopped)

### `src/ml/train_pipeline.py`
- **What:**
  1. reads silver's backfill rows, **pinned to one Delta version** (time travel);
  2. makes a stratified 80/20 train/test split per family (647,533 / 162,029 rows);
  3. adds class weights for the rare families;
  4. fits the feature pipeline, a **RandomForest** (50 trees, depth 10) and a **KMeans** (k = 8) on benign traffic only;
  5. sets the anomaly threshold at the 99.5th percentile (`approxQuantile`);
  6. evaluates on the test split;
  7. **registers v1** and saves the test predictions to `gold_model_predictions`.
- **Why:** this is distributed ML on the full lake the stream built, with no pandas and no sampling. Pinning the version means the model can always be traced to the exact data it learned from.

### `src/ml/evaluate.py` (`make evaluate`, `make evaluate-live`)
- **What:** computes macro-F1, per-class precision/recall, ROC-AUC and a confusion matrix, all with Spark `groupBy` and `BinaryClassificationEvaluator`. It runs on the held-out test predictions, or with `--live` on the flows the stream really scored. Results are saved as `metrics.json` / `metrics_live.json` plus PNG plots in the registry.
- **Why:** this proves the model works on data it never saw. The dashboard's "Model & hosts" tab and the report read these files.

---

## Stage 6: Live traffic (`make live`)

There are no new files here. `replay_producer.py` runs again with `--split live --speedup 300 --rebase`. The always-on stream picks the flows up and they go through Q1 → Q2 → Q3 and **Q4 scoring**, using model v1 loaded by hot reload. Alerts appear on the dashboard a few seconds later.

---

## Stage 7: Show it (`make dashboard`)

### `src/dashboard/app.py`
- **What:** a Streamlit web app (http://localhost:8501) running on the Mac, with 3 tabs:
  - **📡 Pipeline:** whether the stream is up (via the Spark master REST API), rows/s per query and Kafka lag per partition.
  - **🚨 Alerts:** the 4 headline numbers (flows scored, alerts, precision, recall), alerts per minute and the latest alerts with ✅/❌.
  - **🧠 Model & hosts:** model metrics (held-out or live), port-scan suspects and top attacking IPs.

  It reads SQLite, the `gold_scored` Delta table directly with **delta-rs** (no Spark needed), the registry's JSON files, and Kafka for lag.
- **Why:** this is the screen a security analyst would use, and the centrepiece of the demo.

---

## Stage 8: Housekeeping and demo tools

### `src/lakehouse/maintenance.py` (`make maintenance`, stream stopped)
- **What:** runs Delta Lake housekeeping:
  - a table inventory;
  - **OPTIMIZE** (merges the many small files that 5-second batches create);
  - **Z-ORDER BY src_ip** (stores a host's rows together, so lookups skip files);
  - **DESCRIBE HISTORY** (the transaction log);
  - a **time-travel** query on an old silver version;
  - a **schema-evolution** demo (`mergeSchema`);
  - **VACUUM** (deletes unreferenced files).
- **Why:** a streaming lakehouse needs this upkeep, and it's how lakehouse features are shown in the viva. It doesn't ingest data, so it doesn't break Kappa.

### `src/serving/demo_reset.py` (`make demo-reset`)
- **What:** for a fresh demo, it deletes the **live** rows from the 4 gold tables (with delta-rs), empties the 5 SQLite tables and removes the bronze checkpoint. The Makefile then recreates the Kafka topics empty. It keeps the model and all backfill data. `--dry-run` only shows what it would clear.
- **Why:** the dashboard starts from zero for each demo, without rebuilding the lake or retraining (which takes hours).

---

## Stage 9: Tests (`make test`)

| File | What it checks |
|---|---|
| `tests/conftest.py` | Sets up each test run: a throwaway project folder (temp lake, checkpoints, registry, SQLite), a local Spark, and Kafka writes switched off |
| `tests/synthetic.py` | Builds small fake CICIDS-shaped flows (each family with its own pattern, both timestamp formats) so tests don't need the 1.2 GB dataset |
| `tests/test_ingestion.py` | Producer logic: column normalisation, blank lines, a deterministic split, benign sampling, bad-record generation, timestamp parsing |
| `tests/test_pipeline.py` | End to end: runs the **real** streaming queries (bronze → silver → gold → train → scoring) on synthetic data, then checks the data-quality quarantine, idempotent replay, port-scan detection, model registration, hot reload, evaluation and maintenance |
| `tests/test_lake.py` | That the idempotent-write ID changes when a checkpoint is reset. This is the bug that once made Delta silently skip batches |

**Why:** proves that every stage works, in about 3 minutes, without Docker or Kafka (18 tests).

---

## Stage 10: The report (`make report`)

| File | What it does |
|---|---|
| `generate_report_da3_pdf.py` | Turns `REPORT.md` into `Digital_Assignment_3_Report.pdf` in the VIT format: cover page with the logo from `assets/vit_logo.png`, page borders, Times, tables and figures |
| `report_figures.py` | Redraws the 8 report charts from the **current** Delta tables, SQLite and model metrics, so the figures always match the data |

---

## Where the data lives (all generated, under `data/`)

| Folder | Written by | Read by |
|---|---|---|
| `data/raw/cicids2017/` | you (downloaded CSVs) | `replay_producer.py` |
| `data/lake/bronze, silver, gold/` | Q1–Q4, `train_pipeline.py` | the next stage, training, the dashboard, the report |
| `data/checkpoints/<query>/` | each streaming query | the same query after a restart |
| `data/models/registry/vN/` | `train_pipeline.py`, `evaluate.py` | `scoring.py`, the dashboard, the report |
| `data/serving/alerts.db` | `sqlite_sink.py`, `metrics_listener.py` | the dashboard, `wait_caught_up.py` |
