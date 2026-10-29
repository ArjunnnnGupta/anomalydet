# Network Anomaly Platform (simple version)

> This folder is the **simplified** version of `../anomaly-detection-platform`: one cluster size,
> fewer commands and a 3-tab dashboard. The pipeline code is the same. `data/` and `venv/` are
> symlinks to the original folder, so both share the same dataset, lake and trained model.
> Both folders also use the same Docker containers, so run only one at a time: `make cluster-down`
> in one folder before `make cluster-up` in the other.

> **Big Data Analytics (BCSE402L), VIT Chennai.** A streaming-first big data pipeline over the
> CICIDS2017 intrusion-detection dataset (2.83M labelled network flows; the demo replays
> ~1.0M of them: every attack flow plus 20% of the benign ones).

Every record enters through **Apache Kafka**. One always-on **Spark Structured Streaming**
application builds a **Delta Lake** medallion lakehouse (bronze → silver → gold) continuously.
**Spark MLlib** trains distributed models on the data the stream wrote, and those models score
live traffic in real time. The scorer checks the model registry on every micro-batch and swaps in
a newly registered version without a code change or redeploy.

```
 CICIDS2017 CSVs ──► replay producer ──► Kafka (KRaft · 1 broker · 12 partitions)
                     (idempotent, lz4,          │
                      keyed by Source IP)       ▼
            ┌──────────── Spark Structured Streaming app (5 queries) ────────────┐
            │ Q1 bronze ─► Q2 silver ─┬─► Q3a gold: 1-min attack timeline          │
            │ (raw+offsets) (parse·DQ· ├─► Q3b gold: 1-min per-host profiles        │
            │               enrich·    │          + port-scan candidates            │
            │               MERGE)     └─► Q4 scoring (MLlib RF + KMeans, hot reload)│
            └───────────────┬──────────────────────────┬──────────────────────────┘
                            ▼                          ▼
        Delta Lake (ACID · time travel · Z-order)   Kafka flows.scored / alerts / flows.dlq
                            │                          │
         MLlib training ◄───┘                          ▼
         (RandomForest + KMeans, model registry) ─► SQLite serving cache ─► Streamlit dashboard
```

The simplest starting point is **[WORKFLOW.md](WORKFLOW.md)**: the step-by-step flow and a glossary of terms.
The program traced file by file, in the order it runs, is in **[FILE_FLOW.md](FILE_FLOW.md)**, and the bronze/silver/gold code is explained line by line in **[MEDALLION_CODE.md](MEDALLION_CODE.md)**.
A plain-language walkthrough of every component (what, where, why, which big data concept) is in
**[PROJECT_GUIDE.md](PROJECT_GUIDE.md)**. The full design, with a map from each big data concept to the code that uses it, is in **[ARCHITECTURE.md](ARCHITECTURE.md)**.
Build progress is logged in [CHECKPOINT.md](CHECKPOINT.md).

---

## What runs where

| Component | Runs in | UI |
|---|---|---|
| Kafka in KRaft mode, no ZooKeeper (1 broker) | Docker | — |
| Kafka UI (topics, partitions, ISR, consumer lag) | Docker | http://localhost:8080 |
| Spark standalone: 1 master + 1 worker | Docker | http://localhost:8081 |
| Streaming app (driver, client mode) | `spark-master` container | http://localhost:4040 |
| Training / evaluation / maintenance jobs (run with the stream stopped) | `spark-master` container | http://localhost:4040 |
| Replay producer, topic admin, dashboard, tests | host (`./venv`) | http://localhost:8501 |

The project folder is mounted at `/opt/app` in every Spark container. It is the shared
filesystem (the role HDFS or S3 plays on a real cluster) for the Delta tables, checkpoints and model registry.

---

## Quick start

### 1. Prerequisites
- **Docker Desktop** for Apple Silicon, with Settings → Resources set to **~6 GB memory and 6 CPUs**.
- **Python 3.12+** and **Java 17**. Java 17 is only needed for the host test suite (`brew install openjdk@17`).
- About **6 GB of free disk** for Kafka logs, the Delta lake and images.

### 2. Dataset
Download **`GeneratedLabelledFlows.zip`** (the *TrafficLabelling* CSVs that include
Flow ID, Source/Destination IP, ports and Timestamp) from the
[UNB CICIDS2017 page](https://www.unb.ca/cic/datasets/ids-2017.html) and unzip the 8 CSVs into:

```
data/raw/cicids2017/
  Monday-WorkingHours.pcap_ISCX.csv
  Tuesday-WorkingHours.pcap_ISCX.csv
  ... (8 files)
```

The old `cicds.csv` export is not used. It has no timestamps or IPs, and without them event-time
windows and per-host analytics aren't possible.

### 3. Build and start the cluster
```bash
make setup          # host venv: producer, dashboard, tests
make build          # Spark 3.5.3 image with Kafka + Delta jars baked in
make cluster-up     # 1 Kafka broker + Kafka UI + Spark master + 1 worker
make topics         # create topics, print leader/replicas/ISR per partition
```
The cluster is deliberately small (1 Kafka broker, 1 Spark worker) so it fits on an 8 GB Mac.

### 4. Run the pipeline (Kappa: two passes through Kafka)
```bash
# Terminal 1: the always-on streaming app (all 5 queries)
make stream

# Terminal 2
make backfill       # replay ~80% of flows at full speed → stream builds bronze/silver/gold
make wait           # block until Kafka lag = 0 and silver is idle
# → now stop the stream in Terminal 1 (Ctrl+C): batch jobs need its memory
make train          # MLlib on silver → registers model v1
# → start the stream again in Terminal 1 (make stream); it loads v1 on its first batch
make live           # replay a 15% sample of the held-out flows (~30K) at 300×, re-based to "now"
                    # (LIVE_PCT=100 for all ~203K; each run gets its own run_id, so it can be repeated)

# Terminal 3
make dashboard      # http://localhost:8501
```
`train`, `evaluate` and `maintenance` refuse to run while the stream is running: in 6 GB of
Docker memory there is no room for two Spark drivers. Stop the stream (Ctrl+C), run the job, restart the stream.

### 5. More to try
```bash
make train                 # (stream stopped) register v2 → the next `make stream` serves v2
make evaluate              # metrics + plots on the held-out test split
make evaluate-live         # metrics on what the live stream actually scored
make maintenance           # OPTIMIZE / ZORDER, DESCRIBE HISTORY, time travel, schema evolution, VACUUM
```
Stop the streaming app with Ctrl+C and start it again: every query resumes from its checkpoint,
and the idempotent sinks keep the tables free of duplicates.

---

## Make targets

| Target | What it does |
|---|---|
| `setup` / `test` | create the host venv / run the pytest suite (local Spark, no Docker needed) |
| `build`, `cluster-up`, `cluster-down`, `cluster-ps`, `logs` | cluster lifecycle |
| `topics` | create topics from `conf/pipeline.yaml` and show the partition layout |
| `stream` | the always-on streaming app (all 5 queries) |
| `backfill`, `live` (`LIVE_PCT=…`, `SPEEDUP=…`) | Kafka replay producer |
| `wait` | block until the stream has caught up |
| `train`, `evaluate`, `evaluate-live`, `maintenance` | Spark batch jobs on the lake (stream stopped) |
| `dashboard` | Streamlit dashboard, 3 tabs: Pipeline · Alerts · Model & hosts |
| `demo-reset` | fresh demo: clear previous live results and empty Kafka (keeps the model and backfill data) |
| `reset` | wipe the lake, checkpoints, registry and SQLite, and recreate the topics (asks first) |

---

## Repository layout

```
conf/pipeline.yaml        single source of truth: topics, partitions, paths, windows, thresholds, ML params
docker/                   docker-compose.yml (the cluster), spark/Dockerfile
src/common/               config, SparkSession builder, dataset schema, dimension tables, Delta helpers
src/ingestion/            create_topics.py, replay_producer.py, wait_caught_up.py
src/streaming/            stream_app.py + bronze / silver / gold / scoring / metrics_listener
src/ml/                   train_pipeline.py, evaluate.py, model_utils.py, registry.py
src/lakehouse/            maintenance.py (Delta ops)
src/serving/              SQLite schema + idempotent sink
src/dashboard/app.py      Streamlit
tests/                    synthetic CICIDS-shaped data + end-to-end pipeline tests
data/                     (generated) raw/, lake/, checkpoints/, models/registry/, serving/, logs/
```

## Tests

```bash
make test     # 18 tests, ~3 min
```
The suite runs the real streaming queries in `availableNow` mode against a temporary lake. It
covers bronze → silver (DQ quarantine, idempotent replay) → gold windows and port-scan detection →
MLlib training → live scoring into Delta and SQLite → model hot reload →
evaluation → Delta maintenance. Kafka sinks are switched off (`KAFKA_SINKS=0`). The Kafka source
and the cluster itself are exercised by the steps in Quick start.

## Troubleshooting

- **The Mac swaps or containers get OOM-killed.** Keep live replays small (`make live LIVE_PCT=5`) and don't run two Spark jobs at once.
- **`make train` says the streaming app is running.** That's deliberate: stop the stream (Ctrl+C), train or evaluate, then `make stream` again. With 6 GB of Docker memory, two Spark drivers at once got the driver container OOM-killed.
- **`make wait` never finishes.** Check the `make stream` terminal for errors. Progress is only recorded while the streaming app is running.
- **Disk fills up.** Kafka retention is capped per partition in `pipeline.yaml`. `make maintenance` VACUUMs Delta. `make reset` wipes everything.
