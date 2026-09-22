# Flow-Level Network Anomaly Detection Platform

> **Big Data Analytics — University Project**  
> Local build: MacBook Air M2, 8GB unified RAM, Apple Silicon (ARM64)

---

## Architecture

```
cicds.csv
    │
    ▼
┌──────────────────────────────────────────────────────────┐
│  Phase 1: PySpark — Data Cleaning                        │
│  (cast types, drop nulls/Inf, drop zero-variance cols)   │
└───────────────────────┬──────────────────────────────────┘
                        │ Parquet
                        ▼
┌──────────────────────────────────────────────────────────┐
│  Phase 2: Kafka (KRaft) + Spark Structured Streaming     │
│  Producer: Parquet rows → JSON → Kafka topic             │
│  Consumer: Kafka → parse JSON → Parquet sink             │
└───────────────────────┬──────────────────────────────────┘
                        │
                        ▼
┌──────────────────────────────────────────────────────────┐
│  Phase 3: PySpark + scikit-learn                         │
│  Per-flow preprocessing: Inf-cleanup → StandardScaler    │
│  (no windowing, no groupBy IP — dataset has no IP/time)  │
└───────────────────────┬──────────────────────────────────┘
                        │ NumPy arrays
                        ▼
┌──────────────────────────────────────────────────────────┐
│  Phase 4: scikit-learn ML                                │
│  IsolationForest  — unsupervised per-flow anomaly score  │
│  RandomForest     — supervised attack-type classification│
└───────────────────────┬──────────────────────────────────┘
                        │
                        ▼
┌──────────────────────────────────────────────────────────┐
│  Phase 5: SQLite                                         │
│  flow_scores table + alerts table                        │
└───────────────────────┬──────────────────────────────────┘
                        │
                        ▼
┌──────────────────────────────────────────────────────────┐
│  Phase 6: Streamlit Dashboard                            │
│  Live alert feed · score histogram · attack type chart   │
└──────────────────────────────────────────────────────────┘
```

---

## Quick Start

### Prerequisites
- Python 3.11+ (tested on 3.13)
- Docker Desktop for Mac (Apple Silicon build) — only needed for Kafka (Phase 2)

### 1. Set up environment

```bash
# From the project root
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 2. Place the dataset

Copy `cicds.csv` into `data/raw/`:
```bash
cp /path/to/cicds.csv data/raw/cicds.csv
```

### 3. Run the pipeline phase by phase

```bash
# Phase 1 — Data cleaning (~3–10 min depending on Spark startup + dataset size)
python src/processing/clean_data.py

# Phase 3 — Per-flow feature preprocessing
python src/processing/preprocess_flows.py

# Phase 4 — ML training + evaluation
python src/ml/train_isolation_forest.py
python src/ml/train_random_forest.py
python src/ml/evaluate.py

# Phase 5 — Write alerts to SQLite
python src/storage/write_alerts.py

# Phase 6 — Dashboard
streamlit run src/dashboard/app.py
```

### 4. (Optional) Simulated streaming — Phase 2

Requires Docker Desktop running:
```bash
bash scripts/start_kafka.sh
bash scripts/create_topic.sh

# Terminal A — producer
python src/ingestion/kafka_producer.py --sample 5000

# Terminal B — consumer (Spark Structured Streaming)
python src/ingestion/spark_consumer.py
```

### 5. Verify all phases

```bash
python tests/test_pipeline.py
```

---

## Makefile shortcuts

```bash
make setup         # create venv + install deps
make test-spark    # confirm PySpark works
make kafka-start   # start Kafka via Docker
make kafka-topic   # create network-flows topic
make clean-data    # Phase 1
make preprocess    # Phase 3
make train-if      # Phase 4a — Isolation Forest
make train-rf      # Phase 4b — Random Forest
make evaluate      # Phase 4c — metrics + plots
make write-alerts  # Phase 5
make dashboard     # Phase 6
```

---

## Dataset

**cicds.csv** — Kaggle combined CICIDS2017 export.  
- ~2.8 million rows (network flows)  
- 52 flow-level statistical features (packet sizes, byte rates, inter-arrival times, TCP flags, etc.)  
- `Attack Type` column with ~15 classes (BENIGN + various attack types)  
- **No Source IP, Destination IP, or Timestamp columns** — see Scope Decisions below.

---

## Scope Decisions

This build is a scoped-down, runnable version of a larger team proposal. Every simplification is deliberate and justified — not a shortcut.

### 1. Per-flow anomaly detection instead of per-entity behavioral profiling

**Original plan:** Compute sliding-window behavioral features per source IP (connection rate, unique destinations contacted, etc.), producing one "entity profile" row per (source IP, time window). Isolation Forest then scores entity behaviour over time.

**This build:** Each network flow row is scored independently by Isolation Forest. No windowing, no groupBy source IP.

**Why:** The `cicds.csv` dataset is the Kaggle-cleaned combined CICIDS2017 export, which strips Source IP, Destination IP, and Timestamp — the three columns required for entity-level temporal profiling. Without those columns, per-host behavioral profiling is simply not possible. Per-flow scoring is a valid, industry-used IDS approach; many commercial SIEMs flag individual connection records rather than (or in addition to) entity behavioural baselines.

### 2. Single KRaft-mode Kafka broker (no Zookeeper, no multi-broker cluster)

KRaft mode eliminates Zookeeper entirely, saving ~300–500MB RAM. A single broker demonstrates all Kafka concepts (topics, partitions, consumer groups, producer acknowledgements) without the cluster overhead that a 3-broker setup requires.

### 3. SQLite instead of MongoDB / Cassandra

Both MongoDB and Cassandra require an additional running server process (~300–800MB RAM each). SQLite is a file-based database with zero server overhead. For a local demo with tens of thousands of alert rows, SQLite is more than sufficient and is accessed with the same SQL semantics as any production database.

### 4. scikit-learn instead of Spark MLlib

Spark MLlib has no native Isolation Forest implementation (a third-party JAR would be needed). More importantly: after Phase 3 preprocessing, the data passed to the ML step is already a moderately-sized NumPy matrix (at most 500K rows × 52 features), not a distributed dataset. Using Spark's distributed ML framework on data this small would add overhead with no benefit. The realistic industry pattern is exactly this: use Spark for distributed parsing and preprocessing of raw data at scale, then pull the compressed/aggregated output into a lightweight ML framework for modelling.

### 5. Sampling to 500K rows for the Pandas/scikit-learn step

The full ~2.8M row dataset is processed in Spark (phases 1–3). The Pandas/scikit-learn step samples up to 500K rows to stay within 8GB RAM. The sample is stratified-equivalent via Spark's `df.sample()` — attack type distribution is preserved. This is documented explicitly in `preprocess_flows.py` and is disabled if the machine has sufficient RAM.

---

## Output artifacts

| Path | Contents |
|---|---|
| `data/clean_parquet/` | Cleaned, typed Parquet (Phase 1 output) |
| `data/preprocessed_parquet/` | Inf-cleaned Parquet (Phase 3 Spark output) |
| `data/X_scaled.npy` | StandardScaler-normalized feature matrix |
| `data/y_labels.npy` | Ground-truth label array (aligned with X_scaled) |
| `models/scaler.joblib` | Fitted StandardScaler |
| `models/isolation_forest.joblib` | Trained IsolationForest |
| `models/random_forest.joblib` | Trained RandomForestClassifier |
| `models/label_encoder.joblib` | LabelEncoder (string ↔ integer labels) |
| `models/plots/` | Confusion matrices + score distribution plots |
| `alerts.db` | SQLite database: flow_scores + alerts tables |

