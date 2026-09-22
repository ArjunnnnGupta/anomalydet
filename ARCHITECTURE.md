# Architecture & Component Reference
## Flow-Level Network Anomaly Detection Platform

> **Audience:** Teammates who need to understand, run, and extend each component of the pipeline.
> **Style:** Technical/engineering — uses domain-specific terminology throughout.

---

## Table of Contents

1. [System Overview](#1-system-overview)
2. [Phase 0 — Environment & Setup](#2-phase-0--environment--setup)
3. [Phase 1 — Distributed Data Ingestion & Cleaning](#3-phase-1--distributed-data-ingestion--cleaning)
4. [Phase 2 — Kafka Streaming Ingestion](#4-phase-2--kafka-streaming-ingestion)
5. [Phase 3 — Feature Preprocessing](#5-phase-3--feature-preprocessing)
6. [Phase 4a — Isolation Forest (Unsupervised Anomaly Detection)](#6-phase-4a--isolation-forest-unsupervised-anomaly-detection)
7. [Phase 4b — Random Forest (Supervised Multi-Class Classification)](#7-phase-4b--random-forest-supervised-multi-class-classification)
8. [Phase 4c — Evaluation](#8-phase-4c--evaluation)
9. [Phase 5 — SQLite Alert Storage](#9-phase-5--sqlite-alert-storage)
10. [Phase 6 — Streamlit Dashboard](#10-phase-6--streamlit-dashboard)
11. [Test Suite](#11-test-suite)
12. [Data Flow Summary Table](#12-data-flow-summary-table)

---

## 1. System Overview

The platform ingests ~2.52 million labelled **CICIDS2017 network flow records**, cleans and transforms them using **Apache Spark**, runs both unsupervised and supervised ML models for anomaly detection, stores actionable alerts in **SQLite**, and serves them via a **real-time Streamlit dashboard**. An independent **Kafka streaming path** simulates a live sensor feed running in parallel to the batch pipeline.

### Master Pipeline Flowchart

```
┌─────────────────────────────────────────────────────────────────────┐
│                        Raw Dataset                                  │
│              cicds.csv  (2,520,751 rows × 53 cols, ~680 MB)         │
└────────────────────────────┬────────────────────────────────────────┘
                             │
                    ┌────────▼────────┐
                    │   Phase 1       │   Apache Spark (local[*])
                    │   Data Cleaning │   schema enforcement,
                    │                 │   null/inf removal,
                    └────────┬────────┘   label normalisation
                             │
                    Parquet (data/clean_parquet/)
                    2,520,751 rows, 53 cols
                             │
               ┌─────────────┴──────────────┐
               │                            │
      ┌────────▼────────┐          ┌────────▼────────┐
      │   Phase 2       │          │   Phase 3       │
      │   Kafka         │          │   Feature       │
      │   Streaming     │          │   Preprocessing │
      │   (parallel)    │          │                 │
      └────────┬────────┘          └────────┬────────┘
               │                            │
        JSON messages                 X_scaled.npy  (500,407 × 52)
        → network-flows topic         y_labels.npy
        → Parquet sink                scaler.joblib
                             (merged below for ML)
                                            │
                             ┌──────────────┤
                             │              │
                    ┌────────▼──┐    ┌──────▼──────┐
                    │  Phase 4a │    │  Phase 4b   │
                    │ Isolation │    │   Random    │
                    │  Forest   │    │   Forest    │
                    └────────┬──┘    └──────┬──────┘
                             │              │
                       if_flags.npy   rf_predictions.npy
                       if_scores.npy  (99.70% accuracy)
                             │              │
                             └──────┬───────┘
                                    │
                           ┌────────▼────────┐
                           │   Phase 4c      │
                           │   Evaluation    │
                           │   & Plots       │
                           └────────┬────────┘
                                    │
                           ┌────────▼────────┐
                           │   Phase 5       │
                           │   SQLite        │
                           │   alerts.db     │
                           └────────┬────────┘
                                    │
                           ┌────────▼────────┐
                           │   Phase 6       │
                           │   Streamlit     │
                           │   Dashboard     │
                           │   :8501         │
                           └─────────────────┘
```

---

## 2. Phase 0 — Environment & Setup

### What it does
Bootstraps the Python virtual environment and verifies that **Java 17 LTS** is correctly resolved before any Spark or Kafka component is invoked. Java 17 is a hard requirement — PySpark 3.5.1 uses `Subject.getSubject` internally, which was removed from the JDK in Java 21+. Using any newer JDK causes an `UnsupportedOperationException` at Spark initialisation.

### Key design decisions
- **Isolated venv** (`venv/`): prevents dependency conflicts with system Python.
- **`openjdk@17` (keg-only Homebrew)**: not linked into system PATH, so every Spark and Kafka script explicitly sets `JAVA_HOME` to the full Cellar path. This is more robust than relying on `which java`, which returns `/usr/bin/java` — a macOS stub that errors when no JDK is installed.
- **`kafka-python-ng`** is used instead of the original `kafka-python` — the latter has no Python 3.13 wheels.
- **`setuptools`** is installed explicitly to restore the `distutils` shim removed in Python 3.13 (required by PySpark's `toPandas()` path).

### How to run
```bash
make setup          # installs venv + all packages
make test-spark     # verifies PySpark starts cleanly with openjdk@17
```

---

## 3. Phase 1 — Distributed Data Ingestion & Cleaning

### Flowchart

```
cicds.csv (raw, ~680 MB)
        │
        ▼
[Spark CSV reader]
  ├─ inferSchema=true  (Spark auto-detects column dtypes)
  └─ spark.sql.shuffle.partitions=8  (local mode optimisation; default 200 is wasteful)
        │
        ▼
[Schema Enforcement]
  ├─ All columns cast to DoubleType except "Attack Type" (StringType)
  ├─ Column names stripped of leading/trailing whitespace
  └─ "Attack Type" values normalised (strip whitespace)
        │
        ▼
[Null / Infinity Drop]
  ├─ Rows with any null replaced: numeric cols → 0.0
  └─ Rows with ±Infinity in any numeric column → dropped
        │
        ▼
Parquet write (snappy compression, 8 partitions)
→  data/clean_parquet/
   2,520,751 rows × 53 columns
   0 dropped rows (dataset was pre-cleaned by Kaggle source)
```

### What it does
The raw CSV is read into a **Spark DataFrame** using the CSV reader with schema inference. Every numeric column is explicitly re-cast to `DoubleType` to guarantee downstream numpy compatibility when the data is later converted. The only non-numeric column retained is `Attack Type`, which carries the ground-truth label for evaluation.

Null and infinity handling is applied defensively: nulls are filled with `0.0` (network counters that are absent default to zero), and rows with infinite values (e.g., division-by-zero in flow-duration-derived features) are dropped. The output is written as **Parquet** — a columnar format with snappy compression that reduces read time for subsequent phases from ~20s (CSV) to ~2s.

### Key I/O
| | Detail |
|---|---|
| **Input** | `data/raw/cicds.csv` (symlink → `../cicds.csv`) |
| **Output** | `data/clean_parquet/` (8 part-files, snappy) |
| **Row count** | 2,520,751 |
| **Columns** | 53 (52 numeric features + `Attack Type`) |

### How to run
```bash
make clean-data
```

---

## 4. Phase 2 — Kafka Streaming Ingestion

### Flowchart

```
data/clean_parquet/
        │
        ▼
[Kafka Producer]
  ├─ Loads Parquet with pandas (no Spark needed for producer)
  ├─ Optional --sample N flag (default: full dataset)
  ├─ Serialises each row to JSON (keys = column names)
  ├─ Sends in batches of 50 rows
  └─ Sleeps 0.5s between batches (simulates live sensor rate ~100 rows/s)
        │
        ▼ JSON messages
[Kafka Broker — KRaft mode]
  Topic: network-flows
  Partitions: 1
  Broker: localhost:9092
  No ZooKeeper — uses KRaft (Kafka 4.x built-in Raft consensus)
        │
        ▼
[Spark Structured Streaming Consumer]
  ├─ Reads from Kafka using readStream()
  ├─ Parses JSON payload → DataFrame with inferred schema
  ├─ Writes micro-batches to Parquet sink (data/streaming_parquet/)
  └─ Checkpoint dir: data/checkpoint/ (enables exactly-once semantics)
        │
        ▼
data/streaming_parquet/
  (partitioned Parquet, written per micro-batch)
```

### What it does
The streaming path decouples **data production** from **data consumption**, which mirrors a real-world network monitoring architecture (e.g., a packet capture agent → Kafka → a processing cluster).

The **producer** reads rows from the already-cleaned Parquet store and serialises each flow record to a JSON string. It publishes to the `network-flows` Kafka topic in batches, sleeping between batches to control throughput. This makes the producer replay-safe — running it again simply appends to the topic (Kafka offsets are monotonically increasing).

The **consumer** uses Spark Structured Streaming's native Kafka connector. It reads from offset `earliest`, parses each JSON message into a typed DataFrame, and writes micro-batches to a Parquet sink using `outputMode("append")`. A checkpoint directory tracks which offsets have already been committed, ensuring **exactly-once processing** on restart.

**Kafka is deployed in KRaft mode** (Kafka 4.x). This eliminates the ZooKeeper dependency, reducing operational complexity from a two-process cluster to a single broker JVM process.

### Key I/O
| | Detail |
|---|---|
| **Input** | `data/clean_parquet/` (pandas read, optional sample) |
| **Kafka topic** | `network-flows`, 1 partition, localhost:9092 |
| **Output (consumer)** | `data/streaming_parquet/` + `data/checkpoint/` |
| **Throughput demo** | 500 rows published in 5.4s, ~101 rows/s confirmed |

### How to run
```bash
# Terminal 1 — start broker (already done; skip if broker is running)
make kafka-format && make kafka-start

# Terminal 2 — create topic (only needed once)
make kafka-topic

# Terminal 3 — start consumer first, then producer
make consumer     # starts Spark Structured Streaming listener
make producer     # streams 500 rows (--sample 500 by default)
```

---

## 5. Phase 3 — Feature Preprocessing

### Flowchart

```
data/clean_parquet/ (2,520,751 rows)
        │
        ▼
[Pandas read_parquet]
  └─ Loads full cleaned dataset
        │
        ▼
[Stratified Sampling]
  ├─ Target: fit within 8 GB RAM budget on M2 MacBook Air
  ├─ Fraction: 0.198 (empirically tuned)
  └─ Result: 500,407 rows
        │
        ├──────────────────────────────┐
        ▼                              ▼
[Feature Matrix X]              [Label Vector y]
  52 numeric columns              "Attack Type" column
  (all columns except             (object/string, preserved
   "Attack Type")                  as-is for stratification)
        │
        ▼
[StandardScaler fit_transform]
  ├─ Computes μ and σ per feature across 500,407 rows
  ├─ Transforms each feature to zero mean, unit variance
  └─ Ensures heterogeneous features (e.g., flow duration
     in microseconds vs. binary TCP flags) are on the
     same scale before distance-based and tree-based models
        │
        ▼
data/X_scaled.npy    (500,407 × 52, float64)
data/y_labels.npy    (500,407,  object — attack type strings)
data/feature_cols.json  (ordered list of 52 column names)
models/scaler.joblib    (fitted StandardScaler, for inference)
```

### What it does
PySpark is deliberately **not used** in this phase — the full 2.52M-row dataset would require ~4 GB of RAM for a pandas DataFrame, and after stratified sampling to 500,407 rows the entire X matrix (~200 MB as float64) fits comfortably within the 8 GB unified memory budget. Using Spark here would add JVM startup overhead with no parallelism benefit.

**Stratified sampling** is key: the label distribution in the sample preserves the same class proportions as the full dataset (e.g., ~83% Normal, ~8% DoS). This matters for training a meaningful class-weighted Random Forest in Phase 4b.

**StandardScaler** is fitted on the training sample and persisted to `scaler.joblib`. Any new data arriving via Kafka must be transformed with the **same scaler instance** before being scored — applying a fresh fit to new data would shift the score distribution and invalidate comparisons.

### Key I/O
| | Detail |
|---|---|
| **Input** | `data/clean_parquet/` |
| **Output** | `data/X_scaled.npy`, `data/y_labels.npy`, `models/scaler.joblib`, `data/feature_cols.json` |
| **Sample size** | 500,407 rows (fraction ≈ 0.198 of 2.52M) |
| **Feature count** | 52 (all numeric columns) |

### How to run
```bash
make preprocess
```

---

## 6. Phase 4a — Isolation Forest (Unsupervised Anomaly Detection)

### Flowchart

```
data/X_scaled.npy  (500,407 × 52)
        │
        ▼
[IsolationForest — scikit-learn]
  ├─ n_estimators=100   (100 isolation trees in the ensemble)
  ├─ contamination=0.17 (expects ~17% anomalous flows; tuned
  │                      to match dataset's 17% attack rate)
  ├─ n_jobs=-1           (uses all CPU cores in parallel)
  └─ random_state=42
        │
        ├────────────────────────────┐
        ▼                            ▼
[score_samples()]            [predict()]
  Returns anomaly score        Returns flag:
  (more negative = more          -1 = anomaly
   anomalous)                    +1 = normal
        │                            │
        ▼                            ▼
data/if_scores.npy          data/if_flags.npy
(500,407 floats)             (500,407 ints: -1 or +1)
models/isolation_forest.joblib
```

### What it does
Isolation Forest is an **unsupervised** ensemble method. It builds 100 randomised binary decision trees by repeatedly splitting a random subset of samples on a randomly chosen feature at a random threshold. Points that are isolated (reach a leaf node) in fewer splits are anomalous — they occupy sparse, outlier regions of the feature space and are easier to isolate than dense, normal-traffic clusters.

The `contamination` hyperparameter tells the model what fraction of the training data to treat as anomalous when setting the decision threshold. It is set to **0.17** to match the dataset's known 17% attack rate (DoS + DDoS + PortScan + BruteForce + WebAttacks + Bots combined).

The model operates **without any ground-truth labels** — it sees only the 52 scaled feature values. This makes it suitable for detecting **zero-day or novel attack patterns** that a supervised classifier would not recognise.

The anomaly score (`score_samples()`) is a continuous signal between roughly -0.5 (highly anomalous) and 0 (normal). This score is persisted to `if_scores.npy` and stored in `alerts.db` to enable threshold tuning in the dashboard without rerunning the model.

### Key I/O
| | Detail |
|---|---|
| **Input** | `data/X_scaled.npy` |
| **Output** | `data/if_scores.npy`, `data/if_flags.npy`, `models/isolation_forest.joblib` |
| **Anomalies flagged** | 85,070 (17.00% of 500,407) |
| **Binary accuracy vs. ground truth** | 83% |
| **Attack recall** | 50% (expected for unsupervised — no label knowledge) |

### How to run
```bash
make train-if
```

---

## 7. Phase 4b — Random Forest (Supervised Multi-Class Classification)

### Flowchart

```
data/X_scaled.npy  (500,407 × 52)
data/y_labels.npy  (500,407 attack-type strings)
        │
        ▼
[LabelEncoder]
  Maps string labels → integers:
  Bots=0, Brute Force=1, DDoS=2, DoS=3,
  Normal Traffic=4, Port Scanning=5, Web Attacks=6
        │
        ▼
[Stratified 80/20 train/test split]
  Train : 400,325 rows (stratified by class)
  Test  : 100,082 rows (stratified by class)
        │
        ▼
[RandomForestClassifier — scikit-learn]
  ├─ n_estimators=100     (100 decision trees)
  ├─ max_depth=20         (caps tree depth; prevents overfitting)
  ├─ class_weight="balanced" (up-weights minority classes like Bots,
  │                            Web Attacks during impurity calculation)
  ├─ n_jobs=-1            (parallelised tree building)
  └─ random_state=42
        │
        ▼
[Predict on test set (100,082 rows)]
  └─ Per-class F1 ≥ 0.96 for DoS, DDoS, Port Scanning, Normal
     Bots F1 = 0.43 (only 1,558 train samples; class imbalance)
        │
        ▼
data/rf_predictions.npy    (100,082 predicted integer labels)
data/rf_test_y.npy         (100,082 true integer labels)
models/random_forest.joblib
models/label_encoder.joblib
```

### What it does
Random Forest is a **supervised ensemble classifier**. It builds 100 independent decision trees on bootstrap samples of the training data, each split using a random subset of features (≈√52 ≈ 7 features per split). Final class predictions are determined by majority vote across all trees.

`max_depth=20` prevents individual trees from memorising training samples while still allowing deep enough splits to separate subtle attack signatures (e.g., distinguishing Brute Force from Web Attacks by connection duration and packet ratio patterns).

`class_weight="balanced"` automatically adjusts the impurity criterion so minority classes (Bots: 1,558 train samples vs. Normal Traffic: 1,676,045) contribute proportionally to splits. Without this, trees would simply predict "Normal Traffic" for almost everything and still achieve >83% accuracy.

The stratified train/test split guarantees that both partitions contain the same class proportions, preventing the test set from being inadvertently easier or harder than the training distribution.

**Test accuracy: 99.70%** across 100,082 held-out flows.

### Key I/O
| | Detail |
|---|---|
| **Input** | `data/X_scaled.npy`, `data/y_labels.npy` |
| **Output** | `data/rf_predictions.npy`, `data/rf_test_y.npy`, `models/random_forest.joblib`, `models/label_encoder.joblib` |
| **Classes** | 7: Normal Traffic, DoS, DDoS, Port Scanning, Brute Force, Web Attacks, Bots |
| **Test accuracy** | 99.70% |
| **Bots F1** | 0.43 (class imbalance; only 1,558 training samples) |

### How to run
```bash
make train-rf
```

---

## 8. Phase 4c — Evaluation

### Flowchart

```
data/if_flags.npy        data/rf_predictions.npy
data/y_labels.npy        data/rf_test_y.npy
        │                        │
        ▼                        ▼
[Binary evaluation          [Multi-class evaluation
 for IF]                     for RF]
  ├─ True label binarised:    ├─ classification_report()
  │   "Normal Traffic" → 0   │   (per-class precision,
  │   all others      → 1   │    recall, F1)
  ├─ IF flag: -1 → anomaly   └─ Confusion matrix heatmap
  └─ Confusion matrix             → models/plots/rf_confusion_matrix.png
       + score histogram
       → models/plots/if_confusion_matrix.png
       → models/plots/if_score_distribution.png
```

### What it does
Two separate evaluation passes run sequentially:

**Isolation Forest evaluation** converts the multi-class ground-truth labels into a binary "anomalous vs. normal" signal by treating any non-`Normal Traffic` label as positive. IF flags (`-1`/`+1`) are compared against this binary ground truth. The score histogram (`if_score_distribution.png`) shows the distribution of anomaly scores for normal vs. anomalous flows, confirming whether the contamination threshold separates the two populations.

**Random Forest evaluation** uses the held-out 20% test split (`rf_test_y.npy`) directly — no binarisation needed. `classification_report()` reports per-class precision, recall, and F1. The confusion matrix shows where the model confuses attack categories (e.g., Bots often misclassified as DoS due to overlapping flow duration/packet count distributions).

### Key outputs
| File | Description |
|---|---|
| `models/plots/if_confusion_matrix.png` | Heatmap: IF binary TP/FP/TN/FN |
| `models/plots/if_score_distribution.png` | Histogram: anomaly score by ground-truth class |
| `models/plots/rf_confusion_matrix.png` | 7×7 normalised confusion matrix for RF |

### How to run
```bash
make evaluate
```

---

## 9. Phase 5 — SQLite Alert Storage

### Flowchart

```
data/X_scaled.npy      data/if_scores.npy
data/y_labels.npy      data/if_flags.npy
data/rf_predictions.npy
        │
        ▼
[write_alerts.py]
  ├─ Joins all arrays column-wise (by positional index)
  ├─ Decodes RF integer predictions → label strings via LabelEncoder
  └─ Writes two SQLite tables:

        ┌──────────────────────────────────────┐
        │  flow_scores  (500,407 rows)          │
        │  ─────────────────────────────────── │
        │  id, flow_index, anomaly_score,       │
        │  if_flag, rf_label, ground_truth,     │
        │  scored_at                            │
        └──────────────────────────────────────┘

        ┌──────────────────────────────────────┐
        │  alerts  (85,070 rows)               │
        │  ─────────────────────────────────── │
        │  id, flow_index, anomaly_score,       │
        │  rf_label, ground_truth, scored_at   │
        │  (subset: only rows where if_flag=-1) │
        └──────────────────────────────────────┘
```

### What it does
SQLite was chosen over a full RDBMS (PostgreSQL, MySQL) for this phase because the dataset fits within a single ~49 MB database file and the access pattern is read-heavy with no concurrent writers. The dashboard runs `SELECT` queries; no transactions beyond the initial bulk `INSERT` are needed.

**`flow_scores`** stores every scored flow, enabling the dashboard to perform arbitrary threshold analysis (e.g., "show me flows with anomaly score < -0.3"). **`alerts`** is a pre-filtered subset containing only IF-flagged anomalies, enabling fast dashboard load without a full-table scan.

The `scored_at` timestamp column records when each row was written — useful for simulating time-series queries (e.g., alert rate over time) in the dashboard without a live streaming pipeline.

### Key I/O
| | Detail |
|---|---|
| **Input** | `.npy` arrays from Phases 3, 4a, 4b |
| **Output** | `alerts.db` (~49 MB, 2 tables) |
| **`flow_scores` rows** | 500,407 |
| **`alerts` rows** | 85,070 |

### How to run
```bash
make write-alerts
```

---

## 10. Phase 6 — Streamlit Dashboard

### Flowchart

```
alerts.db
    │
    ├─[READ] flow_scores → anomaly score distribution histogram
    ├─[READ] alerts → KPI cards (total anomalies, alert rate, avg score)
    ├─[READ] alerts → attack type bar chart (rf_label breakdown)
    ├─[READ] alerts → data table (sortable, filterable by rf_label)
    └─[READ] alerts → top-N most anomalous flows
        │
        ▼
[Streamlit server — localhost:8501]
  ├─ Sidebar: label filter multi-select
  ├─ Top KPIs: 4 metric cards
  ├─ Charts: Plotly bar + histogram
  └─ Table: paginated alert log
```

### What it does
The dashboard is a **read-only** Streamlit application that queries `alerts.db` on every user interaction. It does not hold data in memory between sessions — each page load or filter change re-executes the relevant `SELECT` query against SQLite. This keeps memory usage negligible (the dashboard process uses <100 MB RAM) and ensures the dashboard always reflects the latest state of the database.

The **sidebar filter** allows filtering the alert table and attack-type chart by RF-predicted label. This lets a user (e.g., a network analyst) isolate "show me only DoS alerts" or "show me all Bots flagged today" without touching the underlying data.

The **anomaly score distribution histogram** (from `flow_scores`) is the primary tool for threshold tuning — the dashboard shows the full score spectrum so that an analyst can decide whether to tighten the contamination threshold and rerun Phase 4a.

### Key I/O
| | Detail |
|---|---|
| **Data source** | `alerts.db` (read-only queries) |
| **Port** | 8501 |
| **Charts** | Plotly bar (attack type breakdown), histogram (score distribution) |
| **Table** | Paginated alerts log with `flow_index`, `anomaly_score`, `rf_label`, `ground_truth` |

### How to run
```bash
make dashboard
# Open: http://localhost:8501
```

---

## 11. Test Suite

### Flowchart

```
tests/test_pipeline.py
        │
        ├─ Phase 1 checks (2):  clean_parquet/ exists, contains files
        ├─ Phase 3 checks (5):  X_scaled.npy, y_labels.npy, scaler.joblib,
        │                        correct shape (500,407 × 52)
        ├─ Phase 4 checks (10): all .joblib + .npy artifacts exist,
        │                        IF flagged non-zero rows,
        │                        both plot PNGs exist
        └─ Phase 5 checks (2):  alerts.db exists, row counts match
                │
                ▼
        19/19 checks — exit code 0
```

### What it does
The test suite is a **pure filesystem and shape validation** suite — it does not re-run any ML training. It checks that every expected output artifact exists, has the right dimensions, and that SQLite row counts are consistent with the flag arrays. This makes it fast (~1s) and safe to run at any time without triggering expensive recomputation.

### How to run
```bash
make test
```

---

## 12. Data Flow Summary Table

| Phase | Tool | Input | Output | Size |
|---|---|---|---|---|
| 1 — Cleaning | PySpark 3.5.1 | `cicds.csv` (~680 MB) | `clean_parquet/` | 2,520,751 rows |
| 2 — Streaming | Kafka 4.3.1 + Spark Structured Streaming | `clean_parquet/` | `network-flows` topic → `streaming_parquet/` | 500 rows/demo |
| 3 — Preprocessing | pandas + scikit-learn | `clean_parquet/` | `X_scaled.npy`, `y_labels.npy`, `scaler.joblib` | 500,407 × 52 |
| 4a — Isolation Forest | scikit-learn | `X_scaled.npy` | `if_scores.npy`, `if_flags.npy`, `isolation_forest.joblib` | 85,070 anomalies |
| 4b — Random Forest | scikit-learn | `X_scaled.npy`, `y_labels.npy` | `rf_predictions.npy`, `random_forest.joblib` | 99.70% accuracy |
| 4c — Evaluation | scikit-learn + matplotlib | `.npy` arrays | `models/plots/*.png` | 3 plots |
| 5 — Storage | SQLite 3 | `.npy` arrays | `alerts.db` | 49 MB, 2 tables |
| 6 — Dashboard | Streamlit + Plotly | `alerts.db` | Web UI at :8501 | Live |

---

*Generated: September 2026 · BCSE402L Big Data Analytics · VIT Chennai*

