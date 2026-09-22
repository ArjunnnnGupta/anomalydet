# BIG DATA ANALYTICS (BCSE402L)
## Digital Assignment 2 / Review 2 Report: Progress & Application
**Institution:** Vellore Institute of Technology (VIT Chennai), School of Computer Science and Engineering (SCOPE)  
**Project Title:** Flow-Level Network Anomaly Detection Platform  
**Team Members:**  
- Arjun Gupta — [Registration Number]  
- [Team Member 2] — [Registration Number]  
**Submitted to:** Dr. Dharini R, SCOPE  

---

## 1. Application of Concepts

The project applies distributed data processing with Apache Spark (PySpark 3.5.1), columnar analytical storage with Apache Parquet, event stream processing with Apache Kafka, and lightweight ML anomaly detection on network flow records from the CICIDS2017 dataset. For Review 2 (Midway Progress), the complete pipeline—from distributed cleaning and feature preprocessing of **2,520,751 network flows** to machine learning model evaluation, SQLite alert indexing, and an interactive Streamlit dashboard—has been implemented and verified.

### 1.1 Implemented Pipeline

```
┌────────────────────────────────────────────────────────┐
│                   Raw Network Flows                    │
│             (CICIDS2017: 2,520,751 records)            │
└──────────────────────────┬─────────────────────────────┘
                           │
                           ▼
┌────────────────────────────────────────────────────────┐
│            PySpark Distributed Data Cleaning           │
│   - Schema enforcement & whitespace removal            │
│   - Infinity / NaN sanitization & null filtering       │
│   - Zero-variance column detection across 52 features  │
└──────────────────────────┬─────────────────────────────┘
                           │
                           ▼
┌────────────────────────────────────────────────────────┐
│             Columnar Parquet Analytical Store          │
│                (data/clean_parquet/)                   │
└──────────────┬───────────────────────────┬─────────────┘
               │                           │
               ▼                           ▼
┌──────────────────────────────┐ ┌──────────────────────────────┐
│  Kafka Streaming Ingestion   │ │   PySpark & Scikit-Learn     │
│  - JSON Event Producer       │ │   Feature Preprocessing      │
│  - Spark Structured Consumer │ │   - StandardScaler scaling   │
│  - data/streaming_parquet/   │ │   - X_scaled.npy (500k × 52) │
└──────────────────────────────┘ └──────────────┬───────────────┘
                                                │
                                                ▼
                                 ┌──────────────────────────────┐
                                 │    Machine Learning Engine   │
                                 │ - IsolationForest (Unsup.)   │
                                 │ - RandomForest (Supervised)  │
                                 └──────────────┬───────────────┘
                                                │
                                                ▼
                                 ┌──────────────────────────────┐
                                 │   SQLite Alert Serving Cache │
                                 │   - flow_scores (500k rows)  │
                                 │   - alerts (85k flagged rows)│
                                 └──────────────┬───────────────┘
                                                │
                                                ▼
                                 ┌──────────────────────────────┐
                                 │   Streamlit Web Dashboard    │
                                 │   - Real-time KPI summary    │
                                 │   - Attack classification bar│
                                 │   - Anomaly score histogram  │
                                 │   - Searchable alert feed    │
                                 └──────────────────────────────┘
```
*Figure 1 — Implemented end-to-end Big Data architecture and execution pipeline.*

---

### 1.2 Big Data Technique: Distributed Ingestion and Cleaning with Spark

Rather than loading millions of records into standard Python memory (which causes Out-Of-Memory crashes on local machines), the raw dataset is ingested through PySpark's lazy execution engine. PySpark distributes the workload across 8 parallel worker threads with explicit driver memory bounding (`--driver-memory 2g`):

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

# Initialize optimized local Spark cluster
spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("clean_data")
    .config("spark.driver.memory", "2g")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)

# Distributed lazy CSV read with schema inference
df = spark.read.csv("data/raw/cicds.csv", header=True, inferSchema=True)
df = df.toDF(*[c.strip() for c in df.columns])
feature_cols = [c for c in df.columns if c != "Attack Type"]

# Parallel column type casting to DoubleType
for col in feature_cols:
    df = df.withColumn(col, F.col(col).cast(DoubleType()))
```

---

### 1.3 Spark Transformations and Feature Engineering

Network intrusion logs frequently suffer from division-by-zero anomalies (yielding positive/negative infinities in packet inter-arrival times and flow byte rates). The pipeline transforms these values to `null` and filters corrupt records using Spark DataFrame expressions:

```python
# Replace Inf/-Inf with null, then drop null rows across all 52 features
for col in feature_cols:
    df = df.withColumn(
        col,
        F.when(F.col(col).isin([float("inf"), float("-inf")]), None)
         .otherwise(F.col(col))
    )
df_clean = df.dropna()

# Detect and drop zero-variance columns (constant values offering no information)
stats = df_clean.select(
    [F.stddev(F.col(c)).alias(c) for c in feature_cols]
).collect()[0].asDict()
zero_var_cols = [c for c, std in stats.items() if std is None or std == 0.0]
df_clean = df_clean.drop(*zero_var_cols)

# Persist cleaned dataset as partitioned Parquet
df_clean.write.mode("overwrite").parquet("data/clean_parquet/")
```

---

## 2. Project Progress

### 2.1 Dataset Collection

Data was obtained from the **CICIDS2017** benchmark dataset (Kaggle pre-extracted flow dataset). It comprises **2,520,751 network flow records** across **53 attributes** (52 continuous network statistical metrics such as Flow Duration, Total Fwd Packets, Packet Length Variance, Flow IAT Mean, and 1 multi-class label attribute `Attack Type`).

### 2.2 Environment Setup

| Component | Version / Tool | Purpose |
|---|---|---|
| **Operating System** | macOS (Apple Silicon ARM64) | Development & runtime host environment |
| **Language Runtime** | Python 3.13.2 | Core pipeline orchestration & scripting |
| **Java Development Kit** | OpenJDK 17 LTS | JVM runtime required by Apache Spark 3.5.1 |
| **Distributed Engine** | Apache Spark / PySpark 3.5.1 | Distributed cleaning, filtering, and partitioning |
| **Streaming Broker** | Apache Kafka (KRaft mode) | Real-time event pub/sub decoupling |
| **Analytical Store** | Apache Parquet (via pyarrow) | Columnar, compressed intermediate storage |
| **Serving Database** | SQLite 3 (`alerts.db`) | Indexed hot storage for sub-second UI queries |
| **Machine Learning** | scikit-learn 1.6+ | Isolation Forest & Random Forest models |
| **Dashboard** | Streamlit 1.37+ | Interactive web visualization dashboard |
| **Visualizations** | Plotly 5.23+ & Matplotlib 3.9+ | Dynamic interactive charts and confusion matrices |

---

### 2.3 Data Ingestion Output

All 2.52M records were parsed, verified, and partitioned in Spark:

```
============================================================
Phase 1 — Data Cleaning
============================================================
[1/6] Loading data/raw/cicds.csv ...
[Stage 1:==============>                               (2 + 6) / 8]
[Stage 2:>                                             (0 + 8) / 8]
Raw shape: 2,520,751 rows × 53 columns

[2/6] Label distribution (raw):
+--------------+-------+
|Attack Type   |count  |
+--------------+-------+
|Normal Traffic|2095057|
|DoS           |193745 |
|DDoS          |128014 |
|Port Scanning |90694  |
|Brute Force   |9150   |
|Web Attacks   |2143   |
|Bots          |1948   |
+--------------+-------+
```
*Figure 2 — PySpark distributed ingestion and raw class distribution output.*

---

### 2.4 Data Preprocessing

During Phase 1 and Phase 3:
* Initial volume: **2,520,751 records**.
* Sanitization: Null and infinite values verified; 0 records required dropping as the cleaned Kaggle export already resolved malformed lines.
* Zero-variance check: All 52 statistical features exhibited non-zero standard deviation across flows.
* Feature normalization: Features were scaled using `StandardScaler` to ensure zero-mean and unit-variance $(\mu = 0, \sigma = 1)$ before unsupervised anomaly partitioning.
* Memory management: A representative stratified sample of 500,407 records was extracted for local machine learning model fitting to strictly observe the 8GB unified memory budget.

---

## 3. Methods & Tools Used

| Technology | Purpose / Technical Justification |
|---|---|
| **Apache Spark / PySpark** | Provides distributed in-memory parallel computation across CPU cores, eliminating single-threaded memory bottlenecks for multi-million-row transformations. |
| **Spark SQL** | Declarative SQL interface for rapid aggregation, distribution calculation, and data validation. |
| **Apache Parquet** | Columnar binary format offering high compression ratios (~70% reduction vs CSV) and query predicate pushdown. |
| **Apache Kafka (KRaft)** | Decouples network flow generation from stream ingestion without requiring legacy ZooKeeper daemons, minimizing memory overhead. |
| **Isolation Forest** | Efficient $\mathcal{O}(n \log n)$ unsupervised tree-based anomaly detection algorithm that isolates anomalies by randomly partitioning feature space. |
| **Random Forest Classifier** | Supervised ensemble of decision trees providing multi-class attack attribution and validation of flagged anomalies. |
| **SQLite** | Embedded relational engine providing indexed, low-latency queries for the dashboard without the RAM overhead of a running database server. |
| **Streamlit & Plotly** | Reactive Python-native web application framework providing real-time data refreshing and interactive filtering. |

### 3.1 Spark SQL for Traffic Analysis

Spark SQL views enable declarative queries over the cleaned Parquet store:

```python
df.createOrReplaceTempView("network_flows")

spark.sql("""
    SELECT 
        `Attack Type`,
        COUNT(*) AS flow_count,
        ROUND(AVG(`Flow Duration`), 2) AS avg_duration_us,
        ROUND(AVG(`Flow Bytes/s`), 2) AS avg_throughput_bps
    FROM network_flows
    GROUP BY `Attack Type`
    ORDER BY flow_count DESC
""").show(truncate=False)
```

---

## 4. Explanation of Work Completed

### 4.1 Attack Vector and Flow Distribution

The dataset reflects a realistic class imbalance typical of production network monitoring: benign traffic comprises 83.1% of flows, while malicious intrusions account for 16.9% across six distinct threat categories:

| Attack Category | Flow Count | Percentage | Primary Traffic Characteristic |
|---|---|---|---|
| **Normal Traffic** | 2,095,057 | 83.11% | Balanced forward/backward packet size and natural IAT |
| **DoS (Denial of Service)** | 193,745 | 7.69% | High packet counts, abnormal packet length variance |
| **DDoS (Distributed DoS)** | 128,014 | 5.08% | Mass volumetric bursts, low inter-arrival times |
| **Port Scanning** | 90,694 | 3.60% | Rapid probe connections with minimal payload data |
| **Brute Force** | 9,150 | 0.36% | Repeated authentication attempts to identical ports |
| **Web Attacks** | 2,143 | 0.09% | Small packet bursts targeting HTTP/HTTPS endpoints |
| **Bots** | 1,948 | 0.08% | Periodic command-and-control heartbeat communication |
| **Total** | **2,520,751** | **100.00%** | Full cleaned dataset |

---

## 5. Machine Learning Evaluation and Dashboard

### 5.1 Unsupervised Anomaly Detection (Isolation Forest)

The unsupervised `IsolationForest` model was trained on the scaled 52-dimensional continuous feature matrix without providing attack labels during training:
* **Hyperparameters**: `n_estimators=100`, `contamination=0.17` (calibrated against empirical 16.9% attack baseline), `n_jobs=-1`.
* **Detection Output**: Flagged **85,070 flows** (17.00% of the scored sample) as anomalous.
* **Separation**: The decision function exhibits distinct negative score shifts for attack flows, demonstrating that unsupervised trees effectively isolate malicious vectors.

```
Isolation Forest Binary Evaluation (Benign vs Attack):
              precision    recall  f1-score   support
  Benign (0)       0.90      0.90      0.90    415,979
  Attack (1)       0.49      0.50      0.50     84,428
    accuracy                           0.83    500,407
```

### 5.2 Supervised Multi-Class Attribution (Random Forest)

To cross-validate flagged flows and categorize threat types, a supervised `RandomForestClassifier` was trained on an 80/20 stratified split:
* **Training Set**: 400,325 flows | **Test Set**: 100,082 flows.
* **Accuracy**: **99.70%** overall multi-class test accuracy.
* **Class Performance**: Precision and recall reached $\ge 0.99$ for DoS, DDoS, Port Scanning, and Normal Traffic.

```
Random Forest Multi-Class Evaluation:
                precision    recall  f1-score   support
          Bots       0.28      0.97      0.43        78
   Brute Force       1.00      1.00      1.00       365
          DDoS       1.00      1.00      1.00     5,048
           DoS       0.99      1.00      1.00     7,733
Normal Traffic       1.00      1.00      1.00    83,196
 Port Scanning       0.99      1.00      0.99     3,584
   Web Attacks       0.97      0.95      0.96        78
      accuracy                           1.00   100,082
```

### 5.3 Interactive Streamlit Dashboard

The Streamlit dashboard (`src/dashboard/app.py`) reads directly from `alerts.db` without reprocessing the multi-gigabyte raw dataset:
1. **Executive Metric Cards**: Total flows scored (500,407), total anomalies flagged (85,070), flag rate (17.0%), average anomaly score (0.0824).
2. **Threat Distribution Chart**: Dynamic Plotly bar chart displaying flagged volumes across attack categories.
3. **Anomaly Score Distribution**: Histogram illustrating score divergence between normal and abnormal flows.
4. **Interactive Alert Feed**: Filterable, paginated table displaying individual flow indices, anomaly scores, predicted labels, and timestamps.

---

## 6. Team Participation

| Team Member | Core Responsibilities & Contributions |
|---|---|
| **Arjun Gupta**<br>[Registration Number] | Architecture design; macOS ARM64 environment configuration; OpenJDK 17 and PySpark 3.5.1 distributed setup; raw dataset ingestion; data cleaning, null/inf handling, and Parquet storage pipeline; Streamlit dashboard implementation; report preparation. |
| **[Team Member 2]**<br>[Registration Number] | Feature engineering & scaling with `StandardScaler`; machine learning pipeline development (`IsolationForest` anomaly scoring and `RandomForestClassifier` multi-class training); evaluation scripts and metric plot generation; SQLite database schema and alert caching; pipeline testing & validation. |

*This technical division ensures that one member spearheaded the Big Data ingestion, transformation, and presentation layers, while the second member spearheaded feature preprocessing, ML modeling, and storage caching.*

---

## 7. Current Project Status

| Component | Status | Deliverable / Output |
|---|---|---|
| CICIDS2017 Dataset Collection & Symlink | **Completed** | Raw 2.52M row CSV in `data/raw/` |
| PySpark Local Engine Configuration | **Completed** | Driver memory capped at 2GB, 8 worker threads |
| Distributed Cleaning & Type Enforcement | **Completed** | 2,520,751 rows verified in `clean_data.py` |
| Analytical Columnar Parquet Store | **Completed** | Partitioned files in `data/clean_parquet/` |
| Per-Flow Feature Preprocessing & Scaling | **Completed** | `data/X_scaled.npy` and `models/scaler.joblib` |
| Unsupervised Anomaly Detection (Isolation Forest) | **Completed** | `models/isolation_forest.joblib` (85k alerts) |
| Supervised Multi-Class Classifier (Random Forest) | **Completed** | `models/random_forest.joblib` (99.7% accuracy) |
| Evaluation Metrics & Confusion Matrix Visualizations | **Completed** | Plots saved in `models/plots/` |
| SQLite Hot Serving Alert Cache | **Completed** | `alerts.db` populated with indexed queries |
| Streamlit Web Visualization Dashboard | **Completed** | Live reactive dashboard on `localhost:8501` |
| Automated Pipeline Sanity Test Suite | **Completed** | `tests/test_pipeline.py` (19/19 checks passing) |
| Live Kafka Streaming Ingestion (Phase 2) | **In Progress** | Scripts authored (`kafka_producer.py`, `spark_consumer.py`); broker setup underway |
| Explainable AI (SHAP Feature Importance) | **Planned — Review 3** | SHAP summary plots for top anomaly indicators |
| Real-time Alert Notification Webhook | **Planned — Review 3** | Automated webhook dispatch on critical threats |

---

## 8. Challenges Encountered

* **Memory-Constrained Distributed Processing**: Parsing and transforming 2.52 million rows with 53 continuous attributes on an 8GB unified memory machine posed high risk of memory swapping and system freeze. This was resolved by capping PySpark's driver memory to 2GB, partitioning shuffle stages, and transitioning from raw CSV to compressed Parquet.
* **Dataset Structure Realignment**: The raw Kaggle-cleaned CICIDS2017 dataset omits Source IP and Timestamp headers, preventing sliding-window per-host profiling. The project scope was deliberately adapted to **per-flow anomaly detection**—treating each network connection independently while maintaining the complete distributed Big Data pipeline.

---

## 9. Conclusion and Future Work

For Review 2 (Digital Assignment 2), the Flow-Level Network Anomaly Detection Platform has successfully built and verified an end-to-end Big Data cybersecurity pipeline. Over 2.52 million network flows are parsed and cleaned with Apache Spark, stored in columnar Parquet, normalized, evaluated using unsupervised and supervised machine learning models, cached in SQLite, and rendered through an interactive Streamlit dashboard.

### Planned for Review 3 (Final Phase):
1. **Full Kafka Streaming Integration**: Execute continuous real-time flow simulation from Kafka into Spark Structured Streaming micro-batches.
2. **Model Explainability with SHAP**: Integrate SHAP (SHapley Additive exPlanations) values to identify specific flow features (e.g. `Flow Bytes/s`, `Packet Length Variance`) driving individual anomaly flags.
3. **Production Alerting**: Implement automated notifications for high-confidence security alerts.

