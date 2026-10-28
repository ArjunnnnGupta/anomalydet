# Rebuild Checkpoints

Progress log for the Kappa-architecture rebuild (Kafka → Spark Structured Streaming → Delta Lake → MLlib).
A new section is added after each major task.

---

## Checkpoint 1: Backup and cleanup (2026-10-19)

**Done**
- Backed up the old code and docs to `../anomaly-detection-platform_backup_v1/` (66 MB; venv and data excluded).
- Removed the old generated files: `data/clean_parquet`, `data/preprocessed_parquet`, `data/*.npy`, `alerts.db`, `models/*.joblib`, `models/plots`.
- Removed the old modules: `src/processing/*`, `src/storage/*`, `src/ml/train_*`, `src/ingestion/kafka_producer.py`, `src/ingestion/spark_consumer.py`, `test_spark.py`, `scripts/*.sh`, `kafka-kraft.properties`, `docker/kafka-compose.yml`, plus the empty `notebooks/` and `screenshots/` folders.

**Known constraints**
- 8 GB RAM, about 15 GB free disk. Kafka retention and memory caps are kept small.
- Docker Desktop is not installed yet (user action).
- The CICIDS2017 `GeneratedLabelledFlows` CSVs are not downloaded yet (user action). They go in `data/raw/cicids2017/`.

**Next:** infrastructure (docker-compose, Spark image, `conf/pipeline.yaml`, `src/common/*`).

---

## Checkpoint 2: Infrastructure and shared foundation (2026-10-19)

**Done**
- `conf/pipeline.yaml` is the single source of truth: dataset files, Kafka topics and producer tuning, Spark sizing, lake paths, streaming windows and thresholds, ML hyperparameters, and the SQLite path.
- `src/common/config.py` loads the config and resolves paths. It picks the internal or external Kafka bootstrap automatically, and `KAFKA_RF` / `KAFKA_BOOTSTRAP` can be overridden from the environment.
- `src/common/spark.py` has one `get_spark()` for every job. It enables Delta, AQE (coalesce + skew join), RocksDB state store, Arrow, event logs and the broadcast threshold, plus FAIR scheduling and `spark.cores.max` for the streaming app.
- `src/common/schemas.py` holds the column normalisation (snake_case plus dedupe of the duplicated `Fwd Header Length`), the 85-column contract, `FEATURE_COLS` (constant columns removed), the all-string Kafka `PAYLOAD_SCHEMA`, and the label cleaning and 8-family mapping.
- `src/common/dims.py` defines the `dim_subnets` (CICIDS testbed networks) and `dim_ports` (port → service) broadcast tables.
- `docker/spark/Dockerfile`: `apache/spark:3.5.3-python3` with the Kafka connector and Delta 3.2.1 jars baked in, plus the Python deps for drivers and executors.
- `docker/docker-compose.yml`: 3-broker KRaft Kafka (RF=3, min.ISR=2, internal listener 19092, host ports 9092/9094/9096), Kafka UI on :8080, Spark master on :8081 (it also hosts the drivers; app UIs on :4040/:4041), and 2 workers (2 cores, 1200m each).
- `docker/docker-compose.lite.yml`: 1 broker + 1 worker fallback (RF=1).

**Deviation from the plan**
- There's no `kafka-init` container. Topics are created by `src/ingestion/create_topics.py` (`make topics`), so partitions, RF and retention come from `pipeline.yaml` rather than being duplicated in a shell script.

**Not yet verified:** Docker isn't installed, so the images and compose files haven't been built or started.

**Next:** Kafka topic admin and the replay producer.

---

## Checkpoint 3: Kafka ingestion and the streaming lakehouse (2026-10-19)

**Done**
- `src/ingestion/create_topics.py` creates topics through the AdminClient: partitions, RF, `min.insync.replicas` and lz4 compression, with `retention.bytes` capped to protect the ~15 GB of free disk. It supports `--recreate` and `--describe` (shows leaders, replicas and ISR for each partition).
- `src/ingestion/replay_producer.py`:
  - Reads with pyarrow, keeping all values as raw strings, and sorts within each file by flow start time.
  - Does the deterministic crc32 80/20 backfill/live split and keys messages by Source IP.
  - The librdkafka producer is idempotent, uses `acks=all`, lz4, `linger.ms` and `batch.size`.
  - Paces against event time (`--speedup`, idle gaps capped at 60s), and can re-base timestamps onto the wall clock for the live pass.
  - Also has `--max-rate`, `--limit` and `--inject-bad` (sends the 3 kinds of malformed record the DQ rules catch). It skips the ~288K empty lines in the Thursday file.
- `src/common/lake.py`: `ensure_table`, `merge_upsert` (MERGE INTO), `idempotent_append` (`txnAppId`/`txnVersion`), and `stream_source` (Delta as a stream source).
- `src/streaming/`:
  - `bronze.py` (Q1): Kafka → Delta, with `maxOffsetsPerTrigger` backpressure and the Kafka coordinates kept.
  - `silver.py` (Q2): from_json, mixed-format timestamps with the 12-hour fix, Inf/NaN → NULL, labels mapped to 8 families, sha2 `flow_uid`, and 4 DQ rules feeding the quarantine table and `flows.dlq`. Then broadcast joins with `dim_subnets`/`dim_ports`, and an insert-only MERGE pruned to the batch's `day` partitions.
  - `gold.py` (Q3a/Q3b): 1-minute tumbling and 5-minute/1-minute sliding windows with a watermark and `approx_count_distinct`, written in update mode with MERGE into Delta and mirrored to SQLite. Includes port-scan candidates.
  - `scoring.py` (Q4): foreachBatch with registry hot-reload, scoring with RF and KMeans, written to Delta `gold_scored` (idempotent), `flows.scored`, `alerts`, and SQLite `alerts`/`score_stats`.
  - `stateful.py` (Q5): `applyInPandasWithState` with an event-time timeout for per-host alert-streak escalation.
  - `metrics_listener.py`: StreamingQueryListener → SQLite `stream_metrics`. It also commits the bronze offsets to the `spark-bronze-ingest` group so Kafka UI shows lag.
  - `stream_app.py`: one Spark app running all 6 queries (FAIR pools, `--only` for subsets). It creates the Delta tables up front.
- `src/serving/schema.sql` and `sqlite_sink.py`: idempotent INSERT OR REPLACE on natural keys, with a thread lock because queries run concurrently.
- `src/ml/model_utils.py` (shared feature pipeline, P(attack), pandas-UDF centroid distance, alert rule) and `src/ml/registry.py` (versioned registry, atomic `latest.json`, HotLoader).
- Host venv now pinned to pyspark 3.5.3, delta-spark 3.2.1, pandas 2.2.3, pyarrow 18.1, confluent-kafka and deltalake.

**Deviations from the plan**
- The broadcast joins with the dimension tables happen in silver (enrichment), not in scoring. Scoring inherits `src_zone`/`dst_zone`/`service`.
- The registry lives at `src/ml/registry.py`, because training and streaming both use it.
- `score_stats` (detections per minute vs ground truth) is a new SQLite table.

**Verified:** a local smoke test of silver `parse` + `enrich` and both gold aggregations. Timestamp formats, the 12-hour fix, label cleaning, Infinity → NULL, all three DQ reasons, broadcast joins, tumbling and sliding windows all came out correct.

**Next:** MLlib training, evaluation and lakehouse maintenance.

---

## Checkpoint 4: MLlib, lakehouse ops and the test suite (2026-10-20)

**Done**
- `src/ml/train_pipeline.py` (`train(spark, use_cv)` plus a CLI):
  - Reads silver's backfill split pinned to a Delta version via time travel.
  - Stratified `sampleBy` with a `left_anti` test split, and √-balanced class weights joined via broadcast.
  - Feature PipelineModel, then RandomForest with a distributed CrossValidator (or `--no-cv`), then IndexToString.
  - KMeans fitted on benign flows, with the anomaly threshold from `approxQuantile`.
  - Held-out metrics (macro-F1, per-class, AUCs, alert precision and recall), registered as `vN`, with test predictions written to `gold_model_predictions`.
- `src/ml/evaluate.py` (`evaluate(spark, version, live)`): groupBy confusion matrix, Spark-side histogram buckets, AUCs and alerts by reason. Writes `metrics[_live].json` plus PNGs (confusion matrix, score distribution, feature importance).
- `src/lakehouse/maintenance.py` (`run(spark)`): DESCRIBE DETAIL inventory, OPTIMIZE (ZORDER BY src_ip on silver/scored/host_profile, compaction elsewhere), DESCRIBE HISTORY, VERSION AS OF row counts, the `mergeSchema` evolution demo, and VACUUM.
- `--available-now` catch-up mode in `stream_app.py` (via the `STREAM_AVAILABLE_NOW` env): drains each stage in upstream → downstream order. Tests use it to run the real streaming queries.
- `KAFKA_SINKS=0` switch so tests run without a broker.
- Tests (`tests/`): `synthetic.py` generates a clearly-synthetic, CICIDS-shaped CSV and payloads (spaced header, duplicated column, 0x96 label, both timestamp formats, blank tail lines).
  - `test_ingestion.py` (5 tests) and `test_pipeline.py` (10 tests) run bronze → silver → gold → train → scoring → stateful → hot reload → evaluate → maintenance.

**Fixes found by the tests**
- `FEATURE_COLS` used the raw `destination_port` name instead of silver's `dst_port`.
- A module-level Column in `gold.py` needed an active SparkContext.
- Python UDF workers used the system python. `get_spark` now defaults `PYSPARK_PYTHON` to the driver's interpreter.

**Verified:** `pytest tests/` → **15 passed** (about 2.5 min, local[2]). The run covers:
- quarantine of the 3 DQ reasons
- MERGE and txn idempotency on replay
- window totals and port-scan detection
- skip-until-model, then v1 trained, registered and hot-loaded, then v2 hot-swapped
- live split fully scored to Delta and SQLite with no duplicate alerts
- escalation for the attacker host
- metrics and plots written
- OPTIMIZE/ZORDER, time travel and schema evolution
(The perfect F1 is only because the synthetic families are separable; real CICIDS numbers will be lower.)

**Not yet verified:** anything that needs Docker or the real dataset: the Kafka source (Q1), producer → broker, the cluster submit.

**Next:** Streamlit dashboard, Makefile, requirements, README/ARCHITECTURE.

---

## Checkpoint 5: Dashboard, Makefile and operational tooling (2026-10-20)

**Done**
- `src/dashboard/app.py`: 5 tabs, with no Spark in the dashboard process.
  - **Live pipeline:** per-query rows/s, Kafka lag per partition (listener end offsets vs broker high-watermarks), throughput and batch-duration charts, query state, pipeline events.
  - **Threat timeline:** detections per minute by predicted family, alert-reason split.
  - **Hosts:** top alerting IPs, stateful escalations, port-scan candidates, a sliding-window scatter.
  - **Alert feed:** filterable.
  - **Lakehouse & model:** Delta table versions and files via delta-rs, the gold timeline, model metrics (held-out vs live), per-class table and plots.
- `src/ingestion/wait_caught_up.py` (`make wait`): blocks until Kafka lag is 0 and silver has been quiet for 30s. Spark 3.5 reports idle triggers via `onQueryIdle`, so it uses a wall-clock quiet period rather than empty batches.
- The producer gained `--bad-only` for `make inject-bad`.
- `Makefile`:
  - Host targets: `setup`, `test`, `topics`, `backfill`, `live`, `inject-bad`, `wait`, `dashboard`.
  - Cluster targets: `build`, `cluster-up/down/ps`, `stream`, `stream-bg` (logs to `data/logs/stream_app.log`), `stream-catchup`, `train`, `evaluate`, `evaluate-live`, `maintenance`.
  - `demo` chains topics → stream-bg → backfill → wait → train → live. `reset` asks for confirmation first. `LITE=1` switches to the lite compose profile.
- `requirements.txt` holds the host pins (pyspark 3.5.3, delta-spark 3.2.1, pandas 2.2.3, confluent-kafka, deltalake, streamlit ≥1.50, …).
- `.gitignore`: all of `data/` is generated or downloaded.
- Config loads `conf/pipeline.yaml` from the repo by default, with `APP_HOME` covering only `data/`.
- Removed the stray `spark-warehouse/` and the empty `models/` from the repo root.

**Verified**
- `pytest tests/` → **16 passed**, including a new check that the StreamingQueryListener records progress for every query.
- Streamlit AppTest run headless against real pipeline output from the tests: all 5 tabs render with no exceptions, the KPIs populate, and all 8 Delta tables are read through delta-rs.
- `make -n` dry runs of the cluster and lite targets.
- Both compose files parse (service list, memory caps, listener layout).

**Still not verified (needs Docker and the real dataset):** image build, broker quorum, the Kafka source and sinks, `spark-submit` on the cluster, `make wait` against real lag.

**Next:** rewrite README.md and ARCHITECTURE.md.

---

## Checkpoint 6: Docs, and a real-Kafka integration run (2026-10-20)

**Done**
- Rewrote `README.md` (quick start, two-pass Kappa run, make targets, layout, troubleshooting) and `ARCHITECTURE.md` (Kappa vs Lambda, topology, Kafka design, streaming DAG, medallion tables, MLlib, serving and observability, delivery-guarantee table, the **big data concept → code** matrix, honest limitations).
- Restored `make report` for the unchanged `generate_report_pdf.py`, and added `reportlab` to `requirements.txt`.
- `stream_app.py --available-now` now calls `spark.stop()` before exiting. Without it, the JVM delivered listener callbacks after Python had exited (a py4j "connection refused" trace).

**Verified against a real Kafka broker** (Homebrew Kafka 4.3.1, single KRaft node, data in the session scratchpad, stopped afterwards; synthetic CSVs under real file names):

| Check | Result |
|---|---|
| `create_topics` | 4 topics, 12/6/3/1 partitions, leader/ISR printed |
| producer backfill | 923 sent, 923 acknowledged, 217 held for live, 0 failed |
| `--inject-bad 30 --bad-only` | 30 malformed records sent |
| Q1 Kafka source → bronze | 953 rows (923 + 30) |
| Q2 silver + quarantine + **Kafka DLQ** | 923 / 30 / `flows.dlq` = 30 messages keyed by DQ reason |
| Q3 gold | 232 timeline windows, 1,491 host windows, 5 port-scan candidates |
| listener → consumer-group lag commit | `spark-bronze-ingest`: committed offsets = log end, lag 0 |
| `make wait` logic | returned "caught up" once lag was 0 and silver was quiet |
| training CLI on the Kafka-built lake | v1 registered from silver@v1 |
| always-on stream + paced live replay (`--speedup 300`) | 217 flows over 12.5s → scored; **Kafka `flows.scored` = 217, `alerts` = 104**, SQLite alerts = 104 (unique); escalation level 2 for the attacker host |
| **hot swap mid-stream** | trained v2 while running → `loaded model v2 (hot reload, batch 3)`; new alerts tagged v2 |
| **kill -9 + restart** | queries resumed from checkpoints (same IDs); bronze 1,291 = messages produced, silver 1,261 unique, gold_scored 338 unique, SQLite alerts 160 unique |

**Still not verified (needs Docker and the real CICIDS2017 CSVs):** the Spark image build, the 3-broker cluster (replication, broker failover), `spark-submit` onto the standalone workers, full-size data volume, and memory headroom on 8 GB.

**Next (user):** install Docker Desktop, download `GeneratedLabelledFlows.zip` into `data/raw/cicids2017/`, then `make setup build cluster-up topics` → `make demo`.

---

## Checkpoint 7: Real dataset in place (2026-10-20)

- CICIDS2017 `GeneratedLabelledFlows` (8 CSVs, 1.2 GB) moved to `data/raw/cicids2017/` (user had unzipped to `data/raw/cicds2017/TrafficLabelling `; the zip is still in `data/raw/cicds2017/` and can be deleted to free 284 MB).
- Validated with the producer's own reader on the real files: **2,830,743 rows**, **0 unparseable timestamps**, span 2017-07-03 08:55 → 2017-07-07 17:02 (the 12-hour afternoon fix lands Thursday/Friday afternoon files correctly at 13:00+), 15 raw labels, all mapped to families. File names match `conf/pipeline.yaml`.
- Docker Desktop is installed and running (server 29.9.1) but `docker` is not on PATH (`/Applications/Docker.app/Contents/Resources/bin`) and its VM has **4.1 GB / 8 CPUs** (plan assumed ~6 GB). Disk: ~10 GB free.
- **Next:** raise Docker memory (or use `LITE=1`), then `make build cluster-up topics`, `make demo`.

---

## Checkpoint 8: First real Docker build, fixed (2026-10-20)

User ran `make build` and hit `numpy==1.26.4` not found. Root cause: `apache/spark:3.5.3-python3` is **Ubuntu 20.04 with Python 3.8.10**, not a modern Python.

**Fixes**
- `docker/spark/Dockerfile`: pins changed to the last 3.8-compatible releases (numpy 1.24.4, pandas 2.0.3, pyarrow 17.0.0, matplotlib 3.7.5, confluent-kafka 2.5.3, pyyaml 6.0.2). pip 20.0.2 in the base image can't see `manylinux_2_28` ARM wheels (it silently tried to compile confluent-kafka), so the Dockerfile upgrades pip and uses `python3 -m pip` throughout.
- `from __future__ import annotations` added to the 12 `src/` modules that use `X | None` / `list[str]` annotations (they'd crash on import under 3.8).

**Verified (real image, built locally):** `docker build` succeeds, and **all 21 `src.*` modules import cleanly on Python 3.8.10** inside the container (with Spark's Python on the path, as `spark-submit` sets it). Host lint and tests unaffected (host runs 3.13).

**Not verified yet:** anything past the image: cluster start, topics on the 3-broker quorum, `spark-submit` on workers, a real run. Python 3.8 executors + pandas 2.0.3 for the pandas UDF / `applyInPandasWithState` paths are covered by the import check only, not an executed run.

---

## Checkpoint 9: Real cluster running; backfill data-loss bug found and fixed (2026-10-20)

**Verified on the real 3-broker cluster:** cluster healthy, topics created (12/6/3/1 partitions, RF=3, ISR=[all 3] on every partition, leaders spread over brokers 1–3), the stream app starts all 6 queries on the Spark standalone workers, the producer delivers the full backfill (2,264,272 messages), bronze → silver ran (silver 260,611 rows from the part of the data that survived).

**Bug (mine):** the replay producer stamped each Kafka record with its original **2017 event time**. Broker retention (`log.retention.hours=48`) works on record timestamps, so ~9 min into the run Kafka logged `Deleting segment … largestRecordTimestamp=1499262600000 due to log retention time 172800000ms` on every partition and the data dir shrank to 1.1 MB. The bronze Kafka source had `failOnDataLoss=false`, so it silently advanced past the deleted offsets: from batch 34 on, offsets moved +50k per batch with **0 rows read**. Symptom that exposed it: Kafka group offset said 1.45M consumed, Delta bronze had 260,638 rows.

**Fix:** (1) producer no longer sets the Kafka record timestamp (CreateTime = send time; event time stays in the payload and the live pass still re-bases `event_time` via `replay_ts_ms`); (2) bronze now uses `failOnDataLoss=true` so skipped offsets fail loudly. ARCHITECTURE.md updated. Recovery: `make reset`, restart the stream, rerun `make backfill`.

**Also noted:** spark-worker-1 uses ~1.53 of 1.56 GiB during streaming (close to its cap); brokers idle at ~450 MiB of 512.

---

## Checkpoint 10: Timestamp fix confirmed; executor out-of-memory fixed (2026-10-20)

**Timestamp fix works:** after `make reset` and a fresh backfill, Kafka retains the full backfill: log start 0, log end **2,264,272** on `flows.raw`, ~806 MB per broker, no retention deletions after the reset.

**New failure:** about 5 minutes into the replay, q1_bronze died with `SparkOutOfMemoryError: [UNABLE_TO_ACQUIRE_MEMORY] Unable to acquire 65536 bytes`, failing 4 times in stage 150. The Spark event log shows the trace in `UnsafeExternalRowSorter.insertRow`: the **partitioned** Delta write (`partitionBy("ingest_date")`) sorts every 50k-row batch of raw JSON. All 6 queries share one executor (640 MB heap, 2 cores), and the sort couldn't get memory. No container was OOM-killed; this was Spark's own execution memory.

**Fix**
- Bronze is no longer partitioned (every row in a batch has the same ingest_date anyway), so the write needs no sort.
- `max_offsets_per_trigger` 50k → 20k.
- `executor_memory` 640m → 900m, and the worker container cap 1600m → 1800m (both compose files).
- The RocksDB state store is capped at 128 MB (`boundedMemoryUsage`).
- `pytest` → 16 passed.

**Recovery (superseded by checkpoint 11):** the backfill is fully retained in Kafka, so the producer doesn't need to run again. Recreate the workers, wipe `data/lake`, `data/checkpoints` and `data/serving` (the old bronze table is partitioned and the old checkpoints point at it), and restart the stream. It replays all 2.26M messages from offset 0.

---

## Checkpoint 11: Simplified for a fast, explainable demo (2026-10-20)

The user's goal: a demo that runs fast, works reliably, and can be explained end to end. The full 2.26M-row replay through 6 queries was slow on the 8 GB MacBook Air and made it hot. Cuts:

1. **Smaller data.** The producer keeps every attack flow and 20% of benign flows (`dataset.benign_pct: 20`, deterministic hash, `--benign-pct` to override). Measured on the real files: **1,012,310 flows** (557,646 attacks + 454,664 benign), about 810K backfill and 203K live. Attacks become ~55% of the traffic, so precision is optimistic; this is documented.
2. **Q5 (stateful escalation, `applyInPandasWithState`) removed.** `stateful.py`, its SQLite table, the dashboard panel, the test and the doc rows are gone. 5 queries remain.
3. **Host windows: 1-minute tumbling** instead of 5-minute sliding (about 5x less work). Sliding is still a one-line config change.
4. **Training: one RandomForest by default.** CrossValidator only runs with `make train ARGS=--cv`.

Kept: 3-broker Kafka with replication, Kappa replay, bronze → silver → gold on Delta, live MLlib scoring with model hot-swap, and the dashboard.

**Verified:** lint clean, `pytest` → 16 passed (Q5 test removed, benign-downsampling test added). README and ARCHITECTURE updated.

**Next (user):** stop the current run, `make reset` (recreates topics, wipes the lake), then `make stream` / `make backfill` / `make wait` / `make train` / `make live`.

---

## Checkpoint 12: First full run; streaming app OOM-killed; batch caps and re-runnable live demo (2026-10-20)

**What worked on the cluster:** the 1.01M-flow backfill was replayed (809,632 messages), the stream built silver, and `make train` registered **model v1, macro-F1 0.907** (silver@v9). `make live` sent all 202,678 live messages to Kafka.

**What failed:** the streaming app died at ~13:15 (Spark master: no active apps). Both worker containers show `OOMKilled=true`: Docker's kernel killed the executor (the `exit code 137` seen in the training log). Bronze had consumed exactly the backfill, so the live messages are still unprocessed in Kafka and nothing was scored (the dashboard showed `q4_scoring` at 0 rows).

**Cause:** only the Kafka source was rate-limited. The Delta-stream stages (silver, gold, scoring) read their whole backlog in one micro-batch: silver took 138K rows in a 214s batch, gold_hosts 276K rows. With 900m of heap plus uncapped off-heap memory, the worker container (1800m) was killed.

**Fixes**
- Delta stream sources now use `maxFilesPerTrigger: 24` / `maxBytesPerTrigger: 16m` (config).
- Executor heap is 800m with `-XX:MaxDirectMemorySize=256m`.
- **Re-runnable live demo:** each live replay carries a `run_id` that goes into `flow_uid`, so `make live` can be run again. Before, silver's MERGE treated a replay as duplicates. Backfill rows hash exactly as before, because `concat_ws` skips the null `run_id`.
- `pytest` → 16 passed. The changed modules import on Python 3.8 in the cluster image.

**Recovery:** no reset needed. `make stream` resumes from the checkpoints and processes the 202,678 live messages already in Kafka with model v1.

**Not yet verified on the cluster:** whether the new memory settings avoid the OOM kill.

---

## Checkpoint 13: DA3 report generated (2026-10-20)

- `../BigData_DA3_Report.pdf` (12 pages) written in the layout of the reference `BigData_DA3.pdf` (VIT cover, bordered pages, numbered sections 1–7), built directly with reportlab from real outputs: model registry v1 `meta.json`, SQLite serving tables and Delta table versions/row counts.
- The report states the live run as observed: silver (q2) terminated at 14:18 UTC with a broadcast timeout (300 s) inside its MERGE; 96,491 of 202,678 live flows reached silver, 19,985 were scored, 10,385 alerts.
- **Next:** fix the silver broadcast timeout (raise `spark.sql.broadcastTimeout` or disable broadcast for the MERGE), rerun the live pass, then regenerate the report numbers.

---

## Checkpoint 13: Silver made fast; live scoring confirmed working (2026-10-20)

- First full run on the cluster: 1.01M-flow backfill replayed, model v1 trained (macro-F1 0.907), and after restarting the stream with batch caps, **live scoring worked: 10,385 alerts** written to the serving table while silver was still catching up.
- **Silver was the bottleneck.** The insert-only `MERGE INTO` on `flow_uid` re-scanned the whole silver table on every micro-batch (~2.5 min per batch at ~800K rows), so scoring (which reads silver) lagged far behind the live replay. Replaced with a Delta **idempotent append** (`txnAppId`/`txnVersion`): same exactly-once on retries, no table scan. Duplicates within a batch are still dropped; cross-batch duplicates can't occur because bronze is exactly-once and each live replay has its own `run_id`. MERGE remains in the small gold window tables.
- ARCHITECTURE.md updated (§4 design notes, §5 table, §8 delivery table, concept matrix).
- `pytest` → 16 passed. Needs `make stream` restart to take effect.

---

## Checkpoint 14: Second cluster run; two OOM kills diagnosed; one-executor-per-worker layout (2026-10-24)

**What happened:** after the restart, the stream processed silver/gold, then `q4_scoring` loaded model v1 (18:28) and never finished a batch. The streaming app disappeared with no ApplicationEnd event. `make evaluate-live` (started 18:28) measured the 19,985 flows scored on Oct 20: alert precision 0.995, recall 0.991, RF AUC 0.9998, KMeans AUC 0.786, macro-F1 0.598 (small, partial sample). The second `make live` ran after the stream had already died, so its 202,678 messages are unconsumed in Kafka.

**Cause, from Docker state and the Spark event log:**
1. `spark-master` container `OOMKilled=true`. It hosts the drivers, and the stream driver plus the evaluate driver (1 GB) together exceeded its 2.5 GB cap, so the streaming driver was killed.
2. `spark-worker-1` killed an executor again (`exit code 137` at 18:36:51) during scoring. With one 2-core executor, all the stream's tasks and both pandas-UDF Python workers ran on worker-1 while worker-2 stayed idle.

**Fix**
- The streaming app now uses two **1-core executors, one per worker** (`stream_executor_cores: 1`, `stream_cores_max: 2`). Batch jobs keep 2-core executors.
- `train`, `evaluate`, `evaluate-live` and `maintenance` now **refuse to start while `kappa-stream-app` is running** (a Makefile guard queries the Spark master), with a clear message to stop the stream first.
- Removed `make demo`, which ran `train` beside the background stream.
- README and ARCHITECTURE updated: core layout, the batch-jobs-need-the-stream-stopped rule, and an honest description of hot swap (the scorer re-reads `latest.json` every batch; on this 6 GB setup a new model is served on the next `make stream`).
- The guard was tested both ways (passes when idle, blocks with exit 1 when the stream app is listed). The config loads in the cluster image.

**Next (user):** `make stream` → `make wait` → stop the stream → `make evaluate-live`.

---

## Checkpoint 15: Dashboard fixed against real data (2026-10-25)

Rendered every tab headless (Streamlit AppTest) against the real run's data and fixed what was wrong or misleading:
- **Sidebar / Live pipeline:** stream status from the Spark master REST API (🟢/🔴), a banner when the stream is stopped, rate cards show the age of their numbers ("batch 9 · 4 days ago") in neutral colour, and the throughput / batch-duration charts show only the last 30 minutes (they had stretched across Oct 20–24).
- **Threat timeline:** said "No live flows scored yet" despite 10,385 alerts, because it read only `score_stats` (the last write of a scoring batch, and both batches had been killed before it). It's now built from the `alerts` table plus a row count of Delta `gold_scored`: flows scored, alerts, precision (alerts that were real attacks) and recall. On the real data it shows 19,985 scored, 10,385 alerts, 99.5% / 99.1%, matching `make evaluate-live` independently.
- **Hosts:** "sliding window" renamed to 1-minute window, captions explain the port-scan threshold and the scatter.
- **Alert feed:** readable column order (time, IPs, port, service, prediction, ✅/❌ vs ground truth, probabilities), with the 64-character `flow_uid` moved last.
- **Lakehouse & model:** "live" numbers no longer appear as red/green change arrows. A switch selects held-out vs live metrics, each metric has a help tooltip, per-class support shows as integers, and a caption shows the evaluated row count.

Verified: no exceptions on any tab, and the metrics switch works.

---

## Checkpoint 16: Lite default, consistency review, two more real-cluster bugs (2026-10-28)

**Switched to the lite profile (now the default):** 1 Kafka broker (RF=1), Kafka UI, Spark master and 1 worker (2 cores, 3 GB container). The full cluster is kept with `LITE=0`, and its Kafka volumes are untouched. On lite, the stream runs as two 1-core executors on the one worker. The full cluster's catch-up had failed again with an executor OOM-killed during a gold MERGE.

**Moving to lite without replaying the backfill:** kept the Delta lake and model v1, reset only the checkpoints that referred to the old Kafka (`q1_bronze`) or to the unscored backlog (gold and scoring), and started once with `make stream SKIP_BACKLOG=1`. That flag makes a query with no checkpoint start at its source table's current end. The old stream_metrics rows were cleared because they referred to the old Kafka.

**Demo-sized replays:** `make live` replays a deterministic 15% sample of the held-out flows (`LIVE_PCT`, `--sample-pct`). Long commands run under `caffeinate -i`, because the Mac sleeping mid-run had killed the stream.

**Consistency review (code ↔ config ↔ docs):** every config key, lake table, Kafka topic and SQLite table is used consistently.
- Removed dead outputs: `score_stats` and the `window_stats` SQLite mirror.
- Removed the unused `merge_upsert` parameters.
- `make wait` now orders by time instead of batch id, which resets with a new checkpoint.
- Fixed stale docstrings and docs: silver "MERGE", the Makefile header workflow, port 4041, and the dashboard links.

**Real-cluster run on lite:** a 30,235-flow demo replay was scored end to end with no memory kills, raising 16,872 alerts. Batches were slow (30–160 s) and alerts trailed the replay by a few minutes.

**Bugs found and fixed**
1. The gold MERGE re-scanned the whole gold table every batch. It now adds literal `split IN (...)` (partition pruning) and `window_start >= min` (file skipping) predicates.
2. **Scored rows were silently dropped from Delta `gold_scored`** after a checkpoint reset. The idempotent-write `txnAppId` was the query name, batch ids restarted at 0, and Delta skipped batches 0–9 as "already written". The txnAppId is now name + query id (read from the checkpoint's `metadata`), as Delta recommends. Alerts were unaffected (they went to SQLite and Kafka). New test: `tests/test_lake.py`.

**Verified:** `pytest` → 18 passed. Pushed to branch `kappa-streaming-pipeline`.

**Not yet verified on the cluster:** fixes 1 and 2 (a stream restart and a demo replay are needed), and `make evaluate-live` on the new rows.

---

## Checkpoint 17: Fixes verified on the lite cluster; Delta snapshot starvation fixed (2026-10-28)

Ran the stream against a small live sample (`make live LIVE_PCT=2 SPEEDUP=0`, 4,060 flows) rather than a full rehearsal.

- **The txnAppId fix works.** `gold_scored` went from 19,985 to **24,045** rows: exactly the 4,060 sample flows, starting at scoring batch 10. Under the old scheme, batch 10 would have been skipped as "already written". Alerts went from 27,257 to 29,496.
- **New finding, CPU starvation.** The Spark REST API showed the stages blocking scoring and silver each had **50 tasks**: Delta's default `spark.databricks.delta.snapshotPartitions=50` for rebuilding the transaction-log snapshot. On 2 cores, every Delta-reading micro-batch queued 50 tiny tasks. It is now set to 4 (`spark.delta_snapshot_partitions`). After the restart no stage has more than 8 tasks.
- **Gold MERGE pruning, combined with the snapshot fix.** Host-profile MERGE scan time went from 76–248 s per batch (Oct 20) to 4–27 s, and total MERGE time from 116–362 s to 5–41 s.
- **Batch times now:** scoring 12–23 s (was 48–138 s), gold hosts 7–33 s (was 140–363 s), gold timeline 27–96 s.
- A duplicate stream app (mine) was started by mistake while another was already running. It sat in WAITING with 0 cores and touched no data, and was stopped. The `make stream-bg` guard does not check for an existing stream app.
- `pytest` → 18 passed. The stream is stopped.

**Still open:** a timed demo rehearsal (`make stream` + `make live SPEEDUP=600`), and `make evaluate-live` on the larger live set (now 24,045 flows) so REPORT.md §5.1 can be updated.
