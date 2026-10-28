# Makefile — Kappa-architecture anomaly-detection pipeline.
#
# Spark jobs run INSIDE the cluster (docker compose exec spark-master spark-submit ...).
# The producer, topic admin, dashboard and tests run on the HOST from ./venv.
#
#   make setup build cluster-up topics      # once
#   make stream                             # terminal 1: always-on streaming app
#   make backfill && make wait              # terminal 2: build the lake
#   (Ctrl+C the stream)  make train         # batch jobs run with the stream stopped
#   make stream  /  make live               # terminal 1 / 2: real-time scoring of a held-out sample
#   make dashboard                          # terminal 3: http://localhost:8501
#
# Default is the LITE profile (1 Kafka broker, 1 Spark worker), sized for an 8 GB Mac.
# LITE=0 (e.g. `make cluster-up LITE=0`) switches every target to the 3-broker / 2-worker cluster.

VENV    := venv
PY      := $(VENV)/bin/python
LITE    ?= 1

ifeq ($(LITE),1)
  COMPOSE := docker compose -f docker/docker-compose.lite.yml
  export KAFKA_RF := 1
  export KAFKA_BOOTSTRAP := localhost:9092
else
  COMPOSE := docker compose -f docker/docker-compose.yml
endif

# SKIP_BACKLOG=1: queries with no checkpoint start at their source table's current end (see src/common/lake.py)
SKIP_ENV = $(if $(SKIP_BACKLOG),-e STREAM_SKIP_BACKLOG=1)
SUBMIT = $(CAFF) $(COMPOSE) exec $(SKIP_ENV) $(1) spark-master spark-submit --master spark://spark-master:7077
# Batch jobs refuse to start while the streaming app runs: in 6 GB there is no room for two Spark
# drivers (running evaluate next to the stream got the driver container OOM-killed).
NO_STREAM = @curl -s http://localhost:8081/json/ | python3 -c "import json,sys; \
  apps=[a['name'] for a in json.load(sys.stdin)['activeapps']]; \
  sys.exit('\n  The streaming app is running. Stop it first (Ctrl+C in its terminal), then rerun.\n  (6 GB has no room for two Spark drivers.)\n' if 'kappa-stream-app' in apps else 0)"
SPEEDUP    ?= 300
# Live demo replays a 15% sample of the held-out flows (~30K, a few minutes)
LIVE_PCT   ?= 15
# Keep the Mac awake while long pipeline commands run (a sleep killed the stream once)
CAFF := $(shell command -v caffeinate >/dev/null 2>&1 && echo caffeinate -i)
INJECT_BAD ?= 50
ARGS       ?=

.PHONY: help setup test build cluster-up cluster-down cluster-ps logs topics topics-describe \
        stream stream-bg stream-logs stream-catchup produce backfill live inject-bad wait train evaluate \
        evaluate-live maintenance dashboard report report-da2 reset

help:
	@grep -E '^## ' Makefile | sed 's/## //'

## setup            create ./venv with host deps (producer, dashboard, tests)
setup:
	python3 -m venv $(VENV) && $(VENV)/bin/pip install -U pip && $(VENV)/bin/pip install -r requirements.txt

## test             pytest suite (local Spark, no Docker/Kafka needed)
test:
	$(PY) -m pytest tests/ -q -p no:warnings

# ── Cluster ────────────────────────────────────────────────────────────────────
## build            build the Spark image (Kafka + Delta jars baked in)
build:
	$(COMPOSE) build

## cluster-up       start Kafka brokers, Kafka UI, Spark master + workers
cluster-up:
	$(COMPOSE) up -d
	@echo "Kafka UI      http://localhost:8080"
	@echo "Spark master  http://localhost:8081"

cluster-down:
	$(COMPOSE) down

cluster-ps:
	$(COMPOSE) ps

logs:
	$(COMPOSE) logs -f --tail=100

## topics           create Kafka topics from conf/pipeline.yaml and show leaders/ISR
topics:
	$(PY) -m src.ingestion.create_topics $(ARGS)

topics-describe:
	$(PY) -m src.ingestion.create_topics --describe

# ── Streaming (Kappa) ─────────────────────────────────────────────────────────
## stream           run the always-on streaming app (Q1–Q4) in the foreground
stream:
	$(call SUBMIT,) --driver-memory 768m /opt/app/src/streaming/stream_app.py $(ARGS)

## stream-bg        same, detached; output in data/logs/stream_app.log (make stream-logs)
stream-bg:
	mkdir -p data/logs
	$(COMPOSE) exec -d $(SKIP_ENV) spark-master sh -c "spark-submit --master spark://spark-master:7077 \
	  --driver-memory 768m /opt/app/src/streaming/stream_app.py $(ARGS) > /opt/app/data/logs/stream_app.log 2>&1"
	@echo "Streaming app started in the background → data/logs/stream_app.log"

stream-logs:
	tail -f data/logs/stream_app.log

## stream-catchup   drain everything available stage by stage, then exit
stream-catchup:
	$(call SUBMIT,) --driver-memory 768m /opt/app/src/streaming/stream_app.py --available-now

## backfill         replay the ~80% backfill split at full speed (historical timestamps)
backfill:
	$(CAFF) $(PY) -m src.ingestion.replay_producer --split backfill $(ARGS)

## live             replay a LIVE_PCT% sample of the held-out flows at SPEEDUP× event time (LIVE_PCT=100: all)
live:
	$(CAFF) $(PY) -m src.ingestion.replay_producer --split live --sample-pct $(LIVE_PCT) --speedup $(SPEEDUP) $(ARGS)

## inject-bad       send INJECT_BAD malformed records (→ quarantine + flows.dlq)
inject-bad:
	$(PY) -m src.ingestion.replay_producer --inject-bad $(INJECT_BAD) --bad-only

## wait             block until the stream has consumed every Kafka message and silver is idle
wait:
	$(CAFF) $(PY) -m src.ingestion.wait_caught_up

# ── ML + lakehouse ops ────────────────────────────────────────────────────────
## train            MLlib training on silver (backfill split) → registers vN (stream must be stopped)
train:
	$(NO_STREAM)
	$(call SUBMIT,) --driver-memory 1g /opt/app/src/ml/train_pipeline.py $(ARGS)

## evaluate         metrics + plots for the latest model on its held-out test split
evaluate:
	$(NO_STREAM)
	$(call SUBMIT,) --driver-memory 1g /opt/app/src/ml/evaluate.py $(ARGS)

## evaluate-live    same, on the live stream's scored output
evaluate-live:
	$(NO_STREAM)
	$(call SUBMIT,) --driver-memory 1g /opt/app/src/ml/evaluate.py --live $(ARGS)

## maintenance      OPTIMIZE/ZORDER, DESCRIBE HISTORY, time travel, schema evolution, VACUUM
maintenance:
	$(NO_STREAM)
	$(call SUBMIT,) --driver-memory 1g /opt/app/src/lakehouse/maintenance.py $(ARGS)

## dashboard        Streamlit on http://localhost:8501
dashboard:
	PYTHONPATH=. $(VENV)/bin/streamlit run src/dashboard/app.py

## report           build Digital_Assignment_3_Report.pdf from REPORT.md (+ evaluation plots)
report:
	$(PY) generate_report_da3_pdf.py

## report-da2       regenerate the old Review-2 PDF (hardcoded text)
report-da2:
	$(PY) generate_report_pdf.py


## reset            wipe the lake, checkpoints, registry and SQLite, and recreate the topics
reset:
	@read -p "Delete data/lake, data/checkpoints, data/models, data/serving and all Kafka data? [y/N] " a; \
	  [ "$$a" = "y" ] || exit 1
	rm -rf data/lake data/checkpoints data/models data/serving data/spark-events
	$(PY) -m src.ingestion.create_topics --recreate
