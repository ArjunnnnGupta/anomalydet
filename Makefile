# Makefile — convenience commands for the anomaly detection pipeline.
# Usage: make <target>
# Requires: Java 17 LTS (export JAVA_HOME before running Spark or Kafka targets)
#   export JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home

VENV      := venv
PYTHON    := $(VENV)/bin/python
PIP       := $(VENV)/bin/pip
JAVA_HOME ?= /opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home
KAFKA_BIN := /opt/homebrew/opt/kafka/bin
KAFKA_CFG := kafka-kraft.properties
KAFKA_LOG := /tmp/kafka-kraft-logs

export JAVA_HOME

.PHONY: all setup test test-spark kafka-format kafka-start kafka-topic kafka-stop \
        clean-data producer consumer preprocess train-if train-rf evaluate \
        write-alerts dashboard report clean

## Full pipeline (no streaming)
all: clean-data preprocess train-if train-rf evaluate write-alerts

## Phase 0 — environment
setup:
	bash scripts/setup.sh

test-spark:
	$(PYTHON) test_spark.py

test:
	$(PYTHON) tests/test_pipeline.py

## Kafka lifecycle (KRaft mode — no ZooKeeper, no Docker required)
kafka-format:
	rm -rf $(KAFKA_LOG)
	JAVA_HOME=$(JAVA_HOME) $(KAFKA_BIN)/kafka-storage format \
		-t $$($(KAFKA_BIN)/kafka-storage random-uuid) \
		-c $(KAFKA_CFG)

kafka-start:
	JAVA_HOME=$(JAVA_HOME) $(KAFKA_BIN)/kafka-server-start $(KAFKA_CFG) \
		> /tmp/kafka.log 2>&1 &
	@echo "Kafka started. Logs at /tmp/kafka.log"
	@sleep 4

kafka-topic:
	JAVA_HOME=$(JAVA_HOME) $(KAFKA_BIN)/kafka-topics \
		--create --topic network-flows --partitions 1 \
		--replication-factor 1 --bootstrap-server localhost:9092

kafka-stop:
	JAVA_HOME=$(JAVA_HOME) $(KAFKA_BIN)/kafka-server-stop || true

## Phase 1 — data cleaning
clean-data:
	$(PYTHON) src/processing/clean_data.py

## Phase 2 — streaming
producer:
	$(PYTHON) src/ingestion/kafka_producer.py --sample 500

consumer:
	$(PYTHON) src/ingestion/spark_consumer.py

## Phase 3 — feature preprocessing
preprocess:
	$(PYTHON) src/processing/preprocess_flows.py

## Phase 4 — ML
train-if:
	$(PYTHON) src/ml/train_isolation_forest.py

train-rf:
	$(PYTHON) src/ml/train_random_forest.py

evaluate:
	$(PYTHON) src/ml/evaluate.py

## Phase 5 — storage
write-alerts:
	$(PYTHON) src/storage/write_alerts.py

## Phase 6 — dashboard
dashboard:
	$(VENV)/bin/streamlit run src/dashboard/app.py

## Report
report:
	$(PYTHON) generate_report_pdf.py

## Full cleanup (preserves raw CSV)
clean:
	rm -rf data/clean_parquet data/preprocessed_parquet data/streaming_parquet \
	       data/checkpoint data/X_scaled.npy data/y_labels.npy \
	       data/if_scores.npy data/if_flags.npy data/rf_predictions.npy data/rf_test_y.npy \
	       data/feature_cols.json models/*.joblib alerts.db
	@echo "Cleaned all generated artifacts."

