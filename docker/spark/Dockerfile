# docker/spark/Dockerfile — Spark 3.5.3 standalone image (master, workers, drivers).
# Kafka connector + Delta Lake jars are baked in, so jobs never resolve packages at runtime.
FROM apache/spark:3.5.3-python3

USER root

ARG MAVEN=https://repo1.maven.org/maven2
RUN cd /opt/spark/jars && \
    curl -fsSLO ${MAVEN}/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.3/spark-sql-kafka-0-10_2.12-3.5.3.jar && \
    curl -fsSLO ${MAVEN}/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.3/spark-token-provider-kafka-0-10_2.12-3.5.3.jar && \
    curl -fsSLO ${MAVEN}/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar && \
    curl -fsSLO ${MAVEN}/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar && \
    curl -fsSLO ${MAVEN}/io/delta/delta-spark_2.12/3.2.1/delta-spark_2.12-3.2.1.jar && \
    curl -fsSLO ${MAVEN}/io/delta/delta-storage/3.2.1/delta-storage-3.2.1.jar

# Python deps for drivers AND executors (pandas/pyarrow: Arrow pandas UDFs).
# The base image is Ubuntu 20.04 with Python 3.8, so pins are the last releases that support 3.8
# (the project code uses `from __future__ import annotations` to stay 3.8-compatible).
# pip 20.0.2 (Ubuntu 20.04) can't see manylinux_2_28 wheels, so upgrade it first and
# call it as `python3 -m pip` (a plain `pip` can stay hashed to the old one in the same shell).
RUN python3 -m pip install --no-cache-dir --upgrade pip && \
    python3 -m pip install --no-cache-dir \
        numpy==1.24.4 pandas==2.0.3 pyarrow==17.0.0 pyyaml==6.0.2 \
        confluent-kafka==2.5.3 matplotlib==3.7.5 importlib-metadata && \
    python3 -m pip install --no-cache-dir --no-deps delta-spark==3.2.1

ENV APP_HOME=/opt/app \
    PYTHONPATH=/opt/app \
    RUNNING_IN_DOCKER=1 \
    PYSPARK_PYTHON=python3 \
    PATH=/opt/spark/bin:/opt/spark/sbin:$PATH

WORKDIR /opt/app
