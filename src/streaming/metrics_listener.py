"""
src/streaming/metrics_listener.py — StreamingQueryListener → SQLite stream_metrics.

Records, for every micro-batch of every query: input rows, input vs processed
rows/s, batch duration, watermark, state-store size and source end offsets.

Spark's Kafka source tracks offsets in its checkpoint, not in a Kafka consumer
group, so lag tools can't see it. After each bronze batch the listener also
commits the processed offsets to the consumer group `spark-bronze-ingest`, which
makes consumer lag visible in Kafka UI.
"""

import json

from pyspark.sql.streaming import StreamingQueryListener

from src.common.config import cfg, kafka_bootstrap
from src.serving import sqlite_sink


class MetricsListener(StreamingQueryListener):
    def __init__(self):
        super().__init__()
        self._consumer = None

    def onQueryStarted(self, event):
        sqlite_sink.log_event("listener", f"query started: {event.name} ({event.id})")

    def onQueryTerminated(self, event):
        sqlite_sink.log_event("listener", f"query terminated: {event.id} exception={event.exception}")

    def onQueryIdle(self, event):  # Spark 3.5+
        pass

    def onQueryProgress(self, event):
        p = event.progress
        try:
            end_offsets = p.sources[0].endOffset if p.sources else None
            state_rows = sum(op.numRowsTotal for op in p.stateOperators) if p.stateOperators else 0
            sqlite_sink.upsert("stream_metrics", [{
                "query_name": p.name,
                "batch_id": p.batchId,
                "ts": p.timestamp,
                "num_input_rows": p.numInputRows,
                "input_rps": float(p.inputRowsPerSecond or 0.0),
                "processed_rps": float(p.processedRowsPerSecond or 0.0),
                "batch_duration_ms": p.batchDuration,
                "trigger_ms": (p.durationMs or {}).get("triggerExecution"),
                "watermark": (p.eventTime or {}).get("watermark"),
                "state_rows": state_rows,
                "end_offsets": end_offsets if isinstance(end_offsets, str) else json.dumps(end_offsets),
            }])
            if p.name == "q1_bronze" and end_offsets:
                self._commit_offsets(end_offsets)
        except Exception as e:  # noqa: BLE001 — a metrics failure must never kill the stream
            print(f"[metrics_listener] {e}", flush=True)

    def _commit_offsets(self, end_offsets) -> None:
        from confluent_kafka import Consumer, TopicPartition

        offsets = json.loads(end_offsets) if isinstance(end_offsets, str) else end_offsets
        if self._consumer is None:
            self._consumer = Consumer({
                "bootstrap.servers": kafka_bootstrap(),
                "group.id": cfg()["kafka"]["lag_consumer_group"],
                "enable.auto.commit": False,
            })
        tps = [TopicPartition(t, int(part), int(off))
               for t, parts in offsets.items() for part, off in parts.items()]
        self._consumer.commit(offsets=tps, asynchronous=False)
