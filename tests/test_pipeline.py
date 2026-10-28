"""
End-to-end pipeline test on synthetic flows, without Kafka:

    bronze (Delta, as Q1 would write it)
      → Q2 silver  (real streaming query, availableNow)
      → Q3 gold    (real streaming queries)
      → MLlib train → registry v1
      → Q4 scoring (real streaming query) → gold_scored + SQLite
    plus idempotency (replayed micro-batches) and model hot-reload.

Runs once (module fixture); each test asserts on one part of the result.
"""

import datetime
import json
import os
import sqlite3
import zlib

import pytest
from pyspark.sql import functions as F

from tests.synthetic import make_rows, payloads


@pytest.fixture(scope="module")
def run(spark, app_home):
    os.environ["STREAM_AVAILABLE_NOW"] = "1"
    from src.common.config import lake
    from src.ml import registry
    from src.ml.train_pipeline import train
    from src.serving import sqlite_sink
    from src.streaming import gold, scoring, silver
    from src.streaming.bronze import BRONZE_SCHEMA
    from src.ingestion.replay_producer import bad_records

    from src.streaming.metrics_listener import MetricsListener
    spark.streams.addListener(MetricsListener())
    out = {}
    rows = make_rows()
    vals = payloads(rows, split_of=lambda r: "live" if zlib.crc32(r["flow_id"].encode()) % 4 == 0 else "backfill")
    vals += [v.decode() for v in bad_records(3)]
    now = datetime.datetime.now()
    bronze_df = spark.createDataFrame(
        [("k", v, "flows.raw", i % 3, i, now, now, now.date()) for i, v in enumerate(vals)], BRONZE_SCHEMA)
    out["n_good"] = len(rows)
    out["n_live"] = sum(json.loads(v).get("split") == "live" for v in vals[:len(rows)])

    silver.ensure_tables(spark)
    gold.ensure_tables(spark)
    scoring.ensure_tables(spark)
    bronze_df.write.format("delta").mode("append").save(lake("bronze_flows"))

    # Q2 silver
    silver.start(spark).awaitTermination()
    count = lambda t: spark.read.format("delta").load(lake(t)).count()  # noqa: E731
    out["silver"] = count("silver_flows")
    out["quarantine"] = count("silver_quarantine")
    out["dq_reasons"] = {r["dq_reason"] for r in spark.read.format("delta").load(lake("silver_quarantine")).collect()}

    # Replay the same micro-batch (batch_id 0): MERGE + txn-guarded append must not duplicate
    replay = silver.enrich(silver.parse(bronze_df), spark)
    silver.write_batch(replay, 0)
    out["silver_after_replay"] = count("silver_flows")
    out["quarantine_after_replay"] = count("silver_quarantine")

    # Q3 gold
    for q in gold.start(spark):
        q.awaitTermination()
    out["timeline_flows"] = spark.read.format("delta").load(lake("gold_attack_timeline")) \
        .agg(F.sum("flows")).collect()[0][0]
    out["port_scans"] = count("gold_port_scan")

    # Q4 before any model: batch is skipped, nothing written
    scoring.write_batch(spark.read.format("delta").load(lake("silver_flows")).limit(5), 999)
    out["scored_before_model"] = count("gold_scored")

    # Train + register v1, then Q4
    out["v1"] = train(spark)
    scoring.start(spark).awaitTermination()
    out["scored"] = count("gold_scored")
    gs = spark.read.format("delta").load(lake("gold_scored"))
    out["scored_alerts"] = gs.filter("is_alert").count()
    out["scored_versions"] = {r[0] for r in gs.select("model_version").distinct().collect()}

    db = sqlite_sink.db_path()
    with sqlite3.connect(db) as c:
        out["sqlite_alerts"] = c.execute("SELECT COUNT(*) FROM alerts").fetchone()[0]
        out["window_rows"] = c.execute("SELECT COUNT(*) FROM host_window_stats").fetchone()[0]
        out["metric_queries"] = {r[0] for r in c.execute("SELECT DISTINCT query_name FROM stream_metrics")}

    # Hot reload: a new version appears, the loader swaps without restart
    loader = registry.HotLoader()
    out["loaded_before"] = loader.get().version
    out["v2"] = train(spark)
    out["loaded_after"] = loader.get().version

    # Replay a scoring batch with the same batch id → SQLite + Delta stay deduplicated
    live = spark.read.format("delta").load(lake("silver_flows")).filter("split = 'live'")
    scoring.write_batch(live, 0)
    with sqlite3.connect(db) as c:
        out["sqlite_dupes"] = c.execute("SELECT COUNT(*) - COUNT(DISTINCT flow_uid) FROM alerts").fetchone()[0]
    out["scored_after_replay"] = count("gold_scored")
    out["metrics"] = json.load(open(os.path.join(registry.root(), f"v{out['v1']}", "meta.json")))["metrics"]
    os.environ.pop("STREAM_AVAILABLE_NOW")
    return out


def test_silver_keeps_good_rows_and_quarantines_bad(run):
    assert run["silver"] == run["n_good"]
    assert run["quarantine"] == 3
    assert run["dq_reasons"] == {"unparseable_payload", "bad_timestamp", "negative_duration"}


def test_replayed_batch_is_idempotent(run):
    assert run["silver_after_replay"] == run["silver"]
    assert run["quarantine_after_replay"] == run["quarantine"]


def test_gold_windows_cover_all_flows_and_find_the_scan(run):
    assert run["timeline_flows"] == run["n_good"]
    assert run["port_scans"] >= 1
    assert run["window_rows"] > 0


def test_scoring_waits_for_a_model(run):
    assert run["scored_before_model"] == 0


def test_training_registers_a_model_with_metrics(run):
    assert run["v1"] == 1
    assert 0.0 <= run["metrics"]["macro_f1"] <= 1.0
    assert run["metrics"]["rf_attack_prob_auc"] > 0.8     # synthetic families are separable


def test_live_split_is_scored_into_delta_and_sqlite(run):
    assert run["scored"] == run["n_live"]
    assert run["scored_versions"] == {1}
    assert run["sqlite_alerts"] == run["scored_alerts"] > 0


def test_hot_reload_and_idempotent_scoring_replay(run):
    assert (run["loaded_before"], run["loaded_after"]) == (1, 2)
    assert run["scored_after_replay"] == run["scored"]       # same txnVersion → skipped
    assert run["sqlite_dupes"] == 0                           # INSERT OR REPLACE on flow_uid


def test_evaluate_writes_metrics_and_plots(run, spark):
    from src.ml import registry
    from src.ml.evaluate import evaluate
    m = evaluate(spark, 1, live=True)
    assert m["source"] == "live_stream" and m["rows"] > 0
    d = os.path.join(registry.root(), "v1")
    assert os.path.exists(os.path.join(d, "metrics_live.json"))
    assert os.path.exists(os.path.join(d, "plots", "confusion_matrix_live.png"))


def test_maintenance_optimize_history_time_travel(run, spark, capsys):
    from src.lakehouse.maintenance import run as maintain
    maintain(spark)
    out = capsys.readouterr().out
    assert "ZORDER BY (src_ip)" in out and "VERSION AS OF 0" in out and "attack_share" in out


def test_listener_records_progress_for_every_query(run):
    assert {"q2_silver", "q3a_gold_timeline", "q3b_gold_hosts", "q4_scoring"} <= run["metric_queries"]
