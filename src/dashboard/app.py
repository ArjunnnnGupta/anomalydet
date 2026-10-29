#!/usr/bin/env python3
"""
src/dashboard/app.py — live operations dashboard for the Kappa pipeline (runs on the host).

Data sources (no Spark needed in this process):
  • SQLite serving cache  (written by the streaming app's foreachBatch + listener)
  • Delta gold_scored     (read directly with delta-rs / `deltalake`)
  • Model registry        (meta.json, metrics*.json)
  • Kafka                 (partition high-watermarks → consumer lag)
  • Spark master REST     (is the streaming app running?)

Run:  make dashboard   →  http://localhost:8501
"""

import json
import os
import sqlite3
import time
import urllib.request
from datetime import datetime, timezone

import pandas as pd
import plotly.express as px
import streamlit as st

from src.common.config import cfg, kafka_bootstrap, lake, topic
from src.ml import registry
from src.serving.sqlite_sink import db_path

st.set_page_config(page_title="Network Anomaly Platform", page_icon="🛡️", layout="wide")

NORMAL = cfg()["ml"]["normal_label"]
RECENT_MINUTES = 30          # live charts show only the most recent activity


# ── Data access ────────────────────────────────────────────────────────────────
def q(sql: str, params: tuple = ()) -> pd.DataFrame:
    p = db_path()
    if not os.path.exists(p):
        return pd.DataFrame()
    try:
        with sqlite3.connect(f"file:{p}?mode=ro", uri=True, timeout=5) as c:
            return pd.read_sql_query(sql, c, params=params)
    except Exception:  # noqa: BLE001 — table may not exist yet
        return pd.DataFrame()


@st.cache_data(ttl=5)
def stream_running() -> bool | None:
    """True/False from the Spark master's REST API; None if the master is unreachable."""
    try:
        with urllib.request.urlopen("http://localhost:8081/json/", timeout=2) as r:
            return "kappa-stream-app" in [a["name"] for a in json.load(r)["activeapps"]]
    except Exception:  # noqa: BLE001
        return None


@st.cache_data(ttl=5)
def kafka_watermarks() -> tuple[dict, str]:
    try:
        from confluent_kafka import Consumer, TopicPartition
        c = Consumer({"bootstrap.servers": kafka_bootstrap(), "group.id": "dashboard-probe",
                      "enable.auto.commit": False, "socket.timeout.ms": 3000})
        md = c.list_topics(topic("raw"), timeout=3)
        parts = md.topics[topic("raw")].partitions
        hw = {p: c.get_watermark_offsets(TopicPartition(topic("raw"), p), timeout=3)[1] for p in parts}
        c.close()
        return hw, ""
    except Exception as e:  # noqa: BLE001
        return {}, str(e)


@st.cache_data(ttl=15)
def scored_totals() -> dict:
    """Headline numbers from ONE source, Delta gold_scored, so they always agree with each other
    (and with `make evaluate-live`): flows scored, alerts, true positives, real attacks."""
    try:
        from deltalake import DeltaTable
        t = DeltaTable(lake("gold_scored")).to_pyarrow_table(columns=["is_alert", "is_attack"]).to_pydict()
        alert, attack = t["is_alert"], t["is_attack"]
        return {"flows": len(alert), "alerts": sum(1 for x in alert if x),
                "tp": sum(1 for x, y in zip(alert, attack) if x and y), "attacks": sum(1 for y in attack if y)}
    except Exception:  # noqa: BLE001
        return {"flows": 0, "alerts": 0, "tp": 0, "attacks": 0}


def latest_model() -> tuple[int | None, dict, dict, dict]:
    v = registry.latest_version()
    if v is None:
        return None, {}, {}, {}
    d = os.path.join(registry.root(), f"v{v}")

    def load(f):
        p = os.path.join(d, f)
        return json.load(open(p)) if os.path.exists(p) else {}
    return v, load("meta.json"), load("metrics.json"), load("metrics_live.json")


def ago(ts: pd.Timestamp) -> str:
    s = (datetime.now(timezone.utc) - ts.to_pydatetime()).total_seconds()
    if s < 90:
        return f"{s:.0f}s ago"
    if s < 5400:
        return f"{s / 60:.0f} min ago"
    if s < 172800:
        return f"{s / 3600:.0f} h ago"
    return f"{s / 86400:.0f} days ago"


# ── Header + sidebar ───────────────────────────────────────────────────────────
st.title("🛡️ Network Anomaly Platform")
st.caption("Kafka → Spark Structured Streaming → Delta Lake (bronze/silver/gold) "
           "→ MLlib RandomForest + KMeans → real-time alerts · CICIDS2017")

running = stream_running()
version, meta, metrics, metrics_live = latest_model()

with st.sidebar:
    st.header("Controls")
    auto = st.toggle("Auto-refresh", value=os.environ.get("DASHBOARD_AUTOREFRESH", "1") == "1")
    every = st.slider("Refresh every (s)", 3, 60, 5)
    st.divider()
    st.markdown("**Streaming app:** " + {True: "🟢 running", False: "🔴 stopped",
                                         None: "⚪ Spark master unreachable"}[running])
    st.metric("Serving model", f"v{version}" if version else "none yet")
    st.divider()
    st.markdown("**Cluster UIs**")
    st.markdown("- [Kafka UI](http://localhost:8080)\n- [Spark master](http://localhost:8081)\n"
                "- [Running Spark app](http://localhost:4040) (stream, or a batch job)\n"
                "- [Streaming queries](http://localhost:4040/StructuredStreaming/)")

tab_pipe, tab_alerts, tab_model = st.tabs(["📡 Pipeline", "🚨 Alerts", "🧠 Model & hosts"])

# ── 1. Pipeline: is data flowing and is the stream keeping up? ─────────────────
with tab_pipe:
    m = q("SELECT * FROM stream_metrics ORDER BY ts DESC LIMIT 5000")
    if running is False:
        st.warning("The streaming app is not running, so the numbers below are from its last run. "
                   "Start it with `make stream`.")
    if m.empty:
        st.info("No streaming progress yet. Start the stream (`make stream`) and replay data (`make live`).")
    else:
        m["ts"] = pd.to_datetime(m["ts"], utc=True, format="ISO8601")
        last = m.sort_values("ts").groupby("query_name").tail(1).set_index("query_name").sort_index()
        cols = st.columns(len(last))
        for col, (name, r) in zip(cols, last.iterrows()):
            col.metric(name, f"{r['processed_rps']:,.0f} rows/s",
                       f"batch {int(r['batch_id'])} · {ago(r['ts'])}", delta_color="off",
                       help="Rows processed per second in this query's latest micro-batch.")

        hw, err = kafka_watermarks()
        bronze = m[m.query_name == "q1_bronze"].sort_values("ts")
        recent = m[m.ts >= m.ts.max() - pd.Timedelta(minutes=RECENT_MINUTES)].sort_values("ts")
        c1, c2 = st.columns(2)
        with c1:
            st.subheader("Kafka consumer lag (flows.raw)")
            if err:
                st.warning(f"Kafka not reachable from the dashboard: {err}")
            elif not bronze.empty and bronze.iloc[-1]["end_offsets"] not in (None, "null"):
                consumed = json.loads(bronze.iloc[-1]["end_offsets"]).get(topic("raw"), {})
                lag = pd.DataFrame([{"partition": p, "lag": hw[p] - int(consumed.get(str(p), 0))}
                                    for p in sorted(hw)])
                st.metric("Total lag (messages)", f"{int(lag['lag'].sum()):,}",
                          help="Messages in Kafka that the bronze query has not read yet.")
                st.plotly_chart(px.bar(lag, x="partition", y="lag", height=260), width="stretch")
            else:
                st.info("Bronze query hasn't consumed anything yet.")
        with c2:
            st.subheader("Throughput per query")
            st.plotly_chart(px.line(recent, x="ts", y="processed_rps", color="query_name", markers=True,
                                    height=320, labels={"processed_rps": "rows/s", "ts": ""}),
                            width="stretch")
        st.caption(f"Charts show the last {RECENT_MINUTES} minutes of recorded micro-batches.")

# ── 2. Alerts: what was detected, and was it right? ────────────────────────────
with tab_alerts:
    a = q("SELECT substr(event_time, 1, 16) AS minute, predicted_family, COUNT(*) AS alerts "
          "FROM alerts GROUP BY 1, 2 ORDER BY 1")
    if a.empty:
        st.info("No alerts yet. Start the stream (`make stream`) and replay the held-out flows (`make live`). "
                "The first scoring batch takes a minute or two.")
    else:
        t = scored_totals()
        k1, k2, k3, k4 = st.columns(4)
        k1.metric("Live flows scored", f"{t['flows']:,}", help="Every live flow the model has judged.")
        k2.metric("Alerts raised", f"{t['alerts']:,}",
                  f"{t['alerts'] / t['flows']:.1%} of flows" if t["flows"] else None, delta_color="off")
        k3.metric("Alerts that were real attacks", f"{t['tp'] / t['alerts']:.1%}" if t["alerts"] else "—",
                  help="Precision: share of alerts whose true label is an attack.")
        k4.metric("Real attacks caught", f"{t['tp'] / t['attacks']:.1%}" if t["attacks"] else "—",
                  help="Recall: share of the real attacks that raised an alert.")
        st.subheader("Alerts per minute by predicted attack family")
        a["minute"] = pd.to_datetime(a["minute"])
        st.plotly_chart(px.bar(a, x="minute", y="alerts", color="predicted_family", height=360,
                               labels={"minute": "", "alerts": "alerts"}), width="stretch")

        st.subheader("Latest alerts")
        fams = sorted(a.predicted_family.dropna().unique())
        pick = st.multiselect("Predicted family", fams)
        sql = ("SELECT event_time, src_ip, dst_ip, dst_port, service, predicted_family, attack_prob, "
               "alert_reason, label_family AS ground_truth FROM alerts")
        params: tuple = ()
        if pick:
            sql += f" WHERE predicted_family IN ({','.join('?' * len(pick))})"
            params = tuple(pick)
        feed = q(sql + " ORDER BY event_time DESC LIMIT 300", params)
        feed.insert(6, "correct?", (feed.ground_truth != NORMAL).map({True: "✅", False: "❌"}))
        st.caption("✅ = the true label is an attack (the alert was right) · ❌ = it was normal traffic")
        st.dataframe(feed.style.format({"attack_prob": "{:.3f}"}), width="stretch", hide_index=True, height=420)

# ── 3. Model & hosts: how good is the model, and who is attacking? ────────────
with tab_model:
    st.subheader(f"Model v{version}" if version else "Model")
    if not version:
        st.info("No model registered yet (`make train`).")
    else:
        sources = {"Held-out test split": metrics or meta.get("metrics", {})}
        if metrics_live:
            sources["Live stream (never trained on)"] = metrics_live
        mm = sources[st.radio("Metrics on", list(sources), horizontal=True)]
        helps = {
            "macro_f1": "Average F1 over the 8 attack families, each counted equally.",
            "rf_attack_prob_auc": "How well the RandomForest separates attacks from normal traffic (1.0 = perfect).",
            "kmeans_anomaly_auc": "The same, for the KMeans distance score.",
            "alert_precision": "Share of alerts that were real attacks.",
            "alert_recall": "Share of real attacks that raised an alert.",
        }
        for c, (lbl, key) in zip(st.columns(5), [("Macro-F1", "macro_f1"), ("RF AUC", "rf_attack_prob_auc"),
                                                 ("KMeans AUC", "kmeans_anomaly_auc"),
                                                 ("Alert precision", "alert_precision"),
                                                 ("Alert recall", "alert_recall")]):
            c.metric(lbl, f"{mm.get(key, float('nan')):.3f}", help=helps[key])
        pc = mm.get("per_class") or {}
        if pc:
            per = pd.DataFrame(pc).T.sort_values("support", ascending=False)
            per["support"] = per["support"].astype(int)
            st.dataframe(per.style.format({"precision": "{:.3f}", "recall": "{:.3f}", "f1": "{:.3f}",
                                           "support": "{:,}"}), width="stretch")
        st.caption(f"Trained on silver_flows@v{meta.get('silver_version')} · {meta.get('train_rows', 0):,} rows · "
                   f"RF {meta.get('rf_params')} · anomaly threshold {meta.get('anomaly_threshold', 0):.2f}")

    c1, c2 = st.columns(2)
    with c1:
        st.subheader("Port-scan suspects")
        st.caption(f"Hosts that touched more than {cfg()['streaming']['port_scan_distinct_ports']} "
                   "different ports in one minute (counting in the gold layer, no ML).")
        ps = q("SELECT src_ip, MAX(distinct_dst_ports) AS max_ports, COUNT(*) AS windows FROM port_scan_candidates "
               "GROUP BY src_ip ORDER BY max_ports DESC LIMIT 20")
        if not ps.empty:
            st.dataframe(ps, width="stretch", hide_index=True)
        else:
            st.info("None detected yet.")
    with c2:
        st.subheader("Top alerting hosts")
        top = q("SELECT src_ip, src_zone, COUNT(*) alerts, GROUP_CONCAT(DISTINCT predicted_family) families "
                "FROM alerts GROUP BY src_ip, src_zone ORDER BY alerts DESC LIMIT 15")
        if not top.empty:
            st.dataframe(top, width="stretch", hide_index=True)
        else:
            st.info("No alerts yet.")

st.caption(f"Rendered {datetime.now():%H:%M:%S} · SQLite {db_path()}")
if auto:
    time.sleep(every)
    st.rerun()
