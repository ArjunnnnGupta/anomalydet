#!/usr/bin/env python3
"""
src/dashboard/app.py — live operations dashboard for the Kappa pipeline (runs on the host).

Data sources (no Spark needed in this process):
  • SQLite serving cache  (written by the streaming app's foreachBatch + listener)
  • Delta gold tables     (read directly with delta-rs / `deltalake`)
  • Model registry        (meta.json, metrics*.json, plots)
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

from src.common.config import cfg, kafka_bootstrap, lake, replication_factor, topic
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


@st.cache_data(ttl=30)
def delta_table(name: str) -> pd.DataFrame:
    try:
        from deltalake import DeltaTable
        return DeltaTable(lake(name)).to_pandas()
    except Exception:  # noqa: BLE001
        return pd.DataFrame()


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


@st.cache_data(ttl=30)
def delta_info(name: str) -> dict | None:
    try:
        from deltalake import DeltaTable
        dt = DeltaTable(lake(name))
        hist = dt.history(5)
        return {"table": name, "version": dt.version(), "files": len(dt.file_uris()),
                "partitions": ", ".join(dt.metadata().partition_columns) or "—",
                "last_op": hist[0].get("operation") if hist else "—"}
    except Exception:  # noqa: BLE001
        return None


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
st.caption(f"Kafka (RF={replication_factor()}) → Spark Structured Streaming → Delta Lake (bronze/silver/gold) "
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

tab_live, tab_threats, tab_hosts, tab_feed, tab_lake = st.tabs(
    ["📡 Live pipeline", "🚨 Threat timeline", "🖥️ Hosts", "📜 Alert feed", "🗄️ Lakehouse & model"])

# ── 1. Live pipeline ───────────────────────────────────────────────────────────
with tab_live:
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
        c3, c4 = st.columns(2)
        with c3:
            st.subheader("Micro-batch duration")
            st.plotly_chart(px.line(recent, x="ts", y="batch_duration_ms", color="query_name", markers=True,
                                    height=300, labels={"batch_duration_ms": "ms", "ts": ""}),
                            width="stretch")
        with c4:
            st.subheader("Query state")
            st.dataframe(last[["batch_id", "num_input_rows", "processed_rps", "batch_duration_ms",
                               "watermark", "state_rows"]], width="stretch")
        st.caption(f"Charts show the last {RECENT_MINUTES} minutes of recorded micro-batches.")
    ev = q("SELECT ts, source, message FROM pipeline_events ORDER BY ts DESC LIMIT 15")
    if not ev.empty:
        st.subheader("Pipeline events")
        st.dataframe(ev, width="stretch", hide_index=True)

# ── 2. Threat timeline ─────────────────────────────────────────────────────────
# Built from the alerts table (SQLite) and the Delta gold_scored table.
with tab_threats:
    a = q("SELECT substr(event_time, 1, 16) AS minute, predicted_family, COUNT(*) AS alerts, "
          "SUM(CASE WHEN label_family != ? THEN 1 ELSE 0 END) AS true_attacks "
          "FROM alerts GROUP BY 1, 2 ORDER BY 1", (NORMAL,))
    if a.empty:
        st.info("No alerts yet. Start the stream (`make stream`) and replay the held-out flows (`make live`).")
    else:
        t = scored_totals()
        k1, k2, k3, k4 = st.columns(4)
        k1.metric("Live flows scored", f"{t['flows']:,}", help="Rows in Delta gold_scored (every live flow the model scored).")
        k2.metric("Alerts raised", f"{t['alerts']:,}",
                  f"{t['alerts'] / t['flows']:.1%} of flows" if t["flows"] else None, delta_color="off")
        k3.metric("Alerts that were real attacks", f"{t['tp'] / t['alerts']:.1%}" if t["alerts"] else "—",
                  help="Precision: share of alerts whose ground-truth label is an attack.")
        k4.metric("Real attacks caught", f"{t['tp'] / t['attacks']:.1%}" if t["attacks"] else "—",
                  help="Recall: share of the real attacks among the scored flows that raised an alert.")
        st.caption("Headline numbers: Delta gold_scored. Chart: every alert in the serving cache.")
        st.subheader("Alerts per minute by predicted attack family")
        a["minute"] = pd.to_datetime(a["minute"])
        st.plotly_chart(px.bar(a, x="minute", y="alerts", color="predicted_family", height=380,
                               labels={"minute": "", "alerts": "alerts"}), width="stretch")
        reasons = q("SELECT alert_reason, COUNT(*) n FROM alerts GROUP BY 1")
        if not reasons.empty:
            st.subheader("Which model raised the alert")
            st.caption("rf = RandomForest (known attack types) · anomaly = KMeans distance (unusual traffic) · "
                       "rf+anomaly = both")
            st.plotly_chart(px.pie(reasons, names="alert_reason", values="n", height=300, hole=0.5),
                            width="stretch")

# ── 3. Hosts ───────────────────────────────────────────────────────────────────
with tab_hosts:
    c1, c2 = st.columns(2)
    with c1:
        st.subheader("Top alerting source hosts")
        top = q("SELECT src_ip, src_zone, COUNT(*) alerts, COUNT(DISTINCT dst_port) dst_ports, "
                "GROUP_CONCAT(DISTINCT predicted_family) families FROM alerts "
                "GROUP BY src_ip, src_zone ORDER BY alerts DESC LIMIT 15")
        if not top.empty:
            st.dataframe(top, width="stretch", hide_index=True)
        else:
            st.info("No alerts yet.")
    with c2:
        st.subheader("Port-scan candidates")
        st.caption(f"Hosts that contacted more than {cfg()['streaming']['port_scan_distinct_ports']} "
                   "distinct destination ports within one 1-minute window.")
        ps = q("SELECT window_start, split, src_ip, distinct_dst_ports, flows FROM port_scan_candidates "
               "ORDER BY distinct_dst_ports DESC LIMIT 100")
        if not ps.empty:
            st.dataframe(ps, width="stretch", hide_index=True)
        else:
            st.info("None detected yet.")
    hw_df = q("SELECT window_start, src_ip, flows, distinct_dst_ips, distinct_dst_ports, syn_ratio, attack_flows "
              "FROM host_window_stats ORDER BY window_start DESC, flows DESC LIMIT 2000")
    if not hw_df.empty:
        st.subheader("Busiest hosts per 1-minute window")
        st.caption("Each dot is one host in one minute. Top right = many flows to many ports (scan / flood).")
        st.plotly_chart(px.scatter(hw_df, x="distinct_dst_ports", y="flows", color="attack_flows",
                                   hover_data=["src_ip", "window_start"], log_x=True, log_y=True, height=380),
                        width="stretch")

# ── 4. Alert feed ──────────────────────────────────────────────────────────────
with tab_feed:
    fams = q("SELECT DISTINCT predicted_family FROM alerts")
    f1, f2, f3 = st.columns(3)
    pick = f1.multiselect("Predicted family", sorted(fams.predicted_family.dropna()) if not fams.empty else [])
    ip = f2.text_input("Source IP contains")
    minp = f3.slider("Min P(attack)", 0.0, 1.0, 0.0, 0.05)
    sql = ("SELECT event_time, src_ip, dst_ip, dst_port, service, predicted_family, attack_prob, "
           "anomaly_score, alert_reason, label_family AS ground_truth, model_version, flow_uid "
           "FROM alerts WHERE attack_prob >= ?")
    params: list = [minp]
    if pick:
        sql += f" AND predicted_family IN ({','.join('?' * len(pick))})"
        params += pick
    if ip:
        sql += " AND src_ip LIKE ?"
        params.append(f"%{ip}%")
    feed = q(sql + " ORDER BY event_time DESC LIMIT 500", tuple(params))
    if feed.empty:
        st.info("No matching alerts.")
    else:
        feed.insert(6, "correct?", (feed.ground_truth != NORMAL).map({True: "✅", False: "❌"}))
        st.caption(f"{len(feed):,} most recent matching alerts · ✅ = ground truth is an attack")
        st.dataframe(feed.style.format({"attack_prob": "{:.3f}", "anomaly_score": "{:.2f}"}),
                     width="stretch", hide_index=True, height=520)

# ── 5. Lakehouse & model ───────────────────────────────────────────────────────
with tab_lake:
    st.subheader("Delta tables")
    infos = [i for i in (delta_info(t) for t in
             ["bronze_flows", "silver_flows", "silver_quarantine", "gold_attack_timeline",
              "gold_host_profile", "gold_port_scan", "gold_scored", "gold_model_predictions"]) if i]
    if infos:
        st.dataframe(pd.DataFrame(infos), width="stretch", hide_index=True)
        st.caption("version = Delta commits so far (each streaming micro-batch is one ACID commit) · "
                   "many small files are what `make maintenance` (OPTIMIZE) compacts.")
    else:
        st.info("No Delta tables yet.")

    tl = delta_table("gold_attack_timeline")
    if not tl.empty:
        st.subheader("Gold: attack timeline (1-minute tumbling windows, ground truth)")
        split = st.radio("Split", sorted(tl.split.dropna().unique()), horizontal=True)
        tls = tl[tl.split == split].sort_values("window_start")
        st.plotly_chart(px.bar(tls, x="window_start", y="flows", color="label_family", height=380),
                        width="stretch")

    st.subheader(f"Model v{version}" if version else "Model")
    if not version:
        st.info("No model registered yet (`make train`).")
    else:
        held_out = metrics or meta.get("metrics", {})
        sources = {"Held-out test split": held_out}
        if metrics_live:
            sources["Live stream (never trained on)"] = metrics_live
        src_name = st.radio("Metrics on", list(sources), horizontal=True)
        mm = sources[src_name]
        cols = st.columns(5)
        helps = {
            "macro_f1": "Average F1 over attack families, each family weighted equally (rare ones count as much "
                        "as common ones).",
            "rf_attack_prob_auc": "How well the RandomForest's P(attack) ranks attacks above normal traffic "
                                  "(1.0 = perfect).",
            "kmeans_anomaly_auc": "Same, for the unsupervised KMeans distance score.",
            "alert_precision": "Share of alerts that were real attacks.",
            "alert_recall": "Share of real attacks that raised an alert.",
        }
        for c, (lbl, key) in zip(cols, [("Macro-F1", "macro_f1"), ("RF AUC", "rf_attack_prob_auc"),
                                        ("KMeans AUC", "kmeans_anomaly_auc"), ("Alert precision", "alert_precision"),
                                        ("Alert recall", "alert_recall")]):
            c.metric(lbl, f"{mm.get(key, float('nan')):.3f}", help=helps[key])
        n_rows = mm.get("rows") or mm.get("test_rows")
        st.caption((f"Evaluated on {n_rows:,} flows · " if n_rows else "")
                   + f"trained on silver_flows@v{meta.get('silver_version')} · {meta.get('train_rows', 0):,} rows · "
                   f"RF {meta.get('rf_params')} · anomaly threshold {meta.get('anomaly_threshold', 0):.3f}")
        pc = mm.get("per_class") or {}
        if pc:
            per = pd.DataFrame(pc).T.sort_values("support", ascending=False)
            per["support"] = per["support"].astype(int)
            st.dataframe(per.style.format({"precision": "{:.3f}", "recall": "{:.3f}", "f1": "{:.3f}",
                                           "support": "{:,}"}), width="stretch")
        plots = os.path.join(registry.root(), f"v{version}", "plots")
        if os.path.isdir(plots):
            imgs = sorted(os.listdir(plots))
            for c, img in zip(st.columns(max(len(imgs), 1)), imgs):
                c.image(os.path.join(plots, img), caption=img, width="stretch")
        else:
            st.caption("Run `make evaluate` / `make evaluate-live` (with the stream stopped) to generate plots.")

st.caption(f"Rendered {datetime.now():%H:%M:%S} · SQLite {db_path()}")
if auto:
    time.sleep(every)
    st.rerun()
