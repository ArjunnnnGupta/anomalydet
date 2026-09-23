#!/usr/bin/env python3
"""
src/dashboard/app.py — Phase 6: Streamlit Dashboard

Tabs:
  1. 📊 Alerts — IF/RF model results from alerts.db
  2. 📡 Live Kafka Feed — real-time messages from the network-flows topic

Run: streamlit run src/dashboard/app.py
"""

import sqlite3
import json
import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime

# ── Config ────────────────────────────────────────────────────────────────────
DB_PATH         = "alerts.db"
KAFKA_BOOTSTRAP = "localhost:9092"
KAFKA_TOPIC     = "network-flows"
REFRESH_SECS    = 15

# Attack type → colour mapping for Kafka feed
ATTACK_COLORS = {
    "Normal Traffic": "🟢",
    "DoS":            "🔴",
    "DDoS":           "🔴",
    "Port Scanning":  "🟡",
    "Brute Force":    "🟠",
    "Web Attacks":    "🟠",
    "Bots":           "🔴",
}

# ── Page setup ────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Flow Anomaly Dashboard",
    page_icon="🛡️",
    layout="wide",
)

st.title("🛡️ Network Flow Anomaly Detection Dashboard")
st.caption(
    "Per-flow IDS scoring via Isolation Forest · "
    "Attack type labels via Random Forest · "
    "Data: CICIDS2017 (Kaggle cleaned)"
)

# ── Tabs ──────────────────────────────────────────────────────────────────────
tab_alerts, tab_kafka = st.tabs(["📊 Alerts & Analysis", "📡 Live Kafka Feed"])

# ══════════════════════════════════════════════════════════════════════════════
# TAB 1 — Alerts & Analysis (existing dashboard)
# ══════════════════════════════════════════════════════════════════════════════
with tab_alerts:

    @st.cache_data(ttl=REFRESH_SECS)
    def load_data():
        try:
            conn = sqlite3.connect(DB_PATH)
            alerts = pd.read_sql_query(
                "SELECT * FROM alerts ORDER BY anomaly_score DESC", conn
            )
            all_flows = pd.read_sql_query(
                "SELECT anomaly_score, if_flag, ground_truth FROM flow_scores", conn
            )
            conn.close()
            return alerts, all_flows
        except Exception:
            return pd.DataFrame(), pd.DataFrame()

    alerts_df, flows_df = load_data()

    if alerts_df.empty:
        st.warning(
            "No data found in alerts.db. "
            "Run Phases 1–5 first, then refresh this page."
        )
    else:
        # Sidebar filters
        st.sidebar.header("Filters")
        all_rf_labels = sorted(alerts_df["rf_label"].dropna().unique().tolist())
        selected_labels = st.sidebar.multiselect(
            "Filter by RF predicted label",
            options=all_rf_labels,
            default=all_rf_labels,
        )
        filtered = alerts_df[alerts_df["rf_label"].isin(selected_labels)] if selected_labels else alerts_df

        # Metric row
        total_flows  = len(flows_df)
        total_alerts = len(alerts_df)
        alert_rate   = total_alerts / total_flows * 100 if total_flows > 0 else 0
        avg_score    = alerts_df["anomaly_score"].mean()

        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Total Flows Scored", f"{total_flows:,}")
        col2.metric("Anomalies Flagged",  f"{total_alerts:,}")
        col3.metric("Flag Rate",          f"{alert_rate:.1f}%")
        col4.metric("Avg Anomaly Score",  f"{avg_score:.4f}")

        st.divider()

        col_left, col_right = st.columns(2)

        with col_left:
            st.subheader("Flagged Flows by Ground-Truth Attack Type")
            counts = (
                filtered["ground_truth"]
                .value_counts()
                .reset_index()
                .rename(columns={"ground_truth": "Attack Type", "count": "Count"})
            )
            fig_bar = px.bar(
                counts, x="Attack Type", y="Count",
                color="Attack Type",
                color_discrete_sequence=px.colors.qualitative.Set2,
            )
            fig_bar.update_layout(showlegend=False, height=380)
            st.plotly_chart(fig_bar, use_container_width=True)

        with col_right:
            st.subheader("Anomaly Score Distribution")
            benign_scores = flows_df[flows_df["if_flag"] == 0]["anomaly_score"]
            attack_scores = flows_df[flows_df["if_flag"] == 1]["anomaly_score"]
            fig_hist = go.Figure()
            fig_hist.add_trace(go.Histogram(
                x=benign_scores, name="Normal", opacity=0.6,
                marker_color="steelblue", nbinsx=80,
            ))
            fig_hist.add_trace(go.Histogram(
                x=attack_scores, name="Anomaly", opacity=0.6,
                marker_color="crimson", nbinsx=80,
            ))
            fig_hist.update_layout(
                barmode="overlay",
                xaxis_title="Anomaly Score (higher = more anomalous)",
                yaxis_title="Flow Count",
                height=380,
                legend=dict(x=0.7, y=0.9),
            )
            st.plotly_chart(fig_hist, use_container_width=True)

        st.divider()

        st.subheader(f"Alert Feed ({len(filtered):,} flows)")
        display_cols = ["flow_index", "anomaly_score", "rf_label", "ground_truth", "scored_at"]
        available    = [c for c in display_cols if c in filtered.columns]
        st.dataframe(
            filtered[available]
            .rename(columns={
                "flow_index":    "Flow ID",
                "anomaly_score": "Anomaly Score",
                "rf_label":      "RF Predicted Label",
                "ground_truth":  "Ground Truth",
                "scored_at":     "Scored At",
            })
            .style.format({"Anomaly Score": "{:.4f}"}),
            use_container_width=True,
            height=420,
        )

        st.caption(
            f"Last loaded: {datetime.now().strftime('%H:%M:%S')} · "
            f"Auto-refreshes every {REFRESH_SECS}s · "
            f"Source: {DB_PATH}"
        )

# ══════════════════════════════════════════════════════════════════════════════
# TAB 2 — Live Kafka Feed
# ══════════════════════════════════════════════════════════════════════════════
with tab_kafka:

    st.subheader("📡 Live Kafka Feed — `network-flows` topic")
    st.caption(
        f"Broker: `{KAFKA_BOOTSTRAP}` · Topic: `{KAFKA_TOPIC}` · "
        "Start the producer in a terminal to see messages flow in."
    )

    # ── Instructions ──────────────────────────────────────────────────────────
    with st.expander("▶ How to start the producer", expanded=False):
        st.code(
            "# In a separate terminal:\n"
            "export JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home\n\n"
            "# If Kafka isn't running yet:\n"
            "make kafka-format && make kafka-start && make kafka-topic\n\n"
            "# Stream 500 flows:\n"
            "make producer",
            language="bash",
        )

    # ── Controls ──────────────────────────────────────────────────────────────
    col_ctrl1, col_ctrl2, col_ctrl3 = st.columns([1, 1, 2])
    max_msgs   = col_ctrl1.number_input("Max messages to fetch", min_value=10, max_value=500, value=50, step=10)
    auto_refresh = col_ctrl2.toggle("Auto-refresh (5s)", value=False)
    fetch_btn  = col_ctrl3.button("🔄 Fetch latest messages", use_container_width=True)

    if auto_refresh:
        st.toast("Auto-refresh is ON — page reloads every 5 seconds", icon="🔄")

    # ── Kafka fetch ───────────────────────────────────────────────────────────
    def fetch_kafka_messages(n: int) -> tuple[list[dict], str]:
        """
        Connects to Kafka, reads up to n messages from the beginning of
        the network-flows topic, and returns them as a list of dicts.
        Returns (messages, error_string). error_string is empty on success.
        """
        try:
            from kafka import KafkaConsumer
            from kafka.errors import NoBrokersAvailable

            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=[KAFKA_BOOTSTRAP],
                auto_offset_reset="earliest",
                enable_auto_commit=False,
                consumer_timeout_ms=3000,          # stop waiting after 3s of no new msgs
                value_deserializer=lambda b: json.loads(b.decode("utf-8")),
            )

            messages = []
            for msg in consumer:
                row = msg.value
                messages.append({
                    "offset":       msg.offset,
                    "timestamp":    datetime.fromtimestamp(msg.timestamp / 1000).strftime("%H:%M:%S"),
                    "Attack Type":  row.get("Attack Type", "Unknown"),
                    "Flow Duration": row.get("Flow Duration", "—"),
                    "Total Fwd Packets": row.get("Total Fwd Packets", "—"),
                    "Total Bwd Packets": row.get("Total Bwd Packets", "—"),
                    "Flow Bytes/s": row.get("Flow Bytes/s", "—"),
                })
                if len(messages) >= n:
                    break

            consumer.close()
            return messages, ""

        except Exception as e:
            err = str(e)
            if "NoBrokersAvailable" in err or "connect" in err.lower():
                return [], "🔴 Kafka broker not reachable at `localhost:9092`. Start the broker first (`make kafka-start`)."
            return [], f"⚠️ Error: {err}"

    # ── Render ────────────────────────────────────────────────────────────────
    if fetch_btn or auto_refresh:
        with st.spinner("Connecting to Kafka..."):
            msgs, err = fetch_kafka_messages(int(max_msgs))

        if err:
            st.error(err)

        elif not msgs:
            st.info(
                "✅ Broker is reachable but the topic has no messages yet. "
                "Run `make producer` to start streaming."
            )

        else:
            # KPI row
            attack_counts = {}
            for m in msgs:
                a = m["Attack Type"]
                attack_counts[a] = attack_counts.get(a, 0) + 1

            normal_count = attack_counts.get("Normal Traffic", 0)
            attack_count = sum(v for k, v in attack_counts.items() if k != "Normal Traffic")

            k1, k2, k3, k4 = st.columns(4)
            k1.metric("Messages fetched",  len(msgs))
            k2.metric("Normal flows",       normal_count)
            k3.metric("Attack flows",       attack_count)
            k4.metric("Unique attack types", len([k for k in attack_counts if k != "Normal Traffic"]))

            st.divider()

            # Attack type breakdown chart
            col_chart, col_feed = st.columns([1, 2])

            with col_chart:
                st.markdown("**Attack type breakdown**")
                df_counts = pd.DataFrame(
                    [{"Attack Type": k, "Count": v} for k, v in sorted(attack_counts.items(), key=lambda x: -x[1])]
                )
                fig = px.bar(
                    df_counts, x="Count", y="Attack Type",
                    orientation="h",
                    color="Attack Type",
                    color_discrete_sequence=px.colors.qualitative.Bold,
                )
                fig.update_layout(showlegend=False, height=300, margin=dict(l=0, r=0, t=10, b=0))
                st.plotly_chart(fig, use_container_width=True)

            with col_feed:
                st.markdown(f"**Last {len(msgs)} messages (newest last)**")
                # Build display rows with emoji indicator
                feed_rows = []
                for m in msgs[-30:]:   # show last 30 in the scrollable feed
                    icon = ATTACK_COLORS.get(m["Attack Type"], "⚪")
                    feed_rows.append({
                        "":              icon,
                        "Offset":        m["offset"],
                        "Time":          m["timestamp"],
                        "Attack Type":   m["Attack Type"],
                        "Flow Duration": f"{m['Flow Duration']:.0f}" if isinstance(m["Flow Duration"], float) else m["Flow Duration"],
                        "Fwd Pkts":      m["Total Fwd Packets"],
                        "Bwd Pkts":      m["Total Bwd Packets"],
                    })
                st.dataframe(
                    pd.DataFrame(feed_rows),
                    use_container_width=True,
                    height=310,
                    hide_index=True,
                )

            st.caption(f"Fetched at {datetime.now().strftime('%H:%M:%S')} from offset 0 → {msgs[-1]['offset']}")

    else:
        st.info("Click **Fetch latest messages** above to connect to Kafka and display live flow data.")

    # ── Architecture diagram ──────────────────────────────────────────────────
    st.divider()
    st.markdown("#### How Kafka fits into the pipeline")
    st.code(
        "clean_parquet/               Kafka Broker (KRaft)         This dashboard\n"
        "  (2.52M flows)                localhost:9092\n"
        "       │                            │                           │\n"
        "       ▼                            │                           │\n"
        " kafka_producer.py  ──JSON──►  network-flows  ◄──fetch──  Tab: Live Feed\n"
        "  50 rows / batch               topic (1 partition)\n"
        "  ~100 rows/sec                 500 messages total\n"
        "       │\n"
        "       ▼\n"
        " spark_consumer.py  ──Parquet sink──►  data/streaming_parquet/",
        language="text",
    )
