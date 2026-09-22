#!/usr/bin/env python3
"""
src/dashboard/app.py — Phase 6: Streamlit Dashboard

Live-refreshing dashboard showing:
  1. Alert summary: count of flagged flows by ground-truth attack type
  2. Anomaly score distribution: histogram of IF scores (flagged vs normal)
  3. Alert feed table: sortable table of recent alerts with score, RF label, ground truth
  4. Filter by predicted attack type (RF label)

Run: streamlit run src/dashboard/app.py
"""

import sqlite3
import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime

# ── Config ────────────────────────────────────────────────────────────────────
DB_PATH         = "alerts.db"
REFRESH_SECS    = 15         # auto-refresh interval

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

# ── Data loading ──────────────────────────────────────────────────────────────
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
    except Exception as e:
        return pd.DataFrame(), pd.DataFrame()

alerts_df, flows_df = load_data()

if alerts_df.empty:
    st.warning(
        "No data found in alerts.db. "
        "Run Phases 1–5 first, then refresh this page."
    )
    st.stop()

# ── Sidebar filters ───────────────────────────────────────────────────────────
st.sidebar.header("Filters")
all_rf_labels = sorted(alerts_df["rf_label"].dropna().unique().tolist())
selected_labels = st.sidebar.multiselect(
    "Filter by RF predicted label",
    options=all_rf_labels,
    default=all_rf_labels,
)
if selected_labels:
    filtered = alerts_df[alerts_df["rf_label"].isin(selected_labels)]
else:
    filtered = alerts_df

# ── Metric row ────────────────────────────────────────────────────────────────
total_flows    = len(flows_df)
total_alerts   = len(alerts_df)
alert_rate     = total_alerts / total_flows * 100 if total_flows > 0 else 0
avg_score      = alerts_df["anomaly_score"].mean()

col1, col2, col3, col4 = st.columns(4)
col1.metric("Total Flows Scored", f"{total_flows:,}")
col2.metric("Anomalies Flagged", f"{total_alerts:,}")
col3.metric("Flag Rate", f"{alert_rate:.1f}%")
col4.metric("Avg Anomaly Score", f"{avg_score:.4f}")

st.divider()

# ── Row 1: attack type distribution + score histogram ─────────────────────────
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

# ── Row 2: Alert feed table ───────────────────────────────────────────────────
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

# ── Footer ────────────────────────────────────────────────────────────────────
st.caption(
    f"Last loaded: {datetime.now().strftime('%H:%M:%S')} · "
    f"Auto-refreshes every {REFRESH_SECS}s · "
    f"Source: {DB_PATH}"
)

