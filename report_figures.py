#!/usr/bin/env python3
"""
report_figures.py — draws the DA3 report figures from the real pipeline outputs.

Reads the Delta tables with delta-rs (no Spark needed), the SQLite serving cache and the
registered model's metrics, and writes PNGs to data/report_figures/. Called by
generate_report_da3_pdf.py, so the figures always match the current lake.
"""

import ast
import json
import os
import sqlite3

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import pandas as pd  # noqa: E402
from deltalake import DeltaTable  # noqa: E402
from matplotlib.patches import FancyBboxPatch  # noqa: E402

OUT = "data/report_figures"
LAKE = "data/lake"
REGISTRY = "data/models/registry"
DB = "data/serving/alerts.db"

BG, GRID, FG, SUB = "#0e1117", "#2a2f3a", "#fafafa", "#a3a8b8"
BLUE, DARK_BLUE, RED = "#83c9ff", "#0068c9", "#ff8080"


def _delta(name, columns=None) -> pd.DataFrame:
    return DeltaTable(os.path.join(LAKE, name)).to_pandas(columns=columns)


def _model():
    version = json.load(open(os.path.join(REGISTRY, "latest.json")))["version"]
    meta = json.load(open(os.path.join(REGISTRY, f"v{version}", "meta.json")))
    metrics = json.load(open(os.path.join(REGISTRY, f"v{version}", "metrics.json")))
    return version, meta, metrics


def _axes(title, subtitle, h=4.2):
    fig, ax = plt.subplots(figsize=(10, h), dpi=200)
    fig.patch.set_facecolor(BG)
    ax.set_facecolor(BG)
    for s in ax.spines.values():
        s.set_visible(False)
    ax.tick_params(colors=SUB, labelsize=8, length=0)
    ax.grid(color=GRID, linewidth=0.8)
    ax.set_axisbelow(True)
    fig.text(0.015, 0.95, title, color=FG, fontsize=13, fontweight="bold", va="top")
    fig.text(0.015, 0.875, subtitle, color=SUB, fontsize=7.5, va="top")
    return fig, ax


def _save(fig, name, top=0.82):
    fig.subplots_adjust(top=top)
    p = os.path.join(OUT, name)
    fig.savefig(p, facecolor=fig.get_facecolor())
    plt.close(fig)
    return p


def _hbars(ax, labels, values, texts, color=BLUE):
    y = range(len(labels))[::-1]
    ax.barh(list(y), values, color=color, height=0.75)
    ax.set_yticks(list(y), labels)
    ax.grid(axis="y", visible=False)
    for yi, v, t in zip(y, values, texts):
        ax.text(v, yi, f" {t}", color=FG, fontsize=7, va="center")


def architecture():
    fig, ax = plt.subplots(figsize=(10, 6.4), dpi=200)
    ax.set_xlim(0, 100)
    ax.set_ylim(0, 64)
    ax.axis("off")
    ax.add_patch(plt.Rectangle((0.5, 0.5), 99, 63, fill=False, lw=1.2, ec="#222"))

    def box(x, y, w, h, text, color):
        ax.add_patch(FancyBboxPatch((x, y), w, h, boxstyle="round,pad=0,rounding_size=1.2",
                                    fc=color, ec="#7b8794", lw=1))
        ax.text(x + w / 2, y + h / 2, text, ha="center", va="center", fontsize=8.6, color="#1f2933")

    def arrow(x1, y1, x2, y2, dashed=False):
        ax.annotate("", (x2, y2), (x1, y1), arrowprops=dict(arrowstyle="-|>", color="#4a5568", lw=1.3,
                                                            ls="--" if dashed else "-", shrinkA=0, shrinkB=0))

    w, h = 24.5, 5.6
    c1, c2, c3 = 5.5, 37.5, 69.5
    rows = [55, 46, 37, 28, 19]
    box(c1, rows[0], w, h, "CICIDS2017 CSVs\n(8 files, 2.83M labelled flows)", "#c6d8ec")
    box(c1, rows[1], w, h, "Replay Producer\n(idempotent, lz4, keyed by Source IP)", "#cfe9f5")
    box(c1, rows[2], w, h, "Apache Kafka (KRaft, 1 broker)\n(flows.raw: 12 partitions)", "#93c6f2")
    box(c1, rows[3], w, h, "Spark Structured Streaming\n(Q1 bronze, Q2 silver)", "#bfe5d6")
    box(c1, rows[4], w, h, "Delta Lake Medallion\n(bronze / silver / gold)", "#f0e8c4")
    box(c2, rows[3], w, h, "Q3a / Q3b Gold Windows\n(1-min timeline, per-host,\nport-scan candidates)", "#bfe5d6")
    box(c2, rows[4], w, h, "Q4 Live Scoring\n(MLlib RF + KMeans, hot reload)", "#f7dbbd")
    box(c3, rows[2], w, h, "Spark MLlib Training\n(RandomForest + KMeans)", "#f7dbbd")
    box(c3, rows[3], w, h, "Model Registry\n(versioned, latest.json)", "#e5e5e5")
    box(c3, rows[4], w, h, "Kafka output topics\n(flows.scored, alerts, flows.dlq)", "#93c6f2")
    box(c1, 4, c3 + w - c1, h, "SQLite Serving Cache  →  Streamlit Dashboard "
                               "(Pipeline · Alerts · Model & hosts)", "#f1b8a6")
    mid = c1 + w / 2
    for a, b in zip(rows, rows[1:]):
        arrow(mid, a, mid, b + h)
    arrow(c1 + w, rows[3] + h / 2, c2, rows[3] + h / 2)
    arrow(c1 + w, rows[4] + h / 2, c2, rows[4] + h / 2)
    arrow(c2 + w, rows[4] + h / 2, c3, rows[4] + h / 2)
    arrow(c3 + w / 2, rows[2], c3 + w / 2, rows[3] + h)
    for x in (mid, c2 + w / 2, c3 + w / 2):
        arrow(x, rows[4], x, 4 + h)
    # Training reads silver (time travel); scoring hot-swaps from the registry
    ax.plot([c1 + w, 33.5, 33.5, 64.5], [rows[4] + 1.2, rows[4] + 1.2, 35, 35], ls="--", color="#4a5568", lw=1.3)
    arrow(64.5, 35, c3, rows[2] + h / 2, dashed=True)
    ax.text(36, 35.8, "train on silver (Delta time travel)", fontsize=7.5, color="#4a5568")
    arrow(c3, rows[3] + 1, c2 + w, rows[4] + h - 0.6, dashed=True)
    ax.text(66.5, 25.6, "hot swap", fontsize=7.5, color="#4a5568")
    fig.subplots_adjust(0, 0, 1, 1)
    p = os.path.join(OUT, "fig1_architecture.png")
    fig.savefig(p, facecolor="white")
    plt.close(fig)
    return p


def families(silver):
    b = silver[silver.split == "backfill"]
    vc = b.label_family.value_counts()
    pct = vc / vc.sum() * 100
    fig, ax = _axes("Flows by family", f"Share of the {len(b):,} backfill flows in the silver Delta table "
                                       "(benign flows downsampled to 20%)")
    _hbars(ax, list(vc.index), list(pct), [f"{p:.2f}%  ({n:,})" for p, n in zip(pct, vc)])
    ax.set_xlabel("Share of flows (%)", color=SUB, fontsize=8)
    ax.set_xlim(0, pct.max() * 1.15)
    fig.subplots_adjust(left=0.17, right=0.97, bottom=0.12)
    return _save(fig, "fig2_families.png")


def hourly(silver):
    b = silver[silver.split == "backfill"].copy()
    b["hour"] = b.event_time.dt.floor("h")
    b["kind"] = (b.label_family == "Normal Traffic").map({True: "Normal traffic", False: "Attack flows"})
    t = b.groupby(["hour", "kind"]).size().unstack(fill_value=0)
    t = t.reindex(pd.date_range(t.index.min(), t.index.max(), freq="h", tz=t.index.tz), fill_value=0)
    t = t[t.index.hour.isin(range(8, 18))]                       # working hours of the capture
    fig, ax = _axes("Activity by hour (event time, 3–7 July 2017)",
                    "Backfill split; benign flows downsampled to 20%")
    x = range(len(t))
    ax.plot(x, t["Normal traffic"], color=BLUE, marker="o", ms=2.5, lw=1.2, label="Normal traffic")
    ax.plot(x, t["Attack flows"], color=RED, marker="o", ms=2.5, lw=1.2, label="Attack flows")
    ticks = list(range(0, len(t), 3))
    ax.set_xticks(ticks, [t.index[i].strftime("%m-%d\n%Hh") for i in ticks], fontsize=6.5)
    ax.yaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(lambda v, _: f"{v / 1000:.0f}k"))
    ax.set_ylabel("Flows per hour", color=SUB, fontsize=8)
    ax.legend(facecolor=BG, edgecolor=GRID, labelcolor=FG, fontsize=7, loc="upper right")
    fig.subplots_adjust(left=0.07, right=0.98, bottom=0.14)
    return _save(fig, "fig3_hourly.png")


def throughput(metrics):
    g = metrics.groupby("query_name").processed_rps.agg(["mean", "max"]).sort_index(ascending=False)
    fig, ax = _axes("Streaming throughput per query", "Rows processed per second, from the "
                                                      "StreamingQueryListener (live-replay batches with input)",
                    h=3.8)
    y = range(len(g))
    ax.barh([i + 0.2 for i in y], g["max"], height=0.38, color=DARK_BLUE, label="Peak")
    ax.barh([i - 0.2 for i in y], g["mean"], height=0.38, color=BLUE, label="Average")
    ax.set_yticks(list(y), g.index)
    ax.grid(axis="y", visible=False)
    ax.set_xlabel("Rows / second", color=SUB, fontsize=8)
    ax.legend(facecolor=BG, edgecolor=GRID, labelcolor=FG, fontsize=7, loc="lower right")
    fig.subplots_adjust(left=0.17, right=0.97, bottom=0.14)
    return _save(fig, "fig4_throughput.png")


def f1_by_class(metrics, version):
    pc = sorted(metrics["per_class"].items(), key=lambda kv: -kv[1]["f1"])
    fig, ax = _axes(f"Held-out F1 by class (RandomForest, model v{version})",
                    f"{metrics['rows']:,} test flows; support shown in brackets", h=3.9)
    _hbars(ax, [k for k, _ in pc], [v["f1"] for _, v in pc], [f"{v['f1']:.3f}  ({v['support']:,})" for _, v in pc])
    ax.set_xlim(0, 1.15)
    ax.set_xlabel("F1 score", color=SUB, fontsize=8)
    fig.subplots_adjust(left=0.17, right=0.97, bottom=0.13)
    return _save(fig, "fig5_f1.png")


def importances(meta):
    fi = meta["feature_importances"]
    fi = ast.literal_eval(fi) if isinstance(fi, str) else fi
    fi = sorted(fi, key=lambda kv: -kv[1])[:10]
    fig, ax = _axes("Top 10 feature importances", "RandomForest ({numTrees} trees, max depth {maxDepth})"
                    .format(**meta["rf_params"]), h=3.9)
    _hbars(ax, [k for k, _ in fi], [v for _, v in fi], [f"{v:.3f}" for _, v in fi])
    ax.set_xlim(0, fi[0][1] * 1.12)
    ax.set_xlabel("Importance", color=SUB, fontsize=8)
    fig.subplots_adjust(left=0.22, right=0.97, bottom=0.13)
    return _save(fig, "fig6_importance.png")


def live_alerts(scored):
    a = scored[scored.is_alert.astype(bool)]
    g = a.groupby(["label_family", "predicted_family"]).size().sort_values(ascending=False)
    labels = [f"truth {t} → predicted {p}" for t, p in g.index]
    colors = [RED if t == "Normal Traffic" else BLUE for t, _ in g.index]
    fig, ax = _axes("Live alerts: predicted family vs ground truth",
                    f"All {len(a):,} alerts raised on the held-out live split (red = false positive)", h=4.0)
    y = list(range(len(g)))[::-1]
    ax.barh(y, g.values, color=colors, height=0.75)
    ax.set_yticks(y, labels, fontsize=7)
    ax.set_xscale("log")
    ax.grid(axis="y", visible=False)
    for yi, v in zip(y, g.values):
        ax.text(v, yi, f" {v:,}", color=FG, fontsize=7, va="center")
    ax.set_xlabel("Alerts (log scale)", color=SUB, fontsize=8)
    fig.subplots_adjust(left=0.36, right=0.95, bottom=0.13)
    return _save(fig, "fig7_live_alerts.png")


def port_scans(ps):
    g = ps.groupby("src_ip").agg(ports=("distinct_dst_ports", "max"), windows=("flows", "size")) \
          .sort_values("ports", ascending=False)
    fig, ax = _axes("Port-scan candidates", "Source IPs touching more than 50 distinct destination ports "
                                            "in a 1-minute window", h=3.6)
    _hbars(ax, list(g.index), list(g.ports), [f"{p:,} ports, {w} windows" for p, w in zip(g.ports, g.windows)])
    ax.set_xlim(0, g.ports.max() * 1.3)
    ax.set_xlabel("Max distinct destination ports in one window", color=SUB, fontsize=8)
    fig.subplots_adjust(left=0.17, right=0.97, bottom=0.14)
    return _save(fig, "fig8_port_scans.png")


def build() -> list:
    os.makedirs(OUT, exist_ok=True)
    plt.rcParams["font.family"] = "DejaVu Sans"
    silver = _delta("silver/flows", ["split", "label_family", "event_time"])
    version, meta, metrics = _model()
    with sqlite3.connect(DB) as c:
        sm = pd.read_sql("SELECT query_name, processed_rps FROM stream_metrics WHERE num_input_rows > 0", c)
    return [architecture(), families(silver), hourly(silver), throughput(sm), f1_by_class(metrics, version),
            importances(meta), live_alerts(_delta("gold/scored_flows")),
            port_scans(_delta("gold/port_scan_candidates"))]


if __name__ == "__main__":
    for p in build():
        print("wrote", p)
