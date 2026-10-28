"""
src/common/schemas.py — dataset contract shared by the producer and the Spark jobs.

CICIDS2017 "GeneratedLabelledFlows" (TrafficLabelling) CSVs have 85 columns with
leading spaces, a duplicated "Fwd Header Length", and labels using a Windows-1252
dash. Names are normalised to snake_case once, in the producer, so every Kafka
payload and Delta table uses the same column names.
"""

from __future__ import annotations

import re

from pyspark.sql import Column
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType


def normalize_name(name: str) -> str:
    n = re.sub(r"[^0-9a-zA-Z]+", "_", name.strip()).strip("_").lower()
    return n


def normalize_columns(names: list[str]) -> list[str]:
    """snake_case + de-duplicate (second 'Fwd Header Length' → fwd_header_length_1)."""
    seen: dict[str, int] = {}
    out = []
    for raw in names:
        n = normalize_name(raw)
        if n in seen:
            seen[n] += 1
            n = f"{n}_{seen[n]}"
        else:
            seen[n] = 0
        out.append(n)
    return out


# ── Column groups ──────────────────────────────────────────────────────────────
ID_COLS = ["flow_id", "source_ip", "source_port", "destination_ip", "destination_port",
           "protocol", "timestamp"]

NUMERIC_COLS = [
    "flow_duration", "total_fwd_packets", "total_backward_packets",
    "total_length_of_fwd_packets", "total_length_of_bwd_packets",
    "fwd_packet_length_max", "fwd_packet_length_min", "fwd_packet_length_mean", "fwd_packet_length_std",
    "bwd_packet_length_max", "bwd_packet_length_min", "bwd_packet_length_mean", "bwd_packet_length_std",
    "flow_bytes_s", "flow_packets_s",
    "flow_iat_mean", "flow_iat_std", "flow_iat_max", "flow_iat_min",
    "fwd_iat_total", "fwd_iat_mean", "fwd_iat_std", "fwd_iat_max", "fwd_iat_min",
    "bwd_iat_total", "bwd_iat_mean", "bwd_iat_std", "bwd_iat_max", "bwd_iat_min",
    "fwd_psh_flags", "bwd_psh_flags", "fwd_urg_flags", "bwd_urg_flags",
    "fwd_header_length", "bwd_header_length", "fwd_packets_s", "bwd_packets_s",
    "min_packet_length", "max_packet_length", "packet_length_mean", "packet_length_std",
    "packet_length_variance",
    "fin_flag_count", "syn_flag_count", "rst_flag_count", "psh_flag_count", "ack_flag_count",
    "urg_flag_count", "cwe_flag_count", "ece_flag_count",
    "down_up_ratio", "average_packet_size", "avg_fwd_segment_size", "avg_bwd_segment_size",
    "fwd_avg_bytes_bulk", "fwd_avg_packets_bulk", "fwd_avg_bulk_rate",
    "bwd_avg_bytes_bulk", "bwd_avg_packets_bulk", "bwd_avg_bulk_rate",
    "subflow_fwd_packets", "subflow_fwd_bytes", "subflow_bwd_packets", "subflow_bwd_bytes",
    "init_win_bytes_forward", "init_win_bytes_backward", "act_data_pkt_fwd", "min_seg_size_forward",
    "active_mean", "active_std", "active_max", "active_min",
    "idle_mean", "idle_std", "idle_max", "idle_min",
]

RAW_COLS = ID_COLS + NUMERIC_COLS + ["label"]

# Constant (all-zero) in CICIDS2017, so useless as model inputs
_CONSTANT_COLS = {"bwd_psh_flags", "bwd_urg_flags", "fwd_avg_bytes_bulk", "fwd_avg_packets_bulk",
                  "fwd_avg_bulk_rate", "bwd_avg_bytes_bulk", "bwd_avg_packets_bulk", "bwd_avg_bulk_rate"}
# Silver column names (destination_port → dst_port)
FEATURE_COLS = ["dst_port", "protocol"] + [c for c in NUMERIC_COLS if c not in _CONSTANT_COLS]

# run_id is set only on live replays: each `make live` gets new flow_uids, so the demo can be re-run
META_COLS = ["source_file", "split", "produced_at", "replay_ts_ms", "run_id"]

# Kafka payload: every value travels as a string (raw, untouched); Spark does all typing.
PAYLOAD_SCHEMA = StructType([StructField(c, StringType(), True) for c in RAW_COLS + META_COLS])


# ── Labels ─────────────────────────────────────────────────────────────────────
LABEL_FAMILIES = ["Normal Traffic", "DoS", "DDoS", "Port Scanning", "Brute Force",
                  "Web Attacks", "Bots", "Infiltration"]


def clean_label(col: Column) -> Column:
    """Replace the 0x96 dash (and any non-ASCII) with '-', collapse whitespace."""
    c = F.regexp_replace(col, r"[^\x00-\x7F]+", "-")
    return F.trim(F.regexp_replace(c, r"\s+", " "))


def label_family(label: Column) -> Column:
    l = F.lower(label)
    return (
        F.when(l == "benign", "Normal Traffic")
        .when(l.isin("ftp-patator", "ssh-patator"), "Brute Force")
        .when(l == "ddos", "DDoS")
        .when(l.startswith("dos") | (l == "heartbleed"), "DoS")
        .when(l == "portscan", "Port Scanning")
        .when(l == "bot", "Bots")
        .when(l.startswith("web attack"), "Web Attacks")
        .when(l == "infiltration", "Infiltration")
        .otherwise(None)
    )
