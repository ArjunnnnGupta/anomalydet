"""
tests/synthetic.py — tiny, clearly-synthetic CICIDS2017-shaped flows for unit tests.

Each attack family gets a distinct feature profile so a model can actually learn
it, and timestamps use both real CICIDS formats (with seconds, and 12-hour
afternoon times without seconds).
"""

import json
import random

from src.common.schemas import NUMERIC_COLS, RAW_COLS

# The real header: leading spaces + duplicated "Fwd Header Length"
CSV_HEADER = (
    "Flow ID, Source IP, Source Port, Destination IP, Destination Port, Protocol, Timestamp, "
    + ", ".join(c.replace("_", " ").title() for c in NUMERIC_COLS[:40])
    + ", Fwd Header Length, " + ", ".join(c.replace("_", " ").title() for c in NUMERIC_COLS[40:])
    + ", Label"
)

PROFILES = {   # label: (count, dst_port(s), duration, fwd_pkts, pkt_len_mean, syn)
    "BENIGN":                     (300, [80, 443, 53], 2e5, 8, 300.0, 0),
    "DoS Hulk":                   (60, [80], 5e6, 2, 0.0, 1),
    "DDoS":                       (40, [80], 1e3, 3, 10.0, 0),
    "PortScan":                   (100, None, 50.0, 1, 0.0, 1),
    "SSH-Patator":                (30, [22], 1e7, 20, 80.0, 0),
    "Web Attack \x96 Brute Force": (20, [80], 6e6, 4, 600.0, 0),
    "Bot":                        (20, [8080], 3e4, 5, 40.0, 0),
}


def make_rows(seed: int = 7) -> list[dict]:
    """Normalised-name dicts with raw string values (what the producer puts in Kafka)."""
    rnd = random.Random(seed)
    rows = []
    for label, (n, ports, dur, pkts, plen, syn) in PROFILES.items():
        for i in range(n):
            minute = 0 if label == "PortScan" else rnd.randint(0, 59)   # a scan burst within one minute
            sec = rnd.randint(0, 59)
            # benign/DoS in the morning with seconds; the rest as 12-hour afternoon times (3:mm → 15:mm)
            ts = (f"05/07/2017 09:{minute:02d}:{sec:02d}" if label in ("BENIGN", "DoS Hulk")
                  else f"5/7/2017 3:{minute:02d}")
            src = "205.174.165.73" if label != "BENIGN" else f"192.168.10.{rnd.randint(2, 30)}"
            port = (1000 + i) if ports is None else rnd.choice(ports)
            r = {c: f"{rnd.uniform(0, 5):.3f}" for c in NUMERIC_COLS}
            r.update(
                flow_id=f"{src}-192.168.10.50-{port}-{i}-{label[:3]}",
                source_ip=src, source_port=str(rnd.randint(30000, 60000)),
                destination_ip="192.168.10.50", destination_port=str(port), protocol="6",
                timestamp=ts, label=label,
                flow_duration=f"{dur * rnd.uniform(0.8, 1.2):.1f}",
                total_fwd_packets=str(max(1, int(pkts * rnd.uniform(0.7, 1.3)))),
                packet_length_mean=f"{plen * rnd.uniform(0.8, 1.2):.2f}",
                syn_flag_count=str(syn),
                flow_bytes_s="Infinity" if rnd.random() < 0.05 else f"{rnd.uniform(1, 1e5):.2f}",
            )
            rows.append({c: r[c] for c in RAW_COLS})
    return rows


def payloads(rows: list[dict], split_of=lambda r: "backfill") -> list[str]:
    return [json.dumps({**r, "split": split_of(r), "source_file": "synthetic.csv",
                        "produced_at": "2026-10-20T00:00:00", "replay_ts_ms": None}) for r in rows]


def write_csv(rows: list[dict], path: str, blank_tail: int = 3) -> None:
    """Write rows in the original TrafficLabelling layout (latin-1, spaced header, dup column)."""
    names = [c for c in RAW_COLS if c != "label"]
    with open(path, "w", encoding="latin1", newline="") as f:
        f.write(CSV_HEADER + "\n")
        for r in rows:
            vals = [r[c] for c in names]
            vals.insert(7 + 40, r["fwd_header_length"])   # duplicated column
            f.write(",".join(vals + [r["label"]]) + "\n")
        f.write(("," * (len(names) + 1) + "\n") * blank_tail)   # like the Thursday file's empty trailing lines
