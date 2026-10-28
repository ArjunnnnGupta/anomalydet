#!/usr/bin/env python3
"""
src/ingestion/replay_producer.py — replays CICIDS2017 flows into Kafka as a live sensor feed.

Kafka is the ONLY way data enters the platform (Kappa architecture). This producer:
  • reads each per-day CSV with pyarrow (all values kept as raw strings) in
    chronological file order, sorting rows by flow start time within a file
  • routes each row to the backfill (~80%) or live (~20%, held out) split with
    a deterministic hash of the row content
  • keys messages by Source IP, so all flows of a host land on the same partition
    and keep their order
  • runs an idempotent, acks=all, lz4-compressed, batched librdkafka producer
  • optionally paces the replay against event time (--speedup) and re-bases the
    timestamps onto the wall clock so the live pass looks like traffic happening now

Usage (from the project root, Kafka reachable at localhost:9092/9094/9096):
    python -m src.ingestion.replay_producer --split backfill                  # full speed
    python -m src.ingestion.replay_producer --split live --speedup 300        # paced + rebased
    python -m src.ingestion.replay_producer --split live --inject-bad 50      # DLQ demo
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import zlib
from datetime import datetime, timezone

import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pacsv
from confluent_kafka import KafkaException, Producer

from src.common.config import cfg, kafka_bootstrap, path, topic
from src.common.schemas import RAW_COLS, normalize_columns

GAP_CAP_S = 60        # idle gaps (nights, between files) shrink to at most this much event time
BATCH_ROWS = 5000
STATS_EVERY_S = 2.0


# ── Reading ────────────────────────────────────────────────────────────────────
def read_file(csv_path: str, encoding: str) -> pa.Table:
    with open(csv_path, encoding=encoding) as f:
        names = normalize_columns(f.readline().rstrip("\r\n").split(","))
    table = pacsv.read_csv(
        csv_path,
        read_options=pacsv.ReadOptions(encoding=encoding, column_names=names, skip_rows=1,
                                       block_size=16 << 20),
        convert_options=pacsv.ConvertOptions(
            column_types={n: pa.string() for n in names},
            null_values=[""], strings_can_be_null=True,
        ),
    )
    keep = [c for c in RAW_COLS if c in table.column_names]
    table = table.select(keep)
    # The Thursday WebAttacks file ends with ~288K completely empty lines: not data, skip them
    return table.filter(pc.is_valid(table["flow_id"]))


def parse_event_time(ts: pd.Series) -> pd.Series:
    """Same rules as the Spark silver layer: two formats + 12-hour clock without AM/PM."""
    t = pd.to_datetime(ts, format="%d/%m/%Y %H:%M:%S", errors="coerce")
    t = t.fillna(pd.to_datetime(ts, format="%d/%m/%Y %H:%M", errors="coerce"))
    # Capture ran ~08:30–17:30; afternoon files write 1:00–7:59 meaning 13:00–19:59
    return t.where(t.dt.hour >= 8, t + pd.Timedelta(hours=12))


def row_hash(values: list) -> int:
    return zlib.crc32("|".join("" if v is None else v for v in values).encode())


def split_of(values: list, backfill_pct: int) -> str:
    return "backfill" if row_hash(values) % 100 < backfill_pct else "live"


def benign_kept(values: list, benign_pct: int) -> bool:
    """Deterministic downsampling of BENIGN rows (uses different hash digits than the split)."""
    return (row_hash(values) // 100) % 100 < benign_pct


def sampled(values: list, sample_pct: int) -> bool:
    """Deterministic sample of whichever split is replayed (a third set of hash digits)."""
    return (row_hash(values) // 10000) % 100 < sample_pct


# ── Producer ───────────────────────────────────────────────────────────────────
class Stats:
    def __init__(self):
        self.sent = self.delivered = self.failed = self.skipped = self.sampled_out = 0
        self.start = time.time()
        self._last = 0.0

    def on_delivery(self, err, _msg):
        if err:
            self.failed += 1
        else:
            self.delivered += 1

    def maybe_print(self, force=False):
        now = time.time()
        if force or now - self._last >= STATS_EVERY_S:
            self._last = now
            el = max(now - self.start, 1e-9)
            print(f"  sent {self.sent:>10,}  delivered {self.delivered:>10,}  failed {self.failed:,}  "
                  f"({self.sent / el:,.0f} msg/s)", end="\r" if not force else "\n", flush=True)


def make_producer() -> Producer:
    p = cfg()["kafka"]["producer"]
    return Producer({
        "bootstrap.servers": kafka_bootstrap(),
        "enable.idempotence": True,          # exactly-once per partition, no duplicates on retry
        "acks": "all",                       # wait for all in-sync replicas
        "compression.type": p["compression"],
        "linger.ms": p["linger_ms"],
        "batch.size": p["batch_size"],
        "queue.buffering.max.messages": 500000,
        "client.id": "cicids-replay-producer",
    })


def produce(producer: Producer, stats: Stats, tp: str, key, value: bytes, ts_ms: int | None):
    while True:
        try:
            producer.produce(tp, key=key, value=value, timestamp=ts_ms or 0, on_delivery=stats.on_delivery)
            stats.sent += 1
            return
        except BufferError:
            producer.poll(0.05)  # local queue full: let librdkafka drain → natural backpressure


def bad_records(n: int) -> list[bytes]:
    """Malformed payloads that exercise each DQ rule in the silver layer."""
    out = []
    for i in range(n):
        kind = i % 3
        if kind == 0:
            out.append(b"{not valid json ::")
        elif kind == 1:
            out.append(json.dumps({"flow_id": f"bad-{i}", "source_ip": "10.0.0.1",
                                   "timestamp": "not-a-time", "label": "BENIGN",
                                   "split": "live"}).encode())
        else:
            out.append(json.dumps({"flow_id": f"bad-{i}", "source_ip": "10.0.0.1",
                                   "timestamp": "07/07/2017 10:00:00", "flow_duration": "-42",
                                   "label": "BENIGN", "split": "live"}).encode())
    return out


def main():
    ap = argparse.ArgumentParser(description="Replay CICIDS2017 flows into Kafka")
    ap.add_argument("--split", choices=["backfill", "live", "all"], default="all")
    ap.add_argument("--speedup", type=float, default=0.0,
                    help="replay N× faster than real event time (0 = as fast as possible)")
    ap.add_argument("--max-rate", type=float, default=0.0, help="cap messages/second (0 = no cap)")
    ap.add_argument("--rebase", choices=["auto", "yes", "no"], default="auto",
                    help="stamp records with wall-clock replay time (auto: only for --split live)")
    ap.add_argument("--files", nargs="*", help="subset of CSV file names (default: all, in order)")
    ap.add_argument("--limit", type=int, default=0, help="stop after N messages (0 = no limit)")
    ap.add_argument("--inject-bad", type=int, default=0, help="also send N malformed records")
    ap.add_argument("--bad-only", action="store_true", help="only send the --inject-bad records, then exit")
    ap.add_argument("--benign-pct", type=int, default=None,
                    help="keep this %% of BENIGN rows, all attack rows (default: dataset.benign_pct)")
    ap.add_argument("--sample-pct", type=int, default=100,
                    help="replay only this %% of the selected rows (deterministic; e.g. a short demo run)")
    args = ap.parse_args()

    d = cfg()["dataset"]
    benign_pct = args.benign_pct if args.benign_pct is not None else d.get("benign_pct", 100)
    raw_dir = path(d["raw_dir"])
    files = args.files or d["files"]
    missing = [f for f in files if not os.path.exists(os.path.join(raw_dir, f))]
    if missing and not args.bad_only:
        print(f"\nERROR: missing CICIDS2017 CSVs in {raw_dir}:\n  " + "\n  ".join(missing))
        print("Download GeneratedLabelledFlows.zip (TrafficLabelling) from "
              "https://www.unb.ca/cic/datasets/ids-2017.html and unzip it there.")
        sys.exit(1)

    rebase = args.rebase == "yes" or (args.rebase == "auto" and args.split == "live")
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S") if rebase else None
    tp = topic("raw")
    producer = make_producer()
    stats = Stats()

    print(f"\n{'=' * 64}\nReplay producer → {tp}  @ {kafka_bootstrap()}")
    print(f"  split={args.split}  speedup={args.speedup or 'max'}  max_rate={args.max_rate or '∞'}  "
          f"rebase={rebase}  run_id={run_id}  benign kept={benign_pct}% (all attacks kept)  sample={args.sample_pct}%\n{'=' * 64}")

    if args.inject_bad:
        for v in bad_records(args.inject_bad):
            produce(producer, stats, tp, b"bad-actor", v, None)
        print(f"Injected {args.inject_bad} malformed records.")
    if args.bad_only:
        producer.flush(30)
        return

    wall_start = time.time()
    virtual_s = 0.0      # event-time seconds elapsed (gaps capped)
    prev_ev = None
    done = False

    for fname in [f for f in files if f not in missing]:
        fpath = os.path.join(raw_dir, fname)
        print(f"\n[{fname}] reading ...", flush=True)
        table = read_file(fpath, d["encoding"])
        ev = parse_event_time(table["timestamp"].to_pandas())
        order = ev.sort_values(kind="stable", na_position="last").index.to_numpy()
        ev_valid = ev.notna().to_numpy()
        ev_ms = ev.to_numpy(dtype="datetime64[ms]").astype("int64")   # NaT → garbage, masked by ev_valid
        cols = table.column_names
        print(f"[{fname}] {table.num_rows:,} rows, replaying ...", flush=True)

        for start in range(0, len(order), BATCH_ROWS):
            idx = order[start:start + BATCH_ROWS]
            rows = table.take(pa.array(idx)).to_pylist()
            for i, row in zip(idx, rows):
                vals = [row[c] for c in cols]
                if (row.get("label") or "").strip().upper() == "BENIGN" and not benign_kept(vals, benign_pct):
                    stats.sampled_out += 1
                    continue
                if args.sample_pct < 100 and not sampled(vals, args.sample_pct):
                    stats.sampled_out += 1
                    continue
                sp = split_of(vals, d["backfill_pct"])
                if args.split != "all" and sp != args.split:
                    stats.skipped += 1
                    continue
                t_ms = int(ev_ms[i]) if ev_valid[i] else None

                # Event-time pacing: advance a virtual clock, sleep while ahead of it
                if t_ms is not None:
                    if prev_ev is not None:
                        virtual_s += min(max((t_ms - prev_ev) / 1000, 0.0), GAP_CAP_S)
                    prev_ev = t_ms if prev_ev is None else max(prev_ev, t_ms)
                if args.speedup:
                    ahead = (wall_start + virtual_s / args.speedup) - time.time()
                    if ahead > 0:
                        producer.poll(0)
                        time.sleep(ahead)
                if args.max_rate:
                    ahead = (wall_start + stats.sent / args.max_rate) - time.time()
                    if ahead > 0:
                        time.sleep(ahead)

                replay_ms = int(time.time() * 1000) if rebase else None
                row.update(source_file=fname, split=sp,
                           produced_at=datetime.now(timezone.utc).isoformat(),
                           replay_ts_ms=None if replay_ms is None else str(replay_ms),
                           run_id=run_id)
                key = (row.get("source_ip") or "").encode()
                # Never stamp the Kafka record with the 2017 event time: broker retention is based on
                # the record timestamp, so Kafka would delete the backfill immediately. The event time
                # travels in the payload; the Kafka timestamp is the send time (CreateTime = now).
                produce(producer, stats, tp, key, json.dumps(row).encode(), None)

                if args.limit and stats.sent >= args.limit:
                    done = True
                    break
            producer.poll(0)
            stats.maybe_print()
            if done:
                break
        del table
        if done:
            break

    print("\nFlushing ...")
    remaining = producer.flush(60)
    stats.maybe_print(force=True)
    el = time.time() - stats.start
    print(f"\nDone: {stats.sent:,} sent, {stats.delivered:,} acknowledged, {stats.failed:,} failed, "
          f"{stats.skipped:,} skipped (other split), {stats.sampled_out:,} sampled out "
          f"in {el:,.1f}s  ({stats.sent / max(el, 1e-9):,.0f} msg/s)")
    if remaining:
        raise KafkaException(f"{remaining} messages still undelivered after flush")


if __name__ == "__main__":
    main()
