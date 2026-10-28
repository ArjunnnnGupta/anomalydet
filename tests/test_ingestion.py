"""Producer-side logic: no Kafka or Spark needed."""

import json
import os

import pandas as pd

from src.common.schemas import RAW_COLS, normalize_columns
from src.ingestion.replay_producer import (bad_records, benign_kept, parse_event_time, read_file,
                                           sampled, split_of)
from tests.synthetic import make_rows, write_csv


def test_normalize_columns_dedupes_and_snake_cases():
    assert normalize_columns([" Flow ID", " Flow Bytes/s", "Fwd Header Length", " Fwd Header Length"]) == \
        ["flow_id", "flow_bytes_s", "fwd_header_length", "fwd_header_length_1"]


def test_read_file_matches_contract_and_skips_blank_lines(app_home):
    rows = make_rows()
    p = os.path.join(app_home, "sample.csv")
    write_csv(rows, p, blank_tail=5)
    t = read_file(p, "latin1")
    assert t.num_rows == len(rows)                     # blank trailing lines dropped
    assert t.column_names == RAW_COLS                  # duplicate column dropped, order kept
    labels = set(t["label"].to_pylist())
    assert "Web Attack \x96 Brute Force" in labels     # latin-1 0x96 survives untouched (raw)


def test_parse_event_time_handles_both_formats_and_12h_clock():
    s = pd.Series(["05/07/2017 09:15:30", "5/7/2017 3:05", "6/7/2017 12:30", "garbage"])
    t = parse_event_time(s)
    assert t[0] == pd.Timestamp("2017-07-05 09:15:30")
    assert t[1] == pd.Timestamp("2017-07-05 15:05:00")   # afternoon file, no AM/PM
    assert t[2] == pd.Timestamp("2017-07-06 12:30:00")
    assert pd.isna(t[3])


def test_split_is_deterministic_and_roughly_80_20():
    rows = make_rows()
    a = [split_of(list(r.values()), 80) for r in rows]
    b = [split_of(list(r.values()), 80) for r in rows]
    assert a == b
    share = a.count("backfill") / len(a)
    assert 0.7 < share < 0.9


def test_benign_downsampling_is_deterministic_and_independent_of_split():
    rows = [list(r.values()) for r in make_rows(seed=11)]
    kept = [benign_kept(v, 20) for v in rows]
    assert kept == [benign_kept(v, 20) for v in rows]
    assert 0.1 < sum(kept) / len(kept) < 0.3
    assert all(benign_kept(v, 100) for v in rows) and not any(benign_kept(v, 0) for v in rows)
    # Kept rows still split ~80/20, i.e. the two hashes don't line up
    sp = [split_of(v, 80) for v, k in zip(rows, kept) if k]
    assert 0.6 < sp.count("backfill") / len(sp) < 0.95


def test_bad_records_cover_each_dq_rule():
    recs = bad_records(3)
    assert recs[0].startswith(b"{not valid")
    assert json.loads(recs[1])["timestamp"] == "not-a-time"
    assert json.loads(recs[2])["flow_duration"] == "-42"


def test_demo_sample_is_deterministic_and_independent():
    rows = [list(r.values()) for r in make_rows(seed=13)]
    keep = [sampled(v, 15) for v in rows]
    assert keep == [sampled(v, 15) for v in rows]
    assert 0.05 < sum(keep) / len(keep) < 0.3
    assert all(sampled(v, 100) for v in rows)
    # the sample doesn't pick only one split or only kept-benign rows
    sp = [split_of(v, 80) for v, k in zip(rows, keep) if k]
    assert 0.5 < sp.count("backfill") / len(sp) < 0.98
