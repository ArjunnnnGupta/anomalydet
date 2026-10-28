-- src/serving/schema.sql — SQLite serving cache for the dashboard.
-- Written by the streaming app (foreachBatch, on the driver); read by Streamlit.
-- Every table has a natural primary key, so a replayed micro-batch overwrites
-- instead of duplicating (INSERT OR REPLACE = idempotent sink).

CREATE TABLE IF NOT EXISTS alerts (
    flow_uid         TEXT PRIMARY KEY,
    event_time       TEXT,
    orig_ts          TEXT,
    src_ip           TEXT,
    src_port         INTEGER,
    dst_ip           TEXT,
    dst_port         INTEGER,
    service          TEXT,
    src_zone         TEXT,
    predicted_family TEXT,
    attack_prob      REAL,
    anomaly_score    REAL,
    alert_reason     TEXT,
    label_family     TEXT,             -- ground truth (held-out live split), for evaluation
    model_version    INTEGER,
    batch_id         INTEGER,
    scored_at        TEXT
);
CREATE INDEX IF NOT EXISTS idx_alerts_event_time ON alerts (event_time DESC);
CREATE INDEX IF NOT EXISTS idx_alerts_src_ip     ON alerts (src_ip);

-- Gold: 1-minute windows per source host (busiest hosts only; the full profile is in Delta)
CREATE TABLE IF NOT EXISTS host_window_stats (
    window_start      TEXT,
    window_end        TEXT,
    split             TEXT,
    src_ip            TEXT,
    src_zone          TEXT,
    flows             INTEGER,
    bytes             REAL,
    distinct_dst_ips  INTEGER,
    distinct_dst_ports INTEGER,
    syn_ratio         REAL,
    rst_ratio         REAL,
    attack_flows      INTEGER,
    PRIMARY KEY (window_start, split, src_ip)
);

CREATE TABLE IF NOT EXISTS port_scan_candidates (
    window_start       TEXT,
    window_end         TEXT,
    split              TEXT,
    src_ip             TEXT,
    distinct_dst_ports INTEGER,
    flows              INTEGER,
    PRIMARY KEY (window_start, split, src_ip)
);

-- StreamingQueryListener progress events
CREATE TABLE IF NOT EXISTS stream_metrics (
    query_name        TEXT,
    batch_id          INTEGER,
    ts                TEXT,
    num_input_rows    INTEGER,
    input_rps         REAL,
    processed_rps     REAL,
    batch_duration_ms INTEGER,
    trigger_ms        INTEGER,
    watermark         TEXT,
    state_rows        INTEGER,
    end_offsets       TEXT,
    PRIMARY KEY (query_name, batch_id)
);
CREATE INDEX IF NOT EXISTS idx_metrics_ts ON stream_metrics (ts DESC);

CREATE TABLE IF NOT EXISTS pipeline_events (
    ts      TEXT,
    source  TEXT,
    message TEXT
);
