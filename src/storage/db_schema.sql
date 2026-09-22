-- src/storage/db_schema.sql — Phase 5: SQLite Schema
--
-- Two tables:
--   flow_scores : one row per scored network flow
--   alerts      : subset of flow_scores where IF flagged as anomalous,
--                 with the wall-clock time of the scoring run

CREATE TABLE IF NOT EXISTS flow_scores (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    flow_index      INTEGER,           -- row index in the sampled dataset
    anomaly_score   REAL,              -- IF score (higher = more anomalous)
    if_flag         INTEGER,           -- 1 = anomaly, 0 = normal (IF output)
    rf_label        TEXT,              -- attack type predicted by Random Forest
    ground_truth    TEXT,              -- actual Attack Type label from dataset
    scored_at       TEXT               -- ISO-8601 wall-clock timestamp of scoring
);

CREATE TABLE IF NOT EXISTS alerts (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    flow_index      INTEGER,
    anomaly_score   REAL,
    rf_label        TEXT,
    ground_truth    TEXT,
    scored_at       TEXT
);

-- Index for dashboard queries: order by most recent alerts first
CREATE INDEX IF NOT EXISTS idx_alerts_scored_at ON alerts (scored_at DESC);
CREATE INDEX IF NOT EXISTS idx_flow_scores_flag  ON flow_scores (if_flag);

