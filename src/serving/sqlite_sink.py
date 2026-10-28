"""
src/serving/sqlite_sink.py — idempotent writes into the SQLite serving cache.

Used from foreachBatch (on the Spark driver). INSERT OR REPLACE on each table's
natural key means a micro-batch that is retried after a failure overwrites its own
rows instead of duplicating them, which keeps the sink effectively exactly-once.
"""

from __future__ import annotations

import os
import sqlite3
import threading
from datetime import date, datetime, timezone
from pathlib import Path

from src.common.config import cfg, path

_SCHEMA = Path(__file__).with_name("schema.sql")
_lock = threading.Lock()   # several streaming queries call foreachBatch concurrently
_initialised: set[str] = set()


def db_path() -> str:
    return path(cfg()["serving"]["sqlite_path"])


def connect(db: str | None = None) -> sqlite3.Connection:
    db = db or db_path()
    os.makedirs(os.path.dirname(db), exist_ok=True)
    conn = sqlite3.connect(db, timeout=30)
    if db not in _initialised:
        conn.executescript(_SCHEMA.read_text())
        conn.commit()
        _initialised.add(db)
    return conn


def _plain(v):
    if isinstance(v, datetime):
        return v.isoformat(sep=" ")
    if isinstance(v, date):
        return v.isoformat()
    return v


def upsert(table: str, rows: list[dict], db: str | None = None) -> int:
    if not rows:
        return 0
    cols = list(rows[0].keys())
    sql = (f"INSERT OR REPLACE INTO {table} ({', '.join(cols)}) "
           f"VALUES ({', '.join('?' for _ in cols)})")
    data = [tuple(_plain(r[c]) for c in cols) for r in rows]
    with _lock:
        conn = connect(db)
        try:
            conn.executemany(sql, data)
            conn.commit()
        finally:
            conn.close()
    return len(data)


def upsert_df(table: str, df, db: str | None = None) -> int:
    """Collect a (small) batch DataFrame to the driver and upsert it."""
    return upsert(table, [r.asDict() for r in df.collect()], db)


def log_event(source: str, message: str, db: str | None = None) -> None:
    with _lock:
        conn = connect(db)
        try:
            conn.execute("INSERT INTO pipeline_events VALUES (?, ?, ?)",
                         (datetime.now(timezone.utc).isoformat(sep=" "), source, message))
            conn.commit()
        finally:
            conn.close()
