"""
database.schema — DDL schema and default configuration values.

Every write-then-read-then-write sequence that must be atomic across
processes uses BEGIN IMMEDIATE … COMMIT. See job_repository for details.
"""

import sqlite3

from queuectl.database.connection import get_connection

SCHEMA: str = """
CREATE TABLE IF NOT EXISTS jobs (
    id            TEXT PRIMARY KEY,
    command       TEXT NOT NULL,
    state         TEXT NOT NULL DEFAULT 'pending'
                  CHECK (state IN ('pending','processing','failed','dead','completed')),
    attempts      INTEGER NOT NULL DEFAULT 0,
    max_retries   INTEGER NOT NULL DEFAULT 3,
    backoff_base  REAL NOT NULL DEFAULT 2,
    worker_id     TEXT,
    next_retry_at TEXT,
    heartbeat_at  TEXT,
    last_error    TEXT,
    created_at    TEXT NOT NULL,
    updated_at    TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS workers (
    worker_id    TEXT PRIMARY KEY,
    pid          INTEGER NOT NULL,
    status       TEXT NOT NULL DEFAULT 'running',
    started_at   TEXT NOT NULL,
    heartbeat_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS config (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
"""

DEFAULT_CONFIG: dict = {
    "max-retries": "3",
    "backoff-base": "2",
    "heartbeat-interval": "3",   # seconds between heartbeat refreshes on an in-flight job
    "recovery-timeout": "15",    # seconds of silence before a 'processing' job is reclaimed
    "poll-interval": "1",        # seconds a worker sleeps when the queue is empty
}


def init_db() -> None:
    """Create all tables and seed default configuration if not already present."""
    conn = get_connection()
    try:
        conn.executescript(SCHEMA)
        for key, value in DEFAULT_CONFIG.items():
            conn.execute(
                "INSERT OR IGNORE INTO config (key, value) VALUES (?, ?)",
                (key, value),
            )
    finally:
        conn.close()
