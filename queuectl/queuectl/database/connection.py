"""
database.connection — SQLite connection factory and timestamp helper.

Storage choice: SQLite, using WAL mode for concurrent readers and
BEGIN IMMEDIATE transactions for atomic write sequences. See
job_repository.claim_next_job for the detailed atomicity explanation.
"""

import os
import sqlite3
from datetime import datetime, timezone

def _default_db_path() -> str:
    """Resolve queue.db relative to the project root at import time."""
    # This file lives at: <project>/queuectl/database/connection.py
    # Project root is:    <project>/
    this_file = os.path.abspath(__file__)
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(this_file)))
    return os.path.join(project_root, "queue.db")


DB_PATH: str = os.environ.get("QUEUECTL_DB", _default_db_path())


def now_iso() -> str:
    """Return the current UTC time as an ISO-8601 string."""
    return datetime.now(timezone.utc).isoformat()


def _get_db_path() -> str:
    """
    Return the current effective database path.

    Reading from globals() at call time (rather than capturing the value at
    import time) means that monkeypatching either this module's DB_PATH or
    the db.py shim's DB_PATH at test setup time correctly redirects all
    subsequent get_connection() calls to the test database.
    """
    import queuectl.database.connection as _self
    return _self.DB_PATH


def get_connection() -> sqlite3.Connection:
    """
    Open and configure a SQLite connection to the queue database.

    PRAGMAs applied:
    - WAL: lets readers (status, list) run concurrently with a writing worker.
    - synchronous=NORMAL: safe durability level for WAL mode.
    - busy_timeout=30000: block up to 30 s waiting for a lock rather than
      raising an immediate OperationalError on contention.
    - foreign_keys=ON: enforce referential integrity if foreign keys are
      ever added to the schema.
    """
    conn = sqlite3.connect(_get_db_path(), timeout=30, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA busy_timeout=30000;")
    conn.execute("PRAGMA foreign_keys=ON;")
    return conn

