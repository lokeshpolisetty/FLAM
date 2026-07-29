"""
database.worker_repository — worker process registration and heartbeat tracking.

Workers write their PID and status here so that the 'worker stop' command
can signal them and the 'status' command can report which workers are alive.
"""

import sqlite3

from queuectl.database.connection import now_iso


def register_worker(conn: sqlite3.Connection, worker_id: str, pid: int) -> None:
    """Insert or replace the worker's row, marking it as running."""
    ts = now_iso()
    conn.execute(
        """INSERT OR REPLACE INTO workers (worker_id, pid, status, started_at, heartbeat_at)
           VALUES (?, ?, 'running', ?, ?)""",
        (worker_id, pid, ts, ts),
    )


def touch_worker_heartbeat(conn: sqlite3.Connection, worker_id: str) -> None:
    """Refresh the worker's own heartbeat timestamp."""
    conn.execute(
        "UPDATE workers SET heartbeat_at = ? WHERE worker_id = ?",
        (now_iso(), worker_id),
    )


def mark_worker_stopped(conn: sqlite3.Connection, worker_id: str) -> None:
    """Mark a worker as stopped in the registry (called on clean shutdown)."""
    conn.execute(
        "UPDATE workers SET status='stopped' WHERE worker_id=?",
        (worker_id,),
    )
