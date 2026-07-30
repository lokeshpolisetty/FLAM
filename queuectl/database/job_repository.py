"""
database.job_repository — atomic job lifecycle operations.

All mutating operations that require read-then-write atomicity use
BEGIN IMMEDIATE transactions. SQLite's RESERVED lock, acquired the
instant BEGIN IMMEDIATE executes, ensures only one connection — across
any number of processes on this machine — can be inside this critical
section at a time. See claim_next_job for the full explanation.
"""

import sqlite3
from datetime import datetime, timedelta, timezone

from queuectl.database.connection import now_iso


def claim_next_job(conn: sqlite3.Connection, worker_id: str) -> dict | None:
    """
    Atomically claim the oldest pending job for the given worker.

    Returns a dict of the job row, or None if the queue is empty.

    Atomicity mechanism: BEGIN IMMEDIATE acquires SQLite's RESERVED lock
    the instant it runs, before any row is read. Only one connection in
    any process can hold that lock at a time. A second worker calling
    this function will block inside its own BEGIN IMMEDIATE until this
    transaction commits. This means the SELECT and the subsequent UPDATE
    are indivisible relative to every other process — there is no window
    in which two workers can both see the same job as 'pending' and both
    write 'processing'. The AND state = 'pending' in the UPDATE is a
    belt-and-suspenders guard against future code changes; the transaction
    lock already makes the race impossible.
    """
    conn.execute("BEGIN IMMEDIATE")
    try:
        row = conn.execute(
            """
            SELECT * FROM jobs
            WHERE state = 'pending'
            ORDER BY created_at ASC
            LIMIT 1
            """
        ).fetchone()
        if row is None:
            conn.execute("COMMIT")
            return None

        ts = now_iso()
        conn.execute(
            """
            UPDATE jobs
            SET state = 'processing', worker_id = ?, heartbeat_at = ?, updated_at = ?
            WHERE id = ? AND state = 'pending'
            """,
            (worker_id, ts, ts, row["id"]),
        )
        conn.execute("COMMIT")
        return dict(row)
    except Exception:
        conn.execute("ROLLBACK")
        raise


def touch_job_heartbeat(
    conn: sqlite3.Connection, job_id: str, worker_id: str
) -> None:
    """Refresh the heartbeat timestamp on an in-flight job."""
    conn.execute(
        "UPDATE jobs SET heartbeat_at = ? WHERE id = ? AND worker_id = ? AND state = 'processing'",
        (now_iso(), job_id, worker_id),
    )


def finish_job(
    conn: sqlite3.Connection, job_id: str, worker_id: str, returncode: int
) -> None:
    """
    Record the outcome of a completed job execution.

    The WHERE clause guards on both worker_id and state = 'processing':
    if this job was reaped and reclaimed by another worker while this
    worker's heartbeat was delayed, this becomes a deliberate no-op
    rather than clobbering the new owner's state.
    """
    conn.execute("BEGIN IMMEDIATE")
    try:
        row = conn.execute(
            "SELECT * FROM jobs WHERE id = ? AND worker_id = ? AND state = 'processing'",
            (job_id, worker_id),
        ).fetchone()
        if row is None:
            conn.execute("COMMIT")
            return

        ts = now_iso()
        if returncode == 0:
            conn.execute(
                """UPDATE jobs SET state='completed', worker_id=NULL, heartbeat_at=NULL,
                   last_error=NULL, updated_at=? WHERE id=?""",
                (ts, job_id),
            )
        else:
            attempts = row["attempts"] + 1
            error_message = f"command exited with code {returncode}"
            if attempts >= row["max_retries"]:
                conn.execute(
                    """UPDATE jobs SET state='dead', attempts=?, worker_id=NULL,
                       heartbeat_at=NULL, last_error=?, updated_at=? WHERE id=?""",
                    (attempts, error_message, ts, job_id),
                )
            else:
                delay = row["backoff_base"] ** attempts
                # Clamp to 30 days to prevent timedelta overflow on extreme attempt counts.
                delay = min(delay, 86400 * 30)
                next_retry = (
                    datetime.now(timezone.utc) + timedelta(seconds=delay)
                ).isoformat()
                conn.execute(
                    """UPDATE jobs SET state='failed', attempts=?, next_retry_at=?,
                       worker_id=NULL, heartbeat_at=NULL, last_error=?, updated_at=?
                       WHERE id=?""",
                    (attempts, next_retry, error_message, ts, job_id),
                )
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise


def reap_stale_jobs(conn: sqlite3.Connection, timeout_seconds: float) -> list[str]:
    """
    Return any 'processing' job whose heartbeat is older than timeout_seconds
    to 'pending', and return the list of reclaimed job IDs.

    Called at the top of every worker loop iteration and at the start of
    the status and list commands, so recovery does not depend on any
    single long-lived process remaining alive.
    """
    cutoff = (
        datetime.now(timezone.utc) - timedelta(seconds=timeout_seconds)
    ).isoformat()
    conn.execute("BEGIN IMMEDIATE")
    try:
        rows = conn.execute(
            "SELECT id FROM jobs WHERE state='processing' AND heartbeat_at < ?",
            (cutoff,),
        ).fetchall()
        ts = now_iso()
        for row in rows:
            conn.execute(
                """UPDATE jobs SET state='pending', worker_id=NULL, heartbeat_at=NULL,
                   updated_at=? WHERE id=?""",
                (ts, row["id"]),
            )
        conn.execute("COMMIT")
        return [row["id"] for row in rows]
    except Exception:
        conn.execute("ROLLBACK")
        raise


def promote_ready_retries(conn: sqlite3.Connection) -> None:
    """
    Move 'failed' jobs whose backoff delay has elapsed back to 'pending'.

    Clears next_retry_at so a promoted job is indistinguishable from a
    freshly enqueued one — leaving it set would bleed stale scheduling
    data into later retry cycles.
    """
    ts = now_iso()
    conn.execute("BEGIN IMMEDIATE")
    try:
        conn.execute(
            """UPDATE jobs SET state='pending', next_retry_at=NULL, updated_at=?
               WHERE state='failed' AND next_retry_at <= ?""",
            (ts, ts),
        )
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
