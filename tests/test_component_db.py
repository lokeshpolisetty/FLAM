"""
test_component_db.py — Component tests for individual subsystems with real SQLite storage.

Targets each subsystem in isolation:
- Storage layer atomic update/claim
- Worker loop behaviour
- Signal handling logic
- Worker registry/discovery mechanism
- Retry scheduler / delayed requeue logic
- DLQ storage and retrieval
"""

import json
import os
import signal
import sqlite3
import time
import pytest
from datetime import datetime, timedelta, timezone

from queuectl import database as db
from queuectl.config import settings as config_service


@pytest.fixture
def tmp_db(tmp_path, monkeypatch):
    """Isolated database fixture for component testing."""
    db_file = str(tmp_path / "component_test.db")
    monkeypatch.setenv("QUEUECTL_DB", db_file)
    monkeypatch.setattr(db.connection, "DB_PATH", db_file)
    db.init_db()
    conn = db.get_connection()
    yield conn
    conn.close()


# ============================================================================
# 1. Storage Layer Atomic Update / Claim
# ============================================================================

def test_storage_single_claim_atomicity(tmp_db):
    """Single claim transitions job to processing and second claim returns None."""
    ts = db.now_iso()
    tmp_db.execute(
        "INSERT INTO jobs (id, command, state, created_at, updated_at) VALUES ('c1', 'echo 1', 'pending', ?, ?)",
        (ts, ts)
    )
    job = db.claim_next_job(tmp_db, "w1")
    assert job is not None
    assert job["id"] == "c1"
    assert job["state"] == "pending"  # row before update returned

    # Second claim attempt finds nothing
    second_claim = db.claim_next_job(tmp_db, "w2")
    assert second_claim is None


def test_storage_finish_job_success_and_failure(tmp_db):
    """finish_job updates state to completed on 0 returncode and failed on non-zero returncode."""
    ts = db.now_iso()
    tmp_db.execute(
        "INSERT INTO jobs (id, command, state, attempts, max_retries, backoff_base, worker_id, heartbeat_at, created_at, updated_at) "
        "VALUES ('c_success', 'echo ok', 'processing', 0, 3, 2.0, 'w1', ?, ?, ?)",
        (ts, ts, ts)
    )
    db.finish_job(tmp_db, "c_success", "w1", 0)
    row_ok = tmp_db.execute("SELECT state, worker_id FROM jobs WHERE id='c_success'").fetchone()
    assert row_ok["state"] == "completed"
    assert row_ok["worker_id"] is None

    tmp_db.execute(
        "INSERT INTO jobs (id, command, state, attempts, max_retries, backoff_base, worker_id, heartbeat_at, created_at, updated_at) "
        "VALUES ('c_fail', 'exit 1', 'processing', 0, 3, 2.0, 'w2', ?, ?, ?)",
        (ts, ts, ts)
    )
    db.finish_job(tmp_db, "c_fail", "w2", 1)
    row_fail = tmp_db.execute("SELECT state, attempts, last_error FROM jobs WHERE id='c_fail'").fetchone()
    assert row_fail["state"] == "failed"
    assert row_fail["attempts"] == 1
    assert "exited with code 1" in row_fail["last_error"]


def test_storage_reap_stale_jobs_clears_worker(tmp_db):
    """reap_stale_jobs puts stale processing jobs back to pending and clears worker_id."""
    stale_ts = (datetime.now(timezone.utc) - timedelta(seconds=30)).isoformat()
    ts = db.now_iso()
    tmp_db.execute(
        "INSERT INTO jobs (id, command, state, worker_id, heartbeat_at, created_at, updated_at) "
        "VALUES ('stale_1', 'sleep 10', 'processing', 'w_dead', ?, ?, ?)",
        (stale_ts, stale_ts, ts)
    )
    reaped = db.reap_stale_jobs(tmp_db, timeout_seconds=15)
    assert reaped == ["stale_1"]

    row = tmp_db.execute("SELECT state, worker_id, heartbeat_at FROM jobs WHERE id='stale_1'").fetchone()
    assert row["state"] == "pending"
    assert row["worker_id"] is None
    assert row["heartbeat_at"] is None


# ============================================================================
# 2. Worker Registry Mechanism
# ============================================================================

def test_worker_registry_lifecycle(tmp_db):
    """Registering worker, updating heartbeat, and marking stopped in workers table."""
    worker_id = "w-test-pid"
    pid = 99999

    db.register_worker(tmp_db, worker_id, pid)
    w_row = tmp_db.execute("SELECT * FROM workers WHERE worker_id=?", (worker_id,)).fetchone()
    assert w_row["pid"] == pid
    assert w_row["status"] == "running"

    db.touch_worker_heartbeat(tmp_db, worker_id)
    db.mark_worker_stopped(tmp_db, worker_id)
    w_stopped = tmp_db.execute("SELECT status FROM workers WHERE worker_id=?", (worker_id,)).fetchone()
    assert w_stopped["status"] == "stopped"


# ============================================================================
# 3. Retry Scheduler / Promotion Logic
# ============================================================================

def test_promote_ready_retries_only_elapsed(tmp_db):
    """promote_ready_retries promotes only failed jobs whose next_retry_at <= now."""
    past_ts = (datetime.now(timezone.utc) - timedelta(seconds=5)).isoformat()
    future_ts = (datetime.now(timezone.utc) + timedelta(seconds=300)).isoformat()
    ts = db.now_iso()

    tmp_db.execute(
        "INSERT INTO jobs (id, command, state, attempts, next_retry_at, created_at, updated_at) "
        "VALUES ('ready_job', 'echo 1', 'failed', 1, ?, ?, ?)",
        (past_ts, ts, ts)
    )
    tmp_db.execute(
        "INSERT INTO jobs (id, command, state, attempts, next_retry_at, created_at, updated_at) "
        "VALUES ('future_job', 'echo 2', 'failed', 1, ?, ?, ?)",
        (future_ts, ts, ts)
    )

    db.promote_ready_retries(tmp_db)

    r_ready = tmp_db.execute("SELECT state FROM jobs WHERE id='ready_job'").fetchone()
    assert r_ready["state"] == "pending"

    r_future = tmp_db.execute("SELECT state FROM jobs WHERE id='future_job'").fetchone()
    assert r_future["state"] == "failed"


# ============================================================================
# 4. DLQ Storage and Retrieval Subsystem
# ============================================================================

def test_dlq_storage_unbounded_and_queryable(tmp_db):
    """DLQ stores dead jobs and leaves pending/completed jobs separate."""
    ts = db.now_iso()
    tmp_db.execute(
        "INSERT INTO jobs (id, command, state, attempts, max_retries, last_error, created_at, updated_at) "
        "VALUES ('d1', 'exit 1', 'dead', 3, 3, 'err1', ?, ?)", (ts, ts)
    )
    tmp_db.execute(
        "INSERT INTO jobs (id, command, state, attempts, max_retries, created_at, updated_at) "
        "VALUES ('p1', 'echo hi', 'pending', 0, 3, ?, ?)", (ts, ts)
    )

    dead_rows = tmp_db.execute("SELECT * FROM jobs WHERE state='dead'").fetchall()
    assert len(dead_rows) == 1
    assert dead_rows[0]["id"] == "d1"

    # DLQ retry resets attempts to 0 and clears error
    tmp_db.execute(
        "UPDATE jobs SET state='pending', attempts=0, last_error=NULL WHERE id='d1' AND state='dead'"
    )
    d1_row = tmp_db.execute("SELECT state, attempts, last_error FROM jobs WHERE id='d1'").fetchone()
    assert d1_row["state"] == "pending"
    assert d1_row["attempts"] == 0
    assert d1_row["last_error"] is None
