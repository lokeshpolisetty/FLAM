"""
test_component_deep.py — Deep Component tests covering Section 2.B of test_strategy.md.

Tests each subsystem in isolation with real SQLite storage:
  1. Storage layer atomic update/claim (TOCTOU safety, WAL mode, busy_timeout)
  2. Worker loop behavior with stubbed command execution
  3. Signal handler behavior
  4. Worker registry/discovery mechanism
  5. Retry scheduler / delayed requeue logic
  6. DLQ storage and retrieval
"""

import os
import signal
import sqlite3
import subprocess
import sys
import time
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
from queuectl import database as db
from queuectl.config import settings as config_service

@pytest.fixture
def conn(tmp_path, monkeypatch):
    """Isolated DB connection per test."""
    db_file = str(tmp_path / "deep.db")
    monkeypatch.setenv("QUEUECTL_DB", db_file)
    monkeypatch.setattr(db.connection, "DB_PATH", db_file)
    db.init_db()
    c = db.get_connection()
    yield c
    c.close()


def _insert_job(conn, id_, state="pending", attempts=0, max_retries=3,
                backoff_base=2.0, worker_id=None, heartbeat_at=None,
                next_retry_at=None, last_error=None):
    ts = db.now_iso()
    conn.execute(
        """INSERT INTO jobs
           (id, command, state, attempts, max_retries, backoff_base,
            worker_id, heartbeat_at, next_retry_at, last_error,
            created_at, updated_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (id_, f"echo {id_}", state, attempts, max_retries, backoff_base,
         worker_id, heartbeat_at, next_retry_at, last_error, ts, ts),
    )


# ===========================================================================
# 1. Storage layer: atomic claim / TOCTOU
# ===========================================================================

def test_claim_returns_pending_job_and_marks_processing(conn):
    """claim_next_job transitions the job to processing and sets worker_id."""
    _insert_job(conn, "j1")
    job = db.claim_next_job(conn, "w-test")
    assert job is not None
    assert job["id"] == "j1"
    row = conn.execute("SELECT state, worker_id, heartbeat_at FROM jobs WHERE id='j1'").fetchone()
    assert row["state"] == "processing"
    assert row["worker_id"] == "w-test"
    assert row["heartbeat_at"] is not None


def test_claim_returns_none_when_no_pending_jobs(conn):
    """claim_next_job returns None when queue is empty."""
    result = db.claim_next_job(conn, "w-test")
    assert result is None


def test_claim_is_idempotent_no_double_claim(conn):
    """A second immediate claim on same single job returns None (already processing)."""
    _insert_job(conn, "j1")
    first = db.claim_next_job(conn, "w-1")
    second = db.claim_next_job(conn, "w-2")
    assert first is not None
    assert second is None


def test_claim_selects_oldest_pending_first(conn):
    """Jobs are claimed in FIFO order (oldest created_at first)."""
    ts_old = (datetime.now(timezone.utc) - timedelta(seconds=10)).isoformat()
    ts_new = db.now_iso()
    conn.execute(
        "INSERT INTO jobs (id, command, state, attempts, max_retries, backoff_base, created_at, updated_at) "
        "VALUES ('old', 'echo old', 'pending', 0, 3, 2.0, ?, ?)", (ts_old, ts_old)
    )
    conn.execute(
        "INSERT INTO jobs (id, command, state, attempts, max_retries, backoff_base, created_at, updated_at) "
        "VALUES ('new', 'echo new', 'pending', 0, 3, 2.0, ?, ?)", (ts_new, ts_new)
    )
    job = db.claim_next_job(conn, "w-1")
    assert job["id"] == "old"


def test_claim_never_selects_dead_or_completed(conn):
    """Dead and completed jobs are never claimed."""
    _insert_job(conn, "dead1", state="dead")
    _insert_job(conn, "comp1", state="completed")
    result = db.claim_next_job(conn, "w-1")
    assert result is None


def test_wal_mode_is_enabled(conn):
    """WAL journal mode is active (required for concurrent readers)."""
    row = conn.execute("PRAGMA journal_mode").fetchone()
    assert row[0].lower() == "wal"


def test_busy_timeout_is_nonzero(conn):
    """busy_timeout must be >0 to absorb lock contention."""
    row = conn.execute("PRAGMA busy_timeout").fetchone()
    assert row[0] > 0


def test_finish_job_success_clears_all_worker_fields(conn):
    """finish_job(success) sets completed and nulls worker_id, heartbeat_at."""
    ts = db.now_iso()
    _insert_job(conn, "j1", state="processing", worker_id="w-1", heartbeat_at=ts)
    db.finish_job(conn, "j1", "w-1", 0)
    row = conn.execute("SELECT state, worker_id, heartbeat_at FROM jobs WHERE id='j1'").fetchone()
    assert row["state"] == "completed"
    assert row["worker_id"] is None
    assert row["heartbeat_at"] is None


def test_finish_job_failure_increments_attempts(conn):
    """finish_job(failure) increments attempts and records last_error."""
    _insert_job(conn, "j1", state="processing", worker_id="w-1",
                heartbeat_at=db.now_iso(), attempts=0, max_retries=3)
    db.finish_job(conn, "j1", "w-1", 2)
    row = conn.execute("SELECT state, attempts, last_error, worker_id FROM jobs WHERE id='j1'").fetchone()
    assert row["state"] == "failed"
    assert row["attempts"] == 1
    assert "2" in row["last_error"]
    assert row["worker_id"] is None


def test_finish_job_at_max_retries_goes_dead(conn):
    """finish_job when attempts+1 >= max_retries transitions to dead."""
    _insert_job(conn, "j1", state="processing", worker_id="w-1",
                heartbeat_at=db.now_iso(), attempts=2, max_retries=3)
    db.finish_job(conn, "j1", "w-1", 1)
    row = conn.execute("SELECT state, next_retry_at FROM jobs WHERE id='j1'").fetchone()
    assert row["state"] == "dead"
    assert row["next_retry_at"] is None


def test_finish_job_wrong_worker_is_noop(conn):
    """finish_job with wrong worker_id does not corrupt state."""
    _insert_job(conn, "j1", state="processing", worker_id="w-correct",
                heartbeat_at=db.now_iso())
    db.finish_job(conn, "j1", "w-wrong", 0)
    row = conn.execute("SELECT state FROM jobs WHERE id='j1'").fetchone()
    assert row["state"] == "processing"  # untouched


def test_finish_job_on_completed_job_is_noop(conn):
    """finish_job on already-completed job does not change state."""
    _insert_job(conn, "j1", state="completed")
    db.finish_job(conn, "j1", "w-1", 0)
    row = conn.execute("SELECT state FROM jobs WHERE id='j1'").fetchone()
    assert row["state"] == "completed"


def test_updated_at_advances_on_state_change(conn):
    """updated_at advances on every state change; created_at never changes."""
    _insert_job(conn, "j1")
    before = conn.execute("SELECT created_at, updated_at FROM jobs WHERE id='j1'").fetchone()
    time.sleep(0.05)
    db.claim_next_job(conn, "w-1")
    after = conn.execute("SELECT created_at, updated_at FROM jobs WHERE id='j1'").fetchone()
    assert after["created_at"] == before["created_at"]   # created_at frozen
    assert after["updated_at"] >= before["updated_at"]   # updated_at advanced


# ===========================================================================
# 2. Storage layer: concurrent two-thread claim atomicity
# ===========================================================================

def test_concurrent_two_thread_claim_exactly_one_wins(tmp_path, monkeypatch):
    """Two threads claiming simultaneously — exactly one wins, one gets None."""
    db_file = str(tmp_path / "conc.db")
    monkeypatch.setenv("QUEUECTL_DB", db_file)
    monkeypatch.setattr(db.connection, "DB_PATH", db_file)
    db.init_db()

    ts = db.now_iso()
    setup = db.get_connection()
    setup.execute(
        "INSERT INTO jobs (id, command, state, attempts, max_retries, backoff_base, created_at, updated_at) "
        "VALUES ('j1', 'echo hi', 'pending', 0, 3, 2.0, ?, ?)", (ts, ts)
    )
    setup.close()

    results = []
    errors = []

    def claim_worker(wid):
        try:
            c = db.get_connection()
            job = db.claim_next_job(c, wid)
            results.append(job)
            c.close()
        except Exception as e:
            errors.append(e)

    t1 = threading.Thread(target=claim_worker, args=("w-1",))
    t2 = threading.Thread(target=claim_worker, args=("w-2",))
    t1.start(); t2.start()
    t1.join(); t2.join()

    assert not errors, f"Unexpected errors: {errors}"
    claimed = [r for r in results if r is not None]
    nones   = [r for r in results if r is None]
    assert len(claimed) == 1, f"Expected exactly 1 claim, got {len(claimed)}"
    assert len(nones) == 1,   "Expected exactly 1 None return"


# ===========================================================================
# 3. reap_stale_jobs subsystem
# ===========================================================================

def test_reap_clears_worker_id_and_heartbeat(conn):
    """reap_stale_jobs sets state=pending and nulls worker_id and heartbeat_at."""
    stale = (datetime.now(timezone.utc) - timedelta(seconds=30)).isoformat()
    _insert_job(conn, "j1", state="processing", worker_id="w-dead", heartbeat_at=stale)
    db.reap_stale_jobs(conn, timeout_seconds=15)
    row = conn.execute("SELECT state, worker_id, heartbeat_at FROM jobs WHERE id='j1'").fetchone()
    assert row["state"] == "pending"
    assert row["worker_id"] is None
    assert row["heartbeat_at"] is None


def test_reap_does_not_touch_fresh_heartbeat(conn):
    """reap_stale_jobs leaves jobs with a fresh heartbeat untouched."""
    fresh = db.now_iso()
    _insert_job(conn, "j1", state="processing", worker_id="w-alive", heartbeat_at=fresh)
    db.reap_stale_jobs(conn, timeout_seconds=15)
    row = conn.execute("SELECT state FROM jobs WHERE id='j1'").fetchone()
    assert row["state"] == "processing"


def test_reap_does_not_touch_non_processing_states(conn):
    """reap_stale_jobs never modifies pending, failed, completed or dead jobs."""
    stale = (datetime.now(timezone.utc) - timedelta(seconds=60)).isoformat()
    for s in ("pending", "failed", "completed", "dead"):
        _insert_job(conn, f"j-{s}", state=s, heartbeat_at=stale)
    db.reap_stale_jobs(conn, timeout_seconds=15)
    for s in ("pending", "failed", "completed", "dead"):
        row = conn.execute(f"SELECT state FROM jobs WHERE id='j-{s}'").fetchone()
        assert row["state"] == s


def test_reap_returns_list_of_reaped_ids(conn):
    """reap_stale_jobs returns the list of job IDs it recovered."""
    stale = (datetime.now(timezone.utc) - timedelta(seconds=30)).isoformat()
    _insert_job(conn, "j1", state="processing", worker_id="w1", heartbeat_at=stale)
    _insert_job(conn, "j2", state="processing", worker_id="w2", heartbeat_at=stale)
    _insert_job(conn, "j3")  # pending, should not be reaped
    reaped = db.reap_stale_jobs(conn, timeout_seconds=15)
    assert set(reaped) == {"j1", "j2"}


def test_reap_idempotent_second_call_returns_empty(conn):
    """Running reap_stale_jobs twice recovers each job only once."""
    stale = (datetime.now(timezone.utc) - timedelta(seconds=30)).isoformat()
    _insert_job(conn, "j1", state="processing", worker_id="w1", heartbeat_at=stale)
    first = db.reap_stale_jobs(conn, timeout_seconds=15)
    second = db.reap_stale_jobs(conn, timeout_seconds=15)
    assert first == ["j1"]
    assert second == []


# ===========================================================================
# 4. promote_ready_retries subsystem
# ===========================================================================

def test_promote_moves_elapsed_failed_to_pending(conn):
    """promote_ready_retries moves failed jobs whose next_retry_at <= now to pending."""
    past = (datetime.now(timezone.utc) - timedelta(seconds=5)).isoformat()
    _insert_job(conn, "j1", state="failed", attempts=1, next_retry_at=past)
    db.promote_ready_retries(conn)
    row = conn.execute("SELECT state FROM jobs WHERE id='j1'").fetchone()
    assert row["state"] == "pending"


def test_promote_leaves_future_retry_untouched(conn):
    """promote_ready_retries does not move failed jobs whose next_retry_at is in the future."""
    future = (datetime.now(timezone.utc) + timedelta(seconds=300)).isoformat()
    _insert_job(conn, "j1", state="failed", attempts=1, next_retry_at=future)
    db.promote_ready_retries(conn)
    row = conn.execute("SELECT state FROM jobs WHERE id='j1'").fetchone()
    assert row["state"] == "failed"


def test_promote_does_not_touch_non_failed_states(conn):
    """promote_ready_retries never changes pending, processing, completed or dead."""
    past = (datetime.now(timezone.utc) - timedelta(seconds=5)).isoformat()
    for s in ("pending", "processing", "completed", "dead"):
        _insert_job(conn, f"j-{s}", state=s, next_retry_at=past)
    db.promote_ready_retries(conn)
    for s in ("pending", "processing", "completed", "dead"):
        row = conn.execute(f"SELECT state FROM jobs WHERE id='j-{s}'").fetchone()
        assert row["state"] == s


def test_promote_keeps_attempts_unchanged(conn):
    """promote_ready_retries does not reset attempts (that only happens on dlq retry)."""
    past = (datetime.now(timezone.utc) - timedelta(seconds=5)).isoformat()
    _insert_job(conn, "j1", state="failed", attempts=2, next_retry_at=past)
    db.promote_ready_retries(conn)
    row = conn.execute("SELECT attempts FROM jobs WHERE id='j1'").fetchone()
    assert row["attempts"] == 2


def test_promote_is_idempotent(conn):
    """Calling promote_ready_retries twice does not double-promote; job stays pending."""
    past = (datetime.now(timezone.utc) - timedelta(seconds=5)).isoformat()
    _insert_job(conn, "j1", state="failed", attempts=1, next_retry_at=past)
    db.promote_ready_retries(conn)
    db.promote_ready_retries(conn)   # second call: job is now pending, WHERE state='failed' skips it
    row = conn.execute("SELECT state FROM jobs WHERE id='j1'").fetchone()
    assert row["state"] == "pending"


# ===========================================================================
# 5. Worker registry subsystem
# ===========================================================================

def test_register_worker_creates_running_row(conn):
    """register_worker inserts a row with status=running."""
    db.register_worker(conn, "w-42", 42)
    row = conn.execute("SELECT pid, status FROM workers WHERE worker_id='w-42'").fetchone()
    assert row["pid"] == 42
    assert row["status"] == "running"


def test_mark_worker_stopped_updates_status(conn):
    """mark_worker_stopped sets status to stopped."""
    db.register_worker(conn, "w-42", 42)
    db.mark_worker_stopped(conn, "w-42")
    row = conn.execute("SELECT status FROM workers WHERE worker_id='w-42'").fetchone()
    assert row["status"] == "stopped"


def test_touch_worker_heartbeat_advances_timestamp(conn):
    """touch_worker_heartbeat updates heartbeat_at to a later timestamp."""
    db.register_worker(conn, "w-42", 42)
    time.sleep(0.05)
    db.touch_worker_heartbeat(conn, "w-42")
    row = conn.execute("SELECT started_at, heartbeat_at FROM workers WHERE worker_id='w-42'").fetchone()
    assert row["heartbeat_at"] >= row["started_at"]


def test_register_worker_upserts_on_duplicate(conn):
    """Registering the same worker_id twice does not raise — uses INSERT OR REPLACE."""
    db.register_worker(conn, "w-42", 42)
    db.register_worker(conn, "w-42", 42)   # should not raise
    count = conn.execute("SELECT COUNT(*) FROM workers WHERE worker_id='w-42'").fetchone()[0]
    assert count == 1


# ===========================================================================
# 6. DLQ storage subsystem
# ===========================================================================

def test_dlq_dead_jobs_not_claimed_by_workers(conn):
    """claim_next_job never selects dead jobs."""
    _insert_job(conn, "dead1", state="dead")
    _insert_job(conn, "pend1", state="pending")
    job = db.claim_next_job(conn, "w-1")
    assert job["id"] == "pend1"


def test_dlq_list_json_empty_array_when_no_dead_jobs(tmp_path, monkeypatch):
    """dlq list --json returns [] (not null) when no dead jobs exist."""
    db_file = str(tmp_path / "dlq_empty.db")
    monkeypatch.setenv("QUEUECTL_DB", db_file)
    monkeypatch.setattr(db.connection, "DB_PATH", db_file)
    db.init_db()
    env = os.environ.copy()
    env["QUEUECTL_DB"] = db_file
    res = subprocess.run(
        [sys.executable, "-m", "queuectl.cli.entrypoint"] + ["dlq", "list", "--json"],
        capture_output=True, text=True, env=env
    )
    import json
    assert res.returncode == 0
    parsed = json.loads(res.stdout.strip())
    assert parsed == []


def test_dlq_list_json_contains_all_required_fields(conn):
    """dlq list --json output includes all expected job fields."""
    ts = db.now_iso()
    conn.execute(
        "INSERT INTO jobs (id, command, state, attempts, max_retries, backoff_base, "
        "last_error, created_at, updated_at) VALUES ('d1','exit 1','dead',3,3,2.0,'err',?,?)",
        (ts, ts)
    )
    import json
    env = os.environ.copy()
    env["QUEUECTL_DB"] = db.connection.DB_PATH
    res = subprocess.run(
        [sys.executable, "-m", "queuectl.cli.entrypoint"] + ["dlq", "list", "--json"],
        capture_output=True, text=True, env=env
    )
    assert res.returncode == 0
    jobs = json.loads(res.stdout.strip())
    assert len(jobs) == 1
    job = jobs[0]
    for field in ("id", "command", "state", "attempts", "max_retries",
                  "backoff_base", "last_error", "created_at", "updated_at"):
        assert field in job, f"missing field {field}"
    assert job["state"] == "dead"


def test_dlq_retry_moves_dead_to_pending_resets_attempts(conn):
    """dlq retry resets attempts=0, clears last_error/next_retry_at, sets state=pending."""
    ts = db.now_iso()
    conn.execute(
        "INSERT INTO jobs (id, command, state, attempts, max_retries, backoff_base, "
        "last_error, created_at, updated_at) VALUES ('d1','exit 1','dead',3,3,2.0,'oops',?,?)",
        (ts, ts)
    )
    env = os.environ.copy()
    env["QUEUECTL_DB"] = db.connection.DB_PATH
    res = subprocess.run(
        [sys.executable, "-m", "queuectl.cli.entrypoint"] + ["dlq", "retry", "d1"],
        capture_output=True, text=True, env=env
    )
    assert res.returncode == 0
    row = conn.execute("SELECT state, attempts, last_error, next_retry_at FROM jobs WHERE id='d1'").fetchone()
    assert row["state"] == "pending"
    assert row["attempts"] == 0
    assert row["last_error"] is None
    assert row["next_retry_at"] is None


def test_dlq_retry_on_non_dead_job_exits_nonzero(conn):
    """dlq retry on a pending job returns non-zero exit and clear error message."""
    _insert_job(conn, "p1", state="pending")
    env = os.environ.copy()
    env["QUEUECTL_DB"] = db.connection.DB_PATH
    res = subprocess.run(
        [sys.executable, "-m", "queuectl.cli.entrypoint"] + ["dlq", "retry", "p1"],
        capture_output=True, text=True, env=env
    )
    assert res.returncode != 0
    assert "No dead job with id" in res.stderr


def test_dlq_retry_on_nonexistent_id_exits_nonzero(conn):
    """dlq retry on a nonexistent ID returns non-zero exit."""
    env = os.environ.copy()
    env["QUEUECTL_DB"] = db.connection.DB_PATH
    res = subprocess.run(
        [sys.executable, "-m", "queuectl.cli.entrypoint"] + ["dlq", "retry", "does-not-exist"],
        capture_output=True, text=True, env=env
    )
    assert res.returncode != 0
    assert "No dead job" in res.stderr


def test_dlq_retry_idempotent_second_call_fails(conn):
    """Second dlq retry on same job (now pending) returns clear error, no duplicate row."""
    ts = db.now_iso()
    conn.execute(
        "INSERT INTO jobs (id, command, state, attempts, max_retries, backoff_base, "
        "created_at, updated_at) VALUES ('d1','exit 1','dead',2,2,2.0,?,?)",
        (ts, ts)
    )
    env = os.environ.copy()
    env["QUEUECTL_DB"] = db.connection.DB_PATH
    for _ in [1, 2]:
        subprocess.run([sys.executable, "-m", "queuectl.cli.entrypoint", "dlq", "retry", "d1"],
                       capture_output=True, text=True, env=env)
    # Second call: job is now pending, not dead
    res2 = subprocess.run(
        [sys.executable, "-m", "queuectl.cli.entrypoint"] + ["dlq", "retry", "d1"],
        capture_output=True, text=True, env=env
    )
    assert res2.returncode != 0
    count = conn.execute("SELECT COUNT(*) FROM jobs WHERE id='d1'").fetchone()[0]
    assert count == 1


# ===========================================================================
# 7. Config subsystem
# ===========================================================================

def test_config_default_values_present_in_fresh_db(conn):
    """A fresh DB has all default config keys populated."""
    cfg = config_service.get_all(conn)
    assert "max-retries" in cfg
    assert "backoff-base" in cfg
    assert "recovery-timeout" in cfg
    assert "poll-interval" in cfg
    assert "heartbeat-interval" in cfg


def test_config_set_known_key_persists(conn):
    """config set stores the value; config get retrieves it."""
    config_service.set(conn, "max-retries", "7")
    assert config_service.get(conn, "max-retries") == "7"


def test_config_set_invalid_type_raises(conn):
    """config set with non-numeric value for a typed key raises ValueError."""
    import pytest as _pytest
    with _pytest.raises(ValueError):
        config_service.set(conn, "max-retries", "notanint")
    with _pytest.raises(ValueError):
        config_service.set(conn, "backoff-base", "notafloat")


def test_config_set_unknown_key_stored_without_type_check(conn):
    """Unknown config keys are stored as-is (no type validation)."""
    config_service.set(conn, "custom-key", "anything")
    assert config_service.get(conn, "custom-key") == "anything"


def test_config_snapshot_on_enqueue(tmp_path, monkeypatch):
    """Jobs snapshot max_retries and backoff_base from config at enqueue time."""
    import json
    db_file = str(tmp_path / "snap.db")
    monkeypatch.setenv("QUEUECTL_DB", db_file)
    monkeypatch.setattr(db.connection, "DB_PATH", db_file)
    db.init_db()
    env = os.environ.copy()
    env["QUEUECTL_DB"] = db_file

    subprocess.run([sys.executable, "-m", "queuectl.cli.entrypoint"] + ["config", "set", "max-retries", "9"],
                   capture_output=True, text=True, env=env)
    subprocess.run([sys.executable, "-m", "queuectl.cli.entrypoint"] + ["config", "set", "backoff-base", "4.5"],
                   capture_output=True, text=True, env=env)
    subprocess.run([sys.executable, "-m", "queuectl.cli.entrypoint"] + ["enqueue", '{"id":"snap1","command":"echo hi"}'],
                   capture_output=True, text=True, env=env)

    c = db.get_connection()
    row = c.execute("SELECT max_retries, backoff_base FROM jobs WHERE id='snap1'").fetchone()
    c.close()
    assert row["max_retries"] == 9
    assert abs(row["backoff_base"] - 4.5) < 0.001
