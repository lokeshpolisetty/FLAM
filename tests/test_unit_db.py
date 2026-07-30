"""
test_unit_db.py — Unit tests for database layer logic.

Targets pure logic, helper functions, and database functions deterministically:
state transition rules, backoff calculation, retry boundaries, config
serialization, and time arithmetic.
"""

import json
import os
import subprocess
import sys
import sqlite3
import pytest
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

from queuectl import database as db
from queuectl.config import settings as config_service

@pytest.fixture
def tmp_db(tmp_path, monkeypatch):
    """Fixture providing an isolated SQLite database for unit tests."""
    db_file = os.path.join(tmp_path, "unit_test.db")
    monkeypatch.setattr(db.connection, "DB_PATH", db_file)
    db.init_db()
    conn = db.get_connection()
    yield conn
    conn.close()


# ============================================================================
# 1. Job Schema Validation
# ============================================================================

def test_schema_valid_minimal_job(tmp_db):
    """Valid minimal job schema defaults correctly."""
    ts = db.now_iso()
    tmp_db.execute(
        "INSERT INTO jobs (id, command, created_at, updated_at) VALUES (?, ?, ?, ?)",
        ("j1", "echo hi", ts, ts)
    )
    row = dict(tmp_db.execute("SELECT * FROM jobs WHERE id='j1'").fetchone())
    assert row["id"] == "j1"
    assert row["command"] == "echo hi"
    assert row["state"] == "pending"
    assert row["attempts"] == 0
    assert row["max_retries"] == 3
    assert row["backoff_base"] == 2.0
    assert row["worker_id"] is None
    assert row["next_retry_at"] is None
    assert row["heartbeat_at"] is None
    assert row["last_error"] is None


def test_schema_missing_required_fields_cli(tmp_db):
    """Missing id or command raises validation error."""
    env = os.environ.copy()
    env["QUEUECTL_DB"] = db.connection.DB_PATH

    res_no_id = subprocess.run(
        [sys.executable, "-m", "queuectl", "enqueue", json.dumps({"command": "echo hi"})],
        capture_output=True, text=True, env=env
    )
    assert res_no_id.returncode != 0
    assert "Job JSON must include at least 'id' and 'command'" in res_no_id.stderr

    res_no_cmd = subprocess.run(
        [sys.executable, "-m", "queuectl", "enqueue", json.dumps({"id": "j1"})],
        capture_output=True, text=True, env=env
    )
    assert res_no_cmd.returncode != 0
    assert "Job JSON must include at least 'id' and 'command'" in res_no_cmd.stderr


def test_schema_max_retries_zero(tmp_db):
    """max_retries = 0 is stored correctly."""
    ts = db.now_iso()
    tmp_db.execute(
        "INSERT INTO jobs (id, command, max_retries, created_at, updated_at) VALUES (?, ?, 0, ?, ?)",
        ("j_zero", "echo hi", ts, ts)
    )
    row = tmp_db.execute("SELECT max_retries FROM jobs WHERE id='j_zero'").fetchone()
    assert row["max_retries"] == 0


def test_schema_unicode_and_special_chars(tmp_db):
    """Job IDs and commands with Unicode and special characters store losslessly."""
    ts = db.now_iso()
    cmd = "echo 'hello \"world\"' && echo 🚀"
    tmp_db.execute(
        "INSERT INTO jobs (id, command, created_at, updated_at) VALUES (?, ?, ?, ?)",
        ("j-🚀-special", cmd, ts, ts)
    )
    row = tmp_db.execute("SELECT command FROM jobs WHERE id='j-🚀-special'").fetchone()
    assert row["command"] == cmd


def test_schema_large_command_payload(tmp_db):
    """Large command string (20k chars) is stored without truncation."""
    ts = db.now_iso()
    long_cmd = "echo " + "x" * 20000
    tmp_db.execute(
        "INSERT INTO jobs (id, command, created_at, updated_at) VALUES (?, ?, ?, ?)",
        ("j_long", long_cmd, ts, ts)
    )
    row = tmp_db.execute("SELECT command FROM jobs WHERE id='j_long'").fetchone()
    assert len(row["command"]) == len(long_cmd)


# ============================================================================
# 2. State Transition Rules
# ============================================================================

def test_transition_pending_to_processing(tmp_db):
    """pending -> processing via claim_next_job."""
    ts = db.now_iso()
    tmp_db.execute(
        "INSERT INTO jobs (id, command, created_at, updated_at) VALUES (?, ?, ?, ?)",
        ("j_claim", "echo hi", ts, ts)
    )
    claimed = db.claim_next_job(tmp_db, worker_id="w1")
    assert claimed is not None
    assert claimed["id"] == "j_claim"

    row = tmp_db.execute("SELECT * FROM jobs WHERE id='j_claim'").fetchone()
    assert row["state"] == "processing"
    assert row["worker_id"] == "w1"
    assert row["heartbeat_at"] is not None


def test_transition_processing_to_completed(tmp_db):
    """processing -> completed via finish_job with exit code 0."""
    ts = db.now_iso()
    tmp_db.execute(
        "INSERT INTO jobs (id, command, state, worker_id, heartbeat_at, created_at, updated_at) "
        "VALUES ('j_comp', 'echo hi', 'processing', 'w1', ?, ?, ?)",
        (ts, ts, ts)
    )
    db.finish_job(tmp_db, job_id="j_comp", worker_id="w1", returncode=0)
    row = tmp_db.execute("SELECT * FROM jobs WHERE id='j_comp'").fetchone()
    assert row["state"] == "completed"
    assert row["worker_id"] is None
    assert row["heartbeat_at"] is None


def test_transition_processing_to_failed(tmp_db):
    """processing -> failed via finish_job with non-zero exit code when attempts < max_retries."""
    ts = db.now_iso()
    tmp_db.execute(
        "INSERT INTO jobs (id, command, state, attempts, max_retries, backoff_base, worker_id, created_at, updated_at) "
        "VALUES ('j_fail', 'exit 1', 'processing', 0, 3, 2.0, 'w1', ?, ?)",
        (ts, ts)
    )
    db.finish_job(tmp_db, job_id="j_fail", worker_id="w1", returncode=1)
    row = tmp_db.execute("SELECT * FROM jobs WHERE id='j_fail'").fetchone()
    assert row["state"] == "failed"
    assert row["attempts"] == 1
    assert row["worker_id"] is None
    assert row["next_retry_at"] is not None
    assert "exited with code 1" in row["last_error"]


def test_transition_processing_to_dead(tmp_db):
    """processing -> dead via finish_job when attempts reach max_retries."""
    ts = db.now_iso()
    tmp_db.execute(
        "INSERT INTO jobs (id, command, state, attempts, max_retries, backoff_base, worker_id, created_at, updated_at) "
        "VALUES ('j_dead', 'exit 1', 'processing', 2, 3, 2.0, 'w1', ?, ?)",
        (ts, ts)
    )
    db.finish_job(tmp_db, job_id="j_dead", worker_id="w1", returncode=1)
    row = tmp_db.execute("SELECT * FROM jobs WHERE id='j_dead'").fetchone()
    assert row["state"] == "dead"
    assert row["attempts"] == 3
    assert row["next_retry_at"] is None


def test_transition_failed_to_pending_on_delay_elapsed(tmp_db):
    """failed -> pending when next_retry_at <= now."""
    past_ts = (datetime.now(timezone.utc) - timedelta(seconds=10)).isoformat()
    ts = db.now_iso()
    tmp_db.execute(
        "INSERT INTO jobs (id, command, state, attempts, next_retry_at, created_at, updated_at) "
        "VALUES ('j_retry', 'echo hi', 'failed', 1, ?, ?, ?)",
        (past_ts, ts, ts)
    )
    db.promote_ready_retries(tmp_db)
    row = tmp_db.execute("SELECT * FROM jobs WHERE id='j_retry'").fetchone()
    assert row["state"] == "pending"


# ============================================================================
# 3. Backoff Calculation (base ^ attempts)
# ============================================================================

@pytest.mark.parametrize("base,attempts,expected_delay", [
    (2.0, 1, 2.0),
    (2.0, 2, 4.0),
    (2.0, 3, 8.0),
    (3.0, 2, 9.0),
    (1.0, 5, 1.0),
])
def test_backoff_formula(tmp_db, base, attempts, expected_delay):
    """Exponential backoff delay formula calculation: base ^ attempts."""
    ts = db.now_iso()
    tmp_db.execute(
        "INSERT INTO jobs (id, command, state, attempts, max_retries, backoff_base, worker_id, created_at, updated_at) "
        "VALUES ('j_bk', 'exit 1', 'processing', ?, ?, ?, 'w1', ?, ?)",
        (attempts - 1, attempts + 5, base, ts, ts)
    )
    start_time = datetime.now(timezone.utc)
    db.finish_job(tmp_db, job_id="j_bk", worker_id="w1", returncode=1)
    row = tmp_db.execute("SELECT next_retry_at FROM jobs WHERE id='j_bk'").fetchone()
    next_retry = datetime.fromisoformat(row["next_retry_at"])
    actual_delay = (next_retry - start_time).total_seconds()
    assert abs(actual_delay - expected_delay) < 1.0


# ============================================================================
# 4. Max Retry Boundary Logic
# ============================================================================

def test_max_retries_boundary_zero(tmp_db):
    """max_retries = 0 immediately moves to dead on first failure."""
    ts = db.now_iso()
    tmp_db.execute(
        "INSERT INTO jobs (id, command, state, attempts, max_retries, worker_id, created_at, updated_at) "
        "VALUES ('j_m0', 'exit 1', 'processing', 0, 0, 'w1', ?, ?)",
        (ts, ts)
    )
    db.finish_job(tmp_db, job_id="j_m0", worker_id="w1", returncode=1)
    row = tmp_db.execute("SELECT state, attempts FROM jobs WHERE id='j_m0'").fetchone()
    assert row["state"] == "dead"
    assert row["attempts"] == 1


def test_max_retries_boundary_one(tmp_db):
    """max_retries = 1 allows exactly 1 failure (failed state), second failure moves to dead."""
    ts = db.now_iso()
    tmp_db.execute(
        "INSERT INTO jobs (id, command, state, attempts, max_retries, worker_id, created_at, updated_at) "
        "VALUES ('j_m1', 'exit 1', 'processing', 0, 1, 'w1', ?, ?)",
        (ts, ts)
    )
    db.finish_job(tmp_db, job_id="j_m1", worker_id="w1", returncode=1)
    row = tmp_db.execute("SELECT state, attempts FROM jobs WHERE id='j_m1'").fetchone()
    assert row["state"] == "dead"
    assert row["attempts"] == 1


# ============================================================================
# 5. DLQ Retry Policy
# ============================================================================

def test_dlq_retry_resets_attempts(tmp_db):
    """dlq retry command resets attempts to 0 and state to pending."""
    ts = db.now_iso()
    tmp_db.execute(
        "INSERT INTO jobs (id, command, state, attempts, max_retries, last_error, created_at, updated_at) "
        "VALUES ('j_dlq', 'exit 1', 'dead', 3, 3, 'err', ?, ?)",
        (ts, ts)
    )
    env = os.environ.copy()
    env["QUEUECTL_DB"] = db.connection.DB_PATH
    res = subprocess.run(
        [sys.executable, "-m", "queuectl", "dlq", "retry", "j_dlq"],
        capture_output=True, text=True, env=env
    )
    assert res.returncode == 0
    row = tmp_db.execute("SELECT * FROM jobs WHERE id='j_dlq'").fetchone()
    assert row["state"] == "pending"
    assert row["attempts"] == 0
    assert row["last_error"] is None
    assert row["next_retry_at"] is None


def test_dlq_retry_non_dead_job_fails(tmp_db):
    """dlq retry fails when attempted on a pending or completed job."""
    ts = db.now_iso()
    tmp_db.execute(
        "INSERT INTO jobs (id, command, state, created_at, updated_at) VALUES ('j_pend', 'echo hi', 'pending', ?, ?)",
        (ts, ts)
    )
    env = os.environ.copy()
    env["QUEUECTL_DB"] = db.connection.DB_PATH
    res = subprocess.run(
        [sys.executable, "-m", "queuectl", "dlq", "retry", "j_pend"],
        capture_output=True, text=True, env=env
    )
    assert res.returncode != 0
    assert "No dead job" in res.stderr


# ============================================================================
# 6. Config Serialization / Deserialization
# ============================================================================

def test_config_get_set(tmp_db):
    """Setting and getting config values works correctly with type validation."""
    config_service.set(tmp_db, "max-retries", "5")
    assert config_service.get(tmp_db, "max-retries") == "5"

    config_service.set(tmp_db, "backoff-base", "3.5")
    assert config_service.get(tmp_db, "backoff-base") == "3.5"


def test_config_invalid_type_raises_value_error(tmp_db):
    """Setting invalid non-numeric value for integer or float config key raises ValueError."""
    with pytest.raises(ValueError):
        config_service.set(tmp_db, "max-retries", "invalid_int")

    with pytest.raises(ValueError):
        config_service.set(tmp_db, "backoff-base", "not_a_float")


# ============================================================================
# 7. Time Arithmetic and Lease Expiry Calculations
# ============================================================================

def test_reap_stale_jobs(tmp_db):
    """reap_stale_jobs reclaims jobs in processing whose heartbeat is older than timeout."""
    stale_ts = (datetime.now(timezone.utc) - timedelta(seconds=20)).isoformat()
    fresh_ts = datetime.now(timezone.utc).isoformat()

    tmp_db.execute(
        "INSERT INTO jobs (id, command, state, worker_id, heartbeat_at, created_at, updated_at) "
        "VALUES ('j_stale', 'sleep 100', 'processing', 'w1', ?, ?, ?)",
        (stale_ts, stale_ts, stale_ts)
    )
    tmp_db.execute(
        "INSERT INTO jobs (id, command, state, worker_id, heartbeat_at, created_at, updated_at) "
        "VALUES ('j_fresh', 'sleep 100', 'processing', 'w2', ?, ?, ?)",
        (fresh_ts, fresh_ts, fresh_ts)
    )

    reaped = db.reap_stale_jobs(tmp_db, timeout_seconds=15)
    assert reaped == ["j_stale"]

    row_stale = tmp_db.execute("SELECT state, worker_id, heartbeat_at FROM jobs WHERE id='j_stale'").fetchone()
    assert row_stale["state"] == "pending"
    assert row_stale["worker_id"] is None

    row_fresh = tmp_db.execute("SELECT state, worker_id FROM jobs WHERE id='j_fresh'").fetchone()
    assert row_fresh["state"] == "processing"
    assert row_fresh["worker_id"] == "w2"


# ============================================================================
# 8. CLI Argument Parsing and Error Messages
# ============================================================================

def test_cli_execution_contract(tmp_db):
    """CLI returns valid status output."""
    env = os.environ.copy()
    env["QUEUECTL_DB"] = db.connection.DB_PATH
    res = subprocess.run([sys.executable, "-m", "queuectl", "status"], capture_output=True, text=True, env=env)
    assert res.returncode == 0
    assert "Job states:" in res.stdout


def test_cli_invalid_command():
    """Invalid CLI command returns non-zero exit code."""
    res = subprocess.run([sys.executable, "-m", "queuectl", "unknowncommand"], capture_output=True, text=True)
    assert res.returncode != 0


def test_cli_invalid_json_enqueue(tmp_db):
    """Invalid JSON payload to enqueue outputs error to stderr and returns non-zero exit code."""
    env = os.environ.copy()
    env["QUEUECTL_DB"] = db.connection.DB_PATH
    res = subprocess.run([sys.executable, "-m", "queuectl", "enqueue", "{broken json"], capture_output=True, text=True, env=env)
    assert res.returncode != 0
    assert "Invalid JSON" in res.stderr
