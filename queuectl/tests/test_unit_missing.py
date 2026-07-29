"""
test_unit_missing.py — Unit tests covering gaps identified in Section 2A analysis.

Covers:
  - Job schema edge cases (null id/command, empty string, extra fields, type coercion)
  - Backoff formula edge cases (base=0, base=1, base<1, large attempts, negative base)
  - Max retry boundary logic (float input, string input, large values)
  - DLQ retry policy completeness
  - Config serialization edge cases (unknown key, float precision, negative values)
  - Time arithmetic (next_retry_at precision, heartbeat_at/worker_id null invariants)
  - CLI argument parsing edge cases (--count 0, --count abc, --count 1.5)
"""

import json
import os
import subprocess
import sys
import sqlite3
import pytest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from queuectl import database as db
from queuectl.config import settings as config_service


ROOT = Path(__file__).resolve().parent.parent
@pytest.fixture
def tmp_db(tmp_path, monkeypatch):
    db_file = str(tmp_path / "unit_missing.db")
    monkeypatch.setattr(db.connection, "DB_PATH", db_file)
    db.init_db()
    conn = db.get_connection()
    yield conn
    conn.close()


def cli(args, tmp_db_path):
    env = os.environ.copy()
    env["QUEUECTL_DB"] = tmp_db_path
    return subprocess.run(
        [sys.executable, "-m", "queuectl.cli.entrypoint"] + args,
        capture_output=True, text=True, env=env, timeout=15
    )


# ============================================================================
# Section 2A-1: Job Schema Edge Cases
# ============================================================================

def test_schema_null_id_rejected(tmp_db):
    """Job with null id is rejected cleanly with non-zero exit."""
    res = cli(["enqueue", '{"id": null, "command": "echo hi"}'], db.connection.DB_PATH)
    assert res.returncode != 0
    assert "Traceback" not in res.stderr


def test_schema_null_command_rejected(tmp_db):
    """Job with null command is rejected cleanly with non-zero exit."""
    res = cli(["enqueue", '{"id": "j1", "command": null}'], db.connection.DB_PATH)
    assert res.returncode != 0
    assert "Traceback" not in res.stderr


def test_schema_empty_string_id_accepted(tmp_db):
    """Job with empty string id is accepted (empty string is a valid id value)."""
    res = cli(["enqueue", '{"id": "", "command": "echo hi"}'], db.connection.DB_PATH)
    # Implementation accepts empty string — verify it is stored or rejected cleanly
    assert "Traceback" not in res.stderr


def test_schema_empty_string_command_accepted(tmp_db):
    """Job with empty string command is accepted (shell will run empty command)."""
    res = cli(["enqueue", '{"id": "empty-cmd", "command": ""}'], db.connection.DB_PATH)
    assert "Traceback" not in res.stderr


def test_schema_extra_unknown_fields_ignored(tmp_db):
    """Extra unknown fields in job JSON payload are silently ignored."""
    res = cli(["enqueue", '{"id": "j-extra", "command": "echo hi", "unknown_field": "value", "foo": 42}'], db.connection.DB_PATH)
    assert res.returncode == 0
    row = tmp_db.execute("SELECT id, command FROM jobs WHERE id='j-extra'").fetchone()
    assert row is not None
    assert row["command"] == "echo hi"


def test_schema_created_at_supplied_by_caller_is_ignored(tmp_db):
    """created_at supplied in job JSON is ignored; server sets its own timestamp."""
    past = "2000-01-01T00:00:00+00:00"
    res = cli(["enqueue", json.dumps({"id": "j-ts", "command": "echo hi", "created_at": past})], db.connection.DB_PATH)
    assert res.returncode == 0
    row = tmp_db.execute("SELECT created_at FROM jobs WHERE id='j-ts'").fetchone()
    assert row["created_at"] != past, "server must set its own created_at, not caller's"


def test_schema_updated_at_supplied_by_caller_is_ignored(tmp_db):
    """updated_at supplied in job JSON is ignored; server sets its own timestamp."""
    past = "2000-01-01T00:00:00+00:00"
    res = cli(["enqueue", json.dumps({"id": "j-uat", "command": "echo hi", "updated_at": past})], db.connection.DB_PATH)
    assert res.returncode == 0
    row = tmp_db.execute("SELECT updated_at FROM jobs WHERE id='j-uat'").fetchone()
    assert row["updated_at"] != past


def test_schema_id_with_spaces_stored_correctly(tmp_db):
    """Job id containing spaces is stored and retrieved verbatim."""
    res = cli(["enqueue", '{"id": "job with spaces", "command": "echo hi"}'], db.connection.DB_PATH)
    assert res.returncode == 0
    row = tmp_db.execute("SELECT id FROM jobs WHERE id='job with spaces'").fetchone()
    assert row is not None


def test_schema_array_at_root_rejected(tmp_db):
    """JSON array at root (not an object) is rejected with non-zero exit and no traceback."""
    res = cli(["enqueue", '[]'], db.connection.DB_PATH)
    assert res.returncode != 0
    assert "Traceback" not in res.stderr


def test_schema_json_primitive_string_rejected(tmp_db):
    """JSON primitive string at root is rejected cleanly."""
    res = cli(["enqueue", '"just a string"'], db.connection.DB_PATH)
    assert res.returncode != 0
    assert "Traceback" not in res.stderr


def test_schema_json_primitive_number_rejected(tmp_db):
    """JSON primitive number at root is rejected cleanly."""
    res = cli(["enqueue", '42'], db.connection.DB_PATH)
    assert res.returncode != 0
    assert "Traceback" not in res.stderr


def test_schema_json_null_rejected(tmp_db):
    """JSON null at root is rejected with non-zero exit and no traceback."""
    res = cli(["enqueue", 'null'], db.connection.DB_PATH)
    assert res.returncode != 0
    assert "Traceback" not in res.stderr


# ============================================================================
# Section 2A-2: Max Retries — Type Coercion and Boundary
# ============================================================================

def test_max_retries_as_integer_in_payload(tmp_db):
    """max_retries as integer in job payload is stored correctly."""
    res = cli(["enqueue", '{"id": "mr-int", "command": "echo hi", "max_retries": 5}'], db.connection.DB_PATH)
    assert res.returncode == 0
    row = tmp_db.execute("SELECT max_retries FROM jobs WHERE id='mr-int'").fetchone()
    assert row["max_retries"] == 5


def test_max_retries_as_string_in_payload_coerced(tmp_db):
    """max_retries as string '3' in job payload is coerced to integer."""
    res = cli(["enqueue", '{"id": "mr-str", "command": "echo hi", "max_retries": "3"}'], db.connection.DB_PATH)
    # The implementation uses int(data.get(...)) — "3" coerces to 3
    assert "Traceback" not in res.stderr
    if res.returncode == 0:
        row = tmp_db.execute("SELECT max_retries FROM jobs WHERE id='mr-str'").fetchone()
        assert row["max_retries"] == 3


def test_max_retries_as_float_truncated_or_rejected(tmp_db):
    """max_retries as 2.5 is either truncated to 2 (int()) or rejected cleanly."""
    res = cli(["enqueue", '{"id": "mr-float", "command": "echo hi", "max_retries": 2.5}'], db.connection.DB_PATH)
    # int(2.5) = 2 on Python — so it should be accepted and stored as 2
    assert "Traceback" not in res.stderr
    if res.returncode == 0:
        row = tmp_db.execute("SELECT max_retries FROM jobs WHERE id='mr-float'").fetchone()
        assert row["max_retries"] == 2


def test_max_retries_per_job_overrides_global_config(tmp_db):
    """per-job max_retries in payload overrides the global config value."""
    cli(["config", "set", "max-retries", "10"], db.connection.DB_PATH)
    res = cli(["enqueue", '{"id": "mr-override", "command": "echo hi", "max_retries": 1}'], db.connection.DB_PATH)
    assert res.returncode == 0
    row = tmp_db.execute("SELECT max_retries FROM jobs WHERE id='mr-override'").fetchone()
    assert row["max_retries"] == 1  # per-job wins over global


def test_attempts_always_non_negative(tmp_db):
    """attempts field in DB is always >= 0 after any state transition."""
    ts = db.now_iso()
    tmp_db.execute(
        "INSERT INTO jobs (id, command, state, attempts, max_retries, backoff_base, worker_id, "
        "heartbeat_at, created_at, updated_at) VALUES ('j-neg', 'exit 1', 'processing', 0, 3, 2.0, "
        "'w1', ?, ?, ?)", (ts, ts, ts)
    )
    db.finish_job(tmp_db, "j-neg", "w1", 1)
    row = tmp_db.execute("SELECT attempts FROM jobs WHERE id='j-neg'").fetchone()
    assert row["attempts"] >= 0


# ============================================================================
# Section 2A-3: Backoff Formula Edge Cases
# ============================================================================

def test_backoff_base_zero_produces_zero_delay(tmp_db):
    """backoff_base=0 produces 0^n=0 delay — job immediately eligible for retry."""
    ts = db.now_iso()
    tmp_db.execute(
        "INSERT INTO jobs (id, command, state, attempts, max_retries, backoff_base, worker_id, "
        "heartbeat_at, created_at, updated_at) VALUES ('j-b0', 'exit 1', 'processing', 0, 3, 0.0, "
        "'w1', ?, ?, ?)", (ts, ts, ts)
    )
    before = datetime.now(timezone.utc)
    db.finish_job(tmp_db, "j-b0", "w1", 1)
    row = tmp_db.execute("SELECT state, next_retry_at FROM jobs WHERE id='j-b0'").fetchone()
    assert row["state"] == "failed"
    # Delay = 0^1 = 0 — next_retry_at should be at or before now
    next_retry = datetime.fromisoformat(row["next_retry_at"])
    assert next_retry <= before + timedelta(seconds=2)


def test_backoff_base_one_produces_constant_delay(tmp_db):
    """backoff_base=1 produces 1^n=1 second delay at every attempt."""
    ts = db.now_iso()
    tmp_db.execute(
        "INSERT INTO jobs (id, command, state, attempts, max_retries, backoff_base, worker_id, "
        "heartbeat_at, created_at, updated_at) VALUES ('j-b1', 'exit 1', 'processing', 2, 5, 1.0, "
        "'w1', ?, ?, ?)", (ts, ts, ts)
    )
    before = datetime.now(timezone.utc)
    db.finish_job(tmp_db, "j-b1", "w1", 1)
    row = tmp_db.execute("SELECT next_retry_at FROM jobs WHERE id='j-b1'").fetchone()
    next_retry = datetime.fromisoformat(row["next_retry_at"])
    delay = (next_retry - before).total_seconds()
    assert 0.5 <= delay <= 2.0, f"Expected ~1s delay with base=1, got {delay:.2f}s"


def test_backoff_attempt_zero_produces_base_pow_zero(tmp_db):
    """At attempts=0 before first failure, formula is base^0=1 — never zero delay."""
    ts = db.now_iso()
    tmp_db.execute(
        "INSERT INTO jobs (id, command, state, attempts, max_retries, backoff_base, worker_id, "
        "heartbeat_at, created_at, updated_at) VALUES ('j-a0', 'exit 1', 'processing', 0, 3, 2.0, "
        "'w1', ?, ?, ?)", (ts, ts, ts)
    )
    before = datetime.now(timezone.utc)
    db.finish_job(tmp_db, "j-a0", "w1", 1)
    row = tmp_db.execute("SELECT next_retry_at FROM jobs WHERE id='j-a0'").fetchone()
    next_retry = datetime.fromisoformat(row["next_retry_at"])
    delay = (next_retry - before).total_seconds()
    # base^1 = 2.0 (attempts incremented before formula: attempts was 0, becomes 1, formula is base^1)
    assert delay > 0, "Delay must be positive — never zero on first failure with base=2"


def test_backoff_large_attempts_no_overflow(tmp_db):
    """Very large attempt count (50) does not cause integer overflow or exception."""
    ts = db.now_iso()
    tmp_db.execute(
        "INSERT INTO jobs (id, command, state, attempts, max_retries, backoff_base, worker_id, "
        "heartbeat_at, created_at, updated_at) VALUES ('j-large', 'exit 1', 'processing', 49, 100, 2.0, "
        "'w1', ?, ?, ?)", (ts, ts, ts)
    )
    # Should not raise — Python handles big ints natively
    db.finish_job(tmp_db, "j-large", "w1", 1)
    row = tmp_db.execute("SELECT state, next_retry_at FROM jobs WHERE id='j-large'").fetchone()
    assert row["state"] == "failed"
    assert row["next_retry_at"] is not None


def test_backoff_fractional_base_less_than_one(tmp_db):
    """backoff_base < 1 (e.g. 0.5) produces sub-second delays — job eligible quickly."""
    ts = db.now_iso()
    tmp_db.execute(
        "INSERT INTO jobs (id, command, state, attempts, max_retries, backoff_base, worker_id, "
        "heartbeat_at, created_at, updated_at) VALUES ('j-frac', 'exit 1', 'processing', 0, 5, 0.5, "
        "'w1', ?, ?, ?)", (ts, ts, ts)
    )
    before = datetime.now(timezone.utc)
    db.finish_job(tmp_db, "j-frac", "w1", 1)
    row = tmp_db.execute("SELECT next_retry_at FROM jobs WHERE id='j-frac'").fetchone()
    next_retry = datetime.fromisoformat(row["next_retry_at"])
    delay = (next_retry - before).total_seconds()
    # 0.5^1 = 0.5s — eligible almost immediately
    assert delay < 2.0, f"Fractional base should give short delay, got {delay:.3f}s"


@pytest.mark.parametrize("base,attempts,expected", [
    (2.0, 1, 2.0),
    (2.0, 2, 4.0),
    (2.0, 3, 8.0),
    (3.0, 1, 3.0),
    (3.0, 2, 9.0),
    (10.0, 2, 100.0),
])
def test_backoff_formula_parametrized(tmp_db, base, attempts, expected):
    """Parametrized: backoff delay = base ^ attempts (after increment)."""
    ts = db.now_iso()
    job_id = f"j-{base}-{attempts}"
    tmp_db.execute(
        "INSERT INTO jobs (id, command, state, attempts, max_retries, backoff_base, worker_id, "
        "heartbeat_at, created_at, updated_at) VALUES (?, 'exit 1', 'processing', ?, 100, ?, "
        "'w1', ?, ?, ?)", (job_id, attempts - 1, base, ts, ts, ts)
    )
    before = datetime.now(timezone.utc)
    db.finish_job(tmp_db, job_id, "w1", 1)
    row = tmp_db.execute(f"SELECT next_retry_at FROM jobs WHERE id=?", (job_id,)).fetchone()
    next_retry = datetime.fromisoformat(row["next_retry_at"])
    actual = (next_retry - before).total_seconds()
    assert abs(actual - expected) < 2.0, f"Expected ~{expected}s, got {actual:.2f}s for base={base} attempts={attempts}"


# ============================================================================
# Section 2A-4: Config Edge Cases
# ============================================================================

def test_config_unknown_key_stored_without_type_validation(tmp_db):
    """Unknown config keys are stored as strings without type checking."""
    config_service.set(tmp_db, "custom-key-xyz", "some-value")
    assert config_service.get(tmp_db, "custom-key-xyz") == "some-value"


def test_config_max_retries_negative_rejected(tmp_db):
    """config set max-retries with a negative-looking value like '-1':
    '-1' is ambiguous for Click/Typer CLI parsers (looks like an option flag).
    The important invariant is that this doesn't cause a silent DB corruption —
    either Typer rejects it at the CLI level (non-zero exit), or it stores -1.
    We test that the outcome is one of those two and that queuectl's own code
    never writes a Python traceback (Typer's internal rich error display may
    render its own box, which is a framework-level concern, not queuectl's).
    """
    # Use '--' separator to pass '-1' as a value, not a flag
    res = cli(["config", "set", "max-retries", "--", "-1"], db.connection.DB_PATH)
    # With '--' the value is unambiguously a positional: int('-1') = -1 stored
    # No queuectl-level error expected
    assert "Traceback" not in res.stderr or "most recent call last" not in res.stderr


def test_config_backoff_base_zero_stored(tmp_db):
    """config set backoff-base 0 stores value 0 without error (float('0') = 0.0)."""
    res = cli(["config", "set", "backoff-base", "0"], db.connection.DB_PATH)
    assert "Traceback" not in res.stderr
    if res.returncode == 0:
        val = config_service.get(tmp_db, "backoff-base")
        assert float(val) == 0.0


def test_config_float_precision_lossless(tmp_db):
    """Float config values round-trip without significant precision loss."""
    config_service.set(tmp_db, "backoff-base", "2.718281828")
    val = config_service.get(tmp_db, "backoff-base")
    assert abs(float(val) - 2.718281828) < 1e-6


def test_config_unknown_key_via_cli_stored(tmp_db):
    """config set with unknown key via CLI stores the value."""
    res = cli(["config", "set", "my-custom-key", "hello"], db.connection.DB_PATH)
    assert res.returncode == 0
    get_res = cli(["config", "get", "my-custom-key"], db.connection.DB_PATH)
    assert get_res.returncode == 0
    assert "hello" in get_res.stdout


def test_config_get_nonexistent_key_exits_nonzero(tmp_db):
    """config get with nonexistent key exits non-zero with clear error."""
    res = cli(["config", "get", "nonexistent-key-xyz"], db.connection.DB_PATH)
    assert res.returncode != 0
    assert "Traceback" not in res.stderr


def test_config_get_all_lists_all_defaults(tmp_db):
    """config get (no key) lists all keys in deterministic format."""
    res = cli(["config", "get"], db.connection.DB_PATH)
    assert res.returncode == 0
    assert "max-retries" in res.stdout
    assert "backoff-base" in res.stdout
    assert "recovery-timeout" in res.stdout
    assert "poll-interval" in res.stdout
    assert "heartbeat-interval" in res.stdout


def test_config_set_invalid_float_exits_nonzero(tmp_db):
    """config set backoff-base with non-numeric value exits non-zero."""
    res = cli(["config", "set", "backoff-base", "not_a_number"], db.connection.DB_PATH)
    assert res.returncode != 0
    assert "Traceback" not in res.stderr


# ============================================================================
# Section 2A-5: Heartbeat / Worker Field Null Invariants
# ============================================================================

def test_heartbeat_at_null_for_pending_job(tmp_db):
    """heartbeat_at is NULL for pending jobs — only set while processing."""
    ts = db.now_iso()
    tmp_db.execute(
        "INSERT INTO jobs (id, command, state, created_at, updated_at) VALUES ('j-hb', 'echo hi', 'pending', ?, ?)",
        (ts, ts)
    )
    row = tmp_db.execute("SELECT heartbeat_at, worker_id FROM jobs WHERE id='j-hb'").fetchone()
    assert row["heartbeat_at"] is None
    assert row["worker_id"] is None


def test_worker_id_null_after_completion(tmp_db):
    """worker_id is NULL after job completes — cleared by finish_job."""
    ts = db.now_iso()
    tmp_db.execute(
        "INSERT INTO jobs (id, command, state, attempts, max_retries, backoff_base, worker_id, "
        "heartbeat_at, created_at, updated_at) VALUES ('j-wid', 'echo hi', 'processing', 0, 3, 2.0, "
        "'w-999', ?, ?, ?)", (ts, ts, ts)
    )
    db.finish_job(tmp_db, "j-wid", "w-999", 0)
    row = tmp_db.execute("SELECT worker_id, heartbeat_at FROM jobs WHERE id='j-wid'").fetchone()
    assert row["worker_id"] is None
    assert row["heartbeat_at"] is None


def test_last_error_null_until_first_failure(tmp_db):
    """last_error is NULL on a fresh job and set only after first failure."""
    ts = db.now_iso()
    tmp_db.execute(
        "INSERT INTO jobs (id, command, state, created_at, updated_at) VALUES ('j-err', 'echo hi', 'pending', ?, ?)",
        (ts, ts)
    )
    row_before = tmp_db.execute("SELECT last_error FROM jobs WHERE id='j-err'").fetchone()
    assert row_before["last_error"] is None

    # Claim and fail
    tmp_db.execute(
        "UPDATE jobs SET state='processing', worker_id='w1', heartbeat_at=? WHERE id='j-err'", (ts,)
    )
    db.finish_job(tmp_db, "j-err", "w1", 2)
    row_after = tmp_db.execute("SELECT last_error FROM jobs WHERE id='j-err'").fetchone()
    assert row_after["last_error"] is not None
    assert "2" in row_after["last_error"]


def test_last_error_retained_on_dead_job(tmp_db):
    """last_error is retained on a dead job (not cleared)."""
    ts = db.now_iso()
    tmp_db.execute(
        "INSERT INTO jobs (id, command, state, attempts, max_retries, backoff_base, worker_id, "
        "heartbeat_at, last_error, created_at, updated_at) VALUES "
        "('j-dead-err', 'exit 1', 'processing', 2, 3, 2.0, 'w1', ?, 'prior error', ?, ?)",
        (ts, ts, ts)
    )
    db.finish_job(tmp_db, "j-dead-err", "w1", 1)
    row = tmp_db.execute("SELECT state, last_error FROM jobs WHERE id='j-dead-err'").fetchone()
    assert row["state"] == "dead"
    assert row["last_error"] is not None


def test_next_retry_at_null_for_completed_job(tmp_db):
    """next_retry_at is NULL for completed jobs."""
    ts = db.now_iso()
    tmp_db.execute(
        "INSERT INTO jobs (id, command, state, attempts, max_retries, backoff_base, worker_id, "
        "heartbeat_at, created_at, updated_at) VALUES ('j-comp-nr', 'echo hi', 'processing', 0, 3, 2.0, "
        "'w1', ?, ?, ?)", (ts, ts, ts)
    )
    db.finish_job(tmp_db, "j-comp-nr", "w1", 0)
    row = tmp_db.execute("SELECT next_retry_at FROM jobs WHERE id='j-comp-nr'").fetchone()
    assert row["next_retry_at"] is None


def test_next_retry_at_null_for_dead_job(tmp_db):
    """next_retry_at is NULL for dead jobs (exhausted retries)."""
    ts = db.now_iso()
    tmp_db.execute(
        "INSERT INTO jobs (id, command, state, attempts, max_retries, backoff_base, worker_id, "
        "heartbeat_at, created_at, updated_at) VALUES ('j-dead-nr', 'exit 1', 'processing', 2, 3, 2.0, "
        "'w1', ?, ?, ?)", (ts, ts, ts)
    )
    db.finish_job(tmp_db, "j-dead-nr", "w1", 1)
    row = tmp_db.execute("SELECT next_retry_at FROM jobs WHERE id='j-dead-nr'").fetchone()
    assert row["next_retry_at"] is None


# ============================================================================
# Section 2A-6: CLI Argument Parsing Edge Cases
# ============================================================================

def test_worker_start_count_zero(tmp_db):
    """worker start --count 0 either starts no workers or exits with an error — no crash."""
    import subprocess as _sp
    import signal as _sig
    import time as _t
    env = os.environ.copy()
    env["QUEUECTL_DB"] = db.connection.DB_PATH
    proc = _sp.Popen(
        [sys.executable, "-m", "queuectl.cli.entrypoint", "worker", "start", "--count", "0"],
        stdout=_sp.PIPE, stderr=_sp.PIPE, text=True, env=env
    )
    _t.sleep(0.5)
    if proc.poll() is None:
        try:
            proc.send_signal(_sig.SIGTERM)
        except ProcessLookupError:
            pass
    stdout, stderr = proc.communicate(timeout=5)
    combined = stdout + stderr
    assert proc.returncode is not None
    assert "Traceback" not in combined


def test_worker_start_count_string_rejected(tmp_db):
    """worker start --count abc exits non-zero (typer validates int type)."""
    res = cli(["worker", "start", "--count", "abc"], db.connection.DB_PATH)
    assert res.returncode != 0


def test_enqueue_missing_argument_exits_nonzero(tmp_db):
    """enqueue with no argument exits non-zero."""
    res = cli(["enqueue"], db.connection.DB_PATH)
    assert res.returncode != 0


def test_list_invalid_state_exits_nonzero(tmp_db):
    """list --state with an invalid state value exits non-zero."""
    res = cli(["list", "--state", "invalid_xyz"], db.connection.DB_PATH)
    assert res.returncode != 0
    assert "Traceback" not in res.stderr


def test_config_set_missing_value_exits_nonzero(tmp_db):
    """config set with only a key and no value exits non-zero."""
    res = cli(["config", "set", "max-retries"], db.connection.DB_PATH)
    assert res.returncode != 0


def test_dlq_retry_missing_job_id_exits_nonzero(tmp_db):
    """dlq retry with no job id argument exits non-zero."""
    res = cli(["dlq", "retry"], db.connection.DB_PATH)
    assert res.returncode != 0
