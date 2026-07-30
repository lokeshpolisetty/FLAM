"""
test_lifecycle.py — Integration tests covering the full job lifecycle
under real OS conditions.

Covers:
- CLI contract & JSON stdout purity
- Atomic process claiming & concurrency (exactly-once execution)
- SIGKILL crash recovery (< 60s)
- Cross-terminal worker shutdown via SIGTERM
- State machine transition safety
- Exponential backoff & DLQ lifecycle
- DLQ operator retry attempt reset
- Persistent storage across restarts
- Config persistence & per-job snapshotting
- Security payload safety (quotes, metacharacters, Unicode)
- Chaos worker kill & restart resilience
"""

import json
import os
import signal
import sqlite3
import subprocess
import sys
from pathlib import Path
import time
from datetime import datetime, timedelta, timezone

import pytest

ROOT = Path(__file__).resolve().parent.parent
from queuectl import database as db
from queuectl.config import settings as config_service


@pytest.fixture
def tmp_db(tmp_path, monkeypatch):
    """Fixture providing a clean temporary SQLite database for each test."""
    db_file = str(tmp_path / "test_queue.db")
    monkeypatch.setenv("QUEUECTL_DB", db_file)
    monkeypatch.setattr(db.connection, "DB_PATH", db_file)
    db.init_db()
    return db_file


def run_cli(*args, db_file=None, check=True):
    """Helper to run queuectl CLI via python app.py as a subprocess."""
    env = os.environ.copy()
    if db_file:
        env["QUEUECTL_DB"] = db_file
    cmd = [sys.executable, "-m", "queuectl"] + list(args)
    proc = subprocess.run(cmd, capture_output=True, text=True, env=env)
    if check and proc.returncode != 0:
        raise RuntimeError(f"CLI command failed ({proc.returncode}): {proc.stderr}\nstdout: {proc.stdout}")
    return proc


# ============================================================================
# 1. CLI Contract & JSON Output Hygiene
# ============================================================================

def test_adv_cli_json_purity(tmp_db):
    """Verify list --state ... --json prints ONLY valid JSON to stdout without extra logging."""
    run_cli("enqueue", json.dumps({"id": "job-json-1", "command": "echo hello"}), db_file=tmp_db)
    run_cli("enqueue", json.dumps({"id": "job-json-2", "command": "echo world"}), db_file=tmp_db)

    proc = run_cli("list", "--state", "pending", "--json", db_file=tmp_db)
    
    # Must be valid JSON array
    raw_stdout = proc.stdout.strip()
    assert raw_stdout.startswith("[") and raw_stdout.endswith("]"), f"stdout was not JSON array: {raw_stdout!r}"
    parsed = json.loads(raw_stdout)
    assert len(parsed) == 2
    assert parsed[0]["id"] in ["job-json-1", "job-json-2"]
    assert "command" in parsed[0]
    assert "state" in parsed[0]
    assert "attempts" in parsed[0]


def test_adv_enqueue_input_validation(tmp_db):
    """Verify enqueue rejects malformed JSON and missing required fields cleanly with non-zero exit."""
    # Invalid JSON string
    res1 = run_cli("enqueue", "invalid-json{", db_file=tmp_db, check=False)
    assert res1.returncode != 0
    assert "Invalid JSON" in res1.stderr

    # Missing command field
    res2 = run_cli("enqueue", json.dumps({"id": "no-cmd"}), db_file=tmp_db, check=False)
    assert res2.returncode != 0
    assert "must include at least 'id' and 'command'" in res2.stderr

    # Duplicate job ID
    run_cli("enqueue", json.dumps({"id": "dup-1", "command": "echo 1"}), db_file=tmp_db)
    res3 = run_cli("enqueue", json.dumps({"id": "dup-1", "command": "echo 2"}), db_file=tmp_db, check=False)
    assert res3.returncode != 0
    assert "already exists" in res3.stderr


# ============================================================================
# 2. Atomic OS Process Claiming & Concurrency
# ============================================================================

def test_adv_atomic_claiming_across_processes(tmp_db):
    """Verify that multiple worker processes running concurrently claim each job exactly once."""
    num_jobs = 30
    marker_file = os.path.join(os.path.dirname(tmp_db), "executed_jobs.txt")

    # Enqueue multiple jobs that append their job ID and worker PID to marker_file
    for i in range(num_jobs):
        cmd = f'python3 -c "import os, time; time.sleep(0.05); open({repr(marker_file)}, \'a\').write(f\'{i}:{{os.getpid()}}\\n\')"'
        run_cli("enqueue", json.dumps({"id": f"c-job-{i}", "command": cmd}), db_file=tmp_db)

    env = os.environ.copy()
    env["QUEUECTL_DB"] = tmp_db

    # Start worker process pool in foreground mode via worker start --count 5
    worker_proc = subprocess.Popen(
        [sys.executable, "-m", "queuectl", "worker", "start", "--count", "5"],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )

    # Wait until all jobs are completed
    start_time = time.time()
    while time.time() - start_time < 25:
        conn = db.get_connection()
        completed_count = conn.execute("SELECT COUNT(*) FROM jobs WHERE state='completed'").fetchone()[0]
        conn.close()
        if completed_count == num_jobs:
            break
        time.sleep(0.3)

    # Stop workers gracefully
    run_cli("worker", "stop", db_file=tmp_db)
    worker_proc.wait(timeout=5)

    # Assert all jobs completed
    conn = db.get_connection()
    completed_jobs = conn.execute("SELECT id FROM jobs WHERE state='completed'").fetchall()
    conn.close()
    assert len(completed_jobs) == num_jobs

    # Read marker file and assert each job index appeared EXACTLY once
    with open(marker_file, "r") as f:
        lines = f.readlines()

    executed_ids = [line.split(":")[0] for line in lines]
    assert len(executed_ids) == num_jobs
    assert len(set(executed_ids)) == num_jobs, "Duplicate execution detected across processes!"


# ============================================================================
# 3. Crash Recovery under SIGKILL
# ============================================================================

def test_adv_sigkill_crash_recovery(tmp_db):
    """Verify that if a worker is SIGKILLed mid-job, the job is recovered and completed within 60 seconds."""
    # Set fast recovery timeout for test (3 seconds)
    conn = db.get_connection()
    config_service.set(conn, "poll-interval", "0.375")
    config_service.set(conn, "heartbeat-interval", "1.5")
    config_service.set(conn, "recovery-timeout", "3")
    config_service.set(conn, "poll-interval", "0.25")
    config_service.set(conn, "heartbeat-interval", "1")
    conn.close()

    # Enqueue a long-running job (sleep 5)
    output_file = os.path.join(os.path.dirname(tmp_db), "recovery_done.txt")
    cmd = f'python3 -c "import os, time; time.sleep(5); open({repr(output_file)}, \'w\').write(\'OK\')"'
    run_cli("enqueue", json.dumps({"id": "sigkill-job", "command": cmd}), db_file=tmp_db)

    # Start 1 worker
    env = os.environ.copy()
    env["QUEUECTL_DB"] = tmp_db

    worker_proc = subprocess.Popen(
        [sys.executable, "-m", "queuectl", "worker", "start", "--count", "1"],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )

    # Wait until job is in 'processing' state
    time.sleep(1.5)
    conn = db.get_connection()
    job_row = conn.execute("SELECT * FROM jobs WHERE id='sigkill-job'").fetchone()
    worker_row = conn.execute("SELECT * FROM workers WHERE status='running'").fetchone()
    conn.close()

    assert job_row["state"] == "processing"
    assert worker_row is not None

    # SIGKILL the actual worker process abruptly (simulating hard crash/power failure)
    actual_pid = worker_row["pid"]
    try:
        os.kill(actual_pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    try:
        os.kill(worker_proc.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    worker_proc.wait()

    # Verify job is left in processing with stale worker info
    conn = db.get_connection()
    stale_job = conn.execute("SELECT state FROM jobs WHERE id='sigkill-job'").fetchone()
    conn.close()
    assert stale_job["state"] == "processing"

    # Sleep past recovery timeout (3s)
    time.sleep(3.5)

    # Start a NEW worker to trigger recovery and complete the job
    new_worker = subprocess.Popen(
        [sys.executable, "-m", "queuectl", "worker", "start", "--count", "1"],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )

    # Wait for completion
    start_wait = time.time()
    success = False
    while time.time() - start_wait < 10:
        conn = db.get_connection()
        res = conn.execute("SELECT state FROM jobs WHERE id='sigkill-job'").fetchone()
        conn.close()
        if res["state"] == "completed":
            success = True
            break
        time.sleep(0.5)

    run_cli("worker", "stop", db_file=tmp_db)
    new_worker.wait(timeout=5)

    assert success, "Job was not recovered and completed after worker SIGKILL!"
    assert os.path.exists(output_file)


# ============================================================================
# 4. Cross-Terminal Worker Stop
# ============================================================================

def test_adv_cross_terminal_worker_stop(tmp_db):
    """Verify queuectl worker stop terminates workers started from a separate terminal session."""
    env = os.environ.copy()
    env["QUEUECTL_DB"] = tmp_db

    # Launch worker start in separate process
    proc = subprocess.Popen(
        [sys.executable, "-m", "queuectl", "worker", "start", "--count", "3"],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )

    time.sleep(1.5)
    # Check status shows 3 running workers
    status_proc = run_cli("status", db_file=tmp_db)
    assert "Running workers: 3" in status_proc.stdout

    # Stop workers from this process (simulating different terminal)
    stop_proc = run_cli("worker", "stop", db_file=tmp_db)
    assert "Sent SIGTERM to worker" in stop_proc.stdout

    # Process should exit gracefully code 0
    proc.wait(timeout=5)
    assert proc.returncode == 0

    # Status now shows 0 running workers
    status_after = run_cli("status", db_file=tmp_db)
    assert "Running workers: 0" in status_after.stdout


# ============================================================================
# 5. State Machine & Transition Rules
# ============================================================================

def test_adv_state_machine_rules(tmp_db):
    """Validate allowable state machine transitions and guard against illegal states."""
    conn = db.get_connection()

    # Enqueue a job manually into DB
    ts = db.now_iso()
    conn.execute(
        "INSERT INTO jobs (id, command, state, attempts, max_retries, backoff_base, created_at, updated_at) "
        "VALUES ('sm-1', 'echo sm', 'pending', 0, 3, 2.0, ?, ?)",
        (ts, ts)
    )

    # Valid claim: pending -> processing
    job = db.claim_next_job(conn, "w-test")
    assert job["id"] == "sm-1"
    row = conn.execute("SELECT state FROM jobs WHERE id='sm-1'").fetchone()
    assert row["state"] == "processing"

    # Attempting to claim when no pending jobs exist returns None
    assert db.claim_next_job(conn, "w-test") is None

    # Valid completion: processing -> completed
    db.finish_job(conn, "sm-1", "w-test", returncode=0)
    row = conn.execute("SELECT state FROM jobs WHERE id='sm-1'").fetchone()
    assert row["state"] == "completed"

    # Double completion or finish by wrong worker is ignored safely
    db.finish_job(conn, "sm-1", "wrong-worker", returncode=0)
    row = conn.execute("SELECT state FROM jobs WHERE id='sm-1'").fetchone()
    assert row["state"] == "completed"

    conn.close()


# ============================================================================
# 6. Exponential Backoff & DLQ Lifecycle
# ============================================================================

def test_adv_exponential_backoff_and_dlq(tmp_db):
    """Verify retry delay follows base^attempts and job transitions to dead upon max_retries exhaustion."""
    conn = db.get_connection()
    ts = db.now_iso()

    # Create job with max_retries = 2, backoff_base = 3
    conn.execute(
        "INSERT INTO jobs (id, command, state, attempts, max_retries, backoff_base, created_at, updated_at) "
        "VALUES ('retry-job', 'exit 1', 'pending', 0, 2, 3.0, ?, ?)",
        (ts, ts)
    )

    # Attempt 1 failure: pending -> processing -> failed (attempt=1)
    db.claim_next_job(conn, "w-1")
    t1 = datetime.now(timezone.utc)
    db.finish_job(conn, "retry-job", "w-1", returncode=1)

    row = conn.execute("SELECT * FROM jobs WHERE id='retry-job'").fetchone()
    assert row["state"] == "failed"
    assert row["attempts"] == 1
    # Delay for attempt 1: 3^1 = 3 seconds
    next_retry = datetime.fromisoformat(row["next_retry_at"])
    expected_delay = (next_retry - t1).total_seconds()
    assert 2.5 <= expected_delay <= 4.0

    # Promote retry immediately for test
    conn.execute("UPDATE jobs SET state='pending', next_retry_at=NULL WHERE id='retry-job'")

    # Attempt 2 failure: pending -> processing -> dead (attempts=2 reaches max_retries=2)
    db.claim_next_job(conn, "w-1")
    db.finish_job(conn, "retry-job", "w-1", returncode=1)

    row2 = conn.execute("SELECT * FROM jobs WHERE id='retry-job'").fetchone()
    assert row2["state"] == "dead"
    assert row2["attempts"] == 2
    assert "command exited with code 1" in row2["last_error"]

    conn.close()


def test_adv_dlq_retry_resets_attempts(tmp_db):
    """Verify dlq retry resets attempts to 0 and re-enqueues dead job."""
    conn = db.get_connection()
    ts = db.now_iso()
    conn.execute(
        "INSERT INTO jobs (id, command, state, attempts, max_retries, backoff_base, last_error, created_at, updated_at) "
        "VALUES ('dlq-job', 'echo test', 'dead', 3, 3, 2.0, 'failed 3 times', ?, ?)",
        (ts, ts)
    )
    conn.close()

    # Verify visible in dlq list
    dlq_list = run_cli("dlq", "list", "--json", db_file=tmp_db)
    parsed = json.loads(dlq_list.stdout)
    assert len(parsed) == 1
    assert parsed[0]["id"] == "dlq-job"

    # Retry job via CLI
    retry_res = run_cli("dlq", "retry", "dlq-job", db_file=tmp_db)
    assert "Re-enqueued job 'dlq-job'" in retry_res.stdout

    # Verify job is now pending with attempts reset to 0
    conn = db.get_connection()
    row = conn.execute("SELECT * FROM jobs WHERE id='dlq-job'").fetchone()
    conn.close()

    assert row["state"] == "pending"
    assert row["attempts"] == 0
    assert row["last_error"] is None


# ============================================================================
# 7. Config Persistence & Per-Job Snapshotting
# ============================================================================

def test_adv_config_persistence_and_snapshot(tmp_db):
    """Verify config changes persist across CLI runs and snapshot onto newly enqueued jobs."""
    # Set custom config values
    run_cli("config", "set", "max-retries", "5", db_file=tmp_db)
    run_cli("config", "set", "backoff-base", "4.0", db_file=tmp_db)

    # Verify config get returns updated values
    cfg_get = run_cli("config", "get", "max-retries", db_file=tmp_db)
    assert cfg_get.stdout.strip() == "5"

    # Enqueue a job without specifying per-job overrides
    run_cli("enqueue", json.dumps({"id": "cfg-job-1", "command": "echo cfg"}), db_file=tmp_db)

    # Verify job snapshotted max_retries=5 and backoff_base=4.0
    conn = db.get_connection()
    row = conn.execute("SELECT max_retries, backoff_base FROM jobs WHERE id='cfg-job-1'").fetchone()
    conn.close()

    assert row["max_retries"] == 5
    assert row["backoff_base"] == 4.0


# ============================================================================
# 8. Security & Special Character Handling
# ============================================================================

def test_adv_security_and_unicode_handling(tmp_db):
    """Verify queuectl safely handles command strings with shell metacharacters, quotes, and Unicode."""
    marker_file = os.path.join(os.path.dirname(tmp_db), "unicode_test.txt")
    
    # Command containing spaces, single quotes, double quotes, subshells, and UTF-8 string
    complex_cmd = f'python3 -c "open({repr(marker_file)}, \'w\', encoding=\'utf-8\').write(\'🚀 QueueCTL Security & UTF-8 Test! <>&;$\')" '

    run_cli("enqueue", json.dumps({"id": "sec-job-🚀", "command": complex_cmd}), db_file=tmp_db)

    # Run worker to process job
    env = os.environ.copy()
    env["QUEUECTL_DB"] = tmp_db

    worker_proc = subprocess.Popen(
        [sys.executable, "-m", "queuectl", "worker", "start", "--count", "1"],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )

    time.sleep(2.5)
    run_cli("worker", "stop", db_file=tmp_db)
    worker_proc.wait(timeout=5)

    # Assert job completed and content was correctly written
    conn = db.get_connection()
    row = conn.execute("SELECT state FROM jobs WHERE id='sec-job-🚀'").fetchone()
    conn.close()

    assert row["state"] == "completed"
    with open(marker_file, "r", encoding="utf-8") as f:
        content = f.read()
    assert "🚀 QueueCTL Security & UTF-8 Test! <>&;$" in content


# ============================================================================
# 9. Chaos Resilience & Persistence Across DB Restarts
# ============================================================================

def test_adv_chaos_worker_crash_resilience(tmp_db):
    """Simulate chaos where workers are repeatedly started and forcefully killed while processing a batch of jobs."""
    num_jobs = 15
    out_dir = os.path.join(os.path.dirname(tmp_db), "chaos_out")
    os.makedirs(out_dir, exist_ok=True)

    # Fast recovery timeout
    conn = db.get_connection()
    config_service.set(conn, "poll-interval", "0.25")
    config_service.set(conn, "heartbeat-interval", "1")
    config_service.set(conn, "recovery-timeout", "2")
    config_service.set(conn, "poll-interval", "0.25")
    config_service.set(conn, "heartbeat-interval", "1")
    conn.close()

    # Enqueue jobs that write a file after a brief delay
    for i in range(num_jobs):
        file_path = os.path.join(out_dir, f"job_{i}.txt")
        cmd = f'python3 -c "import time; time.sleep(0.8); open({repr(file_path)}, \'w\').write(\'DONE\')"'
        run_cli("enqueue", json.dumps({"id": f"chaos-{i}", "command": cmd}), db_file=tmp_db)

    env = os.environ.copy()
    env["QUEUECTL_DB"] = tmp_db

    # Chaos loop: start workers, let them pick up jobs, kill them, repeat
    for wave in range(3):
        w_proc = subprocess.Popen(
            [sys.executable, "-m", "queuectl", "worker", "start", "--count", "3"],
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        time.sleep(1.2)
        # Force kill workers mid-execution
        conn = db.get_connection()
        running_workers = conn.execute("SELECT pid FROM workers WHERE status='running'").fetchall()
        conn.close()
        for r in running_workers:
            try:
                os.kill(r["pid"], signal.SIGKILL)
            except ProcessLookupError:
                pass
        try:
            os.kill(w_proc.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        w_proc.wait()

    # Allow a final stable worker run to clean up and finish all jobs
    stable_worker = subprocess.Popen(
        [sys.executable, "-m", "queuectl", "worker", "start", "--count", "3"],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )

    start_wait = time.time()
    while time.time() - start_wait < 25:
        conn = db.get_connection()
        completed = conn.execute("SELECT COUNT(*) FROM jobs WHERE state='completed'").fetchone()[0]
        conn.close()
        if completed == num_jobs:
            break
        time.sleep(0.5)

    run_cli("worker", "stop", db_file=tmp_db)
    stable_worker.wait(timeout=5)

    # All jobs must reach completed state
    conn = db.get_connection()
    final_completed = conn.execute("SELECT COUNT(*) FROM jobs WHERE state='completed'").fetchone()[0]
    conn.close()

    assert final_completed == num_jobs, f"Expected {num_jobs} completed jobs under chaos, got {final_completed}"
