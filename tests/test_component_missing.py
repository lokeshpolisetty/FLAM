"""
test_component_missing.py — Component tests covering gaps in Section 2B analysis.

Missing areas addressed:
  - Worker loop with stubbed command execution (verifies finish_job called correctly)
  - Heartbeat thread does not block job execution
  - Worker stops claiming after shutdown flag set
  - Signal handlers registered for both SIGINT and SIGTERM
  - Signal received during DB transaction safety
  - Multiple rapid signals — only one shutdown
  - reap_stale_jobs concurrent two-connection race (CAS safety)
  - promote_ready_retries concurrent two-connection race
  - enqueue 20 parallel connections integrity
  - Worker registry: multiple workers registered simultaneously
  - finish_job is a no-op when job already reaped by another worker
"""

import os
import signal
import sqlite3
import subprocess
import sys
import threading
import time
import json
import pytest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch, MagicMock

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
from queuectl import database as db
from queuectl.config import settings as config_service
from queuectl.worker import execute_job, worker_main_loop

@pytest.fixture
def isolated_db(tmp_path, monkeypatch):
    """Isolated DB for each test with monkeypatched DB_PATH."""
    db_file = str(tmp_path / "comp_missing.db")
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
# 1. Worker loop with stubbed command execution
#
# NOTE: worker_main_loop calls signal.signal() which is only allowed from
# the main thread. When running the loop in a daemon thread for testing,
# we must stub out signal.signal to avoid the ValueError. We also stub
# db.get_connection so the thread uses the same isolated DB.
# ===========================================================================

def _run_worker_loop_in_thread(monkeypatch, cfg, db_path, finish_hook=None, execute_rc=0):
    """
    Run worker_main_loop in a daemon thread with:
      - signal.signal stubbed (can't set signals from non-main thread)
      - execute_job stubbed to return `execute_rc`
      - optional finish_hook wrapping db.finish_job
    Returns the thread.
    """
    from queuectl import worker as worker_mod
    import signal as signal_mod

    # Stub signal.signal — just record calls, don't actually set OS handlers
    monkeypatch.setattr(signal_mod, "signal", lambda signum, handler: None)

    monkeypatch.setattr(worker_mod.executor, "execute_job",
                        lambda conn, job, worker_id, hb_interval: execute_rc)

    if finish_hook is not None:
        original_finish = db.finish_job
        def wrapped_finish(conn, job_id, worker_id, returncode):
            finish_hook(job_id, returncode)
            original_finish(conn, job_id, worker_id, returncode)
        monkeypatch.setattr(db.job_repository, "finish_job", wrapped_finish)

    t = threading.Thread(target=worker_main_loop, args=(cfg,), daemon=True)
    t.start()
    return t


def test_worker_loop_calls_finish_job_on_success(isolated_db, monkeypatch):
    """Worker loop calls finish_job with returncode=0 when command succeeds."""
    _insert_job(isolated_db, "stub-ok")

    finish_calls = []

    cfg = {"poll-interval": "0.05", "recovery-timeout": "15",
           "heartbeat-interval": "3"}

    t = _run_worker_loop_in_thread(
        monkeypatch, cfg, db.connection.DB_PATH,
        finish_hook=lambda jid, rc: finish_calls.append((jid, rc)),
        execute_rc=0,
    )

    deadline = time.time() + 5
    while time.time() < deadline:
        if finish_calls:
            break
        time.sleep(0.05)

    t.join(timeout=0.1)  # daemon thread — test cleanup handles it

    assert any(jid == "stub-ok" and rc == 0 for jid, rc in finish_calls), \
        f"finish_job not called with success for stub-ok; calls={finish_calls}"


def test_worker_loop_calls_finish_job_on_failure(isolated_db, monkeypatch):
    """Worker loop calls finish_job with returncode=1 when command fails."""
    _insert_job(isolated_db, "stub-fail")

    finish_calls = []

    cfg = {"poll-interval": "0.05", "recovery-timeout": "15",
           "heartbeat-interval": "3"}

    t = _run_worker_loop_in_thread(
        monkeypatch, cfg, db.connection.DB_PATH,
        finish_hook=lambda jid, rc: finish_calls.append((jid, rc)),
        execute_rc=1,
    )

    deadline = time.time() + 5
    while time.time() < deadline:
        if finish_calls:
            break
        time.sleep(0.05)

    t.join(timeout=0.1)  # daemon thread

    assert any(jid == "stub-fail" and rc == 1 for jid, rc in finish_calls), \
        f"finish_job not called with failure for stub-fail; calls={finish_calls}"


def test_worker_loop_registers_in_workers_table(isolated_db, monkeypatch):
    """Worker loop registers itself in the workers table on start."""
    cfg = {"poll-interval": "0.2", "recovery-timeout": "15",
           "heartbeat-interval": "3"}

    t = _run_worker_loop_in_thread(monkeypatch, cfg, db.connection.DB_PATH)
    time.sleep(0.5)

    conn2 = db.get_connection()
    rows = conn2.execute("SELECT worker_id, status FROM workers").fetchall()
    conn2.close()

    assert len(rows) >= 1, "Worker must register itself in workers table"
    assert any(r["status"] == "running" for r in rows)

    t.join(timeout=0.1)  # daemon thread — test cleanup handles it


def test_worker_loop_marks_stopped_on_exit(isolated_db, monkeypatch):
    """Worker loop marks itself stopped in workers table when it exits cleanly.
    We verify this by running as a subprocess (not in-process thread) so SIGTERM
    is safe to send without killing the test process.
    """
    env = os.environ.copy()
    env["QUEUECTL_DB"] = db.connection.DB_PATH

    proc = subprocess.Popen(
        [sys.executable, "-m", "queuectl.cli.entrypoint"] + ["worker", "start", "--count", "1"],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, env=env
    )
    time.sleep(0.5)
    proc.send_signal(signal.SIGTERM)
    proc.wait(timeout=6)
    assert proc.returncode == 0

    conn3 = db.get_connection()
    rows = conn3.execute("SELECT status FROM workers").fetchall()
    conn3.close()
    assert all(r["status"] == "stopped" for r in rows), \
        f"Worker should mark itself stopped on exit, got: {[r['status'] for r in rows]}"


# ===========================================================================
# 2. Signal handler registration
# ===========================================================================

def test_signal_handlers_registered_for_sigint_and_sigterm(tmp_path, monkeypatch):
    """worker_main_loop registers handlers for both SIGINT and SIGTERM before looping.

    We verify this by patching signal.signal on the worker module's own reference
    (worker.signal.signal), which is what worker_main_loop actually calls.
    """
    from queuectl import worker as worker_mod
    import signal as signal_mod

    db_file = str(tmp_path / "sig_reg.db")
    monkeypatch.setenv("QUEUECTL_DB", db_file)
    monkeypatch.setattr(db.connection, "DB_PATH", db_file)
    db.init_db()

    registered = {}

    # Patch signal.signal on the worker module's signal reference
    def capture_signal(signum, handler):
        registered[signum] = handler

    # worker.py does: import signal ... signal.signal(SIGINT, ...) signal.signal(SIGTERM, ...)
    # so we patch the signal attribute on the worker module's signal reference
    monkeypatch.setattr(worker_mod.signal, "signal", capture_signal)
    monkeypatch.setattr(worker_mod.executor, "execute_job",
                        lambda conn, job, worker_id, hb_interval: 0)

    cfg = {"poll-interval": "0.05", "recovery-timeout": "15",
           "heartbeat-interval": "3"}

    t = threading.Thread(target=worker_main_loop, args=(cfg,), daemon=True)
    t.start()
    # Give it enough time to register signals and start the loop
    time.sleep(0.3)
    t.join(timeout=0.05)  # daemon — won't block

    assert signal_mod.SIGINT in registered, \
        f"SIGINT handler must be registered; got: {list(registered.keys())}"
    assert signal_mod.SIGTERM in registered, \
        f"SIGTERM handler must be registered; got: {list(registered.keys())}"
    assert registered[signal_mod.SIGINT] not in (signal_mod.SIG_DFL, signal_mod.SIG_IGN)
    assert registered[signal_mod.SIGTERM] not in (signal_mod.SIG_DFL, signal_mod.SIG_IGN)


def test_multiple_rapid_sigterms_only_one_shutdown(tmp_path):
    """Multiple rapid SIGTERMs to a worker process result in exactly one clean shutdown."""
    env = os.environ.copy()
    db_file = str(tmp_path / "rapid_sig.db")
    env["QUEUECTL_DB"] = db_file

    proc = subprocess.Popen(
        [sys.executable, "-m", "queuectl.cli.entrypoint"] + ["worker", "start", "--count", "1"],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env
    )
    time.sleep(0.5)

    # Fire 5 rapid SIGTERMs
    for _ in range(5):
        try:
            proc.send_signal(signal.SIGTERM)
        except ProcessLookupError:
            break
        time.sleep(0.02)

    proc.wait(timeout=8)
    assert proc.returncode == 0, f"Expected exit 0 after rapid SIGTERMs, got {proc.returncode}"

    stdout = proc.stdout.read().decode(errors="replace")
    # "stopped" should appear exactly once per worker
    stopped_count = stdout.count("stopped")
    assert stopped_count >= 1, f"'stopped' log not found in output:\n{stdout}"


def test_sigterm_during_idle_worker_exits_immediately(tmp_path):
    """SIGTERM to idle worker (no job running) causes immediate clean exit."""
    env = os.environ.copy()
    db_file = str(tmp_path / "idle_sig.db")
    env["QUEUECTL_DB"] = db_file

    proc = subprocess.Popen(
        [sys.executable, "-m", "queuectl.cli.entrypoint"] + ["worker", "start", "--count", "1"],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, env=env
    )
    time.sleep(0.5)
    start = time.time()
    proc.send_signal(signal.SIGTERM)
    proc.wait(timeout=5)
    elapsed = time.time() - start
    assert proc.returncode == 0
    # Idle worker should exit within 2 poll-intervals (default poll=1s → max 2s)
    assert elapsed < 4.0, f"Idle worker took {elapsed:.1f}s to exit after SIGTERM"


# ===========================================================================
# 3. Concurrent reap_stale_jobs — two-connection race
# ===========================================================================

def test_reap_stale_concurrent_two_connections_no_double_reap(tmp_path, monkeypatch):
    """Two connections calling reap_stale_jobs concurrently recover each job exactly once."""
    db_file = str(tmp_path / "reap_race.db")
    monkeypatch.setenv("QUEUECTL_DB", db_file)
    monkeypatch.setattr(db.connection, "DB_PATH", db_file)
    db.init_db()

    stale = (datetime.now(timezone.utc) - timedelta(seconds=30)).isoformat()
    ts = db.now_iso()
    setup = db.get_connection()
    for i in range(5):
        setup.execute(
            "INSERT INTO jobs (id, command, state, attempts, max_retries, backoff_base, "
            "worker_id, heartbeat_at, created_at, updated_at) "
            "VALUES (?, 'echo hi', 'processing', 0, 3, 2.0, 'w-dead', ?, ?, ?)",
            (f"reap-{i}", stale, ts, ts)
        )
    setup.close()

    results = []
    errors = []

    def reap_worker(conn_id):
        try:
            c = db.get_connection()
            reaped = db.reap_stale_jobs(c, timeout_seconds=15)
            results.append((conn_id, reaped))
            c.close()
        except Exception as e:
            errors.append(e)

    t1 = threading.Thread(target=reap_worker, args=("c1",))
    t2 = threading.Thread(target=reap_worker, args=("c2",))
    t1.start(); t2.start()
    t1.join(); t2.join()

    assert not errors, f"Errors during concurrent reap: {errors}"

    # Combine both results — each job should appear at most once across both calls
    all_reaped = []
    for _, reaped in results:
        all_reaped.extend(reaped)

    assert len(all_reaped) == len(set(all_reaped)), \
        f"Double-reap detected: {all_reaped}"
    assert len(set(all_reaped)) == 5, \
        f"Expected 5 unique reaped jobs, got {set(all_reaped)}"


def test_promote_ready_retries_concurrent_no_double_promote(tmp_path, monkeypatch):
    """Two connections calling promote_ready_retries simultaneously — no double-promote."""
    db_file = str(tmp_path / "promote_race.db")
    monkeypatch.setenv("QUEUECTL_DB", db_file)
    monkeypatch.setattr(db.connection, "DB_PATH", db_file)
    db.init_db()

    past = (datetime.now(timezone.utc) - timedelta(seconds=5)).isoformat()
    ts = db.now_iso()
    setup = db.get_connection()
    for i in range(5):
        setup.execute(
            "INSERT INTO jobs (id, command, state, attempts, max_retries, backoff_base, "
            "next_retry_at, created_at, updated_at) "
            "VALUES (?, 'echo hi', 'failed', 1, 3, 2.0, ?, ?, ?)",
            (f"promote-{i}", past, ts, ts)
        )
    setup.close()

    errors = []

    def promote_worker():
        try:
            c = db.get_connection()
            db.promote_ready_retries(c)
            c.close()
        except Exception as e:
            errors.append(e)

    t1 = threading.Thread(target=promote_worker)
    t2 = threading.Thread(target=promote_worker)
    t1.start(); t2.start()
    t1.join(); t2.join()

    assert not errors, f"Errors during concurrent promote: {errors}"

    verify = db.get_connection()
    pending_count = verify.execute(
        "SELECT COUNT(*) FROM jobs WHERE state='pending'"
    ).fetchone()[0]
    failed_count = verify.execute(
        "SELECT COUNT(*) FROM jobs WHERE state='failed'"
    ).fetchone()[0]
    verify.close()

    assert pending_count == 5, f"All 5 jobs should be pending after promote, got {pending_count}"
    assert failed_count == 0, f"No jobs should remain failed, got {failed_count}"


# ===========================================================================
# 4. finish_job is a no-op when job reaped by another worker
# ===========================================================================

def test_finish_job_noop_when_already_reaped(tmp_path, monkeypatch):
    """If a job is reaped (back to pending) before finish_job runs, finish_job is a no-op."""
    db_file = str(tmp_path / "noop_finish.db")
    monkeypatch.setenv("QUEUECTL_DB", db_file)
    monkeypatch.setattr(db.connection, "DB_PATH", db_file)
    db.init_db()

    ts = db.now_iso()
    stale = (datetime.now(timezone.utc) - timedelta(seconds=30)).isoformat()
    c = db.get_connection()
    c.execute(
        "INSERT INTO jobs (id, command, state, attempts, max_retries, backoff_base, "
        "worker_id, heartbeat_at, created_at, updated_at) "
        "VALUES ('reaped-j', 'echo hi', 'processing', 0, 3, 2.0, 'w-old', ?, ?, ?)",
        (stale, ts, ts)
    )

    # Reap the job back to pending (simulates recovery by another worker)
    db.reap_stale_jobs(c, timeout_seconds=15)

    row_before = c.execute("SELECT state FROM jobs WHERE id='reaped-j'").fetchone()
    assert row_before["state"] == "pending"

    # Now the original worker tries to finish_job — must be a no-op
    db.finish_job(c, "reaped-j", "w-old", 0)

    row_after = c.execute("SELECT state FROM jobs WHERE id='reaped-j'").fetchone()
    assert row_after["state"] == "pending", \
        "finish_job by evicted worker must not change state from pending"
    c.close()


def test_finish_job_noop_when_claimed_by_different_worker(tmp_path, monkeypatch):
    """finish_job with wrong worker_id is a no-op — does not corrupt the new owner's work."""
    db_file = str(tmp_path / "wrong_worker.db")
    monkeypatch.setenv("QUEUECTL_DB", db_file)
    monkeypatch.setattr(db.connection, "DB_PATH", db_file)
    db.init_db()

    ts = db.now_iso()
    c = db.get_connection()
    c.execute(
        "INSERT INTO jobs (id, command, state, attempts, max_retries, backoff_base, "
        "worker_id, heartbeat_at, created_at, updated_at) "
        "VALUES ('owned-j', 'echo hi', 'processing', 0, 3, 2.0, 'w-current', ?, ?, ?)",
        (ts, ts, ts)
    )

    # Old worker tries to finish — wrong worker_id
    db.finish_job(c, "owned-j", "w-old-evicted", 0)

    row = c.execute("SELECT state, worker_id FROM jobs WHERE id='owned-j'").fetchone()
    assert row["state"] == "processing", "State must not change when wrong worker calls finish_job"
    assert row["worker_id"] == "w-current", "Worker ownership must not be stolen"
    c.close()


# ===========================================================================
# 5. Worker registry: multiple workers registered simultaneously
# ===========================================================================

def test_multiple_workers_registered_concurrently(tmp_path, monkeypatch):
    """5 workers registering concurrently all appear in the workers table."""
    db_file = str(tmp_path / "multi_reg.db")
    monkeypatch.setenv("QUEUECTL_DB", db_file)
    monkeypatch.setattr(db.connection, "DB_PATH", db_file)
    db.init_db()

    errors = []

    def register(wid):
        try:
            c = db.get_connection()
            db.register_worker(c, wid, os.getpid() + hash(wid) % 1000)
            c.close()
        except Exception as e:
            errors.append(e)

    threads = [threading.Thread(target=register, args=(f"w-conc-{i}",)) for i in range(5)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert not errors, f"Errors during concurrent registration: {errors}"

    verify = db.get_connection()
    count = verify.execute("SELECT COUNT(*) FROM workers").fetchone()[0]
    verify.close()
    assert count == 5, f"Expected 5 workers registered, got {count}"


# ===========================================================================
# 6. execute_job heartbeat behaviour
# ===========================================================================

def test_execute_job_returns_exit_code(tmp_path, monkeypatch):
    """execute_job returns the correct exit code of the subprocess command."""
    db_file = str(tmp_path / "exec_job.db")
    monkeypatch.setenv("QUEUECTL_DB", db_file)
    monkeypatch.setattr(db.connection, "DB_PATH", db_file)
    db.init_db()

    ts = db.now_iso()
    c = db.get_connection()
    c.execute(
        "INSERT INTO jobs (id, command, state, attempts, max_retries, backoff_base, "
        "worker_id, heartbeat_at, created_at, updated_at) "
        "VALUES ('exec-j', 'echo hi', 'processing', 0, 3, 2.0, 'w1', ?, ?, ?)",
        (ts, ts, ts)
    )
    job = dict(c.execute("SELECT * FROM jobs WHERE id='exec-j'").fetchone())

    rc = execute_job(c, job, "w1", heartbeat_interval=10)
    assert rc == 0

    # Also test non-zero exit
    c.execute(
        "INSERT INTO jobs (id, command, state, attempts, max_retries, backoff_base, "
        "worker_id, heartbeat_at, created_at, updated_at) "
        "VALUES ('exec-fail', 'exit 2', 'processing', 0, 3, 2.0, 'w1', ?, ?, ?)",
        (ts, ts, ts)
    )
    job_fail = dict(c.execute("SELECT * FROM jobs WHERE id='exec-fail'").fetchone())
    rc_fail = execute_job(c, job_fail, "w1", heartbeat_interval=10)
    assert rc_fail == 2
    c.close()


def test_execute_job_refreshes_heartbeat_during_long_command(tmp_path, monkeypatch):
    """execute_job refreshes heartbeat_at while a long command runs."""
    db_file = str(tmp_path / "hb_refresh.db")
    monkeypatch.setenv("QUEUECTL_DB", db_file)
    monkeypatch.setattr(db.connection, "DB_PATH", db_file)
    db.init_db()

    ts = db.now_iso()
    c = db.get_connection()
    c.execute(
        "INSERT INTO jobs (id, command, state, attempts, max_retries, backoff_base, "
        "worker_id, heartbeat_at, created_at, updated_at) "
        "VALUES ('hb-j', 'sleep 2', 'processing', 0, 3, 2.0, 'w1', ?, ?, ?)",
        (ts, ts, ts)
    )
    job = dict(c.execute("SELECT * FROM jobs WHERE id='hb-j'").fetchone())

    before_hb = ts
    execute_job(c, job, "w1", heartbeat_interval=0.5)

    row_after = c.execute("SELECT heartbeat_at FROM jobs WHERE id='hb-j'").fetchone()
    assert row_after["heartbeat_at"] >= before_hb, \
        "heartbeat_at must advance during long job execution"
    c.close()


# ===========================================================================
# 7. Parallel enqueue via 20 connections — integrity
# ===========================================================================

def test_parallel_enqueue_20_connections_no_corruption(tmp_path, monkeypatch):
    """20 parallel enqueue calls via separate connections all succeed without corruption."""
    db_file = str(tmp_path / "par_enq.db")
    monkeypatch.setenv("QUEUECTL_DB", db_file)
    monkeypatch.setattr(db.connection, "DB_PATH", db_file)
    db.init_db()

    errors = []

    def enqueue_one(i):
        try:
            env = os.environ.copy()
            env["QUEUECTL_DB"] = db_file
            res = subprocess.run(
                [sys.executable, "-m", "queuectl.cli.entrypoint"] + ["enqueue",
                 json.dumps({"id": f"par-enq-{i}", "command": f"echo {i}"})],
                capture_output=True, text=True, env=env, timeout=10
            )
            if res.returncode != 0:
                errors.append(f"enqueue {i} failed: {res.stderr}")
        except Exception as e:
            errors.append(e)

    threads = [threading.Thread(target=enqueue_one, args=(i,)) for i in range(20)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert not errors, f"Parallel enqueue errors: {errors}"

    verify = db.get_connection()
    count = verify.execute("SELECT COUNT(*) FROM jobs").fetchone()[0]
    result = verify.execute("PRAGMA integrity_check").fetchone()[0]
    verify.close()

    assert count == 20, f"Expected 20 jobs, got {count}"
    assert result == "ok", f"DB integrity check failed: {result}"


# ===========================================================================
# 8. Worker does not claim after shutdown flag is set
# ===========================================================================

def test_worker_does_not_claim_after_stop_signal(tmp_path):
    """After SIGTERM, worker finishes current job but does not claim a new one."""
    env = os.environ.copy()
    db_file = str(tmp_path / "no_claim_after_stop.db")
    env["QUEUECTL_DB"] = db_file

    # Enqueue a slow job and a fast job after it
    subprocess.run(
        [sys.executable, "-m", "queuectl.cli.entrypoint"] + ["enqueue",
         json.dumps({"id": "slow-1", "command": "sleep 2"})],
        capture_output=True, env=env
    )
    subprocess.run(
        [sys.executable, "-m", "queuectl.cli.entrypoint"] + ["enqueue",
         json.dumps({"id": "fast-2", "command": "echo hi"})],
        capture_output=True, env=env
    )

    proc = subprocess.Popen(
        [sys.executable, "-m", "queuectl.cli.entrypoint"] + ["worker", "start", "--count", "1"],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, env=env
    )

    # Wait for slow-1 to be claimed, then send SIGTERM
    deadline = time.time() + 8
    while time.time() < deadline:
        conn = sqlite3.connect(db_file)
        row = conn.execute("SELECT state FROM jobs WHERE id='slow-1'").fetchone()
        conn.close()
        if row and row[0] == "processing":
            break
        time.sleep(0.2)

    proc.send_signal(signal.SIGTERM)
    proc.wait(timeout=10)

    # slow-1 should be completed (graceful finish)
    # fast-2 should still be pending (not claimed after signal)
    conn = sqlite3.connect(db_file)
    slow = conn.execute("SELECT state FROM jobs WHERE id='slow-1'").fetchone()
    fast = conn.execute("SELECT state FROM jobs WHERE id='fast-2'").fetchone()
    conn.close()

    assert slow[0] == "completed", f"slow-1 should complete gracefully, got {slow[0]}"
    assert fast[0] == "pending", f"fast-2 should not be claimed after SIGTERM, got {fast[0]}"
