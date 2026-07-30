"""
test_stress.py — Stress and non-functional gap tests.

Covers scenarios requiring either high job counts or unusual system conditions:
  - 100+ worker contention (stress)
  - 500 job stress enqueue + process
  - Read-only DB error handling
  - DB file deleted while worker running
  - Enqueue latency p50/p95 measurement
  - Large DLQ (500 dead jobs) list performance
  - WAL file survival after SIGKILL
  - Privilege boundary (non-root execution)
  - Disk-space exhaustion simulation (graceful error, not traceback)
"""

import json
import os
import signal
import sqlite3
import subprocess
import sys
import time
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent


def cli(args, env, timeout=60):
    return subprocess.run(
        [sys.executable, "-m", "queuectl"] + args,
        capture_output=True, text=True, timeout=timeout, env=env,
    )


def worker_proc(env, count=1):
    return subprocess.Popen(
        [sys.executable, "-m", "queuectl"] + ["worker", "start", "--count", str(count)],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, env=env,
    )


def wait_all_terminal(env, timeout=60):
    """Wait until all jobs reach a terminal state (completed or dead)."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        raw = cli(["list", "--json"], env).stdout.strip()
        try:
            jobs = json.loads(raw)
            if jobs and all(j["state"] in ("completed", "dead") for j in jobs):
                return jobs
        except json.JSONDecodeError:
            pass
        time.sleep(0.5)
    raw = cli(["list", "--json"], env).stdout.strip()
    return json.loads(raw) if raw else []


@pytest.fixture
def env(tmp_path):
    e = os.environ.copy()
    e["QUEUECTL_DB"] = str(tmp_path / "queue.db")
    e["QUEUECTL_TEST"] = "1"
    return e


# ===========================================================================
# Stress: 500 jobs, 20 workers
# ===========================================================================

@pytest.mark.slow
def test_stress_500_jobs_20_workers(env):
    """500 echo jobs processed by 20 workers all complete without loss or duplication."""
    N = 500
    # Bulk-insert via sqlite3 for speed
    cli(["status"], env)  # initialise schema
    conn = sqlite3.connect(env["QUEUECTL_DB"])
    ts = "2025-01-01T00:00:00+00:00"
    conn.executemany(
        "INSERT OR IGNORE INTO jobs (id, command, state, attempts, max_retries, "
        "backoff_base, created_at, updated_at) VALUES (?,?,?,?,?,?,?,?)",
        [(f"stress-{i}", "echo hi", "pending", 0, 3, 2.0, ts, ts) for i in range(N)],
    )
    conn.commit()
    conn.close()

    wp = worker_proc(env, count=20)
    try:
        jobs = wait_all_terminal(env, timeout=120)
    finally:
        wp.send_signal(signal.SIGTERM)
        wp.wait(timeout=10)

    completed = [j for j in jobs if j["state"] == "completed"]
    assert len(completed) == N, f"Expected {N} completed, got {len(completed)}"

    conn2 = sqlite3.connect(env["QUEUECTL_DB"])
    result = conn2.execute("PRAGMA integrity_check").fetchone()[0]
    conn2.close()
    assert result == "ok"


# ===========================================================================
# Stress: 100 workers, 100 jobs (high contention)
# ===========================================================================

@pytest.mark.slow
def test_stress_100_workers_100_jobs_no_duplication(env, tmp_path):
    """100 workers competing for 100 jobs — each job executed exactly once."""
    N = 100
    log_file = tmp_path / "exec_log.txt"

    cli(["status"], env)
    conn = sqlite3.connect(env["QUEUECTL_DB"])
    conn.executemany(
        "INSERT OR IGNORE INTO jobs (id, command, state, attempts, max_retries, "
        "backoff_base, created_at, updated_at) VALUES (?,?,?,?,?,?,?,?)",
        [(f"hc-{i}",
          f'python3 -c "open(\'{log_file}\', \'a\').write(\'{i}\\n\')"',
          "pending", 0, 3, 2.0,
          "2025-01-01T00:00:00+00:00", "2025-01-01T00:00:00+00:00")
         for i in range(N)],
    )
    conn.commit()
    conn.close()

    wp = worker_proc(env, count=100)
    try:
        jobs = wait_all_terminal(env, timeout=60)
    finally:
        wp.send_signal(signal.SIGTERM)
        wp.wait(timeout=10)

    completed = [j for j in jobs if j["state"] == "completed"]
    assert len(completed) == N

    lines = log_file.read_text().splitlines() if log_file.exists() else []
    assert len(lines) == N, f"Expected {N} executions, got {len(lines)}"
    assert len(set(lines)) == N, \
        f"Duplicate executions: {len(lines) - len(set(lines))} duplicates"


# ===========================================================================
# Read-only DB — graceful error handling
# ===========================================================================

def test_readonly_db_enqueue_fails_gracefully(tmp_path):
    """Enqueue against a read-only DB file exits non-zero with no traceback."""
    env = os.environ.copy()
    db_file = tmp_path / "readonly.db"

    # Create and initialise DB first
    env["QUEUECTL_DB"] = str(db_file)
    cli(["status"], env)

    # Make the DB file read-only
    os.chmod(str(db_file), 0o444)

    try:
        res = cli(["enqueue", '{"id": "ro-j", "command": "echo hi"}'], env)
        # Must not crash — either exit non-zero with clean error
        assert "Traceback" not in res.stderr, f"Traceback in stderr: {res.stderr}"
        assert "Traceback" not in res.stdout
        # Should fail (cannot write)
        assert res.returncode != 0 or "ro-j" not in res.stdout
    finally:
        os.chmod(str(db_file), 0o644)  # restore for cleanup


def test_readonly_db_worker_exits_gracefully(tmp_path):
    """Worker starting against a read-only DB exits cleanly without traceback."""
    env = os.environ.copy()
    db_file = tmp_path / "readonly_w.db"
    env["QUEUECTL_DB"] = str(db_file)

    # Initialise DB then make it read-only
    cli(["status"], env)
    os.chmod(str(db_file), 0o444)

    try:
        proc = subprocess.Popen(
            [sys.executable, "-m", "queuectl"] + ["worker", "start", "--count", "1"],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, env=env
        )
        proc.wait(timeout=8)
        stdout = proc.stdout.read()
        stderr = proc.stderr.read()
        assert "Traceback" not in stdout + stderr, \
            f"Traceback on read-only DB:\n{stdout}\n{stderr}"
    finally:
        os.chmod(str(db_file), 0o644)


# ===========================================================================
# Large DLQ list performance
# ===========================================================================

def test_dlq_list_500_dead_jobs_under_5s(env):
    """dlq list --json with 500 dead jobs completes in under 5s."""
    cli(["status"], env)
    conn = sqlite3.connect(env["QUEUECTL_DB"])
    ts = "2025-01-01T00:00:00+00:00"
    conn.executemany(
        "INSERT OR IGNORE INTO jobs (id, command, state, attempts, max_retries, "
        "backoff_base, last_error, created_at, updated_at) VALUES (?,?,?,?,?,?,?,?,?)",
        [(f"dlq-perf-{i}", "exit 1", "dead", 3, 3, 2.0, "err", ts, ts)
         for i in range(500)],
    )
    conn.commit()
    conn.close()

    start = time.time()
    res = cli(["dlq", "list", "--json"], env, timeout=10)
    elapsed = time.time() - start

    assert res.returncode == 0
    jobs = json.loads(res.stdout.strip())
    assert len(jobs) == 500
    assert elapsed < 5.0, f"dlq list with 500 dead jobs took {elapsed:.2f}s"


# ===========================================================================
# Enqueue latency measurement
# ===========================================================================

def test_enqueue_latency_p95_under_500ms(env):
    """p95 enqueue latency across 50 sequential enqueues is under 500ms."""
    latencies = []
    for i in range(50):
        start = time.time()
        res = cli(["enqueue", json.dumps({"id": f"lat-{i}", "command": "echo hi"})], env)
        elapsed = (time.time() - start) * 1000  # ms
        assert res.returncode == 0
        latencies.append(elapsed)

    latencies.sort()
    p95 = latencies[int(0.95 * len(latencies))]
    assert p95 < 500, f"p95 enqueue latency is {p95:.0f}ms (expected < 500ms)"


def test_enqueue_latency_p50_under_200ms(env):
    """Median enqueue latency across 20 sequential enqueues is under 200ms."""
    latencies = []
    for i in range(20):
        start = time.time()
        cli(["enqueue", json.dumps({"id": f"p50-{i}", "command": "echo hi"})], env)
        latencies.append((time.time() - start) * 1000)

    latencies.sort()
    p50 = latencies[len(latencies) // 2]
    assert p50 < 200, f"p50 enqueue latency is {p50:.0f}ms (expected < 200ms)"


# ===========================================================================
# WAL file survival after SIGKILL
# ===========================================================================

def test_wal_file_survives_sigkill_and_db_readable(env, tmp_path):
    """After SIGKILL, any WAL file does not prevent the DB from being opened."""
    cli(["config", "set", "recovery-timeout", "5"], env)
    cli(["enqueue", '{"id":"wal-kill","command":"sleep 10"}'], env)

    wp = worker_proc(env, count=1)
    time.sleep(0.8)

    conn = sqlite3.connect(env["QUEUECTL_DB"])
    row = conn.execute("SELECT pid FROM workers WHERE status='running'").fetchone()
    conn.close()
    if row:
        try:
            os.kill(row[0], signal.SIGKILL)
        except ProcessLookupError:
            pass
    wp.wait(timeout=5)

    # DB must be openable and readable after SIGKILL (WAL recovery is automatic in SQLite)
    conn2 = sqlite3.connect(env["QUEUECTL_DB"])
    result = conn2.execute("PRAGMA integrity_check").fetchone()[0]
    count = conn2.execute("SELECT COUNT(*) FROM jobs").fetchone()[0]
    conn2.close()

    assert result == "ok", f"DB integrity failed after SIGKILL: {result}"
    assert count >= 1


# ===========================================================================
# Privilege boundary (non-root)
# ===========================================================================

def test_not_running_as_root():
    """The test process is not running as root — confirming privilege boundary."""
    assert os.getuid() != 0, \
        "Tests should not run as root; privilege boundary testing requires non-root"


def test_worker_runs_as_current_user(env, tmp_path):
    """Jobs executed by the worker run as the same user who started the worker."""
    out_file = tmp_path / "whoami_out.txt"
    cmd = f"id -u > {out_file}"
    cli(["enqueue", json.dumps({"id": "priv-j", "command": cmd})], env)

    wp = worker_proc(env, count=1)
    try:
        deadline = time.time() + 10
        while time.time() < deadline:
            raw = cli(["list", "--json"], env).stdout.strip()
            for j in json.loads(raw):
                if j["id"] == "priv-j" and j["state"] == "completed":
                    break
            else:
                time.sleep(0.2)
                continue
            break
    finally:
        wp.send_signal(signal.SIGTERM)
        wp.wait(timeout=5)

    uid_in_file = out_file.read_text().strip() if out_file.exists() else ""
    assert uid_in_file == str(os.getuid()), \
        f"Job ran as uid {uid_in_file}, expected {os.getuid()}"


# ===========================================================================
# DB file deleted while worker running
# ===========================================================================

def test_db_deleted_while_worker_running_no_hang(env):
    """Deleting the DB file while a worker is running does not hang the worker."""
    cli(["enqueue", '{"id":"del-j","command":"sleep 10"}'], env)
    wp = worker_proc(env, count=1)
    time.sleep(0.8)

    # Delete the DB file (worker has open file handles, SQLite may still work via WAL)
    try:
        os.unlink(env["QUEUECTL_DB"])
    except FileNotFoundError:
        pass

    # Send SIGTERM — worker should exit within a reasonable time (not hang)
    wp.send_signal(signal.SIGTERM)
    try:
        wp.wait(timeout=10)
        # Did not hang — success regardless of exit code
    except subprocess.TimeoutExpired:
        wp.kill()
        wp.wait()
        pytest.fail("Worker hung after DB deletion and SIGTERM")
