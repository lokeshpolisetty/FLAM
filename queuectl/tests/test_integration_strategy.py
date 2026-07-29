"""
test_integration_strategy.py — Integration tests covering Section 2.C of test_strategy.md.

Exercises CLI -> Storage -> Worker -> CLI complete flows:
- Enqueue then process
- Enqueue then fail then retry then succeed
- Enqueue then fail repeatedly then dead-letter
- Multi-worker contention
- Cross-terminal worker stop
- Restart after crash and confirm recovery
"""

import json
import os
import signal
import sqlite3
import subprocess
import sys
import time
import pytest
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
@pytest.fixture
def env(tmp_path):
    """Isolated environment fixture for integration tests."""
    e = os.environ.copy()
    e["QUEUECTL_DB"] = str(tmp_path / "integration_queue.db")
    e["QUEUECTL_TEST"] = "1"
    return e


def run_cli(args, env, check=True):
    res = subprocess.run(
        [sys.executable, "-m", "queuectl.cli.entrypoint"] + args,
        capture_output=True, text=True, env=env
    )
    if check and res.returncode != 0:
        raise RuntimeError(f"CLI command failed ({res.returncode}): {res.stderr}\nstdout: {res.stdout}")
    return res


def list_jobs(env, state=None):
    args = ["list", "--json"]
    if state:
        args = ["list", "--state", state, "--json"]
    out = run_cli(args, env).stdout.strip()
    return json.loads(out)


def start_worker(env, count=1):
    return subprocess.Popen(
        [sys.executable, "-m", "queuectl.cli.entrypoint", "worker", "start", "--count", str(count)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        env=env
    )


# ============================================================================
# 1. Enqueue then process
# ============================================================================

def test_integ_enqueue_then_process(env):
    """Enqueue valid job, worker processes it to completion."""
    run_cli(["enqueue", json.dumps({"id": "int-1", "command": "echo hello_integration"})], env)
    
    pending = list_jobs(env, state="pending")
    assert len(pending) == 1
    assert pending[0]["id"] == "int-1"

    wp = start_worker(env, count=1)
    time.sleep(1.5)
    wp.send_signal(signal.SIGTERM)
    wp.wait(timeout=5)

    completed = list_jobs(env, state="completed")
    assert len(completed) == 1
    assert completed[0]["id"] == "int-1"


# ============================================================================
# 2. Enqueue then fail then retry then succeed
# ============================================================================

def test_integ_fail_retry_then_succeed(env, tmp_path):
    """Job fails once, enters retry backoff, then succeeds on attempt 2."""
    run_cli(["config", "set", "backoff-base", "1"], env)
    marker = tmp_path / "retry_succeed_marker"
    
    # Command fails if marker does not exist; creates marker so second attempt succeeds
    cmd = f'python3 -c "import os, sys; flag={repr(str(marker))}; exists=os.path.exists(flag); open(flag, \'w\').write(\'OK\'); sys.exit(0 if exists else 1)"'
    run_cli(["enqueue", json.dumps({"id": "int-retry", "command": cmd, "max_retries": 3})], env)

    wp = start_worker(env, count=1)
    time.sleep(3.5)
    wp.send_signal(signal.SIGTERM)
    wp.wait(timeout=5)

    completed = list_jobs(env, state="completed")
    assert len(completed) == 1
    assert completed[0]["id"] == "int-retry"
    assert completed[0]["attempts"] == 1


# ============================================================================
# 3. Enqueue then fail repeatedly then dead-letter
# ============================================================================

def test_integ_fail_repeatedly_then_deadletter(env):
    """Job fails repeatedly until max_retries reached, then moves to DLQ."""
    run_cli(["config", "set", "backoff-base", "1"], env)
    run_cli(["enqueue", json.dumps({"id": "int-dlq", "command": "exit 1", "max_retries": 2})], env)

    wp = start_worker(env, count=1)
    time.sleep(4.5)
    wp.send_signal(signal.SIGTERM)
    wp.wait(timeout=5)

    dlq_res = run_cli(["dlq", "list", "--json"], env)
    dead_jobs = json.loads(dlq_res.stdout.strip())
    assert len(dead_jobs) == 1
    assert dead_jobs[0]["id"] == "int-dlq"
    assert dead_jobs[0]["state"] == "dead"
    assert dead_jobs[0]["attempts"] == 2


# ============================================================================
# 4. Multi-worker contention
# ============================================================================

def test_integ_multi_worker_contention(env):
    """10 jobs processed by 3 parallel workers without duplicate execution."""
    for i in range(10):
        run_cli(["enqueue", json.dumps({"id": f"mw-{i}", "command": "echo mw"})], env)

    wp = start_worker(env, count=3)
    time.sleep(2.5)
    wp.send_signal(signal.SIGTERM)
    wp.wait(timeout=5)

    completed = list_jobs(env, state="completed")
    assert len(completed) == 10


# ============================================================================
# 5. Cross-terminal worker stop
# ============================================================================

def test_integ_cross_terminal_worker_stop(env):
    """worker stop from a separate command invocation stops all running workers."""
    run_cli(["enqueue", json.dumps({"id": "int-stop", "command": "sleep 2"})], env)

    wp = start_worker(env, count=2)
    time.sleep(0.5)

    stop_res = run_cli(["worker", "stop"], env)
    assert "Sent SIGTERM to worker" in stop_res.stdout

    wp.wait(timeout=5)

    stop_again = run_cli(["worker", "stop"], env)
    assert "No running workers found." in stop_again.stdout


# ============================================================================
# 6. Restart after crash and confirm recovery
# ============================================================================

def test_integ_restart_after_crash_and_recovery(env, tmp_path):
    """Job stuck in processing after worker SIGKILL is recovered on next worker start."""
    run_cli(["config", "set", "recovery-timeout", "2"], env)
    marker = tmp_path / "crash_recovery_ok"

    run_cli(["enqueue", json.dumps({"id": "int-crash", "command": f"sleep 3 && touch {marker}"})], env)

    wp = start_worker(env, count=1)
    time.sleep(1.2)

    # Get running worker PID from DB using the correct isolated DB path
    conn = sqlite3.connect(env["QUEUECTL_DB"])
    conn.row_factory = sqlite3.Row
    row = conn.execute("SELECT pid FROM workers WHERE status='running'").fetchone()
    conn.close()
    assert row is not None

    os.kill(row[0], signal.SIGKILL)
    try:
        os.kill(wp.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    wp.wait(timeout=5)

    # Wait for recovery-timeout to elapse
    time.sleep(2.5)

    # Start new worker process to recover and complete job
    wp2 = start_worker(env, count=1)
    time.sleep(5.0)
    wp2.send_signal(signal.SIGTERM)
    wp2.wait(timeout=8)

    completed = list_jobs(env, state="completed")
    assert any(j["id"] == "int-crash" for j in completed)
    assert marker.exists()
