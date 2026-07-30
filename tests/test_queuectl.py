"""
Black-box tests that drive the real CLI exactly like the grader's
automated test script will: as subprocesses, through stdin/stdout, using
real signals against real PIDs. Each test gets its own temp SQLite file
via the QUEUECTL_DB env var so tests don't interfere with each other.
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
def run(args, env, timeout=10):
    return subprocess.run(
        [sys.executable, "-m", "queuectl"] + args,
        capture_output=True, text=True, timeout=timeout, env=env,
    )


@pytest.fixture()
def env(tmp_path):
    e = os.environ.copy()
    e["QUEUECTL_DB"] = str(tmp_path / "queue.db")
    e["QUEUECTL_TEST"] = "1"
    return e


def list_jobs(env, state=None):
    args = ["list", "--json"]
    if state:
        args = ["list", "--state", state, "--json"]
    out = run(args, env).stdout.strip()
    return json.loads(out)


def start_worker(env, count=1, logfile=None):
    f = open(logfile, "w") if logfile else subprocess.DEVNULL
    return subprocess.Popen(
        [sys.executable, "-m", "queuectl"] + ["worker", "start", "--count", str(count)],
        stdout=f, stderr=subprocess.STDOUT, env=env,
    )


def wait_for_state(env, job_id, state, timeout=10):
    deadline = time.time() + timeout
    while time.time() < deadline:
        jobs = list_jobs(env)
        for j in jobs:
            if j["id"] == job_id and j["state"] == state:
                return j
        time.sleep(0.2)
    raise AssertionError(f"job {job_id} did not reach state {state} in time; last seen: {jobs}")


# ---------------------------------------------------------------------------
# Scenario 1: a basic job completes
# ---------------------------------------------------------------------------

def test_basic_job_completes(env, tmp_path):
    marker = tmp_path / "ran"
    r = run(["enqueue", json.dumps({"id": "j1", "command": f"touch {marker}"})], env)
    assert r.returncode == 0

    wp = start_worker(env)
    try:
        wait_for_state(env, "j1", "completed", timeout=10)
    finally:
        wp.send_signal(signal.SIGTERM)
        wp.wait(timeout=5)
    assert marker.exists()


# ---------------------------------------------------------------------------
# Scenario 2: a failing job retries with backoff and lands in the DLQ
# ---------------------------------------------------------------------------

def test_failing_job_retries_then_dlq(env):
    run(["config", "set", "backoff-base", "1"], env)
    r = run(["enqueue", json.dumps({"id": "j2", "command": "exit 1", "max_retries": 2})], env)
    assert r.returncode == 0

    wp = start_worker(env)
    try:
        job = wait_for_state(env, "j2", "dead", timeout=15)
    finally:
        wp.send_signal(signal.SIGTERM)
        wp.wait(timeout=5)

    assert job["attempts"] == 2
    dlq = json.loads(run(["dlq", "list", "--json"], env).stdout)
    assert any(j["id"] == "j2" for j in dlq)


def test_dlq_retry_resets_attempts(env):
    run(["config", "set", "backoff-base", "1"], env)
    run(["enqueue", json.dumps({"id": "j2b", "command": "exit 1", "max_retries": 1})], env)
    wp = start_worker(env)
    try:
        wait_for_state(env, "j2b", "dead", timeout=15)
    finally:
        wp.send_signal(signal.SIGTERM)
        wp.wait(timeout=5)

    r = run(["dlq", "retry", "j2b"], env)
    assert r.returncode == 0
    jobs = list_jobs(env)
    job = next(j for j in jobs if j["id"] == "j2b")
    assert job["state"] == "pending"
    assert job["attempts"] == 0


# ---------------------------------------------------------------------------
# Scenario 3: many jobs across multiple workers — every job runs exactly once
# ---------------------------------------------------------------------------

def test_many_jobs_multiple_workers_exactly_once(env, tmp_path):
    log = tmp_path / "exec_log.txt"
    n = 25
    for i in range(n):
        run(["enqueue", json.dumps({"id": f"m{i}", "command": f"echo m{i} >> {log}"})], env)

    workers = [start_worker(env, count=1) for _ in range(4)]
    try:
        deadline = time.time() + 20
        while time.time() < deadline:
            jobs = list_jobs(env)
            if all(j["state"] == "completed" for j in jobs) and len(jobs) == n:
                break
            time.sleep(0.3)
    finally:
        for w in workers:
            w.send_signal(signal.SIGTERM)
        for w in workers:
            w.wait(timeout=5)

    lines = log.read_text().splitlines() if log.exists() else []
    assert len(lines) == n, f"expected {n} executions, got {len(lines)}"
    assert len(set(lines)) == n, "a job ran more than once"


# ---------------------------------------------------------------------------
# Scenario 4: workers are SIGKILLed mid-job; after restart, job completes
# and nothing is stuck in `processing`
# ---------------------------------------------------------------------------

def test_crash_recovery_after_sigkill(env, tmp_path):
    run(["config", "set", "recovery-timeout", "2"], env)
    run(["config", "set", "heartbeat-interval", "1"], env)
    marker = tmp_path / "crash_done"
    run(["enqueue", json.dumps({"id": "c1", "command": f"sleep 3 && touch {marker}"})], env)

    wp = start_worker(env)
    time.sleep(1.2)  # let it claim and start the job

    conn = sqlite3.connect(env["QUEUECTL_DB"])
    row = conn.execute("SELECT pid FROM workers WHERE status='running'").fetchone()
    conn.close()
    assert row is not None, "expected a registered running worker"
    child_pid = row[0]

    os.kill(child_pid, signal.SIGKILL)  # simulate a hard crash, no cleanup
    wp.wait(timeout=5)  # parent process exits once its child dies

    # Right after the crash the job should still show as processing or
    # already reaped back to pending, but never silently vanish.
    jobs = list_jobs(env)
    assert any(j["id"] == "c1" for j in jobs)

    # Wait past recovery-timeout, then start a fresh worker to pick it back up.
    time.sleep(2.5)
    wp2 = start_worker(env)
    try:
        wait_for_state(env, "c1", "completed", timeout=15)
    finally:
        wp2.send_signal(signal.SIGTERM)
        wp2.wait(timeout=5)

    assert marker.exists()


# ---------------------------------------------------------------------------
# Scenario 5: jobs survive a full restart (no worker running at all)
# ---------------------------------------------------------------------------

def test_jobs_survive_restart(env):
    run(["enqueue", json.dumps({"id": "r1", "command": "echo restart-test"})], env)
    jobs = list_jobs(env)
    assert any(j["id"] == "r1" and j["state"] == "pending" for j in jobs)
    # No process is running here at all — persistence must be file-based,
    # not in-memory. Re-reading via a brand new CLI invocation proves it.
    jobs_again = list_jobs(env)
    assert any(j["id"] == "r1" for j in jobs_again)


# ---------------------------------------------------------------------------
# Graceful shutdown: in-flight job finishes, no new job is claimed
# ---------------------------------------------------------------------------

def test_graceful_shutdown_finishes_current_job_only(env, tmp_path):
    m1, m2 = tmp_path / "g1", tmp_path / "g2"
    run(["enqueue", json.dumps({"id": "g1", "command": f"sleep 2 && touch {m1}"})], env)
    run(["enqueue", json.dumps({"id": "g2", "command": f"touch {m2}"})], env)

    wp = start_worker(env, count=1)
    time.sleep(0.7)
    wp.send_signal(signal.SIGTERM)
    wp.wait(timeout=10)

    assert m1.exists(), "in-flight job should have been allowed to finish"
    assert not m2.exists(), "no new job should start after shutdown was requested"

    jobs = {j["id"]: j["state"] for j in list_jobs(env)}
    assert jobs["g1"] == "completed"
    assert jobs["g2"] == "pending"


# ---------------------------------------------------------------------------
# --json output must be pure JSON on stdout (interface contract)
# ---------------------------------------------------------------------------

def test_list_json_is_pure_json(env):
    run(["enqueue", json.dumps({"id": "jj1", "command": "echo hi"})], env)
    r = run(["list", "--json"], env)
    # Must parse cleanly with nothing extra on stdout.
    data = json.loads(r.stdout)
    assert isinstance(data, list)
    assert any(j["id"] == "jj1" for j in data)
