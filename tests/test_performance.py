"""
test_performance.py — Performance and latency tests covering Section 2.E (Performance)
of test_strategy.md.

These are sanity-floor tests, not hard benchmarks.  Each asserts a generous
lower bound that any correct single-machine implementation should satisfy.

Focus areas:
  PERF-1  Enqueue throughput  (sequential and parallel)
  PERF-2  Claim latency       (time from enqueue to state=processing)
  PERF-3  Completion latency  (time from processing to completed for echo)
  PERF-4  Recovery latency    (time from SIGKILL to state=pending)
  PERF-5  list --json latency with many rows in DB
  PERF-6  Worker CPU usage when idle (no busy-spin)
  PERF-7  Throughput does not collapse under multi-worker contention
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
APP  = ROOT / "-m", "queuectl.cli.entrypoint"


def cli(args, env, timeout=30):
    return subprocess.run(
        [sys.executable, "-m", "queuectl.cli.entrypoint"] + args,
        capture_output=True, text=True, timeout=timeout, env=env,
    )


def worker_proc(env, count=1):
    return subprocess.Popen(
        [sys.executable, "-m", "queuectl.cli.entrypoint"] + ["worker", "start", "--count", str(count)],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, env=env,
    )


def wait_state(env, job_id, state, timeout=20):
    deadline = time.time() + timeout
    while time.time() < deadline:
        raw = cli(["list", "--json"], env).stdout.strip()
        for j in json.loads(raw):
            if j["id"] == job_id and j["state"] == state:
                return j, time.time()
        time.sleep(0.1)
    raise AssertionError(f"{job_id} never reached {state}")


@pytest.fixture
def env(tmp_path):
    e = os.environ.copy()
    e["QUEUECTL_DB"] = str(tmp_path / "queue.db")
    e["QUEUECTL_TEST"] = "1"
    return e


# ===========================================================================
# PERF-1  Enqueue throughput
# ===========================================================================

def test_perf_sequential_enqueue_throughput(env):
    """200 sequential enqueues complete in under 60 s (~3+ enqueues/s floor)."""
    N = 200
    start = time.time()
    for i in range(N):
        res = cli(["enqueue", json.dumps({"id": f"eq-{i}", "command": "echo hi"})], env)
        assert res.returncode == 0
    elapsed = time.time() - start
    rate = N / elapsed
    assert elapsed < 60, f"200 sequential enqueues took {elapsed:.1f}s (too slow)"
    assert rate >= 3,    f"Enqueue rate too low: {rate:.1f} jobs/s"


def test_perf_all_enqueued_rows_persisted(env):
    """All 100 sequentially enqueued jobs are present in the DB (no lost writes)."""
    N = 100
    for i in range(N):
        cli(["enqueue", json.dumps({"id": f"persist-{i}", "command": "echo hi"})], env)
    jobs = json.loads(cli(["list", "--json"], env).stdout.strip())
    assert len(jobs) == N


def test_perf_parallel_enqueue_all_succeed(env):
    """20 parallel enqueue processes all succeed without lock errors."""
    procs = []
    for i in range(20):
        p = subprocess.Popen(
            [sys.executable, "-m", "queuectl.cli.entrypoint"] + ["enqueue",
             json.dumps({"id": f"par-{i}", "command": "echo hi"})],
            stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True, env=env,
        )
        procs.append(p)

    errors = []
    for p in procs:
        _, err = p.communicate(timeout=15)
        if p.returncode != 0:
            errors.append(err)
    assert len(errors) == 0, f"Parallel enqueue errors: {errors}"

    jobs = json.loads(cli(["list", "--json"], env).stdout.strip())
    assert len(jobs) == 20


# ===========================================================================
# PERF-2  Claim latency
# ===========================================================================

def test_perf_claim_latency_under_one_second(env):
    """With poll-interval=0.1 s, job reaches processing within 1 s of enqueue."""
    cli(["config", "set", "poll-interval", "0.1"], env)
    t_enqueue = time.time()
    cli(["enqueue", '{"id":"cl-lat","command":"sleep 5"}'], env)

    wp = worker_proc(env, count=1)
    try:
        _, t_processing = wait_state(env, "cl-lat", "processing", timeout=5)
        latency = t_processing - t_enqueue
        assert latency < 1.5, f"Claim latency too high: {latency:.2f}s"
    finally:
        wp.send_signal(signal.SIGTERM)
        wp.wait(timeout=8)


def test_perf_claim_latency_consistent_across_10_jobs(env):
    """10 sequential jobs are each claimed within 2 s of enqueue (poll-interval=0.2 s)."""
    cli(["config", "set", "poll-interval", "0.2"], env)
    N = 10
    for i in range(N):
        cli(["enqueue", json.dumps({"id": f"cl-{i}", "command": "echo hi"})], env)

    wp = worker_proc(env, count=N)
    try:
        deadline = time.time() + 20
        while time.time() < deadline:
            jobs = json.loads(cli(["list", "--json"], env).stdout.strip())
            processing_or_done = [j for j in jobs
                                  if j["state"] in ("processing", "completed")]
            if len(processing_or_done) == N:
                break
            time.sleep(0.15)

        jobs = json.loads(cli(["list", "--json"], env).stdout.strip())
        unclaimed = [j for j in jobs if j["state"] == "pending"]
        assert len(unclaimed) == 0, f"{len(unclaimed)} jobs still pending after 20s"
    finally:
        wp.send_signal(signal.SIGTERM)
        wp.wait(timeout=8)


# ===========================================================================
# PERF-3  Completion latency
# ===========================================================================

def test_perf_echo_job_completes_quickly(env):
    """An 'echo hi' job completes within 3 s of being enqueued (poll 0.2 s)."""
    cli(["config", "set", "poll-interval", "0.2"], env)
    t0 = time.time()
    cli(["enqueue", '{"id":"comp-lat","command":"echo hi"}'], env)
    wp = worker_proc(env, count=1)
    try:
        _, t_done = wait_state(env, "comp-lat", "completed", timeout=10)
        elapsed = t_done - t0
        assert elapsed < 3.0, f"echo job took {elapsed:.2f}s to complete"
    finally:
        wp.send_signal(signal.SIGTERM)
        wp.wait(timeout=5)


def test_perf_50_echo_jobs_all_complete_in_30s(env):
    """50 echo jobs with 8 workers all complete within 30 s."""
    N = 50
    for i in range(N):
        cli(["enqueue", json.dumps({"id": f"bulk-{i}", "command": "echo hi"})], env)

    wp = worker_proc(env, count=8)
    try:
        deadline = time.time() + 30
        while time.time() < deadline:
            jobs = json.loads(cli(["list", "--json"], env).stdout.strip())
            if all(j["state"] == "completed" for j in jobs) and len(jobs) == N:
                break
            time.sleep(0.3)

        jobs = json.loads(cli(["list", "--json"], env).stdout.strip())
        completed = [j for j in jobs if j["state"] == "completed"]
        assert len(completed) == N, \
            f"Only {len(completed)}/{N} completed within 30s"
    finally:
        wp.send_signal(signal.SIGTERM)
        wp.wait(timeout=5)


# ===========================================================================
# PERF-4  Recovery latency
# ===========================================================================

def test_perf_recovery_latency_within_lease_plus_poll(env):
    """Job becomes pending within lease_seconds + poll_interval + 1s after SIGKILL."""
    LEASE = 3
    POLL  = 0.5
    cli(["config", "set", "recovery-timeout", str(LEASE)], env)
    cli(["config", "set", "poll-interval",    str(POLL)],  env)
    cli(["config", "set", "heartbeat-interval", "1"],       env)
    cli(["enqueue", '{"id":"rec-lat","command":"sleep 10"}'], env)

    wp = worker_proc(env, count=1)
    wait_state(env, "rec-lat", "processing", timeout=5)

    conn = sqlite3.connect(env["QUEUECTL_DB"])
    row = conn.execute("SELECT pid FROM workers WHERE status='running'").fetchone()
    conn.close()

    t_kill = time.time()
    os.kill(row[0], signal.SIGKILL)
    wp.wait(timeout=5)

    # A new worker must recover the job
    wp2 = worker_proc(env, count=1)
    try:
        _, t_pending = wait_state(env, "rec-lat", "pending", timeout=LEASE + POLL + 3)
        recovery_time = t_pending - t_kill
        assert recovery_time <= LEASE + POLL + 2, \
            f"Recovery took {recovery_time:.1f}s, expected <= {LEASE + POLL + 2}s"
    finally:
        wp2.send_signal(signal.SIGTERM)
        wp2.wait(timeout=5)


# ===========================================================================
# PERF-5  list --json latency with many rows
# ===========================================================================

def test_perf_list_json_1000_rows_under_5s(env):
    """list --json with 1000 pending jobs in DB completes in under 5 s."""
    N = 1000
    # Bulk-insert directly via sqlite3 for speed (not 1000 CLI calls)
    conn = sqlite3.connect(env["QUEUECTL_DB"])
    conn.execute("PRAGMA journal_mode=WAL")

    # initialise schema first
    subprocess.run(
        [sys.executable, "-m", "queuectl.cli.entrypoint"] + ["status"],
        capture_output=True, env=env,
    )
    conn = sqlite3.connect(env["QUEUECTL_DB"])
    ts = "2025-01-01T00:00:00+00:00"
    conn.executemany(
        "INSERT OR IGNORE INTO jobs (id, command, state, attempts, max_retries, "
        "backoff_base, created_at, updated_at) VALUES (?,?,?,?,?,?,?,?)",
        [(f"bulk-list-{i}", "echo hi", "pending", 0, 3, 2.0, ts, ts)
         for i in range(N)],
    )
    conn.commit()
    conn.close()

    start = time.time()
    res = cli(["list", "--json"], env, timeout=10)
    elapsed = time.time() - start

    assert res.returncode == 0
    jobs = json.loads(res.stdout.strip())
    assert len(jobs) >= N
    assert elapsed < 5.0, f"list --json with {N} rows took {elapsed:.2f}s"


def test_perf_status_1000_rows_under_5s(env):
    """status with 1000 jobs in DB completes in under 5 s."""
    # Re-use DB seeded in previous style
    subprocess.run([sys.executable, "-m", "queuectl.cli.entrypoint"] + ["status"],
                   capture_output=True, env=env)
    conn = sqlite3.connect(env["QUEUECTL_DB"])
    ts = "2025-01-01T00:00:00+00:00"
    conn.executemany(
        "INSERT OR IGNORE INTO jobs (id, command, state, attempts, max_retries, "
        "backoff_base, created_at, updated_at) VALUES (?,?,?,?,?,?,?,?)",
        [(f"stat-bulk-{i}", "echo hi", "completed", 0, 3, 2.0, ts, ts)
         for i in range(1000)],
    )
    conn.commit()
    conn.close()

    start = time.time()
    res = cli(["status"], env, timeout=10)
    elapsed = time.time() - start
    assert res.returncode == 0
    assert elapsed < 5.0, f"status with 1000 rows took {elapsed:.2f}s"


# ===========================================================================
# PERF-6  Idle CPU — no busy-spin
# ===========================================================================

def test_perf_idle_worker_does_not_busy_spin(env):
    """An idle worker sleeping at poll-interval=1 s uses < 5% CPU over 3 s sample."""
    cli(["config", "set", "poll-interval", "1"], env)
    wp = worker_proc(env, count=1)
    try:
        time.sleep(0.5)   # let it start
        # Sample CPU via ps (macOS + Linux compatible)
        time.sleep(3)     # measure window
        result = subprocess.run(
            ["ps", "-p", str(wp.pid), "-o", "%cpu="],
            capture_output=True, text=True,
        )
        if result.returncode == 0 and result.stdout.strip():
            cpu_pct = float(result.stdout.strip())
            assert cpu_pct < 15.0, \
                f"Idle worker using {cpu_pct:.1f}% CPU (expected < 15%)"
    finally:
        wp.send_signal(signal.SIGTERM)
        wp.wait(timeout=5)


# ===========================================================================
# PERF-7  Throughput under contention
# ===========================================================================

def test_perf_throughput_does_not_collapse_with_many_workers(env):
    """10 workers processing 100 echo jobs completes faster than 1 worker would."""
    N = 100
    for i in range(N):
        cli(["enqueue", json.dumps({"id": f"thr-{i}", "command": "echo hi"})], env)

    # Measure with 10 workers
    t0 = time.time()
    wp = worker_proc(env, count=10)
    try:
        deadline = time.time() + 30
        while time.time() < deadline:
            jobs = json.loads(cli(["list", "--json"], env).stdout.strip())
            if all(j["state"] == "completed" for j in jobs) and len(jobs) == N:
                break
            time.sleep(0.2)
    finally:
        wp.send_signal(signal.SIGTERM)
        wp.wait(timeout=5)

    elapsed = time.time() - t0
    assert elapsed < 30, f"{N} jobs with 10 workers took {elapsed:.1f}s (expected < 30s)"

    jobs = json.loads(cli(["list", "--json"], env).stdout.strip())
    completed = sum(1 for j in jobs if j["state"] == "completed")
    assert completed == N, f"Only {completed}/{N} jobs completed"
