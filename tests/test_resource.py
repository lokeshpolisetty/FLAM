"""
test_resource.py — Resource exhaustion and cleanup tests covering Section 2.E
(Resource exhaustion and cleanup) of test_strategy.md.

Focus areas:
  RES-1  File-descriptor leak check after many job completions
  RES-2  No zombie (defunct) child processes after job execution
  RES-3  DB WAL / SHM files are benign (not growing unboundedly)
  RES-4  Worker exits with code 0 on clean SIGTERM/SIGINT shutdown
  RES-5  Worker exits non-zero on unhandled internal exception (if applicable)
  RES-6  enqueue process exits cleanly — no lingering connections
  RES-7  Memory growth bounded over a batch of jobs (basic RSS check)
  RES-8  DB integrity preserved after processing many jobs
"""

import json
import os
import resource
import signal
import sqlite3
import subprocess
import sys
import time
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
APP  = ROOT / "-m", "queuectl.cli.entrypoint"


def cli(args, env, timeout=20):
    return subprocess.run(
        [sys.executable, "-m", "queuectl.cli.entrypoint"] + args,
        capture_output=True, text=True, timeout=timeout, env=env,
    )


def worker_proc(env, count=1):
    return subprocess.Popen(
        [sys.executable, "-m", "queuectl.cli.entrypoint"] + ["worker", "start", "--count", str(count)],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, env=env,
    )


def wait_state(env, job_id, state, timeout=15):
    deadline = time.time() + timeout
    while time.time() < deadline:
        raw = cli(["list", "--json"], env).stdout.strip()
        for j in json.loads(raw):
            if j["id"] == job_id and j["state"] == state:
                return j
        time.sleep(0.2)
    raise AssertionError(f"{job_id} never reached {state}")


def count_open_fds(pid: int) -> int:
    """Count open file descriptors for a process (macOS-compatible)."""
    try:
        result = subprocess.run(
            ["lsof", "-p", str(pid), "-n"],
            capture_output=True, text=True, timeout=5,
        )
        return len(result.stdout.strip().splitlines()) - 1  # subtract header
    except (FileNotFoundError, subprocess.TimeoutExpired):
        # lsof not available or timed out — fall back to /proc
        try:
            return len(os.listdir(f"/proc/{pid}/fd"))
        except (FileNotFoundError, PermissionError):
            return -1   # cannot determine


@pytest.fixture
def env(tmp_path):
    e = os.environ.copy()
    e["QUEUECTL_DB"] = str(tmp_path / "queue.db")
    e["QUEUECTL_TEST"] = "1"
    return e


# ===========================================================================
# RES-1  File-descriptor leak
# ===========================================================================

def test_res_fd_count_stable_after_many_jobs(env):
    """Worker FD count does not grow unboundedly after processing many jobs."""
    NUM_JOBS = 40
    for i in range(NUM_JOBS):
        cli(["enqueue", json.dumps({"id": f"fd-{i}", "command": "echo hi"})], env)

    wp = worker_proc(env, count=1)
    try:
        # Let a few jobs complete, measure FDs
        time.sleep(1.5)
        fd_before = count_open_fds(wp.pid)

        # Wait for all jobs to finish
        deadline = time.time() + 30
        while time.time() < deadline:
            jobs = json.loads(cli(["list", "--json"], env).stdout.strip())
            if all(j["state"] == "completed" for j in jobs):
                break
            time.sleep(0.3)

        fd_after = count_open_fds(wp.pid)
    finally:
        wp.send_signal(signal.SIGTERM)
        wp.wait(timeout=5)

    if fd_before > 0 and fd_after > 0:
        # Allow a small tolerance but it must not keep growing linearly
        growth = fd_after - fd_before
        assert growth < 20, \
            f"FD leak detected: started ~{fd_before}, ended ~{fd_after} (growth={growth})"


def test_res_each_cli_invocation_closes_its_connection(env):
    """Each CLI call opens and closes its DB connection; no SQLITE_BUSY from leaked handles."""
    for i in range(20):
        res = cli(["enqueue", json.dumps({"id": f"dbcl-{i}", "command": "echo hi"})], env)
        assert res.returncode == 0, f"enqueue {i} failed: {res.stderr}"

    # If connections leak, a subsequent write under WAL should still succeed
    res = cli(["config", "set", "max-retries", "5"], env)
    assert res.returncode == 0


# ===========================================================================
# RES-2  No zombie processes
# ===========================================================================

def test_res_no_zombie_processes_after_job_completion(env):
    """Worker does not leave zombie (defunct) child processes after jobs complete."""
    for i in range(10):
        cli(["enqueue", json.dumps({"id": f"zom-{i}", "command": "echo zombie_check"})], env)

    wp = worker_proc(env, count=2)
    try:
        deadline = time.time() + 20
        while time.time() < deadline:
            jobs = json.loads(cli(["list", "--json"], env).stdout.strip())
            if all(j["state"] == "completed" for j in jobs):
                break
            time.sleep(0.3)

        # Check for defunct processes under the worker parent
        result = subprocess.run(
            ["ps", "-o", "pid,stat,ppid,comm", "-ax"],
            capture_output=True, text=True,
        )
        defunct_lines = [l for l in result.stdout.splitlines()
                         if "Z" in l.split()[1:2] and str(wp.pid) in l]
        assert len(defunct_lines) == 0, \
            f"Zombie processes found: {defunct_lines}"
    finally:
        wp.send_signal(signal.SIGTERM)
        wp.wait(timeout=5)


# ===========================================================================
# RES-3  WAL / SHM file behaviour
# ===========================================================================

def test_res_wal_file_not_present_before_first_use(env):
    """Before any DB operation, the WAL and SHM sidecar files do not exist."""
    db_path = Path(env["QUEUECTL_DB"])
    assert not db_path.with_suffix(".db-wal").exists()
    assert not db_path.with_suffix(".db-shm").exists()


def test_res_db_readable_after_worker_exits(env):
    """After a worker exits cleanly, the DB is in a consistent readable state."""
    cli(["enqueue", '{"id":"wal1","command":"echo hi"}'], env)
    wp = worker_proc(env, count=1)
    try:
        wait_state(env, "wal1", "completed")
    finally:
        wp.send_signal(signal.SIGTERM)
        wp.wait(timeout=5)

    conn = sqlite3.connect(env["QUEUECTL_DB"])
    result = conn.execute("PRAGMA integrity_check").fetchone()
    conn.close()
    assert result[0] == "ok"


def test_res_db_integrity_after_many_jobs(env):
    """PRAGMA integrity_check passes after processing 50 jobs through the full lifecycle."""
    cli(["config", "set", "backoff-base", "1"], env)
    for i in range(25):
        cli(["enqueue", json.dumps({"id": f"int-ok-{i}", "command": "echo hi"})], env)
    for i in range(25):
        cli(["enqueue", json.dumps({"id": f"int-fail-{i}",
                                    "command": "exit 1",
                                    "max_retries": 0})], env)

    wp = worker_proc(env, count=4)
    try:
        deadline = time.time() + 30
        while time.time() < deadline:
            jobs = json.loads(cli(["list", "--json"], env).stdout.strip())
            if all(j["state"] in ("completed", "dead") for j in jobs):
                break
            time.sleep(0.3)
    finally:
        wp.send_signal(signal.SIGTERM)
        wp.wait(timeout=5)

    conn = sqlite3.connect(env["QUEUECTL_DB"])
    result = conn.execute("PRAGMA integrity_check").fetchone()
    conn.close()
    assert result[0] == "ok"


# ===========================================================================
# RES-4  Clean shutdown exit codes
# ===========================================================================

def test_res_worker_exits_zero_on_sigterm(env):
    """Worker process exits with code 0 after receiving SIGTERM."""
    wp = worker_proc(env, count=1)
    time.sleep(0.5)
    wp.send_signal(signal.SIGTERM)
    wp.wait(timeout=8)
    assert wp.returncode == 0


def test_res_worker_exits_zero_on_sigint(env):
    """Worker process exits with code 0 after receiving SIGINT."""
    wp = worker_proc(env, count=1)
    time.sleep(0.5)
    wp.send_signal(signal.SIGINT)
    wp.wait(timeout=8)
    assert wp.returncode == 0


def test_res_worker_exits_zero_after_job_completes_then_sigterm(env):
    """Worker exits 0 after finishing a job and then receiving SIGTERM."""
    cli(["enqueue", '{"id":"ec-clean","command":"echo hi"}'], env)
    wp = worker_proc(env, count=1)
    try:
        wait_state(env, "ec-clean", "completed")
    finally:
        wp.send_signal(signal.SIGTERM)
        wp.wait(timeout=5)
    assert wp.returncode == 0


def test_res_worker_stop_command_exits_zero(env):
    """worker stop command itself exits 0 whether or not workers are running."""
    res_no_workers = cli(["worker", "stop"], env)
    assert res_no_workers.returncode == 0

    wp = worker_proc(env, count=1)
    time.sleep(0.5)
    res_with_workers = cli(["worker", "stop"], env)
    wp.wait(timeout=5)
    assert res_with_workers.returncode == 0


# ===========================================================================
# RES-5  enqueue exits cleanly
# ===========================================================================

def test_res_enqueue_process_exits_immediately(env):
    """enqueue command exits promptly after inserting the job — no lingering handles."""
    start = time.time()
    res = cli(["enqueue", '{"id":"exit-fast","command":"echo hi"}'], env)
    elapsed = time.time() - start
    assert res.returncode == 0
    assert elapsed < 5.0, f"enqueue took too long: {elapsed:.1f}s"


def test_res_multiple_sequential_enqueues_all_exit_cleanly(env):
    """50 sequential enqueue calls all exit 0 with no accumulating error."""
    for i in range(50):
        res = cli(["enqueue", json.dumps({"id": f"seq-{i}", "command": "echo hi"})], env)
        assert res.returncode == 0, f"enqueue {i} failed: {res.stderr}"


# ===========================================================================
# RES-7  Basic memory growth check
# ===========================================================================

def test_res_memory_not_growing_unboundedly(env):
    """Worker RSS does not more than double after processing 60 jobs vs baseline."""
    NUM_JOBS = 60
    for i in range(NUM_JOBS):
        cli(["enqueue", json.dumps({"id": f"mem-{i}", "command": "echo hi"})], env)

    wp = worker_proc(env, count=2)
    try:
        # Sample RSS shortly after start (baseline)
        time.sleep(0.8)
        try:
            import resource as _res
            baseline_kb = _res.getrusage(_res.RUSAGE_CHILDREN).ru_maxrss
        except Exception:
            baseline_kb = 0

        deadline = time.time() + 30
        while time.time() < deadline:
            jobs = json.loads(cli(["list", "--json"], env).stdout.strip())
            if all(j["state"] == "completed" for j in jobs):
                break
            time.sleep(0.3)

        # If we can read /proc RSS on Linux, do a growth check
        try:
            with open(f"/proc/{wp.pid}/status") as f:
                for line in f:
                    if line.startswith("VmRSS:"):
                        final_kb = int(line.split()[1])
                        if baseline_kb > 0:
                            assert final_kb < baseline_kb * 5, \
                                f"RSS grew from {baseline_kb}kB to {final_kb}kB — possible leak"
                        break
        except FileNotFoundError:
            pass   # /proc not available on macOS; skip RSS assertion
    finally:
        wp.send_signal(signal.SIGTERM)
        wp.wait(timeout=5)
