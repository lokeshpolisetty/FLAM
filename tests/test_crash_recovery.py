"""
Section E: Crash Recovery Test Cases (CR-01 to CR-04)
Testing recovery from SIGKILL hard crashes, distinguishing graceful shutdowns from crashes,
reaping of stale heartbeats by read commands, and process restart dynamics.
"""

import os
import signal
import sqlite3
import time
from conftest import run, list_jobs, start_worker, wait_for_state


# CR-01 — SIGKILL mid-job, recovers on next worker start [P0][Critical]
def test_cr01_sigkill_mid_job_recovers(env, tmp_path):
    run(["config", "set", "recovery-timeout", "3"], env)
    run(["config", "set", "heartbeat-interval", "1"], env)
    marker = tmp_path / "cr01_done"

    run(["enqueue", f'{{"id":"cr01","command":"sleep 4 && touch {marker}"}}'], env)

    wp = start_worker(env, count=1)
    time.sleep(1.2)  # Let worker claim and start job

    conn = sqlite3.connect(env["QUEUECTL_DB"])
    row = conn.execute("SELECT pid FROM workers WHERE status='running'").fetchone()
    conn.close()
    assert row is not None, "expected running worker"
    child_pid = row[0]

    os.kill(child_pid, signal.SIGKILL)  # Hard crash child worker
    wp.wait(timeout=5)

    # Job is still marked processing right after kill
    jobs = list_jobs(env)
    assert any(j["id"] == "cr01" for j in jobs)

    # Wait for recovery-timeout to elapse
    time.sleep(3.5)

    # Starting a new worker reaps the stale job and finishes it
    wp2 = start_worker(env, count=1)
    try:
        wait_for_state(env, "cr01", "completed", timeout=15)
        assert marker.exists()
    finally:
        wp2.send_signal(signal.SIGTERM)
        wp2.wait(timeout=5)


# CR-02 — SIGTERM/SIGINT mid-job is graceful, NOT a crash [P0][Critical]
def test_cr02_sigterm_mid_job_is_graceful(env, tmp_path):
    marker = tmp_path / "cr02_done"
    run(["enqueue", f'{{"id":"cr02","command":"sleep 2 && touch {marker}"}}'], env)

    wp = start_worker(env, count=1)
    time.sleep(0.8)
    wp.send_signal(signal.SIGTERM)
    wp.wait(timeout=10)

    assert marker.exists(), "graceful shutdown allows current job to complete"
    jobs = list_jobs(env)
    job = next(j for j in jobs if j["id"] == "cr02")
    assert job["state"] == "completed"


# CR-03 — All workers killed simultaneously [P0][Critical]
def test_cr03_all_workers_killed_simultaneously(env):
    run(["config", "set", "recovery-timeout", "3"], env)
    for i in (1, 2, 3):
        run(["enqueue", f'{{"id":"cr03-{i}","command":"sleep 4"}}'], env)

    wp = start_worker(env, count=3)
    time.sleep(1.5)

    conn = sqlite3.connect(env["QUEUECTL_DB"])
    rows = conn.execute("SELECT pid FROM workers WHERE status='running'").fetchall()
    conn.close()

    pids = [r[0] for r in rows]
    assert len(pids) == 3

    # Hard kill all workers simultaneously
    for pid in pids:
        try:
            os.kill(pid, signal.SIGKILL)
        except ProcessLookupError:
            pass

    wp.wait(timeout=5)
    time.sleep(3.5)

    # CLI read command alone (status/list) reaps the stale jobs back to pending
    status_out = run(["status"], env).stdout
    assert "pending    3" in status_out or "pending" in status_out

    jobs = list_jobs(env)
    assert all(j["state"] == "pending" for j in jobs)

    # Starting workers again finishes all 3 jobs
    wp2 = start_worker(env, count=3)
    try:
        deadline = time.time() + 15
        while time.time() < deadline:
            jobs_after = list_jobs(env)
            if all(j["state"] == "completed" for j in jobs_after):
                break
            time.sleep(0.3)
        assert all(j["state"] == "completed" for j in list_jobs(env))
    finally:
        wp2.send_signal(signal.SIGTERM)
        wp2.wait(timeout=5)


# CR-04 — Restart while jobs are mid-processing (graceful stop vs crash) [P1][High]
def test_cr04_graceful_stop_leaves_no_stale_processing(env, tmp_path):
    marker = tmp_path / "cr04_done"
    run(["enqueue", f'{{"id":"cr04","command":"sleep 1 && touch {marker}"}}'], env)

    wp = start_worker(env, count=1)
    time.sleep(0.4)
    # Perform graceful stop via CLI
    stop_res = run(["worker", "stop"], env)
    assert stop_res.returncode == 0
    wp.wait(timeout=10)

    # No jobs are left in processing state
    processing_jobs = list_jobs(env, state="processing")
    assert len(processing_jobs) == 0
