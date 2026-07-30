"""Worker signal-handling and crash-recovery integration tests."""

import os
import signal
import sqlite3
import time

from conftest import list_jobs, run, start_worker, wait_for_state


def test_sigkill_kills_worker_and_job_is_recovered(env):
    """A killed worker leaves its job available for lease recovery."""
    run(["config", "set", "recovery-timeout", "2"], env)
    run(["config", "set", "heartbeat-interval", "1"], env)
    run(["enqueue", '{"id":"hup1","command":"sleep 10"}'], env)
    worker = start_worker(env, count=1)
    wait_for_state(env, "hup1", "processing", timeout=5)

    conn = sqlite3.connect(env["QUEUECTL_DB"])
    pid = conn.execute("SELECT pid FROM workers WHERE status='running'").fetchone()[0]
    conn.close()
    os.kill(pid, signal.SIGKILL)
    worker.wait(timeout=5)

    assert next(job for job in list_jobs(env) if job["id"] == "hup1")["state"] == "processing"
    time.sleep(2.5)
    recovered_worker = start_worker(env, count=1)
    try:
        wait_for_state(env, "hup1", "pending", timeout=5)
    finally:
        recovered_worker.send_signal(signal.SIGTERM)
        recovered_worker.wait(timeout=15)


def test_multiple_rapid_signals_single_shutdown(env, tmp_path):
    """Repeated SIGTERM requests perform one bounded shutdown."""
    log = tmp_path / "rapid.log"
    run(["enqueue", '{"id":"rap1","command":"sleep 3"}'], env)
    worker = start_worker(env, count=1, logfile=log)
    time.sleep(0.5)
    for _ in range(5):
        worker.send_signal(signal.SIGTERM)
        time.sleep(0.05)
    worker.wait(timeout=10)

    stop_count = log.read_text().count("stopped")
    assert 1 <= stop_count <= 2


def test_many_workers_stop_together(env):
    """A coordinated stop marks every worker as stopped."""
    worker = start_worker(env, count=20)
    time.sleep(1.5)
    assert run(["worker", "stop"], env).returncode == 0
    worker.wait(timeout=15)

    conn = sqlite3.connect(env["QUEUECTL_DB"])
    running = conn.execute("SELECT COUNT(*) FROM workers WHERE status='running'").fetchone()[0]
    stopped = conn.execute("SELECT COUNT(*) FROM workers WHERE status='stopped'").fetchone()[0]
    conn.close()
    assert running == 0
    assert stopped == 20
