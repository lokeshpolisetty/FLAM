"""
Section C: State Machine Test Cases (S-01 to S-10)
Testing valid and invalid job state transitions and guarantees against duplicate execution.
"""

import signal
import sqlite3
import time
from conftest import run, list_jobs, start_worker, wait_for_state


# S-01 — Valid transition: pending -> processing [P0][High]
def test_s01_valid_transition_pending_to_processing(env):
    run(["enqueue", '{"id":"s01","command":"sleep 3"}'], env)
    wp = start_worker(env, count=1)
    try:
        job = wait_for_state(env, "s01", "processing", timeout=10)
        assert job["state"] == "processing"
        assert job["worker_id"] is None or job["worker_id"].startswith("w-")
    finally:
        wp.send_signal(signal.SIGTERM)
        wp.wait(timeout=5)


# S-02 — Valid transition: processing -> completed [P0][High]
def test_s02_valid_transition_processing_to_completed(env):
    run(["enqueue", '{"id":"s02","command":"echo done"}'], env)
    wp = start_worker(env, count=1)
    try:
        job = wait_for_state(env, "s02", "completed", timeout=10)
        assert job["state"] == "completed"
    finally:
        wp.send_signal(signal.SIGTERM)
        wp.wait(timeout=5)


# S-03 — Valid transition: processing -> failed [P0][High]
def test_s03_valid_transition_processing_to_failed(env):
    run(["config", "set", "backoff-base", "2"], env)
    run(["enqueue", '{"id":"s03","command":"exit 1","max_retries":3}'], env)
    wp = start_worker(env, count=1)
    try:
        job = wait_for_state(env, "s03", "failed", timeout=10)
        assert job["state"] == "failed"
        assert job["attempts"] == 1
    finally:
        wp.send_signal(signal.SIGTERM)
        wp.wait(timeout=5)


# S-04 — Valid transition: failed -> pending (after backoff) [P0][High]
def test_s04_valid_transition_failed_to_pending(env):
    run(["config", "set", "backoff-base", "2"], env)
    run(["enqueue", '{"id":"s04","command":"exit 1","max_retries":5}'], env)

    wp = start_worker(env, count=1)
    time.sleep(1.0)
    wp.send_signal(signal.SIGTERM)
    wp.wait(timeout=5)

    jobs = list_jobs(env)
    job = next(j for j in jobs if j["id"] == "s04")
    assert job["state"] == "failed"

    # Wait for backoff period (2^1 = 2 seconds) to elapse
    time.sleep(2.5)
    jobs_after = list_jobs(env)  # list triggers promote_ready_retries
    job_promoted = next(j for j in jobs_after if j["id"] == "s04")
    assert job_promoted["state"] == "pending"


# S-05 — Valid transition: dead -> pending only via dlq retry [P0][High]
def test_s05_valid_transition_dead_to_pending_only_via_dlq_retry(env):
    run(["config", "set", "backoff-base", "1"], env)
    run(["enqueue", '{"id":"s05","command":"exit 1","max_retries":1}'], env)

    wp = start_worker(env, count=1)
    try:
        wait_for_state(env, "s05", "dead", timeout=10)
        # Keep worker running for another 2 seconds, dead job stays dead
        time.sleep(2)
        job_dead = next(j for j in list_jobs(env) if j["id"] == "s05")
        assert job_dead["state"] == "dead"
    finally:
        wp.send_signal(signal.SIGTERM)
        wp.wait(timeout=5)

    # Only dlq retry moves it back to pending
    r = run(["dlq", "retry", "s05"], env)
    assert r.returncode == 0
    job_retried = next(j for j in list_jobs(env) if j["id"] == "s05")
    assert job_retried["state"] == "pending"


# S-06 — Invalid transition: completed -> processing [P0][High]
def test_s06_invalid_transition_completed_to_processing(env):
    run(["enqueue", '{"id":"s06","command":"echo done"}'], env)

    wp1 = start_worker(env, count=1)
    try:
        wait_for_state(env, "s06", "completed", timeout=10)
    finally:
        wp1.send_signal(signal.SIGTERM)
        wp1.wait(timeout=5)

    # Start a second worker run — should never re-claim completed job
    wp2 = start_worker(env, count=1)
    try:
        time.sleep(2)
        job = next(j for j in list_jobs(env) if j["id"] == "s06")
        assert job["state"] == "completed"
    finally:
        wp2.send_signal(signal.SIGTERM)
        wp2.wait(timeout=5)


# S-07 — Invalid transition: dead -> completed (without dlq retry) [P0][High]
def test_s07_invalid_transition_dead_to_completed(env):
    run(["config", "set", "backoff-base", "1"], env)
    run(["enqueue", '{"id":"s07","command":"exit 1","max_retries":1}'], env)

    wp = start_worker(env, count=1)
    try:
        wait_for_state(env, "s07", "dead", timeout=10)
    finally:
        wp.send_signal(signal.SIGTERM)
        wp.wait(timeout=5)

    conn = sqlite3.connect(env["QUEUECTL_DB"])
    row = conn.execute("SELECT state FROM jobs WHERE id='s07'").fetchone()
    conn.close()
    assert row[0] == "dead"


# S-08 — Duplicate/repeated transition: retry the same transition twice [P0][Critical]
def test_s08_duplicate_transition_exactly_once(env, tmp_path):
    log1 = tmp_path / "w1.log"
    log2 = tmp_path / "w2.log"
    run(["enqueue", '{"id":"s08","command":"sleep 2"}'], env)

    w1 = start_worker(env, count=1, logfile=log1)
    w2 = start_worker(env, count=1, logfile=log2)
    try:
        wait_for_state(env, "s08", "completed", timeout=10)
    finally:
        for w in (w1, w2):
            w.send_signal(signal.SIGTERM)
            w.wait(timeout=5)

    text1 = log1.read_text() if log1.exists() else ""
    text2 = log2.read_text() if log2.exists() else ""
    c1 = text1.count("running job s08")
    c2 = text2.count("running job s08")
    assert c1 + c2 == 1, f"job s08 was claimed {c1 + c2} times instead of exactly once"
