"""
Section G: Retry & DLQ Boundary Test Cases (R-01 to R-04)
Testing retry boundaries (max_retries 0, 1), custom backoff base, and idempotent DLQ retries.
"""

import signal
import time
from conftest import run, list_jobs, start_worker, wait_for_state


# R-01 — max_retries = 0 [P1][Medium]
def test_r01_max_retries_zero(env):
    run(["enqueue", '{"id":"r01","command":"exit 1","max_retries":0}'], env)

    wp = start_worker(env, count=1)
    try:
        job = wait_for_state(env, "r01", "dead", timeout=10)
        assert job["attempts"] == 1
        assert job["last_error"] == "command exited with code 1"
    finally:
        wp.send_signal(signal.SIGTERM)
        wp.wait(timeout=5)


# R-02 — max_retries = 1 [P1][Medium]
def test_r02_max_retries_one(env):
    run(["config", "set", "backoff-base", "1"], env)
    run(["enqueue", '{"id":"r02","command":"exit 1","max_retries":1}'], env)

    wp = start_worker(env, count=1)
    try:
        job = wait_for_state(env, "r02", "dead", timeout=10)
        assert job["attempts"] == 1
    finally:
        wp.send_signal(signal.SIGTERM)
        wp.wait(timeout=5)


# R-03 — backoff-base = 1 [P1][Medium]
def test_r03_backoff_base_one(env):
    run(["config", "set", "backoff-base", "1"], env)
    run(["enqueue", '{"id":"r03","command":"exit 1","max_retries":3}'], env)

    wp = start_worker(env, count=1)
    try:
        job_failed = wait_for_state(env, "r03", "failed", timeout=10)
        assert job_failed["attempts"] == 1
        assert job_failed["backoff_base"] == 1.0
    finally:
        wp.send_signal(signal.SIGTERM)
        wp.wait(timeout=5)


# R-04 — Repeated dlq retry does not create duplicates [P0][High]
def test_r04_repeated_dlq_retry(env):
    run(["config", "set", "backoff-base", "1"], env)
    run(["enqueue", '{"id":"r04","command":"exit 1","max_retries":1}'], env)

    wp = start_worker(env, count=1)
    try:
        wait_for_state(env, "r04", "dead", timeout=10)
    finally:
        wp.send_signal(signal.SIGTERM)
        wp.wait(timeout=5)

    # First DLQ retry succeeds
    r1 = run(["dlq", "retry", "r04"], env)
    assert r1.returncode == 0
    assert "Re-enqueued job 'r04'" in r1.stdout

    # Second DLQ retry immediately again fails cleanly
    r2 = run(["dlq", "retry", "r04"], env)
    assert r2.returncode == 1
    assert "No dead job with id 'r04' found." in r2.stderr

    jobs = list_jobs(env)
    assert len(jobs) == 1
    assert jobs[0]["id"] == "r04"
    assert jobs[0]["state"] == "pending"
