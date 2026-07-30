"""
Section A: Functional Test Cases (F-01 to F-10)
Testing core queue operations, duplicate handling, full success lifecycle,
retry mechanisms, DLQ transitions, config scoping, and status accuracy.
"""

import json
import signal
import time
from conftest import run, list_jobs, start_worker, wait_for_state


# F-01 — Valid enqueue [P0][High]
def test_f01_valid_enqueue(env):
    r = run(["enqueue", '{"id":"f01","command":"echo hi"}'], env)
    assert r.returncode == 0

    jobs = list_jobs(env)
    assert len(jobs) == 1
    job = jobs[0]
    assert job["id"] == "f01"
    assert job["command"] == "echo hi"
    assert job["state"] == "pending"
    assert job["attempts"] == 0
    assert job["max_retries"] == 3
    assert job["backoff_base"] == 2.0
    assert job["created_at"] is not None
    assert job["updated_at"] is not None


# F-02 — Duplicate job ID rejected [P0][High]
def test_f02_duplicate_job_id_rejected(env):
    r1 = run(["enqueue", '{"id":"f01","command":"echo hi"}'], env)
    assert r1.returncode == 0

    r2 = run(["enqueue", '{"id":"f01","command":"echo again"}'], env)
    assert r2.returncode == 1
    assert "Job with id 'f01' already exists" in r2.stderr

    jobs = list_jobs(env)
    assert len(jobs) == 1
    assert jobs[0]["id"] == "f01"
    assert jobs[0]["command"] == "echo hi"


# F-03 — Full success lifecycle: pending -> processing -> completed [P0][High]
def test_f03_full_success_lifecycle(env, tmp_path):
    log = tmp_path / "worker_f03.log"
    r = run(["enqueue", '{"id":"f03","command":"echo hello"}'], env)
    assert r.returncode == 0

    wp = start_worker(env, count=1, logfile=log)
    try:
        job = wait_for_state(env, "f03", "completed", timeout=10)
    finally:
        wp.send_signal(signal.SIGTERM)
        wp.wait(timeout=5)

    log_text = log.read_text()
    assert "running job f03" in log_text
    assert "job f03 exited 0" in log_text

    assert job["state"] == "completed"
    assert job["worker_id"] is None


# F-04 — Failing command becomes a retryable failure [P0][High]
def test_f04_failing_command_retryable_failure(env):
    run(["config", "set", "backoff-base", "1"], env)
    run(["enqueue", '{"id":"f04","command":"exit 1","max_retries":3}'], env)

    wp = start_worker(env, count=1)
    try:
        job = wait_for_state(env, "f04", "failed", timeout=10)
        assert job["attempts"] == 1
        assert job["next_retry_at"] is not None
        assert job["last_error"] == "command exited with code 1"
    finally:
        wp.send_signal(signal.SIGTERM)
        wp.wait(timeout=5)

    # Wait past backoff-base (1s), then query list to trigger promote_ready_retries
    time.sleep(1.5)
    jobs = list_jobs(env)
    job_after = next(j for j in jobs if j["id"] == "f04")
    assert job_after["state"] == "pending"


# F-05 — Retries exhausted -> dead (DLQ) [P0][High]
def test_f05_retries_exhausted_dead(env):
    run(["config", "set", "backoff-base", "1"], env)
    run(["enqueue", '{"id":"f05","command":"exit 1","max_retries":2}'], env)

    wp = start_worker(env, count=1)
    try:
        job = wait_for_state(env, "f05", "dead", timeout=15)
        assert job["attempts"] == 2
        assert job["last_error"] == "command exited with code 1"
        time.sleep(2)
        # Verify it stays dead and attempts remain 2
        job_still = next(j for j in list_jobs(env) if j["id"] == "f05")
        assert job_still["state"] == "dead"
        assert job_still["attempts"] == 2
    finally:
        wp.send_signal(signal.SIGTERM)
        wp.wait(timeout=5)


# F-06 — dlq list shows dead jobs [P0][High]
def test_f06_dlq_list_shows_dead_jobs(env):
    run(["config", "set", "backoff-base", "1"], env)
    run(["enqueue", '{"id":"f05","command":"exit 1","max_retries":1}'], env)

    wp = start_worker(env, count=1)
    try:
        wait_for_state(env, "f05", "dead", timeout=10)
    finally:
        wp.send_signal(signal.SIGTERM)
        wp.wait(timeout=5)

    r = run(["dlq", "list", "--json"], env)
    assert r.returncode == 0
    dlq_jobs = json.loads(r.stdout)
    assert any(j["id"] == "f05" and j["attempts"] == 1 and j["last_error"] is not None for j in dlq_jobs)


# F-07 — dlq retry re-enqueues with attempts reset [P0][High]
def test_f07_dlq_retry_reenqueues(env):
    run(["config", "set", "backoff-base", "1"], env)
    run(["enqueue", '{"id":"f05","command":"exit 1","max_retries":1}'], env)

    wp = start_worker(env, count=1)
    try:
        wait_for_state(env, "f05", "dead", timeout=10)
    finally:
        wp.send_signal(signal.SIGTERM)
        wp.wait(timeout=5)

    r = run(["dlq", "retry", "f05"], env)
    assert r.returncode == 0
    assert "Re-enqueued job 'f05' (attempts reset to 0)." in r.stdout

    jobs = list_jobs(env)
    job = next(j for j in jobs if j["id"] == "f05")
    assert job["state"] == "pending"
    assert job["attempts"] == 0
    assert job["next_retry_at"] is None
    assert job["last_error"] is None


# F-08 — Config change scope: max-retries [P1][Medium]
def test_f08_config_scope_max_retries(env):
    run(["enqueue", '{"id":"old","command":"exit 1"}'], env)
    run(["config", "set", "max-retries", "1"], env)
    run(["enqueue", '{"id":"new","command":"exit 1"}'], env)

    jobs = {j["id"]: j for j in list_jobs(env)}
    assert jobs["old"]["max_retries"] == 3
    assert jobs["new"]["max_retries"] == 1


# F-09 — Config change scope: backoff-base [P1][Medium]
def test_f09_config_scope_backoff_base(env):
    run(["enqueue", '{"id":"job-a","command":"exit 1"}'], env)
    run(["config", "set", "backoff-base", "5"], env)
    run(["enqueue", '{"id":"job-b","command":"exit 1"}'], env)

    jobs = {j["id"]: j for j in list_jobs(env)}
    assert jobs["job-a"]["backoff_base"] == 2.0
    assert jobs["job-b"]["backoff_base"] == 5.0


# F-10 — status accuracy [P0][High]
def test_f10_status_accuracy(env):
    # Enqueue and complete c1
    run(["enqueue", '{"id":"c1","command":"echo hi"}'], env)
    wp1 = start_worker(env, count=1)
    try:
        wait_for_state(env, "c1", "completed", timeout=10)
    finally:
        wp1.send_signal(signal.SIGTERM)
        wp1.wait(timeout=5)

    # Enqueue a pending job with short sleep
    run(["enqueue", '{"id":"p1","command":"sleep 1"}'], env)

    # Start worker to claim p1
    wp2 = start_worker(env, count=1)
    try:
        wait_for_state(env, "p1", "processing", timeout=10)
        status_out = run(["status"], env).stdout

        assert "completed" in status_out
        assert "Running workers: 1" in status_out
    finally:
        wp2.send_signal(signal.SIGTERM)
        wp2.wait(timeout=5)

