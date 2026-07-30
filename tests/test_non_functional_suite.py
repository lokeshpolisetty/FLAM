"""
Section H: Non-Functional Test Cases (SEC-01, SEC-02, PERF-01, CHAOS-01)
Testing security boundaries, throughput sanity under load, and worker restart chaos handling.
"""

import json
import signal
import time
from conftest import run, list_jobs, start_worker, wait_for_state


# SEC-01 — Shell metacharacters don't escape the job's own command [P1][Medium]
def test_sec01_shell_metacharacters(env, tmp_path):
    marker1 = tmp_path / "sec01_1"
    marker2 = tmp_path / "sec01_2"
    cmd = f"touch {marker1}; touch {marker2}"

    run(["enqueue", json.dumps({"id": "sec01", "command": cmd})], env)
    wp = start_worker(env, count=1)
    try:
        wait_for_state(env, "sec01", "completed", timeout=10)
        assert marker1.exists()
        assert marker2.exists()
    finally:
        wp.send_signal(signal.SIGTERM)
        wp.wait(timeout=5)


# SEC-02 — Malformed JSON never reaches the shell [P0][High]
def test_sec02_malformed_json_never_reaches_shell(env):
    r = run(["enqueue", "malformed json content"], env)
    assert r.returncode == 1
    assert "Invalid JSON" in r.stderr

    jobs = list_jobs(env)
    assert len(jobs) == 0


# PERF-01 — Claim throughput sanity check [P1][Medium]
def test_perf01_claim_throughput_sanity_check(env):
    num_jobs = 100
    for i in range(num_jobs):
        r = run(["enqueue", json.dumps({"id": f"p-{i}", "command": f"echo {i}"})], env)
        assert r.returncode == 0

    wp = start_worker(env, count=8)
    try:
        deadline = time.time() + 20
        while time.time() < deadline:
            jobs = list_jobs(env)
            if all(j["state"] == "completed" for j in jobs) and len(jobs) == num_jobs:
                break
            time.sleep(0.2)

        jobs = list_jobs(env)
        completed_count = sum(1 for j in jobs if j["state"] == "completed")
        assert completed_count == num_jobs, f"expected {num_jobs} completed, got {completed_count}"
    finally:
        wp.send_signal(signal.SIGTERM)
        wp.wait(timeout=5)


# CHAOS-01 — Repeated worker restarts under load [P1][Medium]
def test_chaos01_repeated_worker_restarts_under_load(env):
    run(["config", "set", "backoff-base", "1"], env)
    num_jobs = 20
    for i in range(num_jobs):
        cmd = "echo hi" if i % 2 == 0 else "exit 1"
        run(["enqueue", json.dumps({"id": f"chaos-{i}", "command": cmd, "max_retries": 2})], env)

    # Start and stop worker pool repeatedly
    for _ in range(3):
        wp = start_worker(env, count=3)
        time.sleep(1.0)
        wp.send_signal(signal.SIGTERM)
        wp.wait(timeout=5)

    # Final run to drain the queue completely
    wp_final = start_worker(env, count=3)
    try:
        deadline = time.time() + 20
        while time.time() < deadline:
            jobs = list_jobs(env)
            if all(j["state"] in ("completed", "dead") for j in jobs):
                break
            time.sleep(0.5)

        jobs = list_jobs(env)
        processing_count = sum(1 for j in jobs if j["state"] == "processing")
        pending_count = sum(1 for j in jobs if j["state"] in ("pending", "failed"))
        assert processing_count == 0, "no jobs should remain stuck in processing"
        assert pending_count == 0, "all jobs should reach terminal states (completed or dead)"
    finally:
        wp_final.send_signal(signal.SIGTERM)
        wp_final.wait(timeout=5)
