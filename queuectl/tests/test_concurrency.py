"""
Section D: Concurrency Test Cases (CC-01 to CC-04)
Testing atomic job claims under worker contention, parallel enqueue operations,
worker stopping mid-claim, and starvation avoidance.
"""

import json
import signal
import subprocess
import sys
import time
from pathlib import Path
import pytest
from conftest import run, list_jobs, start_worker


# CC-01 — Exactly-once claim under contention, scaled worker counts [P0][Critical]

ROOT = Path(__file__).resolve().parent.parent
@pytest.mark.parametrize("worker_count", [2, 10, 50])
def test_cc01_exactly_once_claim_under_contention(env, tmp_path, worker_count):
    log = tmp_path / "cc_log.txt"
    num_jobs = 50

    for i in range(1, num_jobs + 1):
        r = run(["enqueue", json.dumps({"id": f"cc-{i}", "command": f"echo {i} >> {log}"})], env)
        assert r.returncode == 0

    wp = start_worker(env, count=worker_count)
    try:
        deadline = time.time() + 25
        while time.time() < deadline:
            jobs = list_jobs(env)
            if all(j["state"] == "completed" for j in jobs) and len(jobs) == num_jobs:
                break
            time.sleep(0.3)
    finally:
        wp.send_signal(signal.SIGTERM)
        wp.wait(timeout=10)

    lines = log.read_text().splitlines() if log.exists() else []
    assert len(lines) == num_jobs, f"expected {num_jobs} lines, got {len(lines)}"
    assert len(set(lines)) == num_jobs, f"duplicate job execution detected in {len(lines) - len(set(lines))} cases"

    status_out = run(["status"], env).stdout
    assert f"completed  {num_jobs}" in status_out or "completed  50" in status_out


# CC-02 — Parallel enqueue calls don't corrupt state [P1][Medium]
def test_cc02_parallel_enqueue(env):
    procs = []
    num_parallel = 20
    for i in range(num_parallel):
        p = subprocess.Popen(
            [sys.executable, "-m", "queuectl.cli.entrypoint"] + ["enqueue", json.dumps({"id": f"par-{i}", "command": f"echo {i}"})],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env, text=True
        )
        procs.append(p)

    for p in procs:
        out, err = p.communicate()
        assert p.returncode == 0, f"enqueue failed with code {p.returncode}: {err}"

    jobs = list_jobs(env)
    assert len(jobs) == num_parallel
    job_ids = {j["id"] for j in jobs}
    assert len(job_ids) == num_parallel


# CC-03 — worker stop while claim loop is active [P0][Critical]
def test_cc03_worker_stop_while_claim_active(env, tmp_path):
    for i in range(10):
        run(["enqueue", json.dumps({"id": f"stop-job-{i}", "command": "sleep 2"})], env)

    wp = start_worker(env, count=5)
    time.sleep(0.8)

    # Invoke worker stop
    r = run(["worker", "stop"], env)
    assert r.returncode == 0

    wp.wait(timeout=10)

    # Verify no job is left in processing state
    jobs = list_jobs(env)
    processing_jobs = [j for j in jobs if j["state"] == "processing"]
    assert len(processing_jobs) == 0, f"found jobs stuck in processing: {processing_jobs}"


# CC-04 — Long job + many short jobs: no starvation [P1][Medium]
def test_cc04_long_job_many_short_jobs_no_starvation(env, tmp_path):
    run(["enqueue", '{"id":"long","command":"sleep 8"}'], env)
    for i in range(10):
        run(["enqueue", json.dumps({"id": f"short-{i}", "command": f"echo {i}"})], env)

    wp = start_worker(env, count=3)
    try:
        # Wait for all short jobs to complete
        deadline = time.time() + 5
        while time.time() < deadline:
            jobs = list_jobs(env)
            short_completed = [j for j in jobs if j["id"].startswith("short-") and j["state"] == "completed"]
            if len(short_completed) == 10:
                break
            time.sleep(0.3)

        jobs = list_jobs(env)
        short_completed = [j for j in jobs if j["id"].startswith("short-") and j["state"] == "completed"]
        assert len(short_completed) == 10, "short jobs should complete quickly without starvation"
    finally:
        wp.send_signal(signal.SIGTERM)
        wp.wait(timeout=10)
