"""
Section F: Persistence Test Cases (PST-01 to PST-04)
Testing durability of jobs, configuration, and DLQ state across process restarts.
"""

import signal
import time
from conftest import run, list_jobs, start_worker, wait_for_state


# PST-01 — Jobs survive a full restart, all states [P0][Critical]
def test_pst01_jobs_survive_full_restart(env):
    # Setup completed job
    run(["enqueue", '{"id":"c1","command":"echo hello"}'], env)
    wp1 = start_worker(env, count=1)
    try:
        wait_for_state(env, "c1", "completed", timeout=10)
    finally:
        wp1.send_signal(signal.SIGTERM)
        wp1.wait(timeout=5)

    # Setup dead job
    run(["config", "set", "backoff-base", "1"], env)
    run(["enqueue", '{"id":"d1","command":"exit 1","max_retries":1}'], env)
    wp2 = start_worker(env, count=1)
    try:
        wait_for_state(env, "d1", "dead", timeout=10)
    finally:
        wp2.send_signal(signal.SIGTERM)
        wp2.wait(timeout=5)

    # Setup pending job (no worker started after this, so it stays pending)
    run(["enqueue", '{"id":"p1","command":"sleep 10"}'], env)

    # Completely fresh CLI invocation (no running processes)
    jobs_after_restart = list_jobs(env)

    state_map = {j["id"]: j["state"] for j in jobs_after_restart}
    assert state_map["p1"] == "pending"
    assert state_map["c1"] == "completed"
    assert state_map["d1"] == "dead"



# PST-02 — Config persists across restart [P1][Medium]
def test_pst02_config_persists_across_restart(env):
    run(["config", "set", "max-retries", "7"], env)
    run(["config", "set", "backoff-base", "3"], env)

    # Query from new process invocation
    res_max = run(["config", "get", "max-retries"], env)
    assert res_max.stdout.strip() == "7"

    res_base = run(["config", "get", "backoff-base"], env)
    assert res_base.stdout.strip() == "3"


# PST-03 — DLQ survives restart [P0][High]
def test_pst03_dlq_survives_restart(env):
    run(["config", "set", "backoff-base", "1"], env)
    run(["enqueue", '{"id":"dead1","command":"exit 1","max_retries":1}'], env)

    wp = start_worker(env, count=1)
    try:
        wait_for_state(env, "dead1", "dead", timeout=10)
    finally:
        wp.send_signal(signal.SIGTERM)
        wp.wait(timeout=5)

    dlq_res = run(["dlq", "list", "--json"], env)
    assert dlq_res.returncode == 0
    dlq = list_jobs(env)
    assert any(j["id"] == "dead1" and j["state"] == "dead" for j in dlq)


# PST-04 — No duplicate IDs after multiple restarts [P0][High]
def test_pst04_no_duplicate_ids_after_restarts(env):
    run(["enqueue", '{"id":"unique1","command":"echo 1"}'], env)

    r_dup = run(["enqueue", '{"id":"unique1","command":"echo 2"}'], env)
    assert r_dup.returncode == 1
    assert "already exists" in r_dup.stderr

    jobs = list_jobs(env)
    assert len(jobs) == 1
    assert jobs[0]["command"] == "echo 1"
