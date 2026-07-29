"""
Section B: CLI Contract Test Cases (C-01 to C-08)
Testing CLI behavior, process execution mode, signal handling,
cross-terminal worker management, JSON output purity, and edge case inputs.
"""

import json
import os
import signal
import sys
import time
import psutil
from conftest import run, list_jobs, start_worker, wait_for_state


# C-01 — Foreground execution [P0][Critical]
def test_c01_foreground_execution(env):
    wp = start_worker(env, count=3)
    try:
        time.sleep(1.0)
        parent = psutil.Process(wp.pid)
        children = parent.children(recursive=True)
        # Should have 3 child worker processes
        assert len(children) == 3
        assert wp.poll() is None  # Process blocks, does not daemonize or exit
    finally:
        wp.send_signal(signal.SIGTERM)
        wp.wait(timeout=5)


# C-02 — SIGINT graceful shutdown [P0][Critical]
def test_c02_sigint_graceful_shutdown(env, tmp_path):
    log = tmp_path / "c02.log"
    marker = tmp_path / "c02_done"
    run(["enqueue", json.dumps({"id": "c02", "command": f"sleep 2 && touch {marker}"})], env)

    wp = start_worker(env, count=1, logfile=log)
    time.sleep(0.8)  # Let it start executing job
    wp.send_signal(signal.SIGINT)
    wp.wait(timeout=10)

    assert marker.exists(), "in-flight job should finish before worker exits"
    log_text = log.read_text()
    assert "job c02 exited 0" in log_text
    assert "stopped" in log_text


# C-03 — SIGTERM graceful shutdown [P0][Critical]
def test_c03_sigterm_graceful_shutdown(env, tmp_path):
    log = tmp_path / "c03.log"
    marker = tmp_path / "c03_done"
    run(["enqueue", json.dumps({"id": "c03", "command": f"sleep 2 && touch {marker}"})], env)

    wp = start_worker(env, count=1, logfile=log)
    time.sleep(0.8)  # Let it start executing job
    wp.send_signal(signal.SIGTERM)
    wp.wait(timeout=10)

    assert marker.exists(), "in-flight job should finish before worker exits"
    log_text = log.read_text()
    assert "job c03 exited 0" in log_text
    assert "stopped" in log_text


# C-04 — Cross-terminal worker stop [P0][Critical]
def test_c04_cross_terminal_worker_stop(env, tmp_path):
    marker = tmp_path / "c04_done"
    run(["enqueue", json.dumps({"id": "c04", "command": f"sleep 2 && touch {marker}"})], env)

    wp = start_worker(env, count=2)
    time.sleep(0.8)

    # Issue worker stop from another process invocation
    stop_res = run(["worker", "stop"], env)
    assert stop_res.returncode == 0
    assert "Sent SIGTERM to worker" in stop_res.stdout

    wp.wait(timeout=10)
    assert marker.exists()

    # Second worker stop should report no running workers
    stop_again = run(["worker", "stop"], env)
    assert "No running workers found." in stop_again.stdout


# C-05 — --json output purity [P0][High]
def test_c05_json_output_purity(env):
    run(["enqueue", '{"id":"c05","command":"echo hi"}'], env)
    res = run(["list", "--state", "pending", "--json"], env)
    assert res.returncode == 0

    stdout_lines = res.stdout.strip().splitlines()
    assert len(stdout_lines) == 1, f"expected single line output, got {len(stdout_lines)}"

    parsed = json.loads(stdout_lines[0])
    assert isinstance(parsed, list)
    assert len(parsed) == 1
    assert parsed[0]["id"] == "c05"


# C-06 — Invalid arguments produce clean errors [P0][High]
def test_c06_invalid_arguments_produce_clean_errors(env):
    # Case 1: Invalid JSON
    r1 = run(["enqueue", "not valid json"], env)
    assert r1.returncode != 0
    assert "Invalid JSON" in r1.stderr
    assert "Traceback" not in r1.stderr

    # Case 2: Missing id
    r2 = run(["enqueue", '{"command":"echo hi"}'], env)
    assert r2.returncode != 0
    assert "Job JSON must include at least 'id' and 'command'" in r2.stderr
    assert "Traceback" not in r2.stderr

    # Case 3: Invalid state option
    r3 = run(["list", "--state", "bogus-state", "--json"], env)
    assert r3.returncode != 0
    assert "Invalid state 'bogus-state'" in r3.stderr
    assert "Traceback" not in r3.stderr

    # Case 4: Not a real command
    r4 = run(["notarealcommand"], env)
    assert r4.returncode != 0
    assert "Traceback" not in r4.stderr


# C-07 — Special characters in command [P1][Medium]
def test_c07_special_characters_in_command(env, tmp_path):
    cmd_str = 'echo "quoted \"value\""'
    r = run(["enqueue", json.dumps({"id": "c07", "command": cmd_str})], env)
    assert r.returncode == 0

    jobs = list_jobs(env)
    assert len(jobs) == 1
    assert jobs[0]["command"] == cmd_str

    wp = start_worker(env, count=1)
    try:
        wait_for_state(env, "c07", "completed", timeout=10)
    finally:
        wp.send_signal(signal.SIGTERM)
        wp.wait(timeout=5)


# C-08 — Large command payload [P1][Medium]
def test_c08_large_command_payload(env):
    long_cmd = "echo " + "x" * 20000
    r = run(["enqueue", json.dumps({"id": "c08", "command": long_cmd})], env)
    assert r.returncode == 0

    jobs = list_jobs(env)
    assert len(jobs) == 1
    assert len(jobs[0]["command"]) == len(long_cmd)
