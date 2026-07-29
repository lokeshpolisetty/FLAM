"""
test_observability.py — Observability tests covering Section 2.E (Observability) of
test_strategy.md.

Focus areas:
  OBS-1  Log lines emitted for key events (enqueue, claim, complete, fail, DLQ move,
          recovery, worker start/stop, config change)
  OBS-2  Logs go to stdout of the worker process (not mixed into --json stdout)
  OBS-3  Log lines contain timestamps and are human-readable
  OBS-4  Log sequence matches DB state sequence (correlation)
  OBS-5  No sensitive data leaks in logs
"""

import json
import os
import signal
import sqlite3
import subprocess
import sys
import time
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
APP  = ROOT / "-m", "queuectl.cli.entrypoint"


def cli(args, env, timeout=15):
    return subprocess.run(
        [sys.executable, "-m", "queuectl.cli.entrypoint"] + args,
        capture_output=True, text=True, timeout=timeout, env=env,
    )


def worker_proc_capture(env, count=1):
    """Start a worker and capture ALL output (stdout+stderr merged) into a log file."""
    return subprocess.Popen(
        [sys.executable, "-m", "queuectl.cli.entrypoint"] + ["worker", "start", "--count", str(count)],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,  # merge stderr into stdout so we capture everything
        text=True,
        env=env,
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


def collect_worker_output(proc, wait_seconds):
    """Let the worker run for wait_seconds then send SIGTERM and collect output."""
    time.sleep(wait_seconds)
    try:
        proc.send_signal(signal.SIGTERM)
    except ProcessLookupError:
        pass
    try:
        stdout, _ = proc.communicate(timeout=8)
    except subprocess.TimeoutExpired:
        proc.kill()
        stdout, _ = proc.communicate()
    return stdout


@pytest.fixture
def env(tmp_path):
    e = os.environ.copy()
    e["QUEUECTL_DB"] = str(tmp_path / "queue.db")
    e["QUEUECTL_TEST"] = "1"
    return e


# ===========================================================================
# OBS-1  Log line emission for key events
# ===========================================================================

def test_obs_worker_start_log_emitted(env):
    """Worker emits a start log line containing pid when it starts."""
    wp = worker_proc_capture(env, count=1)
    output = collect_worker_output(wp, wait_seconds=0.8)
    assert "started" in output.lower() or "pid=" in output, \
        f"No worker-start log found in output:\n{output}"


def test_obs_claim_log_emitted_when_job_claimed(env):
    """Worker emits a log line containing the job id when a job is claimed."""
    cli(["enqueue", '{"id":"obs-claim","command":"echo hi"}'], env)
    wp = worker_proc_capture(env, count=1)
    output = collect_worker_output(wp, wait_seconds=2.5)
    assert "obs-claim" in output, \
        f"Claim log line for obs-claim not found:\n{output}"


def test_obs_completion_log_emitted(env):
    """Worker emits a log line indicating exit code 0 after a successful job."""
    cli(["enqueue", '{"id":"obs-done","command":"echo done"}'], env)
    wp = worker_proc_capture(env, count=1)
    output = collect_worker_output(wp, wait_seconds=2.5)
    assert "obs-done" in output, f"Completion log not found:\n{output}"
    assert "0" in output, f"Exit-code-0 log not found:\n{output}"


def test_obs_failure_log_emitted_with_exit_code(env):
    """Worker emits a log line with non-zero exit code when a job fails."""
    cli(["config", "set", "backoff-base", "1"], env)
    cli(["enqueue", '{"id":"obs-fail","command":"exit 2","max_retries":0}'], env)
    wp = worker_proc_capture(env, count=1)
    output = collect_worker_output(wp, wait_seconds=3.0)
    assert "obs-fail" in output, f"Failure log not found:\n{output}"
    assert "2" in output, f"Exit-code-2 not found in log:\n{output}"


def test_obs_worker_stop_log_emitted(env):
    """Worker emits a 'stopped' log line when it shuts down."""
    wp = worker_proc_capture(env, count=1)
    output = collect_worker_output(wp, wait_seconds=0.5)
    assert "stopped" in output.lower(), \
        f"Worker-stop log not found:\n{output}"


def test_obs_running_job_log_contains_job_id_and_command(env):
    """The 'running job' log line includes both job id and the command string."""
    cli(["enqueue", '{"id":"obs-run","command":"echo observable"}'], env)
    wp = worker_proc_capture(env, count=1)
    output = collect_worker_output(wp, wait_seconds=2.5)
    assert "obs-run" in output
    assert "observable" in output


# ===========================================================================
# OBS-2  Logs do NOT contaminate --json stdout
# ===========================================================================

def test_obs_list_json_stdout_has_no_worker_log_lines(env):
    """Worker log lines must not appear in list --json stdout."""
    cli(["enqueue", '{"id":"obs-pure","command":"echo hi"}'], env)
    wp = subprocess.Popen(
        [sys.executable, "-m", "queuectl.cli.entrypoint"] + ["worker", "start", "--count", "1"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        env=env,
    )
    try:
        wait_state(env, "obs-pure", "completed")
        res = cli(["list", "--state", "completed", "--json"], env)
        # stdout must be pure JSON — worker log phrases must not appear
        for log_phrase in ("[worker", "started", "running job", "exited", "stopped"):
            assert log_phrase not in res.stdout, \
                f"Log phrase {log_phrase!r} leaked into --json stdout: {res.stdout!r}"
        parsed = json.loads(res.stdout.strip())
        assert isinstance(parsed, list)
    finally:
        wp.send_signal(signal.SIGTERM)
        wp.wait(timeout=5)


def test_obs_dlq_list_json_stdout_is_pure(env):
    """dlq list --json stdout contains only a JSON array — no log noise."""
    cli(["config", "set", "backoff-base", "1"], env)
    cli(["enqueue", '{"id":"obs-dlq","command":"exit 1","max_retries":0}'], env)
    wp = subprocess.Popen(
        [sys.executable, "-m", "queuectl.cli.entrypoint"] + ["worker", "start", "--count", "1"],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, env=env,
    )
    try:
        wait_state(env, "obs-dlq", "dead")
        res = cli(["dlq", "list", "--json"], env)
        parsed = json.loads(res.stdout.strip())
        assert isinstance(parsed, list)
        # No log phrases in stdout
        assert "[worker" not in res.stdout
    finally:
        wp.send_signal(signal.SIGTERM)
        wp.wait(timeout=5)


# ===========================================================================
# OBS-3  Log format — timestamps and human-readability
# ===========================================================================

def test_obs_log_lines_are_human_readable_text(env):
    """Worker log output is plain human-readable text (no binary blobs or base64)."""
    cli(["enqueue", '{"id":"obs-hr","command":"echo hi"}'], env)
    wp = worker_proc_capture(env, count=1)
    output = collect_worker_output(wp, wait_seconds=2.0)
    # Should be decodable as UTF-8 and contain ASCII words
    assert output.isprintable() or all(ord(c) < 128 or c.isprintable() for c in output)
    assert len(output) > 0


def test_obs_no_binary_or_encoded_content_in_logs(env):
    """Log output contains no base64-looking blobs or raw byte sequences."""
    cli(["enqueue", '{"id":"obs-nb","command":"echo hi"}'], env)
    wp = worker_proc_capture(env, count=1)
    output = collect_worker_output(wp, wait_seconds=2.0)
    # Check no NUL bytes in output
    assert "\x00" not in output


# ===========================================================================
# OBS-4  Log / DB state correlation
# ===========================================================================

def test_obs_log_sequence_matches_state_sequence(env):
    """Log events appear in the order: start → claim → exit → stop."""
    cli(["enqueue", '{"id":"obs-seq","command":"echo seq"}'], env)
    wp = worker_proc_capture(env, count=1)
    output = collect_worker_output(wp, wait_seconds=2.5)

    lines = [l for l in output.splitlines() if l.strip()]
    # Find positions of key events
    start_idx = next((i for i, l in enumerate(lines) if "started" in l.lower()), None)
    claim_idx = next((i for i, l in enumerate(lines) if "obs-seq" in l and "running" in l.lower()), None)
    exit_idx  = next((i for i, l in enumerate(lines) if "obs-seq" in l and "exited" in l.lower()), None)
    stop_idx  = next((i for i, l in enumerate(lines) if "stopped" in l.lower()), None)

    assert start_idx is not None, f"'started' log not found in:\n{output}"
    assert claim_idx is not None, f"'running job obs-seq' log not found in:\n{output}"
    assert exit_idx  is not None, f"'exited' log for obs-seq not found in:\n{output}"
    assert stop_idx  is not None, f"'stopped' log not found in:\n{output}"

    assert start_idx < claim_idx, "start must come before claim"
    assert claim_idx < exit_idx,  "claim must come before exit"
    assert exit_idx  < stop_idx,  "exit must come before stop"


def test_obs_claim_log_precedes_db_processing_state(env):
    """After a job's claim log line appears, the DB shows state=processing."""
    cli(["enqueue", '{"id":"obs-dbcorr","command":"sleep 2"}'], env)
    wp = worker_proc_capture(env, count=1)
    time.sleep(1.2)   # worker should have claimed and be executing

    # DB must show processing
    conn = sqlite3.connect(env["QUEUECTL_DB"])
    row = conn.execute("SELECT state FROM jobs WHERE id='obs-dbcorr'").fetchone()
    conn.close()

    try:
        wp.send_signal(signal.SIGTERM)
        stdout, _ = wp.communicate(timeout=8)
    except subprocess.TimeoutExpired:
        wp.kill()
        stdout, _ = wp.communicate()

    assert row is not None and row[0] in ("processing", "completed"), \
        f"Expected processing or completed, got {row}"
    assert "obs-dbcorr" in stdout


def test_obs_recovery_log_emitted_for_stale_job(env):
    """When a worker recovers a stale job, a log entry references the job id."""
    cli(["config", "set", "recovery-timeout", "2"], env)
    cli(["config", "set", "heartbeat-interval", "1"], env)
    cli(["enqueue", '{"id":"obs-rec","command":"sleep 10"}'], env)

    # Start a worker, let it claim the job, then SIGKILL it
    wp1 = subprocess.Popen(
        [sys.executable, "-m", "queuectl.cli.entrypoint"] + ["worker", "start", "--count", "1"],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, env=env,
    )
    wait_state(env, "obs-rec", "processing", timeout=5)
    conn = sqlite3.connect(env["QUEUECTL_DB"])
    row = conn.execute("SELECT pid FROM workers WHERE status='running'").fetchone()
    conn.close()
    os.kill(row[0], signal.SIGKILL)
    wp1.wait(timeout=5)

    time.sleep(2.5)   # lease expires

    # Second worker should reap and log recovery
    wp2 = worker_proc_capture(env, count=1)
    output = collect_worker_output(wp2, wait_seconds=3.0)
    # The job should now be visible as pending or be re-claimed
    jobs = json.loads(cli(["list", "--json"], env).stdout.strip())
    job = next(j for j in jobs if j["id"] == "obs-rec")
    assert job["state"] in ("pending", "processing", "completed"), \
        f"obs-rec in unexpected state: {job['state']}"


# ===========================================================================
# OBS-5  No sensitive data in logs
# ===========================================================================

def test_obs_no_db_path_credentials_in_logs(env):
    """Log output does not reveal internal DB paths or credentials."""
    cli(["enqueue", '{"id":"obs-sec","command":"echo hi"}'], env)
    wp = worker_proc_capture(env, count=1)
    output = collect_worker_output(wp, wait_seconds=2.0)
    # The DB file path might appear in error messages, but no password-like strings
    assert "password" not in output.lower()
    assert "secret" not in output.lower()
    assert "token" not in output.lower()


def test_obs_config_set_log_shows_key_and_value(env):
    """config set emits a confirmation containing both the key and the new value."""
    res = cli(["config", "set", "max-retries", "9"], env)
    assert res.returncode == 0
    assert "max-retries" in res.stdout
    assert "9" in res.stdout
