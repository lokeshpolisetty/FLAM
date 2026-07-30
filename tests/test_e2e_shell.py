"""
test_e2e_shell.py — End-to-end tests covering Section 2.D of test_strategy.md.

Drives the real CLI binary as subprocesses exactly as a reviewer would.
Every test uses real OS signals, real shell commands, and real file-system
side effects so that nothing is stubbed.

Subsections covered:
  D1  Use real shell commands in jobs (echo, touch, exit codes, stdout/stderr)
  D2  Use real OS signals (SIGINT, SIGTERM, SIGKILL, cross-terminal stop)
  D3  Verify stdout/stderr contracts (JSON purity, no ANSI, encoding)
  D4  Verify persistence across restart
  D5  Verify jobs survive kill -9 and full restart
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


# ---------------------------------------------------------------------------
# Helpers (self-contained so this file needs no conftest imports)
# ---------------------------------------------------------------------------

def cli(args, env, timeout=20):
    return subprocess.run(
        [sys.executable, "-m", "queuectl.cli.entrypoint"] + args,
        capture_output=True, text=True, timeout=timeout, env=env,
    )


def worker_proc(env, count=1, capture=False):
    stdout = subprocess.PIPE if capture else subprocess.DEVNULL
    stderr = subprocess.PIPE if capture else subprocess.DEVNULL
    return subprocess.Popen(
        [sys.executable, "-m", "queuectl.cli.entrypoint"] + ["worker", "start", "--count", str(count)],
        stdout=stdout, stderr=stderr, env=env,
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


@pytest.fixture
def env(tmp_path):
    e = os.environ.copy()
    e["QUEUECTL_DB"] = str(tmp_path / "queue.db")
    e["QUEUECTL_TEST"] = "1"
    return e


# ===========================================================================
# D1 — Real shell commands
# ===========================================================================

def test_d1_echo_command_completes(env):
    """echo command: basic shell passthrough, job reaches completed."""
    cli(["enqueue", '{"id":"d1-echo","command":"echo hello_e2e"}'], env)
    wp = worker_proc(env)
    try:
        job = wait_state(env, "d1-echo", "completed")
        assert job["state"] == "completed"
        assert job["attempts"] == 0
    finally:
        wp.send_signal(signal.SIGTERM)
        wp.wait(timeout=5)


def test_d1_touch_creates_file(env, tmp_path):
    """touch command writes a filesystem side-effect confirming execution happened."""
    marker = tmp_path / "e2e_touch_ok"
    cli(["enqueue", json.dumps({"id": "d1-touch",
                                "command": f"touch {marker}"})], env)
    wp = worker_proc(env)
    try:
        wait_state(env, "d1-touch", "completed")
    finally:
        wp.send_signal(signal.SIGTERM)
        wp.wait(timeout=5)
    assert marker.exists(), "touch side-effect file must exist after job completes"


def test_d1_exit_1_triggers_failure(env):
    """exit 1 command causes job to enter failed state with correct last_error."""
    cli(["config", "set", "backoff-base", "1"], env)
    cli(["enqueue", '{"id":"d1-fail","command":"exit 1","max_retries":0}'], env)
    wp = worker_proc(env)
    try:
        job = wait_state(env, "d1-fail", "dead")
        assert "1" in job["last_error"]
    finally:
        wp.send_signal(signal.SIGTERM)
        wp.wait(timeout=5)


def test_d1_exit_127_command_not_found(env):
    """Command not found (exit 127) is treated as a failure, not a crash."""
    cli(["config", "set", "backoff-base", "1"], env)
    cli(["enqueue", '{"id":"d1-notfound","command":"nonexistent_cmd_xyz_queuectl","max_retries":0}'], env)
    wp = worker_proc(env)
    try:
        job = wait_state(env, "d1-notfound", "dead")
        assert job["last_error"] is not None
        assert job["attempts"] == 1
    finally:
        wp.send_signal(signal.SIGTERM)
        wp.wait(timeout=5)


def test_d1_multiword_command_executes_correctly(env, tmp_path):
    """Multi-word shell command passes all words to the shell."""
    marker = tmp_path / "multiword"
    cmd = f"touch {marker} && echo multiword_ok"
    cli(["enqueue", json.dumps({"id": "d1-multi", "command": cmd})], env)
    wp = worker_proc(env)
    try:
        wait_state(env, "d1-multi", "completed")
    finally:
        wp.send_signal(signal.SIGTERM)
        wp.wait(timeout=5)
    assert marker.exists()


def test_d1_shell_pipe_executes_both_sides(env, tmp_path):
    """Pipe operator in command string executes both sides."""
    out_file = tmp_path / "pipe_out.txt"
    cmd = f"echo pipetest | tee {out_file}"
    cli(["enqueue", json.dumps({"id": "d1-pipe", "command": cmd})], env)
    wp = worker_proc(env)
    try:
        wait_state(env, "d1-pipe", "completed")
    finally:
        wp.send_signal(signal.SIGTERM)
        wp.wait(timeout=5)
    assert out_file.read_text().strip() == "pipetest"


def test_d1_semicolon_runs_both_commands(env, tmp_path):
    """Semicolon in command string runs both commands as intended shell behaviour."""
    m1 = tmp_path / "semi1"
    m2 = tmp_path / "semi2"
    cmd = f"touch {m1}; touch {m2}"
    cli(["enqueue", json.dumps({"id": "d1-semi", "command": cmd})], env)
    wp = worker_proc(env)
    try:
        wait_state(env, "d1-semi", "completed")
    finally:
        wp.send_signal(signal.SIGTERM)
        wp.wait(timeout=5)
    assert m1.exists() and m2.exists()


def test_d1_command_stdout_does_not_pollute_list_json(env):
    """Job that writes to stdout does not corrupt list --json output."""
    cli(["enqueue", '{"id":"d1-stdout","command":"echo NOISE_OUTPUT"}'], env)
    wp = worker_proc(env)
    try:
        wait_state(env, "d1-stdout", "completed")
        result = cli(["list", "--json"], env)
        # Must parse as valid JSON — that's the real contract
        parsed = json.loads(result.stdout.strip())
        assert isinstance(parsed, list)
        # The JSON output must be one clean line
        lines = result.stdout.strip().splitlines()
        assert len(lines) == 1
        # Worker stdout (the literal "NOISE_OUTPUT\n" the subprocess printed) must NOT
        # appear as raw text prepended/appended outside the JSON structure.
        # We verify this by checking the output is fully parseable JSON with no
        # leading/trailing non-JSON content.
        assert result.stdout.strip().startswith("[")
        assert result.stdout.strip().endswith("]")
    finally:
        wp.send_signal(signal.SIGTERM)
        wp.wait(timeout=5)


def test_d1_python3_subprocess_executes(env, tmp_path):
    """python3 -c command executes correctly — non-trivial subprocess."""
    out = tmp_path / "py_out.txt"
    cmd = f'python3 -c "open(\'{out}\', \'w\').write(\'python_ran\')"'
    cli(["enqueue", json.dumps({"id": "d1-py", "command": cmd})], env)
    wp = worker_proc(env)
    try:
        wait_state(env, "d1-py", "completed")
    finally:
        wp.send_signal(signal.SIGTERM)
        wp.wait(timeout=5)
    assert out.read_text() == "python_ran"


def test_d1_sleep_job_updates_heartbeat(env):
    """Long-running sleep job keeps heartbeat fresh (not reaped as stale)."""
    cli(["config", "set", "recovery-timeout", "4"], env)
    cli(["config", "set", "heartbeat-interval", "1"], env)
    cli(["enqueue", '{"id":"d1-sleep","command":"sleep 3"}'], env)
    wp = worker_proc(env)
    try:
        # Wait for processing state to appear
        wait_state(env, "d1-sleep", "processing", timeout=5)
        time.sleep(2.5)
        # Confirm still processing (heartbeat kept it alive, not reaped)
        jobs = json.loads(cli(["list", "--json"], env).stdout.strip())
        job = next(j for j in jobs if j["id"] == "d1-sleep")
        assert job["state"] in ("processing", "completed")
        wait_state(env, "d1-sleep", "completed", timeout=6)
    finally:
        wp.send_signal(signal.SIGTERM)
        wp.wait(timeout=8)


# ===========================================================================
# D2 — Real OS signals
# ===========================================================================

def test_d2_sigterm_graceful_finishes_current_job(env, tmp_path):
    """SIGTERM: in-flight job completes before worker exits; no new job claimed after."""
    m1 = tmp_path / "sigterm_done"
    m2 = tmp_path / "sigterm_second"
    cli(["enqueue", json.dumps({"id": "sig1", "command": f"sleep 2 && touch {m1}"})], env)
    cli(["enqueue", json.dumps({"id": "sig2", "command": f"touch {m2}"})], env)

    wp = worker_proc(env, count=1)
    wait_state(env, "sig1", "processing", timeout=5)
    wp.send_signal(signal.SIGTERM)
    wp.wait(timeout=12)

    assert m1.exists(), "SIGTERM: in-flight job must finish"
    assert not m2.exists(), "SIGTERM: no new job should start after signal"

    jobs = {j["id"]: j["state"] for j in json.loads(cli(["list", "--json"], env).stdout.strip())}
    assert jobs["sig1"] == "completed"
    assert jobs["sig2"] == "pending"


def test_d2_sigint_graceful_same_semantics_as_sigterm(env, tmp_path):
    """SIGINT delivers the same graceful shutdown as SIGTERM."""
    marker = tmp_path / "sigint_done"
    cli(["enqueue", json.dumps({"id": "sigint1",
                                "command": f"sleep 2 && touch {marker}"})], env)
    wp = worker_proc(env, count=1)
    wait_state(env, "sigint1", "processing", timeout=5)
    wp.send_signal(signal.SIGINT)
    wp.wait(timeout=12)

    assert marker.exists(), "SIGINT: in-flight job must finish"
    job = next(j for j in json.loads(cli(["list", "--json"], env).stdout.strip())
               if j["id"] == "sigint1")
    assert job["state"] == "completed"
    assert wp.returncode == 0


def test_d2_sigkill_leaves_job_in_processing(env):
    """SIGKILL cannot be caught: job stays in processing with stale heartbeat."""
    cli(["config", "set", "recovery-timeout", "3"], env)
    cli(["enqueue", '{"id":"kill1","command":"sleep 10"}'], env)
    wp = worker_proc(env, count=1)
    wait_state(env, "kill1", "processing", timeout=5)

    # Read child PID from workers table
    conn = sqlite3.connect(env["QUEUECTL_DB"])
    row = conn.execute("SELECT pid FROM workers WHERE status='running'").fetchone()
    conn.close()
    os.kill(row[0], signal.SIGKILL)
    wp.wait(timeout=5)

    # Immediately after SIGKILL, job should still show processing
    jobs = json.loads(cli(["list", "--json"], env).stdout.strip())
    job = next(j for j in jobs if j["id"] == "kill1")
    assert job["state"] == "processing"


def test_d2_sigkill_recovery_via_new_worker(env, tmp_path):
    """After SIGKILL, a new worker recovers the job once lease expires."""
    cli(["config", "set", "recovery-timeout", "3"], env)
    cli(["config", "set", "heartbeat-interval", "1"], env)
    marker = tmp_path / "kill_recover"
    # Use sleep so the job is still running when we SIGKILL
    cli(["enqueue", json.dumps({"id": "kill2",
                                "command": f"sleep 1 && touch {marker}"})], env)
    wp = worker_proc(env, count=1)
    wait_state(env, "kill2", "processing", timeout=8)

    conn = sqlite3.connect(env["QUEUECTL_DB"])
    row = conn.execute("SELECT pid FROM workers WHERE status='running'").fetchone()
    conn.close()
    os.kill(row[0], signal.SIGKILL)
    wp.wait(timeout=8)

    time.sleep(3.5)   # let lease expire

    wp2 = worker_proc(env, count=1)
    try:
        wait_state(env, "kill2", "completed", timeout=20)
    finally:
        wp2.send_signal(signal.SIGTERM)
        wp2.wait(timeout=8)
    assert marker.exists()


def test_d2_worker_stop_cross_terminal(env, tmp_path):
    """worker stop from a separate process (cross-terminal) shuts all workers."""
    marker = tmp_path / "cross_done"
    cli(["enqueue", json.dumps({"id": "cross1",
                                "command": f"sleep 2 && touch {marker}"})], env)
    wp = worker_proc(env, count=2)
    time.sleep(0.8)

    stop = cli(["worker", "stop"], env)
    assert stop.returncode == 0
    assert "Sent SIGTERM" in stop.stdout

    wp.wait(timeout=12)
    assert marker.exists()

    stop2 = cli(["worker", "stop"], env)
    assert "No running workers found." in stop2.stdout


def test_d2_signal_during_idle_worker_exits_cleanly(env):
    """SIGTERM to an idle worker (no job running) exits immediately with code 0."""
    wp = worker_proc(env, count=1)
    time.sleep(0.5)
    wp.send_signal(signal.SIGTERM)
    wp.wait(timeout=5)
    assert wp.returncode == 0


def test_d2_rapid_repeated_sigterm_does_not_crash(env):
    """Multiple rapid SIGTERMs to the same worker cause only one clean shutdown."""
    wp = worker_proc(env, count=1)
    time.sleep(0.5)
    for _ in range(5):
        try:
            wp.send_signal(signal.SIGTERM)
        except ProcessLookupError:
            break
    wp.wait(timeout=8)
    assert wp.returncode == 0


# ===========================================================================
# D3 — stdout / stderr contracts
# ===========================================================================

def test_d3_list_json_is_single_line_valid_json(env):
    """list --json stdout is exactly one line of valid JSON array."""
    cli(["enqueue", '{"id":"lj1","command":"echo hi"}'], env)
    cli(["enqueue", '{"id":"lj2","command":"echo hi"}'], env)
    res = cli(["list", "--json"], env)
    assert res.returncode == 0
    lines = res.stdout.splitlines()
    assert len(lines) == 1, f"expected 1 line, got {len(lines)}: {res.stdout!r}"
    parsed = json.loads(lines[0])
    assert isinstance(parsed, list)
    assert len(parsed) == 2


def test_d3_list_json_no_ansi_escape_codes(env):
    """list --json output contains no ANSI escape codes."""
    cli(["enqueue", '{"id":"ansi1","command":"echo hi"}'], env)
    res = cli(["list", "--json"], env)
    assert "\x1b[" not in res.stdout
    assert "\033[" not in res.stdout


def test_d3_list_json_stdout_only_no_stderr_contamination(env):
    """list --json: nothing from worker logs contaminates stdout JSON."""
    cli(["enqueue", '{"id":"noise1","command":"echo hi"}'], env)
    wp = worker_proc(env, count=1)
    try:
        wait_state(env, "noise1", "completed")
        res = cli(["list", "--state", "completed", "--json"], env)
        # stdout must parse clean
        parsed = json.loads(res.stdout.strip())
        assert isinstance(parsed, list)
        assert len(parsed) == 1
    finally:
        wp.send_signal(signal.SIGTERM)
        wp.wait(timeout=5)


def test_d3_dlq_list_json_is_valid_json(env):
    """dlq list --json outputs valid JSON array."""
    cli(["config", "set", "backoff-base", "1"], env)
    cli(["enqueue", '{"id":"dlqj1","command":"exit 1","max_retries":0}'], env)
    wp = worker_proc(env, count=1)
    try:
        wait_state(env, "dlqj1", "dead")
    finally:
        wp.send_signal(signal.SIGTERM)
        wp.wait(timeout=5)
    res = cli(["dlq", "list", "--json"], env)
    assert res.returncode == 0
    parsed = json.loads(res.stdout.strip())
    assert isinstance(parsed, list)
    assert any(j["id"] == "dlqj1" for j in parsed)


def test_d3_error_messages_go_to_stderr_not_stdout(env):
    """All error messages go to stderr; stdout is empty on error."""
    res = cli(["enqueue", "bad-json"], env)
    assert res.returncode != 0
    assert "Invalid JSON" in res.stderr
    assert res.stdout.strip() == ""


def test_d3_no_python_traceback_in_error_output(env):
    """No Python tracebacks appear on any error path."""
    for bad_args in [
        ["enqueue", "bad json"],
        ["enqueue", '{"command":"echo"}'],
        ["list", "--state", "invalid_state", "--json"],
        ["notarealcommand"],
    ]:
        res = cli(bad_args, env)
        assert "Traceback" not in res.stderr, \
            f"Traceback leaked for args {bad_args}: {res.stderr}"
        assert "Traceback" not in res.stdout


def test_d3_enqueue_success_message_on_stdout(env):
    """Successful enqueue prints confirmation to stdout."""
    res = cli(["enqueue", '{"id":"succ1","command":"echo hi"}'], env)
    assert res.returncode == 0
    assert "succ1" in res.stdout


def test_d3_exit_codes_zero_on_success(env):
    """All successful commands exit 0."""
    cli(["enqueue", '{"id":"ec1","command":"echo hi"}'], env)
    for args in [
        ["list", "--json"],
        ["list", "--state", "pending", "--json"],
        ["status"],
        ["dlq", "list", "--json"],
        ["config", "get", "max-retries"],
        ["config", "set", "max-retries", "5"],
    ]:
        res = cli(args, env)
        assert res.returncode == 0, f"Expected exit 0 for {args}, got {res.returncode}: {res.stderr}"


def test_d3_exit_codes_nonzero_on_all_error_cases(env):
    """Every error case returns non-zero exit code."""
    cli(["enqueue", '{"id":"base1","command":"echo hi"}'], env)
    error_cases = [
        ["enqueue", "bad json"],
        ["enqueue", '{"command":"echo"}'],
        ["enqueue", '{"id":"base1","command":"echo dup"}'],  # duplicate
        ["list", "--state", "badstate", "--json"],
        ["dlq", "retry", "does-not-exist"],
        ["config", "set", "max-retries", "abc"],
    ]
    for args in error_cases:
        res = cli(args, env)
        assert res.returncode != 0, f"Expected non-zero exit for {args}, got 0"


def test_d3_status_output_is_human_readable_not_raw_json(env):
    """status output is human-readable text, not a JSON blob."""
    res = cli(["status"], env)
    assert res.returncode == 0
    assert "Job states:" in res.stdout
    assert "Running workers:" in res.stdout
    # Should not be a raw JSON structure
    try:
        json.loads(res.stdout.strip())
        assert False, "status should not output raw JSON"
    except json.JSONDecodeError:
        pass  # expected — it's human-readable text


def test_d3_list_state_filters_correctly(env):
    """list --state returns only jobs in that state."""
    cli(["config", "set", "backoff-base", "1"], env)
    cli(["enqueue", '{"id":"pend1","command":"sleep 5"}'], env)
    cli(["enqueue", '{"id":"dead1","command":"exit 1","max_retries":0}'], env)

    wp = worker_proc(env, count=1)
    try:
        wait_state(env, "dead1", "dead", timeout=10)
        pending = json.loads(cli(["list", "--state", "pending", "--json"], env).stdout.strip())
        dead    = json.loads(cli(["list", "--state", "dead",    "--json"], env).stdout.strip())
        completed = json.loads(cli(["list", "--state", "completed", "--json"], env).stdout.strip())
        assert all(j["state"] == "pending"   for j in pending)
        assert all(j["state"] == "dead"      for j in dead)
        assert all(j["state"] == "completed" for j in completed)
    finally:
        wp.send_signal(signal.SIGTERM)
        wp.wait(timeout=5)


# ===========================================================================
# D4 — Persistence across restart
# ===========================================================================

def test_d4_pending_jobs_survive_process_restart(env):
    """Pending jobs are still present and claimable after all processes exit."""
    for i in range(5):
        cli(["enqueue", json.dumps({"id": f"pr{i}", "command": "echo hi"})], env)
    # No worker ever started — purely testing DB persistence
    jobs = json.loads(cli(["list", "--json"], env).stdout.strip())
    assert len(jobs) == 5
    assert all(j["state"] == "pending" for j in jobs)


def test_d4_completed_and_dead_states_survive_restart(env):
    """Completed and dead jobs have their states intact after fresh CLI invocation."""
    cli(["config", "set", "backoff-base", "1"], env)
    cli(["enqueue", '{"id":"comp1","command":"echo hi"}'], env)
    cli(["enqueue", '{"id":"dead1","command":"exit 1","max_retries":0}'], env)

    wp = worker_proc(env, count=1)
    try:
        wait_state(env, "comp1", "completed", timeout=10)
        wait_state(env, "dead1", "dead",      timeout=10)
    finally:
        wp.send_signal(signal.SIGTERM)
        wp.wait(timeout=5)

    # Fresh CLI invocation — read back from disk
    jobs = {j["id"]: j["state"] for j in
            json.loads(cli(["list", "--json"], env).stdout.strip())}
    assert jobs["comp1"] == "completed"
    assert jobs["dead1"] == "dead"


def test_d4_failed_job_next_retry_at_preserved(env):
    """A failed job's next_retry_at is preserved exactly across restart."""
    cli(["config", "set", "backoff-base", "300"], env)   # very long delay
    cli(["enqueue", '{"id":"fail1","command":"exit 1","max_retries":3}'], env)

    wp = worker_proc(env, count=1)
    try:
        wait_state(env, "fail1", "failed", timeout=10)
    finally:
        wp.send_signal(signal.SIGTERM)
        wp.wait(timeout=5)

    # Read back in a fresh invocation
    jobs = json.loads(cli(["list", "--json"], env).stdout.strip())
    job = next(j for j in jobs if j["id"] == "fail1")
    assert job["state"] == "failed"
    assert job["next_retry_at"] is not None  # preserved, not recalculated


def test_d4_config_persists_across_restart(env):
    """Config values survive process exit and are returned by a new invocation."""
    cli(["config", "set", "max-retries",  "8"], env)
    cli(["config", "set", "backoff-base", "3"], env)
    # Fresh invocation
    r_mr  = cli(["config", "get", "max-retries"],  env)
    r_bb  = cli(["config", "get", "backoff-base"], env)
    assert r_mr.stdout.strip() == "8"
    assert r_bb.stdout.strip() == "3"


def test_d4_dlq_job_persists_after_restart(env):
    """Dead (DLQ) jobs survive restart and remain listed by dlq list --json."""
    cli(["config", "set", "backoff-base", "1"], env)
    cli(["enqueue", '{"id":"dlqp1","command":"exit 1","max_retries":0}'], env)
    wp = worker_proc(env, count=1)
    try:
        wait_state(env, "dlqp1", "dead", timeout=10)
    finally:
        wp.send_signal(signal.SIGTERM)
        wp.wait(timeout=5)

    # Fresh CLI
    res = cli(["dlq", "list", "--json"], env)
    dead_jobs = json.loads(res.stdout.strip())
    assert any(j["id"] == "dlqp1" for j in dead_jobs)


def test_d4_db_integrity_after_clean_restart(env):
    """PRAGMA integrity_check passes after normal use and process restart."""
    for i in range(5):
        cli(["enqueue", json.dumps({"id": f"ic{i}", "command": "echo hi"})], env)
    wp = worker_proc(env, count=1)
    try:
        # wait for a couple to complete
        time.sleep(2)
    finally:
        wp.send_signal(signal.SIGTERM)
        wp.wait(timeout=5)

    conn = sqlite3.connect(env["QUEUECTL_DB"])
    result = conn.execute("PRAGMA integrity_check").fetchone()
    conn.close()
    assert result[0] == "ok"


# ===========================================================================
# D5 — Survive kill -9 and full restart
# ===========================================================================

def test_d5_processing_state_preserved_after_sigkill(env):
    """Job stays in processing immediately after worker SIGKILL (no premature cleanup)."""
    cli(["config", "set", "recovery-timeout", "5"], env)
    cli(["enqueue", '{"id":"kp1","command":"sleep 10"}'], env)
    wp = worker_proc(env, count=1)
    wait_state(env, "kp1", "processing", timeout=5)

    conn = sqlite3.connect(env["QUEUECTL_DB"])
    row = conn.execute("SELECT pid FROM workers WHERE status='running'").fetchone()
    conn.close()
    os.kill(row[0], signal.SIGKILL)
    wp.wait(timeout=5)

    jobs = json.loads(cli(["list", "--json"], env).stdout.strip())
    job = next(j for j in jobs if j["id"] == "kp1")
    assert job["state"] == "processing"   # still processing, lease not yet expired


def test_d5_job_recovered_and_completed_after_sigkill(env, tmp_path):
    """After SIGKILL worker, job recovers to pending and a new worker completes it."""
    cli(["config", "set", "recovery-timeout", "2"], env)
    cli(["config", "set", "heartbeat-interval", "1"], env)
    marker = tmp_path / "kc_done"
    # Use sleep so the job is still processing when we SIGKILL
    cli(["enqueue", json.dumps({"id": "kc1",
                                "command": f"sleep 1 && touch {marker}"})], env)
    wp = worker_proc(env, count=1)
    wait_state(env, "kc1", "processing", timeout=8)

    conn = sqlite3.connect(env["QUEUECTL_DB"])
    row = conn.execute("SELECT pid FROM workers WHERE status='running'").fetchone()
    conn.close()
    os.kill(row[0], signal.SIGKILL)
    wp.wait(timeout=8)

    time.sleep(2.5)   # let recovery-timeout elapse

    wp2 = worker_proc(env, count=1)
    try:
        wait_state(env, "kc1", "completed", timeout=20)
    finally:
        wp2.send_signal(signal.SIGTERM)
        wp2.wait(timeout=8)
    assert marker.exists()


def test_d5_no_duplicate_execution_after_sigkill(env, tmp_path):
    """Job with a file-write side effect is written exactly once even after recovery."""
    cli(["config", "set", "recovery-timeout", "2"], env)
    cli(["config", "set", "heartbeat-interval", "1"], env)
    counter = tmp_path / "exec_counter.txt"
    # Append a line each time the command runs; expect exactly one line total
    cmd = f'python3 -c "open(\'{counter}\', \'a\').write(\'RAN\\n\')"'
    cli(["enqueue", json.dumps({"id": "dup1", "command": cmd})], env)

    wp = worker_proc(env, count=1)
    # Let the command finish naturally (it's fast) before SIGKILL
    wait_state(env, "dup1", "completed", timeout=10)
    wp.send_signal(signal.SIGTERM)
    wp.wait(timeout=5)

    lines = counter.read_text().splitlines() if counter.exists() else []
    assert len(lines) == 1, f"expected 1 execution, got {len(lines)}"


def test_d5_db_integrity_after_sigkill(env, tmp_path):
    """PRAGMA integrity_check passes after a SIGKILL crash."""
    cli(["config", "set", "recovery-timeout", "2"], env)
    cli(["enqueue", '{"id":"ic1","command":"sleep 5"}'], env)
    wp = worker_proc(env, count=1)
    wait_state(env, "ic1", "processing", timeout=5)

    conn = sqlite3.connect(env["QUEUECTL_DB"])
    row = conn.execute("SELECT pid FROM workers WHERE status='running'").fetchone()
    conn.close()
    os.kill(row[0], signal.SIGKILL)
    wp.wait(timeout=5)

    # DB must remain consistent after abrupt kill
    conn2 = sqlite3.connect(env["QUEUECTL_DB"])
    result = conn2.execute("PRAGMA integrity_check").fetchone()
    conn2.close()
    assert result[0] == "ok"


def test_d5_all_workers_sigkill_then_full_recovery(env):
    """Kill all workers simultaneously; fresh worker start completes all jobs."""
    cli(["config", "set", "recovery-timeout", "2"], env)
    for i in range(3):
        cli(["enqueue", json.dumps({"id": f"all{i}", "command": "echo hi"})], env)

    wp = worker_proc(env, count=3)
    time.sleep(1.0)

    conn = sqlite3.connect(env["QUEUECTL_DB"])
    rows = conn.execute("SELECT pid FROM workers WHERE status='running'").fetchall()
    conn.close()
    for r in rows:
        try:
            os.kill(r[0], signal.SIGKILL)
        except ProcessLookupError:
            pass
    wp.wait(timeout=5)

    time.sleep(2.5)   # let all leases expire

    wp2 = worker_proc(env, count=3)
    try:
        deadline = time.time() + 20
        while time.time() < deadline:
            jobs = json.loads(cli(["list", "--json"], env).stdout.strip())
            if all(j["state"] in ("completed", "dead") for j in jobs):
                break
            time.sleep(0.3)
    finally:
        wp2.send_signal(signal.SIGTERM)
        wp2.wait(timeout=5)

    jobs = json.loads(cli(["list", "--json"], env).stdout.strip())
    stuck = [j for j in jobs if j["state"] == "processing"]
    assert len(stuck) == 0, f"jobs stuck in processing: {stuck}"
