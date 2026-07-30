"""
test_security.py — Security and input-safety tests.

Focus areas:
  SEC-1  SQL injection via id and command fields
  SEC-2  Shell metacharacter containment (no early execution at enqueue time)
  SEC-3  Path traversal in id field
  SEC-4  Huge / malformed / deeply-nested JSON payloads
  SEC-5  Null bytes, Unicode normalization edge cases
  SEC-6  CLI argument injection (state filter, config value)
  SEC-7  Jobs table still intact after all injection attempts
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


def cli(args, env, timeout=15):
    return subprocess.run(
        [sys.executable, "-m", "queuectl"] + args,
        capture_output=True, text=True, timeout=timeout, env=env,
    )


def worker_proc(env, count=1):
    return subprocess.Popen(
        [sys.executable, "-m", "queuectl"] + ["worker", "start", "--count", str(count)],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, env=env,
    )


def wait_state(env, job_id, state, timeout=12):
    import time as _t
    deadline = _t.time() + timeout
    while _t.time() < deadline:
        raw = cli(["list", "--json"], env).stdout.strip()
        for j in json.loads(raw):
            if j["id"] == job_id and j["state"] == state:
                return j
        _t.sleep(0.2)
    raise AssertionError(f"{job_id} never reached {state}")


@pytest.fixture
def env(tmp_path):
    e = os.environ.copy()
    e["QUEUECTL_DB"] = str(tmp_path / "queue.db")
    e["QUEUECTL_TEST"] = "1"
    return e


# ===========================================================================
# SEC-1  SQL injection
# ===========================================================================

def test_sql_injection_in_id_stored_as_literal(env):
    """SQL injection in job id is stored as a literal string, never executed."""
    evil_id = "'; DROP TABLE jobs; --"
    res = cli(["enqueue", json.dumps({"id": evil_id, "command": "echo safe"})], env)
    assert res.returncode == 0

    # jobs table must still exist and contain the row
    conn = sqlite3.connect(env["QUEUECTL_DB"])
    tables = [r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")]
    conn.close()
    assert "jobs" in tables, "jobs table was dropped by SQL injection"

    jobs = json.loads(cli(["list", "--json"], env).stdout.strip())
    assert len(jobs) == 1
    assert jobs[0]["id"] == evil_id   # stored verbatim


def test_sql_injection_in_command_stored_as_literal(env):
    """SQL injection in job command is stored verbatim, never interpolated into SQL."""
    evil_cmd = "echo hi'; UPDATE jobs SET state='dead'; --"
    res = cli(["enqueue", json.dumps({"id": "sqlinj", "command": evil_cmd})], env)
    assert res.returncode == 0

    jobs = json.loads(cli(["list", "--json"], env).stdout.strip())
    assert jobs[0]["command"] == evil_cmd
    assert jobs[0]["state"] == "pending"   # UPDATE never ran


def test_sql_injection_in_state_filter_rejected(env):
    """Passing SQL injection as --state value is rejected with a clean error."""
    res = cli(["list", "--state", "pending' OR '1'='1", "--json"], env)
    assert res.returncode != 0
    assert "Traceback" not in res.stderr


def test_sql_injection_in_config_value_rejected_as_invalid_type(env):
    """SQL injection via config set value is rejected as a type error for numeric keys."""
    res = cli(["config", "set", "max-retries", "5; DROP TABLE jobs; --"], env)
    assert res.returncode != 0
    conn = sqlite3.connect(env["QUEUECTL_DB"])
    tables = [r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")]
    conn.close()
    assert "jobs" in tables


# ===========================================================================
# SEC-2  Shell metacharacter containment
# ===========================================================================

def test_shell_metacharacters_not_executed_at_enqueue_time(env, tmp_path):
    """Shell metacharacters in command are NOT executed when the job is enqueued."""
    marker = tmp_path / "should_not_exist"
    evil_cmd = f"touch {marker}"
    # enqueue — must not execute the touch
    cli(["enqueue", json.dumps({"id": "meta1", "command": evil_cmd})], env)
    assert not marker.exists(), "command was executed at enqueue time (shell injection)"


def test_dollar_parens_not_executed_at_enqueue_time(env, tmp_path):
    """$(command) substitution in command field is not executed at enqueue time."""
    marker = tmp_path / "subshell_leak"
    evil_cmd = f"echo $(touch {marker})"
    cli(["enqueue", json.dumps({"id": "meta2", "command": evil_cmd})], env)
    assert not marker.exists(), "subshell executed at enqueue time"


def test_dollar_parens_in_id_not_executed(env, tmp_path):
    """$(command) substitution in id field is not executed at enqueue time."""
    marker = tmp_path / "id_subshell_leak"
    evil_id = f"$(touch {marker})"
    cli(["enqueue", json.dumps({"id": evil_id, "command": "echo hi"})], env)
    assert not marker.exists(), "subshell in id was executed"


def test_backtick_in_command_not_executed_at_enqueue_time(env, tmp_path):
    """Backtick substitution in command field is not executed at enqueue time."""
    marker = tmp_path / "backtick_leak"
    evil_cmd = f"echo `touch {marker}`"
    cli(["enqueue", json.dumps({"id": "meta3", "command": evil_cmd})], env)
    assert not marker.exists(), "backtick executed at enqueue time"


def test_semicolon_in_command_executed_at_worker_time_not_enqueue(env, tmp_path):
    """Semicolon command expansion is intentional shell behaviour — runs at execution time."""
    m1 = tmp_path / "semi1"
    m2 = tmp_path / "semi2"
    cmd = f"touch {m1}; touch {m2}"
    cli(["enqueue", json.dumps({"id": "semi1", "command": cmd})], env)
    # not yet run
    assert not m1.exists() and not m2.exists()

    wp = worker_proc(env)
    try:
        wait_state(env, "semi1", "completed")
    finally:
        wp.send_signal(signal.SIGTERM)
        wp.wait(timeout=5)
    # now both run — that's expected shell behaviour
    assert m1.exists() and m2.exists()


def test_command_with_single_quotes_round_trips(env, tmp_path):
    """Single quotes in command string round-trip through storage intact."""
    out = tmp_path / "sq_out.txt"
    cmd = f"echo 'hello world' > {out}"
    cli(["enqueue", json.dumps({"id": "sq1", "command": cmd})], env)

    jobs = json.loads(cli(["list", "--json"], env).stdout.strip())
    assert jobs[0]["command"] == cmd   # stored verbatim

    wp = worker_proc(env)
    try:
        wait_state(env, "sq1", "completed")
    finally:
        wp.send_signal(signal.SIGTERM)
        wp.wait(timeout=5)
    assert out.read_text().strip() == "hello world"


# ===========================================================================
# SEC-3  Path traversal
# ===========================================================================

def test_path_traversal_in_id_stored_as_literal(env):
    """Path traversal string in job id is stored as a literal — no filesystem access."""
    traversal_id = "../../../etc/passwd"
    res = cli(["enqueue", json.dumps({"id": traversal_id, "command": "echo hi"})], env)
    assert res.returncode == 0

    jobs = json.loads(cli(["list", "--json"], env).stdout.strip())
    assert jobs[0]["id"] == traversal_id   # stored verbatim, no traversal occurred


def test_path_traversal_dlq_retry_uses_parameterized_query(env):
    """dlq retry with a path-traversal id does not open arbitrary files."""
    traversal_id = "../../etc/passwd"
    res = cli(["dlq", "retry", traversal_id], env)
    assert res.returncode != 0            # job doesn't exist
    assert "No dead job" in res.stderr    # clean message, no file-open error


# ===========================================================================
# SEC-4  Huge / malformed / deeply-nested payloads
# ===========================================================================

def test_huge_json_payload_1mb_handled_gracefully(env):
    """Large JSON payloads are either stored or rejected cleanly — no crash or traceback.
    Note: 1 MB via CLI argument exceeds OS execve arg limits on many systems.
    We test with 200 KB (safely within limits) which exercises the same code path.
    """
    # 200 KB command string — well within OS arg limits, exercises large-payload handling
    huge_cmd = "echo " + "x" * (200 * 1024)
    payload = json.dumps({"id": "huge1", "command": huge_cmd})
    res = cli(["enqueue", payload], env)
    # Must not crash; either accept or reject cleanly
    assert "Traceback" not in res.stderr
    assert "Traceback" not in res.stdout


def test_20k_command_stored_without_truncation(env):
    """20 000-character command is stored and retrieved in full."""
    long_cmd = "echo " + "a" * 20000
    cli(["enqueue", json.dumps({"id": "long1", "command": long_cmd})], env)
    jobs = json.loads(cli(["list", "--json"], env).stdout.strip())
    assert len(jobs[0]["command"]) == len(long_cmd)


def test_malformed_json_various_forms_all_rejected(env):
    """All common JSON malformations exit non-zero with no partial DB write."""
    bad_payloads = [
        "not json at all",
        "{id: 'missing-quotes'}",
        '{"id":"j1","command":}',   # trailing syntax error
        '{"id":"j1"}',              # missing command
        '{"command":"echo hi"}',    # missing id
        "null",                     # JSON null at root
        "[]",                       # JSON array at root
        "",                         # empty string
    ]
    for payload in bad_payloads:
        res = cli(["enqueue", payload], env)
        assert res.returncode != 0, f"Expected non-zero for payload: {payload!r}"
        assert "Traceback" not in res.stderr

    jobs = json.loads(cli(["list", "--json"], env).stdout.strip())
    assert len(jobs) == 0, "no partial rows should be inserted for malformed payloads"


def test_deeply_nested_json_does_not_crash(env):
    """Deeply nested JSON (100 levels) is handled without a stack-overflow crash."""
    nested = "x"
    for _ in range(100):
        nested = {"k": nested}
    payload = json.dumps({"id": "nested1", "command": json.dumps(nested)})
    res = cli(["enqueue", payload], env)
    assert "Traceback" not in res.stderr
    assert res.returncode == 0   # deep value is a valid command string


# ===========================================================================
# SEC-5  Null bytes, Unicode edge cases
# ===========================================================================

def test_unicode_id_and_command_round_trip(env):
    """Unicode characters in id and command round-trip through JSON and DB intact."""
    uid  = "job-\u2603-\U0001F680"   # snowman + rocket
    ucmd = "echo '\u4e2d\u6587 hello'"
    cli(["enqueue", json.dumps({"id": uid, "command": ucmd})], env)
    jobs = json.loads(cli(["list", "--json"], env).stdout.strip())
    assert jobs[0]["id"]      == uid
    assert jobs[0]["command"] == ucmd


def test_unicode_id_round_trips_through_dlq_retry(env):
    """Unicode job id works correctly through the full enqueue → dead → dlq retry path."""
    uid = "job-\U0001F525"   # fire emoji
    cli(["config", "set", "backoff-base", "1"], env)
    cli(["enqueue", json.dumps({"id": uid, "command": "exit 1", "max_retries": 0})], env)

    wp = worker_proc(env)
    try:
        wait_state(env, uid, "dead")
    finally:
        wp.send_signal(signal.SIGTERM)
        wp.wait(timeout=5)

    res = cli(["dlq", "retry", uid], env)
    assert res.returncode == 0
    assert uid in res.stdout


def test_null_byte_in_json_string_handled_gracefully(env):
    """Null byte (\\u0000) in a JSON string field does not crash the CLI."""
    payload = '{"id":"nb1","command":"echo \\u0000hi"}'
    res = cli(["enqueue", payload], env)
    assert "Traceback" not in res.stderr
    # Either accepted or rejected cleanly
    assert res.returncode in (0, 1)


# ===========================================================================
# SEC-6  Resource exhaustion via parallel enqueue
# ===========================================================================

def test_parallel_enqueue_no_corruption(env):
    """20 parallel enqueue calls with unique IDs all succeed without DB corruption."""
    procs = []
    for i in range(20):
        p = subprocess.Popen(
            [sys.executable, "-m", "queuectl"] + ["enqueue",
             json.dumps({"id": f"par-{i}", "command": f"echo {i}"})],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, env=env,
        )
        procs.append(p)
    for p in procs:
        p.wait(timeout=15)

    jobs = json.loads(cli(["list", "--json"], env).stdout.strip())
    assert len(jobs) == 20
    ids = {j["id"] for j in jobs}
    assert len(ids) == 20   # no duplicates, no missing

    conn = sqlite3.connect(env["QUEUECTL_DB"])
    result = conn.execute("PRAGMA integrity_check").fetchone()
    conn.close()
    assert result[0] == "ok"


# ===========================================================================
# SEC-7  Jobs table survives all injection attempts
# ===========================================================================

def test_jobs_table_intact_after_all_injection_attempts(env):
    """After all injection tests, jobs table still exists and is queryable."""
    injection_ids = [
        "'; DROP TABLE jobs; --",
        "\" OR 1=1 --",
        "../../../etc/passwd",
        "$(rm -rf /tmp/queuectl_sec_test)",
        "`whoami`",
    ]
    for eid in injection_ids:
        cli(["enqueue", json.dumps({"id": eid, "command": "echo safe"})], env)

    conn = sqlite3.connect(env["QUEUECTL_DB"])
    tables = [r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")]
    rows   = conn.execute("SELECT COUNT(*) FROM jobs").fetchone()[0]
    conn.close()

    assert "jobs" in tables
    assert rows == len(injection_ids)
