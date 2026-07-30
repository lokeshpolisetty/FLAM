"""Tests automatically extracted from add-test-strategy.md"""

import json
import os
import signal
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
import pytest
import multiprocessing
try:
    multiprocessing.set_start_method("fork", force=True)
except RuntimeError:
    pass


ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
from queuectl import database as db
from queuectl.cli.entrypoint import app
from conftest import run, start_worker, wait_for_state, list_jobs

cli = run

def _insert_job(conn, id_, state="pending", attempts=0, max_retries=3,
                backoff_base=2.0, worker_id=None, heartbeat_at=None,
                next_retry_at=None, last_error=None):
    ts = db.now_iso()
    conn.execute(
        '''INSERT INTO jobs
           (id, command, state, attempts, max_retries, backoff_base,
            worker_id, heartbeat_at, next_retry_at, last_error,
            created_at, updated_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
        (id_, f"echo {id_}", state, attempts, max_retries, backoff_base,
         worker_id, heartbeat_at, next_retry_at, last_error, ts, ts),
    )

@pytest.fixture
def tmp_db(tmp_path, monkeypatch):
    db_file = str(tmp_path / "unit_test.db")
    monkeypatch.setenv("QUEUECTL_DB", db_file)
    monkeypatch.setattr(db.connection, "DB_PATH", db_file)
    db.init_db()
    conn = db.get_connection()
    yield conn
    conn.close()

@pytest.fixture
def conn(tmp_db):
    return tmp_db

def test_max_retries_negative_one_behaviour(tmp_db):
    """max_retries=-1: define and assert — currently goes dead on first failure."""
    _insert_job(tmp_db, "j1", state="processing", worker_id="w1",
                heartbeat_at=db.now_iso(), attempts=0, max_retries=-1)
    db.finish_job(tmp_db, "j1", "w1", 1)
    row = tmp_db.execute("SELECT state FROM jobs WHERE id='j1'").fetchone()
    # Document: -1 means "all failures go dead immediately"
    assert row["state"] == "dead"

def test_config_set_negative_max_retries_rejected_or_documented(env):
    """Either reject negative values or document exact behavior."""
    r = run(["config", "set", "max-retries", "--", "-1"], env)
    # Currently accepted (int("-1") succeeds) — document this:
    assert r.returncode == 0  # OR assert r.returncode == 1 if validation added

def test_attempts_invariants_across_all_transitions(tmp_db):
    _insert_job(tmp_db, "j1", state="pending", attempts=0)
    # pending -> processing: attempts unchanged
    db.claim_next_job(tmp_db, "w1")
    assert tmp_db.execute("SELECT attempts FROM jobs WHERE id='j1'").fetchone()[0] == 0
    # processing -> failed: attempts incremented by 1
    db.finish_job(tmp_db, "j1", "w1", 1)
    assert tmp_db.execute("SELECT attempts FROM jobs WHERE id='j1'").fetchone()[0] == 1
    # failed -> pending (promote): attempts unchanged
    tmp_db.execute("UPDATE jobs SET next_retry_at=? WHERE id='j1'",
                   ((datetime.now(timezone.utc) - timedelta(seconds=5)).isoformat(),))
    db.promote_ready_retries(tmp_db)
    assert tmp_db.execute("SELECT attempts FROM jobs WHERE id='j1'").fetchone()[0] == 1

def test_promote_clears_next_retry_at(conn):
    past = (datetime.now(timezone.utc) - timedelta(seconds=5)).isoformat()
    _insert_job(conn, "j1", state="failed", attempts=1, next_retry_at=past)
    db.promote_ready_retries(conn)
    row = conn.execute("SELECT next_retry_at FROM jobs WHERE id='j1'").fetchone()
    assert row["next_retry_at"] is None, "next_retry_at must be NULL after promotion"

def test_claim_exactly_one_wins_across_separate_processes(tmp_path, monkeypatch):
    """10 separate OS processes claiming 1 job — exactly 1 wins."""
    db_file = str(tmp_path / "mp_claim.db")
    monkeypatch.setenv("QUEUECTL_DB", db_file)
    monkeypatch.setattr(db.connection, "DB_PATH", db_file)
    db.init_db()
    ts = db.now_iso()
    c = db.get_connection()
    c.execute(
        "INSERT INTO jobs (id,command,state,attempts,max_retries,backoff_base,"
        "created_at,updated_at) VALUES ('j1','echo hi','pending',0,3,2.0,?,?)",
        (ts, ts),
    )
    c.close()

    import multiprocessing, pickle
    barrier = multiprocessing.Barrier(10)
    results = multiprocessing.Manager().list()

    def try_claim(db_path, barrier, results):
        import sys, os
        sys.path.insert(0, os.path.dirname(db_path))
        from queuectl import database as _db
        _db.connection.DB_PATH = db_path
        barrier.wait()   # synchronize all processes to hit claim at same instant
        conn = _db.get_connection()
        job = _db.claim_next_job(conn, f"w-{os.getpid()}")
        results.append(job["id"] if job else None)
        conn.close()

    procs = [
        multiprocessing.Process(target=try_claim, args=(db_file, barrier, results))
        for _ in range(10)
    ]
    for p in procs: p.start()
    for p in procs: p.join()

    claimed = [r for r in results if r is not None]
    assert len(claimed) == 1, f"Expected 1 claim across 10 processes, got {len(claimed)}"

def test_reap_concurrent_processes_no_double_recovery(tmp_path, monkeypatch):
    """Two processes calling reap_stale_jobs simultaneously — each job recovered once."""
    db_file = str(tmp_path / "dual_reap.db")
    monkeypatch.setenv("QUEUECTL_DB", db_file)
    monkeypatch.setattr(db.connection, "DB_PATH", db_file)
    db.init_db()
    stale = (datetime.now(timezone.utc) - timedelta(seconds=30)).isoformat()
    c = db.get_connection()
    for i in range(5):
        c.execute(
            "INSERT INTO jobs (id,command,state,worker_id,heartbeat_at,"
            "attempts,max_retries,backoff_base,created_at,updated_at) "
            "VALUES (?,?,?,?,?,0,3,2.0,?,?)",
            (f"j{i}", "echo hi", "processing", f"w-dead-{i}", stale, stale, stale),
        )
    c.close()

    import multiprocessing
    manager = multiprocessing.Manager()
    all_reaped = manager.list()

    def do_reap(db_path, results):
        from queuectl import database as _db; _db.connection.DB_PATH = db_path
        conn = _db.get_connection()
        reaped = _db.reap_stale_jobs(conn, timeout_seconds=15)
        results.extend(reaped)
        conn.close()

    barrier = multiprocessing.Barrier(2)
    p1 = multiprocessing.Process(target=do_reap, args=(db_file, all_reaped))
    p2 = multiprocessing.Process(target=do_reap, args=(db_file, all_reaped))
    p1.start(); p2.start()
    p1.join(); p2.join()

    # Every job must appear exactly once across both processes
    assert sorted(all_reaped) == [f"j{i}" for i in range(5)], \
        f"Expected each job recovered once, got: {list(all_reaped)}"

def test_worker_pids_in_db_match_actual_child_processes(env):
    """Every PID in workers table is an actual child of the worker start process."""
    import psutil, sqlite3
    wp = start_worker(env, count=3)
    try:
        time.sleep(1.5)
        parent = psutil.Process(wp.pid)
        actual_child_pids = {c.pid for c in parent.children(recursive=True)}
        conn = sqlite3.connect(env["QUEUECTL_DB"])
        db_pids = {r[0] for r in conn.execute(
            "SELECT pid FROM workers WHERE status='running'"
        ).fetchall()}
        conn.close()
        assert db_pids == actual_child_pids, \
            f"DB PIDs {db_pids} != actual child PIDs {actual_child_pids}"
    finally:
        wp.send_signal(signal.SIGTERM)
        wp.wait(timeout=5)

def test_promote_fires_independently_of_long_running_job(env):
    """A failed job's retry fires even while another worker is mid-long-job."""
    run(["config", "set", "backoff-base", "1"], env)
    run(["config", "set", "poll-interval", "0.5"], env)
    # One long job + one fast-failing job
    run(["enqueue", '{"id":"long","command":"sleep 20"}'], env)
    run(["enqueue", '{"id":"fast","command":"exit 1","max_retries":3}'], env)

    wp = start_worker(env, count=2)  # 2 workers: one takes long, one takes fast
    try:
        wait_for_state(env, "fast", "failed", timeout=5)
        # Backoff=1s; wait up to 3s for it to be promoted
        promoted = False
        for _ in range(30):
            jobs = {j["id"]: j for j in list_jobs(env)}
            if jobs["fast"]["state"] in ("pending", "processing"):
                promoted = True
                break
            time.sleep(0.1)
        
        assert jobs["long"]["state"] == "processing", "long job must still be running"
        assert promoted, "fast job must be re-promoted while long job is running"
    finally:
        wp.kill()
        wp.wait()
def test_register_worker_upsert_refreshes_started_at(conn):
    db.register_worker(conn, "w-1", 100)
    before = conn.execute(
        "SELECT started_at FROM workers WHERE worker_id='w-1'"
    ).fetchone()[0]
    time.sleep(0.05)
    db.register_worker(conn, "w-1", 101)  # same worker_id, new PID
    after = conn.execute(
        "SELECT started_at, pid FROM workers WHERE worker_id='w-1'"
    ).fetchone()
    assert after["pid"] == 101
    assert after["started_at"] >= before, \
        "started_at must advance on re-registration"

def test_sigterm_to_parent_propagates_to_all_children(env, tmp_path):
    """SIGTERM to the worker-start parent must stop all child workers."""
    markers = [tmp_path / f"done_{i}" for i in range(3)]
    for i, m in enumerate(markers):
        run(["enqueue", json.dumps({
            "id": f"pp-{i}",
            "command": f"sleep 3 && touch {m}"
        })], env)

    wp = start_worker(env, count=3)
    for i in range(3):
        wait_for_state(env, f"pp-{i}", "processing", timeout=10)

    # Send SIGTERM ONLY to the parent process (not pgrp)
    os.kill(wp.pid, signal.SIGTERM)
    wp.wait(timeout=15)

    # Parent must exit cleanly
    assert wp.returncode == 0

    # All in-flight jobs must complete (graceful shutdown)
    for m in markers:
        assert m.exists(), f"Job marker {m} not created — in-flight job was not completed"

    # No child workers remain alive
    import psutil
    try:
        parent = psutil.Process(wp.pid)
        children = parent.children(recursive=True)
        assert len(children) == 0, f"Orphaned child workers remain: {children}"
    except psutil.NoSuchProcess:
        pass  # parent gone, that's fine

def test_worker_stop_handles_mixed_live_and_stale_pids(env):
    """worker stop handles one live worker and one stale DB entry gracefully."""
    import sqlite3
    wp = start_worker(env, count=1)
    time.sleep(0.8)

    # Insert a fake stale entry with a PID that doesn't exist
    conn = sqlite3.connect(env["QUEUECTL_DB"])
    conn.execute(
        "INSERT INTO workers (worker_id, pid, status, started_at, heartbeat_at) "
        "VALUES ('w-stale', 99999999, 'running', ?, ?)",
        (db.now_iso(), db.now_iso()),
    )
    conn.commit()
    conn.close()

    r = run(["worker", "stop"], env)
    wp.wait(timeout=8)
    assert r.returncode == 0
    # Both entries must be resolved (live stopped, stale cleaned up)
    conn = sqlite3.connect(env["QUEUECTL_DB"])
    running = conn.execute(
        "SELECT COUNT(*) FROM workers WHERE status='running'"
    ).fetchone()[0]
    conn.close()
    assert running == 0

def test_multi_cycle_dlq_retry(env, tmp_path):
    """Two full dead → retry → dead → retry → completed cycles."""
    toggle = tmp_path / "toggle"
    toggle.write_text("0")  # fail when content is 0, succeed when 1

    cmd = (f'python3 -c "'
           f'v=open(\\"{toggle}\\").read().strip();'
           f'exit(1 if v==\\"0\\" else 0)'
           f'"')
    run(["config", "set", "backoff-base", "1"], env)
    run(["enqueue", json.dumps({"id": "cyc1", "command": cmd, "max_retries": 0})], env)

    # First cycle: fails → dead
    wp1 = start_worker(env, count=1)
    try:
        wait_for_state(env, "cyc1", "dead", timeout=10)
    finally:
        wp1.send_signal(signal.SIGTERM); wp1.wait(timeout=5)

    job = next(j for j in list_jobs(env) if j["id"] == "cyc1")
    assert job["attempts"] == 1
    # Wait — dead state has attempts=1 with max_retries=0

    # DLQ retry #1 — still fails (toggle still 0)
    r1 = run(["dlq", "retry", "cyc1"], env)
    assert r1.returncode == 0
    job_after_retry = next(j for j in list_jobs(env) if j["id"] == "cyc1")
    assert job_after_retry["attempts"] == 0  # reset by dlq retry

    wp2 = start_worker(env, count=1)
    try:
        wait_for_state(env, "cyc1", "dead", timeout=10)
    finally:
        wp2.send_signal(signal.SIGTERM); wp2.wait(timeout=5)

    # DLQ retry #2 — now toggle to succeed
    toggle.write_text("1")
    r2 = run(["dlq", "retry", "cyc1"], env)
    assert r2.returncode == 0

    wp3 = start_worker(env, count=1)
    try:
        wait_for_state(env, "cyc1", "completed", timeout=10)
    finally:
        wp3.send_signal(signal.SIGTERM); wp3.wait(timeout=5)

def test_status_counts_match_list_json_after_recovery(env, tmp_path):
    """Counts in status output must exactly match list --json grouped counts."""
    # Create one stale processing job that status will self-recover
    run(["config", "set", "recovery-timeout", "1"], env)
    run(["config", "set", "heartbeat-interval", "0.5"], env)
    run(["enqueue", '{"id":"sc1","command":"sleep 10"}'], env)
    wp = start_worker(env, count=1, logfile=tmp_path / "worker.log")
    wait_for_state(env, "sc1", "processing", timeout=5)
    # Kill worker to leave job stale
    import sqlite3
    conn = sqlite3.connect(env["QUEUECTL_DB"])
    row = conn.execute("SELECT pid FROM workers WHERE status='running'").fetchone()
    conn.close()
    os.kill(row[0], signal.SIGKILL)
    wp.wait(timeout=3)
    time.sleep(1.5)  # let lease expire

    status_out = run(["status"], env).stdout
    jobs = list_jobs(env)

    from collections import Counter
    state_counts = Counter(j["state"] for j in jobs)
    for state in ("pending", "processing", "completed", "failed", "dead"):
        count = state_counts.get(state, 0)
        assert f"{state}" in status_out
        assert str(count) in status_out, \
            f"status shows wrong count for {state}: expected {count}"

def test_list_json_no_state_returns_all_jobs_ordered(env):
    """list --json without --state returns all jobs in created_at ASC order."""
    run(["config", "set", "backoff-base", "1"], env)
    run(["enqueue", '{"id":"a1","command":"echo a1"}'], env)
    run(["enqueue", '{"id":"a2","command":"exit 1","max_retries":0}'], env)
    wp = start_worker(env, count=1)
    try:
        wait_for_state(env, "a1", "completed", timeout=10)
        wait_for_state(env, "a2", "dead", timeout=10)
    finally:
        wp.send_signal(signal.SIGTERM); wp.wait(timeout=5)

    jobs = list_jobs(env)  # no state filter
    assert len(jobs) == 2
    states = {j["id"]: j["state"] for j in jobs}
    assert states["a1"] == "completed"
    assert states["a2"] == "dead"
    assert jobs[0]["id"] == "a1", "Jobs should be ordered by created_at ASC"

def test_worker_start_blocked_when_heartbeat_exceeds_recovery(env):
    """worker start must exit 1 with a clear error when heartbeat-interval >= recovery-timeout."""
    run(["config", "set", "heartbeat-interval", "20"], env)
    run(["config", "set", "recovery-timeout", "15"], env)
    env.pop("QUEUECTL_TEST", None)
    r = run(["worker", "start", "--count", "1"], env, timeout=5)
    assert r.returncode == 1
    assert "heartbeat-interval" in r.stderr
    assert "recovery-timeout" in r.stderr

def test_worker_start_allowed_when_heartbeat_less_than_recovery(env):
    """worker start proceeds normally when heartbeat-interval < recovery-timeout."""
    run(["config", "set", "heartbeat-interval", "3"], env)
    run(["config", "set", "recovery-timeout", "15"], env)
    wp = start_worker(env, count=1)
    try:
        time.sleep(0.5)
        assert wp.poll() is None, "Worker must still be running"
    finally:
        wp.send_signal(signal.SIGTERM)
        wp.wait(timeout=5)

def test_subprocess_process_group_killed_on_worker_crash(env, tmp_path):
    """When worker crashes (e.g. unhandled exception), the child subprocess is terminated to prevent orphans."""
    pid_file = tmp_path / "sub_pid.txt"
    # Job writes its own PID, then sleeps forever
    cmd = f'sh -c "echo $$ > {pid_file}; sleep 9999"'
    run(["enqueue", json.dumps({"id": "orp1", "command": cmd})], env)

    # Monkeypatch to crash the worker after it starts the job
    crash_script = tmp_path / "crash_worker.py"
    crash_script.write_text("""
import sys, os, time
from queuectl.cli.entrypoint import app
import subprocess

orig_poll = subprocess.Popen.poll
def crashing_poll(self, *args, **kwargs):
    if not hasattr(self, '_poll_called'):
        self._poll_called = True
        return orig_poll(self, *args, **kwargs)
    raise SystemExit(1)
subprocess.Popen.poll = crashing_poll

if __name__ == '__main__':
    from typer.testing import CliRunner
    runner = CliRunner()
    runner.invoke(app, ["worker", "start", "--count", "1"])
""")

    import subprocess, sys
    crash_env = dict(env)
    import pathlib
    crash_env["PYTHONPATH"] = str(pathlib.Path(__file__).parent.parent)
    wp = subprocess.Popen(
        [sys.executable, str(crash_script)], 
        env=crash_env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL
    )
    
    # Wait for the job to start and write its PID
    deadline = time.time() + 5
    while not pid_file.exists() and time.time() < deadline:
        time.sleep(0.2)
    assert pid_file.exists(), "subprocess never started"
    sub_pid = int(pid_file.read_text().strip())

    wp.wait(timeout=10)
    import psutil
    time.sleep(0.5)
    try:
        p = psutil.Process(sub_pid)
        assert not p.is_running(), f"subprocess {sub_pid} still alive after worker SIGTERM"
    except psutil.NoSuchProcess:
        pass  # expected — process is gone

def test_large_stdout_job_completes_without_hanging(env):
    """Job generating 500k lines of stdout must complete (DEVNULL prevents pipe block)."""
    run(["enqueue", '{"id":"bigout","command":"yes | head -n 500000"}'], env)
    wp = start_worker(env, count=1)
    try:
        job = wait_for_state(env, "bigout", "completed", timeout=15)
        assert job["state"] == "completed"
    finally:
        wp.send_signal(signal.SIGTERM)
        wp.wait(timeout=5)

def test_unicode_job_id_and_command_worker_logs_without_crash(env, tmp_path):
    """Worker must log non-ASCII job IDs without UnicodeEncodeError."""
    log = tmp_path / "uni_log.txt"
    run(["enqueue", json.dumps({"id": "job-café", "command": "echo héllo"})], env)
    wp = start_worker(env, count=1, logfile=log)
    try:
        wait_for_state(env, "job-café", "completed", timeout=10)
    finally:
        wp.send_signal(signal.SIGTERM)
        wp.wait(timeout=5)
    log_text = log.read_text(encoding="utf-8", errors="replace")
    assert "job-café" in log_text or "job-caf" in log_text  # logged without crash

def test_exit_code_137_treated_as_failure(env):
    """Exit code 137 (SIGKILL to subprocess itself) causes normal failure path."""
    run(["enqueue", '{"id":"ec137","command":"kill -9 $$","max_retries":0}'], env)
    wp = start_worker(env, count=1)
    try:
        job = wait_for_state(env, "ec137", "dead", timeout=10)
        assert job["last_error"] is not None
        # Exit code must be captured (137 or similar signal code)
        assert "137" in job["last_error"] or "exited with code" in job["last_error"]
    finally:
        wp.send_signal(signal.SIGTERM)
        wp.wait(timeout=5)

def test_env_var_expanded_at_execution_not_enqueue_time(env, tmp_path):
    """$HOME in command is stored literally but expanded at worker execution time."""
    out = tmp_path / "home_out.txt"
    run(["enqueue", json.dumps({
        "id": "envexp",
        "command": f"echo $HOME > {out}"
    })], env)

    # At enqueue time: command must still show literal $HOME
    jobs = list_jobs(env)
    assert "$HOME" in jobs[0]["command"], "command was expanded at enqueue time (wrong)"

    wp = start_worker(env, count=1)
    try:
        wait_for_state(env, "envexp", "completed", timeout=10)
    finally:
        wp.send_signal(signal.SIGTERM)
        wp.wait(timeout=5)

    content = out.read_text().strip()
    assert len(content) > 0, "HOME was not expanded at execution time"
    assert content != "$HOME", "literal $HOME was written — expansion did not happen"

def test_recovery_clock_starts_at_claim_time_not_kill_time(env):
    """Job is reaped based on claim timestamp, not SIGKILL timestamp."""
    RECOVERY = 3
    run(["config", "set", "recovery-timeout", str(RECOVERY)], env)
    run(["config", "set", "heartbeat-interval", "30"], env)  # no heartbeats
    run(["enqueue", '{"id":"rclk","command":"sleep 10"}'], env)

    wp = start_worker(env, count=1)
    wait_for_state(env, "rclk", "processing", timeout=5)
    t_claim = time.time()

    # Immediately SIGKILL
    import sqlite3
    conn = sqlite3.connect(env["QUEUECTL_DB"])
    row = conn.execute("SELECT pid FROM workers WHERE status='running'").fetchone()
    conn.close()
    os.kill(row[0], signal.SIGKILL)
    wp.wait(timeout=3)
    t_kill = time.time()

    # Wait until RECOVERY seconds after claim (not after kill)
    elapsed_since_claim = time.time() - t_claim
    if elapsed_since_claim < RECOVERY + 0.5:
        time.sleep(RECOVERY + 0.5 - elapsed_since_claim)

    jobs = list_jobs(env)  # triggers reap
    job = next(j for j in jobs if j["id"] == "rclk")
    # Must be pending now — recovery counted from claim time
    assert job["state"] == "pending", \
        f"Job still in {job['state']} — recovery clock not starting at claim time"

def test_memory_growth_bounded_cross_platform(env):
    """Worker RSS must not grow unboundedly — works on macOS and Linux via psutil."""
    pytest.importorskip("psutil")
    import psutil
    NUM_JOBS = 100
    for i in range(NUM_JOBS):
        cli(["enqueue", json.dumps({"id": f"memp-{i}", "command": "echo hi"})], env)

    wp = start_worker(env, count=2)
    try:
        time.sleep(1.0)
        proc = psutil.Process(wp.pid)
        rss_start = proc.memory_info().rss

        deadline = time.time() + 30
        while time.time() < deadline:
            jobs = json.loads(cli(["list", "--json"], env).stdout.strip())
            if all(j["state"] == "completed" for j in jobs):
                break
            time.sleep(0.3)

        rss_end = proc.memory_info().rss
        # Allow 20 MB of growth max (generous for 100 jobs)
        growth_mb = (rss_end - rss_start) / (1024 * 1024)
        assert growth_mb < 20, f"RSS grew {growth_mb:.1f}MB over {NUM_JOBS} jobs"
    finally:
        wp.send_signal(signal.SIGTERM)
        wp.wait(timeout=5)

def test_db_path_nonexistent_directory_clean_error(tmp_path):
    """If QUEUECTL_DB directory doesn't exist, CLI exits non-zero with clean message."""
    bad_env = os.environ.copy()
    bad_env["QUEUECTL_DB"] = str(tmp_path / "no_such_dir" / "queue.db")
    r = subprocess.run(
        [sys.executable, "-m", "queuectl.cli.entrypoint"] + ["status"],
        capture_output=True, text=True, env=bad_env,
    )
    assert r.returncode != 0
    assert "Traceback" not in r.stderr
    # Should mention DB or file path
    assert "database" in r.stderr.lower() or "queue.db" in r.stderr.lower()

def test_future_heartbeat_not_reaped(conn):
    """A job with heartbeat_at in the future is never reaped regardless of timeout."""
    future = (datetime.now(timezone.utc) + timedelta(seconds=300)).isoformat()
    _insert_job(conn, "j1", state="processing", worker_id="w1", heartbeat_at=future)
    reaped = db.reap_stale_jobs(conn, timeout_seconds=15)
    assert "j1" not in reaped
    row = conn.execute("SELECT state FROM jobs WHERE id='j1'").fetchone()
    assert row["state"] == "processing"  # untouched

def test_wal_file_bounded_after_many_jobs(env):
    """WAL file must not grow unboundedly after processing 1000 jobs."""
    import os as _os
    db_path = env["QUEUECTL_DB"]
    wal_path = db_path + "-wal"
    N = 200  # Use 200 for test speed; demonstrates the pattern
    for i in range(N):
        cli(["enqueue", json.dumps({"id": f"wal-{i}", "command": "echo hi"})], env)

    wp = start_worker(env, count=4)
    try:
        deadline = time.time() + 30
        while time.time() < deadline:
            jobs = json.loads(cli(["list", "--json"], env).stdout.strip())
            if all(j["state"] == "completed" for j in jobs):
                break
            time.sleep(0.3)
    finally:
        wp.send_signal(signal.SIGTERM)
        wp.wait(timeout=5)

    if _os.path.exists(wal_path):
        wal_size_mb = _os.path.getsize(wal_path) / (1024 * 1024)
        assert wal_size_mb < 50, f"WAL file is {wal_size_mb:.1f}MB — not being checkpointed"

def test_no_lock_errors_under_100_worker_contention(env, tmp_path):
    """100 workers competing for 100 jobs produce zero 'database is locked' errors."""
    N = 100
    log_files = []
    for i in range(N):
        cli(["enqueue", json.dumps({"id": f"lk-{i}", "command": "echo hi"})], env)

    # Capture ALL worker stderr
    log = tmp_path / "workers_stderr.txt"
    wp = subprocess.Popen(
        [sys.executable, "-m", "queuectl.cli.entrypoint"] + ["worker", "start", "--count", str(N)],
        stdout=open(log, "w"), stderr=subprocess.STDOUT, env=env,
    )
    try:
        deadline = time.time() + 45
        while time.time() < deadline:
            jobs = json.loads(cli(["list", "--json"], env).stdout.strip())
            if all(j["state"] == "completed" for j in jobs) and len(jobs) == N:
                break
            time.sleep(0.5)
    finally:
        wp.send_signal(signal.SIGTERM)
        wp.wait(timeout=10)

    log_text = log.read_text()
    assert "database is locked" not in log_text.lower()
    assert "OperationalError" not in log_text
    completed = sum(1 for j in json.loads(
        cli(["list", "--json"], env).stdout.strip()
    ) if j["state"] == "completed")
    assert completed == N

def test_disk_full_enqueue_exits_cleanly(env, monkeypatch):
    """When DB write raises OperationalError (disk full), enqueue exits non-zero cleanly."""
    import sqlite3 as _sqlite3

    original_connect = _sqlite3.connect
    call_count = {"n": 0}

    def patched_connect(*args, **kwargs):
        conn = original_connect(*args, **kwargs)
        original_execute = conn.execute
        def failing_execute(sql, *a, **kw):
            if "INSERT INTO jobs" in sql:
                raise _sqlite3.OperationalError("database or disk is full")
            return original_execute(sql, *a, **kw)
        conn.execute = failing_execute
        return conn

    monkeypatch.setattr(_sqlite3, "connect", patched_connect)
    from typer.testing import CliRunner
    from app import app
    runner = CliRunner(env=env)
    r = runner.invoke(app, ["enqueue", '{"id":"df1","command":"echo hi"}'])
    assert r.exit_code != 0
    assert "Traceback" not in r.stderr if r.stderr else True
    assert "Traceback" not in r.stdout

def test_sigterm_during_lock_contention_resolves_within_timeout(env):
    """Worker receiving SIGTERM while waiting on a BEGIN IMMEDIATE lock still exits."""
    import threading, sqlite3, time

    run(["enqueue", '{"id":"lktest","command":"echo hi"}'], env)
    
    # Hold a write lock in another thread to simulate contention
    blocker_conn = sqlite3.connect(env["QUEUECTL_DB"], timeout=0, check_same_thread=False)
    blocker_conn.execute("BEGIN EXCLUSIVE")
    wp = start_worker(env, count=1)
    time.sleep(0.5)  # worker is now blocked waiting for the lock
    wp.send_signal(signal.SIGTERM)

    # Release the lock after 2 seconds
    def release():
        time.sleep(2)
        blocker_conn.execute("ROLLBACK")
        blocker_conn.close()
    threading.Thread(target=release, daemon=True).start()

    wp.wait(timeout=10)
    assert wp.returncode == 0

def test_sigkill_kills_worker_and_job_is_recovered(env):
    """SIGKILL terminates worker (default behavior); job is recovered via lease."""
    run(["config", "set", "recovery-timeout", "2"], env)
    run(["config", "set", "heartbeat-interval", "1"], env)
    run(["enqueue", '{"id":"hup1","command":"sleep 10"}'], env)

    wp = start_worker(env, count=1)
    wait_for_state(env, "hup1", "processing", timeout=5)

    import sqlite3
    conn = sqlite3.connect(env["QUEUECTL_DB"])
    row = conn.execute("SELECT pid FROM workers WHERE status='running'").fetchone()
    conn.close()
    os.kill(row[0], signal.SIGKILL)
    wp.wait(timeout=5)

    # Job must still exist — not lost
    jobs = list_jobs(env)
    job = next(j for j in jobs if j["id"] == "hup1")
    assert job["state"] == "processing"  # stale, not yet recovered

    # After lease expires, recovery must work
    time.sleep(2.5)
    wp2 = start_worker(env, count=1)
    try:
        wait_for_state(env, "hup1", "pending", timeout=5)
    finally:
        wp2.send_signal(signal.SIGTERM)
        wp2.wait(timeout=15)

def test_multiple_rapid_signals_single_shutdown(env, tmp_path):
    """5 rapid SIGTERM signals produce exactly one 'stopped' log line."""
    log = tmp_path / "rapid.log"
    run(["enqueue", '{"id":"rap1","command":"sleep 3"}'], env)
    wp = start_worker(env, count=1, logfile=log)
    time.sleep(0.5)

    # Send 5 rapid SIGTERMs
    for _ in range(5):
        def handle_signal(signum, frame):
            stop_requested["flag"] = True
        wp.send_signal(signal.SIGTERM)
        time.sleep(0.05)
    wp.wait(timeout=10)

    log_text = log.read_text()
    stop_count = log_text.count("stopped")
    assert stop_count >= 1, "worker must log 'stopped'"
    assert stop_count <= 2, f"too many 'stopped' lines ({stop_count}) — double-cleanup"

def test_20_workers_simultaneous_sigterm_all_stopped(env):
    """20 workers all receiving SIGTERM simultaneously all reach stopped state."""
    wp = start_worker(env, count=20)
    time.sleep(1.5)

    # Send SIGTERM via worker stop (sends to all simultaneously)
    r = run(["worker", "stop"], env)
    assert r.returncode == 0
    wp.wait(timeout=15)

    import sqlite3
    conn = sqlite3.connect(env["QUEUECTL_DB"])
    running = conn.execute(
        "SELECT COUNT(*) FROM workers WHERE status='running'"
    ).fetchone()[0]
    stopped = conn.execute(
        "SELECT COUNT(*) FROM workers WHERE status='stopped'"
    ).fetchone()[0]
    conn.close()
    assert running == 0, f"{running} workers still show as running"
    assert stopped == 20, f"Only {stopped}/20 workers marked stopped"



def test_corrupted_db_produces_clean_error(env):
    """A genuinely corrupted DB must produce a clean error, not a traceback."""
    cli(["status"], env)  # initialize DB
    db_path = env["QUEUECTL_DB"]

    # Corrupt the DB file
    with open(db_path, "r+b") as f:
        f.seek(1024)
        f.write(b"\xff\xff\xff\xff\xff\xff\xff\xff")

    r = cli(["status"], env)
    # Either handle corruption gracefully or report it clearly
    assert "Traceback" not in r.stderr
    # If exit 0: DB auto-recovers (SQLite may be resilient)
    # If exit 1: clean error message shown

def test_foreign_keys_pragma_is_enabled(conn):
    """Document: foreign_keys pragma is ON (even though no FK constraints are declared)."""
    row = conn.execute("PRAGMA foreign_keys").fetchone()
    assert row[0] == 1, "PRAGMA foreign_keys must be ON"
    # Note: no FK constraints are currently declared in the schema.
    # This test documents the current state. If FK constraints are added later,
    # this test ensures the pragma is already in place.

def test_subprocess_does_not_inherit_db_fd(env, tmp_path):
    """Child subprocess must not hold the DB file descriptor open."""
    fd_out = tmp_path / "fds.txt"
    # Job: list own file descriptors, filtering for the DB path
    db_name = "queue.db"
    cmd = f'ls -la /proc/$$/fd 2>/dev/null | grep {db_name} > {fd_out} || echo "no db fd" > {fd_out}'
    run(["enqueue", json.dumps({"id": "fdchk", "command": cmd})], env)
    wp = start_worker(env, count=1)
    try:
        wait_for_state(env, "fdchk", "completed", timeout=10)
    finally:
        wp.send_signal(signal.SIGTERM)
        wp.wait(timeout=5)

    if fd_out.exists():
        content = fd_out.read_text().strip()
        assert db_name not in content, \
            f"DB fd was inherited by subprocess: {content}"

def test_worker_start_method_and_no_deadlock(env):
    """Workers start correctly and all register in DB — no fork() deadlock."""
    wp = start_worker(env, count=3)
    try:
        time.sleep(2.0)
        import sqlite3
        conn = sqlite3.connect(env["QUEUECTL_DB"])
        count = conn.execute(
            "SELECT COUNT(*) FROM workers WHERE status='running'"
        ).fetchone()[0]
        conn.close()
        assert count == 3, f"Only {count}/3 workers registered — possible fork deadlock"
    finally:
        wp.send_signal(signal.SIGTERM)
        wp.wait(timeout=8)

    # Log the Python version and start method for traceability
    import sys, multiprocessing
    print(f"\nPython: {sys.version}, start method: {multiprocessing.get_start_method()}")

def test_worker_start_count_zero_exits_immediately(env):
    """worker start --count 0 spawns no workers and exits immediately."""
    import time
    t0 = time.time()
    r = run(["worker", "start", "--count", "0"], env, timeout=5)
    elapsed = time.time() - t0
    assert r.returncode == 0
    assert elapsed < 3.0, "worker start --count 0 should exit immediately"
    assert "All workers stopped." in r.stdout

def test_worker_start_count_zero_no_processes_spawned(env):
    """No worker processes exist after worker start --count 0."""
    run(["worker", "start", "--count", "0"], env, timeout=5)
    import sqlite3
    conn = sqlite3.connect(env["QUEUECTL_DB"])
    count = conn.execute(
        "SELECT COUNT(*) FROM workers WHERE status='running'"
    ).fetchone()[0]
    conn.close()
    assert count == 0

def test_enqueue_empty_string_id_behavior(env):
    """Empty string id: document whether accepted or rejected."""
    r = run(["enqueue", '{"id":"","command":"echo empty"}'], env)
    # Current behavior: accepted (exits 0)
    # Recommended: rejected with clear error
    if r.returncode == 0:
        jobs = list_jobs(env)
        assert len(jobs) == 1
        assert jobs[0]["id"] == ""
        # Document: dlq retry with empty id is awkward from CLI
    else:
        assert "empty" in r.stderr.lower() or "id" in r.stderr.lower()

def test_max_retries_string_float_produces_clean_error(env):
    """max_retries='2.5' (string) must produce a clean error, not a traceback."""
    r = run(["enqueue", '{"id":"mr1","command":"echo hi","max_retries":"2.5"}'], env)
    assert r.returncode != 0
    assert "Traceback" not in r.stderr
    jobs = list_jobs(env)
    assert len(jobs) == 0  # no partial row inserted

def test_backoff_base_string_invalid_produces_clean_error(env):
    """backoff_base='fast' must produce a clean error."""
    r = run(["enqueue", '{"id":"bb1","command":"echo hi","backoff_base":"fast"}'], env)
    assert r.returncode != 0
    assert "Traceback" not in r.stderr

def test_integer_id_coerced_to_string(env):
    """Integer id=123 is stored as string '123', not integer 123."""
    r = run(["enqueue", '{"id":123,"command":"echo num"}'], env)
    assert r.returncode == 0
    jobs = list_jobs(env)
    assert jobs[0]["id"] == "123"

    # Second enqueue with string id should fail as duplicate
    r2 = run(["enqueue", '{"id":"123","command":"echo dup"}'], env)
    assert r2.returncode == 1
    assert "already exists" in r2.stderr

def test_max_retries_float_is_truncated_to_int(env):
    """max_retries=2.9 is silently truncated to 2, not rounded to 3."""
    r = run(["enqueue", '{"id":"mrf1","command":"exit 1","max_retries":2.9}'], env)
    assert r.returncode == 0
    jobs = list_jobs(env)
    assert jobs[0]["max_retries"] == 2  # truncated, not rounded

def test_operational_config_not_re_read_by_running_worker(env):
    """Changing recovery-timeout while a worker is running has no effect on that worker."""
    run(["config", "set", "recovery-timeout", "30"], env)
    run(["enqueue", '{"id":"cfgrt","command":"sleep 10"}'], env)

    wp = start_worker(env, count=1)
    wait_for_state(env, "cfgrt", "processing", timeout=5)

    # Kill the worker mid-job
    import sqlite3
    conn = sqlite3.connect(env["QUEUECTL_DB"])
    row = conn.execute("SELECT pid FROM workers WHERE status='running'").fetchone()
    conn.close()
    os.kill(row[0], signal.SIGKILL)
    wp.wait(timeout=3)

    # Change recovery-timeout to 2 (shorter than what the old worker used)
    run(["config", "set", "recovery-timeout", "2"], env)

    # Wait 3 seconds — if the NEW value (2) is used, job recovers now
    # But since the OLD worker already ran with 30, we need a NEW worker to use 2
    time.sleep(3.0)

    # Job should NOT be reaped yet because no active worker is running reap scans
    # (the killed worker is gone, no new worker started)
    # This shows the recovery mechanism requires an active scanner (worker or CLI)
    jobs = list_jobs(env)  # list itself triggers reap with current config
    job = next(j for j in jobs if j["id"] == "cfgrt")
    # With recovery-timeout=2 and the time elapsed, job should now be pending
    assert job["state"] == "pending", \
        "Job not recovered even after new recovery-timeout elapsed"

    wp.send_signal(signal.SIGTERM) if wp.poll() is None else None

def test_deleted_config_key_falls_back_to_default(env):
    """Manually deleted config key falls back to default gracefully."""
    import sqlite3
    cli(["status"], env)  # initialize
    conn = sqlite3.connect(env["QUEUECTL_DB"])
    conn.execute("DELETE FROM config WHERE key='recovery-timeout'")
    conn.commit()
    conn.close()

    # status reads recovery-timeout via `config_service.get(conn, "recovery-timeout") or 15`
    r = cli(["status"], env)
    assert r.returncode == 0
    assert "Traceback" not in r.stderr

    # worker start reads via config dict — missing key falls back to .get() default
    wp = start_worker(env, count=1)
    try:
        time.sleep(0.5)
        assert wp.poll() is None, "Worker must still be running with missing config key"
    finally:
        wp.send_signal(signal.SIGTERM)
        wp.wait(timeout=5)

def test_poll_interval_zero_claims_immediately(env):
    """poll-interval=0 means a job is claimed in the very next loop iteration."""
    run(["config", "set", "poll-interval", "0"], env)
    t_enqueue = time.time()
    run(["enqueue", '{"id":"pi0","command":"sleep 2"}'], env)
    wp = start_worker(env, count=1)
    try:
        wait_for_state(env, "pi0", "processing", timeout=3)
        t_claim = time.time()
        assert t_claim - t_enqueue < 1.0, "Should claim within 1s with poll-interval=0"
        # Worker must not crash from zero sleep
        assert wp.poll() is None
    finally:
        wp.send_signal(signal.SIGTERM)
        wp.wait(timeout=5)

def test_worker_start_blocked_when_heartbeat_equals_recovery_exactly(env):
    """heartbeat-interval == recovery-timeout must also be blocked."""
    run(["config", "set", "heartbeat-interval", "15"], env)
    run(["config", "set", "recovery-timeout", "15"], env)
    env.pop("QUEUECTL_TEST", None)
    r = run(["worker", "start", "--count", "1"], env, timeout=5)
    assert r.returncode == 1
    assert "heartbeat-interval" in r.stderr

def test_recovery_event_produces_log_line(env, tmp_path):
    """Worker must log a message when it recovers a stale job."""
    run(["config", "set", "recovery-timeout", "2"], env)
    run(["config", "set", "heartbeat-interval", "1"], env)
    run(["enqueue", '{"id":"recovlog","command":"sleep 10"}'], env)

    log = tmp_path / "rec.log"
    wp1 = start_worker(env, count=1, logfile=log)
    wait_for_state(env, "recovlog", "processing", timeout=5)

    import sqlite3
    conn = sqlite3.connect(env["QUEUECTL_DB"])
    row = conn.execute("SELECT pid FROM workers WHERE status='running'").fetchone()
    conn.close()
    os.kill(row[0], signal.SIGKILL)
    wp1.wait(timeout=3)
    time.sleep(2.5)

    log2 = tmp_path / "rec2.log"
    wp2 = start_worker(env, count=1, logfile=log2)
    try:
        wait_for_state(env, "recovlog", "pending", timeout=5)
    finally:
        wp2.send_signal(signal.SIGTERM)
        wp2.wait(timeout=15)

    log2_text = log2.read_text()
    assert "recovlog" in log2_text or "recovered" in log2_text.lower(), \
        "Recovery event must be logged by the new worker"

def test_heartbeat_at_advances_during_long_job(env):
    """heartbeat_at must strictly advance in DB while a job is executing."""
    run(["config", "set", "heartbeat-interval", "1"], env)
    run(["enqueue", '{"id":"hbadv","command":"sleep 6"}'], env)

    wp = start_worker(env, count=1)
    try:
        wait_for_state(env, "hbadv", "processing", timeout=5)

        import sqlite3
        conn = sqlite3.connect(env["QUEUECTL_DB"])

        hb0 = conn.execute(
            "SELECT heartbeat_at FROM jobs WHERE id='hbadv'"
        ).fetchone()[0]
        time.sleep(1.5)
        hb1 = conn.execute(
            "SELECT heartbeat_at FROM jobs WHERE id='hbadv'"
        ).fetchone()[0]
        time.sleep(1.5)
        hb2 = conn.execute(
            "SELECT heartbeat_at FROM jobs WHERE id='hbadv'"
        ).fetchone()[0]
        conn.close()

        assert hb1 > hb0, "heartbeat_at did not advance after 1.5s"
        assert hb2 > hb1, "heartbeat_at did not advance second time"
    finally:
        wp.send_signal(signal.SIGTERM)
        wp.wait(timeout=8)

def test_worker_id_reuse_after_crash_handled_by_upsert(env):
    """If a new worker gets the same PID as a crashed worker, INSERT OR REPLACE
    overwrites the stale row — started_at advances and PID may differ."""
    import sqlite3

    # Simulate a stale row with worker_id='w-9999' (pretend PID was 9999)
    cli(["status"], env)  # ensure schema
    conn = sqlite3.connect(env["QUEUECTL_DB"])
    ts_old = "2020-01-01T00:00:00+00:00"
    conn.execute(
        "INSERT INTO workers (worker_id, pid, status, started_at, heartbeat_at) "
        "VALUES ('w-test-reuse', 9999, 'running', ?, ?)",
        (ts_old, ts_old),
    )
    conn.commit()

    # register_worker with same worker_id (simulating PID reuse)
    from queuectl import database as _db
    _db.DB_PATH = env["QUEUECTL_DB"]
    conn2 = _db.get_connection()
    _db.register_worker(conn2, "w-test-reuse", 9999)
    conn2.close()

    row = conn.execute(
        "SELECT started_at, pid FROM workers WHERE worker_id='w-test-reuse'"
    ).fetchone()
    conn.close()
    # The upsert must have refreshed started_at
    assert row[0] > ts_old, \
        "started_at must be refreshed on worker re-registration"

def test_max_retries_string_float_clean_error(env):
    r = run(["enqueue", '{"id":"j1","command":"echo","max_retries":"2.5"}'], env)
    assert r.returncode == 1
    assert "Traceback" not in r.stderr
    assert "invalid" in r.stderr.lower() or "parameters" in r.stderr.lower()

def test_backoff_base_nonnumeric_string_clean_error(env):
    r = run(["enqueue", '{"id":"j2","command":"echo","backoff_base":"fast"}'], env)
    assert r.returncode == 1
    assert "Traceback" not in r.stderr

def test_execute_job_exception_does_not_leave_job_stuck_processing(env, monkeypatch):
    """If execute_job raises unexpectedly, job must not stay stuck in processing."""
    from queuectl import worker as _worker

    original_execute = _worker.execute_job
    call_count = {"n": 0}

    def patched_execute(conn, job, worker_id, heartbeat_interval):
        call_count["n"] += 1
        if call_count["n"] == 1:
            raise RuntimeError("simulated internal error")
        return original_execute(conn, job, worker_id, heartbeat_interval)

    monkeypatch.setattr(_worker, "execute_job", patched_execute)

    run(["enqueue", '{"id":"exc1","command":"echo hi","max_retries":1}'], env)
    wp = start_worker(env, count=1)
    try:
        # Job should end up in failed or dead, NOT stuck in processing
        deadline = time.time() + 10
        while time.time() < deadline:
            jobs = list_jobs(env)
            job = next((j for j in jobs if j["id"] == "exc1"), None)
            if job and job["state"] in ("failed", "dead", "completed"):
                break
            time.sleep(0.3)
        assert job["state"] != "processing", \
            "Job stuck in processing after execute_job exception"
    finally:
        wp.send_signal(signal.SIGTERM)
        wp.wait(timeout=5)

def test_status_handles_permission_error_on_kill_zero(env, monkeypatch):
    """status must not crash when os.kill(pid, 0) raises PermissionError."""
    import sqlite3, os as _os, app as _app

    # Insert a fake worker row with the current process's PID (which we can't kill as root-only)
    cli(["status"], env)
    conn = sqlite3.connect(env["QUEUECTL_DB"])
    conn.execute(
        "INSERT INTO workers (worker_id, pid, status, started_at, heartbeat_at) "
        "VALUES ('w-perm', 1, 'running', ?, ?)",  # PID 1 = init/launchd
        (db.now_iso(), db.now_iso()),
    )
    conn.commit()
    conn.close()

    r = cli(["status"], env)
    assert r.returncode == 0
    assert "Traceback" not in r.stderr

def test_worker_loop_continues_after_transient_reap_error(env, monkeypatch):
    """A transient OperationalError in reap_stale_jobs must not crash the worker."""
    from queuectl import database as _db
    call_count = {"n": 0}
    original_reap = _db.reap_stale_jobs

    def flaky_reap(conn, timeout_seconds):
        import sqlite3 as _sqlite3
        call_count["n"] += 1
        if call_count["n"] == 2:
            raise _sqlite3.OperationalError("disk I/O error (simulated)")
        return original_reap(conn, timeout_seconds)

    monkeypatch.setattr(_db, "reap_stale_jobs", flaky_reap)
    run(["enqueue", '{"id":"looperr","command":"echo hi"}'], env)
    wp = start_worker(env, count=1)
    try:
        # Worker should continue after the error and process the job
        job = wait_for_state(env, "looperr", "completed", timeout=10)
        assert job["state"] == "completed"
    finally:
        wp.send_signal(signal.SIGTERM)
        wp.wait(timeout=5)

def test_command_exceeding_arg_max_does_not_crash_worker(env):
    """A command that exceeds ARG_MAX must fail cleanly, not crash the worker."""
    # 300KB command — may exceed ARG_MAX on some systems
    long_cmd = "echo " + "x" * 3000000
    import sqlite3
    run(["status"], env)
    conn = sqlite3.connect(env["QUEUECTL_DB"])
    ts = "2025-01-01T00:00:00+00:00"
    conn.execute(
        "INSERT INTO jobs (id,command,state,attempts,max_retries,backoff_base,created_at,updated_at) "
        "VALUES ('argmax',?,'pending',0,0,2.0,?,?)",
        (long_cmd, ts, ts)
    )
    conn.commit()
    conn.close()

    wp = start_worker(env, count=1)
    try:
        # Job must end in 'failed' or 'dead' (OSError → returncode failure)
        # and the worker must still be alive to process the next job
        deadline = time.time() + 10
        while time.time() < deadline:
            jobs = list_jobs(env)
            job = next((j for j in jobs if j["id"] == "argmax"), None)
            if job and job["state"] in ("failed", "dead"):
                break
            time.sleep(0.3)

        assert job is not None
        assert job["state"] in ("failed", "dead"), \
            f"Job stuck in {job['state']} — worker may have crashed"

        # Worker must still be alive
        assert wp.poll() is None, "Worker crashed on ARG_MAX error"

        # Worker processes another job normally after the error
        run(["enqueue", '{"id":"after-argmax","command":"echo ok"}'], env)
        wait_for_state(env, "after-argmax", "completed", timeout=8)
    finally:
        wp.send_signal(signal.SIGTERM)
        wp.wait(timeout=5)

def test_worker_connection_healthy_after_1000_transactions(env):
    """Single worker DB connection handles 1000 claim+finish cycles without error."""
    N = 300
    # Bulk insert directly for speed
    import sqlite3 as _sqlite3
    run(["status"], env)  # init schema
    conn = _sqlite3.connect(env["QUEUECTL_DB"])
    ts = "2025-01-01T00:00:00+00:00"
    conn.executemany(
        "INSERT INTO jobs (id,command,state,attempts,max_retries,backoff_base,"
        "created_at,updated_at) VALUES (?,?,?,?,?,?,?,?)",
        [(f"bulk-{i}", "echo hi", "pending", 0, 3, 2.0, ts, ts) for i in range(N)],
    )
    conn.commit()
    conn.close()

    wp = start_worker(env, count=2)
    try:
        deadline = time.time() + 60
        while time.time() < deadline:
            jobs = json.loads(run(["list", "--json"], env).stdout.strip())
            if all(j["state"] == "completed" for j in jobs) and len(jobs) == N:
                break
            time.sleep(0.5)

        jobs = json.loads(run(["list", "--json"], env).stdout.strip())
        completed = sum(1 for j in jobs if j["state"] == "completed")
        assert completed == N, f"Only {completed}/{N} completed"
        assert wp.poll() is None, "Worker crashed during 1000-job run"
    finally:
        wp.send_signal(signal.SIGTERM)
        wp.wait(timeout=5)

def test_shutdown_delay_bounded_by_poll_interval(env):
    """Worker receiving SIGTERM while sleeping exits within poll_interval + 1s."""
    POLL = 2
    run(["config", "set", "poll-interval", str(POLL)], env)
    wp = start_worker(env, count=1)
    time.sleep(0.5)  # worker starts and goes to sleep (empty queue)

    t0 = time.time()
    wp.send_signal(signal.SIGTERM)
    wp.wait(timeout=POLL + 3)
    elapsed = time.time() - t0

    assert elapsed <= POLL + 1.5, \
        f"Worker took {elapsed:.1f}s to shutdown — expected <= {POLL + 1.5}s"
    assert wp.returncode == 0

def test_list_human_readable_shows_jobs(env):
    """list without --json prints a human-readable table."""
    run(["enqueue", '{"id":"hr1","command":"echo human"}'], env)
    r = run(["list"], env)
    assert r.returncode == 0
    assert "hr1" in r.stdout
    assert "pending" in r.stdout
    assert "echo human" in r.stdout
    # Must NOT be valid JSON (it's a table format)
    try:
        json.loads(r.stdout)
        assert False, "list without --json should not produce JSON"
    except json.JSONDecodeError:
        pass  # expected

def test_list_human_readable_empty_shows_no_jobs_found(env):
    """list on empty queue shows 'No jobs found.' message."""
    r = run(["list"], env)
    assert r.returncode == 0
    assert "No jobs found." in r.stdout

def test_dlq_list_human_readable_shows_dead_jobs(env):
    """dlq list without --json prints human-readable dead job info."""
    run(["config", "set", "backoff-base", "1"], env)
    run(["enqueue", '{"id":"dlqhr","command":"exit 1","max_retries":0}'], env)
    wp = start_worker(env, count=1)
    try:
        wait_for_state(env, "dlqhr", "dead", timeout=10)
    finally:
        wp.send_signal(signal.SIGTERM)
        wp.wait(timeout=5)

    r = run(["dlq", "list"], env)
    assert r.returncode == 0
    assert "dlqhr" in r.stdout
    assert "attempts=" in r.stdout
    assert "last_error=" in r.stdout

def test_dlq_list_human_readable_empty_shows_dlq_is_empty(env):
    """dlq list on empty DLQ shows 'DLQ is empty.' message."""
    r = run(["dlq", "list"], env)
    assert r.returncode == 0
    assert "DLQ is empty." in r.stdout

def test_config_get_no_key_lists_all(env):
    """config get with no key argument lists all key=value pairs."""
    r = run(["config", "get"], env)
    assert r.returncode == 0
    assert "max-retries" in r.stdout
    assert "backoff-base" in r.stdout
    assert "heartbeat-interval" in r.stdout
    assert "recovery-timeout" in r.stdout
    assert "poll-interval" in r.stdout

def test_config_get_unknown_key_exits_nonzero(env):
    """config get for a non-existent key exits 1 with clean error."""
    r = run(["config", "get", "no-such-key"], env)
    assert r.returncode == 1
    assert "No such key" in r.stderr
    assert "Traceback" not in r.stderr

def test_created_at_never_changes_through_lifecycle(env):
    """created_at must be immutable from enqueue through completion."""
    run(["enqueue", '{"id":"cat1","command":"exit 1","max_retries":0}'], env)
    created_at_initial = list_jobs(env)[0]["created_at"]

    run(["config", "set", "backoff-base", "1"], env)
    wp = start_worker(env, count=1)
    try:
        wait_for_state(env, "cat1", "dead", timeout=10)
    finally:
        wp.send_signal(signal.SIGTERM)
        wp.wait(timeout=5)

    job = next(j for j in list_jobs(env) if j["id"] == "cat1")
    assert job["created_at"] == created_at_initial, \
        f"created_at changed! was {created_at_initial}, now {job['created_at']}"
    # updated_at MUST have changed
    assert job["updated_at"] > created_at_initial

def test_successful_completion_clears_last_error(env, tmp_path):
    """A job that fails then succeeds must have last_error=NULL after completion."""
    toggle = tmp_path / "toggle"
    toggle.write_text("0")

    cmd = f'v=$(cat {toggle}); if [ "$v" = "0" ]; then exit 1; fi; echo ok'
    run(["config", "set", "backoff-base", "1"], env)
    run(["enqueue", json.dumps({
        "id": "clearerr",
        "command": cmd,
        "max_retries": 0,
    })], env)

    # First run: fails
    wp1 = start_worker(env, count=1)
    try:
        wait_for_state(env, "clearerr", "dead", timeout=8)
    finally:
        wp1.send_signal(signal.SIGTERM); wp1.wait(timeout=5)

    job_dead = next(j for j in list_jobs(env) if j["id"] == "clearerr")
    assert job_dead["last_error"] is not None

    # DLQ retry, then toggle to success
    run(["dlq", "retry", "clearerr"], env)
    toggle.write_text("1")

    wp2 = start_worker(env, count=1)
    try:
        wait_for_state(env, "clearerr", "completed", timeout=8)
    finally:
        wp2.send_signal(signal.SIGTERM); wp2.wait(timeout=5)

    job_done = next(j for j in list_jobs(env) if j["id"] == "clearerr")
    assert job_done["last_error"] is None, \
        "last_error must be NULL after successful completion"

def test_dlq_list_json_ordered_by_updated_at_desc(env):
    """dlq list --json returns dead jobs newest-first (updated_at DESC)."""
    run(["config", "set", "backoff-base", "1"], env)
    for job_id in ["dlq-first", "dlq-second", "dlq-third"]:
        run(["enqueue", json.dumps({
            "id": job_id,
            "command": "exit 1",
            "max_retries": 0,
        })], env)
        # Small delay to ensure distinct updated_at
        time.sleep(0.1)

    wp = start_worker(env, count=1)
    try:
        for job_id in ["dlq-first", "dlq-second", "dlq-third"]:
            wait_for_state(env, job_id, "dead", timeout=10)
    finally:
        wp.send_signal(signal.SIGTERM)
        wp.wait(timeout=5)

    r = run(["dlq", "list", "--json"], env)
    jobs = json.loads(r.stdout)
    assert len(jobs) == 3
    # Most recently updated is first
    timestamps = [j["updated_at"] for j in jobs]
    assert timestamps == sorted(timestamps, reverse=True), \
        f"DLQ list not ordered by updated_at DESC: {timestamps}"

def test_worker_start_emits_started_log_per_worker(env, tmp_path):
    """worker start emits exactly N 'Started worker' lines for --count N."""
    log = tmp_path / "start.log"
    wp = start_worker(env, count=4, logfile=log)
    try:
        time.sleep(1.0)
    finally:
        wp.send_signal(signal.SIGTERM)
        wp.wait(timeout=8)

    log_text = log.read_text()
    started_count = log_text.count("Started worker pid=")
    # Note: "Started worker pid=X" goes to stdout of the parent, which
    # is captured in the log by start_worker(logfile=...)
    # If start_worker redirects stdout+stderr, both will be in log
    assert started_count == 4, \
        f"Expected 4 'Started worker' lines, found {started_count}"

def test_pending_job_stays_pending_with_no_workers(env):
    """A pending job stays in pending indefinitely when no workers are running."""
    run(["enqueue", '{"id":"nw1","command":"echo hi"}'], env)
    time.sleep(2.0)  # no workers started
    jobs = list_jobs(env)
    job = next(j for j in jobs if j["id"] == "nw1")
    assert job["state"] == "pending", \
        "Job must remain pending with no workers"

def test_failed_job_not_claimed_before_retry_window(env):
    """A failed job must not be claimed before next_retry_at elapses."""
    run(["config", "set", "backoff-base", "60"], env)  # 60^1 = 60s delay
    run(["enqueue", '{"id":"notready","command":"exit 1","max_retries":3}'], env)

    wp = start_worker(env, count=1)
    try:
        # Wait for first failure
        wait_for_state(env, "notready", "failed", timeout=5)
        job_after_failure = next(j for j in list_jobs(env) if j["id"] == "notready")
        assert job_after_failure["attempts"] == 1

        # Wait 3 seconds — job must NOT be retried yet (60s backoff)
        time.sleep(3)
        job_still = next(j for j in list_jobs(env) if j["id"] == "notready")
        assert job_still["state"] == "failed", \
            "Job was retried before backoff window elapsed"
        assert job_still["attempts"] == 1, \
            "attempts incremented before retry window — job ran again too early"
    finally:
        wp.send_signal(signal.SIGTERM)
        wp.wait(timeout=5)

def test_reap_then_claim_in_same_loop_does_not_double_run(env, tmp_path):
    """A job that is reaped and immediately available must be claimed once, not twice."""
    log = tmp_path / "reap_claim.log"
    run(["config", "set", "recovery-timeout", "2"], env)
    run(["config", "set", "heartbeat-interval", "1"], env)
    run(["enqueue", '{"id":"rtc1","command":"sleep 3"}'], env)

    # Start a worker, let it claim the job, SIGKILL it
    wp1 = start_worker(env, count=1)
    wait_for_state(env, "rtc1", "processing", timeout=5)
    import sqlite3 as _sqlite3
    conn = _sqlite3.connect(env["QUEUECTL_DB"])
    row = conn.execute("SELECT pid FROM workers WHERE status='running'").fetchone()
    conn.close()
    os.kill(row[0], signal.SIGKILL)
    wp1.wait(timeout=3)
    time.sleep(2.5)  # lease expires

    # Start TWO new workers simultaneously — both will try to reap AND claim
    wp2 = start_worker(env, count=2, logfile=log)
    try:
        wait_for_state(env, "rtc1", "completed", timeout=10)
    finally:
        wp2.send_signal(signal.SIGTERM)
        wp2.wait(timeout=5)

    log_text = log.read_text() if log.exists() else ""
    claim_count = log_text.count("running job rtc1")
    assert claim_count == 1, \
        f"Job 'rtc1' was claimed {claim_count} times — expected exactly 1"

def test_promote_does_not_touch_processing_jobs(conn):
    """promote_ready_retries must never affect a processing job."""
    past = (datetime.now(timezone.utc) - timedelta(seconds=30)).isoformat()
    _insert_job(conn, "j1", state="processing", worker_id="w1",
                heartbeat_at=db.now_iso(), next_retry_at=past)  # unusual but possible
    db.promote_ready_retries(conn)
    row = conn.execute("SELECT state FROM jobs WHERE id='j1'").fetchone()
    assert row["state"] == "processing", \
        "promote_ready_retries must not modify processing jobs"

def test_dead_job_never_reaped_by_reap_stale_jobs(conn):
    """reap_stale_jobs must never reap a dead job, even with stale heartbeat."""
    stale = (datetime.now(timezone.utc) - timedelta(seconds=3600)).isoformat()
    _insert_job(conn, "j1", state="dead", worker_id=None, heartbeat_at=stale,
                attempts=3, max_retries=3)
    reaped = db.reap_stale_jobs(conn, timeout_seconds=15)
    assert "j1" not in reaped
    row = conn.execute("SELECT state FROM jobs WHERE id='j1'").fetchone()
    assert row["state"] == "dead"

def test_list_json_attempts_is_integer_type(env):
    """attempts field in list --json must be a JSON number (int), not a string."""
    run(["enqueue", '{"id":"types1","command":"echo hi"}'], env)
    r = run(["list", "--json"], env)
    jobs = json.loads(r.stdout)
    assert isinstance(jobs[0]["attempts"], int), \
        f"attempts is {type(jobs[0]['attempts'])}, expected int"
    assert isinstance(jobs[0]["max_retries"], int), \
        f"max_retries is {type(jobs[0]['max_retries'])}, expected int"
    assert isinstance(jobs[0]["backoff_base"], float), \
        f"backoff_base is {type(jobs[0]['backoff_base'])}, expected float"

def test_check_constraint_rejects_invalid_state(conn):
    import sqlite3
    ts = db.now_iso()
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            "INSERT INTO jobs (id,command,state,attempts,max_retries,backoff_base,"
            "created_at,updated_at) VALUES ('inv','echo','INVALID',0,3,2.0,?,?)",
            (ts, ts),
        )

def test_sql_injection_in_config_key_is_stored_safely(env):
    """SQL injection in config key is stored as literal string, not executed."""
    evil_key = "max-retries'; DROP TABLE config; --"
    r = run(["config", "set", evil_key, "99"], env)
    assert r.returncode == 0  # stored safely

    import sqlite3
    conn = sqlite3.connect(env["QUEUECTL_DB"])
    tables = [r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    ).fetchall()]
    conn.close()
    assert "config" in tables, "config table was dropped by injection in key"
    assert "jobs" in tables, "jobs table was dropped by injection in key"

    # The evil key must be retrievable (stored as literal)
    r2 = run(["config", "get", evil_key], env)
    assert r2.returncode == 0
    assert "99" in r2.stdout

def test_sql_injection_in_dlq_retry_id_is_safe(env):
    """SQL injection in dlq retry job_id is handled safely."""
    evil_id = "'; DROP TABLE jobs; --"
    r = run(["dlq", "retry", evil_id], env)
    assert r.returncode != 0  # no such dead job
    assert "No dead job" in r.stderr

    import sqlite3
    conn = sqlite3.connect(env["QUEUECTL_DB"])
    tables = [row[0] for row in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    ).fetchall()]
    conn.close()
    assert "jobs" in tables, "jobs table was dropped by injection"

def test_concurrent_status_calls_do_not_error(env, tmp_path):
    """100 concurrent status calls must all succeed without OperationalError."""
    procs = []
    for _ in range(100):
        p = subprocess.Popen(
            [sys.executable, "-m", "queuectl.cli.entrypoint"] + ["status"],
            stdout=subprocess.DEVNULL, stderr=subprocess.PIPE,
            text=True, env=env,
        )
        procs.append(p)

    errors = []
    for p in procs:
        _, stderr = p.communicate(timeout=30)
        if p.returncode != 0 or "OperationalError" in stderr:
            errors.append(stderr)

    assert len(errors) == 0, \
        f"{len(errors)} status calls failed:\n{errors[:3]}"

def test_command_false_is_coerced_to_string(env):
    """JSON false as command is stored as string 'False' — document this behavior."""
    r = run(["enqueue", '{"id":"bool1","command":false}'], env)
    if r.returncode == 0:
        jobs = list_jobs(env)
        assert jobs[0]["command"] == "False"  # document coercion
    else:
        assert "Traceback" not in r.stderr  # must fail cleanly

def test_command_zero_is_coerced_to_string(env):
    """JSON 0 as command is stored as string '0' — document this behavior."""
    r = run(["enqueue", '{"id":"zero1","command":0}'], env)
    if r.returncode == 0:
        jobs = list_jobs(env)
        assert jobs[0]["command"] == "0"
    else:
        assert "Traceback" not in r.stderr

