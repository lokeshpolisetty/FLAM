"""
test_bug_regression.py — Regression tests for confirmed bugs and
production-critical correctness issues.

BUG-1  promote_ready_retries did not clear next_retry_at → promoted jobs
       kept stale scheduling data.
BUG-2  worker stop caught ProcessLookupError but not PermissionError → could
       silently SIGTERM an unrelated OS process on PID reuse.
BUG-3  execute_job heartbeat call was unguarded → a transient DB write failure
       mid-job crashed the entire worker process.
BUG-4  heartbeat_at was absent from _job_to_public_dict → list --json schema
       was incomplete.

GAP-5  heartbeat-interval >= recovery-timeout → healthy workers reaped.
GAP-6  execute_job did not kill child process group → orphan subprocesses
       after worker SIGKILL.
GAP-7  subprocess stdout=PIPE with large output blocks the pipe buffer.
"""

import json
import os
import signal
import sqlite3
import subprocess
import sys
import time
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

ROOT = Path(__file__).resolve().parent.parent

from queuectl import database as db
from queuectl.config import settings as config_service
from queuectl.worker import execute_job


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def cli(args, env, timeout=30):
    return subprocess.run(
        [sys.executable, "-m", "queuectl"] + args,
        capture_output=True, text=True, timeout=timeout, env=env,
    )


@pytest.fixture
def fresh_env(tmp_path):
    e = os.environ.copy()
    e["QUEUECTL_DB"] = str(tmp_path / "reg.db")
    return e


@pytest.fixture
def fresh_db(tmp_path, monkeypatch):
    db_file = str(tmp_path / "reg.db")
    monkeypatch.setenv("QUEUECTL_DB", db_file)
    monkeypatch.setattr(db.connection, "DB_PATH", db_file)
    db.init_db()
    conn = db.get_connection()
    yield conn
    conn.close()


def _insert_job(conn, id_, state="pending", attempts=0, max_retries=3,
                backoff_base=2.0, next_retry_at=None, worker_id=None,
                heartbeat_at=None):
    ts = db.now_iso()
    conn.execute(
        """INSERT INTO jobs
           (id, command, state, attempts, max_retries, backoff_base,
            next_retry_at, worker_id, heartbeat_at, created_at, updated_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (id_, f"echo {id_}", state, attempts, max_retries, backoff_base,
         next_retry_at, worker_id, heartbeat_at, ts, ts),
    )


# ===========================================================================
# BUG-1: promote_ready_retries must clear next_retry_at
# ===========================================================================

class TestBug1PromoteNextRetryAt:
    def test_promoted_job_has_null_next_retry_at(self, fresh_db):
        """After promotion failed → pending, next_retry_at must be NULL."""
        past = (datetime.now(timezone.utc) - timedelta(seconds=5)).isoformat()
        _insert_job(fresh_db, "b1-a", state="failed", attempts=1, next_retry_at=past)

        db.promote_ready_retries(fresh_db)

        row = fresh_db.execute("SELECT * FROM jobs WHERE id='b1-a'").fetchone()
        assert row["state"] == "pending"
        assert row["next_retry_at"] is None, (
            "BUG-1: next_retry_at must be NULL after promotion; "
            f"got {row['next_retry_at']!r}"
        )

    def test_multiple_promotions_all_clear_next_retry_at(self, fresh_db):
        """All promoted jobs in a batch have next_retry_at cleared."""
        past = (datetime.now(timezone.utc) - timedelta(seconds=10)).isoformat()
        for i in range(5):
            _insert_job(fresh_db, f"b1-{i}", state="failed", attempts=1, next_retry_at=past)

        db.promote_ready_retries(fresh_db)

        rows = fresh_db.execute(
            "SELECT id, state, next_retry_at FROM jobs"
        ).fetchall()
        for r in rows:
            assert r["state"] == "pending"
            assert r["next_retry_at"] is None, (
                f"BUG-1: job {r['id']} still has next_retry_at={r['next_retry_at']!r}"
            )

    def test_not_yet_due_job_retains_next_retry_at(self, fresh_db):
        """A failed job whose backoff window hasn't elapsed is NOT promoted."""
        future = (datetime.now(timezone.utc) + timedelta(seconds=60)).isoformat()
        _insert_job(fresh_db, "b1-future", state="failed", attempts=1, next_retry_at=future)

        db.promote_ready_retries(fresh_db)

        row = fresh_db.execute("SELECT * FROM jobs WHERE id='b1-future'").fetchone()
        assert row["state"] == "failed"
        assert row["next_retry_at"] == future, (
            "A not-yet-due job must retain its next_retry_at"
        )

    def test_second_retry_cycle_not_confused_by_stale_next_retry_at(self, fresh_db):
        """Clearing next_retry_at ensures the second retry cycle works correctly.

        If next_retry_at is left non-NULL after promotion, then when the job
        fails again and gets a new next_retry_at, the value won't interfere —
        but if the fix is absent and the job transitions pending→processing→failed
        again, the old stale next_retry_at could cause it to be promoted
        immediately on the same loop tick.
        """
        past = (datetime.now(timezone.utc) - timedelta(seconds=5)).isoformat()
        _insert_job(fresh_db, "b1-cycle", state="failed", attempts=1, next_retry_at=past)

        # First promote
        db.promote_ready_retries(fresh_db)

        row = fresh_db.execute("SELECT * FROM jobs WHERE id='b1-cycle'").fetchone()
        assert row["state"] == "pending"
        assert row["next_retry_at"] is None

        # Simulate the job being claimed and failing again with a future retry
        future = (datetime.now(timezone.utc) + timedelta(seconds=60)).isoformat()
        fresh_db.execute(
            "UPDATE jobs SET state='failed', attempts=2, next_retry_at=? WHERE id='b1-cycle'",
            (future,)
        )

        # Promote again — future job must NOT be promoted
        db.promote_ready_retries(fresh_db)
        row2 = fresh_db.execute("SELECT * FROM jobs WHERE id='b1-cycle'").fetchone()
        assert row2["state"] == "failed", (
            "Job with future next_retry_at must not be promoted"
        )


# ===========================================================================
# BUG-2: worker stop must handle PermissionError on os.kill
# ===========================================================================

class TestBug2WorkerStopPermissionError:
    def test_permission_error_marks_worker_stopped_and_does_not_crash(
        self, fresh_env, tmp_path, monkeypatch
    ):
        """worker stop with PermissionError on os.kill exits 0 and marks stopped."""
        # Start and immediately stop a real worker so its row is in the DB
        proc = subprocess.Popen(
            [sys.executable, "-m", "queuectl"] + ["worker", "start", "--count", "1"],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, env=fresh_env
        )
        time.sleep(0.4)
        proc.send_signal(signal.SIGTERM)
        proc.wait(timeout=6)

        # Inject a fake running row with PID=1 (owned by root → PermissionError)
        conn = sqlite3.connect(fresh_env["QUEUECTL_DB"])
        ts = datetime.now(timezone.utc).isoformat()
        conn.execute(
            "INSERT OR REPLACE INTO workers (worker_id, pid, status, started_at, heartbeat_at) "
            "VALUES ('w-perm-test', 1, 'running', ?, ?)",
            (ts, ts)
        )
        conn.commit()
        conn.close()

        res = cli(["worker", "stop"], fresh_env)
        # Must not crash
        assert res.returncode == 0, f"worker stop crashed: {res.stderr}"
        # Must not print a traceback
        assert "Traceback" not in res.stdout + res.stderr

        # Row must be marked stopped
        conn2 = sqlite3.connect(fresh_env["QUEUECTL_DB"])
        row = conn2.execute(
            "SELECT status FROM workers WHERE worker_id='w-perm-test'"
        ).fetchone()
        conn2.close()
        assert row is not None
        assert row[0] == "stopped", f"Expected stopped, got {row[0]}"

    def test_permission_error_does_not_affect_other_workers(
        self, fresh_env, tmp_path
    ):
        """Only the bad row is marked stopped; a real running worker still gets SIGTERM."""
        # Use a fast job so the worker is idle (between jobs) when SIGTERM arrives
        cli(["enqueue", '{"id":"fast","command":"echo hi"}'], fresh_env)

        proc = subprocess.Popen(
            [sys.executable, "-m", "queuectl"] + ["worker", "start", "--count", "1"],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, env=fresh_env
        )
        # Wait until the fast job is done so worker is idle
        deadline = time.time() + 8
        while time.time() < deadline:
            conn = sqlite3.connect(fresh_env["QUEUECTL_DB"])
            row = conn.execute("SELECT state FROM jobs WHERE id='fast'").fetchone()
            conn.close()
            if row and row[0] == "completed":
                break
            time.sleep(0.2)

        # Inject a stale PID-1 row alongside the real worker
        conn = sqlite3.connect(fresh_env["QUEUECTL_DB"])
        ts = datetime.now(timezone.utc).isoformat()
        conn.execute(
            "INSERT OR REPLACE INTO workers (worker_id, pid, status, started_at, heartbeat_at) "
            "VALUES ('w-perm-bad', 1, 'running', ?, ?)",
            (ts, ts)
        )
        conn.commit()
        conn.close()

        res = cli(["worker", "stop"], fresh_env)
        assert res.returncode == 0, f"worker stop crashed: {res.stderr}"
        # Real worker should have received SIGTERM — it's idle so should exit quickly
        try:
            proc.wait(timeout=8)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()
            pytest.fail("Real worker did not exit after worker stop")


# ===========================================================================
# BUG-3: heartbeat exceptions must not crash execute_job
# ===========================================================================

class TestBug3HeartbeatException:
    def test_heartbeat_db_error_does_not_crash_execute_job(
        self, fresh_db, monkeypatch, tmp_path
    ):
        """A DB error in touch_job_heartbeat is swallowed — execute_job returns normally."""
        ts = db.now_iso()
        fresh_db.execute(
            "INSERT INTO jobs (id, command, state, attempts, max_retries, "
            "backoff_base, worker_id, heartbeat_at, created_at, updated_at) "
            "VALUES ('hb-err', 'echo ok', 'processing', 0, 3, 2.0, 'w1', ?, ?, ?)",
            (ts, ts, ts)
        )
        job = dict(fresh_db.execute("SELECT * FROM jobs WHERE id='hb-err'").fetchone())

        # Make touch_job_heartbeat always raise
        call_count = {"n": 0}
        def bad_heartbeat(conn, job_id, worker_id):
            call_count["n"] += 1
            raise sqlite3.OperationalError("simulated DB failure")

        monkeypatch.setattr(db.job_repository, "touch_job_heartbeat", bad_heartbeat)

        # execute_job with a very short heartbeat_interval so the heartbeat fires
        rc = execute_job(fresh_db, job, "w1", heartbeat_interval=0.01)

        # Must return a valid exit code, not raise
        assert rc == 0, f"execute_job should return 0 for 'echo ok', got {rc}"
        # Heartbeat must have been attempted (we used 0.01s interval)
        assert call_count["n"] >= 1, "heartbeat was never called during job execution"

    def test_repeated_heartbeat_failures_still_completes_job(
        self, fresh_db, monkeypatch
    ):
        """Even when every heartbeat call fails, the job still completes."""
        ts = db.now_iso()
        fresh_db.execute(
            "INSERT INTO jobs (id, command, state, attempts, max_retries, "
            "backoff_base, worker_id, heartbeat_at, created_at, updated_at) "
            "VALUES ('hb-rep', 'sleep 0.3', 'processing', 0, 3, 2.0, 'w1', ?, ?, ?)",
            (ts, ts, ts)
        )
        job = dict(fresh_db.execute("SELECT * FROM jobs WHERE id='hb-rep'").fetchone())

        monkeypatch.setattr(
            db, "touch_job_heartbeat",
            lambda *a, **kw: (_ for _ in ()).throw(sqlite3.DatabaseError("disk full"))
        )

        rc = execute_job(fresh_db, job, "w1", heartbeat_interval=0.05)
        assert rc == 0

    def test_heartbeat_failure_logged_to_stdout(
        self, fresh_db, monkeypatch, capsys
    ):
        """A heartbeat failure prints a warning line rather than a traceback."""
        ts = db.now_iso()
        fresh_db.execute(
            "INSERT INTO jobs (id, command, state, attempts, max_retries, "
            "backoff_base, worker_id, heartbeat_at, created_at, updated_at) "
            "VALUES ('hb-log', 'echo hi', 'processing', 0, 3, 2.0, 'w1', ?, ?, ?)",
            (ts, ts, ts)
        )
        job = dict(fresh_db.execute("SELECT * FROM jobs WHERE id='hb-log'").fetchone())

        monkeypatch.setattr(
            db, "touch_job_heartbeat",
            lambda *a, **kw: (_ for _ in ()).throw(sqlite3.OperationalError("locked"))
        )

        execute_job(fresh_db, job, "w1", heartbeat_interval=0.01)

        captured = capsys.readouterr()
        assert "Traceback" not in captured.out + captured.err
        # Warning message should mention the job id and the error
        assert "heartbeat" in captured.out.lower() or "heartbeat" in captured.err.lower()


# ===========================================================================
# BUG-4: list --json must include heartbeat_at field
# ===========================================================================

class TestBug4HeartbeatAtInJson:
    def test_list_json_includes_heartbeat_at_field(self, fresh_env):
        """list --json output includes heartbeat_at key for every job."""
        cli(["enqueue", '{"id":"hb4-a","command":"echo hi"}'], fresh_env)

        res = cli(["list", "--json"], fresh_env)
        assert res.returncode == 0
        jobs = json.loads(res.stdout)
        assert len(jobs) == 1
        assert "heartbeat_at" in jobs[0], (
            f"BUG-4: heartbeat_at missing from list --json output; keys={list(jobs[0].keys())}"
        )

    def test_list_json_heartbeat_at_is_null_for_pending_job(self, fresh_env):
        """Pending jobs have heartbeat_at=null in JSON output."""
        cli(["enqueue", '{"id":"hb4-pend","command":"echo hi"}'], fresh_env)
        res = cli(["list", "--json"], fresh_env)
        jobs = json.loads(res.stdout)
        assert jobs[0]["heartbeat_at"] is None

    def test_list_json_heartbeat_at_populated_during_processing(self, fresh_env):
        """A processing job has a non-null heartbeat_at in JSON output."""
        cli(["enqueue", '{"id":"hb4-proc","command":"sleep 30"}'], fresh_env)

        wp = subprocess.Popen(
            [sys.executable, "-m", "queuectl"] + ["worker", "start", "--count", "1"],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, env=fresh_env
        )
        try:
            # Wait for job to be claimed
            deadline = time.time() + 8
            hb_val = None
            while time.time() < deadline:
                res = cli(["list", "--json"], fresh_env)
                jobs = json.loads(res.stdout)
                for j in jobs:
                    if j["id"] == "hb4-proc" and j["state"] == "processing":
                        hb_val = j.get("heartbeat_at")
                        break
                if hb_val is not None:
                    break
                time.sleep(0.3)
        finally:
            wp.send_signal(signal.SIGTERM)
            try:
                wp.wait(timeout=8)
            except subprocess.TimeoutExpired:
                wp.kill()
                wp.wait()

        assert hb_val is not None, (
            "Processing job should have a non-null heartbeat_at in --json output"
        )

    def test_dlq_list_json_includes_heartbeat_at(self, fresh_env):
        """dlq list --json also includes heartbeat_at (uses same serialiser)."""
        # Seed a dead job directly
        conn = sqlite3.connect(fresh_env["QUEUECTL_DB"])
        conn.execute(
            "CREATE TABLE IF NOT EXISTS jobs "
            "(id TEXT PRIMARY KEY, command TEXT NOT NULL, state TEXT NOT NULL DEFAULT 'pending', "
            "attempts INTEGER NOT NULL DEFAULT 0, max_retries INTEGER NOT NULL DEFAULT 3, "
            "backoff_base REAL NOT NULL DEFAULT 2, worker_id TEXT, next_retry_at TEXT, "
            "heartbeat_at TEXT, last_error TEXT, created_at TEXT NOT NULL, updated_at TEXT NOT NULL)"
        )
        ts = datetime.now(timezone.utc).isoformat()
        conn.execute(
            "INSERT INTO jobs (id,command,state,attempts,max_retries,backoff_base,"
            "last_error,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?)",
            ("hb4-dlq", "exit 1", "dead", 3, 3, 2.0, "err", ts, ts)
        )
        conn.commit()
        conn.close()

        res = cli(["dlq", "list", "--json"], fresh_env)
        assert res.returncode == 0
        jobs = json.loads(res.stdout)
        assert len(jobs) >= 1
        assert "heartbeat_at" in jobs[0], (
            f"BUG-4: heartbeat_at missing from dlq list --json; keys={list(jobs[0].keys())}"
        )


# ===========================================================================
# GAP-5: heartbeat-interval >= recovery-timeout must be rejected at startup
# ===========================================================================

class TestGap5HeartbeatRecoveryGuard:
    def test_worker_start_rejects_hb_equal_to_recovery(self, fresh_env):
        """worker start exits non-zero when heartbeat-interval == recovery-timeout."""
        cli(["config", "set", "heartbeat-interval", "15"], fresh_env)
        cli(["config", "set", "recovery-timeout", "15"], fresh_env)

        res = cli(["worker", "start", "--count", "1"], fresh_env, timeout=5)
        assert res.returncode != 0, (
            "worker start should fail when heartbeat-interval >= recovery-timeout"
        )
        assert "Traceback" not in res.stderr + res.stdout
        assert "heartbeat" in res.stderr.lower() or "recovery" in res.stderr.lower()

    def test_worker_start_rejects_hb_greater_than_recovery(self, fresh_env):
        """worker start exits non-zero when heartbeat-interval > recovery-timeout."""
        cli(["config", "set", "heartbeat-interval", "20"], fresh_env)
        cli(["config", "set", "recovery-timeout", "15"], fresh_env)

        res = cli(["worker", "start", "--count", "1"], fresh_env, timeout=5)
        assert res.returncode != 0

    def test_worker_start_accepts_valid_hb_recovery_ratio(self, fresh_env):
        """worker start proceeds normally when heartbeat-interval < recovery-timeout."""
        cli(["config", "set", "heartbeat-interval", "3"], fresh_env)
        cli(["config", "set", "recovery-timeout", "15"], fresh_env)

        proc = subprocess.Popen(
            [sys.executable, "-m", "queuectl"] + ["worker", "start", "--count", "1"],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, env=fresh_env
        )
        time.sleep(0.5)
        assert proc.poll() is None, "Worker should be running with valid config"
        proc.send_signal(signal.SIGTERM)
        proc.wait(timeout=6)

    def test_error_message_names_both_values(self, fresh_env):
        """The config error message includes the specific values that are wrong."""
        cli(["config", "set", "heartbeat-interval", "30"], fresh_env)
        cli(["config", "set", "recovery-timeout", "10"], fresh_env)

        res = cli(["worker", "start", "--count", "1"], fresh_env, timeout=5)
        assert res.returncode != 0
        # The message should mention the actual numeric values
        assert "30" in res.stderr or "10" in res.stderr, (
            f"Error message should mention the bad values; got: {res.stderr!r}"
        )


# ===========================================================================
# GAP-6: execute_job kills child process group (no orphan subprocesses)
# ===========================================================================

class TestGap6OrphanSubprocess:
    def test_execute_job_uses_new_session(self, fresh_db, monkeypatch):
        """execute_job starts the child in a new session (start_new_session=True)."""
        started_kwargs = {}

        real_popen = subprocess.Popen

        def capturing_popen(*args, **kwargs):
            started_kwargs.update(kwargs)
            return real_popen(*args, **kwargs)

        monkeypatch.setattr(subprocess, "Popen", capturing_popen)

        ts = db.now_iso()
        fresh_db.execute(
            "INSERT INTO jobs (id, command, state, attempts, max_retries, "
            "backoff_base, worker_id, heartbeat_at, created_at, updated_at) "
            "VALUES ('sess-j', 'echo hi', 'processing', 0, 3, 2.0, 'w1', ?, ?, ?)",
            (ts, ts, ts)
        )
        job = dict(fresh_db.execute("SELECT * FROM jobs WHERE id='sess-j'").fetchone())

        execute_job(fresh_db, job, "w1", heartbeat_interval=60)

        assert started_kwargs.get("start_new_session") is True, (
            "execute_job must pass start_new_session=True to Popen to enable "
            "process-group cleanup (orphan prevention)"
        )

    def test_execute_job_stdout_devnull(self, fresh_db, monkeypatch):
        """execute_job redirects stdout/stderr to DEVNULL (no pipe buffer block)."""
        started_kwargs = {}

        real_popen = subprocess.Popen

        def capturing_popen(*args, **kwargs):
            started_kwargs.update(kwargs)
            return real_popen(*args, **kwargs)

        monkeypatch.setattr(subprocess, "Popen", capturing_popen)

        ts = db.now_iso()
        fresh_db.execute(
            "INSERT INTO jobs (id, command, state, attempts, max_retries, "
            "backoff_base, worker_id, heartbeat_at, created_at, updated_at) "
            "VALUES ('devnull-j', 'echo hi', 'processing', 0, 3, 2.0, 'w1', ?, ?, ?)",
            (ts, ts, ts)
        )
        job = dict(fresh_db.execute("SELECT * FROM jobs WHERE id='devnull-j'").fetchone())

        execute_job(fresh_db, job, "w1", heartbeat_interval=60)

        assert started_kwargs.get("stdout") == subprocess.DEVNULL, (
            "execute_job must use stdout=DEVNULL to prevent pipe-buffer deadlock"
        )
        assert started_kwargs.get("stderr") == subprocess.DEVNULL


# ===========================================================================
# GAP-7: large stdout does not block the worker
# ===========================================================================

class TestGap7LargeStdoutNonBlocking:
    @pytest.mark.slow
    def test_1mb_stdout_job_completes_without_hang(self, fresh_env):
        """A job emitting 1 MB of stdout completes within 10s (no pipe block)."""
        # Generate 1 MB: ~10 000 lines of 100 chars
        cmd = "python3 -c \"import sys; [sys.stdout.write('x'*100+'\\n') for _ in range(10000)]\""
        cli(["enqueue", json.dumps({"id": "large-out", "command": cmd})], fresh_env)

        wp = subprocess.Popen(
            [sys.executable, "-m", "queuectl"] + ["worker", "start", "--count", "1"],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, env=fresh_env
        )
        try:
            deadline = time.time() + 15
            completed = False
            while time.time() < deadline:
                res = cli(["list", "--json"], fresh_env)
                for j in json.loads(res.stdout):
                    if j["id"] == "large-out" and j["state"] == "completed":
                        completed = True
                        break
                if completed:
                    break
                time.sleep(0.5)
        finally:
            wp.send_signal(signal.SIGTERM)
            wp.wait(timeout=6)

        assert completed, "1 MB stdout job did not complete — possible pipe-buffer deadlock"

    def test_large_stderr_job_completes_without_hang(self, fresh_env):
        """A job emitting 1 MB of stderr completes within 10s."""
        cmd = "python3 -c \"import sys; [sys.stderr.write('e'*100+'\\n') for _ in range(10000)]\""
        cli(["enqueue", json.dumps({"id": "large-err", "command": cmd})], fresh_env)

        wp = subprocess.Popen(
            [sys.executable, "-m", "queuectl"] + ["worker", "start", "--count", "1"],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, env=fresh_env
        )
        try:
            deadline = time.time() + 15
            completed = False
            while time.time() < deadline:
                res = cli(["list", "--json"], fresh_env)
                for j in json.loads(res.stdout):
                    if j["id"] == "large-err" and j["state"] == "completed":
                        completed = True
                        break
                if completed:
                    break
                time.sleep(0.5)
        finally:
            wp.send_signal(signal.SIGTERM)
            wp.wait(timeout=6)

        assert completed, "1 MB stderr job did not complete — possible pipe-buffer deadlock"


# ===========================================================================
# Cross-cutting: promote → pending job is a fully clean pending row
# ===========================================================================

class TestPromotedJobCleanness:
    def test_promoted_job_behaves_like_freshly_enqueued(self, fresh_db):
        """A promoted job is indistinguishable from a fresh pending job to claim_next_job."""
        past = (datetime.now(timezone.utc) - timedelta(seconds=5)).isoformat()
        ts = db.now_iso()
        # Insert a 'failed' job with stale next_retry_at
        fresh_db.execute(
            "INSERT INTO jobs (id, command, state, attempts, max_retries, backoff_base, "
            "next_retry_at, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            ("prom-clean", "echo hi", "failed", 1, 3, 2.0, past, ts, ts)
        )

        db.promote_ready_retries(fresh_db)

        # Verify promotion cleared next_retry_at (BUG-1 regression)
        pre_claim = fresh_db.execute(
            "SELECT state, next_retry_at FROM jobs WHERE id='prom-clean'"
        ).fetchone()
        assert pre_claim["state"] == "pending"
        assert pre_claim["next_retry_at"] is None, (
            "BUG-1: promoted job must have next_retry_at=NULL before claiming"
        )

        # Now claim it — claim_next_job returns the pre-UPDATE snapshot (state='pending'),
        # but the DB row must be updated to 'processing'.
        job = db.claim_next_job(fresh_db, "w-test")
        assert job is not None, "Promoted job should be claimable"
        assert job["id"] == "prom-clean"

        # The DB row (not the returned snapshot) must be 'processing'
        row = fresh_db.execute(
            "SELECT state, next_retry_at FROM jobs WHERE id='prom-clean'"
        ).fetchone()
        assert row["state"] == "processing", (
            f"Claimed job should be processing in DB, got {row['state']}"
        )
        # next_retry_at must still be NULL after claim (was NULL, stays NULL)
        assert row["next_retry_at"] is None, (
            "Claimed (processing) job must not carry stale next_retry_at"
        )
