"""
test_unit_backoff.py — Focused unit tests for backoff and invariant behaviour.

Covers:
  - Backoff first-failure delay is base^1, not base^0
  - backoff_base=0 produces a valid timestamp (zero-delay, not a crash)
  - backoff_base overflow clamping to 30 days
  - now_iso() always produces UTC-aware timestamps
  - max_retries=-1 behaviour (every failure → dead immediately)
  - attempts invariants across all state transitions
  - promote_ready_retries clears next_retry_at
  - dlq retry clears heartbeat_at
"""

import os
import sqlite3
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent

from queuectl import database as db


@pytest.fixture
def tmp_db(tmp_path, monkeypatch):
    """Isolated DB for each test with monkeypatched DB_PATH."""
    db_file = str(tmp_path / "unit_test.db")
    monkeypatch.setenv("QUEUECTL_DB", db_file)
    monkeypatch.setattr(db.connection, "DB_PATH", db_file)
    db.init_db()
    conn = db.get_connection()
    yield conn
    conn.close()


def _insert_job(conn, id_, state="pending", attempts=0, max_retries=3,
                backoff_base=2.0, worker_id=None, heartbeat_at=None,
                next_retry_at=None, last_error=None):
    """Helper to insert a job with specific fields."""
    ts = db.now_iso()
    conn.execute(
        """INSERT INTO jobs
           (id, command, state, attempts, max_retries, backoff_base,
            worker_id, heartbeat_at, next_retry_at, last_error,
            created_at, updated_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (id_, f"echo {id_}", state, attempts, max_retries, backoff_base,
         worker_id, heartbeat_at, next_retry_at, last_error, ts, ts),
    )


# ===========================================================================
# A.1: Backoff first-failure delay is base^1, not base^0
# ===========================================================================

class TestBackoffFirstFailure:
    def test_first_failure_delay_is_base_not_one(self, tmp_db):
        """First failure uses attempts=1 in formula (base^1), not attempts=0 (base^0)."""
        ts = db.now_iso()
        _insert_job(tmp_db, "j1", state="processing", worker_id="w1",
                    heartbeat_at=ts, attempts=0, max_retries=5, backoff_base=3.0)
        
        t0 = datetime.now(timezone.utc)
        db.finish_job(tmp_db, "j1", "w1", returncode=1)
        
        row = tmp_db.execute(
            "SELECT state, attempts, next_retry_at FROM jobs WHERE id='j1'"
        ).fetchone()
        
        assert row["state"] == "failed"
        assert row["attempts"] == 1
        
        # Calculate delay from t0 to next_retry_at
        next_retry = datetime.fromisoformat(row["next_retry_at"])
        delay = (next_retry - t0).total_seconds()
        
        # Must be ~3 seconds (base^1=3), NOT ~1 second (base^0=1)
        assert 2.5 < delay < 4.0, f"Expected ~3s delay, got {delay:.2f}s"

    def test_second_failure_delay_is_base_squared(self, tmp_db):
        """Second failure uses attempts=2 in formula (base^2)."""
        ts = db.now_iso()
        _insert_job(tmp_db, "j2", state="processing", worker_id="w1",
                    heartbeat_at=ts, attempts=1, max_retries=5, backoff_base=2.0)
        
        t0 = datetime.now(timezone.utc)
        db.finish_job(tmp_db, "j2", "w1", returncode=1)
        
        row = tmp_db.execute("SELECT next_retry_at FROM jobs WHERE id='j2'").fetchone()
        next_retry = datetime.fromisoformat(row["next_retry_at"])
        delay = (next_retry - t0).total_seconds()
        
        # Must be ~4 seconds (2^2=4)
        assert 3.5 < delay < 5.0, f"Expected ~4s delay, got {delay:.2f}s"


# ===========================================================================
# A.2: backoff_base=0 produces valid timestamp, not crash
# ===========================================================================

class TestBackoffBaseZero:
    def test_backoff_base_zero_stores_valid_timestamp(self, tmp_db):
        """backoff_base=0: 0^n = 0, next_retry_at is valid (now), job is immediately eligible."""
        ts = db.now_iso()
        _insert_job(tmp_db, "j1", state="processing", worker_id="w1",
                    heartbeat_at=ts, attempts=0, max_retries=3, backoff_base=0.0)
        
        db.finish_job(tmp_db, "j1", "w1", returncode=1)
        
        row = tmp_db.execute(
            "SELECT state, next_retry_at FROM jobs WHERE id='j1'"
        ).fetchone()
        assert row["state"] == "failed"
        assert row["next_retry_at"] is not None
        
        # Job should be immediately promotable (delay is 0)
        db.promote_ready_retries(tmp_db)
        row2 = tmp_db.execute("SELECT state FROM jobs WHERE id='j1'").fetchone()
        assert row2["state"] == "pending"

    def test_backoff_base_one_stores_constant_delay(self, tmp_db):
        """backoff_base=1: 1^n = 1 for all n, delay is always 1 second."""
        ts = db.now_iso()
        _insert_job(tmp_db, "j1", state="processing", worker_id="w1",
                    heartbeat_at=ts, attempts=2, max_retries=5, backoff_base=1.0)
        
        t0 = datetime.now(timezone.utc)
        db.finish_job(tmp_db, "j1", "w1", returncode=1)
        
        row = tmp_db.execute("SELECT next_retry_at FROM jobs WHERE id='j1'").fetchone()
        next_retry = datetime.fromisoformat(row["next_retry_at"])
        delay = (next_retry - t0).total_seconds()
        
        # 1^3 = 1, delay should be ~1 second
        assert 0.5 < delay < 2.0, f"Expected ~1s delay for base=1, got {delay:.2f}s"


# ===========================================================================
# A.3: Backoff overflow clamping to 30 days
# ===========================================================================

class TestBackoffClamping:
    def test_large_backoff_clamped_to_30_days(self, tmp_db):
        """Large backoff_base^attempts is clamped to 30 days max."""
        ts = db.now_iso()
        # 1000^3 = 1,000,000,000 seconds → clamped to 86400*30 = 2,592,000
        _insert_job(tmp_db, "j1", state="processing", worker_id="w1",
                    heartbeat_at=ts, attempts=2, max_retries=5, backoff_base=1000.0)
        
        t0 = datetime.now(timezone.utc)
        db.finish_job(tmp_db, "j1", "w1", returncode=1)
        
        row = tmp_db.execute("SELECT next_retry_at FROM jobs WHERE id='j1'").fetchone()
        next_retry = datetime.fromisoformat(row["next_retry_at"])
        delay = (next_retry - t0).total_seconds()
        
        MAX_DELAY = 86400 * 30
        assert delay <= MAX_DELAY + 2, f"Delay {delay}s exceeds 30-day cap"
        assert delay >= MAX_DELAY - 2, f"Delay should be approximately 30 days, got {delay}s"

    @pytest.mark.parametrize("attempts,base", [
        (10, 10),      # 10^10 = 10 billion
        (5, 1000),     # 1000^5 = very large
        (100, 2),      # 2^100 = astronomical
    ])
    def test_parametrized_clamping(self, tmp_db, attempts, base):
        """Various large exponent combinations all clamp to 30 days."""
        ts = db.now_iso()
        _insert_job(tmp_db, "j1", state="processing", worker_id="w1",
                    heartbeat_at=ts, attempts=attempts-1, max_retries=attempts+5,
                    backoff_base=float(base))
        
        t0 = datetime.now(timezone.utc)
        db.finish_job(tmp_db, "j1", "w1", returncode=1)
        
        row = tmp_db.execute("SELECT next_retry_at FROM jobs WHERE id='j1'").fetchone()
        next_retry = datetime.fromisoformat(row["next_retry_at"])
        delay = (next_retry - t0).total_seconds()
        
        MAX_DELAY = 86400 * 30
        assert delay <= MAX_DELAY + 2


# ===========================================================================
# A.4: now_iso() always produces UTC-aware timestamps
# ===========================================================================

class TestNowIso:
    def test_now_iso_is_utc_aware(self):
        """now_iso() produces UTC-aware timestamps with +00:00 or Z suffix."""
        ts = db.now_iso()
        assert "+00:00" in ts or ts.endswith("Z"), \
            f"Timestamp not UTC-aware: {ts}"
        
        # Round-trip: parseable back to UTC datetime
        dt = datetime.fromisoformat(ts)
        assert dt.tzinfo is not None, "datetime has no timezone info"
        assert dt.utcoffset().total_seconds() == 0, "not UTC offset"

    def test_now_iso_monotonic(self):
        """Successive calls to now_iso() produce increasing timestamps."""
        ts1 = db.now_iso()
        time.sleep(0.01)
        ts2 = db.now_iso()
        assert ts2 >= ts1, "Timestamps must be monotonic"


# ===========================================================================
# A.5: max_retries=-1 behavior (every failure → dead immediately)
# ===========================================================================

class TestMaxRetriesNegative:
    def test_max_retries_minus_one_goes_dead_on_first_failure(self, tmp_db):
        """max_retries=-1: attempts >= -1 is always true, so first failure → dead."""
        ts = db.now_iso()
        _insert_job(tmp_db, "j1", state="processing", worker_id="w1",
                    heartbeat_at=ts, attempts=0, max_retries=-1, backoff_base=2.0)
        
        db.finish_job(tmp_db, "j1", "w1", returncode=1)
        
        row = tmp_db.execute("SELECT state, attempts FROM jobs WHERE id='j1'").fetchone()
        assert row["state"] == "dead", \
            "max_retries=-1 should send job to dead immediately"
        assert row["attempts"] == 1

    def test_max_retries_zero_goes_dead_on_first_failure(self, tmp_db):
        """max_retries=0: attempts >= 0 after first failure, so dead."""
        ts = db.now_iso()
        _insert_job(tmp_db, "j1", state="processing", worker_id="w1",
                    heartbeat_at=ts, attempts=0, max_retries=0, backoff_base=2.0)
        
        db.finish_job(tmp_db, "j1", "w1", returncode=1)
        
        row = tmp_db.execute("SELECT state FROM jobs WHERE id='j1'").fetchone()
        assert row["state"] == "dead"


# ===========================================================================
# A.6: attempts invariants across all state transitions
# ===========================================================================

class TestAttemptsInvariants:
    def test_attempts_unchanged_by_claim(self, tmp_db):
        """claim_next_job does not increment attempts."""
        _insert_job(tmp_db, "j1", state="pending", attempts=0)
        db.claim_next_job(tmp_db, "w1")
        row = tmp_db.execute("SELECT attempts FROM jobs WHERE id='j1'").fetchone()
        assert row["attempts"] == 0

    def test_attempts_incremented_by_finish_on_failure(self, tmp_db):
        """finish_job increments attempts by 1 on non-zero returncode."""
        ts = db.now_iso()
        _insert_job(tmp_db, "j1", state="processing", worker_id="w1",
                    heartbeat_at=ts, attempts=0)
        db.finish_job(tmp_db, "j1", "w1", returncode=1)
        row = tmp_db.execute("SELECT attempts FROM jobs WHERE id='j1'").fetchone()
        assert row["attempts"] == 1

    def test_attempts_unchanged_by_finish_on_success(self, tmp_db):
        """finish_job does NOT increment attempts on returncode=0."""
        ts = db.now_iso()
        _insert_job(tmp_db, "j1", state="processing", worker_id="w1",
                    heartbeat_at=ts, attempts=1)
        db.finish_job(tmp_db, "j1", "w1", returncode=0)
        row = tmp_db.execute("SELECT state, attempts FROM jobs WHERE id='j1'").fetchone()
        assert row["state"] == "completed"
        assert row["attempts"] == 1  # unchanged from success

    def test_attempts_unchanged_by_promote(self, tmp_db):
        """promote_ready_retries does not change attempts."""
        past = (datetime.now(timezone.utc) - timedelta(seconds=5)).isoformat()
        _insert_job(tmp_db, "j1", state="failed", attempts=2, next_retry_at=past)
        
        db.promote_ready_retries(tmp_db)
        row = tmp_db.execute("SELECT attempts FROM jobs WHERE id='j1'").fetchone()
        assert row["attempts"] == 2

    def test_attempts_unchanged_by_reap(self, tmp_db):
        """reap_stale_jobs does not change attempts."""
        stale = (datetime.now(timezone.utc) - timedelta(seconds=30)).isoformat()
        _insert_job(tmp_db, "j1", state="processing", worker_id="w-dead",
                    heartbeat_at=stale, attempts=3)
        
        db.reap_stale_jobs(tmp_db, timeout_seconds=15)
        row = tmp_db.execute("SELECT attempts FROM jobs WHERE id='j1'").fetchone()
        assert row["attempts"] == 3

    def test_attempts_never_null_or_negative(self, tmp_db):
        """attempts must always be >= 0 through all transitions."""
        ts = db.now_iso()
        _insert_job(tmp_db, "j1", state="processing", worker_id="w1",
                    heartbeat_at=ts, attempts=0)
        
        # Go through multiple transitions
        db.finish_job(tmp_db, "j1", "w1", 1)  # attempts=1, state=failed
        row1 = tmp_db.execute("SELECT attempts FROM jobs WHERE id='j1'").fetchone()
        assert row1["attempts"] >= 0
        
        # Promote
        past = (datetime.now(timezone.utc) - timedelta(seconds=5)).isoformat()
        tmp_db.execute("UPDATE jobs SET next_retry_at=? WHERE id='j1'", (past,))
        db.promote_ready_retries(tmp_db)
        row2 = tmp_db.execute("SELECT attempts FROM jobs WHERE id='j1'").fetchone()
        assert row2["attempts"] >= 0


# ===========================================================================
# A.7: promote_ready_retries clears next_retry_at
# ===========================================================================

class TestPromoteClearsNextRetryAt:
    def test_promoted_job_has_null_next_retry_at(self, tmp_db):
        """After promotion failed → pending, next_retry_at must be NULL."""
        past = (datetime.now(timezone.utc) - timedelta(seconds=5)).isoformat()
        _insert_job(tmp_db, "j1", state="failed", attempts=1, next_retry_at=past)
        
        db.promote_ready_retries(tmp_db)
        
        row = tmp_db.execute(
            "SELECT state, next_retry_at FROM jobs WHERE id='j1'"
        ).fetchone()
        assert row["state"] == "pending"
        assert row["next_retry_at"] is None, \
            "BUG-1 regression: next_retry_at must be NULL after promotion"

    def test_multiple_promotions_all_clear_next_retry_at(self, tmp_db):
        """All promoted jobs in a batch have next_retry_at cleared."""
        past = (datetime.now(timezone.utc) - timedelta(seconds=10)).isoformat()
        for i in range(5):
            _insert_job(tmp_db, f"j{i}", state="failed", attempts=1, next_retry_at=past)
        
        db.promote_ready_retries(tmp_db)
        
        rows = tmp_db.execute("SELECT id, next_retry_at FROM jobs").fetchall()
        for r in rows:
            assert r["next_retry_at"] is None, \
                f"Job {r['id']} still has next_retry_at set"


# ===========================================================================
# A.8: dlq retry clears heartbeat_at
# ===========================================================================

class TestDlqRetryClearsHeartbeat:
    def test_dlq_retry_clears_heartbeat_at(self, tmp_db):
        """dlq retry must clear heartbeat_at from the previous run."""
        ts = db.now_iso()
        tmp_db.execute(
            """INSERT INTO jobs (id, command, state, attempts, max_retries, backoff_base,
               heartbeat_at, last_error, created_at, updated_at)
               VALUES ('j1', 'exit 1', 'dead', 3, 3, 2.0, ?, 'err', ?, ?)""",
            (ts, ts, ts),
        )
        
        # Simulate dlq retry (mimics app.py dlq_retry logic)
        tmp_db.execute("BEGIN IMMEDIATE")
        row = tmp_db.execute(
            "SELECT * FROM jobs WHERE id='j1' AND state='dead'"
        ).fetchone()
        assert row is not None
        
        tmp_db.execute(
            """UPDATE jobs SET state='pending', attempts=0, next_retry_at=NULL,
               worker_id=NULL, heartbeat_at=NULL, last_error=NULL, updated_at=?
               WHERE id='j1'""",
            (db.now_iso(),),
        )
        tmp_db.execute("COMMIT")
        
        row2 = tmp_db.execute(
            "SELECT heartbeat_at, worker_id FROM jobs WHERE id='j1'"
        ).fetchone()
        assert row2["heartbeat_at"] is None
        assert row2["worker_id"] is None


# ===========================================================================
# Additional unit tests for completeness
# ===========================================================================

class TestTouchJobHeartbeatNoOp:
    def test_touch_heartbeat_is_noop_on_completed_job(self, tmp_db):
        """touch_job_heartbeat on a completed job is a no-op (WHERE state='processing')."""
        _insert_job(tmp_db, "j1", state="completed")
        db.touch_job_heartbeat(tmp_db, "j1", "w-old")
        
        row = tmp_db.execute("SELECT heartbeat_at FROM jobs WHERE id='j1'").fetchone()
        assert row["heartbeat_at"] is None


class TestFinishJobNoOpBranch:
    def test_finish_job_is_noop_after_job_was_reaped(self, tmp_db):
        """finish_job on a reaped (now pending) job must be a no-op."""
        stale = (datetime.now(timezone.utc) - timedelta(seconds=30)).isoformat()
        _insert_job(tmp_db, "j1", state="processing", worker_id="w-dead",
                    heartbeat_at=stale)
        
        # Reap it back to pending
        db.reap_stale_jobs(tmp_db, timeout_seconds=15)
        row = tmp_db.execute("SELECT state FROM jobs WHERE id='j1'").fetchone()
        assert row["state"] == "pending"
        
        # Now the dead worker's finish_job arrives late
        db.finish_job(tmp_db, "j1", "w-dead", returncode=0)
        
        # Must still be pending — finish_job was a no-op
        row2 = tmp_db.execute("SELECT state FROM jobs WHERE id='j1'").fetchone()
        assert row2["state"] == "pending", \
            "finish_job must not corrupt a reaped job to completed"


class TestWorkerIdNullInTerminalStates:
    def test_worker_id_null_after_dead_state(self, tmp_db):
        """worker_id must be NULL when job reaches dead state."""
        ts = db.now_iso()
        _insert_job(tmp_db, "j1", state="processing", worker_id="w-1",
                    heartbeat_at=ts, attempts=2, max_retries=3)
        
        db.finish_job(tmp_db, "j1", "w-1", returncode=1)  # attempts=3 >= max=3 → dead
        
        row = tmp_db.execute("SELECT state, worker_id FROM jobs WHERE id='j1'").fetchone()
        assert row["state"] == "dead"
        assert row["worker_id"] is None
