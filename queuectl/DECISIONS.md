# DECISIONS.md

## 1. Which exact line(s) prevent two workers from claiming the same job, and why is that operation atomic across separate OS processes?

In `db.py`, function `claim_next_job`:

```python
conn.execute("BEGIN IMMEDIATE")
try:
    row = conn.execute(
        "SELECT * FROM jobs WHERE state = 'pending' ORDER BY created_at ASC LIMIT 1"
    ).fetchone()
    ...
    conn.execute(
        "UPDATE jobs SET state = 'processing', worker_id = ?, heartbeat_at = ?, updated_at = ? "
        "WHERE id = ? AND state = 'pending'",
        (worker_id, ts, ts, row["id"]),
    )
    conn.execute("COMMIT")
```

The line doing the actual work is `conn.execute("BEGIN IMMEDIATE")`. In
SQLite's rollback-journal/WAL locking model, `BEGIN IMMEDIATE` acquires a
**RESERVED lock on the database file itself** at the moment it runs, not
lazily on the first write. SQLite guarantees that only one connection —
in any process, on this machine, full stop — can hold that lock at a
time. A second worker process calling `claim_next_job()` will block
inside its own `BEGIN IMMEDIATE` (up to `busy_timeout`, set to 30s via
`PRAGMA busy_timeout=30000` in `get_connection()`) until the first
worker's transaction commits or rolls back.

This is what makes it atomic *across processes*, not just across
threads: the lock lives in the OS-level file locking SQLite uses under
the hood, so it's enforced regardless of which process, which Python
interpreter, or which connection object is asking. There is no window in
which two processes can both see `job1` as `pending` and both write
`processing` — the second process's SELECT literally cannot execute
until the first process's UPDATE has already committed, at which point
its own SELECT will simply not return that row anymore (it's no longer
`pending`).

The `AND state = 'pending'` in the UPDATE is a secondary guard, not the
primary mechanism — it protects against a hypothetical future bug where
some code path reads outside a `BEGIN IMMEDIATE` block; today it's
unreachable dead-code-safety, since the transaction already makes the
race impossible.

**Rejected alternative:** an in-memory `threading.Lock` — doesn't work
at all, since workers are separate OS processes with separate memory
spaces. A `multiprocessing.Lock` would only work if all workers were
forked from one common parent holding the lock object, which breaks the
requirement that workers can be started from independent terminal
sessions with no shared parent.

## 2. A worker is SIGKILLed halfway through a job. Walk through, step by step, what state the job is in and how it eventually runs again. What is the worst-case delay before recovery?

Step by step:

1. Worker `w-123` claims `job1`, setting `state='processing'`,
   `worker_id='w-123'`, `heartbeat_at=<now>`.
2. While the job's subprocess runs, `execute_job()` refreshes
   `heartbeat_at` every `heartbeat-interval` seconds (default 3s).
3. `kill -9 <pid>` hits the worker process. No Python code runs at all —
   no signal handler fires, no `finally` block executes. The OS just
   reclaims the process. `job1` is left exactly as it was at the last
   heartbeat write: `state='processing'`, `worker_id='w-123'`,
   `heartbeat_at=<stale timestamp>`.
4. Nothing changes until *some* process calls `reap_stale_jobs()`. That
   happens at the top of every worker loop iteration (any other running
   worker will do it within `poll-interval` seconds, default 1s) **and**
   at the top of `status` and `list`, **and** as the first thing any
   fresh `worker start` does before entering its loop — so recovery
   does not depend on any specific process surviving.
5. `reap_stale_jobs(conn, recovery_timeout)` finds any row with
   `state='processing'` and `heartbeat_at` older than `recovery-timeout`
   seconds (default 15s) and resets it: `state='pending'`,
   `worker_id=NULL`, `heartbeat_at=NULL`.
6. `job1` is now `pending` again and gets claimed by whichever worker
   calls `claim_next_job()` next (same atomic mechanism as Q1 — it's
   just a normal pending job now, no special-casing needed).
7. It runs again from `attempts` as it was (the crash doesn't count as a
   retry attempt — the job never got the chance to exit with a
   meaningful code, so charging an attempt would be punishing it for our
   own infrastructure failure, not for the job failing).

**Worst-case delay:** `heartbeat-interval + recovery-timeout` from the
last successful heartbeat write, i.e. up to `3s + 15s = 18s` by default,
plus one `poll-interval` (1s) for some worker to notice = **≤ 19s**,
comfortably under the 60s requirement. If literally every worker is
killed simultaneously, the bound becomes: time until the next `worker
start` (or `status`/`list` call) — which is why every read command also
reaps, so even a human just running `queuectl status` to check on
things triggers recovery.

## 3. Does `dlq retry` reset `attempts`? Why is that the right call?

Yes — see `dlq_retry()` in `app.py`: `attempts=0` is set explicitly.

Reasoning: a DLQ retry is an *operator* decision, made after looking at
`last_error` and presumably fixing whatever was wrong (a downstream
service was down, a bad config value, a missing file that's now
present). It represents "conditions have changed, give this a full,
fresh shot" — not "here's attempt N+1 of the original failing run." If
we kept the old attempt count, a job with `max_retries=3` that died
after 3 failures would, on manual retry, immediately be one failure away
from `dead` again, even though the operator's whole point was to give it
a clean run. That makes `dlq retry` nearly useless for anything with a
tight retry budget.

The trade-off: if the underlying problem *wasn't* actually fixed, a
reset-to-zero job will burn through the full retry budget again before
re-dying, which is slower to detect than an immediate re-death would be.
I accept that trade-off because DLQ retries are rare, human-initiated
events, not something in a hot path — optimizing for "give the operator
what they asked for" over "fail fast a second time" is the right
default here.

## 4. What designs did you consider and rejected for `worker stop` (cross-process signaling), and why?

**Chosen: DB-row-based PID registry.** Each worker inserts/updates its
own row in the `workers` table (`worker_id`, `pid`, `status`,
`heartbeat_at`) on start. `worker stop` queries `SELECT pid FROM workers
WHERE status='running'` and calls `os.kill(pid, SIGTERM)` on each one.

Considered and rejected:

- **PID files on disk** (e.g. one file per worker in a `.queuectl/`
  directory). Functionally almost identical to the DB approach, but adds
  a second source of truth that has to be kept in sync with the
  database (what happens if the file write succeeds but the DB insert
  fails, or vice versa?) for no real benefit, since we already have a
  transactional store sitting right there. Rejected for the extra
  consistency surface with no upside.
- **A control socket / named pipe** that `worker start` listens on and
  `worker stop` writes a "stop" message to. This is more "correct" in
  spirit (an explicit control channel instead of repurposing signals),
  but adds a listener thread inside every worker, a socket-path
  management problem (permissions, cleanup of stale sockets after a
  crash), and doesn't actually solve anything `SIGTERM` doesn't already
  solve for free — `SIGTERM` *is* a perfectly good "please stop"
  message that Python can trap. Rejected as unnecessary complexity for
  this problem size.
- **Process-group based signaling** (`worker start` puts all its
  children in one process group, `worker stop` sends the signal to the
  whole group via a group-id file). This only works for workers spawned
  by *this specific* `worker start` invocation; it doesn't generalize to
  "signal every worker regardless of which terminal/invocation started
  it," which the assignment explicitly requires. Rejected for not
  meeting the actual requirement.
- **A single always-on daemon** that owns all workers and exposes a stop
  RPC. Much heavier than the problem calls for, and reintroduces "what
  if the daemon itself crashes" as a new failure mode we'd then need to
  recover from — the DB already *is* the durable, crash-tolerant shared
  state; adding a daemon on top of it just to relay stop signals doesn't
  buy anything.

The DB-row approach also has one nice side effect for free: if a
worker's process is already dead (crashed) when `worker stop` tries to
signal it, `os.kill` raises `ProcessLookupError`, which we catch and use
to mark the stale row `stopped` — so `worker stop` and `status` stay
accurate without any extra bookkeeping.

## 5. If priorities were added tomorrow (high-priority jobs jump the queue), which parts of your design survive unchanged and which break?

**Survives unchanged:**
- The atomic claim mechanism (`BEGIN IMMEDIATE` + conditional UPDATE) —
  priority only changes *which* job the SELECT picks, not how the
  claim is made atomic.
- Crash recovery (`reap_stale_jobs`) — completely orthogonal to
  ordering; a stale job goes back to `pending` regardless of priority.
- Retry/backoff/DLQ state machine — unaffected; a retried job would
  simply re-enter the queue at whatever priority it already has.
- `worker stop` / graceful shutdown — no interaction with ordering at
  all.
- The `workers` and `config` tables — untouched.

**Breaks / needs changes:**
- The `jobs` table needs a `priority` column, and the claim query's
  `ORDER BY created_at ASC` becomes `ORDER BY priority DESC, created_at
  ASC` (still `LIMIT 1`, still inside the same `BEGIN IMMEDIATE` block —
  the atomicity story is identical, only the ordering predicate
  changes).
- `enqueue` needs an optional `priority` field in the job JSON.
- Starvation becomes a real risk once priority exists (a constant stream
  of high-priority jobs could indefinitely delay low-priority ones) —
  would need either aging (priority creeps up with wait time) or a
  reserved fraction of worker capacity for lower priorities. That's new
  design work, not something the current schema/queue gives you for
  free.
- `list`/`status` output would likely want to show/sort by priority for
  operator visibility, though this is cosmetic, not structural.

In short: the *concurrency-safety* and *recovery* stories don't change
at all — they only ever cared about single-row transitions, never about
ordering. Only the "which row does the SELECT pick" question changes,
plus the new starvation concern that priority always introduces.
