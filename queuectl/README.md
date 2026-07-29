# queuectl

A small, persistent, CLI-driven background job queue. Jobs are shell
commands; multiple worker **processes** (real OS processes, not threads)
pull jobs off a SQLite-backed queue, retry failures with exponential
backoff, and dead-letter anything that fails too many times. A crashed
worker (even `SIGKILL`) never leaves a job stuck — it's automatically
reclaimed within a bounded time.

## Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Requires Python 3.11+ and bash. No external services — everything lives
in a single SQLite file (`queue.db` by default, override with the
`QUEUECTL_DB` environment variable, which is also how the test suite
isolates each test run).

## Usage

```bash
# Add jobs
python3 app.py enqueue '{"id":"job1","command":"sleep 2"}'
python3 app.py enqueue '{"id":"job2","command":"exit 1","max_retries":2}'

# Start 3 workers in the foreground (open another terminal for the rest)
python3 app.py worker start --count 3

# From a different terminal:
python3 app.py status
python3 app.py list --state pending
python3 app.py list --state pending --json
python3 app.py dlq list
python3 app.py dlq retry job2
python3 app.py config set max-retries 3
python3 app.py config set backoff-base 2
python3 app.py worker stop        # graceful stop, from any terminal
```

`Ctrl+C` (SIGINT) or `SIGTERM` on the `worker start` process also
triggers the same graceful shutdown: every worker finishes the job it's
currently running, then exits without picking up new work.

`SIGKILL` on a worker (`kill -9 <pid>`) simulates a hard crash. The job
it was running is automatically detected as abandoned and returned to
`pending` — worst case within `recovery-timeout` seconds (default 15) of
the next `reap` check, which every CLI command (and every worker loop
iteration, default every 1s) performs. See `DECISIONS.md` Q2 for the
full walkthrough.

## Architecture

```
app.py             Typer CLI: enqueue / worker start|stop / status /
                    list / dlq list|retry / config set|get
worker.py           Worker process main loop + job execution + signal
                    handling
db.py               SQLite schema, atomic job claim, crash recovery
                    (reap_stale_jobs), retry promotion, worker registry
config_service.py   Read/write the `config` table
tests/              Black-box pytest suite that drives the real CLI as
                    subprocesses, exactly like the grader's script will
```

**Storage: SQLite**, chosen specifically for its cross-process file
locking (a single writer at a time, enforced by the OS beneath us) —
that's what makes the atomic job claim possible without inventing our
own locking protocol. WAL mode lets `status`/`list` read concurrently
without blocking a worker.

**Job lifecycle:**

```
pending -> processing -> completed
                       -> failed -> (backoff elapses) -> pending
                                                       -> dead   (after max_retries)
```

**Worker model:** `worker start --count N` forks N real child processes
(`multiprocessing.Process`, not threads). Each child registers its PID
in the `workers` table, then loops: reap stale jobs -> promote
ready retries -> refresh its own heartbeat -> (stop requested? exit) ->
atomically claim a job -> run it -> record the result. `worker stop`,
run from anywhere, reads that table and sends `SIGTERM` to each PID.

## Database schema

**jobs**: `id, command, state, attempts, max_retries, backoff_base,
worker_id, next_retry_at, heartbeat_at, last_error, created_at,
updated_at`

**workers**: `worker_id, pid, status, started_at, heartbeat_at`

**config**: `key, value`

## Configuration

| key | default | meaning |
|---|---|---|
| `max-retries` | 3 | default retry budget for newly enqueued jobs |
| `backoff-base` | 2 | default backoff base for newly enqueued jobs |
| `heartbeat-interval` | 3s | how often an in-flight job's heartbeat is refreshed |
| `recovery-timeout` | 15s | how long a stale heartbeat is tolerated before a job is reclaimed |
| `poll-interval` | 1s | how long an idle worker sleeps before checking again |

`max-retries` and `backoff-base` are **snapshotted onto each job at
enqueue time** — changing them only affects jobs created afterward. See
`DECISIONS.md` for the reasoning.

## Testing

```bash
python3 -m pytest tests/ -v
```

The suite is black-box: it shells out to `app.py` exactly as a real user
(or the grader's script) would, and covers:

- a basic job completing
- a failing job retrying with backoff and landing in the DLQ
- `dlq retry` resetting attempts
- 25 jobs across 4 concurrent worker processes, each running exactly once
- a worker being `SIGKILL`ed mid-job, with recovery verified after a
  fresh `worker start`
- jobs surviving a full restart (no worker process alive at all)
- graceful shutdown finishing the in-flight job but not starting a new one
- `list --json` producing pure, parseable JSON on stdout

## Demo recording

_(link to be added before submission)_

## Screenshots

_(to be added before submission — not required for functionality, just
for the README)_
