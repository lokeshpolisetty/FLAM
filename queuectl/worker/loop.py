"""
worker.loop — the worker process event loop.

Each worker process runs this loop independently. On every iteration the
worker reaps stale jobs (so crash recovery does not depend on any single
process surviving), promotes failed jobs whose retry window has elapsed,
refreshes its own heartbeat, and then claims and executes one pending job.

A SIGINT or SIGTERM sets a flag that is checked between jobs, allowing
the worker to finish its current job before exiting cleanly.
"""

import os
import signal
import time

from queuectl.database import connection as db_connection
from queuectl.database import job_repository, worker_repository
from queuectl.worker import executor


def worker_main_loop(config: dict) -> None:
    """
    Entry point for a worker child process.

    Accepts the full configuration dict (already loaded by the parent
    process before forking) so the worker never touches the config table
    on startup — only the jobs and workers tables.
    """
    worker_id = f"w-{os.getpid()}"
    stop_requested = {"flag": False}

    def handle_signal(signum, frame):
        stop_requested["flag"] = True

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    conn = db_connection.get_connection()
    worker_repository.register_worker(conn, worker_id, os.getpid())

    poll_interval = float(config.get("poll-interval", 1))
    recovery_timeout = float(config.get("recovery-timeout", 15))
    heartbeat_interval = float(config.get("heartbeat-interval", 3))

    print(f"[worker {worker_id}] started (pid={os.getpid()})", flush=True)

    try:
        while True:
            reaped = job_repository.reap_stale_jobs(conn, recovery_timeout)
            for job_id in reaped:
                print(
                    f"[worker {worker_id}] recovered stale job {job_id}",
                    flush=True,
                )

            job_repository.promote_ready_retries(conn)
            worker_repository.touch_worker_heartbeat(conn, worker_id)

            if stop_requested["flag"]:
                break

            job = job_repository.claim_next_job(conn, worker_id)
            if job is None:
                time.sleep(poll_interval)
                continue

            print(
                f"[worker {worker_id}] running job {job['id']}: {job['command']!r}",
                flush=True,
            )

            try:
                returncode = executor.execute_job(conn, job, worker_id, heartbeat_interval)
            except Exception as error:
                print(
                    f"[worker {worker_id}] ERROR in execute_job for {job['id']}: "
                    f"{error} — marking failed and continuing",
                    flush=True,
                )
                returncode = 1

            job_repository.finish_job(conn, job["id"], worker_id, returncode)
            print(
                f"[worker {worker_id}] job {job['id']} exited {returncode}",
                flush=True,
            )
    finally:
        worker_repository.mark_worker_stopped(conn, worker_id)
        conn.close()
        print(f"[worker {worker_id}] stopped", flush=True)
