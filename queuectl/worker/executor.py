"""
worker.executor — subprocess execution with heartbeat and orphan prevention.

Responsibilities:
- Launch a shell command in its own process group.
- Periodically refresh the job's heartbeat while the subprocess runs.
- Kill the entire process group on unexpected termination to prevent orphans.
- Redirect subprocess stdio to /dev/null to prevent pipe-buffer deadlocks
  for jobs that produce large output.
"""

import os
import signal
import subprocess
import time
import sqlite3

from queuectl.database import job_repository


def execute_job(
    conn: sqlite3.Connection,
    job: dict,
    worker_id: str,
    heartbeat_interval: float,
) -> int:
    """
    Run job['command'] via the shell and return its exit code.

    The subprocess is started in its own session (start_new_session=True)
    so that on cleanup SIGKILL can be sent to the entire process group,
    preventing shell-spawned grandchildren from becoming orphans.

    Heartbeat errors are swallowed so a transient database write failure
    does not crash the worker mid-job — the worst case is one missed
    heartbeat update, which the recovery mechanism handles gracefully.
    """
    proc = subprocess.Popen(
        job["command"],
        shell=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
    )
    last_heartbeat = time.time()
    try:
        while proc.poll() is None:
            time.sleep(0.2)
            if time.time() - last_heartbeat >= heartbeat_interval:
                try:
                    job_repository.touch_job_heartbeat(conn, job["id"], worker_id)
                except Exception as error:
                    print(
                        f"[worker {worker_id}] heartbeat update failed for job "
                        f"{job['id']}: {error} (continuing)",
                        flush=True,
                    )
                last_heartbeat = time.time()
        return proc.returncode
    except KeyboardInterrupt:
        # Ctrl+C arriving mid-wait: let the subprocess finish; the actual
        # shutdown flag is checked by the worker loop's signal handler.
        proc.wait()
        return proc.returncode
    except BaseException:
        # Any other unexpected error — kill the child process group to
        # avoid orphans, then re-raise so the worker loop can handle it.
        try:
            os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
        except (ProcessLookupError, OSError):
            pass
        proc.wait()
        raise
