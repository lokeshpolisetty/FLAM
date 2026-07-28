"""
cli.status_command — 'status' command implementation.

Reports a summary of job state counts and lists all currently running workers,
cleaning up any stale worker rows whose PIDs no longer exist.
"""

import os

import typer

from queuectl.cli.main import app, init_db_safe
from queuectl.database import connection as db_connection
from queuectl.database import job_repository
from queuectl.config import settings


@app.command()
def status():
    """Summary of job states and active workers."""
    init_db_safe()
    conn = db_connection.get_connection()
    try:
        recovery_timeout = float(settings.get(conn, "recovery-timeout") or 15)
        job_repository.reap_stale_jobs(conn, recovery_timeout)
        job_repository.promote_ready_retries(conn)

        counts = {"pending": 0, "processing": 0, "completed": 0, "failed": 0, "dead": 0}
        for row in conn.execute("SELECT state, COUNT(*) AS c FROM jobs GROUP BY state"):
            if row["state"] in counts:
                counts[row["state"]] = row["c"]

        workers = conn.execute(
            "SELECT worker_id, pid, status, heartbeat_at FROM workers"
        ).fetchall()
        alive_workers = []
        for worker in workers:
            if worker["status"] != "running":
                continue
            try:
                os.kill(worker["pid"], 0)
                alive_workers.append(worker)
            except (ProcessLookupError, PermissionError):
                # PermissionError: PID was recycled and belongs to another user's process.
                conn.execute(
                    "UPDATE workers SET status='stopped' WHERE worker_id=?",
                    (worker["worker_id"],),
                )

        typer.echo("Job states:")
        for state, count in counts.items():
            typer.echo(f"  {state:<10} {count}")
        typer.echo(f"Running workers: {len(alive_workers)}")
        for worker in alive_workers:
            typer.echo(f"  {worker['worker_id']} (pid={worker['pid']})")
    finally:
        conn.close()
