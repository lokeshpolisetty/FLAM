"""
cli.dlq_commands — 'dlq list' and 'dlq retry' command implementations.

The Dead Letter Queue holds jobs that have exhausted their retry budget.
'dlq retry' resets attempts to 0 — a manual retry is an operator decision
made after investigating the root cause, not continuation of a failed run.
See DECISIONS.md Q3 for the full reasoning.
"""

import json
import sys

import typer

from queuectl.cli.main import dlq_app, init_db_safe
from queuectl.database import connection as db_connection
from queuectl.cli.job_commands import _job_to_public_dict


@dlq_app.command("list")
def dlq_list(
    as_json: bool = typer.Option(False, "--json")
):
    """List jobs currently in the Dead Letter Queue."""
    init_db_safe()
    conn = db_connection.get_connection()
    try:
        rows = conn.execute(
            "SELECT * FROM jobs WHERE state='dead' ORDER BY updated_at DESC"
        ).fetchall()
        jobs = [_job_to_public_dict(dict(row)) for row in rows]
        if as_json:
            sys.stdout.write(json.dumps(jobs))
            sys.stdout.write("\n")
        else:
            if not jobs:
                typer.echo("DLQ is empty.")
            for job in jobs:
                typer.echo(
                    f"{job['id']:<20} attempts={job['attempts']}  last_error={job['last_error']}"
                )
    finally:
        conn.close()


@dlq_app.command("retry")
def dlq_retry(job_id: str = typer.Argument(...)):
    """
    Re-enqueue a dead job.

    Resets attempts to 0 — a DLQ retry is an operator asserting that
    conditions have changed and the job deserves a full, fresh run.
    Preserving the old attempt count would let a healthy-again job skip
    straight back to 'dead' after a single failure, defeating the purpose.
    """
    init_db_safe()
    conn = db_connection.get_connection()
    try:
        conn.execute("BEGIN IMMEDIATE")
        row = conn.execute(
            "SELECT * FROM jobs WHERE id = ? AND state = 'dead'", (job_id,)
        ).fetchone()
        if row is None:
            conn.execute("ROLLBACK")
            typer.echo(f"No dead job with id '{job_id}' found.", err=True)
            raise typer.Exit(code=1)
        ts = db_connection.now_iso()
        conn.execute(
            """UPDATE jobs SET state='pending', attempts=0, next_retry_at=NULL,
               worker_id=NULL, heartbeat_at=NULL, last_error=NULL, updated_at=?
               WHERE id=?""",
            (ts, job_id),
        )
        conn.execute("COMMIT")
        typer.echo(f"Re-enqueued job '{job_id}' (attempts reset to 0).")
    finally:
        conn.close()
