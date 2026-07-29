"""
cli.job_commands — 'enqueue' and 'list' command implementations.
"""

import json
import sqlite3
import sys

import typer

from queuectl.cli.main import app, init_db_safe
from queuectl.database import connection as db_connection
from queuectl.database import job_repository
from queuectl.config import settings


def _job_to_public_dict(row: dict) -> dict:
    """Serialize a job database row to the public API representation."""
    return {
        "id": row["id"],
        "command": row["command"],
        "state": row["state"],
        "attempts": row["attempts"],
        "max_retries": row["max_retries"],
        "backoff_base": row["backoff_base"],
        "worker_id": row["worker_id"],
        "heartbeat_at": row["heartbeat_at"],
        "next_retry_at": row["next_retry_at"],
        "last_error": row["last_error"],
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
    }


@app.command()
def enqueue(
    job_json: str = typer.Argument(
        ..., help="JSON object, e.g. '{\"id\":\"job1\",\"command\":\"sleep 2\"}'"
    )
):
    """Add a new job to the queue."""
    init_db_safe()

    try:
        data = json.loads(job_json)
    except json.JSONDecodeError as error:
        typer.echo(f"Invalid JSON: {error}", err=True)
        raise typer.Exit(code=1)

    if not isinstance(data, dict):
        typer.echo("Job JSON must be a JSON object, not an array or primitive", err=True)
        raise typer.Exit(code=1)

    if "id" not in data or "command" not in data:
        typer.echo("Job JSON must include at least 'id' and 'command'", err=True)
        raise typer.Exit(code=1)

    if data["id"] is None or data["command"] is None:
        typer.echo("Job 'id' and 'command' must not be null", err=True)
        raise typer.Exit(code=1)

    job_id = str(data["id"]).strip()
    if not job_id:
        typer.echo("Job 'id' must not be empty or whitespace-only", err=True)
        raise typer.Exit(code=1)

    conn = db_connection.get_connection()
    try:
        cfg = settings.get_all(conn)
        ts = db_connection.now_iso()

        try:
            max_retries = int(data.get("max_retries", cfg["max-retries"]))
            backoff_base = float(data.get("backoff_base", cfg["backoff-base"]))
        except (ValueError, TypeError) as error:
            typer.echo(f"Invalid job parameters: {error}", err=True)
            raise typer.Exit(code=1)

        if max_retries < 0:
            typer.echo(f"max_retries must be >= 0, got {max_retries}", err=True)
            raise typer.Exit(code=1)
        if backoff_base < 0:
            typer.echo(f"backoff_base must be >= 0, got {backoff_base}", err=True)
            raise typer.Exit(code=1)

        try:
            conn.execute(
                """INSERT INTO jobs (id, command, state, attempts, max_retries, backoff_base,
                   created_at, updated_at) VALUES (?, ?, 'pending', 0, ?, ?, ?, ?)""",
                (job_id, str(data["command"]), max_retries, backoff_base, ts, ts),
            )
        except sqlite3.IntegrityError:
            typer.echo(f"Job with id '{job_id}' already exists", err=True)
            raise typer.Exit(code=1)
        except sqlite3.OperationalError as error:
            if "readonly" in str(error).lower():
                typer.echo("Cannot enqueue: database is read-only", err=True)
            else:
                typer.echo(f"Database error: {error}", err=True)
            raise typer.Exit(code=1)
        typer.echo(f"Enqueued job '{job_id}'")
    finally:
        conn.close()


@app.command("list")
def list_jobs(
    state: str = typer.Option(None, "--state", help="Filter by job state."),
    as_json: bool = typer.Option(False, "--json", help="Print a JSON array to stdout."),
):
    """List jobs, optionally filtered by state."""
    init_db_safe()
    conn = db_connection.get_connection()
    try:
        recovery_timeout = float(settings.get(conn, "recovery-timeout") or 15)
        job_repository.reap_stale_jobs(conn, recovery_timeout)
        job_repository.promote_ready_retries(conn)

        valid_states = {"pending", "processing", "completed", "failed", "dead"}
        if state:
            if state not in valid_states:
                typer.echo(
                    f"Invalid state '{state}'. Must be one of: "
                    + ", ".join(sorted(valid_states)),
                    err=True,
                )
                raise typer.Exit(code=1)
            rows = conn.execute(
                "SELECT * FROM jobs WHERE state = ? ORDER BY created_at ASC", (state,)
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM jobs ORDER BY created_at ASC"
            ).fetchall()

        jobs = [_job_to_public_dict(dict(row)) for row in rows]

        if as_json:
            sys.stdout.write(json.dumps(jobs))
            sys.stdout.write("\n")
        else:
            if not jobs:
                typer.echo("No jobs found.")
            for job in jobs:
                typer.echo(
                    f"{job['id']:<20} {job['state']:<11} "
                    f"attempts={job['attempts']}/{job['max_retries']}  {job['command']}"
                )
    finally:
        conn.close()
