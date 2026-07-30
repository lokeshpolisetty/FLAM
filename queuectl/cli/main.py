"""
cli.main — Typer application root: sub-app registration and database initialisation.
"""

import sqlite3
import time
import typer

from queuectl.database import schema as db_schema

app = typer.Typer(add_completion=False, help="queuectl — a small persistent job queue.")
worker_app = typer.Typer(help="Manage worker processes.")
dlq_app = typer.Typer(help="Inspect and retry dead-lettered jobs.")
config_app = typer.Typer(help="Get/set persisted configuration.")

app.add_typer(worker_app, name="worker")
app.add_typer(dlq_app, name="dlq")
app.add_typer(config_app, name="config")


def init_db_safe() -> None:
    """
    Initialise the database, retrying on transient lock failures.

    Uses short 100 ms sleeps so that a pending SIGTERM (when the process
    list is empty) can interrupt the init loop rather than blocking for
    the full SQLite busy_timeout (30 s).
    """
    for _ in range(60):  # up to ~6 s of retries at 100 ms each
        try:
            db_schema.init_db()
            return
        except sqlite3.OperationalError as error:
            message = str(error).lower()
            if "readonly" in message:
                typer.echo("Cannot write: database is read-only", err=True)
                raise typer.Exit(code=1)
            if "locked" in message or "unable to open" in message:
                time.sleep(0.1)
                continue
            typer.echo(f"Error accessing database file: {error}", err=True)
            raise typer.Exit(code=1)
    typer.echo("Database is locked after 6 s; giving up.", err=True)
    raise typer.Exit(code=1)
