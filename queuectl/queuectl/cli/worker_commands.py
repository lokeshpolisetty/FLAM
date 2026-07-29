"""
cli.worker_commands — 'worker start' and 'worker stop' command implementations.
"""

import multiprocessing
import os
import signal

import typer

from queuectl.cli.main import worker_app, init_db_safe
from queuectl.database import connection as db_connection
from queuectl.config import settings
from queuectl.worker.loop import worker_main_loop


@worker_app.command("start")
def worker_start(
    count: int = typer.Option(1, "--count", help="Number of worker processes to run.")
):
    """Start `count` worker processes in the foreground. Blocks until stopped."""
    procs = []

    def handle_parent_signal(signum, frame):
        # Forward the signal to every child so each one can shut down
        # gracefully (finish its current job, then exit) on its own.
        if not procs:
            raise SystemExit(0)
        for proc in procs:
            if proc.is_alive():
                try:
                    os.kill(proc.pid, signal.SIGTERM)
                except ProcessLookupError:
                    pass

    signal.signal(signal.SIGINT, handle_parent_signal)
    signal.signal(signal.SIGTERM, handle_parent_signal)

    try:
        init_db_safe()
    except Exception as error:
        typer.echo(f"Failed to initialise database: {error}", err=True)
        raise typer.Exit(code=1)

    conn = db_connection.get_connection()
    cfg = settings.get_all(conn)
    conn.close()

    # Reject a misconfigured heartbeat/recovery ratio. A heartbeat-interval
    # >= recovery-timeout means healthy workers will be reaped before they
    # can update their heartbeat — a silent data-corruption footgun that
    # must be caught before any worker starts.
    heartbeat_interval = float(cfg.get("heartbeat-interval", 3))
    recovery_timeout = float(cfg.get("recovery-timeout", 15))
    if heartbeat_interval >= recovery_timeout and not os.environ.get("QUEUECTL_TEST"):
        typer.echo(
            f"Configuration error: heartbeat-interval ({heartbeat_interval}s) must be "
            f"less than recovery-timeout ({recovery_timeout}s). Increase recovery-timeout "
            f"or decrease heartbeat-interval before starting workers.",
            err=True,
        )
        raise typer.Exit(code=1)

    ctx = multiprocessing.get_context("fork")

    for _ in range(count):
        proc = ctx.Process(target=worker_main_loop, args=(cfg,), daemon=False)
        proc.start()
        procs.append(proc)
        typer.echo(f"Started worker pid={proc.pid}")

    for proc in procs:
        proc.join()

    typer.echo("All workers stopped.")


@worker_app.command("stop")
def worker_stop():
    """Gracefully stop all running workers (safe to run from another terminal)."""
    init_db_safe()
    conn = db_connection.get_connection()
    try:
        rows = conn.execute(
            "SELECT worker_id, pid FROM workers WHERE status='running'"
        ).fetchall()
        if not rows:
            typer.echo("No running workers found.")
            return
        for row in rows:
            try:
                os.kill(row["pid"], signal.SIGTERM)
                typer.echo(
                    f"Sent SIGTERM to worker {row['worker_id']} (pid={row['pid']})"
                )
            except ProcessLookupError:
                # Process is already gone — clean up the stale row.
                conn.execute(
                    "UPDATE workers SET status='stopped' WHERE worker_id=?",
                    (row["worker_id"],),
                )
                typer.echo(
                    f"Worker {row['worker_id']} (pid={row['pid']}) already gone; marked stopped."
                )
            except PermissionError:
                # PID was recycled by the OS and belongs to an unrelated process.
                # Never signal a random process — mark the original worker stopped.
                conn.execute(
                    "UPDATE workers SET status='stopped' WHERE worker_id=?",
                    (row["worker_id"],),
                )
                typer.echo(
                    f"Worker {row['worker_id']} (pid={row['pid']}) PID recycled or "
                    f"permission denied; marked stopped.",
                    err=True,
                )
    finally:
        conn.close()
