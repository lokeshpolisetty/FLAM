"""
cli.config_commands — 'config get' and 'config set' command implementations.
"""

import typer

from queuectl.cli.main import config_app, init_db_safe
from queuectl.database import connection as db_connection
from queuectl.config import settings


@config_app.command("set")
def config_set(
    key: str = typer.Argument(...),
    value: str = typer.Argument(...),
):
    """Set a configuration value, e.g. `queuectl config set max-retries 5`."""
    init_db_safe()
    conn = db_connection.get_connection()
    try:
        try:
            settings.set(conn, key, value)
        except ValueError as error:
            typer.echo(f"Invalid value for '{key}': {error}", err=True)
            raise typer.Exit(code=1)
        typer.echo(f"Set {key} = {value}")
    finally:
        conn.close()


@config_app.command("get")
def config_get(key: str = typer.Argument(None)):
    """Get a configuration value, or list all if no key is given."""
    init_db_safe()
    conn = db_connection.get_connection()
    try:
        if key is None:
            for k, v in settings.get_all(conn).items():
                typer.echo(f"{k} = {v}")
        else:
            value = settings.get(conn, key)
            if value is None:
                typer.echo(f"No such key '{key}'", err=True)
                raise typer.Exit(code=1)
            typer.echo(value)
    finally:
        conn.close()
