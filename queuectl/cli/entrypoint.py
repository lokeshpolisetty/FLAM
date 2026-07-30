"""
cli.entrypoint — imports all command modules to trigger Typer registration,
then exposes the root `app` for use by the top-level entry point.

Typer commands are registered as a side effect of importing the module
that contains the @app.command() / @sub_app.command() decorator calls.
This module is the single place that ensures every command group is loaded
before the CLI is invoked.
"""

# Importing these modules registers their @app.command decorators.
import queuectl.cli.job_commands      # noqa: F401  enqueue, list
import queuectl.cli.status_command    # noqa: F401  status
import queuectl.cli.worker_commands   # noqa: F401  worker start, worker stop
import queuectl.cli.dlq_commands      # noqa: F401  dlq list, dlq retry
import queuectl.cli.config_commands   # noqa: F401  config get, config set

from queuectl.cli.main import app

__all__ = ["app"]

if __name__ == "__main__":
    app()
