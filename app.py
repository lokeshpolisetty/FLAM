#!/usr/bin/env python3
"""
app.py — CLI entry point.

Professional production entry point for the queuectl CLI. This file exists to enable:
  1. Direct invocation: python3 app.py <command> <args>
  2. Test subprocess invocation: subprocess.run([sys.executable, "app.py", ...])
  3. Programmatic access: from app import app
"""

from queuectl.cli.entrypoint import app

if __name__ == "__main__":
    app()
