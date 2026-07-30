#!/usr/bin/env python3
"""
__main__.py — Entry point for `python3 -m queuectl.cli`

Enables invocation as:
    python3 -m queuectl.cli
"""

from queuectl.cli.entrypoint import app

if __name__ == "__main__":
    app()
