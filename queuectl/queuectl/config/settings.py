"""
config.settings — read/write the persisted configuration table.

Configuration values are global operational knobs (heartbeat-interval,
recovery-timeout, poll-interval) and the default policy values applied
to newly enqueued jobs (max-retries, backoff-base). Per-job values are
snapshotted onto each job row at enqueue time, so runtime config changes
only ever affect jobs created after the change.
"""

import sqlite3

KNOWN_KEYS: dict = {
    "max-retries": int,
    "backoff-base": float,
    "heartbeat-interval": float,
    "recovery-timeout": float,
    "poll-interval": float,
}


def get_all(conn: sqlite3.Connection) -> dict:
    """Return all configuration key-value pairs as a plain dict."""
    rows = conn.execute("SELECT key, value FROM config").fetchall()
    return {row["key"]: row["value"] for row in rows}


def get(conn: sqlite3.Connection, key: str) -> str | None:
    """Return the string value for a single key, or None if absent."""
    row = conn.execute(
        "SELECT value FROM config WHERE key = ?", (key,)
    ).fetchone()
    return row["value"] if row else None


def set(conn: sqlite3.Connection, key: str, value: str) -> None:
    """
    Persist a configuration value.

    If the key is one of the well-known typed keys, the value is
    validated by attempting a type conversion before writing. This
    surfaces bad values with a clear error at set-time rather than
    failing silently deep in a worker loop.
    """
    if key in KNOWN_KEYS:
        KNOWN_KEYS[key](value)
    conn.execute(
        "INSERT INTO config (key, value) VALUES (?, ?) "
        "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        (key, str(value)),
    )
