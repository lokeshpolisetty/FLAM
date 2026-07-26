"""
database — connection, schema, and repository layers.

The module-level names (DB_PATH, init_db, touch_job_heartbeat, …) are forwarded
directly to their canonical sub-module so that pytest's monkeypatch.setattr works
transparently:

    monkeypatch.setattr(db, "touch_job_heartbeat", mock)

propagates the patch to queuectl.database.job_repository.touch_job_heartbeat,
which is the actual call site inside executor.py and the worker loop.
"""

import sys
import types

from . import connection
from . import schema
from . import job_repository
from . import worker_repository

# Public re-exports for convenience
from .connection import DB_PATH, get_connection, now_iso
from .schema import SCHEMA, DEFAULT_CONFIG, init_db
from .job_repository import (
    claim_next_job,
    touch_job_heartbeat,
    finish_job,
    reap_stale_jobs,
    promote_ready_retries,
)
from .worker_repository import (
    register_worker,
    touch_worker_heartbeat,
    mark_worker_stopped,
)

# ---------------------------------------------------------------------------
# Forwarding table: attribute name → (sub-module, attribute)
# Any monkeypatch.setattr(db, name, value) is propagated so that every
# internal call site that does `from queuectl.database import job_repository`
# and then calls `job_repository.touch_job_heartbeat(…)` also sees the mock.
# ---------------------------------------------------------------------------
_FORWARD_TABLE: dict = {
    "DB_PATH":               (connection,        "DB_PATH"),
    "get_connection":        (connection,        "get_connection"),
    "now_iso":               (connection,        "now_iso"),
    "init_db":               (schema,            "init_db"),
    "claim_next_job":        (job_repository,    "claim_next_job"),
    "touch_job_heartbeat":   (job_repository,    "touch_job_heartbeat"),
    "finish_job":            (job_repository,    "finish_job"),
    "reap_stale_jobs":       (job_repository,    "reap_stale_jobs"),
    "promote_ready_retries": (job_repository,    "promote_ready_retries"),
    "register_worker":       (worker_repository, "register_worker"),
    "touch_worker_heartbeat":(worker_repository, "touch_worker_heartbeat"),
    "mark_worker_stopped":   (worker_repository, "mark_worker_stopped"),
}


class _ForwardingModule(types.ModuleType):
    """
    Module subclass that propagates attribute writes to canonical sub-modules.

    When pytest calls monkeypatch.setattr(db, "touch_job_heartbeat", mock) this
    __setattr__ ensures queuectl.database.job_repository.touch_job_heartbeat is
    replaced too — so executor.py, which imports the function via job_repository,
    sees the mock without needing to be aware of the test infrastructure.

    DB_PATH is handled the same way: writing db.DB_PATH also writes to
    queuectl.database.connection.DB_PATH, and __getattr__ always reads back from
    the canonical source so the value is consistent regardless of which path the
    patch used.
    """

    def __setattr__(self, name: str, value) -> None:
        if name in _FORWARD_TABLE:
            target_module, target_attr = _FORWARD_TABLE[name]
            setattr(target_module, target_attr, value)
        super().__setattr__(name, value)

    def __getattr__(self, name: str):
        if name == "DB_PATH":
            return connection.DB_PATH
        raise AttributeError(
            f"module 'queuectl.database' has no attribute {name!r}"
        )


# Replace this module in sys.modules with the forwarding subclass instance.
_forwarding = _ForwardingModule(__name__, __doc__)
_forwarding.__dict__.update(
    {k: v for k, v in globals().items() if not k.startswith("_") and k != "DB_PATH"}
)
_forwarding.DB_PATH = connection.DB_PATH
sys.modules[__name__] = _forwarding
