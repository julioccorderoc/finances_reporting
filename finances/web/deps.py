"""FastAPI dependency wiring for the web viewer (EPIC-022 / ADR-012).

A single per-request ``sqlite3.Connection`` dependency is exposed
so route handlers can do::

    @router.get("/foo")
    def foo(conn: sqlite3.Connection = Depends(get_conn)):
        ...

The connection is configured to mirror ``finances.db.connection.get_connection``
behaviour (Row factory, FK enforcement) but is per-request rather than
process-wide. The DB path lives on ``app.state.settings`` so tests can
swap it out at app-construction time.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator

from fastapi import Request

from finances.web.settings import WebSettings


def get_settings(request: Request) -> WebSettings:
    """Return the ``WebSettings`` stashed on ``app.state`` by ``create_app``."""
    settings = getattr(request.app.state, "settings", None)
    if settings is None:
        raise RuntimeError(
            "WebSettings not found on app.state — was create_app() called?"
        )
    return settings


def get_conn(request: Request) -> Iterator[sqlite3.Connection]:
    """Yield a per-request ``sqlite3.Connection`` to the configured DB.

    Mirrors the production connection setup: ``Row`` factory, FK on,
    autocommit (``isolation_level=None``). The connection is closed on
    teardown regardless of handler outcome.
    """
    settings = get_settings(request)
    # Lazy import to keep import-time cycles off the Decimal adapter
    # registration in finances.db.connection.
    from finances.db.connection import _register_decimal_adapters

    _register_decimal_adapters()
    conn = sqlite3.connect(
        str(settings.db_path),
        detect_types=sqlite3.PARSE_DECLTYPES,
        isolation_level=None,
    )
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    try:
        yield conn
    finally:
        conn.close()
