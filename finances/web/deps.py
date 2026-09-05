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


def open_conn(settings: WebSettings) -> sqlite3.Connection:
    """Open a connection the way every request does.

    Mirrors the production connection setup: ``Row`` factory, FK on,
    autocommit (``isolation_level=None``). The caller closes it. Shared
    by :func:`get_conn` and by the rail, which renders outside the
    dependency graph (see ``services/rail.py``).

    ``check_same_thread=False`` because a request is not a thread.
    FastAPI runs a sync dependency's setup, the endpoint body and the
    teardown as three separate hops into the anyio worker pool, and
    under any concurrency at all those land on different workers — the
    connection opened on one is then executed, and closed, on another.
    Sequential requests reuse one idle worker, which is why the viewer
    ran for months before htmx fired two requests at once (a keystroke
    in the Flow search box is enough) and every page started 500ing.

    This is safe *because* the connection is per-request: the hops are
    sequential, so no two threads ever touch it at the same moment.
    Sharing one connection between concurrent requests would not be, and
    nothing here does that.
    """
    # Lazy import to keep import-time cycles off the Decimal adapter
    # registration in finances.db.connection.
    from finances.db.connection import _register_decimal_adapters

    _register_decimal_adapters()
    conn = sqlite3.connect(
        str(settings.db_path),
        detect_types=sqlite3.PARSE_DECLTYPES,
        isolation_level=None,
        check_same_thread=False,
    )
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def get_conn(request: Request) -> Iterator[sqlite3.Connection]:
    """Yield a per-request ``sqlite3.Connection`` to the configured DB.

    The connection is closed on teardown regardless of handler outcome.
    """
    conn = open_conn(get_settings(request))
    try:
        yield conn
    finally:
        conn.close()


def dismissed_pairs(request: Request) -> set[str]:
    """Pair proposals the owner said "Not a pair" to, for this run.

    Held on ``app.state`` rather than in the database, because declining
    a GUESS is not a fact about the money: the two rows are unchanged, and
    the next statement may well make the same proposal worth another look.
    A restart forgets it, which is the same lifetime the design gives a
    sitting.
    """
    state = request.app.state
    dismissed: set[str] | None = getattr(state, "triage_dismissed_pairs", None)
    if dismissed is None:
        dismissed = set()
        state.triage_dismissed_pairs = dismissed
    return dismissed
