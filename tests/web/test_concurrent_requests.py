"""Two requests in flight at once must both answer (2026-09-05).

The viewer 500'd with

    sqlite3.ProgrammingError: SQLite objects created in a thread can only
    be used in that same thread. The object was created in thread id
    6249541632 and this is thread id 6181089280.

whenever htmx had more than one request open — typing in the Flow search
box is the everyday way to get there, because each keystroke fires one.

The cause is not the search. ``get_conn`` is a *sync generator*
dependency, and FastAPI runs a sync dependency's setup, the endpoint body
and the teardown as separate hops into the anyio worker pool. Sequential
requests keep landing on the same idle worker, so the bug is invisible;
overlapping ones do not, and the connection opened on worker A is then
executed on worker B.

The fix is ``check_same_thread=False``: one connection per request, used
by one thread at a time, which is the standard FastAPI/SQLite pairing.
"""

from __future__ import annotations

import asyncio
import sqlite3
from collections.abc import Callable

import httpx
from fastapi.testclient import TestClient

#: Enough parallelism that anyio must hand out more than one worker.
FANOUT = 12


def _hammer(app, paths: list[str]) -> list[httpx.Response]:
    """Fire every path at ``app`` concurrently on ONE event loop.

    ``TestClient`` opens a fresh portal per request, so calling it from
    several threads gives each its own loop and its own worker pool — the
    races never meet. One loop plus ``gather`` is what the browser does.
    """

    async def _run() -> list[httpx.Response]:
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://testserver"
        ) as client:
            return await asyncio.gather(*(client.get(p) for p in paths))

    return asyncio.run(_run())


def test_concurrent_searches_all_answer(
    seeded_web_db: sqlite3.Connection,
    web_client_factory: Callable[[], TestClient],
) -> None:
    """Twelve overlapping searches: twelve 200s, no cross-thread error."""
    app = web_client_factory().app
    paths = [f"/transactions?q=row{n}" for n in range(FANOUT)]

    responses = _hammer(app, paths)

    assert [r.status_code for r in responses] == [200] * FANOUT


def test_concurrent_mixed_pages_all_answer(
    seeded_web_db: sqlite3.Connection,
    web_client_factory: Callable[[], TestClient],
) -> None:
    """Not a search bug — any two pages at once hit the same connection."""
    app = web_client_factory().app
    paths = ["/", "/transactions", "/accounts", "/monthly", "/rates"] * 3

    responses = _hammer(app, paths)

    assert [r.status_code for r in responses] == [200] * len(paths)
