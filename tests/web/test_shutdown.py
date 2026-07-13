"""Stop-server button: always-visible nav control that gracefully stops
the local server from the browser.

POST /shutdown schedules a SIGINT to the running process (after a short
delay so the response flushes) and returns a self-contained goodbye
page — self-contained because the server is gone moments later, so it
can't reference /static assets. Graceful SIGINT means uvicorn runs the
lifespan shutdown hook, which already regenerates report.html.
"""

from __future__ import annotations

import sqlite3
import time

from finances.web.routers import pages


def test_nav_has_stop_server_button(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    body = web_client_factory().get("/").text
    assert 'action="/shutdown"' in body
    assert "Stop server" in body
    # Guard against fat-finger shutdowns.
    assert "confirm(" in body


def test_shutdown_returns_goodbye_and_schedules_terminate(
    seeded_web_db: sqlite3.Connection, web_client_factory, monkeypatch
) -> None:
    fired: list[bool] = []
    monkeypatch.setattr(pages, "_terminate", lambda: fired.append(True))
    monkeypatch.setattr(pages, "_SHUTDOWN_DELAY_SECONDS", 0.01)

    resp = web_client_factory().post("/shutdown")

    assert resp.status_code == 200
    assert "stopped" in resp.text.lower()
    # Goodbye page must be self-contained — no /static references.
    assert "/static/" not in resp.text

    deadline = time.monotonic() + 2.0
    while not fired and time.monotonic() < deadline:
        time.sleep(0.02)
    assert fired, "terminate was never scheduled"


def test_shutdown_rejects_get(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    resp = web_client_factory().get("/shutdown")
    assert resp.status_code == 405
