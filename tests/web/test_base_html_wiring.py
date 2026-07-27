"""The browser half of the staleness story, asserted at the markup level.

Reload keeps the SERVER matching the disk. It cannot keep a page that has
been open for twenty minutes matching the server, and it makes restarts
far more frequent — so the page has to notice when it has outlived the
process that rendered it, and say so without stealing focus from someone
mid-edit.

The failure listeners cover the other half. htmx fires ``sendError`` when
it cannot reach the server at all and ``timeout`` when a request never
answers; the app listened for NEITHER, so a server that was down,
restarting, or wedged on a syntax error produced an infinite spinner and
no feedback whatsoever. The supervisor makes brief unreachability a normal
occurrence, which turns that blind spot into a daily one.

Wiring tests by nature (rule-011 exempts these from the logic-coverage
bar) — they assert the handlers are bound, not what a browser does with
them.
"""

from __future__ import annotations

import re
import sqlite3
from typing import Callable

from starlette.testclient import TestClient


def _body(web_client_factory: Callable[[], TestClient]) -> str:
    with web_client_factory() as client:
        return client.get("/").text


def test_boot_meta_and_banner_are_present(
    web_db: sqlite3.Connection,
    web_client_factory: Callable[[], TestClient],
) -> None:
    body = _body(web_client_factory)

    assert 'name="finances-boot"' in body
    assert 'id="server-restarted"' in body
    assert "picked up new code" in body


def test_the_banner_never_reloads_on_its_own(
    web_db: sqlite3.Connection,
    web_client_factory: Callable[[], TestClient],
) -> None:
    """The owner may be mid-modal with unsaved input; reloading is theirs.

    The Reload control reuses the existing ``modalDirty()`` guard rather
    than discarding a half-typed form.
    """
    body = _body(web_client_factory)

    assert "noteBoot" in body
    assert "dismissRestart" in body
    assert "modalDirty()" in body
    assert re.search(r"x-show=\"serverRestarted\"", body)


def test_send_error_and_timeout_listeners_exist(
    web_db: sqlite3.Connection,
    web_client_factory: Callable[[], TestClient],
) -> None:
    body = _body(web_client_factory)

    assert "@htmx:send-error.window" in body
    assert "@htmx:timeout.window" in body
    assert "htmx.config.timeout" in body


def test_error_toasts_do_not_auto_dismiss(
    web_db: sqlite3.Connection,
    web_client_factory: Callable[[], TestClient],
) -> None:
    """A four-second toast is invisible to someone typing into a form.

    That is precisely how the original outage went unnoticed for as long
    as it did. Success toasts still fade; errors stay until dismissed.
    """
    body = _body(web_client_factory)

    assert re.search(r"level === 'success'[^\n]*\n?[^\n]*setTimeout", body) or (
        "if (t.level === 'success')" in body
    )
    assert "toast-dismiss" in body
