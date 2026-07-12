"""WP2 — safety + feedback tests (ux-overhaul, docs/plans/ux-overhaul/00-design.md §2).

Covers:

* base.html toast infrastructure (toast host div, JSON HX-Trigger
  parsing, htmx:responseError listener, show-toast plumbing),
* HX-Trigger toast JSON on the edit / triage-edit / pair-confirm POSTs,
* edit-modal dirty tracking (an untouched select must NOT clear the
  category), the explicit "remove category" control, and autofocus on
  the category control — for both modal_transaction.html and
  modal_transaction_triage.html.

House style notes: template behavior is pinned via string markers on the
rendered partials (same approach as tests/web/test_transactions_write.py),
endpoint semantics via form POSTs + repo re-reads. All DB access goes
through the tmp-DB fixtures in tests/web/conftest.py — never the real
finances.db.
"""

from __future__ import annotations

import sqlite3


# ---------------------------------------------------------------------------
# Task 1 — base.html toast infrastructure.
# ---------------------------------------------------------------------------


def test_base_html_has_toast_host(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    client = web_client_factory()
    resp = client.get("/")
    assert resp.status_code == 200
    assert 'id="toast-host"' in resp.text


def test_base_html_parses_json_hx_trigger_and_dispatches_show_toast(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    """The <body> listener must parse JSON HX-Trigger headers (not only the
    legacy comma list) and re-dispatch closeModal / toast as window events."""
    client = web_client_factory()
    body = client.get("/").text
    assert "JSON.parse" in body
    assert "show-toast" in body
    assert "close-modal" in body  # legacy close path must survive


def test_base_html_has_htmx_response_error_listener(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    client = web_client_factory()
    body = client.get("/").text
    assert "htmx:response-error" in body
    # The listener surfaces the server's error body (JSON detail field).
    assert "responseText" in body


def test_app_css_has_toast_styles(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    client = web_client_factory()
    resp = client.get("/static/css/app.css")
    assert resp.status_code == 200
    assert ".toast-host" in resp.text
    assert ".toast-success" in resp.text
    assert ".toast-error" in resp.text
