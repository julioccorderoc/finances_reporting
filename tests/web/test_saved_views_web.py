"""Wave 2 Thing 2 — saved filter views: HTMX partial endpoints + page wiring.

Plan: docs/plans/wave2/02-saved-views.md. Per rule-011 these tests land
before the implementation. Covers:

* ``GET  /_partial/views``            — chip row + save form partial,
* ``POST /_partial/views``            — create (form fields ``name``,
  ``query_string``); duplicate name → 422 whose JSON ``detail`` is the
  error-toast message (surfaced by the global htmx:response-error
  listener per the WP2 toast contract),
* ``POST /_partial/views/{id}/delete`` — delete + re-rendered chip row,
* chip ``href`` reproduces the exact saved querystring,
* /transactions page server-renders the saved-views section.
"""

from __future__ import annotations

import html
import json
import re
import sqlite3

from fastapi.testclient import TestClient

from finances.db.repos import saved_views as saved_views_repo
from finances.domain.models import SavedView


def _create_view(client: TestClient, name: str, query_string: str):
    return client.post(
        "/_partial/views",
        data={"name": name, "query_string": query_string},
    )


def _chip_hrefs(body: str) -> list[str]:
    """Extract unescaped hrefs of the saved-view chip links."""
    return [
        html.unescape(m)
        for m in re.findall(r'data-view-chip[^>]*href="([^"]+)"', body)
        or re.findall(r'href="([^"]+)"[^>]*data-view-chip', body)
    ]


def _success_toast(resp) -> dict:
    payload = json.loads(resp.headers["HX-Trigger"])
    return payload["toast"]


# ---------------------------------------------------------------------------
# GET /_partial/views — chip row partial.
# ---------------------------------------------------------------------------


def test_views_partial_renders_save_form_when_empty(
    web_db: sqlite3.Connection, web_client_factory
) -> None:
    client = web_client_factory()
    resp = client.get("/_partial/views")
    assert resp.status_code == 200
    assert 'id="saved-views"' in resp.text
    assert 'hx-post="/_partial/views"' in resp.text
    assert 'name="name"' in resp.text


def test_views_partial_lists_existing_views(
    web_db: sqlite3.Connection, web_client_factory
) -> None:
    saved_views_repo.insert(
        web_db, SavedView(name="Needs review", query_string="needs_review=yes")
    )
    client = web_client_factory()
    resp = client.get("/_partial/views")
    assert resp.status_code == 200
    assert "Needs review" in resp.text


# ---------------------------------------------------------------------------
# POST /_partial/views — create.
# ---------------------------------------------------------------------------


def test_create_renders_chip_with_name_and_success_toast(
    web_db: sqlite3.Connection, web_client_factory
) -> None:
    client = web_client_factory()
    resp = _create_view(client, "July expenses", "kinds=expense&date_from=2026-07-01")
    assert resp.status_code == 200
    assert "July expenses" in resp.text
    toast = _success_toast(resp)
    assert toast["level"] == "success"
    assert "July expenses" in toast["message"]


def test_chip_href_reproduces_exact_saved_querystring(
    web_db: sqlite3.Connection, web_client_factory
) -> None:
    qs = "date_from=2026-07-01&date_to=2026-07-31&kinds=expense&kinds=income&q=COM.PAGO"
    client = web_client_factory()
    resp = _create_view(client, "July detail", qs)
    assert resp.status_code == 200
    assert f"/transactions?{qs}" in _chip_hrefs(resp.text)


def test_create_strips_leading_question_mark(
    web_db: sqlite3.Connection, web_client_factory
) -> None:
    client = web_client_factory()
    resp = _create_view(client, "From location.search", "?needs_review=yes")
    assert resp.status_code == 200
    assert "/transactions?needs_review=yes" in _chip_hrefs(resp.text)

    views = saved_views_repo.list_all(web_db)
    assert len(views) == 1
    assert views[0].query_string == "needs_review=yes"


def test_create_with_empty_querystring_links_to_bare_transactions(
    web_db: sqlite3.Connection, web_client_factory
) -> None:
    client = web_client_factory()
    resp = _create_view(client, "Everything", "")
    assert resp.status_code == 200
    assert "/transactions" in _chip_hrefs(resp.text)


def test_create_duplicate_name_returns_422_with_toastable_detail(
    web_db: sqlite3.Connection, web_client_factory
) -> None:
    client = web_client_factory()
    assert _create_view(client, "Dup", "q=1").status_code == 200

    resp = _create_view(client, "Dup", "q=2")
    assert resp.status_code == 422
    # The error toast is raised client-side by the global
    # htmx:response-error listener, which shows the JSON ``detail``
    # (WP2 contract — see partials.py). The detail must name the view.
    assert "Dup" in resp.json()["detail"]


def test_create_blank_name_returns_422(
    web_db: sqlite3.Connection, web_client_factory
) -> None:
    client = web_client_factory()
    resp = _create_view(client, "   ", "q=1")
    assert resp.status_code == 422


# ---------------------------------------------------------------------------
# POST /_partial/views/{id}/delete.
# ---------------------------------------------------------------------------


def test_delete_removes_chip_and_returns_success_toast(
    web_db: sqlite3.Connection, web_client_factory
) -> None:
    saved = saved_views_repo.insert(
        web_db, SavedView(name="Doomed view", query_string="q=x")
    )
    client = web_client_factory()

    resp = client.post(f"/_partial/views/{saved.id}/delete")
    assert resp.status_code == 200
    assert "Doomed view" not in resp.text
    assert _success_toast(resp)["level"] == "success"
    assert saved_views_repo.list_all(web_db) == []


def test_delete_missing_view_returns_404(
    web_db: sqlite3.Connection, web_client_factory
) -> None:
    client = web_client_factory()
    resp = client.post("/_partial/views/9999/delete")
    assert resp.status_code == 404


def test_delete_button_asks_for_confirmation(
    web_db: sqlite3.Connection, web_client_factory
) -> None:
    saved_views_repo.insert(web_db, SavedView(name="Keep me", query_string="q=k"))
    client = web_client_factory()
    resp = client.get("/_partial/views")
    assert "hx-confirm" in resp.text


# ---------------------------------------------------------------------------
# /transactions page wiring — chips render server-side above the filters.
# ---------------------------------------------------------------------------


def test_transactions_page_renders_saved_views_section(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    saved_views_repo.insert(
        seeded_web_db,
        SavedView(name="Groceries VES", query_string="currencies=VES"),
    )
    client = web_client_factory()
    resp = client.get("/transactions")
    assert resp.status_code == 200
    assert 'id="saved-views"' in resp.text
    assert "Groceries VES" in resp.text


def test_saved_view_round_trips_the_paired_filter(
    web_db: sqlite3.Connection, web_client_factory
) -> None:
    """paired rides the stored query_string — no schema or repo change."""
    client: TestClient = web_client_factory()

    created = _create_view(client, "Unpaired P2P sells", "sources=binance&paired=no")
    assert created.status_code == 200, created.text

    resp = client.get("/_partial/views")
    assert resp.status_code == 200, resp.text
    assert any("paired=no" in href for href in _chip_hrefs(resp.text)), resp.text
