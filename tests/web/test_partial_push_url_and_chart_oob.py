"""Two things only a browser showed on 2026-09-03.

1. The Flow filter form, its sort chips, pager and per-page select, and
   the Monthly filter form all carry ``hx-push-url="true"`` on requests
   to ``/_partial/...``. htmx pushes the REQUEST url, so after one change
   the address bar read ``/_partial/transactions/list?...`` and a reload
   landed on a bare fragment with no shell (no rail, no title, a
   favicon 404). The rates toggle had the same bug and was fixed with a
   literal push url; these have a live querystring, so the partial
   endpoints answer with ``HX-Push-Url`` pointing at the page — the header
   overrides the attribute, and the attribute stays for anything that
   reads it.

2. The Monthly filter form swaps only ``#monthly-pivot``, so the chart
   went stale after a range change (pivot 3 months, chart 6). The pivot
   partial now carries an out-of-band twin of the chart, only when htmx
   asked for it — the full page includes the chart itself, and two
   elements with one id is the bug the rail badge guard already names.

Tests precede the implementation per rule-011.
"""

from __future__ import annotations

import json
import re
import sqlite3

from fastapi.testclient import TestClient

HX = {"HX-Request": "true"}


# ---------------------------------------------------------------------------
# HX-Push-Url points at the page, never the partial.
# ---------------------------------------------------------------------------


def test_transactions_list_partial_pushes_the_page_url(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    client: TestClient = web_client_factory()

    resp = client.get(
        "/_partial/transactions/list",
        params=[("date_from", "2000-01-01"), ("accounts", "Provincial"), ("page_size", "25")],
        headers=HX,
    )

    assert resp.status_code == 200
    assert resp.headers["HX-Push-Url"] == (
        "/transactions?date_from=2000-01-01&accounts=Provincial&page_size=25"
    )


def test_transactions_list_partial_with_no_query_pushes_the_bare_page(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    client: TestClient = web_client_factory()

    resp = client.get("/_partial/transactions/list", headers=HX)

    assert resp.headers["HX-Push-Url"] == "/transactions"


def test_a_plain_get_of_the_list_partial_pushes_nothing(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    client: TestClient = web_client_factory()

    resp = client.get("/_partial/transactions/list", params={"date_from": "2000-01-01"})

    assert "HX-Push-Url" not in resp.headers


def test_monthly_pivot_partial_pushes_the_page_url(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    client: TestClient = web_client_factory()

    resp = client.get(
        "/_partial/monthly/pivot",
        params=[("range_preset", "3m"), ("kind", "expense")],
        headers=HX,
    )

    assert resp.status_code == 200
    assert resp.headers["HX-Push-Url"] == "/monthly?range_preset=3m&kind=expense"


# ---------------------------------------------------------------------------
# The chart follows the filters, out of band.
# ---------------------------------------------------------------------------


def _json_block(html: str, element_id: str) -> dict:
    m = re.search(
        rf'<script id="{element_id}" type="application/json">(.*?)</script>', html, re.S
    )
    assert m, f"no #{element_id} JSON block"
    return json.loads(m.group(1))


def test_an_htmx_pivot_swap_carries_the_chart_out_of_band(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    from datetime import date

    from finances.web.services.monthly_view import MonthlyFilter, build_chart, build_pivot

    client: TestClient = web_client_factory()

    swapped = client.get(
        "/_partial/monthly/pivot", params={"range_preset": "3m"}, headers=HX
    ).text

    section = re.search(r"<section[^>]*id=\"monthly-chart\"[^>]*>.*?</section>", swapped, re.S)
    assert section, "the pivot swap carries no chart twin"
    assert 'hx-swap-oob="true"' in section.group(0)
    # The script that draws it must travel INSIDE the swapped element, or
    # the out-of-band swap replaces the canvas and never redraws it.
    assert "<script>" in section.group(0) or "<script " in section.group(0)
    assert "new window.Chart(" in section.group(0)
    # ...and it destroys the instance bound to the canvas it replaces.
    assert "window.__monthlyChart" in section.group(0)
    assert ".destroy()" in section.group(0)

    f = MonthlyFilter(range_preset="3m")
    expected = build_chart(seeded_web_db, f, today=date.today())
    payload = _json_block(swapped, "monthly-chart-data")
    assert payload["months"] == expected.months
    assert len(payload["months"]) == len(build_pivot(seeded_web_db, f, today=date.today()).months)


def test_a_plain_pivot_render_and_the_full_page_carry_one_chart_at_most(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    client: TestClient = web_client_factory()

    plain = client.get("/_partial/monthly/pivot", params={"range_preset": "3m"}).text
    assert 'id="monthly-chart"' not in plain

    page = client.get("/monthly", params={"layout": "desktop", "range_preset": "3m"}).text
    assert page.count('id="monthly-chart"') == 1
    assert page.count('id="monthly-chart-data"') == 1
    assert "hx-swap-oob" not in page
