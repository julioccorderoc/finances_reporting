"""Filter-polish tests for WP6 (docs/plans/ux-overhaul/00-design.md §6).

Per rule-011 these land before the implementation. They cover:

* /transactions: the four multi-selects (accounts, kinds, currencies,
  sources) are replaced by checkbox-chip groups that keep the SAME
  query-param names, render checked state from the URL, and still
  narrow the result set through the unchanged filter_from_query dep,
* /monthly: since/until are native <input type="month"> controls that
  round-trip the YYYY-MM format _monthly_filter_dep already parses,
* both filter forms carry a plain "Clear filters" link back to the bare
  page URL (transactions default = last-30-days window via
  transactions_query.resolve_defaults; monthly default = 6m preset).

Uses the tmp-DB fixtures from tests/web/conftest.py — never the real
finances.db.
"""

from __future__ import annotations

import sqlite3

from fastapi.testclient import TestClient

_DESKTOP_UA = {"User-Agent": "Mozilla/5.0 desktop"}


# ---------------------------------------------------------------------------
# Task 1 — /transactions checkbox-chip groups.
# ---------------------------------------------------------------------------


def test_transactions_filters_render_checkbox_chip_groups(
    seeded_web_db: sqlite3.Connection,
    web_client_factory,
) -> None:
    client: TestClient = web_client_factory()
    resp = client.get("/transactions")
    assert resp.status_code == 200
    body = resp.text
    # The four multi-selects are gone (needs_review / page_size selects stay).
    for name in ("accounts", "kinds", "currencies", "sources"):
        assert f'<select name="{name}"' not in body
    # ...replaced by checkboxes with the SAME param names (unchecked by
    # default: the default filter constrains dates only, not these lists).
    assert '<input type="checkbox" name="accounts" value="Provincial">' in body
    assert '<input type="checkbox" name="kinds" value="expense">' in body
    assert '<input type="checkbox" name="currencies" value="VES">' in body
    assert '<input type="checkbox" name="sources" value="provincial">' in body
    # Chips are styled via the shared classes.
    assert 'class="choice-chips"' in body
    assert 'class="choice-chip"' in body


def test_transactions_filter_chips_reflect_checked_state_from_url(
    seeded_web_db: sqlite3.Connection,
    web_client_factory,
) -> None:
    client: TestClient = web_client_factory()
    resp = client.get(
        "/transactions",
        params=[
            ("accounts", "Provincial"),
            ("accounts", "Cash USD"),
            ("kinds", "expense"),
        ],
    )
    assert resp.status_code == 200
    body = resp.text
    assert '<input type="checkbox" name="accounts" value="Provincial" checked>' in body
    assert '<input type="checkbox" name="accounts" value="Cash USD" checked>' in body
    assert (
        '<input type="checkbox" name="accounts" value="Binance Spot" checked>'
        not in body
    )
    assert '<input type="checkbox" name="kinds" value="expense" checked>' in body
    assert '<input type="checkbox" name="kinds" value="income" checked>' not in body


def test_checkbox_repeated_params_still_narrow_the_list(
    seeded_web_db: sqlite3.Connection,
    web_client_factory,
) -> None:
    """Contract guard: repeated checkbox params == repeated select params.

    filter_from_query (unchanged) must keep narrowing rows. Expected to
    pass BEFORE the template change too — it pins the param contract.
    """
    client: TestClient = web_client_factory()
    resp = client.get(
        "/transactions",
        params=[("accounts", "Provincial"), ("date_from", "2000-01-01")],
    )
    assert resp.status_code == 200
    body = resp.text
    assert 'data-account="Provincial"' in body
    assert 'data-account="Cash USD"' not in body
    assert 'data-account="Binance Spot"' not in body


# ---------------------------------------------------------------------------
# Task 2 — /monthly native month inputs.
# ---------------------------------------------------------------------------


def test_monthly_since_until_render_as_month_inputs_and_round_trip(
    seeded_web_db: sqlite3.Connection,
    web_client_factory,
) -> None:
    client: TestClient = web_client_factory()
    resp = client.get(
        "/monthly",
        params={"range_preset": "custom", "since": "2026-01", "until": "2026-05"},
        headers=_DESKTOP_UA,
    )
    assert resp.status_code == 200
    body = resp.text
    # Native month inputs round-trip the YYYY-MM value from the URL.
    assert '<input type="month" name="since" value="2026-01"' in body
    assert '<input type="month" name="until" value="2026-05"' in body
    # The free-text placeholders are gone.
    assert 'placeholder="2026-01"' not in body
    assert 'placeholder="2026-05"' not in body

    # Empty state renders an empty value attribute, never "None".
    resp_default = client.get("/monthly", headers=_DESKTOP_UA)
    assert resp_default.status_code == 200
    assert '<input type="month" name="since" value=""' in resp_default.text
    assert '<input type="month" name="until" value=""' in resp_default.text


def test_monthly_month_param_validation_is_unchanged(
    seeded_web_db: sqlite3.Connection,
    web_client_factory,
) -> None:
    """Contract guard: _monthly_filter_dep still 422s malformed months.

    <input type="month"> submits exactly YYYY-MM — the only format
    _MONTH_RE accepts. Expected to pass BEFORE the template change too.
    """
    client: TestClient = web_client_factory()
    resp = client.get("/monthly", params={"since": "2026-13"}, headers=_DESKTOP_UA)
    assert resp.status_code == 422
