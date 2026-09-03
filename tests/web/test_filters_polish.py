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
    assert '<input type="checkbox" class="tcheck" name="accounts" value="Provincial">' in body
    assert '<input type="checkbox" class="tcheck" name="kinds" value="expense">' in body
    assert '<input type="checkbox" class="tcheck" name="currencies" value="VES">' in body
    assert '<input type="checkbox" class="tcheck" name="sources" value="provincial">' in body
    # Since 2026-09-03 the checkboxes live in dropdown menus
    # (tests/web/test_flow_filter_dropdowns.py); the chips are gone.
    assert 'class="flow-dd-option"' in body
    assert "choice-chip" not in body


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
    assert '<input type="checkbox" class="tcheck" name="accounts" value="Provincial" checked>' in body
    assert '<input type="checkbox" class="tcheck" name="accounts" value="Cash USD" checked>' in body
    assert (
        '<input type="checkbox" class="tcheck" name="accounts" value="Binance Spot" checked>'
        not in body
    )
    assert '<input type="checkbox" class="tcheck" name="kinds" value="expense" checked>' in body
    assert '<input type="checkbox" class="tcheck" name="kinds" value="income" checked>' not in body


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


# ---------------------------------------------------------------------------
# Task 3 — Clear-filters links.
# ---------------------------------------------------------------------------


def test_transactions_clear_filters_link_resets_to_bare_url(
    seeded_web_db: sqlite3.Connection,
    web_client_factory,
) -> None:
    client: TestClient = web_client_factory()
    resp = client.get("/transactions", params={"q": "COM.PAGO"})
    assert resp.status_code == 200
    body = resp.text
    # Plain link to the bare page URL — the server re-derives the
    # last-30-days default via transactions_query.resolve_defaults.
    assert '<a href="/transactions" data-clear-filters' in body
    assert ">Clear filters</a>" in body


def test_monthly_clear_filters_link_resets_to_bare_url(
    seeded_web_db: sqlite3.Connection,
    web_client_factory,
) -> None:
    client: TestClient = web_client_factory()
    resp = client.get(
        "/monthly",
        params={"range_preset": "custom", "since": "2026-01"},
        headers=_DESKTOP_UA,
    )
    assert resp.status_code == 200
    body = resp.text
    assert '<a href="/monthly" data-clear-filters' in body
    assert ">Clear filters</a>" in body


# ---------------------------------------------------------------------------
# paired filter — isolate rows that still need a transfer_id.
# ---------------------------------------------------------------------------


def _pair_two_rows(conn: sqlite3.Connection) -> None:
    """Stamp a transfer_id on the seeded Binance income row."""
    conn.execute(
        "UPDATE transactions SET transfer_id = 'tid-test' WHERE source_ref = ?",
        ("bin-1",),
    )


def test_paired_no_returns_only_unpaired_rows(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    _pair_two_rows(seeded_web_db)
    client: TestClient = web_client_factory()

    resp = client.get(
        "/api/transactions", params={"paired": "no", "date_from": "2009-01-01"}
    )

    assert resp.status_code == 200, resp.text
    descriptions = [row["description"] for row in resp.json()["rows"]]
    assert "Earn payout" not in descriptions
    assert "COM.PAGO bodega" in descriptions


def test_paired_yes_returns_only_paired_rows(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    _pair_two_rows(seeded_web_db)
    client: TestClient = web_client_factory()

    resp = client.get(
        "/api/transactions", params={"paired": "yes", "date_from": "2009-01-01"}
    )

    assert resp.status_code == 200, resp.text
    descriptions = [row["description"] for row in resp.json()["rows"]]
    assert descriptions == ["Earn payout"]


def test_paired_any_is_the_default_and_returns_everything(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    _pair_two_rows(seeded_web_db)
    client: TestClient = web_client_factory()

    default = client.get("/api/transactions", params={"date_from": "2009-01-01"})
    explicit = client.get(
        "/api/transactions", params={"paired": "any", "date_from": "2009-01-01"}
    )

    assert default.status_code == 200 and explicit.status_code == 200
    assert len(default.json()["rows"]) == len(explicit.json()["rows"])
    assert len(default.json()["rows"]) > 1


def test_paired_rejects_unknown_value(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    client: TestClient = web_client_factory()

    resp = client.get("/api/transactions", params={"paired": "maybe"})

    assert resp.status_code == 422
