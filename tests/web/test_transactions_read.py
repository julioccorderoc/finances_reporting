"""Read-only tests for the /transactions web page (EPIC-023, Phase 2b).

Per rule-011 these land before the implementation. They cover:

* the full HTML page renders and shows seeded card-rows,
* every URL filter narrows the result set correctly,
* sort + pagination,
* HTMX fragment-only response from /_partial/transactions/list,
* JSON shape from /api/transactions,
* the canonical card_transaction.html partial renders standalone with a
  hand-built TransactionCard (so Phase 2a can include it from the
  dashboard),
* the rate-source projection covers native_usd, user_rate and
  needs_review paths.
"""

from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from decimal import Decimal

from fastapi.testclient import TestClient


# ---------------------------------------------------------------------------
# Page render / smoke.
# ---------------------------------------------------------------------------


def test_transactions_page_renders_default_30_day_range(
    seeded_web_db: sqlite3.Connection,
    web_client_factory,
) -> None:
    client: TestClient = web_client_factory()
    resp = client.get("/transactions")
    assert resp.status_code == 200
    body = resp.text
    assert "Transactions" in body
    # at least one card-row from the seeded recent rows
    assert "data-card-row" in body
    # full page extends base.html → has <body>
    assert "<body" in body


def test_filter_by_account(
    seeded_web_db: sqlite3.Connection,
    web_client_factory,
) -> None:
    client = web_client_factory()
    resp = client.get("/transactions", params={"accounts": "Provincial"})
    assert resp.status_code == 200
    body = resp.text
    # Cash USD / Binance Spot account names should not appear in the visible cards.
    # We assert via the data-account attribute we'll render on each card.
    assert "Provincial" in body
    assert 'data-account="Cash USD"' not in body
    assert 'data-account="Binance Spot"' not in body


def test_filter_by_kind(
    seeded_web_db: sqlite3.Connection,
    web_client_factory,
) -> None:
    client = web_client_factory()
    resp = client.get("/transactions", params={"kinds": "expense"})
    assert resp.status_code == 200
    body = resp.text
    # All visible cards should carry data-kind="expense".
    # We do a coarse check: no income markers leaked.
    assert 'data-kind="income"' not in body
    assert 'data-kind="expense"' in body


def test_filter_by_needs_review(
    seeded_web_db: sqlite3.Connection,
    web_client_factory,
) -> None:
    client = web_client_factory()
    resp = client.get(
        "/transactions",
        params={
            "needs_review": "yes",
            "date_from": "2000-01-01",
        },
    )
    assert resp.status_code == 200
    body = resp.text
    # Every rendered card should have the needs-review marker.
    card_count = body.count("data-card-row")
    nr_count = body.count("data-needs-review=\"true\"")
    assert card_count >= 1
    assert nr_count == card_count


def test_filter_by_date_range(
    seeded_web_db: sqlite3.Connection,
    web_client_factory,
) -> None:
    client = web_client_factory()
    today = datetime.now(tz=timezone.utc).date()
    # window covering only "today" entries
    df = today.isoformat()
    dt = today.isoformat()
    resp = client.get(
        "/transactions",
        params={"date_from": df, "date_to": dt},
    )
    assert resp.status_code == 200

    # Direct SQL count for the same window from the seeded DB.
    expected = seeded_web_db.execute(
        """
        SELECT COUNT(*) AS c FROM transactions
        WHERE date(occurred_at) >= ? AND date(occurred_at) <= ?
        """,
        (df, dt),
    ).fetchone()["c"]
    assert resp.text.count("data-card-row") == expected


def test_filter_by_description_search(
    seeded_web_db: sqlite3.Connection,
    web_client_factory,
) -> None:
    client = web_client_factory()
    resp = client.get(
        "/transactions",
        params={"q": "COM.PAGO", "date_from": "2000-01-01"},
    )
    assert resp.status_code == 200
    body = resp.text
    assert "COM.PAGO bodega" in body
    assert "ABONO nomina" not in body


def test_sort_amount_desc(
    seeded_web_db: sqlite3.Connection,
    web_client_factory,
) -> None:
    client = web_client_factory()
    resp = client.get(
        "/api/transactions",
        params={
            "sort": "amount_native",
            "direction": "desc",
            "date_from": "2000-01-01",
            "page_size": 25,
        },
    )
    assert resp.status_code == 200
    payload = resp.json()
    rows = payload["rows"]
    assert len(rows) >= 2
    amounts = [Decimal(r["amount_native"]) for r in rows]
    # largest first
    for a, b in zip(amounts, amounts[1:]):
        assert a >= b


def test_pagination(
    seeded_web_db: sqlite3.Connection,
    web_client_factory,
) -> None:
    client = web_client_factory()
    p1 = client.get(
        "/api/transactions",
        params={"page_size": 25, "page": 1, "date_from": "2000-01-01"},
    ).json()
    p2 = client.get(
        "/api/transactions",
        params={"page_size": 25, "page": 2, "date_from": "2000-01-01"},
    ).json()
    assert p1["page"] == 1
    assert p2["page"] == 2
    assert p1["page_size"] == 25
    # Same total in both responses.
    assert p1["total"] == p2["total"]
    # No id overlap if page 1 was filled.
    p1_ids = {r["id"] for r in p1["rows"]}
    p2_ids = {r["id"] for r in p2["rows"]}
    assert p1_ids.isdisjoint(p2_ids)


# ---------------------------------------------------------------------------
# HTMX partial.
# ---------------------------------------------------------------------------


def test_htmx_partial_endpoint_returns_fragment_only(
    seeded_web_db: sqlite3.Connection,
    web_client_factory,
) -> None:
    client = web_client_factory()
    resp = client.get(
        "/_partial/transactions/list",
        headers={"HX-Request": "true"},
        params={"page_size": 25, "date_from": "2000-01-01"},
    )
    assert resp.status_code == 200
    body = resp.text
    # No full HTML shell — partial only.
    assert "<html" not in body.lower()
    assert "<body" not in body.lower()
    # But it should contain at least one card.
    assert "data-card-row" in body


# ---------------------------------------------------------------------------
# JSON API.
# ---------------------------------------------------------------------------


def test_api_transactions_returns_paginated_json(
    seeded_web_db: sqlite3.Connection,
    web_client_factory,
) -> None:
    client = web_client_factory()
    resp = client.get(
        "/api/transactions",
        params={"page_size": 25, "date_from": "2000-01-01"},
    )
    assert resp.status_code == 200
    payload = resp.json()
    for key in ("rows", "total", "page", "page_size", "total_pages", "filter"):
        assert key in payload, f"missing key in JSON: {key}"
    assert isinstance(payload["rows"], list)
    assert payload["page_size"] == 25


# ---------------------------------------------------------------------------
# Card partial — standalone include.
# ---------------------------------------------------------------------------


def test_card_partial_renders_standalone(
    web_client_factory,
) -> None:
    """Phase 2a will include card_transaction.html from dashboard.html.

    The card must render correctly given only a ``card`` context variable
    — no reliance on parent state. We exercise this through the app's
    Jinja2Templates instance directly.
    """
    from fastapi import FastAPI

    from finances.web.app import create_app
    from finances.web.services.transactions_query import TransactionCard
    from finances.web.settings import WebSettings

    app: FastAPI = create_app(WebSettings(host="127.0.0.1"))
    templates = app.state.templates

    card = TransactionCard(
        id=42,
        occurred_at=datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc),
        account_name="Provincial",
        description="STANDALONE PROBE",
        amount_native=Decimal("100.00"),
        currency="VES",
        amount_usd=Decimal("2.74"),
        rate_source="binance_p2p_median",
        is_bcv_fallback=False,
        kind="expense",
        category_name="Food",
        needs_review=False,
    )
    rendered = templates.get_template("partials/card_transaction.html").render(
        card=card
    )
    assert "data-card-row" in rendered
    assert "STANDALONE PROBE" in rendered
    assert "Provincial" in rendered


# ---------------------------------------------------------------------------
# Rate-source projections.
# ---------------------------------------------------------------------------


def test_rate_source_native_for_usd_account(
    seeded_web_db: sqlite3.Connection,
    web_client_factory,
) -> None:
    client = web_client_factory()
    resp = client.get(
        "/api/transactions",
        params={
            "accounts": "Cash USD",
            "date_from": "2000-01-01",
        },
    )
    assert resp.status_code == 200
    rows = resp.json()["rows"]
    assert len(rows) == 1
    row = rows[0]
    assert row["rate_source"] == "native_usd"
    assert Decimal(row["amount_usd"]) == Decimal(row["amount_native"])


def test_rate_source_user_rate_when_set(
    seeded_web_db: sqlite3.Connection,
    web_client_factory,
) -> None:
    client = web_client_factory()
    resp = client.get(
        "/api/transactions",
        params={
            "q": "grocery",
            "date_from": "2000-01-01",
        },
    )
    assert resp.status_code == 200
    rows = resp.json()["rows"]
    assert len(rows) == 1
    row = rows[0]
    assert row["rate_source"] == "user_rate"
    # 3650 / 36.0 = 101.388...
    assert Decimal(row["amount_usd"]) == (
        Decimal("3650.00") / Decimal("36.0")
    )


def test_rate_source_needs_review_when_no_rate(
    seeded_web_db: sqlite3.Connection,
    web_client_factory,
) -> None:
    client = web_client_factory()
    # Pull the row dated 2010 — no matching rate exists.
    resp = client.get(
        "/api/transactions",
        params={
            "q": "LEGACY",
            "date_from": "2000-01-01",
        },
    )
    assert resp.status_code == 200
    rows = resp.json()["rows"]
    assert len(rows) == 1
    row = rows[0]
    assert row["rate_source"] == "needs_review"
    assert row["amount_usd"] is None

    # The HTML card for the same row should carry the needs-review marker.
    html = client.get(
        "/transactions",
        params={"q": "LEGACY", "date_from": "2000-01-01"},
    ).text
    assert 'data-needs-review="true"' in html


def test_txn_query_base_selects_every_column_row_to_transaction_needs(
    seeded_web_db: sqlite3.Connection,
) -> None:
    """One shared SELECT prefix, so adding a column has one place to update."""
    from finances.web.services.transactions_query import (
        TXN_QUERY_BASE,
        _row_to_transaction,
    )

    row = seeded_web_db.execute(
        TXN_QUERY_BASE + " WHERE t.source_ref = ?", ("prov-1",)
    ).fetchone()
    assert row is not None

    txn = _row_to_transaction(row)
    assert txn.source_ref == "prov-1"
    assert row["account_name"] == "Provincial"
    assert row["category_name"] == "Groceries"
