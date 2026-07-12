"""Rendered-formatting tests for the web viewer (UX overhaul WP1).

Covers: fmt_* Jinja filter registration, the rewired _macros.html
helpers, the monthly pivot/mobile sweep, and the sign-before-symbol
fix on $-prefixed USD sites. All tests use the tmp-DB web fixtures
from tests/web/conftest.py — never the real finances.db.

Expenses are seeded NEGATIVE (real sign convention). The shared
``seeded_web_db`` fixture stores expenses positive — a known wart —
so these tests seed their own rows on the plain ``web_db`` fixture.
"""

from __future__ import annotations

import sqlite3
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

from finances.db.repos import accounts as accounts_repo
from finances.db.repos import transactions as transactions_repo
from finances.domain.models import (
    Account,
    AccountKind,
    Transaction,
    TransactionKind,
)
from finances.format import fmt_date, fmt_money, fmt_month, fmt_number
from finances.web.app import create_app
from finances.web.settings import WebSettings


def _seed_negative_usd_expense(
    conn: sqlite3.Connection,
    *,
    amount: Decimal = Decimal("-1234.56"),
    occurred_at: datetime = datetime(2024, 1, 15, 12, 0, tzinfo=UTC),
    source_ref: str = "fmt-smoke-1",
) -> None:
    """One Cash-USD expense, NEGATIVE per the real sign convention.

    USD is the native_usd rate path — amount_usd == amount, so no rate
    rows are needed. 2024-01-15 is a Monday in a past year, so fmt_date
    must render "Mon, Jan 15, 2024" (weekday + year) deterministically.
    """
    account = accounts_repo.insert(
        conn, Account(name="Cash USD", kind=AccountKind.CASH, currency="USD")
    )
    transactions_repo.insert(
        conn,
        Transaction(
            account_id=account.id,
            occurred_at=occurred_at,
            kind=TransactionKind.EXPENSE,
            amount=amount,
            currency="USD",
            description="formatting smoke",
            source="cash_cli",
            source_ref=source_ref,
        ),
    )


# ---------------------------------------------------------------------------
# Task 2 — filter registration.
# ---------------------------------------------------------------------------


def test_fmt_filters_registered_on_app_templates(web_db_path: Path) -> None:
    app = create_app(WebSettings(host="127.0.0.1", db_path=web_db_path))
    filters = app.state.templates.env.filters
    assert filters["fmt_number"] is fmt_number
    assert filters["fmt_money"] is fmt_money
    assert filters["fmt_date"] is fmt_date
    assert filters["fmt_month"] is fmt_month


# ---------------------------------------------------------------------------
# Task 3 — _macros.html format_amount/format_date delegate to the filters.
# ---------------------------------------------------------------------------


def test_macros_render_grouped_amount_and_weekday_date(
    web_db: sqlite3.Connection, web_client_factory
) -> None:
    _seed_negative_usd_expense(web_db)
    client = web_client_factory()
    resp = client.get(
        "/transactions",
        params={"date_from": "2024-01-01", "date_to": "2024-01-31"},
    )
    assert resp.status_code == 200
    body = resp.text
    # format_amount → fmt_number: grouped, sign preserved (native column).
    assert "-1,234.56" in body
    # format_date → fmt_date: weekday + year (2024 != current year).
    assert "Mon, Jan 15, 2024" in body


# ---------------------------------------------------------------------------
# Task 4 — /monthly pivot + mobile formatting sweep.
# ---------------------------------------------------------------------------


def test_pivot_month_labels_and_totals_formatted(
    web_db: sqlite3.Connection, web_client_factory
) -> None:
    _seed_negative_usd_expense(
        web_db, amount=Decimal("-2345.67"), source_ref="fmt-monthly-1"
    )
    client = web_client_factory()
    resp = client.get(
        "/monthly",
        params={
            "layout": "desktop",
            "range_preset": "custom",
            "since": "2024-01",
            "until": "2024-02",
        },
    )
    assert resp.status_code == 200
    body = resp.text
    assert "Jan 2024" in body                # pivot header via fmt_month
    assert "Feb 2024" in body
    assert 'data-month="2024-01"' in body    # machine-readable key untouched
    assert "-2,345.67" in body               # cell + column total via fmt_number


def test_mobile_month_nav_and_total_formatted(
    web_db: sqlite3.Connection, web_client_factory
) -> None:
    _seed_negative_usd_expense(
        web_db, amount=Decimal("-2345.67"), source_ref="fmt-monthly-2"
    )
    client = web_client_factory()
    resp = client.get(
        "/monthly", params={"layout": "mobile", "month": "2024-01"}
    )
    assert resp.status_code == 200
    body = resp.text
    assert "Jan 2024" in body                # centre month label via fmt_month
    assert "-$2,345.67" in body              # month total via fmt_money
    assert "$-2,345.67" not in body          # sign never after the symbol


# ---------------------------------------------------------------------------
# Task 5 — sign before symbol on $-prefixed USD sites.
# ---------------------------------------------------------------------------


def test_transactions_list_usd_sign_before_symbol(
    web_db: sqlite3.Connection, web_client_factory
) -> None:
    _seed_negative_usd_expense(web_db, source_ref="fmt-sign-1")
    client = web_client_factory()
    resp = client.get(
        "/transactions",
        params={"date_from": "2024-01-01", "date_to": "2024-01-31"},
    )
    assert resp.status_code == 200
    body = resp.text
    assert "-$1,234.56" in body
    assert "$-1,234.56" not in body


def test_transaction_modal_usd_sign_before_symbol(
    web_db: sqlite3.Connection, web_client_factory
) -> None:
    _seed_negative_usd_expense(web_db, source_ref="fmt-sign-2")
    txn_id = web_db.execute(
        "SELECT id FROM transactions WHERE source_ref = 'fmt-sign-2'"
    ).fetchone()["id"]
    client = web_client_factory()
    resp = client.get(f"/_partial/transactions/{txn_id}/modal")
    assert resp.status_code == 200
    assert "-$1,234.56" in resp.text
    assert "$-1,234.56" not in resp.text


def test_accounts_page_usd_sign_before_symbol(
    web_db: sqlite3.Connection, web_client_factory
) -> None:
    _seed_negative_usd_expense(web_db, source_ref="fmt-sign-3")
    client = web_client_factory()
    resp = client.get("/accounts")
    assert resp.status_code == 200
    body = resp.text
    assert "-$1,234.56" in body      # balance_usdt via fmt_money
    assert "$-1,234.56" not in body


# ---------------------------------------------------------------------------
# Task 6 — dashboard KPI money via the shared formatter.
# ---------------------------------------------------------------------------


def test_dashboard_money_is_shared_formatter() -> None:
    from finances.web.services import dashboard

    # Single source of truth: no module-private formatter left behind.
    assert dashboard.fmt_money is fmt_money
    assert not hasattr(dashboard, "_format_money")


def test_kpi_tiles_sign_before_symbol_and_grouped(
    web_db: sqlite3.Connection,
) -> None:
    from finances.web.services.dashboard import build_kpis

    _seed_negative_usd_expense(
        web_db,
        amount=Decimal("-1234567.89"),
        occurred_at=datetime.now(tz=UTC),
        source_ref="fmt-kpi-1",
    )
    kpis = build_kpis(web_db, today=datetime.now(tz=UTC).date())
    # >1M, negative, grouped, sign BEFORE the symbol.
    assert kpis.month_spend.value == "-$1,234,567.89"
    for tile in (kpis.net_worth, kpis.month_spend, kpis.month_income):
        assert "$-" not in tile.value
        assert "$-" not in (tile.hint or "")
