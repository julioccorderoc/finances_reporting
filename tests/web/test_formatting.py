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
