"""Static report formatting matches the viewer (UX overhaul WP1).

The static ``report.html`` must format money/dates/months through
``finances.format`` — the same single source of truth the live viewer
uses — so both surfaces render identically by construction.
"""

from __future__ import annotations

import sqlite3
from datetime import UTC, datetime
from decimal import Decimal

from finances import format as fmt
from finances.db.repos import accounts as accounts_repo
from finances.db.repos import transactions as transactions_repo
from finances.domain.models import (
    Account,
    AccountKind,
    Transaction,
    TransactionKind,
)
from finances.reports import html_export


def test_export_env_registers_shared_filters() -> None:
    env = html_export._jinja_env()
    assert env.filters["fmt_money"] is fmt.fmt_money
    assert env.filters["fmt_number"] is fmt.fmt_number
    assert env.filters["fmt_date"] is fmt.fmt_date
    assert env.filters["fmt_month"] is fmt.fmt_month


def _seed(conn: sqlite3.Connection) -> None:
    account = accounts_repo.insert(
        conn, Account(name="Cash USD", kind=AccountKind.CASH, currency="USD")
    )
    transactions_repo.insert(
        conn,
        Transaction(
            account_id=account.id,
            occurred_at=datetime(2024, 1, 15, 12, 0, tzinfo=UTC),  # a Monday
            kind=TransactionKind.EXPENSE,
            amount=Decimal("-1234.56"),  # real convention: expenses negative
            currency="USD",
            description="report formatting smoke",
            source="cash_cli",
            source_ref="fmt-report-1",
        ),
    )


def test_report_renders_shared_formatting(web_db: sqlite3.Connection) -> None:
    _seed(web_db)
    now = datetime.now(tz=UTC)
    ctx = html_export.build_report_context(web_db, now=now)
    html = html_export.render_html(ctx, chartjs_source="/* stub */")
    # Recent-transaction date: weekday + year (2024 is a past year).
    assert "Mon, Jan 15, 2024" in html
    # USD money grouped, sign BEFORE the symbol (recent row + account tile).
    assert "-$1,234.56" in html
    assert "$-1,234.56" not in html
    # Native amount grouped.
    assert "-1,234.56" in html
    # Month headings via fmt_month ("Jul 2026"-style; %b is English in the
    # C locale, so this expectation is independent of finances.format).
    assert now.strftime("%b %Y") in html
