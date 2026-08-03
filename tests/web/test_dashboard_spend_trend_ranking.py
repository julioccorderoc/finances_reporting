"""The dashboard's top-5 spend chart must show the *biggest* categories.

``build_spend_trend`` ranked categories with::

    sorted(cat_total.items(), key=lambda kv: (kv[1], kv[0]), reverse=True)

Expense totals are negative, so reverse-sorting them puts the *least*
negative first: the chart was built out of the five cheapest categories and
swept the expensive ones into "Other". Against the live ledger it rendered
Fees ($3.26), Loan Repayment ($20) and Clothing ($75) while Purchases
($1,103), Lending ($659) and Groceries ($655) were invisible.

``monthly_view`` ranks the same data correctly, with ``abs()``. Only the
dashboard drifted, and the web fixture's positive expense amounts are why a
green suite never noticed: with positive inputs, ``reverse=True`` is right.
"""

from __future__ import annotations

import sqlite3
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from finances.db.repos import accounts as accounts_repo
from finances.db.repos import categories as categories_repo
from finances.db.repos import transactions as txn_repo
from finances.domain.models import Account, AccountKind, Transaction, TransactionKind
from finances.web.services.dashboard import build_spend_trend

# name -> magnitude. Deliberately more than five so something must fall into
# "Other", and spread widely so a reversed ranking is unmistakable.
SPEND = {
    "Groceries": "700",
    "Transport": "500",
    "Health": "300",
    "Leisure": "200",
    "Utilities": "100",
    "Fees": "3",
    "Clothing": "2",
}


@pytest.fixture
def spend_db(in_memory_db: sqlite3.Connection) -> sqlite3.Connection:
    conn = in_memory_db
    account = accounts_repo.insert(
        conn,
        Account(name="Cash USD", kind=AccountKind.CASH, currency="USD"),
    )
    for i, (name, magnitude) in enumerate(SPEND.items()):
        category = categories_repo.get_by_name(conn, TransactionKind.EXPENSE, name)
        assert category is not None, name
        txn_repo.insert(
            conn,
            Transaction(
                account_id=account.id,
                occurred_at=datetime(2026, 5, 12, tzinfo=UTC),
                kind=TransactionKind.EXPENSE,
                # Negative, as production stores expenses.
                amount=Decimal(magnitude) * -1,
                currency="USD",
                description=name,
                category_id=category.id,
                source="cash_cli",
                source_ref=f"c{i}",
            ),
        )
    return conn


def test_top_series_are_the_biggest_spends(spend_db):
    trend = build_spend_trend(spend_db, today=datetime(2026, 5, 31, tzinfo=UTC).date())
    shown = {s.category for s in trend.series}

    assert "Groceries" in shown
    assert "Transport" in shown


def test_the_cheapest_categories_are_not_promoted(spend_db):
    """Fees at \$3 must never displace Groceries at \$700."""
    trend = build_spend_trend(spend_db, today=datetime(2026, 5, 31, tzinfo=UTC).date())
    shown = {s.category for s in trend.series}

    assert "Fees" not in shown
    assert "Clothing" not in shown


def test_series_are_ordered_biggest_first(spend_db):
    trend = build_spend_trend(spend_db, today=datetime(2026, 5, 31, tzinfo=UTC).date())
    named = [s.category for s in trend.series if s.category != "Other"]

    assert named == ["Groceries", "Transport", "Health", "Leisure", "Utilities"]


def test_the_overflow_bucket_holds_the_remainder(spend_db):
    trend = build_spend_trend(spend_db, today=datetime(2026, 5, 31, tzinfo=UTC).date())
    other = next((s for s in trend.series if s.category == "Other"), None)

    assert other is not None
    # Fees + Clothing = 5, negative.
    assert sum(other.values, Decimal("0")) == Decimal("-5")


def test_totals_are_preserved_across_the_split(spend_db):
    """Ranking may reorder; it may never lose money."""
    trend = build_spend_trend(spend_db, today=datetime(2026, 5, 31, tzinfo=UTC).date())
    charted = sum(
        (sum(s.values, Decimal("0")) for s in trend.series), Decimal("0")
    )
    expected = -sum((Decimal(v) for v in SPEND.values()), Decimal("0"))

    assert charted == expected
