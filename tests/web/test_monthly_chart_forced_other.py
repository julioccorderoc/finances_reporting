"""A category stored as ``Other`` is folded into the remainder, always.

ADR-023 §2.6 as amended 2026-09-08. The chart ranks series by their total
over the window, so a category with one big month keeps a coloured slot
for the whole window — which is how Lending held the fifth slot through
six months while Health fell into Other, the opposite of what the owner
asked for. ``group_name = 'Other'`` is the data-side answer: not a group
competing for a slot, but an instruction to fold the category into the
computed remainder whatever its rank.

The remainder is still computed. What misses the cap and what is stored
as Other land in one ``Other`` series, flat, largest first, in the neutral.
"""

from __future__ import annotations

import sqlite3
from datetime import UTC, datetime
from decimal import Decimal
from urllib.parse import parse_qs, urlsplit

from finances.db.repos import accounts as accounts_repo
from finances.db.repos import categories as categories_repo
from finances.db.repos import transactions as transactions_repo
from finances.domain.models import (
    Account,
    AccountKind,
    Transaction,
    TransactionKind,
)
from finances.web.services.monthly_view import (
    OTHER_COLOR_SLOT,
    MonthlyChart,
    MonthlyFilter,
    MonthlyKind,
    build_chart,
)

# Lending would rank first by a mile; it must still be Other.
LOUD_LENDING = {
    "Lending": "1000.00",
    "Groceries": "300.00",
    "Purchases": "200.00",
    "Health": "50.00",
}

# Seven ungrouped categories over the cap, plus a small Lending: the two
# kinds of Other — computed and stored — have to become one series.
OVER_THE_CAP = {
    "Purchases": "700.00",
    "Groceries": "600.00",
    "Health": "500.00",
    "Fees": "400.00",
    "Other Expense": "300.00",
    "Education": "200.00",
    "Subscriptions": "100.00",
    "Lending": "50.00",
}


def _seed(conn: sqlite3.Connection, amounts: dict[str, str]) -> datetime:
    today = datetime.now(tz=UTC)
    cash = accounts_repo.insert(
        conn, Account(name="Cash USD", kind=AccountKind.CASH, currency="USD")
    )
    for idx, (name, amount) in enumerate(amounts.items()):
        category = categories_repo.get_by_name(conn, TransactionKind.EXPENSE, name)
        assert category is not None, name
        transactions_repo.insert(
            conn,
            Transaction(
                account_id=cash.id,
                occurred_at=today,
                kind=TransactionKind.EXPENSE,
                amount=Decimal(amount),
                currency="USD",
                description=f"seed-{name}",
                category_id=category.id,
                source="cash_cli",
                source_ref=f"forced-{idx}",
            ),
        )
    return today


def _chart(conn: sqlite3.Connection, today: datetime, **filter_kw) -> MonthlyChart:
    return build_chart(
        conn, MonthlyFilter(kind=MonthlyKind.EXPENSE, **filter_kw), today=today.date()
    )


def _month_index(chart: MonthlyChart, today: datetime) -> int:
    return chart.months.index(f"{today.year:04d}-{today.month:02d}")


def _other(chart: MonthlyChart):
    others = [s for s in chart.series if s.category == "Other"]
    assert len(others) == 1, f"expected exactly one Other, got {[s.category for s in chart.series]}"
    return others[0]


def test_a_category_stored_as_other_never_takes_a_slot(
    web_db: sqlite3.Connection,
) -> None:
    today = _seed(web_db, LOUD_LENDING)
    chart = _chart(web_db, today)
    i = _month_index(chart, today)

    assert [s.category for s in chart.series] == ["Groceries", "Purchases", "Health", "Other"]
    other = _other(chart)
    assert other.color_slot == OTHER_COLOR_SLOT
    assert other.values[i] == Decimal("1000.00")
    assert [m.category for m in other.members] == ["Lending"]


def test_a_stored_other_holds_no_palette_rank(web_db: sqlite3.Connection) -> None:
    """Ranked with the rest, Lending at 1000 would own slot 0 and push
    Groceries to slot 1 — a colour spent on a series that is never drawn."""
    today = _seed(web_db, LOUD_LENDING)
    chart = _chart(web_db, today)

    groceries = next(s for s in chart.series if s.category == "Groceries")
    assert groceries.color_slot == 0


def test_filtering_to_a_stored_other_category_draws_other(
    web_db: sqlite3.Connection,
) -> None:
    """Same rule as a group under a filter (§2.5): the chart does not
    change shape because a filter is on. Lending alone is still Other."""
    today = _seed(web_db, LOUD_LENDING)
    chart = _chart(web_db, today, categories=["Lending"])

    assert [s.category for s in chart.series] == ["Other"]
    assert [m.category for m in _other(chart).members] == ["Lending"]


def test_stored_other_and_the_computed_tail_are_one_series(
    web_db: sqlite3.Connection,
) -> None:
    today = _seed(web_db, OVER_THE_CAP)
    chart = _chart(web_db, today)
    i = _month_index(chart, today)

    assert [s.category for s in chart.series] == [
        "Purchases",
        "Groceries",
        "Health",
        "Fees",
        "Other Expense",
        "Other",
    ]
    other = _other(chart)
    assert [m.category for m in other.members] == ["Education", "Subscriptions", "Lending"]
    assert other.values[i] == Decimal("350.00")
    assert sum((m.values[i] for m in other.members), Decimal("0")) == other.values[i]


def test_other_members_are_flat(web_db: sqlite3.Connection) -> None:
    """Lending sits directly under Other. There is no inner "Other" for the
    overlay to fail to open — a stored Other is not a group."""
    today = _seed(web_db, OVER_THE_CAP)
    other = _other(_chart(web_db, today))

    assert all(m.category != "Other" for m in other.members)
    assert all(m.members == [] for m in other.members)


def test_other_drills_to_the_stored_category_too(web_db: sqlite3.Connection) -> None:
    today = _seed(web_db, OVER_THE_CAP)
    chart = _chart(web_db, today)
    i = _month_index(chart, today)

    q = parse_qs(urlsplit(_other(chart).drill_urls[i]).query)
    assert set(q["categories"]) == {"Education", "Subscriptions", "Lending"}


def test_no_other_when_nothing_is_stored_or_folded(web_db: sqlite3.Connection) -> None:
    today = _seed(web_db, {"Groceries": "10.00", "Purchases": "5.00"})
    chart = _chart(web_db, today)

    assert all(s.category != "Other" for s in chart.series)
