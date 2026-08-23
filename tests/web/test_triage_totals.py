"""``TriageQueue.total`` — the unfiltered size, whatever is being asked.

The redesigned queue has no filter chips: rows are grouped by what is
wrong with them and the run walks every group, so the markup half of the
original file (which pinned the chip row's placement and its
``data-active``) went with them.

What survives is the arithmetic that made those chips wrong in the first
place, and which the header still depends on: ``items`` is the filtered
list, ``counts`` is per-type over the UNFILTERED set, and ``total`` is the
sum of ``counts`` — never ``len(items)``. Parked rows are outside all
three.
"""

from __future__ import annotations

import sqlite3
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from finances.db.repos import accounts as accounts_repo
from finances.db.repos import categories as categories_repo
from finances.db.repos import transactions as transactions_repo
from finances.domain.models import (
    Account,
    AccountKind,
    Transaction,
    TransactionKind,
)
from finances.web.services.triage import TriageType, build_queue


# ---------------------------------------------------------------------------
# Fixture: a queue whose filtered and unfiltered counts differ.
# ---------------------------------------------------------------------------


@pytest.fixture
def chips_db(web_db: sqlite3.Connection) -> sqlite3.Connection:
    """Three live items: 2 CATEGORY + 1 RATE. No pairs.

    The two category rows are USD, so setting a category fully resolves
    them (``rates.resolve`` takes the native path) and they leave the
    queue — which is what makes the post-run count assertions
    deterministic.

    The rate row is VES dated 2010 with a category already set, so it
    lands on the RATE surface only: no rate exists that far back, and it
    is not also missing a category.
    """
    cash = accounts_repo.insert(
        web_db, Account(name="Cash USD", kind=AccountKind.CASH, currency="USD")
    )
    provincial = accounts_repo.insert(
        web_db,
        Account(
            name="Provincial",
            kind=AccountKind.BANK,
            currency="VES",
            institution="Provincial",
        ),
    )
    groceries = categories_repo.get_by_name(
        web_db, TransactionKind.EXPENSE, "Groceries"
    )
    assert groceries is not None

    for day, ref in ((10, "cat-one"), (11, "cat-two")):
        transactions_repo.insert(
            web_db,
            Transaction(
                account_id=cash.id,
                occurred_at=datetime(2026, 5, day, tzinfo=UTC),
                kind=TransactionKind.EXPENSE,
                amount=Decimal("-10.00"),
                currency="USD",
                description=ref,
                source="cash",
                source_ref=ref,
            ),
        )

    transactions_repo.insert(
        web_db,
        Transaction(
            account_id=provincial.id,
            occurred_at=datetime(2010, 1, 1, tzinfo=UTC),
            kind=TransactionKind.EXPENSE,
            amount=Decimal("-999.00"),
            currency="VES",
            description="LEGACY needs review",
            category_id=groceries.id,
            source="provincial",
            source_ref="rate-one",
            needs_review=True,
        ),
    )
    return web_db


def _cat_txn_id(conn: sqlite3.Connection, ref: str) -> int:
    row = conn.execute(
        "SELECT id FROM transactions WHERE source_ref = ?", (ref,)
    ).fetchone()
    assert row is not None
    return int(row["id"])


# ---------------------------------------------------------------------------
# TriageQueue.total — the unfiltered size, available under any filter.
# ---------------------------------------------------------------------------


def test_total_is_the_unfiltered_count(chips_db: sqlite3.Connection) -> None:
    queue = build_queue(chips_db)

    assert queue.total == 3
    assert queue.total == len(queue.items)


def test_total_ignores_the_type_filter(chips_db: sqlite3.Connection) -> None:
    """The whole point: "All" must not shrink when a chip is active."""
    queue = build_queue(chips_db, type_filter=TriageType.CATEGORY)

    assert len(queue.items) == 2
    assert queue.total == 3


def test_total_agrees_with_counts(chips_db: sqlite3.Connection) -> None:
    """``counts`` is per-type on the unfiltered set; ``total`` is its sum."""
    queue = build_queue(chips_db, type_filter=TriageType.RATE)

    assert queue.total == sum(queue.counts.values())


def test_total_excludes_parked_rows(chips_db: sqlite3.Connection) -> None:
    """Parked is a separate surface with its own count, never part of items."""
    transactions_repo.update(
        chips_db, id=_cat_txn_id(chips_db, "cat-one"), parked=True
    )

    queue = build_queue(chips_db)

    assert queue.total == 2
    assert queue.parked_count == 1
