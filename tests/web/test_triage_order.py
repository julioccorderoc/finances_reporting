"""Triage queue ordering is deterministic (spec §4).

204 of 243 live triage items share an ``occurred_at`` value, because the
Provincial CSV carries no time component. Sorting on the timestamp alone
therefore leaves ~84% of the queue in whatever order SQLite happened to
return, which changes with the query plan. These tests pin a total order.
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
from finances.web.services.triage import build_queue

# Every row shares this timestamp, which is the whole point.
TIED_AT = datetime(2025, 3, 4, tzinfo=UTC)


@pytest.fixture
def tied_db(web_db: sqlite3.Connection) -> sqlite3.Connection:
    """Three triage items at one timestamp, one per issue shape.

    Insertion order is id-ascending, but the two collection queries in
    ``_collect_txn_items`` visit them in a different order: the
    needs_review query yields ids 1 and 3, then the missing-category
    query adds id 2. Any implementation that preserves that visit order
    returns [1, 3, 2].
    """
    account = accounts_repo.insert(
        web_db,
        Account(name="Provincial", kind=AccountKind.BANK, currency="VES"),
    )
    groceries = categories_repo.get_by_name(
        web_db, TransactionKind.EXPENSE, "Groceries"
    )
    assert groceries is not None

    def _txn(ref: str, *, category_id: int | None, needs_review: bool) -> None:
        transactions_repo.insert(
            web_db,
            Transaction(
                account_id=account.id,
                occurred_at=TIED_AT,
                kind=TransactionKind.EXPENSE,
                amount=Decimal("100.00"),
                currency="VES",
                description=ref,
                category_id=category_id,
                source="provincial",
                source_ref=ref,
                needs_review=needs_review,
            ),
        )

    # id 1 — rate issue only (has a category).
    _txn("tied-a", category_id=groceries.id, needs_review=True)
    # id 2 — category issue only.
    _txn("tied-b", category_id=None, needs_review=False)
    # id 3 — both issues, so it appears in BOTH collection queries.
    _txn("tied-c", category_id=None, needs_review=True)

    return web_db


def test_tied_timestamps_order_by_item_id(tied_db: sqlite3.Connection) -> None:
    """Items sharing occurred_at fall back to item_id, not visit order.

    The bucket leads the sort. Since the triage redesign that means the
    two rows missing a category (ids 2 and 3) come first, tiebreaking on
    item_id because all three rows share ``TIED_AT``, and the row that
    only needs a rate (id 1) walks last.
    """
    queue = build_queue(tied_db)
    ids = [item.item_id for item in queue.items]

    assert ids == ["txn:2", "txn:3", "txn:1"]


def test_build_queue_is_repeatable(tied_db: sqlite3.Connection) -> None:
    """Two builds against the same data return the identical sequence."""
    first = [item.item_id for item in build_queue(tied_db).items]
    second = [item.item_id for item in build_queue(tied_db).items]

    assert first == second


def test_merged_item_keeps_both_badges(tied_db: sqlite3.Connection) -> None:
    """Guard: the ordering fix must not disturb badge merging."""
    queue = build_queue(tied_db)
    by_id = {item.item_id: item for item in queue.items}

    assert by_id["txn:3"].txn_issue_badges == ["category", "rate"]
