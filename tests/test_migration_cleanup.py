"""EPIC-012 interactive cleanup — red-phase tests.

The cleanup walker iterates ``WHERE needs_review=1`` rows, asks the user
for a category (name) + optional user_rate, and persists the answer.
Input/output are injected so tests drive deterministic prompts.
"""
from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from finances.db.repos import accounts as accounts_repo
from finances.db.repos import categories as categories_repo
from finances.db.repos import transactions as txn_repo
from finances.domain.models import (
    Account,
    AccountKind,
    Category,
    Transaction,
    TransactionKind,
)


@pytest.fixture
def db_with_review_rows(
    in_memory_db: sqlite3.Connection,
) -> Iterator[sqlite3.Connection]:
    """Seed two needs_review rows + one resolved row, and one expense category."""
    acct = accounts_repo.insert(
        in_memory_db,
        Account(
            name="Provincial Bolivares",
            kind=AccountKind.BANK,
            currency="VES",
            institution="Provincial",
        ),
    )
    assert acct.id is not None
    cat = categories_repo.get_by_name(
        in_memory_db, TransactionKind.EXPENSE, "Food"
    )
    if cat is None:
        cat = categories_repo.insert(
            in_memory_db,
            Category(kind=TransactionKind.EXPENSE, name="Food"),
        )
    assert cat.id is not None

    occurred = datetime(2025, 11, 5, tzinfo=UTC)
    txn_repo.insert(
        in_memory_db,
        Transaction(
            account_id=acct.id,
            occurred_at=occurred,
            kind=TransactionKind.EXPENSE,
            amount=Decimal("-100"),
            currency="VES",
            description="UNMATCHED DEBIT 1",
            source="provincial",
            source_ref="hash:aaaa111100000001",
            needs_review=True,
        ),
    )
    txn_repo.insert(
        in_memory_db,
        Transaction(
            account_id=acct.id,
            occurred_at=occurred,
            kind=TransactionKind.EXPENSE,
            amount=Decimal("-200"),
            currency="VES",
            description="UNMATCHED DEBIT 2",
            source="provincial",
            source_ref="hash:aaaa111100000002",
            needs_review=True,
        ),
    )
    txn_repo.insert(
        in_memory_db,
        Transaction(
            account_id=acct.id,
            occurred_at=occurred,
            kind=TransactionKind.EXPENSE,
            amount=Decimal("-300"),
            currency="VES",
            description="RESOLVED DEBIT",
            source="provincial",
            source_ref="hash:aaaa111100000003",
            needs_review=False,
        ),
    )
    yield in_memory_db


def test_cleanup_applies_category_and_clears_flag(
    db_with_review_rows: sqlite3.Connection,
) -> None:
    from finances.migration.interactive_cleanup import run_cleanup

    # Two scripted answers, one per needs_review row.
    answers = iter([
        ("Food", None),      # first row: category only
        ("Food", "225"),     # second row: category + user_rate
    ])

    def prompt(_row: sqlite3.Row) -> tuple[str | None, str | None]:
        return next(answers)

    report = run_cleanup(db_with_review_rows, prompt=prompt)

    remaining = db_with_review_rows.execute(
        "SELECT COUNT(*) FROM transactions WHERE needs_review=1"
    ).fetchone()[0]
    assert remaining == 0
    assert report.rows_resolved == 2

    # user_rate persisted on the second row only.
    rate_rows = db_with_review_rows.execute(
        "SELECT source_ref, user_rate FROM transactions WHERE source_ref IN "
        "(?, ?) ORDER BY source_ref",
        ("hash:aaaa111100000001", "hash:aaaa111100000002"),
    ).fetchall()
    assert rate_rows[0]["user_rate"] is None
    assert Decimal(str(rate_rows[1]["user_rate"])) == Decimal("225")


def test_cleanup_skip_leaves_row_flagged(
    db_with_review_rows: sqlite3.Connection,
) -> None:
    from finances.migration.interactive_cleanup import run_cleanup

    answers = iter([
        (None, None),        # first row: skipped (no category supplied)
        ("Food", None),      # second row: answered
    ])

    def prompt(_row: sqlite3.Row) -> tuple[str | None, str | None]:
        return next(answers)

    report = run_cleanup(db_with_review_rows, prompt=prompt)

    remaining = db_with_review_rows.execute(
        "SELECT COUNT(*) FROM transactions WHERE needs_review=1"
    ).fetchone()[0]
    assert remaining == 1
    assert report.rows_resolved == 1
    assert report.rows_skipped == 1


def test_cleanup_unknown_category_raises(
    db_with_review_rows: sqlite3.Connection,
) -> None:
    from finances.migration.interactive_cleanup import run_cleanup

    def prompt(_row: sqlite3.Row) -> tuple[str | None, str | None]:
        return ("This Category Does Not Exist", None)

    with pytest.raises(ValueError, match="category"):
        run_cleanup(db_with_review_rows, prompt=prompt)
