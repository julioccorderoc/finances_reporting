"""The owner's reason for a plug is stored with it (ADR-018 amendment).

``record_adjustment`` writes a description naming both figures and the date
— rule-012 requires exactly that and nothing here changes it. What it did
not carry was *why* the owner believed the gap was unrecoverable. The
viewer's Set balance surface requires that sentence before it will write,
so the domain function has to have somewhere to put it: ``notes``, the same
column the transaction modal already edits.
"""

from __future__ import annotations

import sqlite3
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from finances.db.migrate import apply_migrations
from finances.db.repos import accounts as accounts_repo
from finances.db.repos import transactions as transactions_repo
from finances.domain.models import (
    Account,
    AccountKind,
    Transaction,
    TransactionKind,
)
from finances.domain.reconciliation_adjustments import record_adjustment


@pytest.fixture
def conn() -> sqlite3.Connection:
    connection = sqlite3.connect(":memory:", isolation_level=None)
    connection.row_factory = sqlite3.Row
    apply_migrations(connection)
    return connection


@pytest.fixture
def account_id(conn: sqlite3.Connection) -> int:
    account = accounts_repo.insert(
        conn,
        Account(name="Binance Spot", kind=AccountKind.CRYPTO_SPOT, currency="USDT"),
    )
    assert account.id is not None
    transactions_repo.insert(
        conn,
        Transaction(
            account_id=account.id,
            occurred_at=datetime(2026, 8, 1, tzinfo=UTC),
            kind=TransactionKind.INCOME,
            amount=Decimal("100"),
            currency="USDT",
            description="seed",
            source="binance",
            source_ref="seed-1",
        ),
    )
    return account.id


def test_record_adjustment_stores_the_owner_note(
    conn: sqlite3.Connection, account_id: int
) -> None:
    result = record_adjustment(
        conn,
        account_id=account_id,
        currency="USDT",
        actual=Decimal("90"),
        occurred_at=datetime(2026, 9, 3, tzinfo=UTC),
        note="Spot↔Funding history older than six months; nothing left to sync.",
    )

    assert result is not None
    written = transactions_repo.get_by_id(conn, result.transaction_id)
    assert written is not None
    assert written.notes == (
        "Spot↔Funding history older than six months; nothing left to sync."
    )


def test_record_adjustment_without_a_note_leaves_notes_null(
    conn: sqlite3.Connection, account_id: int
) -> None:
    """The CLI path (ADR-018 §2.3) passes no note and must keep working."""
    result = record_adjustment(
        conn,
        account_id=account_id,
        currency="USDT",
        actual=Decimal("90"),
        occurred_at=datetime(2026, 9, 3, tzinfo=UTC),
    )

    assert result is not None
    written = transactions_repo.get_by_id(conn, result.transaction_id)
    assert written is not None
    assert written.notes is None


def test_the_note_never_replaces_the_description(
    conn: sqlite3.Connection, account_id: int
) -> None:
    """rule-012: the description still names both figures and the date."""
    result = record_adjustment(
        conn,
        account_id=account_id,
        currency="USDT",
        actual=Decimal("90"),
        occurred_at=datetime(2026, 9, 3, tzinfo=UTC),
        note="counted it myself",
    )

    assert result is not None
    written = transactions_repo.get_by_id(conn, result.transaction_id)
    assert written is not None
    assert written.description is not None
    assert "100" in written.description
    assert "90" in written.description
    assert "2026-09-03" in written.description
