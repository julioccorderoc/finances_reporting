"""Reversing a plug, through the module that wrote it (ADR-028 §2).

ADR-018's amendment says a plug the owner no longer believes can be removed
outright — "ADR-022's delete covers removing one outright". ADR-022 §2.3
refuses every ``reconciliation`` row by default, because removing one by hand
re-opens what it closed. Both stay true: the door is narrow and belongs to the
module that wrote the row. :func:`reverse_adjustment` is that door — a
tombstone naming the reason, the row gone, and the position it was holding
open released to say what it really holds.

An opening position (``opening_balance``) is the different claim and has no
door here at all: it is *restated* through ``record_opening`` (ADR-020 §2.4),
never deleted, so a reversal of one is refused loudly.
"""

from __future__ import annotations

import sqlite3
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from finances.db.repos import accounts as accounts_repo
from finances.db.repos import transactions as txn_repo
from finances.domain.models import Transaction, TransactionKind
from finances.domain.reconciliation_adjustments import (
    record_adjustment,
    reverse_adjustment,
)

WHEN = datetime(2026, 8, 8, 11, 41, tzinfo=UTC)


@pytest.fixture
def earn(seeded_db: sqlite3.Connection):
    conn = seeded_db
    account = accounts_repo.get_by_name(conn, "Binance Earn")
    assert account is not None and account.id is not None
    return conn, account


def _row(conn: sqlite3.Connection, account_id: int, amount: str, ref: str):
    return txn_repo.insert(
        conn,
        Transaction(
            account_id=account_id,
            occurred_at=datetime(2026, 3, 1, tzinfo=UTC),
            kind=TransactionKind.INCOME,
            amount=Decimal(amount),
            currency="USDT",
            description="Earn reward",
            source="binance",
            source_ref=ref,
        ),
    )


def _balance(conn: sqlite3.Connection, account_id: int, currency: str = "USDT") -> Decimal:
    row = conn.execute(
        "SELECT COALESCE(SUM(CAST(amount AS REAL)), 0) FROM transactions "
        "WHERE account_id = ? AND currency = ?",
        (account_id, currency),
    ).fetchone()
    return Decimal(str(round(row[0], 8)))


def _plug(conn: sqlite3.Connection, account_id: int) -> int:
    """A plug the ledger is carrying, written the way the ledger writes one."""
    _row(conn, account_id, "10.00", "binance:earn-1")
    result = record_adjustment(
        conn,
        account_id=account_id,
        currency="USDT",
        actual=Decimal("7.00"),
        occurred_at=WHEN,
    )
    assert result is not None
    return result.transaction_id


def _tombstones(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM deleted_transactions ORDER BY source, source_ref"
    ).fetchall()


def test_reversing_a_plug_removes_the_row(earn) -> None:
    conn, account = earn
    plug_id = _plug(conn, account.id)

    reverse_adjustment(conn, transaction_id=plug_id, reason="ADR-028: explained")

    assert txn_repo.get_by_id(conn, plug_id) is None


def test_reversing_a_plug_returns_the_position_it_held(earn) -> None:
    """The plug made the position read 7.00; reversing it releases that claim."""
    conn, account = earn
    plug_id = _plug(conn, account.id)
    assert _balance(conn, account.id) == Decimal("7")

    reverse_adjustment(conn, transaction_id=plug_id, reason="ADR-028: explained")

    assert _balance(conn, account.id) == Decimal("10")


def test_the_tombstone_carries_the_reason(earn) -> None:
    """Why it went matters as much as that it went (ADR-022 §2.1)."""
    conn, account = earn
    plug_id = _plug(conn, account.id)

    reverse_adjustment(
        conn, transaction_id=plug_id, reason="ADR-028: the BONUS misplacement"
    )

    rows = _tombstones(conn)
    assert len(rows) == 1
    assert rows[0]["source"] == "reconciliation"
    assert rows[0]["reason"] == "ADR-028: the BONUS misplacement"
    assert datetime.fromisoformat(rows[0]["deleted_at"]).tzinfo is not None


def test_reversal_reports_what_it_removed(earn) -> None:
    conn, account = earn
    plug_id = _plug(conn, account.id)

    result = reverse_adjustment(conn, transaction_id=plug_id, reason="gone")

    assert result.transaction_id == plug_id
    assert result.account_id == account.id
    assert result.currency == "USDT"
    assert result.amount == Decimal("-3.00")


def test_a_plain_delete_still_refuses_the_ledgers_own_row(earn) -> None:
    """The guard is untouched: only the module that wrote the plug may remove it."""
    conn, account = earn
    plug_id = _plug(conn, account.id)

    with pytest.raises(ValueError, match="reconciliation"):
        txn_repo.delete(conn, plug_id, reason="by hand")

    assert txn_repo.get_by_id(conn, plug_id) is not None
    assert _tombstones(conn) == []


def test_an_opening_position_is_refused(earn) -> None:
    """It is restated through record_opening, never reversed (ADR-020)."""
    conn, account = earn
    opening = txn_repo.insert(
        conn,
        Transaction(
            account_id=account.id,
            occurred_at=datetime(2025, 10, 3, tzinfo=UTC),
            kind=TransactionKind.ADJUSTMENT,
            amount=Decimal("50"),
            currency="USDT",
            description="Opening balance USDT",
            source="opening_balance",
            source_ref=f"opening:{account.id}:USDT",
        ),
    )
    assert opening.id is not None

    with pytest.raises(ValueError, match="opening"):
        reverse_adjustment(conn, transaction_id=opening.id, reason="nope")

    assert txn_repo.get_by_id(conn, opening.id) is not None
    assert _tombstones(conn) == []


def test_an_ordinary_row_is_refused(earn) -> None:
    conn, account = earn
    row = _row(conn, account.id, "10.00", "binance:earn-2")
    assert row.id is not None

    with pytest.raises(ValueError, match="plug"):
        reverse_adjustment(conn, transaction_id=row.id, reason="nope")

    assert _tombstones(conn) == []


def test_an_unknown_id_raises_lookup_error(earn) -> None:
    conn, _account = earn
    with pytest.raises(LookupError):
        reverse_adjustment(conn, transaction_id=9999, reason="gone")


def test_reversing_twice_raises_lookup_error(earn) -> None:
    """The tombstone does not resurrect the row; the second call simply fails."""
    conn, account = earn
    plug_id = _plug(conn, account.id)
    reverse_adjustment(conn, transaction_id=plug_id, reason="gone")

    with pytest.raises(LookupError):
        reverse_adjustment(conn, transaction_id=plug_id, reason="again")
