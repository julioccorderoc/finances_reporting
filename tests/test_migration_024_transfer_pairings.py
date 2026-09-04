"""Migration 024 — ``transfer_pairings``, the pre-image a pairing leaves behind.

``_promote_to_transfer`` overwrites ``kind`` and ``needs_review`` with a raw
UPDATE and records nothing, so until now breaking a pair could only *guess*
what the rows were before. ``transaction_edits`` cannot hold the answer:
migration 009 constrains its ``field`` column to
``('category_id','user_rate','notes')``, and widening that would put a
machine-written ``kind`` promotion into the owner's own edit history.

So the pre-image gets its own table. It is pairing provenance, not an edit.
"""

from __future__ import annotations

import sqlite3

import pytest


def _columns(conn: sqlite3.Connection, table: str) -> dict[str, sqlite3.Row]:
    return {r["name"]: r for r in conn.execute(f"PRAGMA table_info({table})")}


def _a_transaction(conn: sqlite3.Connection, source_ref: str = "pay:1") -> int:
    """One real row — ``transaction_id`` is a foreign key and FKs are ON."""
    from datetime import UTC, datetime
    from decimal import Decimal

    from finances.db.repos import transactions as transactions_repo
    from finances.domain.models import Transaction, TransactionKind

    stored = transactions_repo.insert(
        conn,
        Transaction(
            account_id=2,
            occurred_at=datetime(2026, 8, 15, tzinfo=UTC),
            kind=TransactionKind.EXPENSE,
            amount=Decimal("-580"),
            currency="USDT",
            description="Binance Pay C2C (outgoing)",
            source="binance",
            source_ref=source_ref,
        ),
    )
    assert stored.id is not None
    return stored.id


def test_table_exists(in_memory_db: sqlite3.Connection) -> None:
    row = in_memory_db.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='transfer_pairings'"
    ).fetchone()
    assert row is not None, "migration 024 did not create transfer_pairings"


def test_records_the_pre_image_of_one_leg(in_memory_db: sqlite3.Connection) -> None:
    cols = _columns(in_memory_db, "transfer_pairings")
    assert set(cols) == {
        "transfer_id",
        "transaction_id",
        "prior_kind",
        "prior_needs_review",
        "prior_user_rate",
        "created_at",
    }


def test_one_row_per_leg(seeded_db: sqlite3.Connection) -> None:
    """``transaction_id`` is the primary key: a row is one leg of one pair.

    Not ``(transfer_id, transaction_id)`` — a transaction belongs to at most
    one transfer at a time, and a second provenance row for the same leg
    would make the replay ambiguous.
    """
    txn_id = _a_transaction(seeded_db)
    seeded_db.execute(
        "INSERT INTO transfer_pairings "
        "(transfer_id, transaction_id, prior_kind, prior_needs_review, created_at) "
        "VALUES ('t1', ?, 'expense', 1, '2026-09-04T00:00:00+00:00')",
        (txn_id,),
    )
    with pytest.raises(sqlite3.IntegrityError):
        seeded_db.execute(
            "INSERT INTO transfer_pairings "
            "(transfer_id, transaction_id, prior_kind, prior_needs_review, created_at) "
            "VALUES ('t2', ?, 'income', 0, '2026-09-04T00:00:00+00:00')",
            (txn_id,),
        )


def test_provenance_goes_when_the_row_goes(seeded_db: sqlite3.Connection) -> None:
    """ON DELETE CASCADE: a deleted transaction leaves no orphan pre-image.

    Without the cascade, deleting an unpaired ex-leg would strand its row
    here, and a later transaction reusing that rowid would inherit somebody
    else's prior kind.
    """
    txn_id = _a_transaction(seeded_db)
    seeded_db.execute(
        "INSERT INTO transfer_pairings "
        "(transfer_id, transaction_id, prior_kind, prior_needs_review, created_at) "
        "VALUES ('t1', ?, 'expense', 1, '2026-09-04T00:00:00+00:00')",
        (txn_id,),
    )
    seeded_db.execute("DELETE FROM transactions WHERE id = ?", (txn_id,))

    assert (
        seeded_db.execute("SELECT COUNT(*) FROM transfer_pairings").fetchone()[0] == 0
    )
