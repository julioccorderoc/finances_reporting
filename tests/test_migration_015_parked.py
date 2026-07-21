"""transactions.parked — durable triage deferral (spec §5.3).

Replaces a per-process in-memory skip set that was destroyed by the
always-visible Stop-server button, i.e. by the designed way to end a
session. The column must survive re-ingest, because the whole promise of
Park is that a deferral outlives the session that made it.
"""

from __future__ import annotations

import sqlite3
from datetime import UTC, datetime
from decimal import Decimal

from finances.db.repos import accounts as accounts_repo
from finances.db.repos import transactions as transactions_repo
from finances.domain.models import (
    Account,
    AccountKind,
    Transaction,
    TransactionKind,
)


def _txn(account_id: int, **over) -> Transaction:
    base = dict(
        account_id=account_id,
        occurred_at=datetime(2026, 5, 1, tzinfo=UTC),
        kind=TransactionKind.EXPENSE,
        amount=Decimal("-100.00"),
        currency="VES",
        description="COM.PAGO bodega",
        source="provincial",
        source_ref="park-1",
    )
    base.update(over)
    return Transaction(**base)


def test_column_exists_and_defaults_to_zero(seeded_db: sqlite3.Connection) -> None:
    cols = {
        r["name"]: r
        for r in seeded_db.execute("PRAGMA table_info(transactions)").fetchall()
    }
    assert "parked" in cols
    assert cols["parked"]["notnull"] == 1
    assert str(cols["parked"]["dflt_value"]) == "0"


def test_model_defaults_to_not_parked(seeded_db: sqlite3.Connection) -> None:
    account = accounts_repo.insert(
        seeded_db, Account(name="P", kind=AccountKind.BANK, currency="VES")
    )
    stored = transactions_repo.insert(seeded_db, _txn(account.id))

    assert stored.parked is False
    assert transactions_repo.get_by_id(seeded_db, stored.id).parked is False


def test_update_can_park_and_unpark(seeded_db: sqlite3.Connection) -> None:
    account = accounts_repo.insert(
        seeded_db, Account(name="P", kind=AccountKind.BANK, currency="VES")
    )
    stored = transactions_repo.insert(seeded_db, _txn(account.id))

    transactions_repo.update(seeded_db, id=stored.id, parked=True)
    assert transactions_repo.get_by_id(seeded_db, stored.id).parked is True

    transactions_repo.update(seeded_db, id=stored.id, parked=False)
    assert transactions_repo.get_by_id(seeded_db, stored.id).parked is False


def test_parking_does_not_touch_needs_review(
    seeded_db: sqlite3.Connection,
) -> None:
    """rule-012: parked is a separate flag, never a needs_review proxy."""
    account = accounts_repo.insert(
        seeded_db, Account(name="P", kind=AccountKind.BANK, currency="VES")
    )
    stored = transactions_repo.insert(
        seeded_db, _txn(account.id, needs_review=True)
    )

    transactions_repo.update(seeded_db, id=stored.id, parked=True)

    assert transactions_repo.get_by_id(seeded_db, stored.id).needs_review is True


def test_parked_survives_reingest(seeded_db: sqlite3.Connection) -> None:
    """The core promise: re-running ingest must not un-park a row.

    `parked` is deliberately absent from upsert_by_source_ref's
    ON CONFLICT DO UPDATE SET list, so the column is simply left alone.
    """
    account = accounts_repo.insert(
        seeded_db, Account(name="P", kind=AccountKind.BANK, currency="VES")
    )
    first = transactions_repo.upsert_by_source_ref(seeded_db, _txn(account.id))
    transactions_repo.update(seeded_db, id=first["id"], parked=True)

    # Same source_ref arriving again from a statement re-drop.
    again = transactions_repo.upsert_by_source_ref(
        seeded_db, _txn(account.id, description="COM.PAGO bodega")
    )

    assert again["rows_updated"] == 1
    assert transactions_repo.get_by_id(seeded_db, first["id"]).parked is True


def test_factory_never_parks_randomly() -> None:
    """polyfactory does not pin bools; an unpinned parked would flake tests."""
    from tests.conftest import TransactionFactory

    assert all(TransactionFactory.build().parked is False for _ in range(25))
