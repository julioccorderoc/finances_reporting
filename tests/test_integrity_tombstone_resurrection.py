"""`finances doctor` must notice a deleted row that came back (ADR-022 §4).

The tombstone rule lives in exactly one place: ``upsert_by_source_ref``.
That is the design — every importer and the backfill enter the ledger
through it — but it also means a future write path that bypasses the repo
would resurrect deleted rows silently, and the owner would only find out
by seeing the row again.

So the check states the consequence rather than the mechanism: a
``(source, source_ref)`` that is tombstoned AND present in
``transactions`` is an error, whatever wrote it.
"""

from __future__ import annotations

import sqlite3
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from finances.db.repos import accounts as accounts_repo
from finances.db.repos import transactions as txn_repo
from finances.domain import integrity
from finances.domain.models import (
    Account,
    AccountKind,
    Transaction,
    TransactionKind,
)

CHECK = "tombstoned_row_is_back"


@pytest.fixture
def spot(in_memory_db: sqlite3.Connection) -> Account:
    return accounts_repo.insert(
        in_memory_db,
        Account(name="Binance Spot", kind=AccountKind.CRYPTO_SPOT, currency="USDT"),
    )


def _row(account: Account, source_ref: str = "pay:1") -> Transaction:
    return Transaction(
        account_id=account.id,
        occurred_at=datetime(2025, 11, 6, tzinfo=UTC),
        kind=TransactionKind.EXPENSE,
        amount=Decimal("-700"),
        currency="USDT",
        description="Binance Pay C2C (outgoing)",
        source="binance",
        source_ref=source_ref,
    )


def _names(report: integrity.IntegrityReport) -> set[str]:
    return {f.check for f in report.findings}


def test_the_check_is_registered() -> None:
    assert CHECK in {c.name for c in integrity.CHECKS}


def test_a_clean_ledger_says_nothing(
    in_memory_db: sqlite3.Connection, spot: Account
) -> None:
    txn_repo.insert(in_memory_db, _row(spot))

    assert CHECK not in _names(integrity.run_checks(in_memory_db))


def test_a_deleted_row_stays_quiet(
    in_memory_db: sqlite3.Connection, spot: Account
) -> None:
    """A tombstone with no row behind it is the normal, healthy state."""
    stored = txn_repo.insert(in_memory_db, _row(spot))
    txn_repo.delete(in_memory_db, stored.id, reason="twin of 859")

    assert CHECK not in _names(integrity.run_checks(in_memory_db))


def test_a_resurrected_row_is_an_error(
    in_memory_db: sqlite3.Connection, spot: Account
) -> None:
    """Something bypassed the repo — the one way this can happen."""
    stored = txn_repo.insert(in_memory_db, _row(spot))
    txn_repo.delete(in_memory_db, stored.id, reason="twin of 859")
    # A hand-rolled INSERT, i.e. exactly the write path the check exists for.
    in_memory_db.execute(
        """
        INSERT INTO transactions
            (account_id, occurred_at, kind, amount, currency, description,
             source, source_ref)
        VALUES (?, '2025-11-06T00:00:00+00:00', 'expense', '-700', 'USDT',
                'resurrected', 'binance', 'pay:1')
        """,
        (spot.id,),
    )

    report = integrity.run_checks(in_memory_db)

    finding = next(f for f in report.findings if f.check == CHECK)
    assert finding.severity is integrity.Severity.ERROR
    assert finding.count == 1
    assert len(finding.sample_ids) == 1


def test_the_tombstone_only_speaks_for_its_own_pair(
    in_memory_db: sqlite3.Connection, spot: Account
) -> None:
    """``pay:1`` deleted must not indict a live ``pay:2``."""
    stored = txn_repo.insert(in_memory_db, _row(spot))
    txn_repo.delete(in_memory_db, stored.id, reason=None)
    txn_repo.insert(in_memory_db, _row(spot, source_ref="pay:2"))

    assert CHECK not in _names(integrity.run_checks(in_memory_db))
