"""`doctor` watches the pre-images that make unpairing possible.

Migration 024 records what each row was before it became a transfer leg,
and :func:`finances.domain.transfers.unpair` consumes that record when it
breaks the pair. A pre-image left behind — the pair gone, or the row since
paired into a *different* transfer — is not visible anywhere: the ledger
looks right, and the damage only appears the next time someone breaks a
pair and the row is restored to a kind it had in another life.

That is exactly the class of defect `finances doctor` exists to name.

Per rule-011 these land before the implementation.
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
from finances.domain.transfers import create_transfer, unpair

CHECK = "stale_pair_pre_image"
AT = datetime(2026, 5, 10, 12, 0, tzinfo=UTC)


@pytest.fixture
def paired(in_memory_db: sqlite3.Connection) -> sqlite3.Connection:
    """One bank deposit paired with one Binance sell, pre-images recorded."""
    conn = in_memory_db
    bank = accounts_repo.insert(
        conn, Account(name="Provincial", kind=AccountKind.BANK, currency="VES")
    )
    spot = accounts_repo.insert(
        conn,
        Account(name="Binance Spot", kind=AccountKind.CRYPTO_SPOT, currency="USDT"),
    )
    deposit = txn_repo.insert(
        conn,
        Transaction(
            account_id=bank.id,
            occurred_at=AT,
            kind=TransactionKind.INCOME,
            amount=Decimal("20000"),
            currency="VES",
            source="provincial",
            source_ref="dep-1",
        ),
    )
    sell = txn_repo.insert(
        conn,
        Transaction(
            account_id=spot.id,
            occurred_at=AT,
            kind=TransactionKind.EXPENSE,
            amount=Decimal("-30.83"),
            currency="USDT",
            user_rate=Decimal("648.65"),
            source="binance",
            source_ref="sell-1",
        ),
    )
    create_transfer(
        conn,
        anchor_transaction_id=deposit.id,
        counterpart_transaction_id=sell.id,
    )
    return conn


def _names(report) -> set[str]:
    return {f.check for f in report.findings}


def test_a_recorded_pair_is_not_a_finding(paired: sqlite3.Connection) -> None:
    assert CHECK not in _names(integrity.run_checks(paired))


def test_unpairing_leaves_nothing_behind(paired: sqlite3.Connection) -> None:
    transfer_id = paired.execute(
        "SELECT transfer_id FROM transactions WHERE source_ref = 'dep-1'"
    ).fetchone()["transfer_id"]

    unpair(paired, transfer_id=transfer_id)

    assert CHECK not in _names(integrity.run_checks(paired))


def test_a_pre_image_whose_pair_is_gone_is_reported(
    paired: sqlite3.Connection,
) -> None:
    # deliberate malformed fixture: the pair broken without consuming the
    # pre-image, which is what any path other than unpair() would do.
    paired.execute(
        "UPDATE transactions SET kind = 'income', transfer_id = NULL "
        "WHERE source_ref = 'dep-1'"
    )

    report = integrity.run_checks(paired)

    assert CHECK in _names(report)
    finding = next(f for f in report.findings if f.check == CHECK)
    deposit_id = paired.execute(
        "SELECT id FROM transactions WHERE source_ref = 'dep-1'"
    ).fetchone()["id"]
    assert deposit_id in finding.sample_ids


def test_a_pre_image_pointing_at_another_transfer_is_reported(
    paired: sqlite3.Connection,
) -> None:
    """The row was re-paired without recording afresh: restoring it would
    replay a kind from a transfer it no longer belongs to."""
    paired.execute(
        "UPDATE transactions SET transfer_id = 'some-other-transfer' "
        "WHERE source_ref = 'sell-1'"
    )

    assert CHECK in _names(integrity.run_checks(paired))
