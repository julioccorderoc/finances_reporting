"""`finances doctor` names every plug the ledger is carrying (ADR-018 §2).

An adjustment is a deliberate admission that a difference could not be
explained. ADR-018 argues it must stay *visible* — a residual that quietly
disappears into the balance is the failure mode ADR-020 §1.2 recorded, where
three plugs sized against corrupted balances left doctor reporting a healthy
ledger that overstated income by 10,462.71 USDC.

So doctor lists them. WARNING, not ERROR: writing one is a legitimate act,
carrying one silently is not.
"""

from __future__ import annotations

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

CHECK = "reconciliation_adjustments"


def _names(report):
    return {f.check for f in report.findings}


def _finding(report, name):
    return next(f for f in report.findings if f.check == name)


@pytest.fixture
def spot(in_memory_db):
    conn = in_memory_db
    account = accounts_repo.insert(
        conn,
        Account(name="Binance Spot", kind=AccountKind.CRYPTO_SPOT, currency="USDT"),
    )
    return conn, account


def _adjustment(account, *, source: str, source_ref: str, amount: str = "10.5"):
    return Transaction(
        account_id=account.id,
        occurred_at=datetime(2026, 9, 3, tzinfo=UTC),
        kind=TransactionKind.ADJUSTMENT,
        amount=Decimal(amount),
        currency="USDT",
        description="Reconciliation to custodian balance",
        source=source,
        source_ref=source_ref,
    )


def test_check_is_registered(spot) -> None:
    assert CHECK in {c.name for c in integrity.CHECKS}


def test_doctor_lists_a_reconciliation_adjustment(spot) -> None:
    conn, account = spot
    written = txn_repo.insert(
        conn,
        _adjustment(account, source="reconciliation", source_ref="reconcile:1:USDT:a"),
    )

    report = integrity.run_checks(conn)

    assert CHECK in _names(report)
    finding = _finding(report, CHECK)
    assert finding.severity is integrity.Severity.WARNING
    assert finding.count == 1
    assert finding.sample_ids == [written.id]


def test_a_plug_is_a_warning_not_an_error(spot) -> None:
    """A ledger carrying plugs is still a passing ledger — ``doctor --strict``
    must not exit non-zero over a decision the owner deliberately made."""
    conn, account = spot
    txn_repo.insert(
        conn,
        _adjustment(account, source="reconciliation", source_ref="reconcile:1:USDT:a"),
    )

    report = integrity.run_checks(conn)

    assert report.ok


def test_opening_positions_are_not_listed(spot) -> None:
    """ADR-020 rows are also ``kind='adjustment'`` and are a different claim:
    the books began mid-story, not "this drifted and I plugged it"."""
    conn, account = spot
    txn_repo.insert(
        conn,
        _adjustment(
            account, source="opening_balance", source_ref="opening:1:USDT", amount="50"
        ),
    )

    report = integrity.run_checks(conn)

    assert CHECK not in _names(report)


def test_a_clean_ledger_says_nothing(spot) -> None:
    conn, _account = spot
    assert CHECK not in _names(integrity.run_checks(conn))
