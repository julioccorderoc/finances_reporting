"""Reconciling a position the custodian's history can no longer explain.

Binance serves internal-transfer history for six months. The ledger starts in
October 2025, so the movements that would explain most of the Binance
imbalance are gone from the API and are not coming back — probed directly:
a query starting six months back succeeds, seven months back fails with
``-5026``.

ADR-018 answers that with a reconciliation adjustment: one
``kind='adjustment'`` row per (account, currency), recording the difference
between the ledger and the custodian on the day the reconciliation was
performed. It is dated today rather than at the ledger's start because the gap
is mostly missing *mid-history* transfers — dating it to day one would assert
an opening position the owner never held, and for Binance Spot USDC that
position would have to be negative.

The custodian figure is always an input. Reading it from the API would make
the ledger agree with the API by construction, which is not a reconciliation.
"""

from __future__ import annotations

import sqlite3
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from finances.db.repos import accounts as accounts_repo
from finances.db.repos import transactions as txn_repo
from finances.domain.models import Account, AccountKind, Transaction, TransactionKind
from finances.domain.reconciliation_adjustments import record_adjustment
from finances.reports import consolidated_usd

WHEN = datetime(2026, 8, 4, 12, 0, tzinfo=UTC)


@pytest.fixture
def spot(in_memory_db: sqlite3.Connection):
    conn = in_memory_db
    account = accounts_repo.insert(
        conn,
        Account(
            name="Binance Spot",
            kind=AccountKind.CRYPTO_SPOT,
            currency="USDT",
            institution="Binance",
        ),
    )
    assert account.id is not None
    return conn, account


def _spend(conn: sqlite3.Connection, account, amount: str, currency: str, ref: str):
    return txn_repo.insert(
        conn,
        Transaction(
            account_id=account.id,
            occurred_at=datetime(2026, 3, 1, tzinfo=UTC),
            kind=TransactionKind.EXPENSE,
            amount=Decimal(amount),
            currency=currency,
            description="spend",
            source="binance",
            source_ref=ref,
        ),
    )


def _balance(conn: sqlite3.Connection, account_id: int, currency: str) -> Decimal:
    row = conn.execute(
        "SELECT COALESCE(SUM(CAST(amount AS REAL)), 0) FROM transactions "
        "WHERE account_id = ? AND currency = ?",
        (account_id, currency),
    ).fetchone()
    return Decimal(str(round(row[0], 8)))


def test_an_impossible_balance_is_lifted_to_the_custodian_figure(spot):
    """The ledger says -500 USDT; Binance says 0.05. Only one is possible."""
    conn, account = spot
    _spend(conn, account, "-500.00", "USDT", "binance:spend-1")

    result = record_adjustment(
        conn,
        account_id=account.id,
        currency="USDT",
        actual=Decimal("0.05"),
        occurred_at=WHEN,
    )

    assert result is not None
    assert result.delta == Decimal("500.05")
    assert _balance(conn, account.id, "USDT") == Decimal("0.05")


def test_the_row_is_an_adjustment_and_says_what_it_reconciled(spot):
    conn, account = spot
    _spend(conn, account, "-500.00", "USDT", "binance:spend-2")

    result = record_adjustment(
        conn,
        account_id=account.id,
        currency="USDT",
        actual=Decimal("0.05"),
        occurred_at=WHEN,
    )

    row = txn_repo.get_by_id(conn, result.transaction_id)
    assert row is not None
    assert row.kind is TransactionKind.ADJUSTMENT
    assert row.currency == "USDT"
    assert row.occurred_at.date() == WHEN.date()
    # Both figures and the date must be legible from the row itself — a
    # reader should never have to reconstruct why it exists.
    assert "-500" in row.description
    assert "0.05" in row.description
    assert "2026-08-04" in row.description


def test_an_adjustment_is_not_income(spot):
    """A +500 adjustment would otherwise be the biggest earning on record."""
    conn, account = spot
    _spend(conn, account, "-500.00", "USDT", "binance:spend-3")
    before = consolidated_usd.build_report(conn)

    record_adjustment(
        conn,
        account_id=account.id,
        currency="USDT",
        actual=Decimal("0.05"),
        occurred_at=WHEN,
    )
    after = consolidated_usd.build_report(conn)

    assert after.total_usd == before.total_usd
    assert len(after.rows) == len(before.rows)


def test_a_matching_position_is_left_alone(spot):
    """Nothing to reconcile means no row — re-running is safe."""
    conn, account = spot
    _spend(conn, account, "-500.00", "USDT", "binance:spend-4")
    record_adjustment(
        conn,
        account_id=account.id,
        currency="USDT",
        actual=Decimal("0.05"),
        occurred_at=WHEN,
    )

    again = record_adjustment(
        conn,
        account_id=account.id,
        currency="USDT",
        actual=Decimal("0.05"),
        occurred_at=WHEN,
    )

    assert again is None
    assert (
        conn.execute(
            "SELECT COUNT(*) FROM transactions WHERE kind = 'adjustment'"
        ).fetchone()[0]
        == 1
    )


def test_a_downward_adjustment_is_negative(spot):
    """Funding was overstated by 8,907 — the correction goes the other way."""
    conn, account = spot
    txn_repo.insert(
        conn,
        Transaction(
            account_id=account.id,
            occurred_at=datetime(2026, 3, 1, tzinfo=UTC),
            kind=TransactionKind.INCOME,
            amount=Decimal("9398.51"),
            currency="USDT",
            description="deposit",
            source="binance",
            source_ref="binance:deposit-1",
        ),
    )

    result = record_adjustment(
        conn,
        account_id=account.id,
        currency="USDT",
        actual=Decimal("490.68"),
        occurred_at=WHEN,
    )

    assert result.delta == Decimal("-8907.83")
    assert _balance(conn, account.id, "USDT") == Decimal("490.68")


def test_each_currency_reconciles_independently(spot):
    """A healthy USDC balance must not mask a broken USDT one."""
    conn, account = spot
    _spend(conn, account, "-500.00", "USDT", "binance:spend-5")
    _spend(conn, account, "-10.00", "USDC", "binance:spend-6")

    record_adjustment(
        conn,
        account_id=account.id,
        currency="USDT",
        actual=Decimal("0.05"),
        occurred_at=WHEN,
    )

    assert _balance(conn, account.id, "USDT") == Decimal("0.05")
    assert _balance(conn, account.id, "USDC") == Decimal("-10")


def test_repeated_reconciliations_do_not_collide(spot):
    """A later reconciliation of the same position gets its own row."""
    conn, account = spot
    _spend(conn, account, "-500.00", "USDT", "binance:spend-7")
    first = record_adjustment(
        conn,
        account_id=account.id,
        currency="USDT",
        actual=Decimal("0.05"),
        occurred_at=WHEN,
    )
    _spend(conn, account, "-7.00", "USDT", "binance:spend-8")

    second = record_adjustment(
        conn,
        account_id=account.id,
        currency="USDT",
        actual=Decimal("0.05"),
        occurred_at=datetime(2026, 9, 1, 12, 0, tzinfo=UTC),
    )

    assert second is not None
    assert second.transaction_id != first.transaction_id
    assert _balance(conn, account.id, "USDT") == Decimal("0.05")
