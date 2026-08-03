"""Currency movement must not be reported as income or expense.

The live ledger carried 46 rows the owner had categorised ``Internal
Transfer`` / ``External Transfer`` which every report still counted as
spending, because reports filtered on ``kind`` alone. That was $7 526 on a
headline of -$982 — the reported figure had the wrong sign.

These tests pin the rule in both report builders and in the shared
predicate they now share.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from finances.db.repos import accounts as accounts_repo
from finances.db.repos import categories as categories_repo
from finances.db.repos import transactions as txn_repo
from finances.domain import money
from finances.domain.models import (
    Account,
    AccountKind,
    Category,
    Transaction,
    TransactionKind,
)
from finances.reports import consolidated_usd, monthly


@pytest.fixture
def ledger(in_memory_db):
    """One USD account, one expense category, one transfer category."""
    conn = in_memory_db
    account = accounts_repo.insert(
        conn,
        Account(name="Binance Spot", kind=AccountKind.CRYPTO_SPOT, currency="USDT"),
    )
    groceries = categories_repo.get_by_name(
        conn, TransactionKind.EXPENSE, "Groceries"
    )
    internal = categories_repo.get_by_name(
        conn, TransactionKind.TRANSFER, "Internal Transfer"
    )
    assert groceries is not None and internal is not None
    return conn, account, groceries, internal


def _spend(account, category, *, amount, ref, kind=TransactionKind.EXPENSE):
    return Transaction(
        account_id=account.id,
        occurred_at=datetime(2026, 5, 12, tzinfo=UTC),
        kind=kind,
        amount=Decimal(amount),
        currency="USDT",
        description="row",
        category_id=category.id if category else None,
        source="binance",
        source_ref=ref,
    )


def test_movement_category_ids_finds_transfer_kinds(ledger):
    conn, _account, groceries, internal = ledger
    ids = money.movement_category_ids(conn)
    assert internal.id in ids
    assert groceries.id not in ids


def test_is_currency_movement_covers_both_shapes(ledger):
    conn, account, groceries, internal = ledger
    ids = money.movement_category_ids(conn)

    real_spend = _spend(account, groceries, amount="-10", ref="a")
    tagged_movement = _spend(account, internal, amount="-10", ref="b")
    paired_transfer = _spend(
        account, None, amount="-10", ref="c", kind=TransactionKind.TRANSFER
    )

    assert money.is_currency_movement(real_spend, ids) is False
    assert money.is_currency_movement(tagged_movement, ids) is True
    assert money.is_currency_movement(paired_transfer, ids) is True


def test_consolidated_excludes_rows_categorised_as_transfer(ledger):
    """The regression that cost $7 526: category ignored, only kind read."""
    conn, account, groceries, internal = ledger
    txn_repo.insert(conn, _spend(account, groceries, amount="-40", ref="spend"))
    txn_repo.insert(conn, _spend(account, internal, amount="-1000", ref="moved"))

    report = consolidated_usd.build_report(conn)

    assert [r.amount_native for r in report.rows] == [Decimal("-40")]
    assert report.total_usd == Decimal("-40")


def test_monthly_excludes_rows_categorised_as_transfer(ledger):
    conn, account, groceries, internal = ledger
    txn_repo.insert(conn, _spend(account, groceries, amount="-40", ref="spend"))
    txn_repo.insert(conn, _spend(account, internal, amount="-1000", ref="moved"))

    report = monthly.build_report(conn, month="2026-05")

    assert report.grand_total_usd == Decimal("-40")
    assert all(r.category_name != "Internal Transfer" for r in report.rows)


def test_income_side_of_a_convert_is_excluded_too(ledger):
    """A same-account convert books an expense AND an income row.

    Both carry the transfer category, so both must drop out — otherwise the
    net looks right while gross income and gross expense are each inflated.
    """
    conn, account, groceries, internal = ledger
    txn_repo.insert(conn, _spend(account, groceries, amount="-40", ref="spend"))
    txn_repo.insert(conn, _spend(account, internal, amount="-500", ref="conv-out"))
    txn_repo.insert(
        conn,
        _spend(
            account, internal, amount="500", ref="conv-in", kind=TransactionKind.INCOME
        ),
    )

    report = monthly.build_report(conn, month="2026-05")

    assert report.grand_total_usd == Decimal("-40")
    assert sum(r.tx_count for r in report.rows) == 1


def test_uncategorised_rows_are_still_counted(ledger):
    """The exclusion keys off a transfer category, never off its absence."""
    conn, account, _groceries, _internal = ledger
    txn_repo.insert(conn, _spend(account, None, amount="-25", ref="no-cat"))

    report = consolidated_usd.build_report(conn)

    assert report.total_usd == Decimal("-25")


def test_both_reports_still_agree(ledger):
    """The cross-builder invariant must survive the new predicate."""
    conn, account, groceries, internal = ledger
    txn_repo.insert(conn, _spend(account, groceries, amount="-40", ref="spend"))
    txn_repo.insert(conn, _spend(account, internal, amount="-1000", ref="moved"))
    txn_repo.insert(
        conn,
        _spend(
            account, groceries, amount="90", ref="in", kind=TransactionKind.INCOME
        ),
    )

    assert (
        consolidated_usd.build_report(conn).total_usd
        == monthly.build_report(conn).grand_total_usd
    )


def test_to_usd_is_the_only_conversion(ledger):
    """Native-USD short-circuits before user_rate can be misread as a rate."""
    conn, account, groceries, _internal = ledger
    fill = _spend(account, groceries, amount="-50", ref="p2p:1")
    fill = fill.model_copy(update={"user_rate": Decimal("848.10")})

    amount_usd, source = money.to_usd(conn, fill)

    assert source == money.NATIVE_USD_SOURCE
    assert amount_usd == Decimal("-50")


def test_category_kinds_are_the_only_movement_marker(ledger):
    """An expense-kind category never excludes a row, whatever it is named."""
    conn, account, _groceries, _internal = ledger
    decoy = categories_repo.insert(
        conn, Category(kind=TransactionKind.EXPENSE, name="Transfer Fee")
    )
    txn_repo.insert(conn, _spend(account, decoy, amount="-5", ref="fee"))

    assert consolidated_usd.build_report(conn).total_usd == Decimal("-5")
