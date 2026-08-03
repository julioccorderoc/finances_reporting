"""New ledger invariants, each one a defect that reached production unseen.

``finances doctor`` ran ten checks and every defect found in the 2026-08-03
logic review slipped past all of them. These are the gaps, each stated as
the thing that would have caught it on the day it happened:

* a Binance USDT position of **-6,990.30** — an impossible asset balance,
  and nothing asked
* cross-currency transfer pairs, exempted from the netting check, which is
  the hole the manual pair-confirm walks through
* rows carrying a category from a contradicting kind (65 of them)
* uncategorized rows that carry no ``needs_review`` flag (37 of them), so
  every surface reading the raw column calls them finished
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from finances.db.repos import accounts as accounts_repo
from finances.db.repos import categories as categories_repo
from finances.db.repos import transactions as txn_repo
from finances.domain import integrity
from finances.domain.models import (
    Account,
    AccountKind,
    Transaction,
    TransactionKind,
)


def _names(report):
    return {f.check for f in report.findings}


def _finding(report, name):
    return next(f for f in report.findings if f.check == name)


@pytest.fixture
def accounts(in_memory_db):
    conn = in_memory_db
    spot = accounts_repo.insert(
        conn,
        Account(name="Binance Spot", kind=AccountKind.CRYPTO_SPOT, currency="USDT"),
    )
    bank = accounts_repo.insert(
        conn, Account(name="Provincial", kind=AccountKind.BANK, currency="VES")
    )
    return conn, spot, bank


def _row(account, **kw):
    base = dict(
        account_id=account.id,
        occurred_at=datetime(2026, 5, 12, tzinfo=UTC),
        kind=TransactionKind.EXPENSE,
        amount=Decimal("-10"),
        currency="USDT",
        description="row",
        source="binance",
        source_ref="r1",
    )
    base.update(kw)
    return Transaction(**base)


# ---------------------------------------------------------------------------
# negative_asset_balance
# ---------------------------------------------------------------------------


def test_negative_asset_balance_is_an_error(accounts):
    """You cannot hold minus seven thousand USDT."""
    conn, spot, _bank = accounts
    txn_repo.insert(conn, _row(spot, amount=Decimal("-500"), source_ref="a"))
    txn_repo.insert(
        conn,
        _row(spot, kind=TransactionKind.INCOME, amount=Decimal("100"), source_ref="b"),
    )

    report = integrity.run_checks(conn)

    assert "negative_asset_balance" in _names(report)
    finding = _finding(report, "negative_asset_balance")
    assert finding.severity is integrity.Severity.ERROR
    assert finding.count == 1


def test_a_positive_asset_balance_is_silent(accounts):
    conn, spot, _bank = accounts
    txn_repo.insert(
        conn,
        _row(spot, kind=TransactionKind.INCOME, amount=Decimal("100"), source_ref="b"),
    )
    txn_repo.insert(conn, _row(spot, amount=Decimal("-40"), source_ref="a"))

    assert "negative_asset_balance" not in _names(integrity.run_checks(conn))


def test_each_asset_is_checked_separately(accounts):
    """A positive USDC balance must not mask a negative USDT one."""
    conn, spot, _bank = accounts
    txn_repo.insert(
        conn,
        _row(
            spot,
            kind=TransactionKind.INCOME,
            amount=Decimal("5000"),
            currency="USDC",
            source_ref="usdc",
        ),
    )
    txn_repo.insert(conn, _row(spot, amount=Decimal("-500"), source_ref="usdt"))

    assert "negative_asset_balance" in _names(integrity.run_checks(conn))


# ---------------------------------------------------------------------------
# category_kind_mismatch
# ---------------------------------------------------------------------------


def test_income_category_on_an_expense_row_is_flagged(accounts):
    conn, spot, _bank = accounts
    salary = categories_repo.get_by_name(conn, TransactionKind.INCOME, "Salary")
    txn_repo.insert(
        conn,
        _row(
            spot,
            kind=TransactionKind.INCOME,
            amount=Decimal("50"),
            source_ref="ok",
            category_id=salary.id,
        ),
    )
    txn_repo.insert(conn, _row(spot, source_ref="bad", category_id=salary.id))

    report = integrity.run_checks(conn)

    assert "category_kind_mismatch" in _names(report)
    assert _finding(report, "category_kind_mismatch").count == 1


def test_a_transfer_category_is_not_a_mismatch(accounts):
    """It is how the owner declares money movement — reports act on it."""
    conn, spot, _bank = accounts
    internal = categories_repo.get_by_name(
        conn, TransactionKind.TRANSFER, "Internal Transfer"
    )
    txn_repo.insert(conn, _row(spot, source_ref="moved", category_id=internal.id))

    assert "category_kind_mismatch" not in _names(integrity.run_checks(conn))


# ---------------------------------------------------------------------------
# uncategorized_not_flagged
# ---------------------------------------------------------------------------


def test_uncategorized_row_without_the_flag_is_surfaced(accounts):
    conn, spot, _bank = accounts
    txn_repo.insert(conn, _row(spot, source_ref="silent", needs_review=False))

    report = integrity.run_checks(conn)

    assert "uncategorized_not_flagged" in _names(report)
    assert _finding(report, "uncategorized_not_flagged").severity is (
        integrity.Severity.WARNING
    )


def test_uncategorized_row_with_the_flag_is_fine(accounts):
    conn, spot, _bank = accounts
    txn_repo.insert(conn, _row(spot, source_ref="queued", needs_review=True))

    assert "uncategorized_not_flagged" not in _names(integrity.run_checks(conn))


def test_transfers_are_never_expected_to_carry_a_category(accounts):
    conn, spot, bank = accounts
    txn_repo.insert(
        conn,
        _row(
            spot,
            kind=TransactionKind.TRANSFER,
            transfer_id="t1",
            source_ref="l1",
            needs_review=False,
        ),
    )
    txn_repo.insert(
        conn,
        _row(
            bank,
            kind=TransactionKind.TRANSFER,
            amount=Decimal("10"),
            transfer_id="t1",
            source_ref="l2",
            needs_review=False,
        ),
    )

    assert "uncategorized_not_flagged" not in _names(integrity.run_checks(conn))


# ---------------------------------------------------------------------------
# transfer_usd_imbalance — the hole the manual pair-confirm walks through
# ---------------------------------------------------------------------------


def test_a_wildly_mismatched_cross_currency_pair_is_flagged(accounts):
    """2,261 Bs against 200 USDT: the exact shape confirm_pair accepted."""
    conn, spot, bank = accounts
    txn_repo.insert(
        conn,
        _row(
            spot,
            kind=TransactionKind.TRANSFER,
            amount=Decimal("-200.44"),
            transfer_id="t1",
            source_ref="sell",
            user_rate=Decimal("848.10"),
        ),
    )
    txn_repo.insert(
        conn,
        _row(
            bank,
            kind=TransactionKind.TRANSFER,
            amount=Decimal("2261"),
            currency="VES",
            transfer_id="t1",
            source_ref="dep",
            user_rate=Decimal("848.10"),
        ),
    )

    report = integrity.run_checks(conn)

    assert "transfer_usd_imbalance" in _names(report)


def test_a_sound_cross_currency_pair_is_silent(accounts):
    conn, spot, bank = accounts
    txn_repo.insert(
        conn,
        _row(
            spot,
            kind=TransactionKind.TRANSFER,
            amount=Decimal("-100"),
            transfer_id="t1",
            source_ref="sell",
            user_rate=Decimal("800"),
        ),
    )
    txn_repo.insert(
        conn,
        _row(
            bank,
            kind=TransactionKind.TRANSFER,
            amount=Decimal("80000"),
            currency="VES",
            transfer_id="t1",
            source_ref="dep",
            user_rate=Decimal("800"),
        ),
    )

    assert "transfer_usd_imbalance" not in _names(integrity.run_checks(conn))


def test_an_unpriceable_pair_is_not_reported_as_imbalanced(accounts):
    """No rate anywhere is a different problem, with a different remedy."""
    conn, spot, bank = accounts
    txn_repo.insert(
        conn,
        _row(
            spot,
            kind=TransactionKind.TRANSFER,
            amount=Decimal("-100"),
            transfer_id="t1",
            source_ref="sell",
        ),
    )
    txn_repo.insert(
        conn,
        _row(
            bank,
            kind=TransactionKind.TRANSFER,
            amount=Decimal("80000"),
            currency="VES",
            transfer_id="t1",
            source_ref="dep",
        ),
    )

    assert "transfer_usd_imbalance" not in _names(integrity.run_checks(conn))


# ---------------------------------------------------------------------------
# A clean ledger stays clean.
# ---------------------------------------------------------------------------


def test_clean_ledger_reports_nothing(accounts):
    conn, spot, _bank = accounts
    groceries = categories_repo.get_by_name(conn, TransactionKind.EXPENSE, "Groceries")
    txn_repo.insert(
        conn,
        _row(
            spot,
            kind=TransactionKind.INCOME,
            amount=Decimal("100"),
            source_ref="in",
            category_id=categories_repo.get_by_name(
                conn, TransactionKind.INCOME, "Salary"
            ).id,
        ),
    )
    txn_repo.insert(
        conn, _row(spot, amount=Decimal("-10"), source_ref="out", category_id=groceries.id)
    )

    report = integrity.run_checks(conn)

    assert report.findings == []
    assert report.ok
