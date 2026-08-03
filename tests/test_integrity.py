"""Ledger integrity checks (``finances doctor``).

The test suite verifies *code* against freshly seeded fixtures. Nothing
verified the *ledger*, so a defect that corrupts real rows stays invisible
for as long as nobody happens to look at the right screen. The paired-
transfer review flag lived in production data for three months that way,
across 1009 green tests.

These checks close that gap: the invariants CLAUDE.md states as prose,
expressed once, runnable against the real database.

Each test builds the violation deliberately with raw SQL — going through
``create_transfer`` would be prevented from producing it, which is the
whole point of that function.
"""

from __future__ import annotations

import sqlite3
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from finances.db.repos import accounts as acc_repo
from finances.db.repos import transactions as txn_repo
from finances.domain.integrity import (
    CHECKS,
    IntegrityReport,
    Severity,
    run_checks,
)
from finances.domain.models import Transaction, TransactionKind

FIXED_AT = datetime(2026, 7, 7, 12, 0, tzinfo=UTC)


def _account_id(conn: sqlite3.Connection, name: str) -> int:
    acct = acc_repo.get_by_name(conn, name)
    assert acct is not None and acct.id is not None
    return acct.id


def _row(
    conn: sqlite3.Connection,
    *,
    account_id: int,
    amount: Decimal,
    currency: str = "VES",
    kind: TransactionKind = TransactionKind.INCOME,
    source_ref: str,
) -> int:
    txn = txn_repo.insert(
        conn,
        Transaction(
            account_id=account_id,
            occurred_at=FIXED_AT,
            kind=kind,
            amount=amount,
            currency=currency,
            source="test",
            source_ref=source_ref,
        ),
    )
    assert txn.id is not None
    return txn.id


def _finding(report: IntegrityReport, check: str):
    return next((f for f in report.findings if f.check == check), None)


class TestCleanLedger:
    def test_seeded_ledger_reports_no_findings(
        self, seeded_db: sqlite3.Connection
    ):
        report = run_checks(seeded_db)
        assert report.findings == []
        assert report.ok is True

    def test_every_check_runs(self, seeded_db: sqlite3.Connection):
        report = run_checks(seeded_db)
        assert report.checks_run == len(CHECKS)

    def test_check_names_are_unique(self):
        names = [c.name for c in CHECKS]
        assert len(names) == len(set(names))


class TestTransferChecks:
    def test_detects_transfer_kind_without_transfer_id(
        self, seeded_db: sqlite3.Connection
    ):
        """rule-002: an orphan half-transfer."""
        spot = _account_id(seeded_db, "Binance Spot")
        txn_id = _row(
            seeded_db,
            account_id=spot,
            amount=Decimal("-10"),
            currency="USDT",
            kind=TransactionKind.TRANSFER,
            source_ref="orphan-half",
        )

        found = _finding(run_checks(seeded_db), "transfer_missing_transfer_id")
        assert found is not None
        assert found.severity is Severity.ERROR
        assert found.count == 1
        assert txn_id in found.sample_ids

    def test_detects_transfer_id_on_a_non_transfer_kind(
        self, seeded_db: sqlite3.Connection
    ):
        spot = _account_id(seeded_db, "Binance Spot")
        txn_id = _row(
            seeded_db,
            account_id=spot,
            amount=Decimal("-10"),
            currency="USDT",
            kind=TransactionKind.EXPENSE,
            source_ref="mislabelled-leg",
        )
        # deliberate malformed fixture
        seeded_db.execute(
            "UPDATE transactions SET transfer_id = 'x' WHERE id = ?", (txn_id,)
        )

        found = _finding(run_checks(seeded_db), "transfer_id_on_wrong_kind")
        assert found is not None
        assert found.severity is Severity.ERROR
        assert txn_id in found.sample_ids

    def test_detects_transfer_group_without_exactly_two_legs(
        self, seeded_db: sqlite3.Connection
    ):
        spot = _account_id(seeded_db, "Binance Spot")
        txn_id = _row(
            seeded_db,
            account_id=spot,
            amount=Decimal("-10"),
            currency="USDT",
            kind=TransactionKind.TRANSFER,
            source_ref="lonely-leg",
        )
        # deliberate malformed fixture: one leg carrying a transfer_id
        seeded_db.execute(
            "UPDATE transactions SET transfer_id = 'solo' WHERE id = ?", (txn_id,)
        )

        found = _finding(run_checks(seeded_db), "transfer_leg_count")
        assert found is not None
        assert found.severity is Severity.ERROR
        assert txn_id in found.sample_ids

    def test_detects_both_legs_on_the_same_account(
        self, seeded_db: sqlite3.Connection
    ):
        spot = _account_id(seeded_db, "Binance Spot")
        a = _row(
            seeded_db,
            account_id=spot,
            amount=Decimal("-10"),
            currency="USDT",
            kind=TransactionKind.TRANSFER,
            source_ref="same-acct-a",
        )
        b = _row(
            seeded_db,
            account_id=spot,
            amount=Decimal("10"),
            currency="USDT",
            kind=TransactionKind.TRANSFER,
            source_ref="same-acct-b",
        )
        # deliberate malformed fixture
        seeded_db.execute(
            "UPDATE transactions SET transfer_id = 'sameacct' WHERE id IN (?, ?)",
            (a, b),
        )

        found = _finding(run_checks(seeded_db), "transfer_legs_same_account")
        assert found is not None
        assert found.severity is Severity.ERROR

    def test_detects_same_currency_legs_that_do_not_net_to_zero(
        self, seeded_db: sqlite3.Connection
    ):
        spot = _account_id(seeded_db, "Binance Spot")
        funding = _account_id(seeded_db, "Binance Funding")
        a = _row(
            seeded_db,
            account_id=spot,
            amount=Decimal("-10"),
            currency="USDT",
            kind=TransactionKind.TRANSFER,
            source_ref="lopsided-a",
        )
        b = _row(
            seeded_db,
            account_id=funding,
            amount=Decimal("25"),
            currency="USDT",
            kind=TransactionKind.TRANSFER,
            source_ref="lopsided-b",
        )
        # deliberate malformed fixture
        seeded_db.execute(
            "UPDATE transactions SET transfer_id = 'lopsided' WHERE id IN (?, ?)",
            (a, b),
        )

        found = _finding(run_checks(seeded_db), "transfer_same_currency_imbalance")
        assert found is not None
        assert found.severity is Severity.ERROR

    def test_same_currency_legs_within_a_cent_are_accepted(
        self, seeded_db: sqlite3.Connection
    ):
        """Rounding noise is not corruption."""
        spot = _account_id(seeded_db, "Binance Spot")
        funding = _account_id(seeded_db, "Binance Funding")
        a = _row(
            seeded_db,
            account_id=spot,
            amount=Decimal("-10.00"),
            currency="USDT",
            kind=TransactionKind.TRANSFER,
            source_ref="rounding-a",
        )
        b = _row(
            seeded_db,
            account_id=funding,
            amount=Decimal("10.004"),
            currency="USDT",
            kind=TransactionKind.TRANSFER,
            source_ref="rounding-b",
        )
        seeded_db.execute(
            "UPDATE transactions SET transfer_id = 'rounding' WHERE id IN (?, ?)",
            (a, b),
        )

        assert _finding(run_checks(seeded_db), "transfer_same_currency_imbalance") is None

    def test_detects_paired_transfer_still_flagged_for_review(
        self, seeded_db: sqlite3.Connection
    ):
        """The defect that motivated this module."""
        spot = _account_id(seeded_db, "Binance Spot")
        funding = _account_id(seeded_db, "Binance Funding")
        a = _row(
            seeded_db,
            account_id=spot,
            amount=Decimal("-10"),
            currency="USDT",
            kind=TransactionKind.TRANSFER,
            source_ref="flagged-a",
        )
        b = _row(
            seeded_db,
            account_id=funding,
            amount=Decimal("10"),
            currency="USDT",
            kind=TransactionKind.TRANSFER,
            source_ref="flagged-b",
        )
        # deliberate malformed fixture: the pre-migration-016 state
        seeded_db.execute(
            "UPDATE transactions SET transfer_id = 'flagged', needs_review = 1 "
            "WHERE id IN (?, ?)",
            (a, b),
        )

        found = _finding(run_checks(seeded_db), "paired_transfer_needs_review")
        assert found is not None
        assert found.severity is Severity.ERROR
        assert found.count == 2


class TestSourceRefChecks:
    def test_detects_missing_source_ref(self, seeded_db: sqlite3.Connection):
        spot = _account_id(seeded_db, "Binance Spot")
        txn_id = _row(
            seeded_db,
            account_id=spot,
            amount=Decimal("-10"),
            currency="USDT",
            kind=TransactionKind.EXPENSE,
            source_ref="will-be-nulled",
        )
        # deliberate malformed fixture: source_ref is nullable in schema but
        # CLAUDE.md holds it as always-set (rule-010).
        seeded_db.execute(
            "UPDATE transactions SET source_ref = NULL WHERE id = ?", (txn_id,)
        )

        found = _finding(run_checks(seeded_db), "missing_source_ref")
        assert found is not None
        assert found.severity is Severity.ERROR
        assert txn_id in found.sample_ids


class TestBacklogChecks:
    def test_reports_unpaired_p2p_sells_as_a_warning_not_an_error(
        self, seeded_db: sqlite3.Connection
    ):
        """A backlog is work to do, not corruption — it must not fail a run."""
        spot = _account_id(seeded_db, "Binance Spot")
        # Fund the account first. A lone -30 sell would leave the position
        # negative, which negative_asset_balance now (correctly) calls an
        # error — and this test is about the backlog check's severity, so it
        # must not lean on an impossible balance to make its point.
        _row(
            seeded_db,
            account_id=spot,
            amount=Decimal("100"),
            currency="USDT",
            kind=TransactionKind.INCOME,
            source_ref="funding-for-backlog",
        )
        _row(
            seeded_db,
            account_id=spot,
            amount=Decimal("-30"),
            currency="USDT",
            kind=TransactionKind.EXPENSE,
            source_ref="p2p:unpaired-backlog",
        )

        report = run_checks(seeded_db)
        found = _finding(report, "unpaired_p2p_sells")
        assert found is not None
        assert found.severity is Severity.WARNING
        # Warnings alone keep the ledger "ok" — only errors break it.
        assert report.ok is True
        assert report.has_errors is False


class TestConvertChecks:
    def test_detects_a_convert_leg_with_no_counterpart(
        self, seeded_db: sqlite3.Connection
    ):
        """A half-recorded conversion is phantom expense.

        A Binance convert writes two legs sharing one order id -- USDC out,
        USDT in. When only the outgoing leg exists the money looks spent,
        because reports count every non-transfer row. Legacy backfill
        produced exactly this: each leg hashed to its own source_ref, so
        the halves never referenced a common id.
        """
        spot = _account_id(seeded_db, "Binance Spot")
        lonely = _row(
            seeded_db,
            account_id=spot,
            amount=Decimal("-1350"),
            currency="USDC",
            kind=TransactionKind.EXPENSE,
            source_ref="convert:hash:deadbeefcafe0001:from",
        )

        found = _finding(run_checks(seeded_db), "convert_leg_without_counterpart")
        assert found is not None
        assert found.severity is Severity.WARNING
        assert lonely in found.sample_ids

    def test_a_complete_convert_pair_is_not_reported(
        self, seeded_db: sqlite3.Connection
    ):
        spot = _account_id(seeded_db, "Binance Spot")
        _row(
            seeded_db,
            account_id=spot,
            amount=Decimal("-540.520524"),
            currency="USDC",
            kind=TransactionKind.EXPENSE,
            source_ref="convert:2257663689585206258:from",
        )
        _row(
            seeded_db,
            account_id=spot,
            amount=Decimal("540.41715053"),
            currency="USDT",
            kind=TransactionKind.INCOME,
            source_ref="convert:2257663689585206258:to",
        )

        assert _finding(run_checks(seeded_db), "convert_leg_without_counterpart") is None


class TestReportShape:
    def test_errors_make_the_report_not_ok(self, seeded_db: sqlite3.Connection):
        spot = _account_id(seeded_db, "Binance Spot")
        _row(
            seeded_db,
            account_id=spot,
            amount=Decimal("-10"),
            currency="USDT",
            kind=TransactionKind.TRANSFER,
            source_ref="breaks-it",
        )

        report = run_checks(seeded_db)
        assert report.has_errors is True
        assert report.ok is False

    def test_sample_ids_are_capped(self, seeded_db: sqlite3.Connection):
        """A wide breakage must not print a thousand ids."""
        spot = _account_id(seeded_db, "Binance Spot")
        for n in range(15):
            _row(
                seeded_db,
                account_id=spot,
                amount=Decimal("-1"),
                currency="USDT",
                kind=TransactionKind.TRANSFER,
                source_ref=f"many-{n}",
            )

        found = _finding(run_checks(seeded_db), "transfer_missing_transfer_id")
        assert found is not None
        assert found.count == 15
        assert len(found.sample_ids) <= 10

    def test_findings_are_ordered_errors_first(
        self, seeded_db: sqlite3.Connection
    ):
        spot = _account_id(seeded_db, "Binance Spot")
        _row(
            seeded_db,
            account_id=spot,
            amount=Decimal("-30"),
            currency="USDT",
            kind=TransactionKind.EXPENSE,
            source_ref="p2p:backlog-order",
        )
        _row(
            seeded_db,
            account_id=spot,
            amount=Decimal("-10"),
            currency="USDT",
            kind=TransactionKind.TRANSFER,
            source_ref="error-order",
        )

        severities = [f.severity for f in run_checks(seeded_db).findings]
        assert severities == sorted(
            severities, key=lambda s: 0 if s is Severity.ERROR else 1
        )
