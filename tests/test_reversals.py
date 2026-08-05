"""ADR-019 — bank reversals (RETORNOS) pair with their failed charge.

A rejected pago móvil leaves a failed expense, a ``REVERSO CARGO`` income
for the same amount (and the same dance for the commission), then a retry.
The reversal and the failed charge become a zero-sum pair sharing a
``transfer_id`` — same mechanism as rule-002 transfer legs — so neither
counts as spending or income. The retry stays the one real expense.

Covers the ``BankReversalPairing`` strategy (match/apply), its greedy
claim preferences, the ingest wiring, the ``finances reconcile
reversals`` CLI, and migration 019 (the ``REVERSO CARGO`` → Fees rule
retires — a reversal is not fee income).
"""

from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta
from decimal import Decimal

import pytest
from typer.testing import CliRunner

from finances.config import CARACAS_TZ
from finances.db.repos import accounts as accounts_repo
from finances.db.repos import categories as categories_repo
from finances.db.repos import transactions as txn_repo
from finances.domain.models import Transaction, TransactionKind
from finances.domain.reconciliation import run_reconciliation_pass


_DAY = datetime(2026, 7, 12, tzinfo=CARACAS_TZ)


def _provincial_id(conn: sqlite3.Connection) -> int:
    account = accounts_repo.get_by_name(conn, "Provincial Bolivares")
    assert account is not None and account.id is not None
    return account.id


def _insert(
    conn: sqlite3.Connection,
    *,
    account_id: int,
    kind: TransactionKind,
    amount: str,
    description: str,
    ref: str,
    when: datetime = _DAY,
    category_id: int | None = None,
) -> Transaction:
    return txn_repo.insert(
        conn,
        Transaction(
            account_id=account_id,
            occurred_at=when,
            kind=kind,
            amount=Decimal(amount),
            currency="VES",
            description=description,
            category_id=category_id,
            source="provincial",
            source_ref=ref,
        ),
    )


class TestBankReversalPairing:
    def test_pairs_reversal_with_failed_charge_and_commission(
        self, seeded_db: sqlite3.Connection
    ) -> None:
        from finances.domain.reversals import BankReversalPairing

        acct = _provincial_id(seeded_db)
        failed = _insert(seeded_db, account_id=acct, kind=TransactionKind.EXPENSE,
                         amount="-1250", description="DR OB V27209763 102BANCO", ref="rv-f")
        failed_com = _insert(seeded_db, account_id=acct, kind=TransactionKind.EXPENSE,
                             amount="-3.75", description="COM. PAGO MOVIL", ref="rv-fc")
        rev = _insert(seeded_db, account_id=acct, kind=TransactionKind.INCOME,
                      amount="1250", description="REVERSO CARGO", ref="rv-r")
        rev_com = _insert(seeded_db, account_id=acct, kind=TransactionKind.INCOME,
                          amount="3.75", description="REVERSO CARGO", ref="rv-rc")

        report = run_reconciliation_pass(BankReversalPairing(seeded_db))

        assert report.proposals_found == 2
        assert report.proposals_applied == 2
        assert report.errors == []

        for orig, reversal in ((failed, rev), (failed_com, rev_com)):
            a = txn_repo.get_by_id(seeded_db, orig.id)
            b = txn_repo.get_by_id(seeded_db, reversal.id)
            assert a.kind is TransactionKind.TRANSFER
            assert b.kind is TransactionKind.TRANSFER
            assert a.transfer_id is not None
            assert a.transfer_id == b.transfer_id
            assert a.amount + b.amount == 0
            assert a.needs_review is False and b.needs_review is False
            # The reversal leg sheds its rule-27 "Fees income" label.
            assert b.category_id is None

    def test_prefers_uncategorized_charge_over_hand_triaged_retry(
        self, seeded_db: sqlite3.Connection
    ) -> None:
        from finances.domain.reversals import BankReversalPairing

        acct = _provincial_id(seeded_db)
        food = categories_repo.get_by_name(
            seeded_db, TransactionKind.EXPENSE, "Groceries"
        )
        retry = _insert(seeded_db, account_id=acct, kind=TransactionKind.EXPENSE,
                        amount="-500", description="DR OB X", ref="rv-retry",
                        category_id=food.id)
        failed = _insert(seeded_db, account_id=acct, kind=TransactionKind.EXPENSE,
                         amount="-500", description="DR OB X", ref="rv-failed")
        _insert(seeded_db, account_id=acct, kind=TransactionKind.INCOME,
                amount="500", description="REVERSO CARGO", ref="rv-rev")

        run_reconciliation_pass(BankReversalPairing(seeded_db))

        assert txn_repo.get_by_id(seeded_db, failed.id).kind is TransactionKind.TRANSFER
        kept = txn_repo.get_by_id(seeded_db, retry.id)
        assert kept.kind is TransactionKind.EXPENSE
        assert kept.category_id == food.id

    def test_no_matching_charge_leaves_reversal_alone(
        self, seeded_db: sqlite3.Connection
    ) -> None:
        from finances.domain.reversals import BankReversalPairing

        acct = _provincial_id(seeded_db)
        rev = _insert(seeded_db, account_id=acct, kind=TransactionKind.INCOME,
                      amount="777", description="REVERSO CARGO", ref="rv-lone")
        # Wrong amount, right window.
        _insert(seeded_db, account_id=acct, kind=TransactionKind.EXPENSE,
                amount="-778", description="DR OB Y", ref="rv-near")

        report = run_reconciliation_pass(BankReversalPairing(seeded_db))

        assert report.proposals_found == 0
        after = txn_repo.get_by_id(seeded_db, rev.id)
        assert after.kind is TransactionKind.INCOME
        assert after.transfer_id is None

    def test_charge_outside_window_not_matched(
        self, seeded_db: sqlite3.Connection
    ) -> None:
        from finances.domain.reversals import BankReversalPairing

        acct = _provincial_id(seeded_db)
        _insert(seeded_db, account_id=acct, kind=TransactionKind.EXPENSE,
                amount="-900", description="DR OB Z", ref="rv-old",
                when=_DAY - timedelta(days=5))
        _insert(seeded_db, account_id=acct, kind=TransactionKind.INCOME,
                amount="900", description="REVERSO CARGO", ref="rv-late")

        report = run_reconciliation_pass(BankReversalPairing(seeded_db))
        assert report.proposals_found == 0

    def test_each_charge_claimed_at_most_once(
        self, seeded_db: sqlite3.Connection
    ) -> None:
        """Two same-amount reversals, one charge: only one pairs."""
        from finances.domain.reversals import BankReversalPairing

        acct = _provincial_id(seeded_db)
        _insert(seeded_db, account_id=acct, kind=TransactionKind.EXPENSE,
                amount="-100", description="DR OB W", ref="rv-once")
        _insert(seeded_db, account_id=acct, kind=TransactionKind.INCOME,
                amount="100", description="REVERSO CARGO", ref="rv-a")
        _insert(seeded_db, account_id=acct, kind=TransactionKind.INCOME,
                amount="100", description="REVERSO CARGO", ref="rv-b")

        report = run_reconciliation_pass(BankReversalPairing(seeded_db))
        assert report.proposals_found == 1
        assert report.proposals_applied == 1

    def test_second_pass_is_a_noop(self, seeded_db: sqlite3.Connection) -> None:
        from finances.domain.reversals import BankReversalPairing

        acct = _provincial_id(seeded_db)
        _insert(seeded_db, account_id=acct, kind=TransactionKind.EXPENSE,
                amount="-60", description="DR OB Q", ref="rv-i1")
        _insert(seeded_db, account_id=acct, kind=TransactionKind.INCOME,
                amount="60", description="REVERSO CARGO", ref="rv-i2")

        run_reconciliation_pass(BankReversalPairing(seeded_db))
        second = run_reconciliation_pass(BankReversalPairing(seeded_db))
        assert second.proposals_found == 0


class TestIngestWiring:
    def test_ingest_csv_pairs_reversals_automatically(
        self, tmp_path, seeded_db: sqlite3.Connection
    ) -> None:
        from finances.ingest.provincial import ingest_csv

        csv_path = tmp_path / "prov.csv"
        csv_path.write_text(
            "Fecha;Descripción;Monto;Saldo\n"
            "12/07/2026;COM. PAGO MOVIL;-3,75;100,00\n"
            "12/07/2026;DR OB V27209763 102BANCO;-1.250,00;103,75\n"
            "12/07/2026;REVERSO CARGO;3,75;1.353,75\n"
            "12/07/2026;REVERSO CARGO;1.250,00;1.350,00\n"
            "12/07/2026;COM. PAGO MOVIL;-3,75;100,00\n"
            "12/07/2026;DR OB V27209763 102BANCO;-1.250,00;103,75\n",
            encoding="utf-8",
        )

        report = ingest_csv(seeded_db, csv_path)

        assert report.reversals is not None
        assert report.reversals.proposals_applied == 2
        acct = _provincial_id(seeded_db)
        rows = txn_repo.list_by_account(seeded_db, acct)
        transfers = [t for t in rows if t.kind is TransactionKind.TRANSFER]
        expenses = [t for t in rows if t.kind is TransactionKind.EXPENSE]
        assert len(transfers) == 4  # two zero-sum pairs
        assert len(expenses) == 2  # the retry + its commission
        assert sum(t.amount for t in transfers) == 0


class TestReconcileReversalsCli:
    @pytest.fixture
    def cli_db(self, tmp_path, monkeypatch):
        from finances import config
        from finances.db.connection import get_connection
        from finances.db.migrate import apply_migrations
        from finances.db.repos import accounts as accounts_repo_

        db_file = tmp_path / "cli.db"
        conn = get_connection(db_file)
        apply_migrations(conn)
        from finances.domain.models import Account, AccountKind

        acct = accounts_repo_.insert(
            conn,
            Account(name="Provincial Bolivares", kind=AccountKind.BANK, currency="VES"),
        )
        _insert(conn, account_id=acct.id, kind=TransactionKind.EXPENSE,
                amount="-45", description="DR OB C", ref="rv-cli-1")
        _insert(conn, account_id=acct.id, kind=TransactionKind.INCOME,
                amount="45", description="REVERSO CARGO", ref="rv-cli-2")
        conn.commit()
        conn.close()
        monkeypatch.setattr(config, "DB_PATH", db_file)
        import finances.cli.main as cli_main

        monkeypatch.setattr(cli_main, "DB_PATH", db_file)
        return db_file

    def test_dry_run_rolls_back(self, cli_db) -> None:
        from finances.cli.main import app
        from finances.db.connection import get_connection

        result = CliRunner().invoke(app, ["reconcile", "reversals", "--dry-run"])
        assert result.exit_code == 0, result.output
        assert "1/1" in result.output

        conn = get_connection(cli_db)
        try:
            n = conn.execute(
                "SELECT COUNT(*) FROM transactions WHERE kind='transfer'"
            ).fetchone()[0]
        finally:
            conn.close()
        assert n == 0

    def test_real_run_pairs(self, cli_db) -> None:
        from finances.cli.main import app
        from finances.db.connection import get_connection

        result = CliRunner().invoke(app, ["reconcile", "reversals"])
        assert result.exit_code == 0, result.output

        conn = get_connection(cli_db)
        try:
            n = conn.execute(
                "SELECT COUNT(*) FROM transactions WHERE kind='transfer'"
            ).fetchone()[0]
        finally:
            conn.close()
        assert n == 2


def test_migration_019_retires_reverso_fees_rule(
    seeded_db: sqlite3.Connection,
) -> None:
    """A reversal is not fee income; the old rule must stop firing."""
    rows = seeded_db.execute(
        "SELECT active FROM category_rules WHERE pattern = 'REVERSO CARGO'"
    ).fetchall()
    assert rows, "REVERSO CARGO rule should still exist (deactivated, not deleted)"
    assert all(r["active"] == 0 for r in rows)
