"""`finances reconcile reverse-adjustment` — the CLI door for ADR-028 §2.

The domain function is the approval; this is the invocation. It exists
because the plugs being reversed are found in `finances doctor`'s output,
and the smallest honest command is one that names the id doctor printed.
"""

from __future__ import annotations

import sqlite3
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import pytest
from typer.testing import CliRunner

from finances.cli.main import app
from finances.db.repos import accounts as acc_repo
from finances.db.repos import transactions as txn_repo
from finances.domain.models import Account, AccountKind, Transaction, TransactionKind

WHEN = datetime(2026, 8, 8, 11, 41, tzinfo=UTC)


@pytest.fixture
def ledger_with_a_plug(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """A real on-disk ledger carrying one reconciliation plug."""
    from finances.db.connection import get_connection
    from finances.db.migrate import apply_migrations

    db_path = tmp_path / "finances.db"
    conn = get_connection(db_path)
    apply_migrations(conn)

    account = acc_repo.insert(
        conn,
        Account(
            name="Binance Spot",
            kind=AccountKind.CRYPTO_SPOT,
            currency="USDT",
            institution="Binance",
        ),
    )
    assert account.id is not None
    inserted = txn_repo.insert(
        conn,
        Transaction(
            account_id=account.id,
            occurred_at=WHEN,
            kind=TransactionKind.ADJUSTMENT,
            amount=Decimal("-3.92884318"),
            currency="USDT",
            description="Reconciliation to custodian balance",
            source="reconciliation",
            source_ref="reconcile:1:USDT:cfbd3cbe-d221-43f1-8725-bc221de88514",
        ),
    )
    assert inserted.id is not None
    (tmp_path / "plug_id").write_text(str(inserted.id))
    conn.commit()
    conn.close()

    monkeypatch.setattr("finances.cli.main.DB_PATH", db_path)
    return db_path


def _plug_id(db_path: Path) -> int:
    return int((db_path.parent / "plug_id").read_text())


def _counts(db_path: Path) -> tuple[int, int]:
    conn = sqlite3.connect(db_path)
    try:
        live = conn.execute(
            "SELECT COUNT(*) FROM transactions WHERE source = 'reconciliation'"
        ).fetchone()[0]
        tombstones = conn.execute(
            "SELECT COUNT(*) FROM deleted_transactions"
        ).fetchone()[0]
        return live, tombstones
    finally:
        conn.close()


def test_cli_reverses_the_plug(ledger_with_a_plug: Path) -> None:
    result = CliRunner().invoke(
        app,
        [
            "reconcile",
            "reverse-adjustment",
            "--id",
            str(_plug_id(ledger_with_a_plug)),
            "--reason",
            "ADR-028: the BONUS misplacement",
        ],
    )

    assert result.exit_code == 0, result.output
    assert "reversed" in result.output
    assert _counts(ledger_with_a_plug) == (0, 1)


def test_cli_dry_run_writes_nothing(ledger_with_a_plug: Path) -> None:
    result = CliRunner().invoke(
        app,
        [
            "reconcile",
            "reverse-adjustment",
            "--id",
            str(_plug_id(ledger_with_a_plug)),
            "--reason",
            "checking",
            "--dry-run",
        ],
    )

    assert result.exit_code == 0, result.output
    assert "dry run" in result.output
    assert _counts(ledger_with_a_plug) == (1, 0)


def test_cli_refuses_an_unknown_id(ledger_with_a_plug: Path) -> None:
    result = CliRunner().invoke(
        app,
        ["reconcile", "reverse-adjustment", "--id", "9999", "--reason", "x"],
    )

    assert result.exit_code == 2
    assert "9999" in result.output
    assert _counts(ledger_with_a_plug) == (1, 0)


def test_cli_refuses_an_opening_position(ledger_with_a_plug: Path) -> None:
    """It is restated, never reversed — the refusal names the command."""
    from finances.db.connection import get_connection
    from finances.db.migrate import apply_migrations

    conn = get_connection(ledger_with_a_plug)
    apply_migrations(conn)
    account_id = conn.execute("SELECT id FROM accounts LIMIT 1").fetchone()[0]
    inserted = txn_repo.insert(
        conn,
        Transaction(
            account_id=account_id,
            occurred_at=WHEN,
            kind=TransactionKind.ADJUSTMENT,
            amount=Decimal("50"),
            currency="USDT",
            description="Opening balance USDT",
            source="opening_balance",
            source_ref="opening:1:USDT",
        ),
    )
    assert inserted.id is not None
    row_id = inserted.id
    conn.commit()
    conn.close()

    result = CliRunner().invoke(
        app,
        ["reconcile", "reverse-adjustment", "--id", str(row_id), "--reason", "x"],
    )

    assert result.exit_code == 2
    assert "opening" in result.output
    assert _counts(ledger_with_a_plug) == (1, 0)
