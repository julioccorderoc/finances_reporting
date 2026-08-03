"""`finances reconcile converts` — the one-shot repair for existing rows.

The ingest post-pass fixes conversions from here on. This command exists so
the rows already in the ledger can be paired without an API round-trip, and
so the pass can be re-run after a future legacy import.
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

FIXED_AT = datetime(2025, 11, 23, 4, 0, tzinfo=UTC)


@pytest.fixture
def ledger_with_unpaired_conversion(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> Path:
    """A real on-disk ledger holding one unpaired conversion."""
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
    for kind, amount, currency, ref in (
        (TransactionKind.EXPENSE, Decimal("-1240.00"), "USDC", "convert:o-1:from"),
        (TransactionKind.INCOME, Decimal("1239.18"), "USDT", "convert:o-1:to"),
    ):
        txn_repo.insert(
            conn,
            Transaction(
                account_id=account.id,
                occurred_at=FIXED_AT,
                kind=kind,
                amount=amount,
                currency=currency,
                source="binance",
                source_ref=ref,
            ),
        )
    conn.commit()
    conn.close()

    monkeypatch.setattr("finances.cli.main.DB_PATH", db_path)
    return db_path


def _convert_kinds(db_path: Path) -> set[str]:
    conn = sqlite3.connect(db_path)
    try:
        return {
            row[0]
            for row in conn.execute(
                "SELECT kind FROM transactions WHERE source_ref LIKE 'convert:%'"
            )
        }
    finally:
        conn.close()


def test_reconcile_converts_pairs_the_legs(
    ledger_with_unpaired_conversion: Path,
) -> None:
    result = CliRunner().invoke(app, ["reconcile", "converts"])

    assert result.exit_code == 0, result.output
    assert "1/1" in result.output
    assert _convert_kinds(ledger_with_unpaired_conversion) == {"transfer"}


def test_reconcile_converts_dry_run_writes_nothing(
    ledger_with_unpaired_conversion: Path,
) -> None:
    result = CliRunner().invoke(app, ["reconcile", "converts", "--dry-run"])

    assert result.exit_code == 0, result.output
    assert "dry run" in result.output
    assert _convert_kinds(ledger_with_unpaired_conversion) == {"expense", "income"}
