"""Tests for EPIC-011 — Cash CLI Tool.

Covers the module ``finances/ingest/cash_cli.py`` and the ``finances cash add``
Typer subcommand wired into ``finances/cli/main.py``.

Per rule-011 every public function has ≥1 happy-path and ≥1 failure-mode test,
and test commits precede implementation commits in branch history.
"""

from __future__ import annotations

import re
import sqlite3
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import pytest
from typer.testing import CliRunner

import finances.config as config
from finances.db.connection import get_connection
from finances.db.migrate import apply_migrations
from finances.db.repos import accounts as accounts_repo
from finances.db.repos import categories as categories_repo
from finances.db.repos import transactions as transactions_repo
from finances.domain.models import (
    Account,
    AccountKind,
    Transaction,
    TransactionKind,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


_UUID4_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"
)


# ---------------------------------------------------------------------------
# ensure_cash_usd_account
# ---------------------------------------------------------------------------


def test_ensure_cash_usd_account_creates_when_missing(
    in_memory_db: sqlite3.Connection,
) -> None:
    from finances.ingest.cash_cli import (
        CASH_USD_ACCOUNT_NAME,
        ensure_cash_usd_account,
    )

    account = ensure_cash_usd_account(in_memory_db)
    assert account.id is not None
    assert account.name == CASH_USD_ACCOUNT_NAME
    assert account.kind == AccountKind.CASH
    assert account.currency == "USD"
    fetched = accounts_repo.get_by_name(in_memory_db, CASH_USD_ACCOUNT_NAME)
    assert fetched is not None
    assert fetched.id == account.id


def test_ensure_cash_usd_account_returns_existing_without_duplicate(
    in_memory_db: sqlite3.Connection,
) -> None:
    from finances.ingest.cash_cli import (
        CASH_USD_ACCOUNT_NAME,
        ensure_cash_usd_account,
    )

    first = ensure_cash_usd_account(in_memory_db)
    second = ensure_cash_usd_account(in_memory_db)
    assert first.id == second.id
    matches = [
        a
        for a in accounts_repo.list_all(in_memory_db, include_inactive=True)
        if a.name == CASH_USD_ACCOUNT_NAME
    ]
    assert len(matches) == 1


def test_ensure_cash_usd_account_rejects_wrong_currency(
    in_memory_db: sqlite3.Connection,
) -> None:
    from finances.ingest.cash_cli import (
        CASH_USD_ACCOUNT_NAME,
        ensure_cash_usd_account,
    )

    accounts_repo.insert(
        in_memory_db,
        Account(name=CASH_USD_ACCOUNT_NAME, kind=AccountKind.CASH, currency="VES"),
    )
    with pytest.raises(ValueError, match="Cash USD"):
        ensure_cash_usd_account(in_memory_db)


# ---------------------------------------------------------------------------
# suggest_recent_categories
# ---------------------------------------------------------------------------


def _insert_cash_expense(
    conn: sqlite3.Connection,
    *,
    account_id: int,
    category_id: int,
    occurred_at: datetime,
    amount: str,
    source_ref: str,
) -> None:
    transactions_repo.upsert_by_source_ref(
        conn,
        Transaction(
            account_id=account_id,
            occurred_at=occurred_at,
            kind=TransactionKind.EXPENSE,
            amount=Decimal(amount),
            currency="USD",
            description="seed",
            category_id=category_id,
            source="cash_cli",
            source_ref=source_ref,
        ),
    )


def _require_expense_category(
    conn: sqlite3.Connection, name: str
) -> int:
    from finances.domain.models import Category

    found = categories_repo.get_by_name(conn, TransactionKind.EXPENSE, name)
    if found is not None and found.id is not None:
        return found.id
    inserted = categories_repo.insert(
        conn, Category(kind=TransactionKind.EXPENSE, name=name)
    )
    assert inserted.id is not None
    return inserted.id


def test_suggest_recent_categories_orders_by_most_recent_usage(
    in_memory_db: sqlite3.Connection,
) -> None:
    from finances.ingest.cash_cli import (
        ensure_cash_usd_account,
        suggest_recent_categories,
    )

    cash = ensure_cash_usd_account(in_memory_db)
    assert cash.id is not None
    food_id = _require_expense_category(in_memory_db, "Food")
    transport_id = _require_expense_category(in_memory_db, "Transport")
    fees_id = _require_expense_category(in_memory_db, "Fees")

    _insert_cash_expense(
        in_memory_db,
        account_id=cash.id,
        category_id=fees_id,
        occurred_at=datetime(2026, 1, 1, tzinfo=UTC),
        amount="-1",
        source_ref="ref-fees",
    )
    _insert_cash_expense(
        in_memory_db,
        account_id=cash.id,
        category_id=transport_id,
        occurred_at=datetime(2026, 2, 1, tzinfo=UTC),
        amount="-2",
        source_ref="ref-transport",
    )
    _insert_cash_expense(
        in_memory_db,
        account_id=cash.id,
        category_id=food_id,
        occurred_at=datetime(2026, 3, 1, tzinfo=UTC),
        amount="-3",
        source_ref="ref-food",
    )

    suggestions = suggest_recent_categories(in_memory_db, cash.id, limit=2)
    assert [c.name for c in suggestions] == ["Food", "Transport"]


def test_suggest_recent_categories_returns_empty_when_no_usage(
    in_memory_db: sqlite3.Connection,
) -> None:
    from finances.ingest.cash_cli import (
        ensure_cash_usd_account,
        suggest_recent_categories,
    )

    cash = ensure_cash_usd_account(in_memory_db)
    assert cash.id is not None
    assert suggest_recent_categories(in_memory_db, cash.id) == []


# ---------------------------------------------------------------------------
# add_cash_expense
# ---------------------------------------------------------------------------


def test_add_cash_expense_writes_negative_transaction_with_uuid_source_ref(
    in_memory_db: sqlite3.Connection,
) -> None:
    from finances.ingest.cash_cli import (
        CASH_CLI_SOURCE,
        CASH_USD_ACCOUNT_NAME,
        add_cash_expense,
    )

    occurred_at = datetime(2026, 4, 15, 12, 0, tzinfo=UTC)
    txn = add_cash_expense(
        in_memory_db,
        amount=Decimal("12"),
        description="lunch",
        occurred_at=occurred_at,
    )
    assert txn.id is not None
    assert txn.kind == TransactionKind.EXPENSE
    assert txn.currency == "USD"
    assert txn.amount == Decimal("-12")
    assert txn.description == "lunch"
    assert txn.source == CASH_CLI_SOURCE
    assert txn.source_ref is not None
    assert _UUID4_RE.match(txn.source_ref), txn.source_ref

    account = accounts_repo.get_by_id(in_memory_db, txn.account_id)
    assert account is not None
    assert account.name == CASH_USD_ACCOUNT_NAME


def test_add_cash_expense_records_category_when_provided(
    in_memory_db: sqlite3.Connection,
) -> None:
    from finances.ingest.cash_cli import add_cash_expense

    food_id = _require_expense_category(in_memory_db, "Food")
    txn = add_cash_expense(
        in_memory_db,
        amount=Decimal("8.50"),
        description="arepas",
        occurred_at=datetime(2026, 4, 15, tzinfo=UTC),
        category_id=food_id,
    )
    assert txn.category_id == food_id
    assert txn.amount == Decimal("-8.50")


def test_add_cash_expense_rejects_zero_amount(
    in_memory_db: sqlite3.Connection,
) -> None:
    from finances.ingest.cash_cli import add_cash_expense

    with pytest.raises(ValueError, match="positive"):
        add_cash_expense(
            in_memory_db,
            amount=Decimal("0"),
            description="nope",
            occurred_at=datetime(2026, 4, 15, tzinfo=UTC),
        )


def test_add_cash_expense_rejects_negative_amount(
    in_memory_db: sqlite3.Connection,
) -> None:
    from finances.ingest.cash_cli import add_cash_expense

    with pytest.raises(ValueError, match="positive"):
        add_cash_expense(
            in_memory_db,
            amount=Decimal("-5"),
            description="nope",
            occurred_at=datetime(2026, 4, 15, tzinfo=UTC),
        )


# ---------------------------------------------------------------------------
# CLI: finances cash add
# ---------------------------------------------------------------------------


@pytest.fixture
def cli_db(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Path:
    db_file = tmp_path / "cli-finances.db"
    conn = get_connection(db_file)
    apply_migrations(conn)
    conn.close()
    monkeypatch.setattr(config, "DB_PATH", db_file)
    return db_file


def test_cli_cash_add_creates_row_visible_in_balance_and_usd_views(
    cli_db: Path,
) -> None:
    from finances.cli.main import app

    runner = CliRunner()
    result = runner.invoke(
        app, ["cash", "add", "--amount", "12", "--description", "lunch"]
    )
    assert result.exit_code == 0, result.output

    conn = get_connection(cli_db)
    try:
        balance_row = conn.execute(
            "SELECT account_name, currency, balance_native "
            "FROM v_account_balances WHERE account_name = 'Cash USD'"
        ).fetchone()
        assert balance_row is not None
        assert balance_row["currency"] == "USD"
        assert float(balance_row["balance_native"]) == pytest.approx(-12.0)

        usd_row = conn.execute(
            "SELECT amount, amount_usd, currency, source FROM v_transactions_usd "
            "WHERE source = 'cash_cli' ORDER BY transaction_id DESC LIMIT 1"
        ).fetchone()
        assert usd_row is not None
        assert usd_row["currency"] == "USD"
        assert usd_row["source"] == "cash_cli"
        assert float(usd_row["amount_usd"]) == pytest.approx(-12.0)
    finally:
        conn.close()


def test_cli_cash_add_uses_explicit_date_flag(cli_db: Path) -> None:
    from finances.cli.main import app

    runner = CliRunner()
    result = runner.invoke(
        app,
        [
            "cash",
            "add",
            "--amount",
            "5",
            "--description",
            "coffee",
            "--date",
            "2026-01-15",
        ],
    )
    assert result.exit_code == 0, result.output

    conn = get_connection(cli_db)
    try:
        row = conn.execute(
            "SELECT occurred_at FROM transactions "
            "WHERE source = 'cash_cli' ORDER BY id DESC LIMIT 1"
        ).fetchone()
        assert row is not None
        occurred_at = row["occurred_at"]
        rendered = (
            occurred_at.isoformat()
            if hasattr(occurred_at, "isoformat")
            else str(occurred_at)
        )
        assert "2026-01-15" in rendered
    finally:
        conn.close()


def test_cli_cash_add_rejects_zero_amount(cli_db: Path) -> None:
    from finances.cli.main import app

    runner = CliRunner()
    result = runner.invoke(
        app, ["cash", "add", "--amount", "0", "--description", "nope"]
    )
    assert result.exit_code != 0
    assert "positive" in result.output.lower()

    conn = get_connection(cli_db)
    try:
        count = conn.execute("SELECT COUNT(*) AS c FROM transactions").fetchone()["c"]
        assert count == 0
    finally:
        conn.close()


def test_cli_cash_add_rejects_non_cash_usd_account_flag(cli_db: Path) -> None:
    from finances.cli.main import app

    runner = CliRunner()
    result = runner.invoke(
        app,
        [
            "cash",
            "add",
            "--amount",
            "1",
            "--description",
            "x",
            "--account",
            "Provincial Bolivares",
        ],
    )
    assert result.exit_code != 0
    assert "Cash USD" in result.output

    conn = get_connection(cli_db)
    try:
        count = conn.execute("SELECT COUNT(*) AS c FROM transactions").fetchone()["c"]
        assert count == 0
    finally:
        conn.close()
