"""Tests for EPIC-013 — ``finances/reports/balances.py``.

Per rule-011: every public function gets ≥1 happy-path AND ≥1 failure-mode test,
and these tests are committed **before** the implementation.
"""

from __future__ import annotations

import csv
import io
import json
import sqlite3
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from finances.db.repos import accounts as accounts_repo
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


def _insert_account(
    conn: sqlite3.Connection,
    name: str,
    *,
    kind: AccountKind = AccountKind.BANK,
    currency: str = "USD",
) -> Account:
    return accounts_repo.insert(
        conn,
        Account(name=name, kind=kind, currency=currency, institution=None),
    )


def _insert_txn(
    conn: sqlite3.Connection,
    account_id: int,
    amount: Decimal,
    *,
    currency: str = "USD",
    kind: TransactionKind = TransactionKind.EXPENSE,
    source_ref: str,
    description: str | None = None,
) -> Transaction:
    txn = Transaction(
        account_id=account_id,
        occurred_at=datetime(2026, 1, 15, 12, 0, tzinfo=UTC),
        kind=kind,
        amount=amount,
        currency=currency,
        description=description,
        source="test",
        source_ref=source_ref,
    )
    return transactions_repo.insert(conn, txn)


# ---------------------------------------------------------------------------
# get_balances
# ---------------------------------------------------------------------------


def test_get_balances_happy_path_three_accounts(
    in_memory_db: sqlite3.Connection,
) -> None:
    from finances.reports.balances import AccountBalance, get_balances

    a1 = _insert_account(in_memory_db, "Alpha Bank", currency="USD")
    a2 = _insert_account(in_memory_db, "Bravo Bolivares", currency="VES")
    a3 = _insert_account(in_memory_db, "Charlie Spot", currency="USDT")

    assert a1.id is not None
    assert a2.id is not None
    assert a3.id is not None

    _insert_txn(
        in_memory_db, a1.id, Decimal("100.50"), source_ref="a1-1",
    )
    _insert_txn(
        in_memory_db, a1.id, Decimal("-25.25"), source_ref="a1-2",
    )
    _insert_txn(
        in_memory_db, a2.id, Decimal("1000.00"), currency="VES", source_ref="a2-1",
    )
    _insert_txn(
        in_memory_db, a3.id, Decimal("42.00"), currency="USDT", source_ref="a3-1",
    )

    balances = get_balances(in_memory_db)

    assert all(isinstance(b, AccountBalance) for b in balances)
    # Ordered by account name.
    assert [b.account_name for b in balances] == [
        "Alpha Bank",
        "Bravo Bolivares",
        "Charlie Spot",
    ]

    by_name = {b.account_name: b for b in balances}
    assert by_name["Alpha Bank"].balance_native == Decimal("75.25")
    assert by_name["Alpha Bank"].currency == "USD"
    assert by_name["Bravo Bolivares"].balance_native == Decimal("1000.00")
    assert by_name["Bravo Bolivares"].currency == "VES"
    assert by_name["Charlie Spot"].balance_native == Decimal("42.00")
    assert by_name["Charlie Spot"].currency == "USDT"


def test_get_balances_empty_db_returns_empty_list(
    in_memory_db: sqlite3.Connection,
) -> None:
    from finances.reports.balances import get_balances

    assert get_balances(in_memory_db) == []


def test_get_balances_account_with_no_transactions_is_zero(
    in_memory_db: sqlite3.Connection,
) -> None:
    from finances.reports.balances import get_balances

    _insert_account(in_memory_db, "Idle Account")

    balances = get_balances(in_memory_db)
    assert len(balances) == 1
    assert balances[0].balance_native == Decimal("0")


def test_get_balances_mixed_sign_transactions_sum_correctly(
    in_memory_db: sqlite3.Connection,
) -> None:
    from finances.reports.balances import get_balances

    acc = _insert_account(in_memory_db, "Mixed Bag", currency="USD")
    assert acc.id is not None

    _insert_txn(
        in_memory_db,
        acc.id,
        Decimal("500.00"),
        kind=TransactionKind.INCOME,
        source_ref="mix-1",
    )
    _insert_txn(
        in_memory_db,
        acc.id,
        Decimal("-120.00"),
        kind=TransactionKind.EXPENSE,
        source_ref="mix-2",
    )
    _insert_txn(
        in_memory_db,
        acc.id,
        Decimal("-30.50"),
        kind=TransactionKind.EXPENSE,
        source_ref="mix-3",
    )

    balances = get_balances(in_memory_db)
    assert len(balances) == 1
    assert balances[0].balance_native == Decimal("349.50")


# ---------------------------------------------------------------------------
# AccountBalance model validation (failure-mode)
# ---------------------------------------------------------------------------


def test_account_balance_rejects_float_amount() -> None:
    from pydantic import ValidationError

    from finances.reports.balances import AccountBalance

    with pytest.raises(ValidationError):
        AccountBalance(
            account_id=1,
            account_name="Alpha",
            currency="USD",
            balance_native=1.23,  # type: ignore[arg-type]
        )


def test_account_balance_extra_field_forbidden() -> None:
    from pydantic import ValidationError

    from finances.reports.balances import AccountBalance

    with pytest.raises(ValidationError):
        AccountBalance(
            account_id=1,
            account_name="Alpha",
            currency="USD",
            balance_native=Decimal("1.00"),
            sneaky="nope",  # type: ignore[call-arg]
        )


# ---------------------------------------------------------------------------
# render_json
# ---------------------------------------------------------------------------


def test_render_json_round_trips_with_decimal_as_string(
    in_memory_db: sqlite3.Connection,
) -> None:
    from finances.reports.balances import get_balances, render_json

    acc = _insert_account(in_memory_db, "JSON Bank", currency="USD")
    assert acc.id is not None
    _insert_txn(in_memory_db, acc.id, Decimal("12.34"), source_ref="j-1")

    balances = get_balances(in_memory_db)
    payload = render_json(balances)

    parsed = json.loads(payload)
    assert isinstance(parsed, list)
    assert len(parsed) == 1
    row = parsed[0]
    assert row["account_name"] == "JSON Bank"
    assert row["currency"] == "USD"
    # Decimals must be serialised as strings, never floats.
    assert isinstance(row["balance_native"], str)
    assert row["balance_native"] == "12.34"


def test_render_json_empty_list_returns_empty_json_array() -> None:
    from finances.reports.balances import render_json

    assert json.loads(render_json([])) == []


# ---------------------------------------------------------------------------
# render_csv
# ---------------------------------------------------------------------------


def test_render_csv_happy_path_header_and_row(
    in_memory_db: sqlite3.Connection,
) -> None:
    from finances.reports.balances import get_balances, render_csv

    acc = _insert_account(in_memory_db, "CSV Bank", currency="USD")
    assert acc.id is not None
    _insert_txn(in_memory_db, acc.id, Decimal("10.00"), source_ref="c-1")

    balances = get_balances(in_memory_db)
    out = render_csv(balances)

    reader = csv.reader(io.StringIO(out))
    rows = list(reader)
    assert rows[0] == ["account_id", "account_name", "currency", "balance_native"]
    assert len(rows) == 2
    assert rows[1][1] == "CSV Bank"
    assert rows[1][2] == "USD"
    assert rows[1][3] == "10.00"


def test_render_csv_empty_list_returns_header_only() -> None:
    from finances.reports.balances import render_csv

    out = render_csv([])
    reader = csv.reader(io.StringIO(out))
    rows = list(reader)
    assert rows == [["account_id", "account_name", "currency", "balance_native"]]


# ---------------------------------------------------------------------------
# render_table
# ---------------------------------------------------------------------------


def test_render_table_contains_every_account_name_and_currency(
    in_memory_db: sqlite3.Connection,
) -> None:
    from finances.reports.balances import get_balances, render_table

    a1 = _insert_account(in_memory_db, "Alpha Bank", currency="USD")
    a2 = _insert_account(in_memory_db, "Bravo Bolivares", currency="VES")
    assert a1.id is not None
    assert a2.id is not None
    _insert_txn(in_memory_db, a1.id, Decimal("100.50"), source_ref="t-1")
    _insert_txn(
        in_memory_db, a2.id, Decimal("250.00"), currency="VES", source_ref="t-2"
    )

    balances = get_balances(in_memory_db)
    out = render_table(balances)

    assert "Alpha Bank" in out
    assert "Bravo Bolivares" in out
    assert "USD" in out
    assert "VES" in out
    assert "100.50" in out
    assert "250.00" in out
    # Header labels must appear.
    assert "Account" in out
    assert "Currency" in out
    assert "Balance" in out


def test_render_table_empty_list_still_prints_headers() -> None:
    from finances.reports.balances import render_table

    out = render_table([])
    assert "Account" in out
    assert "Currency" in out
    assert "Balance" in out
