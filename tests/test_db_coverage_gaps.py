"""Retroactive coverage tests for EPIC-002 (merged) — EPIC-002b sweep.

Rule-011 mandates ≥85% coverage for ``finances/db/**`` and
``finances/domain/models.py``. The existing ``tests/test_db_schema.py`` covers
the happy paths but leaves a few branches and validator error paths
unexercised. This module closes those gaps with small, focused tests —
one happy-path + one failure-mode per previously-untested function, per
rule-011's "tests per function" constraint.

These tests deliberately avoid duplicating coverage already provided by
``test_db_schema.py`` — if a branch is green there, it is not retested here.
"""

from __future__ import annotations

import sqlite3
from datetime import UTC, date, datetime
from decimal import Decimal

import pytest

from finances.db.repos import (
    accounts as accounts_repo,
    categories as categories_repo,
    import_state as import_state_repo,
    positions as positions_repo,
    rates as rates_repo,
    transactions as transactions_repo,
)
from finances.domain.models import (
    Account,
    AccountKind,
    Category,
    EarnPosition,
    Rate,
    Transaction,
    TransactionKind,
)


# ---------------------------------------------------------------------------
# accounts repo
# ---------------------------------------------------------------------------


def test_accounts_get_by_name_returns_account(db_conn: sqlite3.Connection) -> None:
    inserted = accounts_repo.insert(
        db_conn, Account(name="Lookup Target", kind=AccountKind.BANK, currency="USD")
    )
    fetched = accounts_repo.get_by_name(db_conn, "Lookup Target")
    assert fetched is not None
    assert fetched.id == inserted.id


def test_accounts_get_by_name_returns_none_when_missing(
    db_conn: sqlite3.Connection,
) -> None:
    assert accounts_repo.get_by_name(db_conn, "does-not-exist") is None


def test_accounts_list_all_includes_inactive_when_requested(
    db_conn: sqlite3.Connection,
) -> None:
    accounts_repo.insert(
        db_conn, Account(name="Active One", kind=AccountKind.CASH, currency="USD")
    )
    accounts_repo.insert(
        db_conn,
        Account(
            name="Retired One", kind=AccountKind.CASH, currency="USD", active=False
        ),
    )
    active_only = {a.name for a in accounts_repo.list_all(db_conn)}
    assert active_only == {"Active One"}
    all_incl_inactive = {
        a.name for a in accounts_repo.list_all(db_conn, include_inactive=True)
    }
    assert all_incl_inactive == {"Active One", "Retired One"}


# ---------------------------------------------------------------------------
# categories repo
# ---------------------------------------------------------------------------


def test_categories_get_by_id_returns_category(db_conn: sqlite3.Connection) -> None:
    cat = categories_repo.insert(
        db_conn, Category(kind=TransactionKind.EXPENSE, name="LookupMe")
    )
    fetched = categories_repo.get_by_id(db_conn, cat.id)
    assert fetched is not None
    assert fetched.name == "LookupMe"


def test_categories_get_by_id_returns_none_when_missing(
    db_conn: sqlite3.Connection,
) -> None:
    assert categories_repo.get_by_id(db_conn, 99_999) is None


def test_categories_get_by_name_accepts_string_kind(db_conn: sqlite3.Connection) -> None:
    categories_repo.insert(
        db_conn, Category(kind=TransactionKind.INCOME, name="String Kind Path")
    )
    fetched = categories_repo.get_by_name(db_conn, "income", "String Kind Path")
    assert fetched is not None


def test_categories_list_all_includes_inactive_when_requested(
    db_conn: sqlite3.Connection,
) -> None:
    categories_repo.insert(
        db_conn, Category(kind=TransactionKind.EXPENSE, name="LiveCat")
    )
    categories_repo.insert(
        db_conn,
        Category(kind=TransactionKind.EXPENSE, name="DeadCat", active=False),
    )
    active_only = {c.name for c in categories_repo.list_all(db_conn)}
    assert "LiveCat" in active_only
    assert "DeadCat" not in active_only
    incl_inactive = {
        c.name for c in categories_repo.list_all(db_conn, include_inactive=True)
    }
    assert "DeadCat" in incl_inactive


# ---------------------------------------------------------------------------
# transactions repo
# ---------------------------------------------------------------------------


def test_transactions_upsert_requires_source_ref(db_conn: sqlite3.Connection) -> None:
    acct = accounts_repo.insert(
        db_conn, Account(name="A1", kind=AccountKind.BANK, currency="VES")
    )
    txn = Transaction(
        account_id=acct.id,
        occurred_at=datetime(2026, 4, 1, tzinfo=UTC),
        kind=TransactionKind.INCOME,
        amount=Decimal("1"),
        currency="VES",
        source="manual",
        source_ref=None,
    )
    with pytest.raises(ValueError, match="source_ref"):
        transactions_repo.upsert_by_source_ref(db_conn, txn)


def test_transactions_list_by_account_respects_limit(db_conn: sqlite3.Connection) -> None:
    acct = accounts_repo.insert(
        db_conn, Account(name="A2", kind=AccountKind.BANK, currency="VES")
    )
    for idx in range(3):
        transactions_repo.insert(
            db_conn,
            Transaction(
                account_id=acct.id,
                occurred_at=datetime(2026, 4, idx + 1, tzinfo=UTC),
                kind=TransactionKind.EXPENSE,
                amount=Decimal("-1"),
                currency="VES",
                source="manual",
                source_ref=f"ref-{idx}",
            ),
        )
    all_rows = transactions_repo.list_by_account(db_conn, acct.id)
    limited = transactions_repo.list_by_account(db_conn, acct.id, limit=2)
    assert len(all_rows) == 3
    assert len(limited) == 2


def test_transactions_roundtrip_preserves_user_rate(db_conn: sqlite3.Connection) -> None:
    acct = accounts_repo.insert(
        db_conn, Account(name="A3", kind=AccountKind.BANK, currency="VES")
    )
    txn = Transaction(
        account_id=acct.id,
        occurred_at=datetime(2026, 4, 1, tzinfo=UTC),
        kind=TransactionKind.EXPENSE,
        amount=Decimal("-10"),
        currency="VES",
        user_rate=Decimal("38.123456"),
        source="manual",
        source_ref="user-rate-ref",
    )
    inserted = transactions_repo.insert(db_conn, txn)
    fetched = transactions_repo.get_by_id(db_conn, inserted.id)
    assert fetched is not None
    assert fetched.user_rate == Decimal("38.123456")


def test_transactions_get_by_source_ref_returns_none_when_missing(
    db_conn: sqlite3.Connection,
) -> None:
    assert (
        transactions_repo.get_by_source_ref(db_conn, "manual", "never-inserted")
        is None
    )


# ---------------------------------------------------------------------------
# rates repo
# ---------------------------------------------------------------------------


def test_rates_latest_on_or_before_returns_closest_prior_row(
    db_conn: sqlite3.Connection,
) -> None:
    rates_repo.insert(
        db_conn,
        Rate(
            as_of_date=date(2026, 4, 1),
            base="USD",
            quote="VES",
            rate=Decimal("38.00"),
            source="bcv",
        ),
    )
    rates_repo.insert(
        db_conn,
        Rate(
            as_of_date=date(2026, 4, 10),
            base="USD",
            quote="VES",
            rate=Decimal("39.00"),
            source="bcv",
        ),
    )
    result = rates_repo.latest_on_or_before(
        db_conn,
        as_of_date=date(2026, 4, 15),
        base="USD",
        quote="VES",
        source="bcv",
    )
    assert result is not None
    assert result.rate == Decimal("39.00")


def test_rates_latest_on_or_before_returns_none_when_no_prior_row(
    db_conn: sqlite3.Connection,
) -> None:
    assert (
        rates_repo.latest_on_or_before(
            db_conn,
            as_of_date=date(2020, 1, 1),
            base="USD",
            quote="VES",
            source="bcv",
        )
        is None
    )


def test_rates_get_returns_none_when_missing(db_conn: sqlite3.Connection) -> None:
    assert (
        rates_repo.get(
            db_conn,
            as_of_date=date(2026, 1, 1),
            base="USD",
            quote="VES",
            source="bcv",
        )
        is None
    )


# ---------------------------------------------------------------------------
# positions repo
# ---------------------------------------------------------------------------


def test_positions_insert_with_snapshot_at_persists_snapshot(
    db_conn: sqlite3.Connection,
) -> None:
    acct = accounts_repo.insert(
        db_conn,
        Account(name="EarnSnap", kind=AccountKind.CRYPTO_EARN, currency="USDT"),
    )
    snap = datetime(2026, 4, 1, 12, 0, tzinfo=UTC)
    pos = positions_repo.insert(
        db_conn,
        EarnPosition(
            account_id=acct.id,
            product_id="USDT-SNAP",
            asset="USDT",
            principal=Decimal("100"),
            apy=Decimal("0.05"),
            started_at=datetime(2026, 3, 1, tzinfo=UTC),
            snapshot_at=snap,
        ),
    )
    open_positions = positions_repo.list_open(db_conn)
    ids = {p.id for p in open_positions}
    assert pos.id in ids


def test_positions_close_sets_ended_at(db_conn: sqlite3.Connection) -> None:
    acct = accounts_repo.insert(
        db_conn,
        Account(name="EarnClose", kind=AccountKind.CRYPTO_EARN, currency="USDT"),
    )
    pos = positions_repo.insert(
        db_conn,
        EarnPosition(
            account_id=acct.id,
            product_id="USDT-CLOSE",
            asset="USDT",
            principal=Decimal("100"),
            started_at=datetime(2026, 3, 1, tzinfo=UTC),
        ),
    )
    positions_repo.close(db_conn, pos.id, datetime(2026, 4, 1, tzinfo=UTC))
    still_open_ids = {p.id for p in positions_repo.list_open(db_conn)}
    assert pos.id not in still_open_ids


def test_positions_list_open_without_account_filter(
    db_conn: sqlite3.Connection,
) -> None:
    acct_a = accounts_repo.insert(
        db_conn,
        Account(name="EarnAll-A", kind=AccountKind.CRYPTO_EARN, currency="USDT"),
    )
    acct_b = accounts_repo.insert(
        db_conn,
        Account(name="EarnAll-B", kind=AccountKind.CRYPTO_EARN, currency="USDT"),
    )
    for acct in (acct_a, acct_b):
        positions_repo.insert(
            db_conn,
            EarnPosition(
                account_id=acct.id,
                product_id=f"USDT-{acct.id}",
                asset="USDT",
                principal=Decimal("10"),
                started_at=datetime(2026, 3, 1, tzinfo=UTC),
            ),
        )
    all_open = positions_repo.list_open(db_conn)
    assert len(all_open) >= 2


# ---------------------------------------------------------------------------
# import_state repo
# ---------------------------------------------------------------------------


def test_import_state_get_state_returns_none_when_missing(
    db_conn: sqlite3.Connection,
) -> None:
    assert import_state_repo.get_state(db_conn, "never-registered") is None


def test_import_state_get_run_returns_none_when_missing(
    db_conn: sqlite3.Connection,
) -> None:
    assert import_state_repo.get_run(db_conn, 999_999) is None


def test_import_state_finish_run_rejects_invalid_status(
    db_conn: sqlite3.Connection,
) -> None:
    run_id = import_state_repo.start_run(db_conn, "binance")
    with pytest.raises(ValueError, match="invalid status"):
        import_state_repo.finish_run(db_conn, run_id, status="weird")


# ---------------------------------------------------------------------------
# domain/models validators — failure modes
# ---------------------------------------------------------------------------


def test_transaction_rejects_float_amount() -> None:
    with pytest.raises(ValueError, match="float monetary inputs are forbidden"):
        Transaction(
            account_id=1,
            occurred_at=datetime(2026, 4, 1, tzinfo=UTC),
            kind=TransactionKind.EXPENSE,
            amount=1.23,  # type: ignore[arg-type]
            currency="USD",
            source="manual",
        )


def test_transaction_rejects_bool_amount() -> None:
    with pytest.raises(ValueError, match="bool is not a valid monetary value"):
        Transaction(
            account_id=1,
            occurred_at=datetime(2026, 4, 1, tzinfo=UTC),
            kind=TransactionKind.EXPENSE,
            amount=True,  # type: ignore[arg-type]
            currency="USD",
            source="manual",
        )


def test_transaction_rejects_unsupported_amount_type() -> None:
    with pytest.raises(ValueError, match="cannot coerce"):
        Transaction(
            account_id=1,
            occurred_at=datetime(2026, 4, 1, tzinfo=UTC),
            kind=TransactionKind.EXPENSE,
            amount=[1, 2, 3],  # type: ignore[arg-type]
            currency="USD",
            source="manual",
        )


def test_transaction_requires_tzaware_occurred_at() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        Transaction(
            account_id=1,
            occurred_at=datetime(2026, 4, 1),  # naive
            kind=TransactionKind.EXPENSE,
            amount=Decimal("1"),
            currency="USD",
            source="manual",
        )


def test_transaction_accepts_int_and_str_amount() -> None:
    from_int = Transaction(
        account_id=1,
        occurred_at=datetime(2026, 4, 1, tzinfo=UTC),
        kind=TransactionKind.INCOME,
        amount=42,
        currency="usd",
        source="manual",
    )
    from_str = Transaction(
        account_id=1,
        occurred_at=datetime(2026, 4, 1, tzinfo=UTC),
        kind=TransactionKind.INCOME,
        amount="42.5",
        currency="usd",
        source="manual",
    )
    assert from_int.amount == Decimal("42")
    assert from_str.amount == Decimal("42.5")
    # currency validator should uppercase on both paths.
    assert from_int.currency == "USD"
    assert from_str.currency == "USD"


def test_account_created_at_validator_rejects_naive() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        Account(
            name="BadTz",
            kind=AccountKind.BANK,
            currency="USD",
            created_at=datetime(2026, 4, 1),
        )


def test_category_created_at_validator_rejects_naive() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        Category(
            kind=TransactionKind.EXPENSE,
            name="BadCat",
            created_at=datetime(2026, 4, 1),
        )


def test_rate_created_at_validator_rejects_naive() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        Rate(
            as_of_date=date(2026, 4, 1),
            base="usd",
            quote="ves",
            rate=Decimal("38"),
            source="bcv",
            created_at=datetime(2026, 4, 1),
        )


def test_rate_uppercases_base_and_quote() -> None:
    rate = Rate(
        as_of_date=date(2026, 4, 1),
        base="usd",
        quote="ves",
        rate="38",
        source="bcv",
    )
    assert rate.base == "USD"
    assert rate.quote == "VES"


def test_earn_position_uppercases_asset_and_rejects_naive_started_at() -> None:
    pos = EarnPosition(
        account_id=1,
        product_id="p",
        asset="usdt",
        principal="1",
        started_at=datetime(2026, 3, 1, tzinfo=UTC),
    )
    assert pos.asset == "USDT"
    with pytest.raises(ValueError, match="timezone-aware"):
        EarnPosition(
            account_id=1,
            product_id="p",
            asset="usdt",
            principal="1",
            started_at=datetime(2026, 3, 1),  # naive
        )


# ---------------------------------------------------------------------------
# in_memory_db / seeded_db smoke coverage (proves the fixture applies
# migrations and the seed inserts rows).
# ---------------------------------------------------------------------------


def test_in_memory_db_has_expected_tables(in_memory_db: sqlite3.Connection) -> None:
    rows = in_memory_db.execute(
        "SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name"
    ).fetchall()
    names = {r["name"] for r in rows}
    # Core EPIC-002 tables plus the migrations bookkeeping table.
    for required in (
        "_migrations",
        "accounts",
        "categories",
        "transactions",
        "rates",
        "earn_positions",
        "import_state",
        "import_runs",
    ):
        assert required in names, f"missing table: {required}"


def test_seeded_db_contains_v1_taxonomy(seeded_db: sqlite3.Connection) -> None:
    categories = seeded_db.execute(
        "SELECT kind, name FROM categories ORDER BY kind, name"
    ).fetchall()
    kinds = {r["kind"] for r in categories}
    assert {"income", "expense", "transfer", "adjustment"}.issubset(kinds)
    accounts = seeded_db.execute("SELECT name FROM accounts").fetchall()
    names = {r["name"] for r in accounts}
    assert {
        "Provincial Bolivares",
        "Binance Spot",
        "Binance Funding",
        "Binance Earn",
        "Cash USD",
    } == names
