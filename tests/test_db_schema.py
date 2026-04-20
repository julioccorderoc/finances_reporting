from __future__ import annotations

import sqlite3
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path

import pytest

from finances.db.connection import get_connection
from finances.db.migrate import apply_migrations
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

DOMAIN_DIR = Path(__file__).resolve().parent.parent / "finances" / "domain"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def seeded(db_conn: sqlite3.Connection) -> dict[str, int]:
    """Seed a minimal fixture: two accounts, two categories, two transactions
    forming a transfer pair, a rate, and an earn position. Returns id lookup.
    """
    bank = accounts_repo.insert(
        db_conn,
        Account(
            name="Provincial", kind=AccountKind.BANK, currency="VES", institution="Provincial"
        ),
    )
    binance = accounts_repo.insert(
        db_conn,
        Account(name="Binance Spot", kind=AccountKind.CRYPTO_SPOT, currency="USDT"),
    )
    earn_acct = accounts_repo.insert(
        db_conn,
        Account(name="Binance Earn", kind=AccountKind.CRYPTO_EARN, currency="USDT"),
    )
    # Reuse the v1-taxonomy rows seeded by 002_seed_categories.sql instead of
    # re-inserting (the UNIQUE(kind, name) constraint rejects the duplicate).
    cat_salary = categories_repo.get_by_name(
        db_conn, TransactionKind.INCOME, "Salary"
    )
    assert cat_salary is not None
    cat_food = categories_repo.get_by_name(
        db_conn, TransactionKind.EXPENSE, "Food"
    )
    assert cat_food is not None

    now = datetime(2026, 4, 1, 12, 0, tzinfo=UTC)
    transactions_repo.upsert_by_source_ref(
        db_conn,
        Transaction(
            account_id=bank.id,
            occurred_at=now,
            kind=TransactionKind.INCOME,
            amount=Decimal("1000.00"),
            currency="VES",
            description="Salary deposit",
            category_id=cat_salary.id,
            source="provincial",
            source_ref="ref-income-001",
        ),
    )
    transactions_repo.upsert_by_source_ref(
        db_conn,
        Transaction(
            account_id=binance.id,
            occurred_at=now,
            kind=TransactionKind.EXPENSE,
            amount=Decimal("-12.50"),
            currency="USDT",
            description="Lunch",
            category_id=cat_food.id,
            source="binance",
            source_ref="ref-expense-001",
        ),
    )
    # Paired transfer legs.
    transactions_repo.upsert_by_source_ref(
        db_conn,
        Transaction(
            account_id=binance.id,
            occurred_at=now,
            kind=TransactionKind.TRANSFER,
            amount=Decimal("-100.00"),
            currency="USDT",
            transfer_id="transfer-abc",
            source="binance",
            source_ref="ref-transfer-binance",
        ),
    )
    transactions_repo.upsert_by_source_ref(
        db_conn,
        Transaction(
            account_id=earn_acct.id,
            occurred_at=now,
            kind=TransactionKind.TRANSFER,
            amount=Decimal("100.00"),
            currency="USDT",
            transfer_id="transfer-abc",
            source="binance",
            source_ref="ref-transfer-earn",
        ),
    )
    rates_repo.upsert(
        db_conn,
        Rate(
            as_of_date=date(2026, 4, 1),
            base="USDT",
            quote="VES",
            rate=Decimal("40.0"),
            source="binance_p2p_median",
        ),
    )
    positions_repo.insert(
        db_conn,
        EarnPosition(
            account_id=earn_acct.id,
            product_id="USDT-FLEX",
            asset="USDT",
            principal=Decimal("500.00"),
            apy=Decimal("0.08"),
            started_at=now,
        ),
    )
    import_state_repo.upsert_state(db_conn, source="binance", last_synced_at=now)
    run_id = import_state_repo.start_run(db_conn, "binance")
    import_state_repo.finish_run(db_conn, run_id, status="success", rows_inserted=4)

    return {
        "bank_id": int(bank.id),
        "binance_id": int(binance.id),
        "earn_id": int(earn_acct.id),
        "cat_salary_id": int(cat_salary.id),
        "cat_food_id": int(cat_food.id),
    }


# ---------------------------------------------------------------------------
# Table-level queryability
# ---------------------------------------------------------------------------


def test_accounts_insertable_and_queryable(db_conn: sqlite3.Connection) -> None:
    acct = accounts_repo.insert(
        db_conn,
        Account(name="Cash USD", kind=AccountKind.CASH, currency="USD"),
    )
    assert acct.id is not None
    fetched = accounts_repo.get_by_id(db_conn, acct.id)
    assert fetched is not None
    assert fetched.name == "Cash USD"
    assert fetched.kind == AccountKind.CASH


def test_categories_insertable_and_queryable(db_conn: sqlite3.Connection) -> None:
    # Use a name not present in the v1 taxonomy seeded by 002_seed_categories.sql.
    cat = categories_repo.insert(
        db_conn, Category(kind=TransactionKind.EXPENSE, name="SchemaProbe")
    )
    assert cat.id is not None
    fetched = categories_repo.get_by_name(db_conn, TransactionKind.EXPENSE, "SchemaProbe")
    assert fetched is not None
    assert fetched.id == cat.id


def test_category_rules_insertable(db_conn: sqlite3.Connection) -> None:
    cat = categories_repo.insert(
        db_conn, Category(kind=TransactionKind.EXPENSE, name="RulesSchemaProbe")
    )
    db_conn.execute(
        "INSERT INTO category_rules (pattern, category_id, source, priority) VALUES (?, ?, ?, ?)",
        ("(?i)probe", cat.id, "schema_test", 10),
    )
    row = db_conn.execute(
        "SELECT pattern, priority FROM category_rules WHERE source = 'schema_test'"
    ).fetchone()
    assert row["pattern"] == "(?i)probe"
    assert row["priority"] == 10


def test_transactions_roundtrip_decimal_precision(db_conn: sqlite3.Connection) -> None:
    acct = accounts_repo.insert(
        db_conn, Account(name="A", kind=AccountKind.BANK, currency="VES")
    )
    txn = Transaction(
        account_id=acct.id,
        occurred_at=datetime(2026, 4, 1, tzinfo=UTC),
        kind=TransactionKind.EXPENSE,
        amount=Decimal("-123.456789"),
        currency="VES",
        source="provincial",
        source_ref="ref-1",
    )
    result = transactions_repo.upsert_by_source_ref(db_conn, txn)
    fetched = transactions_repo.get_by_id(db_conn, result["id"])
    assert fetched is not None
    assert fetched.amount == Decimal("-123.456789")


def test_rates_insertable(db_conn: sqlite3.Connection) -> None:
    rate = rates_repo.upsert(
        db_conn,
        Rate(
            as_of_date=date(2026, 4, 15),
            base="USD",
            quote="VES",
            rate=Decimal("38.12"),
            source="bcv",
        ),
    )
    fetched = rates_repo.get(
        db_conn, as_of_date=date(2026, 4, 15), base="USD", quote="VES", source="bcv"
    )
    assert fetched is not None
    assert fetched.rate == Decimal("38.12")
    assert fetched.id == rate.id


def test_earn_positions_insertable(db_conn: sqlite3.Connection) -> None:
    acct = accounts_repo.insert(
        db_conn, Account(name="Binance Earn", kind=AccountKind.CRYPTO_EARN, currency="USDT")
    )
    pos = positions_repo.insert(
        db_conn,
        EarnPosition(
            account_id=acct.id,
            product_id="USDT-LOCK-30",
            asset="USDT",
            principal=Decimal("1000.00"),
            apy=Decimal("0.05"),
            started_at=datetime(2026, 3, 1, tzinfo=UTC),
        ),
    )
    assert pos.id is not None
    open_positions = positions_repo.list_open(db_conn, acct.id)
    assert len(open_positions) == 1
    assert open_positions[0].principal == Decimal("1000.00")


def test_import_state_and_runs(db_conn: sqlite3.Connection) -> None:
    import_state_repo.upsert_state(
        db_conn, source="provincial", last_synced_at=datetime(2026, 4, 1, tzinfo=UTC)
    )
    state = import_state_repo.get_state(db_conn, "provincial")
    assert state is not None
    assert state["source"] == "provincial"

    run_id = import_state_repo.start_run(db_conn, "provincial")
    import_state_repo.finish_run(db_conn, run_id, status="success", rows_inserted=42)
    run = import_state_repo.get_run(db_conn, run_id)
    assert run is not None
    assert run["status"] == "success"
    assert run["rows_inserted"] == 42


# ---------------------------------------------------------------------------
# Constraints and idempotency
# ---------------------------------------------------------------------------


def test_transactions_source_source_ref_unique(db_conn: sqlite3.Connection) -> None:
    acct = accounts_repo.insert(
        db_conn, Account(name="A", kind=AccountKind.BANK, currency="VES")
    )
    base = Transaction(
        account_id=acct.id,
        occurred_at=datetime(2026, 4, 1, tzinfo=UTC),
        kind=TransactionKind.INCOME,
        amount=Decimal("100"),
        currency="VES",
        source="provincial",
        source_ref="dup-key",
    )
    transactions_repo.insert(db_conn, base)
    with pytest.raises(sqlite3.IntegrityError):
        transactions_repo.insert(db_conn, base)


def test_upsert_by_source_ref_is_idempotent(db_conn: sqlite3.Connection) -> None:
    acct = accounts_repo.insert(
        db_conn, Account(name="A", kind=AccountKind.BANK, currency="VES")
    )
    txn = Transaction(
        account_id=acct.id,
        occurred_at=datetime(2026, 4, 1, tzinfo=UTC),
        kind=TransactionKind.EXPENSE,
        amount=Decimal("-42.00"),
        currency="VES",
        source="provincial",
        source_ref="stable-ref-xyz",
    )
    first = transactions_repo.upsert_by_source_ref(db_conn, txn)
    second = transactions_repo.upsert_by_source_ref(db_conn, txn)

    assert first["rows_inserted"] == 1
    assert first["rows_updated"] == 0
    assert second["rows_inserted"] == 0
    assert second["rows_updated"] == 1
    assert first["id"] == second["id"]
    assert transactions_repo.count(db_conn) == 1


def test_rates_unique_constraint(db_conn: sqlite3.Connection) -> None:
    rate = Rate(
        as_of_date=date(2026, 4, 1),
        base="USD",
        quote="VES",
        rate=Decimal("38"),
        source="bcv",
    )
    rates_repo.insert(db_conn, rate)
    with pytest.raises(sqlite3.IntegrityError):
        rates_repo.insert(db_conn, rate)


# ---------------------------------------------------------------------------
# Views
# ---------------------------------------------------------------------------


def test_v_account_balances_exists(db_conn: sqlite3.Connection, seeded) -> None:
    rows = db_conn.execute(
        "SELECT account_id, account_name, currency, balance_native FROM v_account_balances ORDER BY account_name"
    ).fetchall()
    assert len(rows) >= 3
    by_name = {r["account_name"]: r for r in rows}
    assert "Provincial" in by_name
    assert by_name["Provincial"]["balance_native"] == pytest.approx(1000.00)


def test_v_transactions_usd_exists(db_conn: sqlite3.Connection, seeded) -> None:
    rows = db_conn.execute(
        "SELECT transaction_id, kind, amount_usd, rate_source FROM v_transactions_usd"
    ).fetchall()
    assert len(rows) >= 2
    # USDT/USDC/USD rows should resolve to native USD with rate_source='native_usd'
    assert any(r["rate_source"] == "native_usd" for r in rows)
    # At least one row should be VES resolved via the rates table.
    assert any(r["rate_source"] == "rates_table" and r["amount_usd"] is not None for r in rows)


def test_v_monthly_summary_excludes_transfers(db_conn: sqlite3.Connection, seeded) -> None:
    rows = db_conn.execute(
        "SELECT month, kind, tx_count, total_native FROM v_monthly_summary"
    ).fetchall()
    assert len(rows) >= 1
    assert all(r["kind"] != "transfer" for r in rows)


def test_v_unreconciled_transfers_empty_when_paired(
    db_conn: sqlite3.Connection, seeded
) -> None:
    # Our fixture created two legs sharing a transfer_id -> should be zero rows.
    rows = db_conn.execute("SELECT transfer_id, leg_count FROM v_unreconciled_transfers").fetchall()
    assert rows == []


def test_v_unreconciled_transfers_flags_orphan_leg(
    db_conn: sqlite3.Connection, seeded
) -> None:
    acct = accounts_repo.insert(
        db_conn, Account(name="Orphan", kind=AccountKind.BANK, currency="VES")
    )
    transactions_repo.insert(
        db_conn,
        Transaction(
            account_id=acct.id,
            occurred_at=datetime(2026, 4, 1, tzinfo=UTC),
            kind=TransactionKind.TRANSFER,
            amount=Decimal("50.00"),
            currency="VES",
            transfer_id="orphan-abc",
            source="manual",
            source_ref="orphan-ref-1",
        ),
    )
    rows = db_conn.execute("SELECT transfer_id, leg_count FROM v_unreconciled_transfers").fetchall()
    transfer_ids = [r["transfer_id"] for r in rows]
    assert "orphan-abc" in transfer_ids


# ---------------------------------------------------------------------------
# Migrate runner + migration bookkeeping
# ---------------------------------------------------------------------------


def test_migrate_is_idempotent(tmp_path: Path) -> None:
    path = tmp_path / "reapply.db"
    conn = get_connection(path)
    first = apply_migrations(conn)
    second = apply_migrations(conn)
    assert "001_initial.sql" in first
    assert "002_seed_categories.sql" in first
    assert second == []
    rows = conn.execute("SELECT filename FROM _migrations").fetchall()
    assert {r["filename"] for r in rows} == {
        "001_initial.sql",
        "002_seed_categories.sql",
    }
    conn.close()


# ---------------------------------------------------------------------------
# Guard: no dataclasses in finances/domain/
# ---------------------------------------------------------------------------


def test_no_dataclass_in_domain() -> None:
    offenders: list[str] = []
    for py in DOMAIN_DIR.rglob("*.py"):
        text = py.read_text(encoding="utf-8")
        if "from dataclasses import dataclass" in text:
            offenders.append(str(py))
        if "import dataclasses" in text:
            offenders.append(str(py))
    assert offenders == [], f"dataclass usage forbidden in finances/domain/: {offenders}"
