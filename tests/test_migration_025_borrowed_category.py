"""Migration 025 — `Borrowed`, for money someone lends *you*.

The ledger has `Lending` (expense) and `Loan Repayment` (income), both
about money the owner lends *out*. Money lent *to* him had no category:
filed as income it inflated his income, and the repayment filed as an
expense inflated his burn. Neither happened — a loan is not earnings and
returning it is not spending.

Owner decision 2026-09-04 (`docs/plans/2026-09-03-borrowed-money-findings.md`,
Option 1): a transfer-kind `Borrowed` category takes both legs, so what he
still owes is what came in minus what has gone back, and the distinction
from `External Transfer` — *money passing through for someone else* —
survives. Pickable from the list, never on a numbered chip, exactly the
shape migration 022 gave the other two transfer categories.

`Lending` / `Loan Repayment` keep their kinds (same decision): money he
lends out still counts as spending the month it leaves.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from finances.db.migrate import apply_migrations
from finances.db.repos import categories as categories_repo
from finances.domain.models import TransactionKind

ICONS = (
    Path(__file__).resolve().parents[1]
    / "finances"
    / "web"
    / "templates"
    / "_icons.html"
)


@pytest.fixture()
def migrated_db(tmp_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(tmp_path / "test.db")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    apply_migrations(conn)
    yield conn
    conn.close()


def _borrowed(conn: sqlite3.Connection) -> sqlite3.Row:
    row = conn.execute(
        "SELECT kind, active, auto_only, chip_eligible, icon "
        "FROM categories WHERE name = 'Borrowed'"
    ).fetchone()
    assert row is not None, "migration 025 did not insert Borrowed"
    return row


def test_borrowed_is_a_transfer_kind_category(migrated_db: sqlite3.Connection) -> None:
    """Transfer-kind is the whole decision: neither income nor spending."""
    assert _borrowed(migrated_db)["kind"] == "transfer"


def test_borrowed_is_pickable_but_never_a_chip(migrated_db: sqlite3.Connection) -> None:
    row = _borrowed(migrated_db)

    assert row["active"] == 1
    assert row["auto_only"] == 0
    assert row["chip_eligible"] == 0


def test_borrowed_carries_a_vendored_icon(migrated_db: sqlite3.Connection) -> None:
    """An icon name the macro does not vendor renders nothing (SIGNAL)."""
    icon = _borrowed(migrated_db)["icon"]

    assert icon == "hand-coins"
    assert f'"{icon}":' in ICONS.read_text(encoding="utf-8")


def test_the_picker_offers_borrowed(migrated_db: sqlite3.Connection) -> None:
    names = [c.name for c in categories_repo.list_pickable(migrated_db)]
    assert "Borrowed" in names


def test_lending_and_loan_repayment_keep_their_kinds(
    migrated_db: sqlite3.Connection,
) -> None:
    """Owner decision 2026-09-04: the mirror image is left alone.

    Making them transfer-kind would be a change to what an existing
    category *means*, which gets an ADR first.
    """
    lending = categories_repo.get_by_name(
        migrated_db, TransactionKind.EXPENSE, "Lending"
    )
    repayment = categories_repo.get_by_name(
        migrated_db, TransactionKind.INCOME, "Loan Repayment"
    )

    assert lending is not None and lending.kind == TransactionKind.EXPENSE
    assert repayment is not None and repayment.kind == TransactionKind.INCOME


def test_no_rule_routes_anything_to_borrowed(migrated_db: sqlite3.Connection) -> None:
    """Nothing in a bank string says "this is a loan".

    The sender's account number is the only signal and it is a person, not
    a merchant, so this stays a triage decision (rule-006 chain untouched).
    """
    borrowed_id = migrated_db.execute(
        "SELECT id FROM categories WHERE name = 'Borrowed'"
    ).fetchone()["id"]
    rules = migrated_db.execute(
        "SELECT COUNT(*) AS c FROM category_rules WHERE category_id = ?",
        (borrowed_id,),
    ).fetchone()

    assert rules["c"] == 0


def test_the_migration_is_recorded_once(migrated_db: sqlite3.Connection) -> None:
    row = migrated_db.execute(
        "SELECT COUNT(*) AS c FROM _migrations WHERE filename LIKE '025_%'"
    ).fetchone()
    assert row["c"] == 1


def test_re_running_inserts_no_second_borrowed(
    migrated_db: sqlite3.Connection,
) -> None:
    apply_migrations(migrated_db)

    row = migrated_db.execute(
        "SELECT COUNT(*) AS c FROM categories WHERE name = 'Borrowed'"
    ).fetchone()
    assert row["c"] == 1
