"""``categories_repo.list_pickable`` — the hand-pickable set (rule-011 RED first).

One definition of "pickable", in SQL, in one place: ``active = 1 AND
auto_only = 0``. Every picker surface reads it from here rather than
re-deriving the rule, and rule-009 means it comes back as ``Category``
models, never raw dicts.
"""

from __future__ import annotations

import sqlite3

import pytest

from finances.db.migrate import apply_migrations
from finances.db.repos import categories as categories_repo
from finances.domain.models import Category, TransactionKind


@pytest.fixture()
def migrated_db(tmp_path) -> sqlite3.Connection:
    conn = sqlite3.connect(tmp_path / "test.db")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    apply_migrations(conn)
    yield conn
    conn.close()


def test_returns_pydantic_models_carrying_the_picker_fields(
    migrated_db: sqlite3.Connection,
) -> None:
    pickable = categories_repo.list_pickable(migrated_db)
    assert pickable, "seed data should yield a pickable set"
    assert all(isinstance(c, Category) for c in pickable)
    groceries = next(c for c in pickable if c.name == "Groceries")
    assert groceries.kind is TransactionKind.EXPENSE
    assert groceries.icon == "shopping-basket"
    assert groceries.auto_only is False
    assert groceries.chip_eligible is True


def test_excludes_auto_only_and_inactive(migrated_db: sqlite3.Connection) -> None:
    names = {c.name for c in categories_repo.list_pickable(migrated_db)}
    # auto-only
    assert names.isdisjoint(
        {"Internal Transfer", "External Transfer", "FX Diff", "Reconciliation", "Interest"}
    )
    # retired
    assert names.isdisjoint({"Clothing", "Lifestyle", "Tools"})


def test_keeps_fees_because_it_is_pickable_not_chippable(
    migrated_db: sqlite3.Connection,
) -> None:
    fees = next(c for c in categories_repo.list_pickable(migrated_db) if c.name == "Fees")
    assert fees.chip_eligible is False


def test_only_expense_and_income_kinds_survive(migrated_db: sqlite3.Connection) -> None:
    kinds = {c.kind for c in categories_repo.list_pickable(migrated_db)}
    assert kinds == {TransactionKind.EXPENSE, TransactionKind.INCOME}


def test_ordered_by_kind_then_name(migrated_db: sqlite3.Connection) -> None:
    pickable = categories_repo.list_pickable(migrated_db)
    keys = [(c.kind.value, c.name) for c in pickable]
    assert keys == sorted(keys)


def test_matches_the_active_and_not_auto_only_definition(
    migrated_db: sqlite3.Connection,
) -> None:
    expected = {
        r["name"]
        for r in migrated_db.execute(
            "SELECT name FROM categories WHERE active = 1 AND auto_only = 0"
        )
    }
    assert {c.name for c in categories_repo.list_pickable(migrated_db)} == expected
