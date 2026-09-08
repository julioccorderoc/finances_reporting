"""``categories.group_name`` crosses the repo boundary as a typed field.

ADR-026 stores a category's group on the category row. rule-009 says the
repo hands out Pydantic, never raw rows, so ``Category`` gains
``group_name`` and every read path carries it — a caller that got the
column from one query and ``None`` from another would draw a category
grouped on one screen and standing alone on the next.
"""

from __future__ import annotations

import sqlite3

import pytest

from finances.db.repos import categories as categories_repo
from finances.domain.models import Category, TransactionKind


def test_category_defaults_to_no_group() -> None:
    """NULL is the majority case and not an error state (ADR-026 §2.1)."""
    category = Category(kind=TransactionKind.EXPENSE, name="Anything")
    assert category.group_name is None


def test_get_by_name_carries_the_group(in_memory_db: sqlite3.Connection) -> None:
    rent = categories_repo.get_by_name(in_memory_db, TransactionKind.EXPENSE, "Rent")
    assert rent is not None
    assert rent.group_name == "Home"


def test_get_by_id_carries_the_group(in_memory_db: sqlite3.Connection) -> None:
    rent = categories_repo.get_by_name(in_memory_db, TransactionKind.EXPENSE, "Rent")
    assert rent is not None and rent.id is not None

    again = categories_repo.get_by_id(in_memory_db, rent.id)
    assert again is not None
    assert again.group_name == "Home"


def test_list_all_carries_the_group(in_memory_db: sqlite3.Connection) -> None:
    groups = {c.name: c.group_name for c in categories_repo.list_all(in_memory_db)}
    assert groups["Rent"] == "Home"
    assert groups["Dating"] == "Social"
    assert groups["Groceries"] is None


def test_list_all_with_inactive_carries_the_group(
    in_memory_db: sqlite3.Connection,
) -> None:
    groups = {
        c.name: c.group_name
        for c in categories_repo.list_all(in_memory_db, include_inactive=True)
    }
    assert groups["Utilities"] == "Home"
    assert groups["Clothing"] is None


def test_list_for_kind_carries_the_group(in_memory_db: sqlite3.Connection) -> None:
    groups = {
        c.name: c.group_name
        for c in categories_repo.list_for_kind(in_memory_db, TransactionKind.EXPENSE)
    }
    assert groups["Transport"] == "Home"
    assert groups["Internal Transfer"] is None


def test_list_pickable_carries_the_group(in_memory_db: sqlite3.Connection) -> None:
    groups = {c.name: c.group_name for c in categories_repo.list_pickable(in_memory_db)}
    assert groups["Going Out"] == "Social"
    assert groups["Purchases"] is None


@pytest.mark.parametrize("group_name", ["Home", None])
def test_insert_round_trips_the_group(
    in_memory_db: sqlite3.Connection, group_name: str | None
) -> None:
    inserted = categories_repo.insert(
        in_memory_db,
        Category(kind=TransactionKind.EXPENSE, name="Insurance", group_name=group_name),
    )
    assert inserted.id is not None

    stored = categories_repo.get_by_id(in_memory_db, inserted.id)
    assert stored is not None
    assert stored.group_name == group_name
