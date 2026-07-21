"""Migration 013 — `Lifestyle` and `Tools` leave the pickers.

Both were seeded by migration 002 and never used: zero transactions in
nine months, and no definition in ADR-006 beyond the name. Every concrete
example routes somewhere sharper — tours/events to `Leisure`, gym and
grooming to `Personal Care`, gadgets to `Purchases`, apparel to
`Clothing` (migration 005 already rejected `Lifestyle` for that).

Deactivate, never DELETE: the rows keep their ids so any future
resurrection is an `active = 1` flip, and `get_by_name()` (which ignores
``active``) keeps resolving for auto-assignment paths.
"""

from __future__ import annotations

import sqlite3

import pytest

from finances.db.repos import categories as categories_repo


@pytest.fixture()
def migrated_db(seeded_db: sqlite3.Connection) -> sqlite3.Connection:
    return seeded_db


@pytest.mark.parametrize("name", ["Lifestyle", "Tools"])
def test_category_is_deactivated(migrated_db: sqlite3.Connection, name: str) -> None:
    category = categories_repo.get_by_name(migrated_db, "expense", name)
    assert category is not None, f"{name} must survive as a row"
    assert category.active is False


@pytest.mark.parametrize("name", ["Lifestyle", "Tools"])
def test_pickers_no_longer_offer_them(
    migrated_db: sqlite3.Connection, name: str
) -> None:
    active_names = {c.name for c in categories_repo.list_all(migrated_db)}
    assert name not in active_names


@pytest.mark.parametrize("name", ["Leisure", "Going Out", "Groceries", "Personal Care"])
def test_replacement_buckets_stay_active(
    migrated_db: sqlite3.Connection, name: str
) -> None:
    """The categories that absorb Lifestyle/Tools spend must remain pickable."""
    active_names = {c.name for c in categories_repo.list_all(migrated_db)}
    assert name in active_names


def test_rows_are_not_deleted(migrated_db: sqlite3.Connection) -> None:
    # DELETE would cascade category_rules and orphan any historical
    # transaction that ever pointed here.
    row = migrated_db.execute(
        """
        SELECT COUNT(*) AS n FROM categories
        WHERE kind = 'expense' AND name IN ('Lifestyle', 'Tools')
        """
    ).fetchone()
    assert row["n"] == 2
