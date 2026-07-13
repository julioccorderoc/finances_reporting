"""Migration 011 — Interest (income) and Fees (expense) leave the manual
pickers but keep working everywhere else.

Both categories are auto-assigned (Binance earn rewards via
``get_by_name``; bank commission markers via category_rules, which store
``category_id`` directly). Neither path filters on ``active``, so
deactivating only removes them from ``list_all()`` — the source of the
triage picker and top-chips — without touching historical rows or
auto-assignment.
"""

from __future__ import annotations

import sqlite3

import pytest

from finances.db.migrate import apply_migrations
from finances.db.repos import categories as categories_repo


@pytest.fixture()
def migrated_db(tmp_path) -> sqlite3.Connection:
    conn = sqlite3.connect(tmp_path / "test.db")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    apply_migrations(conn)
    yield conn
    conn.close()


def test_interest_and_fees_deactivated(migrated_db: sqlite3.Connection) -> None:
    interest = categories_repo.get_by_name(migrated_db, "income", "Interest")
    fees = categories_repo.get_by_name(migrated_db, "expense", "Fees")
    assert interest is not None and interest.active is False
    assert fees is not None and fees.active is False


def test_pickers_no_longer_offer_them(migrated_db: sqlite3.Connection) -> None:
    active_names = {c.name for c in categories_repo.list_all(migrated_db)}
    assert "Interest" not in active_names
    assert "Fees" not in active_names


def test_get_by_name_still_resolves_for_auto_assignment(
    migrated_db: sqlite3.Connection,
) -> None:
    # The Binance ingest path (binance.py) resolves Interest by name and
    # must keep getting an id after deactivation.
    interest = categories_repo.get_by_name(migrated_db, "income", "Interest")
    assert interest is not None
    assert interest.id is not None


def test_fees_rules_still_point_at_the_category(
    migrated_db: sqlite3.Connection,
) -> None:
    row = migrated_db.execute(
        """
        SELECT COUNT(*) AS n
        FROM category_rules r JOIN categories c ON c.id = r.category_id
        WHERE c.name = 'Fees' AND r.active = 1
        """
    ).fetchone()
    assert row["n"] >= 1
