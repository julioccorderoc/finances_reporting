"""Migration 018 — Fees returns to the manual pickers.

Migration 011 hid Fees (and Interest) from the pickers because both were
auto-assigned. Owner decision 2026-08-05: Fees must be pickable again —
the reversal work (ADR-019) and hand-triage both need it. Interest stays
hidden; nothing about it changed.
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


def test_fees_active_again(migrated_db: sqlite3.Connection) -> None:
    fees = categories_repo.get_by_name(migrated_db, "expense", "Fees")
    assert fees is not None and fees.active is True


def test_picker_offers_fees_but_not_interest(
    migrated_db: sqlite3.Connection,
) -> None:
    active_names = {c.name for c in categories_repo.list_all(migrated_db)}
    assert "Fees" in active_names
    assert "Interest" not in active_names
