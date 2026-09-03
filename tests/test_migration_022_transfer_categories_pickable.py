"""Migration 022 — the two transfer categories may be picked by hand again.

Migration 021 made every transfer-kind category ``auto_only`` on the
reasoning that a transfer is confirmed as a *pair*, never declared by
tagging one leg. The owner's ledger disagrees: money passes through in
a transitional way — a deposit forwarded to someone, cash withdrawn to
the wallet whose other leg is not in the ledger — and it is neither
income nor spending. ``category_fits`` has always allowed a
transfer-kind category on an income or expense row for exactly that
reason ("this money moved, it was not spent"), and
:data:`finances.domain.money.SQL_NOT_CURRENCY_MOVEMENT` acts on it. The
picker was the only surface refusing what the write path accepts.

Owner decision 2026-09-03: both ``Internal Transfer`` and ``External
Transfer`` are pickable, never on a numbered chip. Adjustment categories
and ``Interest`` stay system-written.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from finances.db.migrate import apply_migrations


@pytest.fixture()
def migrated_db(tmp_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(tmp_path / "test.db")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    apply_migrations(conn)
    yield conn
    conn.close()


def _flags(conn: sqlite3.Connection, name: str) -> sqlite3.Row:
    row = conn.execute(
        "SELECT active, auto_only, chip_eligible FROM categories WHERE name = ?",
        (name,),
    ).fetchone()
    assert row is not None, name
    return row


@pytest.mark.parametrize("name", ["Internal Transfer", "External Transfer"])
def test_the_transfer_categories_are_pickable_but_never_chips(
    migrated_db: sqlite3.Connection, name: str
) -> None:
    row = _flags(migrated_db, name)

    assert row["active"] == 1
    assert row["auto_only"] == 0
    assert row["chip_eligible"] == 0


@pytest.mark.parametrize("name", ["FX Diff", "Reconciliation", "Interest"])
def test_adjustments_and_interest_stay_system_written(
    migrated_db: sqlite3.Connection, name: str
) -> None:
    assert _flags(migrated_db, name)["auto_only"] == 1


def test_the_migration_is_recorded_once(migrated_db: sqlite3.Connection) -> None:
    rows = migrated_db.execute(
        "SELECT COUNT(*) AS c FROM _migrations WHERE filename LIKE '022_%'"
    ).fetchone()
    assert rows["c"] == 1
