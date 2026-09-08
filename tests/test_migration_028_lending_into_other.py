"""Migration 028 — Lending is folded into Other, by data (ADR-026 amended).

The owner asked for "lending can go into others". ADR-026 honoured that by
leaving Lending ungrouped, expecting it to miss the top five on its own.
On the real ledger it did not: the chart ranks series by their total over
the whole window, and one big loan in one month kept Lending in the top
five for six months while Health fell into Other.

Owner decision 2026-09-08: Lending goes into Other regardless of rank.
The mechanism is the same column, with the label ``Other``: not a group
that competes for a slot, but an instruction to fold the category into
the computed remainder unconditionally. The remainder is still computed;
this only adds to it.
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


def _stored_other(conn: sqlite3.Connection) -> list[tuple[str, str]]:
    rows = conn.execute(
        "SELECT kind, name FROM categories WHERE group_name = 'Other' ORDER BY kind, name"
    ).fetchall()
    return [(r["kind"], r["name"]) for r in rows]


def test_lending_is_folded_into_other(migrated_db: sqlite3.Connection) -> None:
    row = migrated_db.execute(
        "SELECT group_name FROM categories WHERE kind = 'expense' AND name = 'Lending'"
    ).fetchone()
    assert row is not None
    assert row["group_name"] == "Other"


def test_only_lending_is_stored_as_other(migrated_db: sqlite3.Connection) -> None:
    """A stored Other is an instruction, not a bucket: the remainder is
    still computed, and everything else that misses the cap still lands in
    it without being written down."""
    assert _stored_other(migrated_db) == [("expense", "Lending")]


def test_the_migration_is_recorded_once(migrated_db: sqlite3.Connection) -> None:
    row = migrated_db.execute(
        "SELECT COUNT(*) AS c FROM _migrations WHERE filename LIKE '028_%'"
    ).fetchone()
    assert row["c"] == 1


def test_re_running_changes_no_rows(migrated_db: sqlite3.Connection) -> None:
    before = migrated_db.execute(
        "SELECT id, group_name FROM categories ORDER BY id"
    ).fetchall()

    assert apply_migrations(migrated_db) == []

    after = migrated_db.execute(
        "SELECT id, group_name FROM categories ORDER BY id"
    ).fetchall()
    assert [tuple(r) for r in after] == [tuple(r) for r in before]
