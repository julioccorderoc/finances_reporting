"""Migration 023 — the tombstone table a delete writes (ADR-022 §2.1).

A delete is a real ``DELETE`` plus a row here, so the next import of the
same statement cannot resurrect what the owner removed. The table is
keyed on ``(source, source_ref)`` — the ledger's dedup key (rule-010) —
because that, not the row id, is what an importer arrives holding.

``snapshot`` keeps the deleted row as JSON: nothing is truly lost, and a
future undo is a re-insert plus a tombstone drop.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from pathlib import Path

import pytest

from finances.db.migrate import apply_migrations

MIGRATION = "023_deleted_transactions.sql"


@pytest.fixture()
def migrated_db(tmp_path: Path) -> Iterator[sqlite3.Connection]:
    conn = sqlite3.connect(tmp_path / "test.db")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    apply_migrations(conn)
    yield conn
    conn.close()


def _columns(conn: sqlite3.Connection) -> dict[str, sqlite3.Row]:
    return {
        row["name"]: row
        for row in conn.execute("PRAGMA table_info(deleted_transactions)")
    }


def test_migration_is_applied(migrated_db: sqlite3.Connection) -> None:
    applied = {
        row["filename"]
        for row in migrated_db.execute("SELECT filename FROM _migrations")
    }
    assert MIGRATION in applied


def test_table_carries_the_dedup_key_and_the_record(
    migrated_db: sqlite3.Connection,
) -> None:
    cols = _columns(migrated_db)
    assert set(cols) == {
        "source",
        "source_ref",
        "deleted_at",
        "reason",
        "snapshot",
    }
    # The dedup key and the record of what was removed are mandatory; the
    # owner's words are optional.
    assert cols["source"]["notnull"] == 1
    assert cols["source_ref"]["notnull"] == 1
    assert cols["deleted_at"]["notnull"] == 1
    assert cols["snapshot"]["notnull"] == 1
    assert cols["reason"]["notnull"] == 0


def test_primary_key_is_source_plus_source_ref(
    migrated_db: sqlite3.Connection,
) -> None:
    cols = _columns(migrated_db)
    assert cols["source"]["pk"] == 1
    assert cols["source_ref"]["pk"] == 2

    migrated_db.execute(
        "INSERT INTO deleted_transactions (source, source_ref, deleted_at, snapshot)"
        " VALUES ('binance', 'pay:1', '2026-09-03T00:00:00+00:00', '{}')"
    )
    with pytest.raises(sqlite3.IntegrityError):
        migrated_db.execute(
            "INSERT INTO deleted_transactions"
            " (source, source_ref, deleted_at, snapshot)"
            " VALUES ('binance', 'pay:1', '2026-09-03T00:00:01+00:00', '{}')"
        )


def test_same_ref_under_a_different_source_is_a_different_tombstone(
    migrated_db: sqlite3.Connection,
) -> None:
    """The key is the pair, exactly as ``UNIQUE(source, source_ref)`` is."""
    for source in ("binance", "provincial"):
        migrated_db.execute(
            "INSERT INTO deleted_transactions"
            " (source, source_ref, deleted_at, snapshot)"
            " VALUES (?, 'ref-1', '2026-09-03T00:00:00+00:00', '{}')",
            (source,),
        )
    count = migrated_db.execute(
        "SELECT COUNT(*) AS c FROM deleted_transactions"
    ).fetchone()["c"]
    assert count == 2


def test_migration_is_idempotent(
    migrated_db: sqlite3.Connection, tmp_path: Path
) -> None:
    """Re-running the runner applies nothing and keeps the tombstones."""
    migrated_db.execute(
        "INSERT INTO deleted_transactions (source, source_ref, deleted_at, snapshot)"
        " VALUES ('binance', 'pay:1', '2026-09-03T00:00:00+00:00', '{}')"
    )
    assert apply_migrations(migrated_db) == []
    survived = migrated_db.execute(
        "SELECT COUNT(*) AS c FROM deleted_transactions"
    ).fetchone()["c"]
    assert survived == 1
