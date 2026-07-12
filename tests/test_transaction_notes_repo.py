"""WP3 — transaction notes: schema, model, and repo thread.

Plan: docs/plans/ux-overhaul/03-notes.md. Per rule-011 these tests land
before the implementation. This file covers the DB side:

* migration 008_add_transaction_notes.sql adds a nullable ``notes`` column,
* ``Transaction.notes`` Pydantic field (default ``None``),
* repo round-trip (insert / get_by_id / get_by_source_ref / list_by_account),
* ``update(notes=...)`` via the ``_UNSET`` sentinel,
* ``upsert_by_source_ref`` NEVER overwrites an existing manual note on
  re-ingest (same enrichment-preservation contract as category_id/user_rate).
"""

from __future__ import annotations

import sqlite3

from finances.db.repos import transactions as transactions_repo


# ---------------------------------------------------------------------------
# Task 1 — schema + model.
# ---------------------------------------------------------------------------


def test_transactions_table_has_notes_column(
    in_memory_db: sqlite3.Connection,
) -> None:
    cols = {
        row["name"]
        for row in in_memory_db.execute("PRAGMA table_info(transactions)").fetchall()
    }
    assert "notes" in cols


def test_transaction_model_accepts_notes_and_defaults_to_none(
    transaction_factory,
) -> None:
    with_note = transaction_factory.build(notes="split with Maria")
    assert with_note.notes == "split with Maria"

    without_note = transaction_factory.build(notes=None)
    assert without_note.notes is None
