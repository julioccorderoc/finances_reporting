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


# ---------------------------------------------------------------------------
# Task 2 — repo thread: insert/select/update/upsert.
# ---------------------------------------------------------------------------


def test_insert_and_get_round_trips_notes(
    seeded_db: sqlite3.Connection, transaction_factory
) -> None:
    txn = transaction_factory.build(account_id=1, notes="cash from abuela")
    saved = transactions_repo.insert(seeded_db, txn)
    assert saved.id is not None

    fetched = transactions_repo.get_by_id(seeded_db, saved.id)
    assert fetched is not None
    assert fetched.notes == "cash from abuela"


def test_list_by_account_carries_notes(
    seeded_db: sqlite3.Connection, transaction_factory
) -> None:
    transactions_repo.insert(
        seeded_db, transaction_factory.build(account_id=1, notes="visible in lists")
    )
    rows = transactions_repo.list_by_account(seeded_db, 1)
    assert any(t.notes == "visible in lists" for t in rows)


def test_update_sets_notes_only(
    seeded_db: sqlite3.Connection, transaction_factory
) -> None:
    saved = transactions_repo.insert(
        seeded_db, transaction_factory.build(account_id=1)
    )
    assert saved.id is not None

    updated = transactions_repo.update(seeded_db, id=saved.id, notes="ask Maria")

    assert updated.notes == "ask Maria"
    assert updated.category_id == saved.category_id  # untouched
    assert updated.user_rate == saved.user_rate      # untouched
    assert updated.amount == saved.amount            # untouched


def test_update_unset_leaves_notes_untouched(
    seeded_db: sqlite3.Connection, transaction_factory
) -> None:
    saved = transactions_repo.insert(
        seeded_db, transaction_factory.build(account_id=1, notes="keep me")
    )
    assert saved.id is not None

    updated = transactions_repo.update(seeded_db, id=saved.id, needs_review=True)
    assert updated.notes == "keep me"


def test_update_clears_notes_with_explicit_none(
    seeded_db: sqlite3.Connection, transaction_factory
) -> None:
    saved = transactions_repo.insert(
        seeded_db, transaction_factory.build(account_id=1, notes="obsolete")
    )
    assert saved.id is not None

    updated = transactions_repo.update(seeded_db, id=saved.id, notes=None)
    assert updated.notes is None


def test_upsert_reingest_never_wipes_manual_note(
    seeded_db: sqlite3.Connection, transaction_factory
) -> None:
    """MANDATORY (00-design.md §4): re-ingesting the same statement row over
    a manually-noted transaction keeps the note."""
    incoming = transaction_factory.build(
        account_id=1, source="provincial", source_ref="prov-note-1", notes=None
    )
    first = transactions_repo.upsert_by_source_ref(seeded_db, incoming)
    assert first["rows_inserted"] == 1

    transactions_repo.update(
        seeded_db, id=first["id"], notes="rent — split with Maria"
    )

    # Re-ingest: identical (source, source_ref); statements never carry notes.
    second = transactions_repo.upsert_by_source_ref(seeded_db, incoming)
    assert second["rows_inserted"] == 0
    assert second["rows_updated"] == 1

    after = transactions_repo.get_by_source_ref(
        seeded_db, "provincial", "prov-note-1"
    )
    assert after is not None
    assert after.notes == "rent — split with Maria"


def test_upsert_incoming_note_never_overwrites_existing_note(
    seeded_db: sqlite3.Connection, transaction_factory
) -> None:
    incoming = transaction_factory.build(
        account_id=1, source="provincial", source_ref="prov-note-2", notes=None
    )
    row = transactions_repo.upsert_by_source_ref(seeded_db, incoming)
    transactions_repo.update(seeded_db, id=row["id"], notes="manual wins")

    noisy = incoming.model_copy(update={"notes": "machine note"})
    transactions_repo.upsert_by_source_ref(seeded_db, noisy)

    after = transactions_repo.get_by_source_ref(
        seeded_db, "provincial", "prov-note-2"
    )
    assert after is not None
    assert after.notes == "manual wins"


def test_upsert_fills_note_when_row_has_none(
    seeded_db: sqlite3.Connection, transaction_factory
) -> None:
    """Pins the COALESCE clause itself: simply omitting notes from the
    UPDATE branch would pass the two tests above but fail this one."""
    bare = transaction_factory.build(
        account_id=1, source="provincial", source_ref="prov-note-3", notes=None
    )
    transactions_repo.upsert_by_source_ref(seeded_db, bare)

    with_note = bare.model_copy(update={"notes": "added on second import"})
    transactions_repo.upsert_by_source_ref(seeded_db, with_note)

    after = transactions_repo.get_by_source_ref(
        seeded_db, "provincial", "prov-note-3"
    )
    assert after is not None
    assert after.notes == "added on second import"
