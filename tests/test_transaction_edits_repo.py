"""Wave 2 — Thing 3: edit history (DB + repo layer).

Plan: docs/plans/wave2/03-edit-history.md. Per rule-011 these tests land
before the implementation. Coverage:

* migration 009 adds ``transaction_edits`` (schema + CHECK + cascade),
* ``transactions_repo.update()`` records ONE row per field that actually
  CHANGED (category_id / user_rate / notes); passed-but-equal values and
  ``needs_review`` record nothing,
* ingest paths (``insert`` / ``upsert_by_source_ref``) record nothing,
* ``TransactionEdit`` Pydantic model,
* ``transaction_edits_repo.list_for_transaction`` — newest first.
"""

from __future__ import annotations

import sqlite3
from decimal import Decimal

from finances.db.repos import categories as categories_repo
from finances.db.repos import transaction_edits as edits_repo
from finances.db.repos import transactions as transactions_repo
from finances.domain.models import TransactionKind


# ---------------------------------------------------------------------------
# Helpers.
# ---------------------------------------------------------------------------


def _edit_rows(conn: sqlite3.Connection, txn_id: int) -> list[sqlite3.Row]:
    return conn.execute(
        """
        SELECT field, old_value, new_value
        FROM transaction_edits
        WHERE transaction_id = ?
        ORDER BY id
        """,
        (txn_id,),
    ).fetchall()


def _two_expense_categories(conn: sqlite3.Connection):
    cats = [
        c
        for c in categories_repo.list_all(conn)
        if c.kind == TransactionKind.EXPENSE
    ]
    assert len(cats) >= 2, "seed taxonomy must have >= 2 expense categories"
    return cats[0], cats[1]


# ---------------------------------------------------------------------------
# Schema.
# ---------------------------------------------------------------------------


def test_transaction_edits_table_has_expected_columns(
    in_memory_db: sqlite3.Connection,
) -> None:
    cols = {
        row["name"]
        for row in in_memory_db.execute(
            "PRAGMA table_info(transaction_edits)"
        ).fetchall()
    }
    assert cols == {
        "id",
        "transaction_id",
        "edited_at",
        "field",
        "old_value",
        "new_value",
    }


def test_field_check_constraint_rejects_unknown_field(
    seeded_db: sqlite3.Connection, transaction_factory
) -> None:
    txn = transactions_repo.insert(seeded_db, transaction_factory.build(account_id=1))
    try:
        seeded_db.execute(
            "INSERT INTO transaction_edits (transaction_id, field, old_value, new_value)"
            " VALUES (?, 'needs_review', '0', '1')",
            (txn.id,),
        )
    except sqlite3.IntegrityError:
        return
    raise AssertionError("CHECK(field IN (...)) did not reject 'needs_review'")


# ---------------------------------------------------------------------------
# Recording in update().
# ---------------------------------------------------------------------------


def test_single_field_edit_records_exactly_one_row(
    seeded_db: sqlite3.Connection, transaction_factory
) -> None:
    cat_a, _ = _two_expense_categories(seeded_db)
    txn = transactions_repo.insert(
        seeded_db, transaction_factory.build(account_id=1, category_id=None)
    )

    transactions_repo.update(seeded_db, id=txn.id, category_id=cat_a.id)

    rows = _edit_rows(seeded_db, txn.id)
    assert len(rows) == 1
    assert rows[0]["field"] == "category_id"
    assert rows[0]["old_value"] is None
    assert rows[0]["new_value"] == str(cat_a.id)


def test_multi_field_edit_records_one_row_per_changed_field(
    seeded_db: sqlite3.Connection, transaction_factory
) -> None:
    cat_a, _ = _two_expense_categories(seeded_db)
    txn = transactions_repo.insert(
        seeded_db,
        transaction_factory.build(
            account_id=1, category_id=None, user_rate=None, notes=None
        ),
    )

    transactions_repo.update(
        seeded_db,
        id=txn.id,
        category_id=cat_a.id,
        user_rate=Decimal("36.50"),
        notes="split with Maria",
    )

    rows = _edit_rows(seeded_db, txn.id)
    assert len(rows) == 3
    assert {r["field"] for r in rows} == {"category_id", "user_rate", "notes"}
    by_field = {r["field"]: r for r in rows}
    assert by_field["user_rate"]["new_value"] == "36.50"
    assert by_field["notes"]["new_value"] == "split with Maria"


def test_noop_update_records_nothing(
    seeded_db: sqlite3.Connection, transaction_factory
) -> None:
    cat_a, _ = _two_expense_categories(seeded_db)
    txn = transactions_repo.insert(
        seeded_db,
        transaction_factory.build(
            account_id=1,
            category_id=cat_a.id,
            user_rate=Decimal("10"),
            notes="unchanged",
        ),
    )

    transactions_repo.update(
        seeded_db,
        id=txn.id,
        category_id=cat_a.id,
        user_rate=Decimal("10"),
        notes="unchanged",
    )

    assert _edit_rows(seeded_db, txn.id) == []


def test_equal_but_differently_scaled_rate_records_nothing(
    seeded_db: sqlite3.Connection, transaction_factory
) -> None:
    """``Decimal("36.0") == Decimal("36.00")`` — same value, no history row."""
    txn = transactions_repo.insert(
        seeded_db,
        transaction_factory.build(account_id=1, user_rate=Decimal("36.0")),
    )

    transactions_repo.update(seeded_db, id=txn.id, user_rate=Decimal("36.00"))

    assert _edit_rows(seeded_db, txn.id) == []


def test_needs_review_update_records_nothing(
    seeded_db: sqlite3.Connection, transaction_factory
) -> None:
    txn = transactions_repo.insert(
        seeded_db, transaction_factory.build(account_id=1, needs_review=False)
    )

    transactions_repo.update(seeded_db, id=txn.id, needs_review=True)

    assert _edit_rows(seeded_db, txn.id) == []


def test_clearing_a_field_records_none_new_value(
    seeded_db: sqlite3.Connection, transaction_factory
) -> None:
    txn = transactions_repo.insert(
        seeded_db, transaction_factory.build(account_id=1, notes="obsolete")
    )

    transactions_repo.update(seeded_db, id=txn.id, notes=None)

    rows = _edit_rows(seeded_db, txn.id)
    assert len(rows) == 1
    assert rows[0]["field"] == "notes"
    assert rows[0]["old_value"] == "obsolete"
    assert rows[0]["new_value"] is None


# ---------------------------------------------------------------------------
# Ingest paths record nothing.
# ---------------------------------------------------------------------------


def test_insert_records_no_history(
    seeded_db: sqlite3.Connection, transaction_factory
) -> None:
    cat_a, _ = _two_expense_categories(seeded_db)
    txn = transactions_repo.insert(
        seeded_db,
        transaction_factory.build(
            account_id=1,
            category_id=cat_a.id,
            user_rate=Decimal("5"),
            notes="from statement",
        ),
    )
    assert _edit_rows(seeded_db, txn.id) == []


def test_upsert_reingest_records_no_history(
    seeded_db: sqlite3.Connection, transaction_factory
) -> None:
    incoming = transaction_factory.build(
        account_id=1, source="provincial", source_ref="edit-hist-1", category_id=None
    )
    first = transactions_repo.upsert_by_source_ref(seeded_db, incoming)
    transactions_repo.upsert_by_source_ref(seeded_db, incoming)

    assert _edit_rows(seeded_db, first["id"]) == []


# ---------------------------------------------------------------------------
# Cascade.
# ---------------------------------------------------------------------------


def test_deleting_transaction_cascades_to_edits(
    seeded_db: sqlite3.Connection, transaction_factory
) -> None:
    txn = transactions_repo.insert(
        seeded_db, transaction_factory.build(account_id=1, notes=None)
    )
    transactions_repo.update(seeded_db, id=txn.id, notes="will be orphaned")
    assert len(_edit_rows(seeded_db, txn.id)) == 1

    seeded_db.execute("DELETE FROM transactions WHERE id = ?", (txn.id,))

    assert _edit_rows(seeded_db, txn.id) == []


# ---------------------------------------------------------------------------
# Repo: list_for_transaction (newest first) + model type.
# ---------------------------------------------------------------------------


def test_list_for_transaction_returns_models_newest_first(
    seeded_db: sqlite3.Connection, transaction_factory
) -> None:
    from finances.domain.models import TransactionEdit

    cat_a, cat_b = _two_expense_categories(seeded_db)
    txn = transactions_repo.insert(
        seeded_db, transaction_factory.build(account_id=1, category_id=None)
    )

    transactions_repo.update(seeded_db, id=txn.id, category_id=cat_a.id)  # 1st
    transactions_repo.update(seeded_db, id=txn.id, category_id=cat_b.id)  # 2nd

    edits = edits_repo.list_for_transaction(seeded_db, txn.id)
    assert len(edits) == 2
    assert all(isinstance(e, TransactionEdit) for e in edits)

    # Newest first: the cat_a -> cat_b change precedes the None -> cat_a one.
    assert edits[0].field == "category_id"
    assert edits[0].transaction_id == txn.id
    assert edits[0].old_value == str(cat_a.id)
    assert edits[0].new_value == str(cat_b.id)
    assert edits[0].edited_at is not None
    assert edits[1].old_value is None
    assert edits[1].new_value == str(cat_a.id)


def test_list_for_transaction_empty_when_no_edits(
    seeded_db: sqlite3.Connection, transaction_factory
) -> None:
    txn = transactions_repo.insert(
        seeded_db, transaction_factory.build(account_id=1)
    )
    assert edits_repo.list_for_transaction(seeded_db, txn.id) == []
