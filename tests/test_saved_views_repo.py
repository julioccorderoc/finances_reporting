"""Wave 2 Thing 2 — saved filter views: schema, model, and repo thread.

Plan: docs/plans/wave2/02-saved-views.md. Per rule-011 these tests land
before the implementation. This file covers the DB side:

* migration 009_saved_views.sql creates the ``saved_views`` table,
* ``SavedView`` Pydantic model (id/name/query_string/created_at),
* repo round-trip (insert / list_all / get_by_id / delete),
* UNIQUE(name) violation surfaces as ``sqlite3.IntegrityError``.
"""

from __future__ import annotations

import sqlite3

import pytest

from finances.db.repos import saved_views as saved_views_repo
from finances.domain.models import SavedView


# ---------------------------------------------------------------------------
# Task 1 — schema + model.
# ---------------------------------------------------------------------------


def test_saved_views_table_exists_with_expected_columns(
    in_memory_db: sqlite3.Connection,
) -> None:
    cols = {
        row["name"]
        for row in in_memory_db.execute("PRAGMA table_info(saved_views)").fetchall()
    }
    assert cols == {"id", "name", "query_string", "created_at"}


def test_saved_views_name_is_unique_at_schema_level(
    in_memory_db: sqlite3.Connection,
) -> None:
    in_memory_db.execute(
        "INSERT INTO saved_views (name, query_string) VALUES (?, ?)",
        ("Groceries VES", "kinds=expense&currencies=VES"),
    )
    with pytest.raises(sqlite3.IntegrityError):
        in_memory_db.execute(
            "INSERT INTO saved_views (name, query_string) VALUES (?, ?)",
            ("Groceries VES", "q=other"),
        )


def test_saved_view_model_defaults() -> None:
    view = SavedView(name="Needs review", query_string="needs_review=yes")
    assert view.id is None
    assert view.name == "Needs review"
    assert view.query_string == "needs_review=yes"
    assert view.created_at is None


# ---------------------------------------------------------------------------
# Task 2 — repo round-trip (Pydantic in/out only, per ADR-009).
# ---------------------------------------------------------------------------


def test_insert_returns_model_with_id(in_memory_db: sqlite3.Connection) -> None:
    saved = saved_views_repo.insert(
        in_memory_db,
        SavedView(name="This month expenses", query_string="kinds=expense"),
    )
    assert isinstance(saved, SavedView)
    assert saved.id is not None
    assert saved.name == "This month expenses"


def test_get_by_id_round_trips(in_memory_db: sqlite3.Connection) -> None:
    qs = "date_from=2026-07-01&date_to=2026-07-31&kinds=expense&kinds=income"
    saved = saved_views_repo.insert(
        in_memory_db, SavedView(name="July", query_string=qs)
    )
    assert saved.id is not None

    fetched = saved_views_repo.get_by_id(in_memory_db, saved.id)
    assert fetched is not None
    assert isinstance(fetched, SavedView)
    assert fetched.id == saved.id
    assert fetched.name == "July"
    assert fetched.query_string == qs


def test_get_by_id_missing_returns_none(in_memory_db: sqlite3.Connection) -> None:
    assert saved_views_repo.get_by_id(in_memory_db, 9999) is None


def test_list_all_returns_models_ordered_by_name(
    in_memory_db: sqlite3.Connection,
) -> None:
    saved_views_repo.insert(in_memory_db, SavedView(name="Zeta", query_string="q=z"))
    saved_views_repo.insert(in_memory_db, SavedView(name="Alpha", query_string="q=a"))
    saved_views_repo.insert(in_memory_db, SavedView(name="Mid", query_string="q=m"))

    views = saved_views_repo.list_all(in_memory_db)
    assert [v.name for v in views] == ["Alpha", "Mid", "Zeta"]
    assert all(isinstance(v, SavedView) for v in views)
    assert all(v.id is not None for v in views)


def test_list_all_empty_db_returns_empty_list(
    in_memory_db: sqlite3.Connection,
) -> None:
    assert saved_views_repo.list_all(in_memory_db) == []


def test_delete_removes_row_and_reports_outcome(
    in_memory_db: sqlite3.Connection,
) -> None:
    saved = saved_views_repo.insert(
        in_memory_db, SavedView(name="Doomed", query_string="q=x")
    )
    assert saved.id is not None

    assert saved_views_repo.delete(in_memory_db, saved.id) is True
    assert saved_views_repo.get_by_id(in_memory_db, saved.id) is None
    # Second delete of the same id: nothing left to remove.
    assert saved_views_repo.delete(in_memory_db, saved.id) is False


def test_insert_duplicate_name_raises_integrity_error(
    in_memory_db: sqlite3.Connection,
) -> None:
    saved_views_repo.insert(in_memory_db, SavedView(name="Dup", query_string="q=1"))
    with pytest.raises(sqlite3.IntegrityError):
        saved_views_repo.insert(
            in_memory_db, SavedView(name="Dup", query_string="q=2")
        )
