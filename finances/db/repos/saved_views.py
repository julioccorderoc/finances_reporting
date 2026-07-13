"""Saved /transactions filter views (Wave 2 Thing 2).

Accepts and returns Pydantic ``SavedView`` models only (ADR-009).
``query_string`` is the raw querystring without the leading ``?``; the
web layer owns normalization and duplicate-name error handling (the
UNIQUE(name) violation propagates as ``sqlite3.IntegrityError``).
"""

from __future__ import annotations

import sqlite3

from finances.domain.models import SavedView


def _row_to_saved_view(row: sqlite3.Row) -> SavedView:
    return SavedView(
        id=row["id"],
        name=row["name"],
        query_string=row["query_string"],
    )


def insert(conn: sqlite3.Connection, view: SavedView) -> SavedView:
    cur = conn.execute(
        "INSERT INTO saved_views (name, query_string) VALUES (?, ?)",
        (view.name, view.query_string),
    )
    return view.model_copy(update={"id": cur.lastrowid})


def get_by_id(conn: sqlite3.Connection, view_id: int) -> SavedView | None:
    row = conn.execute(
        "SELECT id, name, query_string FROM saved_views WHERE id = ?",
        (view_id,),
    ).fetchone()
    return _row_to_saved_view(row) if row else None


def list_all(conn: sqlite3.Connection) -> list[SavedView]:
    rows = conn.execute(
        "SELECT id, name, query_string FROM saved_views ORDER BY name"
    ).fetchall()
    return [_row_to_saved_view(r) for r in rows]


def delete(conn: sqlite3.Connection, view_id: int) -> bool:
    cur = conn.execute("DELETE FROM saved_views WHERE id = ?", (view_id,))
    return cur.rowcount > 0
