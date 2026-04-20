from __future__ import annotations

import sqlite3

from finances.domain.models import Category, TransactionKind


def _row_to_category(row: sqlite3.Row) -> Category:
    return Category(
        id=row["id"],
        kind=TransactionKind(row["kind"]),
        name=row["name"],
        active=bool(row["active"]),
    )


def insert(conn: sqlite3.Connection, category: Category) -> Category:
    cur = conn.execute(
        "INSERT INTO categories (kind, name, active) VALUES (?, ?, ?)",
        (category.kind.value, category.name, 1 if category.active else 0),
    )
    return category.model_copy(update={"id": cur.lastrowid})


def get_by_id(conn: sqlite3.Connection, category_id: int) -> Category | None:
    row = conn.execute(
        "SELECT id, kind, name, active FROM categories WHERE id = ?",
        (category_id,),
    ).fetchone()
    return _row_to_category(row) if row else None


def get_by_name(
    conn: sqlite3.Connection, kind: TransactionKind | str, name: str
) -> Category | None:
    kind_value = kind.value if isinstance(kind, TransactionKind) else kind
    row = conn.execute(
        "SELECT id, kind, name, active FROM categories WHERE kind = ? AND name = ?",
        (kind_value, name),
    ).fetchone()
    return _row_to_category(row) if row else None


def list_all(conn: sqlite3.Connection, *, include_inactive: bool = False) -> list[Category]:
    if include_inactive:
        rows = conn.execute(
            "SELECT id, kind, name, active FROM categories ORDER BY kind, name"
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT id, kind, name, active FROM categories WHERE active = 1 ORDER BY kind, name"
        ).fetchall()
    return [_row_to_category(r) for r in rows]
