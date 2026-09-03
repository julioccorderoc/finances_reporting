from __future__ import annotations

import sqlite3

from finances.domain.models import Category, TransactionKind


_PICKER_COLUMNS = "auto_only, chip_eligible, icon"


def _row_to_category(row: sqlite3.Row) -> Category:
    """Project a row into ``Category``.

    The picker columns (migration 021) are optional in the SELECT: the
    older callers here list their columns by hand and do not need them,
    so their absence falls back to the model defaults rather than
    forcing every query to widen.
    """
    keys = row.keys()
    return Category(
        id=row["id"],
        kind=TransactionKind(row["kind"]),
        name=row["name"],
        active=bool(row["active"]),
        auto_only=bool(row["auto_only"]) if "auto_only" in keys else False,
        chip_eligible=bool(row["chip_eligible"]) if "chip_eligible" in keys else True,
        icon=row["icon"] if "icon" in keys else None,
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


def list_for_kind(
    conn: sqlite3.Connection,
    kind: TransactionKind | str,
    *,
    include_inactive: bool = False,
) -> list[Category]:
    """Categories a transaction of ``kind`` may legitimately carry.

    Its own kind, plus every transfer-kind category. The second half is not
    a loophole: a transfer category on an income or expense row is how the
    owner declares "this money moved, it was not spent", which
    ``finances.domain.money`` acts on when deciding what counts as spending.

    Applying an *expense* category to an income row (or the reverse) is
    never meaningful, and this is what keeps the picker from offering it.
    """
    kind_value = kind.value if isinstance(kind, TransactionKind) else kind
    transfer = TransactionKind.TRANSFER.value
    sql = "SELECT id, kind, name, active FROM categories WHERE kind IN (?, ?)"
    if not include_inactive:
        sql += " AND active = 1"
    sql += " ORDER BY kind, name"
    rows = conn.execute(sql, (kind_value, transfer)).fetchall()
    return [_row_to_category(r) for r in rows]


def list_pickable(conn: sqlite3.Connection) -> list[Category]:
    """Every category a human may choose by hand.

    ``active = 1 AND auto_only = 0`` (migration 021) — not retired, and
    not something the system writes for itself. This is the one place
    that definition lives; picker surfaces read it rather than
    re-deriving it, so "why is Fees in the list but not on a chip?" has a
    single answer in the data.

    Adjustment categories and ``Interest`` are ``auto_only`` and never
    come back here. The two transfer categories DO, since migration 022
    (owner decision 2026-09-03): a transfer-kind tag on an income or
    expense row is the owner saying "this moved, it was not spent", the
    write path has always accepted it, and money that enters
    transitionally has to be filable from the queue. They stay
    ``chip_eligible = 0`` so the pairing that writes ``Internal Transfer``
    constantly never ranks it onto a number key.
    """
    rows = conn.execute(
        f"SELECT id, kind, name, active, {_PICKER_COLUMNS} FROM categories"
        " WHERE active = 1 AND auto_only = 0 ORDER BY kind, name"
    ).fetchall()
    return [_row_to_category(r) for r in rows]


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
