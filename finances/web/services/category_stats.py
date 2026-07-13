"""Category usage statistics for the shared picker (UX overhaul WP4).

``top_categories`` powers the top-8 chips in
``partials/category_picker.html``. Ranking = usage count over a trailing
window of calendar months; when history is thin the remainder is padded
with active categories in seed (id) order so chips are never empty on a
fresh DB.

Read-only module — SELECTs only (rule-012 governs writes, which stay in
``transactions_repo.update``).
"""

from __future__ import annotations

import sqlite3
from datetime import date

from finances.domain.models import Category, TransactionKind


def _cutoff_iso(months: int, *, today: date | None = None) -> str:
    """First day of the month ``months`` calendar months before today.

    Returned as an ISO ``YYYY-MM-DD`` string. ``occurred_at`` is stored
    as ISO text, so lexicographic ``>=`` comparison is correct.
    """
    anchor = today or date.today()
    total = anchor.year * 12 + (anchor.month - 1) - months
    year, month0 = divmod(total, 12)
    return date(year, month0 + 1, 1).isoformat()


def _row_to_category(row: sqlite3.Row) -> Category:
    return Category(
        id=row["id"],
        kind=TransactionKind(row["kind"]),
        name=row["name"],
        active=bool(row["active"]),
    )


def top_categories(
    conn: sqlite3.Connection,
    kind: TransactionKind | str | None = None,
    limit: int = 8,
    months: int = 12,
) -> list[Category]:
    """Most-used active categories over the trailing ``months`` window.

    Ordered by usage count (desc; ties broken by id = seed order). When
    fewer than ``limit`` categories have any usage in the window, the
    list is padded with the remaining active categories in seed (id)
    order. ``kind`` filters to one ``TransactionKind`` (enum or plain
    string); ``None`` mixes all kinds (used by the bulk action bar).
    """
    kind_value = kind.value if isinstance(kind, TransactionKind) else kind

    params: list[object] = [_cutoff_iso(months)]
    kind_sql = ""
    if kind_value is not None:
        kind_sql = "AND c.kind = ?"
        params.append(kind_value)
    params.append(limit)

    ranked_rows = conn.execute(
        f"""
        SELECT c.id AS id, c.kind AS kind, c.name AS name, c.active AS active,
               COUNT(t.id) AS uses
        FROM categories c
        JOIN transactions t ON t.category_id = c.id
        WHERE c.active = 1
          AND t.occurred_at >= ?
          {kind_sql}
        GROUP BY c.id
        ORDER BY uses DESC, c.id ASC
        LIMIT ?
        """,
        params,
    ).fetchall()
    result = [_row_to_category(r) for r in ranked_rows]

    if len(result) < limit:
        seen = {c.id for c in result}
        pad_sql = "SELECT id, kind, name, active FROM categories WHERE active = 1"
        pad_params: list[object] = []
        if kind_value is not None:
            pad_sql += " AND kind = ?"
            pad_params.append(kind_value)
        pad_sql += " ORDER BY id ASC"
        for row in conn.execute(pad_sql, pad_params):
            if row["id"] in seen:
                continue
            result.append(_row_to_category(row))
            if len(result) >= limit:
                break

    return result


__all__ = ["top_categories"]
