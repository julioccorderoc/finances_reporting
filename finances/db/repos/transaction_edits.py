"""Read-only repo for the ``transaction_edits`` audit trail (Wave 2 Thing 3).

Rows are inserted ONLY from inside
``finances.db.repos.transactions.update()`` — the single sanctioned write
path (rule-012). This module intentionally exposes no public insert: web
code must never write history directly, it edits a transaction and the
recording happens as a side effect of the update.
"""

from __future__ import annotations

import sqlite3

from finances.domain.models import TransactionEdit


def _row_to_edit(row: sqlite3.Row) -> TransactionEdit:
    return TransactionEdit(
        id=row["id"],
        transaction_id=row["transaction_id"],
        edited_at=row["edited_at"],
        field=row["field"],
        old_value=row["old_value"],
        new_value=row["new_value"],
    )


def list_for_transaction(
    conn: sqlite3.Connection, transaction_id: int
) -> list[TransactionEdit]:
    """Return every recorded edit for ``transaction_id``, newest first.

    ``edited_at`` has one-second granularity (SQLite ``CURRENT_TIMESTAMP``),
    so ``id DESC`` breaks ties to keep same-second edits in write order.
    """
    rows = conn.execute(
        """
        SELECT id, transaction_id, edited_at, field, old_value, new_value
        FROM transaction_edits
        WHERE transaction_id = ?
        ORDER BY edited_at DESC, id DESC
        """,
        (transaction_id,),
    ).fetchall()
    return [_row_to_edit(r) for r in rows]
