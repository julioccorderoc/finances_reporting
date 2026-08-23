"""Bulk triage deferral — deciding not to categorise a period.

``transactions.parked`` (migration 015) is a durable "not now": the triage
queue skips it, and it is deliberately absent from
``upsert_by_source_ref``'s update list so a re-ingest cannot resurrect it.
Until now it could only be set one row at a time from the viewer.

That is fine for the row you are looking at and useless for the decision the
owner actually makes, which is about a *period*: "I am not going back through
2025." Pulling ten months of Binance history into the ledger turned that from
a preference into a blocker — the queue grew by rows the owner had already
decided, in advance, not to work through.

Parking is an answer to "do you want to categorise this?", and the answer is
no. Nothing here deletes or alters a transaction's money; a parked row still
counts in every balance and every report exactly as it did.
"""

from __future__ import annotations

import sqlite3
from datetime import datetime

SQL_PARK_BEFORE = """
    UPDATE transactions
       SET parked = 1, updated_at = CURRENT_TIMESTAMP
     WHERE parked = 0
       AND category_id IS NULL
       AND kind IN ('income', 'expense')
       AND occurred_at < :before
"""


def park_before(conn: sqlite3.Connection, *, before: datetime) -> int:
    """Park every still-outstanding income/expense row older than ``before``.

    Returns how many rows were parked. Re-running parks nothing further, so
    this is safe to repeat and safe to script.

    Scoped deliberately:

    * **Only uncategorised rows.** A categorised row is finished work, not
      outstanding work, and parking it would misrepresent a decision the
      owner already made.
    * **Only income and expense.** Transfers and adjustments are never
      asked about in the first place (rule-006, ADR-018), so parking them
      would be noise with no effect.
    """
    cursor = conn.execute(SQL_PARK_BEFORE, {"before": before.isoformat()})
    return cursor.rowcount


SQL_UNPARK_ALL = """
    UPDATE transactions
       SET parked = 0, updated_at = CURRENT_TIMESTAMP
     WHERE parked = 1
"""


def unpark_all(conn: sqlite3.Connection) -> int:
    """Return every parked row to the queue. Returns how many moved.

    The counterpart to :func:`park_before`, and the whole of the triage
    sheet's *Bring back all N*. There is no ordering to restore: the queue
    sorts itself by ``(bucket, occurred_at, item_id)``, so rows come back
    oldest-first by construction rather than by anything written here.

    Deliberately unscoped, unlike ``park_before``. Parking is a decision
    about a *period*; un-parking is the owner saying they want the backlog
    after all, and a filtered version of that would leave rows stranded
    with no way to name them.
    """
    return conn.execute(SQL_UNPARK_ALL).rowcount


__all__ = ["park_before", "unpark_all"]
