"""WP3 — transaction notes on the web layer.

Plan: docs/plans/ux-overhaul/03-notes.md. Written before the
implementation per rule-011. Coverage:

* ``TransactionCard.notes`` projected by ``query_transactions``,
* the ``q`` free-text filter matches description OR notes,
* ``apply_edit`` / ``TransactionEditRequest`` set + clear notes (Task 4),
* PATCH /api/transactions/{id} notes round-trip (Task 4),
* modal partials render a prefilled ``notes`` textarea (Task 5),
* form-encoded edit endpoints persist notes (Task 5),
* card_transaction.html shows a note indicator + snippet (Task 5).
"""

from __future__ import annotations

import sqlite3
from datetime import date

from fastapi.testclient import TestClient

from finances.db.repos import transactions as transactions_repo


def _txn_id_by_source_ref(conn: sqlite3.Connection, source_ref: str) -> int:
    row = conn.execute(
        "SELECT id FROM transactions WHERE source_ref = ?", (source_ref,)
    ).fetchone()
    assert row is not None, f"seeded txn {source_ref} not present"
    return int(row["id"])


# ---------------------------------------------------------------------------
# Task 3 — card projection + q search.
# ---------------------------------------------------------------------------


def test_query_transactions_projects_notes(
    seeded_web_db: sqlite3.Connection,
) -> None:
    from finances.web.services.transactions_query import (
        TransactionsFilter,
        query_transactions,
    )

    txn_id = _txn_id_by_source_ref(seeded_web_db, "prov-1")
    transactions_repo.update(seeded_web_db, id=txn_id, notes="bodega tab settled")

    page = query_transactions(
        seeded_web_db, TransactionsFilter(date_from=date(2000, 1, 1))
    )
    card = next(c for c in page.rows if c.id == txn_id)
    assert card.notes == "bodega tab settled"


def test_q_filter_matches_notes(seeded_web_db: sqlite3.Connection) -> None:
    from finances.web.services.transactions_query import (
        TransactionsFilter,
        query_transactions,
    )

    txn_id = _txn_id_by_source_ref(seeded_web_db, "prov-2")
    transactions_repo.update(
        seeded_web_db, id=txn_id, notes="vacation fund with Maria"
    )

    page = query_transactions(
        seeded_web_db,
        TransactionsFilter(date_from=date(2000, 1, 1), q="vacation"),
    )
    assert [c.id for c in page.rows] == [txn_id]


def test_q_filter_still_matches_description(
    seeded_web_db: sqlite3.Connection,
) -> None:
    from finances.web.services.transactions_query import (
        TransactionsFilter,
        query_transactions,
    )

    txn_id = _txn_id_by_source_ref(seeded_web_db, "prov-1")
    page = query_transactions(
        seeded_web_db,
        TransactionsFilter(date_from=date(2000, 1, 1), q="bodega"),
    )
    assert [c.id for c in page.rows] == [txn_id]


def test_api_transactions_q_searches_notes(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    txn_id = _txn_id_by_source_ref(seeded_web_db, "prov-2")
    transactions_repo.update(seeded_web_db, id=txn_id, notes="vacation fund")

    client: TestClient = web_client_factory()
    resp = client.get(
        "/api/transactions", params={"q": "vacation", "date_from": "2000-01-01"}
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["total"] == 1
    assert body["rows"][0]["id"] == txn_id
    assert body["rows"][0]["notes"] == "vacation fund"
