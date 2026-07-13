"""Wave 2 — Thing 3: edit history on the web layer.

Plan: docs/plans/wave2/03-edit-history.md. Written before the
implementation per rule-011. Coverage:

* bulk-edit N transactions records N history rows (the sanctioned
  ``transactions_repo.update()`` path covers bulk automatically),
* the transaction edit modal renders a collapsible History section with
  category ids resolved to category NAMES,
* the modal omits the History section entirely when a txn has no edits.
"""

from __future__ import annotations

import sqlite3

from fastapi.testclient import TestClient

from finances.db.repos import categories as categories_repo
from finances.db.repos import transactions as transactions_repo
from finances.domain.models import TransactionKind


def _txn_id_by_source_ref(conn: sqlite3.Connection, source_ref: str) -> int:
    row = conn.execute(
        "SELECT id FROM transactions WHERE source_ref = ?", (source_ref,)
    ).fetchone()
    assert row is not None, f"seeded txn {source_ref} not present"
    return int(row["id"])


def _other_expense_category(conn: sqlite3.Connection, *, not_named: str):
    cat = next(
        c
        for c in categories_repo.list_all(conn)
        if c.kind == TransactionKind.EXPENSE and c.name != not_named
    )
    return cat


def test_bulk_edit_records_one_history_row_per_transaction(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    ids = [
        _txn_id_by_source_ref(seeded_web_db, ref)
        for ref in ("prov-1", "prov-2", "prov-3")
    ]
    target = _other_expense_category(seeded_web_db, not_named="Groceries")

    client: TestClient = web_client_factory()
    resp = client.post(
        "/api/transactions/bulk-edit",
        json={"ids": ids, "category_id": target.id},
    )
    assert resp.status_code == 200, resp.text
    assert resp.json()["updated"] == 3

    total = seeded_web_db.execute(
        "SELECT COUNT(*) AS c FROM transaction_edits WHERE field = 'category_id'"
    ).fetchone()["c"]
    assert total == 3


def test_modal_renders_history_with_resolved_category_names(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    txn_id = _txn_id_by_source_ref(seeded_web_db, "prov-1")  # currently Groceries
    target = _other_expense_category(seeded_web_db, not_named="Groceries")

    transactions_repo.update(seeded_web_db, id=txn_id, category_id=target.id)

    client: TestClient = web_client_factory()
    resp = client.get(f"/_partial/transactions/{txn_id}/modal")
    assert resp.status_code == 200, resp.text
    body = resp.text

    assert "History (1)" in body
    # Both endpoints of the change are shown as NAMES, not raw ids.
    assert f"Groceries → {target.name}" in body
    # The raw ids must not leak as the arrow value.
    assert f"→ {target.id}" not in body


def test_modal_omits_history_section_when_no_edits(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    txn_id = _txn_id_by_source_ref(seeded_web_db, "cash-1")  # never edited

    client: TestClient = web_client_factory()
    resp = client.get(f"/_partial/transactions/{txn_id}/modal")
    assert resp.status_code == 200, resp.text
    assert "History (" not in resp.text
