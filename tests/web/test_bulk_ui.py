"""WP4 — bulk-select UI on /transactions (tests precede impl per rule-011).

Markup contract (JS behaviour is covered by the manual gate):
* per-row checkbox [data-bulk-checkbox] with the txn id as value,
* header select-all [data-bulk-select-all],
* the .flow-rows grid gains the is-selectable variant (checkbox track) —
  ``cards--selectable`` until 2026-09-03, renamed with the reskin,
* action bar #bulk-bar with the shared picker + [data-bulk-apply]
  posting to /api/transactions/bulk-edit,
* dashboard recent-activity cards stay checkbox-free (GUARD),
* the single-edit card swap keeps the checkbox cell so the subgrid row
  stays aligned after a modal save.
"""

from __future__ import annotations

import sqlite3


def _txn_id(conn: sqlite3.Connection, source_ref: str) -> int:
    row = conn.execute(
        "SELECT id FROM transactions WHERE source_ref = ?", (source_ref,)
    ).fetchone()
    assert row is not None
    return int(row["id"])


def test_transactions_page_has_bulk_bar_with_picker(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    client = web_client_factory()

    resp = client.get("/transactions", params={"date_from": "2000-01-01"})
    assert resp.status_code == 200
    body = resp.text

    assert 'id="bulk-bar"' in body
    assert "data-category-picker" in body
    assert "data-bulk-apply" in body
    assert "/api/transactions/bulk-edit" in body


def test_list_partial_rows_have_checkboxes(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    client = web_client_factory()

    resp = client.get(
        "/_partial/transactions/list", params={"date_from": "2000-01-01"}
    )
    assert resp.status_code == 200
    body = resp.text

    assert "data-bulk-checkbox" in body
    assert "data-bulk-select-all" in body
    assert '<div class="flow-rows is-selectable">' in body
    assert "cards--selectable" not in body


def test_checkbox_value_is_txn_id(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    client = web_client_factory()
    txn_id = _txn_id(seeded_web_db, "prov-1")

    body = client.get(
        "/_partial/transactions/list", params={"date_from": "2000-01-01"}
    ).text

    assert f'data-bulk-checkbox value="{txn_id}"' in body


def test_dashboard_cards_have_no_checkboxes(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    """GUARD: card_transaction.html stays checkbox-free outside /transactions."""
    client = web_client_factory()

    body = client.get("/").text

    assert "data-bulk-checkbox" not in body


def test_single_edit_card_swap_keeps_checkbox_cell(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    client = web_client_factory()
    txn_id = _txn_id(seeded_web_db, "prov-1")

    resp = client.post(
        f"/_partial/transactions/{txn_id}/edit",
        data={
            "set_category": "false",
            "category_id": "",
            "set_user_rate": "false",
            "user_rate": "",
        },
    )

    assert resp.status_code == 200, resp.text
    assert "data-bulk-checkbox" in resp.text
