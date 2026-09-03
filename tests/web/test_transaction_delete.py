"""Deleting a row from the Flow modal (ADR-022 §2.4).

One control, in the modal footer, left of Cancel: a ghost **Delete**, a
confirm in plain words, then the row is gone from the ledger and from the
list, the modal closes and a toast says what went.

The write goes through ``transactions_write.delete_transaction`` →
``transactions_repo.delete`` (rule-012: the viewer runs no SQL of its
own), so the refusals the repo states — a paired row, a reconciliation
row — are the refusals the viewer shows, and they arrive as a toast
rather than a stack trace.
"""

from __future__ import annotations

import json
import sqlite3

import pytest
from fastapi.testclient import TestClient

from finances.db.repos import transactions as transactions_repo
from finances.format import fmt_native


def _txn_id(conn: sqlite3.Connection, source_ref: str) -> int:
    row = conn.execute(
        "SELECT id FROM transactions WHERE source_ref = ?", (source_ref,)
    ).fetchone()
    assert row is not None, f"seeded txn {source_ref} not present"
    return int(row["id"])


def _trigger(resp) -> dict:
    return json.loads(resp.headers.get("HX-Trigger", "{}"))


# ---------------------------------------------------------------------------
# The endpoint
# ---------------------------------------------------------------------------


def test_delete_removes_the_row(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    client: TestClient = web_client_factory()
    txn_id = _txn_id(seeded_web_db, "prov-1")

    resp = client.post(f"/_partial/transactions/{txn_id}/delete")

    assert resp.status_code == 200, resp.text
    assert transactions_repo.get_by_id(seeded_web_db, txn_id) is None


def test_delete_writes_the_tombstone(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    client: TestClient = web_client_factory()
    txn_id = _txn_id(seeded_web_db, "prov-1")

    client.post(f"/_partial/transactions/{txn_id}/delete")

    row = seeded_web_db.execute(
        "SELECT * FROM deleted_transactions WHERE source_ref = 'prov-1'"
    ).fetchone()
    assert row is not None
    assert row["source"] == "provincial"


def test_delete_asks_the_page_to_refresh_its_list(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    """The response swaps nothing; it says the list is stale.

    The modal opens from /transactions AND from the dashboard's recent
    activity, and htmx refuses to even SEND a request whose hx-target
    matches nothing — so a response shaped as "here is the new #tx-list"
    would make Delete a dead button on the dashboard. The row is removed
    by a follow-up refresh on whatever surface has a list, exactly as
    ``queueDirty`` already does for the triage queue.
    """
    client: TestClient = web_client_factory()
    txn_id = _txn_id(seeded_web_db, "prov-1")

    resp = client.post(f"/_partial/transactions/{txn_id}/delete")

    assert resp.text == ""
    assert _trigger(resp)["listDirty"] == {"id": txn_id}


def test_delete_closes_the_modal_and_toasts_what_went(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    client: TestClient = web_client_factory()
    txn_id = _txn_id(seeded_web_db, "prov-1")
    before = transactions_repo.get_by_id(seeded_web_db, txn_id)
    assert before is not None and before.description is not None

    trigger = _trigger(client.post(f"/_partial/transactions/{txn_id}/delete"))

    assert trigger.get("closeModal") is True
    message = trigger["toast"]["message"]
    assert trigger["toast"]["level"] == "success"
    assert message.startswith("Deleted")
    # Names the row, so an accidental delete is obvious at once.
    assert before.description[:20] in message
    # In the viewer's own money words (ADR-022 §2.1's "−Bs. 800.00"),
    # not a raw repr — this is the last time the owner sees the row.
    assert fmt_native(before.amount, before.currency, signed=True) in message


def test_the_refresh_carries_the_owner_s_filter(
    web_db: sqlite3.Connection, web_client_factory
) -> None:
    """The refresh reads the filter off the URL, the source of truth.

    Asserted on the rendered call, not on the intent: a refresh that
    dropped ``window.location.search`` would replace a filtered list with
    an unfiltered one and every server test would still pass.
    """
    with web_client_factory() as client:
        body = client.get("/transactions").text

    assert (
        "htmx.ajax('GET', '/_partial/transactions/list' + window.location.search, "
        "{ target: '#tx-list', swap: 'outerHTML' })" in body
    )


def test_delete_takes_a_reason(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    client: TestClient = web_client_factory()
    txn_id = _txn_id(seeded_web_db, "prov-1")

    client.post(
        f"/_partial/transactions/{txn_id}/delete", data={"reason": "twin of 859"}
    )

    row = seeded_web_db.execute(
        "SELECT reason FROM deleted_transactions WHERE source_ref = 'prov-1'"
    ).fetchone()
    assert row["reason"] == "twin of 859"


def test_unknown_id_is_404(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    client: TestClient = web_client_factory()

    assert client.post("/_partial/transactions/999999/delete").status_code == 404


def test_a_paired_row_is_refused_with_a_readable_message(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    """422 + the repo's words; the global htmx error listener toasts them."""
    client: TestClient = web_client_factory()
    txn_id = _txn_id(seeded_web_db, "prov-1")
    seeded_web_db.execute(
        "UPDATE transactions SET transfer_id = 'pair-1' WHERE id = ?", (txn_id,)
    )

    resp = client.post(f"/_partial/transactions/{txn_id}/delete")

    assert resp.status_code == 422
    assert "half of a transfer" in resp.json()["detail"]
    assert transactions_repo.get_by_id(seeded_web_db, txn_id) is not None


def test_an_opening_balance_row_is_refused(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    client: TestClient = web_client_factory()
    txn_id = _txn_id(seeded_web_db, "prov-1")
    seeded_web_db.execute(
        "UPDATE transactions SET source = 'opening_balance' WHERE id = ?",
        (txn_id,),
    )

    resp = client.post(f"/_partial/transactions/{txn_id}/delete")

    assert resp.status_code == 422
    assert transactions_repo.get_by_id(seeded_web_db, txn_id) is not None


def test_a_cash_row_deletes_without_a_tombstone(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    client: TestClient = web_client_factory()
    txn_id = _txn_id(seeded_web_db, "cash-1")

    resp = client.post(f"/_partial/transactions/{txn_id}/delete")

    assert resp.status_code == 200
    assert transactions_repo.get_by_id(seeded_web_db, txn_id) is None
    assert (
        seeded_web_db.execute(
            "SELECT COUNT(*) AS c FROM deleted_transactions"
        ).fetchone()["c"]
        == 0
    )


# ---------------------------------------------------------------------------
# The control
# ---------------------------------------------------------------------------


def test_modal_footer_has_a_ghost_delete_button(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    client: TestClient = web_client_factory()
    txn_id = _txn_id(seeded_web_db, "prov-1")

    body = client.get(f"/_partial/transactions/{txn_id}/modal").text

    assert f'hx-post="/_partial/transactions/{txn_id}/delete"' in body
    assert "data-modal-delete" in body
    assert "tbtn-ghost" in body
    # Targets itself and swaps nothing: the modal opens from two pages and
    # htmx will not send a request whose target is missing.
    assert 'hx-target="this"' in body
    assert 'hx-swap="none"' in body


def test_delete_sits_left_of_cancel(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    """ADR-022 §2.4 places it in the footer, before Cancel."""
    client: TestClient = web_client_factory()
    txn_id = _txn_id(seeded_web_db, "prov-1")

    body = client.get(f"/_partial/transactions/{txn_id}/modal").text
    footer = body[body.index("flow-modal-footer") :]

    assert footer.index("data-modal-delete") < footer.index(">Cancel<")


def test_delete_asks_first_in_the_adr_s_words(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    client: TestClient = web_client_factory()
    txn_id = _txn_id(seeded_web_db, "prov-1")

    body = client.get(f"/_partial/transactions/{txn_id}/modal").text

    assert (
        "hx-confirm=\"Delete this row from the ledger? It will not come back "
        "when the statement is imported again.\"" in body
    )


def test_a_surface_with_no_list_still_loses_the_row(
    web_db: sqlite3.Connection, web_client_factory
) -> None:
    """The dashboard shows cards with no #tx-list to refresh.

    ``listDirty`` carries the deleted id so that page can drop the card it
    is still showing, instead of displaying a row that no longer exists
    until someone happens to reload.
    """
    with web_client_factory() as client:
        body = client.get("/").text

    assert (
        "Array.from(document.querySelectorAll('article[data-tx-id]'))"
        in body
    )
    # And it must never build that selector with a literal double quote:
    # x-data is a double-quoted attribute, so one " there silently kills
    # Alpine for the whole page while every server test stays green.
    x_data = body[body.index("<body") : body.index("@close-modal.window")]
    assert 'data-tx-id="' not in x_data
