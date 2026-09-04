"""«Became cash» and «Unpair» in the Flow modal footer.

One slot, two verbs. On an unpaired outgoing row the footer offers **Became
cash…**; press it and a single field asks how many dollars came back. On a
row that is already a transfer leg the same slot offers **Unpair**, which
puts both rows back the way they were.

They are one control because they are one question — *what is the other side
of this row, and can I take it back?* — and because a Became cash with no way
back would leave the row showing a Delete that refuses with a sentence about
breaking a pair nothing could break (ADR-022 §2.3).

Every write goes through the domain (``cash_conversion.convert_to_cash``,
``transfers.unpair``); the router runs no SQL of its own, so the refusals the
domain states are the refusals the viewer shows.
"""

from __future__ import annotations

import json
import sqlite3
from decimal import Decimal

from fastapi.testclient import TestClient

from finances.db.repos import transactions as transactions_repo
from finances.domain import cash_conversion


def _txn_id(conn: sqlite3.Connection, source_ref: str) -> int:
    row = conn.execute(
        "SELECT id FROM transactions WHERE source_ref = ?", (source_ref,)
    ).fetchone()
    assert row is not None, f"seeded txn {source_ref} not present"
    return int(row["id"])


def _trigger(resp) -> dict:
    return json.loads(resp.headers.get("HX-Trigger", "{}"))


def _modal(client: TestClient, txn_id: int) -> str:
    resp = client.get(f"/_partial/transactions/{txn_id}/modal")
    assert resp.status_code == 200, resp.text
    return resp.text


# ---------------------------------------------------------------------------
# The footer slot
# ---------------------------------------------------------------------------


def test_an_unpaired_outgoing_row_offers_became_cash(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    client: TestClient = web_client_factory()

    html = _modal(client, _txn_id(seeded_web_db, "prov-1"))

    assert "Became cash" in html
    assert "Unpair" not in html


def test_an_incoming_row_offers_neither(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    """Money arriving is the other half of a conversion, never its origin."""
    client: TestClient = web_client_factory()

    html = _modal(client, _txn_id(seeded_web_db, "prov-4"))

    assert "Became cash" not in html
    assert "Unpair" not in html


def test_a_cash_row_offers_neither(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    """Cash into cash moves nothing."""
    client: TestClient = web_client_factory()

    html = _modal(client, _txn_id(seeded_web_db, "cash-1"))

    assert "Became cash" not in html


def test_a_paired_row_offers_unpair_instead(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    client: TestClient = web_client_factory()
    anchor_id = _txn_id(seeded_web_db, "prov-1")
    cash_conversion.convert_to_cash(
        seeded_web_db, transaction_id=anchor_id, usd_received=Decimal("40")
    )

    html = _modal(client, anchor_id)

    assert "Unpair" in html
    assert "Became cash" not in html


# ---------------------------------------------------------------------------
# The panel — one field, pre-filled
# ---------------------------------------------------------------------------


def test_the_panel_asks_for_dollars_received(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    client: TestClient = web_client_factory()
    txn_id = _txn_id(seeded_web_db, "prov-1")

    resp = client.get(f"/_partial/transactions/{txn_id}/became-cash")

    assert resp.status_code == 200, resp.text
    assert 'name="usd_received"' in resp.text


def test_the_panel_prefills_the_rows_own_usd_value(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    """The best guess available: what the ledger already prices the row at."""
    client: TestClient = web_client_factory()
    txn_id = _txn_id(seeded_web_db, "prov-3")  # -3650 VES at user_rate 36.0

    resp = client.get(f"/_partial/transactions/{txn_id}/became-cash")

    assert 'value="101.39"' in resp.text


def test_the_panel_refuses_a_row_that_cannot_convert(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    client: TestClient = web_client_factory()
    txn_id = _txn_id(seeded_web_db, "prov-4")  # income

    resp = client.get(f"/_partial/transactions/{txn_id}/became-cash")

    assert resp.status_code == 422


# ---------------------------------------------------------------------------
# Recording it
# ---------------------------------------------------------------------------


def test_recording_pairs_the_row_with_a_cash_leg(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    client: TestClient = web_client_factory()
    txn_id = _txn_id(seeded_web_db, "prov-1")

    resp = client.post(
        f"/_partial/transactions/{txn_id}/became-cash", data={"usd_received": "40"}
    )

    assert resp.status_code == 200, resp.text
    anchor = transactions_repo.get_by_id(seeded_web_db, txn_id)
    assert anchor is not None
    assert anchor.transfer_id is not None


def test_recording_closes_the_modal_and_refreshes_the_list(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    """Same three events Delete sends: the modal opens from two pages."""
    client: TestClient = web_client_factory()
    txn_id = _txn_id(seeded_web_db, "prov-1")

    resp = client.post(
        f"/_partial/transactions/{txn_id}/became-cash", data={"usd_received": "40"}
    )

    trigger = _trigger(resp)
    assert "closeModal" in trigger
    assert "listDirty" in trigger


def test_the_toast_names_the_dollars_and_the_struck_rate(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    client: TestClient = web_client_factory()
    txn_id = _txn_id(seeded_web_db, "prov-1")  # -365.00 VES

    resp = client.post(
        f"/_partial/transactions/{txn_id}/became-cash", data={"usd_received": "40"}
    )

    message = _trigger(resp)["toast"]["message"]
    assert "$40" in message
    assert "9.1250" in message  # 365 / 40, quote units per dollar


def test_recording_refuses_a_bad_amount_with_the_domains_words(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    client: TestClient = web_client_factory()
    txn_id = _txn_id(seeded_web_db, "prov-1")

    resp = client.post(
        f"/_partial/transactions/{txn_id}/became-cash", data={"usd_received": "0"}
    )

    assert resp.status_code == 422
    assert "dollars received must be positive" in resp.text


def test_recording_refuses_junk_in_the_amount_field(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    client: TestClient = web_client_factory()
    txn_id = _txn_id(seeded_web_db, "prov-1")

    resp = client.post(
        f"/_partial/transactions/{txn_id}/became-cash", data={"usd_received": "forty"}
    )

    assert resp.status_code == 422


def test_recording_refuses_an_already_paired_row(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    """The server refuses independently — the hidden button is a courtesy."""
    client: TestClient = web_client_factory()
    txn_id = _txn_id(seeded_web_db, "prov-1")
    cash_conversion.convert_to_cash(
        seeded_web_db, transaction_id=txn_id, usd_received=Decimal("40")
    )

    resp = client.post(
        f"/_partial/transactions/{txn_id}/became-cash", data={"usd_received": "40"}
    )

    assert resp.status_code == 422


# ---------------------------------------------------------------------------
# Taking it back
# ---------------------------------------------------------------------------


def test_unpair_breaks_the_pair_and_leaves_both_rows(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    client: TestClient = web_client_factory()
    anchor_id = _txn_id(seeded_web_db, "prov-1")
    result = cash_conversion.convert_to_cash(
        seeded_web_db, transaction_id=anchor_id, usd_received=Decimal("40")
    )

    resp = client.post(f"/_partial/transactions/{anchor_id}/unpair")

    assert resp.status_code == 200, resp.text
    anchor = transactions_repo.get_by_id(seeded_web_db, anchor_id)
    cash = transactions_repo.get_by_id(seeded_web_db, result.cash_transaction_id)
    assert anchor is not None and cash is not None
    assert anchor.transfer_id is None
    assert cash.transfer_id is None


def test_unpair_says_the_orphan_is_still_there(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    """No hidden deletes: the toast names the row the owner may now remove."""
    client: TestClient = web_client_factory()
    anchor_id = _txn_id(seeded_web_db, "prov-1")
    cash_conversion.convert_to_cash(
        seeded_web_db, transaction_id=anchor_id, usd_received=Decimal("40")
    )

    resp = client.post(f"/_partial/transactions/{anchor_id}/unpair")

    message = _trigger(resp)["toast"]["message"]
    assert "Cash USD" in message
    assert "delete" in message.lower()


def test_unpair_refuses_a_pair_with_no_pre_image(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    client: TestClient = web_client_factory()
    anchor_id = _txn_id(seeded_web_db, "prov-1")
    other_id = _txn_id(seeded_web_db, "prov-2")
    seeded_web_db.execute(
        "UPDATE transactions SET kind='transfer', transfer_id='legacy' "
        "WHERE id IN (?, ?)",
        (anchor_id, other_id),
    )

    resp = client.post(f"/_partial/transactions/{anchor_id}/unpair")

    assert resp.status_code == 422
    assert "no record of what these rows were" in resp.text


def test_unpair_refuses_a_row_that_is_not_paired(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    client: TestClient = web_client_factory()

    resp = client.post(
        f"/_partial/transactions/{_txn_id(seeded_web_db, 'prov-1')}/unpair"
    )

    assert resp.status_code == 422


def test_delete_works_on_the_orphan_after_unpair(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    """The whole point: a wrong number is recoverable without a script."""
    client: TestClient = web_client_factory()
    anchor_id = _txn_id(seeded_web_db, "prov-1")
    result = cash_conversion.convert_to_cash(
        seeded_web_db, transaction_id=anchor_id, usd_received=Decimal("4000")
    )

    client.post(f"/_partial/transactions/{anchor_id}/unpair")
    resp = client.post(f"/_partial/transactions/{result.cash_transaction_id}/delete")

    assert resp.status_code == 200, resp.text
    assert (
        transactions_repo.get_by_id(seeded_web_db, result.cash_transaction_id) is None
    )
