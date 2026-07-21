"""HTMX surface for manual P2P pairing: candidate partial + confirm.

Per rule-011 these land before the implementation.
"""

from __future__ import annotations

import sqlite3

from tests.web.test_pairing import pairing_db  # noqa: F401  (fixture reuse)


def _txn_id(conn: sqlite3.Connection, source_ref: str) -> int:
    row = conn.execute(
        "SELECT id FROM transactions WHERE source_ref = ?", (source_ref,)
    ).fetchone()
    assert row is not None, f"missing fixture row {source_ref!r}"
    return int(row["id"])


def test_pair_candidates_partial_lists_nearby_deposits(
    pairing_db: sqlite3.Connection, web_client_factory
) -> None:
    client = web_client_factory()
    sell_id = _txn_id(pairing_db, "p2p-sell-1")

    resp = client.get(f"/_partial/transactions/{sell_id}/pair-candidates")

    assert resp.status_code == 200, resp.text
    assert "dep-exact" in resp.text
    assert "dep-too-old" not in resp.text


def test_pair_candidates_partial_widens_the_window(
    pairing_db: sqlite3.Connection, web_client_factory
) -> None:
    client = web_client_factory()
    sell_id = _txn_id(pairing_db, "p2p-sell-1")

    resp = client.get(
        f"/_partial/transactions/{sell_id}/pair-candidates",
        params={"window_days": 7},
    )

    assert resp.status_code == 200, resp.text
    assert "dep-too-old" in resp.text


def test_pair_candidates_partial_disables_same_sign_rows(
    pairing_db: sqlite3.Connection, web_client_factory
) -> None:
    client = web_client_factory()
    sell_id = _txn_id(pairing_db, "p2p-sell-1")

    resp = client.get(f"/_partial/transactions/{sell_id}/pair-candidates")

    assert resp.status_code == 200, resp.text
    assert "same sign" in resp.text
    assert "disabled" in resp.text


def test_pair_candidates_partial_404s_for_unknown_sell(
    pairing_db: sqlite3.Connection, web_client_factory
) -> None:
    client = web_client_factory()

    resp = client.get("/_partial/transactions/999999/pair-candidates")

    assert resp.status_code == 404


# ---------------------------------------------------------------------------
# Confirm flow + modal wiring.
# ---------------------------------------------------------------------------


def test_pair_confirm_creates_the_transfer(
    pairing_db: sqlite3.Connection, web_client_factory
) -> None:
    client = web_client_factory()
    sell_id = _txn_id(pairing_db, "p2p-sell-1")
    deposit_id = _txn_id(pairing_db, "dep-exact")

    resp = client.post(f"/_partial/transactions/{sell_id}/pair/{deposit_id}")

    assert resp.status_code == 200, resp.text
    assert "closeModal" in resp.headers.get("HX-Trigger", "")

    rows = pairing_db.execute(
        "SELECT transfer_id, kind FROM transactions WHERE id IN (?, ?)",
        (sell_id, deposit_id),
    ).fetchall()
    transfer_ids = {row["transfer_id"] for row in rows}
    assert len(transfer_ids) == 1 and None not in transfer_ids
    assert {row["kind"] for row in rows} == {"transfer"}


def test_pair_confirm_422s_when_already_paired(
    pairing_db: sqlite3.Connection, web_client_factory
) -> None:
    client = web_client_factory()
    sell_id = _txn_id(pairing_db, "p2p-sell-1")
    deposit_id = _txn_id(pairing_db, "dep-exact")

    first = client.post(f"/_partial/transactions/{sell_id}/pair/{deposit_id}")
    assert first.status_code == 200, first.text

    second = client.post(f"/_partial/transactions/{sell_id}/pair/{deposit_id}")
    assert second.status_code == 422


def test_pair_confirm_404s_for_unknown_deposit(
    pairing_db: sqlite3.Connection, web_client_factory
) -> None:
    client = web_client_factory()
    sell_id = _txn_id(pairing_db, "p2p-sell-1")

    resp = client.post(f"/_partial/transactions/{sell_id}/pair/999999")

    assert resp.status_code == 404


def test_modal_shows_the_pair_section_for_an_unpaired_sell(
    pairing_db: sqlite3.Connection, web_client_factory
) -> None:
    client = web_client_factory()
    sell_id = _txn_id(pairing_db, "p2p-sell-1")

    resp = client.get(f"/_partial/transactions/{sell_id}/modal")

    assert resp.status_code == 200, resp.text
    assert "pair-candidates" in resp.text


def test_modal_hides_the_pair_section_once_paired(
    pairing_db: sqlite3.Connection, web_client_factory
) -> None:
    client = web_client_factory()
    sell_id = _txn_id(pairing_db, "p2p-sell-1")
    pairing_db.execute(
        "UPDATE transactions SET transfer_id = 'tid-x' WHERE id = ?", (sell_id,)
    )

    resp = client.get(f"/_partial/transactions/{sell_id}/modal")

    assert resp.status_code == 200, resp.text
    assert "pair-candidates" not in resp.text
