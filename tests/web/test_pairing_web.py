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
