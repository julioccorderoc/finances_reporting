"""Save & next must actually advance: after the triage edit response
swaps the fresh queue in, the first remaining card's modal auto-opens.

Root cause of the bug: the edit endpoint has always sent
``HX-Trigger: closeModal, advanceQueue`` but nothing ever listened for
``advanceQueue`` — the modal closed and the user was dumped back on the
queue. The fix wires the listener in base.html: the HX-Trigger parser
arms a one-shot ``advanceNext`` flag, and an ``htmx:after-settle``
handler (fires after the queue swap settles) clicks the first modal
trigger left in the queue.
"""

from __future__ import annotations

import sqlite3


def _page(web_client_factory) -> str:
    client = web_client_factory()
    return client.get("/triage").text


def test_hx_trigger_parser_arms_advance_flag(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    body = _page(web_client_factory)
    assert "advanceQueue" in body
    assert "advanceNext" in body


def test_after_settle_handler_opens_next_queue_item(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    body = _page(web_client_factory)
    # One-shot: handler only acts on the triage queue swap, then disarms.
    assert "htmx:after-settle.window" in body
    assert "triage-queue" in body
    # It advances by clicking the first remaining modal trigger.
    assert '[hx-get*=\\\'/modal\\\']' in body or "[hx-get*='/modal']" in body


def test_edit_response_still_sends_both_triggers(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    client = web_client_factory()
    txn_id = seeded_web_db.execute(
        "SELECT id FROM transactions WHERE needs_review = 1 LIMIT 1"
    ).fetchone()["id"]
    resp = client.post(
        f"/_partial/triage/{txn_id}/edit",
        data={
            "set_category": "false",
            "category_id": "",
            "set_user_rate": "false",
            "user_rate": "",
            "set_notes": "false",
            "notes": "",
        },
    )
    assert resp.status_code == 200, resp.text
    trigger = resp.headers["HX-Trigger"]
    assert "closeModal" in trigger
    assert "advanceQueue" in trigger
