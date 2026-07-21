"""WP4 — keyboard shortcuts on the transaction modals (tests first).

Server-rendered markup contract only (JS behaviour is not executable
under pytest; the manual gate covers it): the overlay carries a
window-scoped keydown handler that (1) ignores keystrokes while typing
in form controls, (2) maps keys 1-8 to [data-chip] clicks, (3) maps
Enter to the form's submit button (= Save & next in the triage modal),
(4) maps s to [data-park-btn] — triage modal only. Esc close pre-exists.
"""

from __future__ import annotations

import sqlite3


def _txn_id(conn: sqlite3.Connection, source_ref: str) -> int:
    row = conn.execute(
        "SELECT id FROM transactions WHERE source_ref = ?", (source_ref,)
    ).fetchone()
    assert row is not None
    return int(row["id"])


def test_edit_modal_has_scoped_keydown_handler(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    client = web_client_factory()
    txn_id = _txn_id(seeded_web_db, "prov-1")

    body = client.get(f"/_partial/transactions/{txn_id}/modal").text

    assert "@keydown.window=" in body
    assert "isTyping($event)" in body      # inert while typing in inputs
    assert "data-chip" in body             # 1-8 targets exist
    assert 'tabindex="-1"' in body         # card takes focus → keys land here
    assert "data-park-btn" not in body     # park is triage-only


def test_triage_modal_has_keydown_handler_and_park_key(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    client = web_client_factory()
    txn_id = _txn_id(seeded_web_db, "prov-needs-review")

    body = client.get(f"/_partial/triage/{txn_id}/modal").text

    assert "@keydown.window=" in body
    assert "isTyping($event)" in body
    assert "data-park-btn" in body
    assert 'tabindex="-1"' in body
