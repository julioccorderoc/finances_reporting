"""Triage accessibility — the J group Wave 2 deferred.

J1 and J4 (the dialog's role/label, an accessible name on every icon-only
control) shipped with Wave 2 and are asserted in ``test_triage_modal_run``.
What is here is the rest:

* **J2** focus moves into the dialog on open, is *trapped* while it is open,
  and returns to the row that opened it on close.
* **J5** toasts announce politely, and the queue counts announce rather than
  changing in silence.
* **J6** tab order follows visual order in both modal columns, and the
  category chips are reachable and operable from the keyboard alone.

The trap and the restore are plain JS in ``triage.js`` — no library — so
their contract is asserted against the source the page actually loads, the
same way ``test_triage_modal_nav`` pins the arrow handler. The behaviour
itself was driven in a browser; see ``design_handoff_triage/
ACCEPTANCE-REPORT.md``.
"""

from __future__ import annotations

import pathlib
import re
import sqlite3
from collections.abc import Callable

import pytest
from starlette.testclient import TestClient


WEB = pathlib.Path(__file__).resolve().parents[2] / "finances" / "web"


@pytest.fixture(scope="module")
def triage_js() -> str:
    return (WEB / "static" / "js" / "triage.js").read_text(encoding="utf-8")


def _page(factory: Callable[[], TestClient]) -> str:
    with factory() as client:
        response = client.get("/triage")
    assert response.status_code == 200, response.text
    return response.text


def _modal(factory: Callable[[], TestClient], txn_id: int = 1) -> str:
    with factory() as client:
        response = client.get(f"/_partial/triage/{txn_id}/modal")
    assert response.status_code == 200, response.text
    return response.text


# ---------------------------------------------------------------------------
# J2 — the focus trap
# ---------------------------------------------------------------------------


def test_the_dialog_takes_focus_on_open(
    triage_web_db: sqlite3.Connection,
    web_client_factory: Callable[[], TestClient],
    triage_js: str,
) -> None:
    """Advancing INTO a dialog replaces the element that had focus.

    Without this the keyboard is left on ``<body>`` and the very first Tab
    walks into the queue behind the scrim.
    """
    html = _modal(web_client_factory)

    assert 'tabindex="-1"' in html
    assert "x-init=\"adopt()\"" in html
    assert "dialog.focus()" in triage_js


def test_tab_is_trapped_inside_the_open_dialog(triage_js: str) -> None:
    """Tab and Shift-Tab cycle within the dialog, both ends wrapped."""
    assert '"Tab"' in triage_js
    assert "trapTab" in triage_js
    assert "shiftKey" in triage_js
    # Both ends: the last element wraps to the first and the first back to
    # the last. A trap that only guards one end leaks on Shift-Tab.
    assert re.search(r"first\s*\.focus\(\)", triage_js)
    assert re.search(r"last\s*\.focus\(\)", triage_js)


def test_the_trap_skips_disabled_and_invisible_controls(triage_js: str) -> None:
    """A disabled arrow at the end of the run must not swallow a Tab.

    Neither must a control inside a collapsed disclosure — the picker's
    full list is in the DOM the whole time and is merely ``x-show``n.
    """
    assert "button:not([disabled])" in triage_js
    assert "getClientRects" in triage_js


def test_the_trap_is_bound_only_while_the_dialog_is_mounted(
    triage_web_db: sqlite3.Connection,
    web_client_factory: Callable[[], TestClient],
) -> None:
    """C7's rule applies to Tab as well: no listener outlives the dialog.

    ``@keydown.window`` on the dialog's own element is what buys that —
    Alpine removes it when the element is swapped away.
    """
    html = _modal(web_client_factory)

    assert "@keydown.window=\"onKey($event)\"" in html


def test_closing_returns_focus_to_the_row_that_opened_the_run(
    triage_web_db: sqlite3.Connection,
    web_client_factory: Callable[[], TestClient],
    triage_js: str,
) -> None:
    """Keyed by item id, not by element.

    Closing refreshes the queue, so every row node the run started from
    has been replaced by the time focus is restored; a stored element
    reference would point at a detached node.
    """
    page = _page(web_client_factory)

    assert "rememberOrigin(" in page
    assert "restoreFocus()" in page
    assert "focusOrigin" in triage_js
    # The fallbacks, in order: the row's own open button, the run button,
    # the queue itself. Focus must never be dropped on <body>.
    assert "triage-row-open" in triage_js
    assert "[data-sort-all]" in triage_js


# ---------------------------------------------------------------------------
# J5 — announcements
# ---------------------------------------------------------------------------


def test_toasts_announce_politely(
    triage_web_db: sqlite3.Connection,
    web_client_factory: Callable[[], TestClient],
) -> None:
    page = _page(web_client_factory)

    opening = page[page.index("<div\n      id=\"toast-host\"") :]
    opening = opening[: opening.index("@show-toast")]
    assert 'aria-live="polite"' in opening


def test_the_queue_counts_have_a_live_region_that_survives_a_swap(
    triage_web_db: sqlite3.Connection,
    web_client_factory: Callable[[], TestClient],
) -> None:
    """The announcer sits OUTSIDE ``#triage-queue`` on purpose.

    Every save swaps that element's innerHTML. A live region *inside* the
    swap is a brand-new node each time, and a newly inserted region does
    not announce — the update would be silent exactly when it matters.
    """
    with web_client_factory() as client:
        page = client.get("/triage").text
        # Exactly what a save swaps into #triage-queue.
        swapped = client.get("/_partial/triage/queue").text

    assert 'id="triage-announcer"' in page
    announcer = page[page.index('id="triage-announcer"') :]
    announcer = announcer[: announcer.index(">")]
    assert 'aria-live="polite"' in announcer
    assert 'aria-atomic="true"' in announcer

    assert "triage-announcer" not in swapped


def test_the_announcer_is_fed_by_the_queue_swap(
    triage_web_db: sqlite3.Connection,
    web_client_factory: Callable[[], TestClient],
    triage_js: str,
) -> None:
    """It repeats the headline answer and the three counts, on settle."""
    page = _page(web_client_factory)

    assert "announceCounts()" in page
    assert "htmx:after-settle" in page
    assert "triage-answer" in triage_js
    assert "triage-meta" in triage_js


# ---------------------------------------------------------------------------
# J6 — tab order and keyboard-only category choice
# ---------------------------------------------------------------------------


def test_nothing_on_the_screen_sets_a_positive_tabindex(
    triage_web_db: sqlite3.Connection,
    web_client_factory: Callable[[], TestClient],
) -> None:
    """Visual order IS DOM order; a positive tabindex would break that."""
    for html in (_page(web_client_factory), _modal(web_client_factory)):
        assert not re.search(r'tabindex="[1-9]', html)


def test_the_modal_columns_are_in_reading_order(
    triage_web_db: sqlite3.Connection,
    web_client_factory: Callable[[], TestClient],
) -> None:
    """Header, then the facts column, then the decision column, then the
    footer — the order the eye takes them in."""
    html = _modal(web_client_factory)

    order = [
        html.index("tmodal-header"),
        html.index("tmodal-facts"),
        html.index("tmodal-decision"),
        html.index("tmodal-footer"),
    ]
    assert order == sorted(order)


def test_category_chips_are_real_buttons(
    triage_web_db: sqlite3.Connection,
    web_client_factory: Callable[[], TestClient],
) -> None:
    """Enter and Space come free from the element; a div would need both
    wired by hand and would still be missing from the tab order."""
    html = _modal(web_client_factory)

    chips = re.findall(r"<button[^>]*class=\"catchip\"", html)
    assert chips, "no category chips rendered"
    for chip in chips:
        assert 'type="button"' in chip


def test_every_pickable_category_shows_its_test_on_keyboard_focus(
    triage_web_db: sqlite3.Connection,
    web_client_factory: Callable[[], TestClient],
) -> None:
    """E5 by keyboard as well as by mouse.

    The chips already fed the strip on ``@focus``; the rows in the
    expanded list only did it on ``@mouseenter``, so tabbing through them
    left the strip empty.
    """
    html = _modal(web_client_factory)

    rows = re.findall(r"<button[^>]*class=\"catrow\"[^>]*>", html, re.S)
    assert rows, "no category rows rendered"
    for row in rows:
        assert "@focus=\"hoverFrom($el)\"" in row
        assert "@blur=\"hoverClear()\"" in row
