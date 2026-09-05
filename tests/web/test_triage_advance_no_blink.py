"""Advancing through the run must not re-play the entrance (2026-09-05).

Julio: "the triage page blinks when I triage a transaction and then it
goes to the next one".

Every advance — save, park, pair-confirm, «Became cash», and both arrows
— answers with the NEXT entry's dialog swapped into #triage-modal-host.
That replaces the whole ``.tover``, scrim included, and the CSS runs
``triage-fade`` (opacity 0 -> 1) on it and ``triage-rise`` on the panel
every single time. For one frame the scrim is transparent and the queue
behind it shows through: the blink.

Opening the run should still animate. Moving inside it should not, which
is the same rule criterion B3 already states about geometry — paging
through the queue must not move the frame by a pixel.

``advancing`` is decided server-side rather than in the browser: the
arrows and the write routes all know which they are, and a marker in the
markup is assertable here instead of only in a browser.
"""

from __future__ import annotations

import pathlib
import re
import sqlite3
from collections.abc import Callable

from starlette.testclient import TestClient

CSS = (
    pathlib.Path(__file__).resolve().parents[2]
    / "finances"
    / "web"
    / "static"
    / "css"
    / "triage.css"
)

STATIC = "tover-static"


def _overlay(html: str) -> str:
    """The opening ``<div class="tover…">`` tag."""
    match = re.search(r'<div class="tover[^"]*"', html)
    assert match is not None, "no overlay in this response"
    return match.group(0)


# ---------------------------------------------------------------------------
# Opening still animates.
# ---------------------------------------------------------------------------


def test_opening_the_run_keeps_the_entrance(
    triage_web_db: sqlite3.Connection,
    web_client_factory: Callable[[], TestClient],
) -> None:
    with web_client_factory() as client:
        html = client.get("/_partial/triage/1/modal").text

    assert STATIC not in _overlay(html)


# ---------------------------------------------------------------------------
# Moving inside it does not.
# ---------------------------------------------------------------------------


def test_the_arrows_point_at_an_advance(
    triage_web_db: sqlite3.Connection,
    web_client_factory: Callable[[], TestClient],
) -> None:
    """A neighbour's URL is the same route, flagged — no endpoint of its own."""
    with web_client_factory() as client:
        html = client.get("/_partial/triage/1/modal").text

    for marker in ("data-nav-prev", "data-nav-next"):
        button = re.search(rf"<button[^>]*{marker}[^>]*>", html)
        assert button is not None
        if "disabled" in button.group(0):
            continue
        assert "advance=1" in button.group(0), marker


def test_an_advance_url_renders_without_the_entrance(
    triage_web_db: sqlite3.Connection,
    web_client_factory: Callable[[], TestClient],
) -> None:
    with web_client_factory() as client:
        html = client.get("/_partial/triage/1/modal?advance=1").text

    assert STATIC in _overlay(html)


def test_saving_advances_without_the_entrance(
    triage_web_db: sqlite3.Connection,
    web_client_factory: Callable[[], TestClient],
) -> None:
    """The reported path: sort a row, land on the next one, no flash."""
    with web_client_factory() as client:
        response = client.post(
            "/_partial/triage/3/edit",
            data={
                "set_user_rate": "true",
                "user_rate": "150.00",
                "set_category": "false",
                "set_notes": "false",
            },
        )

    assert response.status_code == 200
    assert 'role="dialog"' in response.text
    assert STATIC in _overlay(response.text)


def test_parking_advances_without_the_entrance(
    triage_web_db: sqlite3.Connection,
    web_client_factory: Callable[[], TestClient],
) -> None:
    with web_client_factory() as client:
        response = client.post("/_partial/triage/1/park")

    assert response.status_code == 200
    assert STATIC in _overlay(response.text)


# ---------------------------------------------------------------------------
# And the marker has to actually stop the animation.
# ---------------------------------------------------------------------------


def test_the_static_overlay_runs_no_animation() -> None:
    """Both halves: the scrim's fade AND the panel's rise.

    Killing only the scrim would leave the dialog itself fading up from
    nothing on every advance, which is the same blink in a smaller box.
    """
    css = CSS.read_text(encoding="utf-8")
    rule = re.search(
        r"\.tover-static[^{]*\{[^}]*\}", css, re.S
    )
    assert rule is not None, "no .tover-static rule in triage.css"
    assert "animation: none" in rule.group(0)
    selector = rule.group(0).split("{")[0]
    assert ".tover-static .tmodal" in selector
