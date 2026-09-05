"""The rate chip explains itself on screen, not through a title (2026-09-05).

Julio: "the 'realized' help overlay on the triage modal shows nothing".

The explanation was never missing — the chip renders
``title="Your own cost basis from a P2P sell within 14 days · 796.52"``
and always has. What was missing is a tooltip anyone can rely on: the
native ``title`` is the browser's to draw or not, it never appears on
keyboard focus, it cannot be styled to match anything around it, and
``cursor: help`` had been promising it for months.

So the chip carries its own. A ``.prov-help`` element, shown on hover AND
on focus, with the chip made focusable so the keyboard can reach the same
explanation the mouse gets. The ``title`` goes: two tooltips for one chip
is one too many, and the native one is the one that cannot be fixed.
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


def _chip(html: str, source: str) -> str:
    """The whole ``<span class="prov …">…</span>`` for one tier."""
    start = html.index(f'data-prov="{source}"')
    start = html.rindex("<span", 0, start)
    depth = 0
    for tag in re.finditer(r"<span\b|</span>", html[start:]):
        depth += 1 if tag.group(0).startswith("<span") else -1
        if depth == 0:
            return html[start : start + tag.end()]
    raise AssertionError(f"unbalanced chip for {source}")


def test_the_realized_chip_carries_its_own_explanation(
    triage_web_db: sqlite3.Connection,
    web_client_factory: Callable[[], TestClient],
) -> None:
    with web_client_factory() as client:
        chip = _chip(client.get("/triage").text, "binance_p2p_median_nearest")

    bubble = re.search(r'<span class="prov-help"[^>]*>(.*?)</span>', chip, re.S)
    assert bubble is not None, "no help bubble on this chip"
    assert "Approximate" in bubble.group(1)


def test_every_tier_says_which_tier_it_is(
    triage_web_db: sqlite3.Connection,
    web_client_factory: Callable[[], TestClient],
) -> None:
    """The wording is prov_title's, unchanged — only where it lives moved.

    Asserted INSIDE the bubble, not merely somewhere in the chip: the old
    title attribute carried the same sentence, so a looser check passes
    before a single line is written.
    """
    with web_client_factory() as client:
        html = client.get("/_partial/triage/1/modal").text

    chip = _chip(html, "binance_p2p_median_carry")
    bubble = re.search(r'<span class="prov-help"[^>]*>(.*?)</span>', chip, re.S)
    assert bubble is not None, "no help bubble on this chip"
    assert "14-day median of Binance P2P sells" in bubble.group(1)


def test_the_chip_is_reachable_by_keyboard(
    triage_web_db: sqlite3.Connection,
    web_client_factory: Callable[[], TestClient],
) -> None:
    """A tooltip only a mouse can open is half a tooltip."""
    with web_client_factory() as client:
        chip = _chip(client.get("/triage").text, "binance_p2p_median_nearest")

    assert 'tabindex="0"' in chip


def test_the_native_tooltip_is_gone(
    triage_web_db: sqlite3.Connection,
    web_client_factory: Callable[[], TestClient],
) -> None:
    """Two tooltips for one chip is one too many."""
    with web_client_factory() as client:
        chip = _chip(client.get("/triage").text, "binance_p2p_median_nearest")

    assert "title=" not in chip


def test_the_help_opens_on_hover_and_on_focus() -> None:
    css = CSS.read_text(encoding="utf-8")

    assert ".prov-help {" in css
    reveal = re.search(r"[^}]*\.prov-help\s*\{[^}]*visibility:\s*visible", css, re.S)
    assert reveal is not None, "nothing ever reveals .prov-help"
    assert ".prov:hover .prov-help" in css
    assert ".prov:focus-within .prov-help" in css


def test_the_help_does_not_take_the_pointer() -> None:
    """Hovering the chip must not be interrupted by its own bubble."""
    css = CSS.read_text(encoding="utf-8")

    rule = re.search(r"\.prov-help\s*\{[^}]*\}", css, re.S)
    assert rule is not None
    assert "pointer-events: none" in rule.group(0)
