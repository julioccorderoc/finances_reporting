"""The note under a description is meant to be read, not merely noticed.

The owner, 2026-09-07: "I want to see the notes on the transactions…
this will help me understand what's going on." They were already on the
row — ``partials/card_transaction.html`` has rendered them since WP3 —
but at 10.5px in placeholder ink and cut at 60 characters, which reads
as a smudge under the description rather than as "regalo franklin
zapatos". 475 rows in the live ledger carry one.

Nothing moves: the note keeps its place under the description, keeps its
single line, keeps the ellipsis and the full text in ``title``. Only its
size, its ink and the server-side truncation change. That was the owner's
pick over a dedicated Notes column, which would have taken width from
Description and Account at the 1196px cap.

Tests precede the implementation per rule-011.
"""

from __future__ import annotations

import re
import sqlite3

from fastapi.testclient import TestClient

from finances.web.app import TEMPLATES_DIR

# Longer than the 60 characters the template used to keep.
LONG_NOTE = (
    "Prestamo a naty para su telefono nuevo, se lo devuelve en dos quincenas"
)


def _note_span(body: str) -> str:
    m = re.search(r'<span class="flow-row-note"[^>]*>.*?</span>', body, re.S)
    assert m, "no note rendered"
    return m.group(0)


def _flow_css() -> str:
    return (
        TEMPLATES_DIR.parent / "static" / "css" / "flow.css"
    ).read_text(encoding="utf-8")


def _rule(css: str, selector: str) -> str:
    start = css.index(selector + " {")
    return css[start : css.index("}", start)]


def test_a_long_note_is_no_longer_cut_at_sixty_characters(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    """The truncation was doing the ellipsis's job, worse.

    ``| truncate(60)`` decides in Jinja how much of a note fits, on a
    column whose width it cannot know, and appends its own "..." that no
    amount of browser width will ever expand. CSS ``text-overflow`` does
    the same job at the real width, and the full text stays in ``title``.
    """
    seeded_web_db.execute(
        "UPDATE transactions SET notes = ? WHERE source_ref = 'prov-1'",
        (LONG_NOTE,),
    )
    client: TestClient = web_client_factory()

    body = client.get("/transactions").text
    span = _note_span(body)

    assert LONG_NOTE in span
    assert "..." not in span
    assert f'title="{LONG_NOTE}"' in span
    # Still one line, still elided by CSS rather than by the server.
    note_rule = _rule(_flow_css(), ".flow-row-note")
    assert "text-overflow: ellipsis" in note_rule
    assert "white-space: nowrap" in note_rule


def test_the_note_is_sized_and_inked_to_be_read(
    seeded_web_db: sqlite3.Connection,
) -> None:
    """12px in secondary ink — below the description, above a whisper.

    ``--text-placeholder`` is the token for text that is not content
    (an empty field's prompt). A note the owner typed is content, so it
    takes ``--text-secondary`` (#4b4b46, AA on the canvas). The size
    stays under the description's 13px: this is still the second line of
    the cell, not a competing headline.
    """
    note_rule = _rule(_flow_css(), ".flow-row-note")

    assert "font-size: 12px" in note_rule
    assert "var(--text-secondary)" in note_rule
    assert "--text-placeholder" not in note_rule


def test_a_row_without_a_note_renders_no_note_line(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    """The empty case gains nothing — no dash, no reserved line."""
    client: TestClient = web_client_factory()

    body = client.get("/transactions").text

    assert "flow-row-note" not in body


def test_the_note_shows_on_todays_recent_activity_too(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    """One card template, both surfaces — the dashboard gets it free."""
    seeded_web_db.execute(
        "UPDATE transactions SET notes = ? WHERE source_ref = 'cash-1'",
        ("lunch with the team",),
    )
    client: TestClient = web_client_factory()

    body = client.get("/").text

    assert "lunch with the team" in _note_span(body)
