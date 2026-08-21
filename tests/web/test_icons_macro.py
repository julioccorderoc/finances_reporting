"""The icon macro is a cross-session contract, so it is tested as one.

Wave 2 of the Triage redesign builds templates against ``icon(name, size)``
in another session. Two things have to hold for that to be safe:

* every name the design actually uses renders something — a typo'd or
  missing name renders an empty string by design, which no server-side
  test would otherwise notice (the same class of silent hole as the
  vendored-Tailwind class trap next door in
  ``test_template_css_classes.py``); and
* the SVG shape stays fixed — 24x24 viewBox, ``currentColor``,
  ``stroke-width="2"``, size on width/height — because SIGNAL uses one
  stroke weight at every size from 10px to 28px.

The name list below is the contract itself, transcribed from
``design_handoff_triage/README.md`` §Assets and from every ``icon:`` value
in ``design/ui_kits/finances/triage-data.js``. It is duplicated here on
purpose: the handoff directory is gitignored, so the test cannot read it.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest
from jinja2 import Environment, FileSystemLoader, StrictUndefined

TEMPLATES = Path(__file__).resolve().parents[2] / "finances" / "web" / "templates"

#: README §Assets — the chrome of the queue, the modal and the sheets.
UI_ICONS = (
    "archive",
    "check",
    "check-check",
    "chevron-down",
    "chevron-left",
    "chevron-right",
    "circle-slash",
    "file-code",
    "git-compare-arrows",
    "history",
    "link",
    "maximize-2",
    "play",
    "scale",
    "search",
    "tag",
    "triangle-alert",
    "undo-2",
    "x",
)

#: Every ``icon:`` in triage-data.js — the 26 categories plus the account
#: kinds the row and pair views show.
DATA_ICONS = (
    "arrow-down-left",
    "arrow-left-right",
    "banknote",
    "bitcoin",
    "briefcase",
    "car",
    "circle-dashed",
    "flag",
    "gift",
    "graduation-cap",
    "hand-coins",
    "heart",
    "heart-pulse",
    "house",
    "landmark",
    "laptop",
    "package",
    "percent",
    "receipt",
    "repeat",
    "rotate-ccw",
    "scale",
    "scissors",
    "shirt",
    "shopping-basket",
    "ticket",
    "users",
    "utensils",
    "wallet",
    "zap",
)

REQUIRED = tuple(sorted(set(UI_ICONS) | set(DATA_ICONS)))


@pytest.fixture(scope="module")
def render():
    """Render ``icon(...)`` through a plain Jinja env, as templates import it."""
    env = Environment(
        loader=FileSystemLoader(str(TEMPLATES)),
        undefined=StrictUndefined,
        autoescape=True,
    )
    template = env.from_string(
        '{% from "_icons.html" import icon %}{{ icon(name, size) }}'
    )

    def _render(name: str, size: int = 16) -> str:
        return template.render(name=name, size=size)

    return _render


@pytest.mark.parametrize("name", REQUIRED)
def test_every_declared_icon_renders_an_svg(render, name: str) -> None:
    markup = render(name, 16)

    assert markup.startswith("<svg"), f"{name} rendered nothing"
    assert markup.endswith("</svg>")
    assert f'data-icon="{name}"' in markup
    # A wrapper with no drawing in it is the same hole as no icon at all.
    assert re.search(r"<(path|circle|rect|line|polyline|polygon|ellipse)\b", markup), (
        f"{name} rendered an empty <svg>"
    )


def test_the_pinned_signature_is_name_then_size(render) -> None:
    """``icon("archive", 15)`` — the call Wave 2 templates are written to."""
    markup = render("archive", 15)

    assert 'width="15"' in markup
    assert 'height="15"' in markup


@pytest.mark.parametrize("size", [10, 11, 15, 17, 28])
def test_the_svg_shape_is_fixed_at_every_size(render, size: int) -> None:
    markup = render("triangle-alert", size)

    assert 'viewBox="0 0 24 24"' in markup
    assert 'stroke="currentColor"' in markup
    assert 'stroke-width="2"' in markup
    assert 'fill="none"' in markup
    assert f'width="{size}"' in markup
    assert f'height="{size}"' in markup


def test_icons_are_decorative_to_screen_readers(render) -> None:
    """Labels live on the button; a second reading of the glyph is noise."""
    markup = render("maximize-2", 15)

    assert 'aria-hidden="true"' in markup
    assert 'focusable="false"' in markup


def test_an_unknown_name_renders_nothing_rather_than_raising(render) -> None:
    """A missing icon must not take a page down mid-triage."""
    assert render("no-such-icon", 16) == ""


def test_the_svg_markup_is_not_escaped(render) -> None:
    """Autoescape is on everywhere; the path data has to survive it."""
    markup = render("check", 16)

    assert "&lt;path" not in markup
    assert "<path" in markup
