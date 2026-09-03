"""After the reskin, nothing hand-written styles a surface that is gone.

``tests/web/test_triage_cleanup.py`` guards three class families in app.css
(the ones the triage redesign replaced). The viewer reskin replaced the
rest of the old viewer, so the guard widens to the whole file and to every
hand-written sheet: a selector that matches nothing any template or script
renders is dead weight the next reader cannot tell from a live rule. The
same goes for a Jinja macro nobody calls — the old badge and chip macros
emitted Tailwind utilities, and a dead macro is the same trap one level up.

Both checks are deliberately generous (a bare word match anywhere in the
templates or the JS counts as live), so they can only ever under-report.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
WEB = ROOT / "finances" / "web"
CSS = WEB / "static" / "css"
JS = WEB / "static" / "js"
TEMPLATES = WEB / "templates"

#: tailwind.css is a vendored extract and is not ours to prune.
HAND_WRITTEN = sorted(p.name for p in CSS.glob("*.css") if p.name != "tailwind.css")

#: Classes applied by htmx or Alpine at runtime, never written in a template.
_RUNTIME = {"htmx-request", "htmx-settling", "htmx-swapping", "htmx-added"}


def _haystack() -> str:
    """Everything that can put a class on an element.

    Templates and scripts, plus the view services and routers: a chip's
    tone class (``prov-trusted``) is decided in Python and reaches the
    template as a value, never as a literal in the markup.
    """
    parts = [p.read_text(encoding="utf-8") for p in TEMPLATES.rglob("*.html")]
    parts += [p.read_text(encoding="utf-8") for p in JS.glob("*.js")]
    parts += [p.read_text(encoding="utf-8") for p in (WEB / "services").glob("*.py")]
    parts += [p.read_text(encoding="utf-8") for p in (WEB / "routers").glob("*.py")]
    return "\n".join(parts)


def _class_selectors(css: str) -> set[str]:
    """Class names in SELECTORS only — never the ``.5rem`` inside a value."""
    stripped = re.sub(r"/\*.*?\*/", "", css, flags=re.DOTALL)
    names: set[str] = set()
    for selector in re.findall(r"([^{}]+)\{", stripped):
        if selector.strip().startswith("@"):
            continue
        names.update(re.findall(r"\.([A-Za-z_][\w-]*)", selector))
    return names


@pytest.mark.parametrize("sheet", HAND_WRITTEN, ids=lambda n: n)
def test_every_class_a_sheet_styles_is_rendered_somewhere(sheet: str) -> None:
    css = (CSS / sheet).read_text(encoding="utf-8")
    hay = _haystack()

    dead = sorted(
        name
        for name in _class_selectors(css)
        if name not in _RUNTIME and not re.search(rf"\b{re.escape(name)}\b", hay)
    )

    assert not dead, (
        f"{sheet} styles classes nothing renders — delete the rules or the "
        f"surface they belonged to came back without them: {dead}"
    )


def test_every_macro_in_macros_html_has_a_caller() -> None:
    source = (TEMPLATES / "_macros.html").read_text(encoding="utf-8")
    defined = re.findall(r"\{%-?\s*macro\s+(\w+)\s*\(", source)
    assert defined, "could not parse any macro out of _macros.html"

    callers = "\n".join(
        p.read_text(encoding="utf-8")
        for p in TEMPLATES.rglob("*.html")
        if p.name != "_macros.html"
    )
    orphans = sorted(name for name in defined if not re.search(rf"\b{name}\b", callers))

    assert not orphans, (
        "macros in _macros.html that no template imports or calls — the "
        f"reskin retired the surfaces they drew: {orphans}"
    )
