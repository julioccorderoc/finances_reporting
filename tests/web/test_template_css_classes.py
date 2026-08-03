"""Every CSS class a template uses must actually exist in a stylesheet.

``finances/web/static/css/tailwind.css`` is a **fixed extract** with no build
step: it carries the utilities that were in use when it was vendored and
nothing else. Reaching for a plausible-looking Tailwind class that is not in
it — ``rounded-md``, ``sr-only``, ``border-2``, ``py-2`` — renders an
unstyled element, and every server-side test still passes because the markup
is perfectly valid. This is the class-level cousin of the tojson-in-attribute
trap, and it has now cost time twice.

Anything genuinely new belongs in ``app.css``, written by hand.
"""

from __future__ import annotations

import re
from pathlib import Path

STATIC = Path(__file__).resolve().parents[2] / "finances" / "web" / "static"
TEMPLATES = Path(__file__).resolve().parents[2] / "finances" / "web" / "templates"

#: Classes applied by JS at runtime rather than written in a class attribute,
#: so they never appear in a stylesheet the scanner can see.
_RUNTIME_CLASSES = {"htmx-request", "htmx-settling", "htmx-swapping"}


def _defined_classes() -> set[str]:
    css = "".join(
        p.read_text(encoding="utf-8") for p in (STATIC / "css").glob("*.css")
    )
    return {m.replace("\\", "") for m in re.findall(r"\.((?:[\w-]|\\.)+)", css)}


def _used_classes(html: str) -> set[str]:
    used: set[str] = set()
    # The lookbehind matters: `x-bind:class="` and `:class="` hold JS
    # expressions, not class lists, and a bare `class="` pattern matches
    # their tails too.
    for value in re.findall(r'(?<![-:\w])class="([^"]*)"', html):
        # Drop Jinja blocks; what is left is literal class text. Alpine's
        # x-bind:class holds a JS expression, not a class list, and is not
        # matched here because it uses a different attribute name.
        literal = re.sub(r"\{[%{].*?[%}]\}", " ", value)
        used.update(
            token
            for token in literal.split()
            if token and "{" not in token and "'" not in token and ":" != token
        )
    return used


def test_every_template_class_is_defined_somewhere() -> None:
    defined = _defined_classes() | _RUNTIME_CLASSES
    offenders: dict[str, list[str]] = {}

    for path in sorted(TEMPLATES.rglob("*.html")):
        missing = sorted(
            c
            for c in _used_classes(path.read_text(encoding="utf-8"))
            if c not in defined and re.fullmatch(r"[a-z][\w:.\\/-]*", c)
        )
        if missing:
            offenders[str(path.relative_to(TEMPLATES))] = missing

    assert not offenders, (
        "templates use CSS classes that exist in no stylesheet — the vendored "
        "tailwind.css has no build step, so a class it lacks renders unstyled "
        "while every other test stays green. Add the rule to app.css or use a "
        f"class that exists: {offenders}"
    )
