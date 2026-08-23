"""The L group — cleanup and non-regression after the Triage redesign.

Three silent failure modes, each of which leaves every other test green:

* **L1/L2** the old review UI's CSS outliving the markup it styled. A dead
  ``.pair-row`` rule costs nothing at runtime and everything in confidence:
  the next person to read app.css cannot tell which of the two triage
  screens is the live one.
* **L5** an undefined custom property. ``color: var(--text-signal-x)``
  renders *nothing* — no error, no fallback, no console line. The only
  witness is a browser and a good eye.
* **L5** the ``/favicon.ico`` probe every browser makes when no icon is
  declared, which has been a standing 404 in the console since the viewer
  shipped.

Plus the motion contract (I9) for the one raised surface app.css owns.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
STATIC = ROOT / "finances" / "web" / "static"
CSS = STATIC / "css"
TEMPLATES = ROOT / "finances" / "web" / "templates"
JS = STATIC / "js"


def _haystack() -> str:
    """Everything that can put a class on an element."""
    parts = [p.read_text(encoding="utf-8") for p in TEMPLATES.rglob("*.html")]
    parts += [p.read_text(encoding="utf-8") for p in JS.glob("*.js")]
    return "\n".join(parts)


def _class_selectors(css: str) -> set[str]:
    return {m.replace("\\", "") for m in re.findall(r"\.((?:[\w-]|\\.)+)", css)}


# ---------------------------------------------------------------------------
# L1 / L2 — no CSS for a surface that no longer exists
# ---------------------------------------------------------------------------


def test_app_css_carries_no_dead_triage_pair_or_parked_rule() -> None:
    """The redesign replaced the old review UI; its styles go with it.

    Scoped to the three families the redesign owns rather than to every
    class in the file — the rest of app.css is the other five pages and is
    not this wave's to prune.
    """
    app_css = (CSS / "app.css").read_text(encoding="utf-8")
    used = _haystack()

    families = ("triage", "parked", "pair")
    dead = sorted(
        name
        for name in _class_selectors(app_css)
        if any(name.startswith(f) or f"-{f}" in name for f in families)
        # A substring match is deliberately generous: if `tpair-leg`
        # appears anywhere the class `pair-leg` is treated as live, so
        # this can only ever under-report.
        and name not in used
    )

    assert not dead, (
        "app.css still styles the replaced review UI. Every one of these "
        f"selectors matches nothing any template or script renders: {dead}"
    )


def test_the_redesigned_triage_styles_live_in_their_own_sheet() -> None:
    """triage.css owns the new surface; app.css must not grow a second copy."""
    app_css = (CSS / "app.css").read_text(encoding="utf-8")

    assert ".triage-row" not in app_css
    assert ".tmodal" not in app_css
    assert ".catpicker" not in app_css


# ---------------------------------------------------------------------------
# L5 — no unresolved custom property
# ---------------------------------------------------------------------------


def _defined_properties() -> set[str]:
    """Every ``--name`` a stylesheet declares, plus the inline ones.

    A template may set a custom property in a ``style`` attribute — the
    monthly pivot passes its column count that way — and that is a real
    definition even though no stylesheet holds it.
    """
    defined: set[str] = set()
    for path in CSS.glob("*.css"):
        defined.update(
            re.findall(r"(--[\w-]+)\s*:", path.read_text(encoding="utf-8"))
        )
    for path in TEMPLATES.rglob("*.html"):
        defined.update(
            re.findall(
                r'style="[^"]*?(--[\w-]+)\s*:', path.read_text(encoding="utf-8")
            )
        )
    return defined


#: tailwind.css is a vendored extract with no build step and is not ours to
#: fix; its ``--tw-*`` properties are set by the utility classes themselves,
#: at runtime, and never appear as a declaration in the sheet.
_HAND_WRITTEN = sorted(
    p.name for p in CSS.glob("*.css") if p.name != "tailwind.css"
)


@pytest.mark.parametrize("sheet", _HAND_WRITTEN, ids=lambda n: n)
def test_every_custom_property_a_stylesheet_reads_is_defined(sheet: str) -> None:
    """An undefined token renders nothing, silently.

    ``var(--x, fallback)`` is exempt: the fallback IS the definition, and
    that is how a sheet legitimately reads a property a template sets.
    """
    css = (CSS / sheet).read_text(encoding="utf-8")
    defined = _defined_properties()

    unresolved = sorted(
        {
            name
            for name in re.findall(r"var\(\s*(--[\w-]+)\s*\)", css)
            if name not in defined
        }
    )

    assert not unresolved, (
        f"{sheet} reads custom properties nothing defines, which render as "
        f"nothing at all rather than as an error: {unresolved}"
    )


# ---------------------------------------------------------------------------
# L5 — the favicon 404
# ---------------------------------------------------------------------------


def test_the_page_declares_an_icon_so_the_browser_stops_probing() -> None:
    """Without a declared icon every browser requests /favicon.ico and 404s."""
    head = (TEMPLATES / "base.html").read_text(encoding="utf-8")

    assert 'rel="icon"' in head
    assert "/static/favicon.svg" in head
    assert (STATIC / "favicon.svg").exists()


def test_the_favicon_is_served_and_is_local(web_client_factory) -> None:
    with web_client_factory() as client:
        response = client.get("/static/favicon.svg")

    assert response.status_code == 200
    assert "svg" in response.headers["content-type"]
    # ``xmlns`` is an identifier, not a fetch; anything that would actually
    # be requested is not allowed (the offline rule).
    body = response.text.replace('xmlns="http://www.w3.org/2000/svg"', "")
    assert "http://" not in body
    assert "https://" not in body


# ---------------------------------------------------------------------------
# I9 — motion on the one raised surface app.css owns
# ---------------------------------------------------------------------------


def test_the_toast_rises_and_collapses_under_reduced_motion() -> None:
    """6px up and a fade, on the lift curve, like the modal and the bar.

    The keyframes are declared in app.css rather than borrowed from
    triage.css: the toast shows on every page, and a shared component must
    not depend on a sheet that exists for one screen.
    """
    css = (CSS / "app.css").read_text(encoding="utf-8")

    block = css[css.index("\n.toast {") :]
    block = block[: block.index("\n.toast-success")]
    assert "animation:" in block
    assert "--dur-base" in block
    assert "--ease-lift" in block

    keyframes = css[css.index("@keyframes toast-rise") :]
    keyframes = keyframes[: keyframes.index("}\n}") + 3]
    assert "translateY(6px)" in keyframes
    assert "opacity: 0" in keyframes


# ---------------------------------------------------------------------------
# L5 — the console, continued
# ---------------------------------------------------------------------------


def test_the_save_form_disables_a_button_that_actually_exists(
    triage_web_db, web_client_factory
) -> None:
    """``hx-disabled-elt`` must resolve, or htmx logs on every save.

    The design puts the primary button in the FOOTER, outside the form it
    submits; it associates by ``form=`` id instead. ``find
    button[type=submit]`` searches inside the form, finds nothing, and
    htmx prints *The selector "find button[type=submit]" on
    hx-disabled-elt returned no matches!* — twice per double-submit —
    while the save itself works perfectly. Every server-side test stayed
    green; a browser was the only witness.
    """
    with web_client_factory() as client:
        html = client.get("/_partial/triage/1/modal").text

    assert "find button[type=submit]" not in html
    assert 'hx-disabled-elt="[data-modal-primary]"' in html
    # And the thing it names is really there, in the footer.
    assert "data-modal-primary" in html
    assert 'form="triage-decision-form"' in html
