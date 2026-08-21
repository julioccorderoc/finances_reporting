"""The viewer must be fully styled with the network unplugged.

tailwind.css was extracted offline for exactly this reason, and the same
rule binds the SIGNAL foundation: a Google Fonts ``@import`` is a
prototyping convenience, and it fails silently — the page still renders,
in a fallback face, and every server-side test stays green.

So this file asserts the two halves that a browser would otherwise be the
only witness to: no template or stylesheet reaches an external host, and
every ``url()`` in fonts.css points at a file that is actually committed.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
STATIC = ROOT / "finances" / "web" / "static"
TEMPLATES = ROOT / "finances" / "web" / "templates"

#: Hosts a stylesheet or template must never reference. Anything served from
#: outside this repo is a request that fails when the Wi-Fi is off.
FORBIDDEN = (
    "fonts.googleapis.com",
    "fonts.gstatic.com",
    "cdn.jsdelivr.net",
    "unpkg.com",
    "cdnjs.cloudflare.com",
    "cdn.tailwindcss.com",
)


def _scanned_files() -> list[Path]:
    return sorted(TEMPLATES.rglob("*.html")) + sorted((STATIC / "css").glob("*.css"))


@pytest.mark.parametrize("path", _scanned_files(), ids=lambda p: p.name)
def test_no_template_or_stylesheet_calls_out_to_a_cdn(path: Path) -> None:
    text = path.read_text(encoding="utf-8")
    # Comments explaining why we do NOT use these hosts are legitimate; a
    # url()/href/src/@import that resolves to one is not.
    live = re.findall(
        r"""(?:url\(|href=|src=|@import\s+url\(|@import\s+)["']?(https?://[^"'\s)]+)""",
        text,
    )
    offenders = [u for u in live if any(host in u for host in FORBIDDEN)]

    assert not offenders, (
        f"{path.relative_to(ROOT)} loads assets from an external host: {offenders}"
    )


def test_base_html_links_the_signal_foundation() -> None:
    head = (TEMPLATES / "base.html").read_text(encoding="utf-8")

    assert '<link rel="stylesheet" href="/static/css/fonts.css">' in head
    assert '<link rel="stylesheet" href="/static/css/signal.css">' in head
    # After the existing sheets: tokens and faces must not be overridden by a
    # stylesheet that loads later.
    assert head.index("/static/css/tailwind.css") < head.index("/static/css/fonts.css")
    assert head.index("/static/css/app.css") < head.index("/static/css/signal.css")


def test_every_vendored_font_file_exists() -> None:
    css = (STATIC / "css" / "fonts.css").read_text(encoding="utf-8")
    refs = re.findall(r'url\("(/static/[^"]+)"\)', css)

    assert refs, "fonts.css declares no @font-face src"
    missing = [r for r in refs if not (STATIC / r.removeprefix("/static/")).is_file()]

    assert not missing, f"fonts.css points at files that are not committed: {missing}"


def test_the_three_families_are_all_declared() -> None:
    css = (STATIC / "css" / "fonts.css").read_text(encoding="utf-8")

    for family in ("Doto", "Inter", "JetBrains Mono"):
        assert f'font-family: "{family}"' in css, f"{family} has no @font-face"
    # A blocked first paint on a ledger you opened to read is the worse trade.
    assert css.count("font-display: swap") == css.count("@font-face")


def test_signal_css_is_tokens_and_nothing_else() -> None:
    """Wave 2 owns component styles; this file stays a variable sheet.

    A class rule landing here would sit outside app.css, where the class
    guard and everyone's expectations live.
    """
    css = (STATIC / "css" / "signal.css").read_text(encoding="utf-8")
    stripped = re.sub(r"/\*.*?\*/", "", css, flags=re.DOTALL)
    selectors = re.findall(r"([^{}]+)\{", stripped)
    allowed = {":root", ":focus-visible", "@media (prefers-reduced-motion: reduce)"}
    unexpected = [s.strip() for s in selectors if s.strip() not in allowed]

    assert not unexpected, f"signal.css grew non-token rules: {unexpected}"


def test_signal_css_carries_the_focus_ring_and_reduced_motion() -> None:
    css = (STATIC / "css" / "signal.css").read_text(encoding="utf-8")

    assert "--focus-ring: 0 0 0 2px var(--paper-000), 0 0 0 4px var(--red-500);" in css
    assert "@media (prefers-reduced-motion: reduce)" in css
    reduced = css.split("@media (prefers-reduced-motion: reduce)")[1]
    for token in ("--dur-instant", "--dur-fast", "--dur-base", "--dur-slow"):
        assert f"{token}: 1ms;" in reduced, f"{token} does not collapse under reduce"
