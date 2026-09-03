"""Plans and Ahead — the two placeholders below the rail's hairline.

The rail lists them so it is honest about the roadmap; each page is a
SIGNAL empty state that says plainly what has to exist before the page
can (REPO-RECONCILE §B1): Plans needs a plan table over sorted data,
Ahead needs assumptions derived from history. No fake data, no dead
controls, and none of the prototype's inventions (§B2).

Markup-level by nature. The two things a browser alone can see — the
Doto headline resolving at 26px, the column centring — are on the
checklist in ``docs/plans/redesign/shell-notes.md``.
"""

from __future__ import annotations

import inspect
import re
import sqlite3
from collections.abc import Callable
from pathlib import Path

import pytest
from starlette.testclient import TestClient

ROOT = Path(__file__).resolve().parents[2]
TEMPLATES = ROOT / "finances" / "web" / "templates"
CSS = ROOT / "finances" / "web" / "static" / "css"

#: path, rail label, template, the §B1 phrase, the icon, the one real link.
PLACEHOLDERS = (
    ("/plans", "Plans", "pages/plans.html", "plan table", "target", "/triage"),
    ("/ahead", "Ahead", "pages/ahead.html", "assumptions", "route", "/monthly"),
)

#: §B2 inventions and the marketing register, none of which may appear.
FORBIDDEN = (
    "coming soon",
    "mortgage",
    "credit card",
    "bank says",
    "notification",
    "budget",
)


def _page(factory: Callable[[], TestClient], path: str) -> str:
    with factory() as client:
        response = client.get(path)
    assert response.status_code == 200, path
    return response.text


def _rail(html: str) -> str:
    start = html.index('<nav class="rail"')
    return html[start : html.index("</nav>", start)]


def _content(html: str) -> str:
    """The content column only — the rail and the base scripts excluded."""
    start = html.index("<main")
    return html[start : html.index("</main>", start)]


# ---------------------------------------------------------------------------
# The routes exist, and render literal templates
# ---------------------------------------------------------------------------


def test_the_two_routes_are_appended_to_the_pages_router() -> None:
    from finances.web.routers.pages import router

    by_path = {route.path: route for route in router.routes}

    for path, _label, template, *_ in PLACEHOLDERS:
        assert path in by_path, f"{path} is not a route"
        source = inspect.getsource(by_path[path].endpoint)
        # A literal name is what lets test_template_contract see the call.
        assert f'"{template}"' in source


# ---------------------------------------------------------------------------
# Inside the shell, with the rail marking the destination
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("path", "label"),
    [(p, l) for p, l, *_ in PLACEHOLDERS],
    ids=("plans", "ahead"),
)
def test_each_placeholder_renders_inside_the_shell(
    web_db: sqlite3.Connection,
    web_client_factory: Callable[[], TestClient],
    path: str,
    label: str,
) -> None:
    body = _page(web_client_factory, path)

    assert f"<title>{label}</title>" in body
    assert re.search(r'<body\s+class="shell"', body)
    rail = _rail(body)
    marked = re.findall(r'<a class="rail-link[^"]*"[^>]*aria-current="page"', rail)
    assert len(marked) == 1, "exactly one rail destination is current"
    assert f'href="{path}"' in marked[0]
    assert '<main class="shell-content">' in body


# ---------------------------------------------------------------------------
# One answer, one empty headline, the §B1 reason, one real link
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("path", "question", "answer", "headline", "phrase", "icon", "link"),
    [
        (
            "/plans",
            "Where is the money going next?",
            "No plans yet",
            "Plans need a plan table.",
            "plan table",
            "target",
            "/triage",
        ),
        (
            "/ahead",
            "What happens if nothing changes?",
            "Nothing to project yet",
            "Ahead needs assumptions from history.",
            "assumptions",
            "route",
            "/monthly",
        ),
    ],
    ids=("plans", "ahead"),
)
def test_each_placeholder_is_one_signal_empty_state(
    web_db: sqlite3.Connection,
    web_client_factory: Callable[[], TestClient],
    path: str,
    question: str,
    answer: str,
    headline: str,
    phrase: str,
    icon: str,
    link: str,
) -> None:
    content = _content(_page(web_client_factory, path))

    # The header: the question as the kicker, the state as THE Doto answer.
    assert f'<span class="page-question">{question}</span>' in content
    assert content.count('class="page-answer"') == 1
    assert f'<h1 class="page-answer">{answer}</h1>' in content

    # The empty state: icon, one Doto headline, prose, in the triage idiom.
    empty = re.search(
        r'<section class="placeholder-empty"[^>]*>.*?</section>', content, re.DOTALL
    )
    assert empty, "no .placeholder-empty block"
    block = empty.group(0)
    assert f'data-icon="{icon}"' in block
    assert block.count('class="placeholder-empty-headline"') == 1
    assert content.count("placeholder-empty-headline") == 1
    assert re.search(
        rf'<h2 class="placeholder-empty-headline"[^>]*>{re.escape(headline)}</h2>',
        block,
    )
    assert phrase in block
    bodies = re.findall(
        r'<p class="placeholder-empty-body">(.*?)</p>', block, re.DOTALL
    )
    assert 1 <= len(bodies) <= 2
    for paragraph in bodies:
        sentences = [s for s in re.split(r"(?<=[.?])\s+", paragraph.strip()) if s]
        assert len(sentences) <= 2, (
            f"more than two sentences before a break: {paragraph!r}"
        )

    # The one real thing the owner can do now — and nothing else.
    links = re.findall(r'<a class="tlink" href="([^"]+)">([^<]+)</a>', block)
    assert [href for href, _ in links] == [link]
    assert len(links[0][1].split()) <= 3, "link labels are three words or fewer"
    assert "<button" not in block
    assert "<form" not in block
    assert "hx-" not in block


# ---------------------------------------------------------------------------
# Nothing invented, nothing marketed
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "path", [p for p, *_ in PLACEHOLDERS], ids=("plans", "ahead")
)
def test_no_b2_invention_and_no_marketing_register(
    web_db: sqlite3.Connection,
    web_client_factory: Callable[[], TestClient],
    path: str,
) -> None:
    content = _content(_page(web_client_factory, path)).lower()

    for word in FORBIDDEN:
        assert word not in content, f"{path} says {word!r}"


@pytest.mark.parametrize(
    "template", [t for _, _, t, *_ in PLACEHOLDERS], ids=("plans", "ahead")
)
def test_the_template_source_is_calm(template: str) -> None:
    """No exclamation marks, no emoji, no Tailwind, no borrowed triage class."""
    source = (TEMPLATES / template).read_text(encoding="utf-8")

    assert "!" not in source
    assert not any(
        ord(ch) >= 0x1F000 or 0x2600 <= ord(ch) <= 0x27BF for ch in source
    )
    assert 'extends "base.html"' in source
    assert "triage-empty" not in source, "mirror the idiom; do not reuse its classes"

    used: set[str] = set()
    for value in re.findall(r'(?<![-:\w])class="([^"]*)"', source):
        used.update(re.sub(r"\{[%{].*?[%}]\}", " ", value).split())
    allowed = {"tlink"}
    stray = sorted(
        c for c in used if c not in allowed and not c.startswith("placeholder-")
    )
    assert not stray, f"{template} uses classes outside its own prefix: {stray}"


# ---------------------------------------------------------------------------
# The sheet mirrors the triage empty state, on tokens, under one prefix
# ---------------------------------------------------------------------------


def _rule(css: str, selector: str) -> str:
    start = css.index(f"\n{selector} {{")
    return css[start : css.index("}", start)]


def _selectors(css: str) -> set[str]:
    without_comments = re.sub(r"/\*.*?\*/", "", css, flags=re.DOTALL)
    names: set[str] = set()
    for chunk in without_comments.split("}"):
        if "{" not in chunk:
            continue
        names.update(re.findall(r"\.([\w-]+)", chunk[: chunk.index("{")]))
    return names


def test_placeholders_css_is_the_triage_empty_state_under_its_own_prefix() -> None:
    css = (CSS / "placeholders.css").read_text(encoding="utf-8")

    stray = sorted(s for s in _selectors(css) if not s.startswith("placeholder-"))
    assert not stray, f"placeholders.css styles outside its prefix: {stray}"

    empty = _rule(css, ".placeholder-empty")
    assert "display: flex;" in empty
    assert "flex-direction: column;" in empty
    assert "align-items: center;" in empty
    assert "gap: var(--space-4);" in empty
    assert "padding: var(--space-8)" in empty
    assert "text-align: center;" in empty

    headline = _rule(css, ".placeholder-empty-headline")
    assert "font-family: var(--font-display);" in headline
    assert "font-size: var(--display-3-size);" in headline
    assert "font-weight: 500;" in headline
    assert "margin: 0;" in headline

    body = _rule(css, ".placeholder-empty-body")
    assert "max-width: 460px;" in body
    assert "font-size: var(--body-size);" in body
    assert "line-height: 1.55;" in body
    assert "color: var(--text-secondary);" in body

    # No literal colour or font face in a declaration: tokens only.
    declarations = re.sub(r"/\*.*?\*/", "", css, flags=re.DOTALL)
    assert not re.search(r"#[0-9a-fA-F]{3,6}\b", declarations)
    assert "Doto" not in declarations
