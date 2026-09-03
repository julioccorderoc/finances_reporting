"""The SIGNAL shell — the rail, the width cap and the page header.

The Triage redesign restyled one surface and left it inside the old top
nav, which reads as "new design, wrongly implemented". This file pins the
shell every page now renders inside:

* the 244px rail (``partials/rail.html``) replaces the top nav on every
  page, with Triage's blocking count as a live mono badge;
* the content column is capped at the design frame's width
  (1440 − 244 = 1196px) so nothing stretches across a wide monitor;
* ``page_header(question, answer, meta)`` is the one way a page opens.

Markup-level assertions by nature: what a browser does with the rail at
2560px is verified in a browser and written up in
``docs/plans/redesign/shell-notes.md``.
"""

from __future__ import annotations

import re
import sqlite3
from collections.abc import Callable
from pathlib import Path

import pytest
from jinja2 import Environment, FileSystemLoader, StrictUndefined
from markupsafe import Markup
from starlette.testclient import TestClient

from finances.web.services.triage import build_queue, count_blocking

ROOT = Path(__file__).resolve().parents[2]
TEMPLATES = ROOT / "finances" / "web" / "templates"
CSS = ROOT / "finances" / "web" / "static" / "css"

#: Every full page the viewer serves, in rail order.
PAGES = ("/triage", "/", "/transactions", "/monthly", "/accounts", "/rates")

#: The rail's destinations, top to bottom. Plans and Ahead are the
#: placeholders below the hairline (Phase 2d adds their routes).
DESTINATIONS = (
    ("/triage", "Triage"),
    ("/", "Today"),
    ("/transactions", "Flow"),
    ("/monthly", "Monthly"),
    ("/accounts", "Accounts"),
    ("/plans", "Plans"),
    ("/ahead", "Ahead"),
)


def _page(factory: Callable[[], TestClient], path: str = "/") -> str:
    with factory() as client:
        response = client.get(path)
    assert response.status_code == 200, path
    return response.text


def _rail(html: str) -> str:
    start = html.index('<nav class="rail"')
    return html[start : html.index("</nav>", start)]


# ---------------------------------------------------------------------------
# The rail replaces the top nav, on every page
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("path", PAGES)
def test_every_page_renders_inside_the_rail(
    seeded_web_db: sqlite3.Connection,
    web_client_factory: Callable[[], TestClient],
    path: str,
) -> None:
    body = _page(web_client_factory, path)

    assert '<nav class="rail"' in body
    assert 'aria-label="Primary"' in _rail(body)
    # The old top nav is gone, not hidden.
    assert "max-w-6xl" not in body
    assert ">Dashboard<" not in body
    assert '<body class="shell"' in body


def test_the_rail_lists_the_destinations_in_order(
    seeded_web_db: sqlite3.Connection,
    web_client_factory: Callable[[], TestClient],
) -> None:
    rail = _rail(_page(web_client_factory))

    positions = []
    for href, label in DESTINATIONS:
        match = re.search(
            rf'<a class="rail-link[^"]*"[^>]*href="{re.escape(href)}"[^>]*>.*?{label}',
            rail,
            flags=re.DOTALL,
        )
        assert match, f"{label} ({href}) is not a rail destination"
        positions.append(match.start())
    assert positions == sorted(positions), "destinations are out of order"

    # A hairline separates the five live destinations from the two
    # placeholders, and every destination carries an icon.
    accounts = rail.index('href="/accounts"')
    plans = rail.index('href="/plans"')
    assert accounts < rail.index('class="rail-divider"') < plans
    assert len(re.findall(r"<a class=\"rail-link[^>]*>\s*<svg", rail)) == len(
        DESTINATIONS
    )


@pytest.mark.parametrize(
    ("path", "current"),
    [
        ("/", "/"),
        ("/triage", "/triage"),
        ("/transactions", "/transactions"),
        ("/transactions?accounts=Provincial", "/transactions"),
        ("/monthly", "/monthly"),
        ("/accounts", "/accounts"),
    ],
)
def test_the_active_destination_is_marked_and_nothing_else_is(
    seeded_web_db: sqlite3.Connection,
    web_client_factory: Callable[[], TestClient],
    path: str,
    current: str,
) -> None:
    """Weight and ink, not a fill — and ``aria-current`` for everyone else."""
    rail = _rail(_page(web_client_factory, path))

    marked = re.findall(r'<a class="rail-link[^"]*"[^>]*aria-current="page"', rail)
    assert len(marked) == 1
    assert f'href="{current}"' in marked[0]


def test_rates_is_demoted_out_of_the_destinations(
    seeded_web_db: sqlite3.Connection,
    web_client_factory: Callable[[], TestClient],
) -> None:
    rail = _rail(_page(web_client_factory, "/rates"))

    assert 'class="rail-link' not in rail.split('href="/rates"')[0][-40:]
    assert re.search(r'<a class="rail-minor-link"[^>]*href="/rates"', rail)
    # Still marked when you are there.
    assert re.search(r'href="/rates"[^>]*aria-current="page"', rail) or re.search(
        r'aria-current="page"[^>]*href="/rates"', rail
    )


# ---------------------------------------------------------------------------
# The footer: upload first, rates demoted, stop restyled
# ---------------------------------------------------------------------------


def test_upload_a_statement_is_a_first_class_rail_control(
    seeded_web_db: sqlite3.Connection,
    web_client_factory: Callable[[], TestClient],
) -> None:
    """The design brief's non-negotiable: reachable from every page.

    It links the EXISTING Provincial drop flow — no new upload code —
    and the query flag opens the panel on arrival.
    """
    rail = _rail(_page(web_client_factory, "/monthly"))

    upload = re.search(r'<a class="rail-upload"[^>]*>.*?</a>', rail, flags=re.DOTALL)
    assert upload, "no upload control in the rail"
    assert 'href="/transactions?upload=1#upload"' in upload.group(0)
    assert "Upload a statement" in upload.group(0)
    assert 'data-icon="upload"' in upload.group(0)


def test_the_upload_flag_opens_the_drop_panel_where_the_rail_lands(
    seeded_web_db: sqlite3.Connection,
    web_client_factory: Callable[[], TestClient],
) -> None:
    closed = _page(web_client_factory, "/transactions")
    opened = _page(web_client_factory, "/transactions?upload=1")

    assert 'id="upload"' in closed
    assert not re.search(r'<details[^>]*id="upload"[^>]*\sopen', closed)
    assert re.search(r'<details[^>]*id="upload"[^>]*\sopen', opened)


def test_the_stop_control_still_stops_the_server(
    seeded_web_db: sqlite3.Connection,
    web_client_factory: Callable[[], TestClient],
) -> None:
    rail = _rail(_page(web_client_factory))

    form = re.search(r"<form[^>]*action=\"/shutdown\"[^>]*>.*?</form>", rail, re.DOTALL)
    assert form, "the stop-server form left the rail"
    assert 'method="post"' in form.group(0)
    assert "confirm(" in form.group(0)
    assert 'class="rail-stop"' in form.group(0)
    assert "Stop server" in form.group(0)


# ---------------------------------------------------------------------------
# The Triage badge: live, mono, and cheap
# ---------------------------------------------------------------------------


def test_the_triage_badge_is_the_blocking_count(
    triage_web_db: sqlite3.Connection,
    web_client_factory: Callable[[], TestClient],
) -> None:
    """Seven rows block on the fixture (six categories, one pair); the
    approximate row does not count, exactly as the /triage header says."""
    rail = _rail(_page(web_client_factory, "/accounts"))

    badge = re.search(r'<span id="rail-triage-count"[^>]*>([^<]*)</span>', rail)
    assert badge, "no badge element in the rail"
    assert badge.group(1).strip() == "7"
    assert 'class="rail-count"' in badge.group(0)


def test_the_badge_is_empty_rather_than_zero_when_nothing_blocks(
    web_db: sqlite3.Connection,
    web_client_factory: Callable[[], TestClient],
) -> None:
    rail = _rail(_page(web_client_factory))

    badge = re.search(r'<span id="rail-triage-count"[^>]*>([^<]*)</span>', rail)
    assert badge
    assert badge.group(1).strip() == ""


def test_count_blocking_agrees_with_the_full_queue_build(
    triage_web_db: sqlite3.Connection,
) -> None:
    """The badge must never disagree with the header it points at.

    ``build_queue`` prices every VES row to find the approximate ones;
    the badge only needs the categories and the pairs, so it takes a
    cheaper path — and this is what keeps the two honest.
    """
    queue = build_queue(triage_web_db)

    assert count_blocking(triage_web_db) == queue.blocking_count == 7

    pair = next(item for item in queue.items if item.item_id.startswith("pair:"))
    dismissed = {pair.item_id}
    assert (
        count_blocking(triage_web_db, dismissed=dismissed)
        == build_queue(triage_web_db, dismissed=dismissed).blocking_count
        == 6
    )


def test_count_blocking_ignores_parked_transfers_and_adjustments(
    web_db: sqlite3.Connection,
) -> None:
    from datetime import UTC, datetime
    from decimal import Decimal

    from finances.db.repos import accounts as accounts_repo
    from finances.db.repos import transactions as transactions_repo
    from finances.domain.models import (
        Account,
        AccountKind,
        Transaction,
        TransactionKind,
    )

    bank = accounts_repo.insert(
        web_db, Account(name="Provincial", kind=AccountKind.BANK, currency="VES")
    )
    when = datetime(2026, 7, 3, tzinfo=UTC)
    for n, (kind, parked) in enumerate(
        (
            (TransactionKind.EXPENSE, False),
            (TransactionKind.EXPENSE, True),
            (TransactionKind.TRANSFER, False),
            (TransactionKind.ADJUSTMENT, False),
        )
    ):
        transactions_repo.insert(
            web_db,
            Transaction(
                account_id=bank.id,
                occurred_at=when,
                kind=kind,
                amount=Decimal("-10.00"),
                currency="VES",
                description=f"row {n}",
                source="provincial",
                source_ref=f"blocking-{n}",
                parked=parked,
            ),
        )

    assert count_blocking(web_db) == 1


def test_a_queue_swap_refreshes_the_badge_out_of_band(
    triage_web_db: sqlite3.Connection,
    web_client_factory: Callable[[], TestClient],
) -> None:
    """The badge lives outside ``#triage-queue``; a sitting changes it.

    The queue partial carries an out-of-band twin of the badge ONLY when
    htmx asked for it — the full page includes the same partial, and two
    elements with one id is the bug this guards against.
    """
    with web_client_factory() as client:
        page = client.get("/triage").text
        swapped = client.get(
            "/_partial/triage/queue", headers={"HX-Request": "true"}
        ).text

    assert page.count('id="rail-triage-count"') == 1
    oob = re.search(r'<span id="rail-triage-count"[^>]*>([^<]*)</span>', swapped)
    assert oob, "the queue swap does not refresh the rail badge"
    assert 'hx-swap-oob="true"' in oob.group(0)
    assert oob.group(1).strip() == "7"


# ---------------------------------------------------------------------------
# The width cap
# ---------------------------------------------------------------------------


def test_shell_css_caps_the_content_column_at_the_design_frame() -> None:
    """1440 − 244: the design's own LaptopFrame minus its rail."""
    shell = (CSS / "shell.css").read_text(encoding="utf-8")

    assert "--content-cap: 1196px;" in shell
    assert "--rail-width: 244px;" in shell
    content = shell[shell.index("\n.shell-content {") :]
    content = content[: content.index("}")]
    assert "max-width: var(--content-cap)" in content
    assert "margin: 0 auto" in content
    cap = shell[shell.index("\n.shell-cap {") :]
    cap = cap[: cap.index("}")]
    assert "max-width: var(--content-cap)" in cap


def test_base_html_links_shell_css_after_the_tokens() -> None:
    head = (TEMPLATES / "base.html").read_text(encoding="utf-8")

    assert '<link rel="stylesheet" href="/static/css/shell.css">' in head
    assert head.index("/static/css/signal.css") < head.index("/static/css/shell.css")
    assert head.index("/static/css/shell.css") < head.index("/static/css/triage.css")


def test_the_triage_queue_is_capped_inside_a_full_width_scroller(
    triage_web_db: sqlite3.Connection,
    web_client_factory: Callable[[], TestClient],
) -> None:
    """The scroller spans the column so its scrollbar sits at the edge;
    the cap is the swap target inside it, so a queue refresh keeps it."""
    body = _page(web_client_factory, "/triage")

    assert re.search(
        r'<div class="triage-scroll">\s*<div id="triage-queue" class="shell-cap">',
        body,
    )
    assert 'class="triage-main"' in body


def test_triage_no_longer_assumes_a_55px_top_nav() -> None:
    triage = (CSS / "triage.css").read_text(encoding="utf-8")

    assert "calc(100vh - 55px)" not in triage
    screen = triage[triage.index("\n.triage-screen {") :]
    screen = screen[: screen.index("}")]
    assert "height: 100vh;" in screen


def test_the_modal_overlay_still_covers_only_the_content_area() -> None:
    """B11: absolute over the screen, never fixed over the rail."""
    triage = (CSS / "triage.css").read_text(encoding="utf-8")

    tover = triage[triage.index("\n.tover {") :]
    tover = tover[: tover.index("}")]
    assert "position: absolute;" in tover


# ---------------------------------------------------------------------------
# The page header macro
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def render() -> Callable[[str], str]:
    env = Environment(
        loader=FileSystemLoader(str(TEMPLATES)),
        undefined=StrictUndefined,
        autoescape=True,
    )

    def _render(source: str, **context: object) -> str:
        return env.from_string(source).render(**context)

    return _render


def test_page_header_is_a_question_over_one_answer(render) -> None:
    html = render(
        '{% from "_macros.html" import page_header %}'
        '{{ page_header("Where do I stand?", "$1,284.60") }}'
    )

    assert '<header class="page-header">' in html
    assert '<span class="page-question">Where do I stand?</span>' in html
    assert '<h1 class="page-answer">$1,284.60</h1>' in html
    assert "page-meta" not in html
    assert "page-actions" not in html


def test_page_header_meta_is_markup_and_actions_come_from_the_call_block(
    render,
) -> None:
    html = render(
        '{% from "_macros.html" import page_header %}'
        "{% set meta %}<span class=\"tbadge\">3 accounts</span>{% endset %}"
        '{% call page_header("What do I hold?", "$9.00", meta) %}'
        '<button class="tbtn">Add</button>'
        "{% endcall %}"
    )

    assert '<div class="page-meta"><span class="tbadge">3 accounts</span></div>' in html
    assert '<div class="page-actions"><button class="tbtn">Add</button></div>' in html


def test_page_header_escapes_a_plain_string_answer(render) -> None:
    html = render(
        '{% from "_macros.html" import page_header %}{{ page_header(q, a) }}',
        q="<b>q</b>",
        a="<i>a</i>",
    )

    assert "&lt;b&gt;q&lt;/b&gt;" in html
    assert "&lt;i&gt;a&lt;/i&gt;" in html


def test_page_header_answer_is_the_one_doto_figure() -> None:
    shell = (CSS / "shell.css").read_text(encoding="utf-8")

    answer = shell[shell.index("\n.page-answer {") :]
    answer = answer[: answer.index("}")]
    assert "font-family: var(--font-display)" in answer
    assert "font-size: var(--display-2-size)" in answer
    question = shell[shell.index("\n.page-question {") :]
    question = question[: question.index("}")]
    assert "font-family: var(--font-mono)" in question
    assert "text-transform: uppercase" in question


def test_markup_survives_the_macro(render) -> None:
    html = render(
        '{% from "_macros.html" import page_header %}{{ page_header("q", a) }}',
        a=Markup('<span class="tmoney-usd">$1.00</span>'),
    )

    assert '<span class="tmoney-usd">$1.00</span>' in html
