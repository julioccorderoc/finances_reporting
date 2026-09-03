"""Today (``/``) after the viewer reskin (2026-09).

The page is a reskin of the dashboard, not a rethink: same services, same
endpoints, same structure. What changes is the sheet it wears — SIGNAL via
``today.css`` on ``signal.css``'s tokens — and the shell contract it now
obeys (``docs/plans/redesign/shell-notes.md``):

* it opens with ``page_header("Where do I stand?", <net worth>)``, and that
  figure is the ONE Doto answer on the page;
* the needs-review tile is a card that says how many rows need you and
  still links exactly where ``test_dashboard`` says it must;
* the source chips keep ``data-source`` / ``data-severity`` / ``data-status``
  and stop borrowing the Tailwind ``sync_chip`` macro;
* the chart payload rides in a ``<script type="application/json">`` block,
  never ``tojson`` inside an attribute (the trap that truncates JS while
  every server test stays green);
* recent activity wraps the canonical card partial in ``.flow-rows``;
* not one Tailwind utility is left in the five templates the page owns.
"""

from __future__ import annotations

import json
import re
import sqlite3
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

from finances.db.repos import accounts as accounts_repo
from finances.db.repos import categories as categories_repo
from finances.db.repos import transactions as transactions_repo
from finances.domain.models import Account, AccountKind, Transaction, TransactionKind
from finances.web.services.dashboard import build_kpis

ROOT = Path(__file__).resolve().parents[2]
TEMPLATES = ROOT / "finances" / "web" / "templates"
CSS = ROOT / "finances" / "web" / "static" / "css"

#: The five templates Today owns; nothing else on the page is this track's.
TODAY_TEMPLATES = (
    "pages/dashboard.html",
    "partials/kpi_tiles.html",
    "partials/sync_status_strip.html",
    "partials/recent_activity.html",
    "partials/flows_chart.html",
)


def _page(client) -> str:
    resp = client.get("/")
    assert resp.status_code == 200
    return resp.text


def _seed_flagged_rows(conn: sqlite3.Connection, n: int) -> None:
    today = datetime.now(tz=UTC)
    cash = accounts_repo.insert(
        conn, Account(name="Cash USD", kind=AccountKind.CASH, currency="USD")
    )
    food = categories_repo.get_by_name(conn, TransactionKind.EXPENSE, "Groceries")
    assert food is not None
    for i in range(n):
        transactions_repo.insert(
            conn,
            Transaction(
                account_id=cash.id,
                occurred_at=today,
                kind=TransactionKind.EXPENSE,
                amount=Decimal("-1.00"),
                currency="USD",
                description=f"flagged-{i}",
                category_id=food.id,
                source="cash_cli",
                source_ref=f"flagged-{i}",
                needs_review=True,
            ),
        )


# ---------------------------------------------------------------------------
# The header: one question, one answer.
# ---------------------------------------------------------------------------


def test_today_opens_with_the_question_and_the_real_net_worth(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    """The Doto answer is the USDT net worth build_kpis computes — the same
    string the KPI API returns — not a number the template made up."""
    kpis = build_kpis(seeded_web_db, today=datetime.now(tz=UTC).date())
    body = _page(web_client_factory())

    assert '<span class="page-question">Where do I stand?</span>' in body
    assert f'<h1 class="page-answer">{kpis.net_worth.value}</h1>' in body
    # The hint (Bank · Crypto · Cash) rides in the header's meta row.
    assert kpis.net_worth.hint is not None
    meta = body.split('<div class="page-meta">', 1)[1].split("</div>", 1)[0]
    assert kpis.net_worth.hint in meta
    # The old intro is gone.
    assert "Headline net worth (USDT-denominated)" not in body


def test_today_has_exactly_one_page_answer(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    """At most one Doto figure per view — a second one is the design broken."""
    body = _page(web_client_factory())

    assert body.count('class="page-answer"') == 1
    # No other surface's display class leaks onto this page.
    assert "triage-answer" not in body
    assert "triage-empty-headline" not in body


def test_missing_pair_warning_is_shown_not_hidden(
    web_db: sqlite3.Connection, web_client_factory
) -> None:
    """A VES balance with no P2P rate is the honest state: a warning badge
    in the header meta, on the warning trio, never a red fill."""
    today = datetime.now(tz=UTC)
    provincial = accounts_repo.insert(
        web_db, Account(name="Provincial", kind=AccountKind.BANK, currency="VES")
    )
    food = categories_repo.get_by_name(web_db, TransactionKind.EXPENSE, "Groceries")
    assert food is not None
    transactions_repo.insert(
        web_db,
        Transaction(
            account_id=provincial.id,
            occurred_at=today,
            kind=TransactionKind.EXPENSE,
            amount=Decimal("-100.00"),
            currency="VES",
            description="unpriced",
            category_id=food.id,
            source="provincial",
            source_ref="unpriced",
        ),
    )

    body = _page(web_client_factory())
    meta = body.split('<div class="page-meta">', 1)[1].split("</div>", 1)[0]
    assert 'class="tbadge tbadge-warning tbadge-dot"' in meta
    assert "VES→USDT" in meta


def test_no_warning_badge_when_every_balance_is_priced(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    body = _page(web_client_factory())
    meta = body.split('<div class="page-meta">', 1)[1].split("</div>", 1)[0]
    assert "tbadge-warning" not in meta


# ---------------------------------------------------------------------------
# Tiles and the needs-you card.
# ---------------------------------------------------------------------------


def test_month_tiles_show_the_figures_as_the_data_says(
    web_db: sqlite3.Connection, web_client_factory
) -> None:
    """A real expense total is NEGATIVE and the tile shows it that way —
    no sign flip, and the string is the service's own."""
    _seed_flagged_rows(web_db, 1)
    kpis = build_kpis(web_db, today=datetime.now(tz=UTC).date())
    assert kpis.month_spend.value.startswith("-$")

    body = _page(web_client_factory())
    spend = body.split('data-kpi="month_spend"', 1)[1].split("</article>", 1)[0]
    assert kpis.month_spend.value in spend
    assert "This month spend" in spend
    income = body.split('data-kpi="month_income"', 1)[1].split("</article>", 1)[0]
    assert kpis.month_income.value in income
    assert "This month income" in income


def test_needs_you_card_counts_rows_and_links_to_triage(
    web_db: sqlite3.Connection, web_client_factory
) -> None:
    _seed_flagged_rows(web_db, 3)

    body = _page(web_client_factory())
    card = body.split('data-kpi="needs_review"', 1)[1].split("</a>", 1)[0]
    # The href test_dashboard pins, unchanged.
    assert 'href="/triage?type_filter=rate"' in body
    assert '<span class="today-needs-count">3</span> rows need you' in card
    assert 'data-severity="alert"' in card
    # The label the older tests read is still on the card.
    assert "Needs review" in card


def test_needs_you_card_at_zero_says_nothing_needs_you(
    web_db: sqlite3.Connection, web_client_factory
) -> None:
    body = _page(web_client_factory())
    card = body.split('data-kpi="needs_review"', 1)[1].split("</a>", 1)[0]
    assert "Nothing needs you" in card
    assert "rows need you" not in card
    assert 'data-severity="normal"' in card


# ---------------------------------------------------------------------------
# Source chips.
# ---------------------------------------------------------------------------


def test_source_chips_keep_their_data_attributes_without_the_tailwind_macro(
    web_db: sqlite3.Connection, web_client_factory
) -> None:
    client = web_client_factory()
    resp = client.get(
        "/_partial/dashboard/sync-status", headers={"HX-Request": "true"}
    )
    assert resp.status_code == 200
    body = resp.text

    for source in ("binance", "provincial", "bcv", "p2p_rates"):
        assert re.search(
            rf'data-source="{source}"\s+data-severity="(green|yellow|red)"\s+'
            rf'data-status="(success|running|error|never)"',
            body,
        ), source
    # The old macro's pill is gone; the chip is today.css's.
    assert "rounded-full" not in body
    assert 'class="today-source' in body


def test_sources_strip_keeps_its_polling_wrapper_and_gets_a_heading(
    web_db: sqlite3.Connection, web_client_factory
) -> None:
    body = _page(web_client_factory())
    strip = body.split('id="sync-status-strip"', 1)[1].split(">", 1)[0]
    assert 'hx-get="/_partial/dashboard/sync-status"' in strip
    assert 'hx-trigger="load delay:12s, every 60s"' in strip
    assert 'hx-swap="innerHTML"' in strip
    assert 'aria-live="polite"' in strip
    assert re.search(r'<h2 class="teyebrow"[^>]*>\s*Sources\s*</h2>', body)


# ---------------------------------------------------------------------------
# The chart payload.
# ---------------------------------------------------------------------------


def test_flows_payload_is_a_json_block_not_tojson_in_an_attribute(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    body = _page(web_client_factory())

    assert "data-chart=" not in body
    match = re.search(
        r'<script id="flows-chart-data" type="application/json">(.*?)</script>',
        body,
        re.S,
    )
    assert match, "the chart payload must ride in its own JSON block"
    payload = json.loads(match.group(1))
    assert set(payload) == {"months", "labels", "income", "expense"}
    assert len(payload["months"]) == 6
    # The hydration script reads that block, by id.
    assert "getElementById('flows-chart-data')" in body
    assert 'id="flows-chart"' in body


def test_flows_chart_is_greyscale_plus_at_most_one_red() -> None:
    src = (TEMPLATES / "partials/flows_chart.html").read_text(encoding="utf-8")
    colours = {c.lower() for c in re.findall(r"#[0-9a-fA-F]{6}\b", src)}
    allowed_ink = {
        "#131312", "#22221f", "#33332f", "#4b4b46", "#6d6d69", "#666662",
        "#9a9a95", "#bebdb8", "#c7c6c0", "#dbdad5", "#e4e4e1",
        "#f5f5f3", "#ececea",
    }
    reds = {"#e5231b", "#c51a13", "#a5140e"}
    assert colours <= allowed_ink | reds, colours - (allowed_ink | reds)
    assert len(colours & reds) <= 1
    # The old palette is gone.
    for old in ("#2a78d6", "#eb6834", "#64748b"):
        assert old not in src


def test_today_css_owns_the_chart_height() -> None:
    today = (CSS / "today.css").read_text(encoding="utf-8")
    block = today[today.index("\n.today-chart {") :]
    block = block[: block.index("}")]
    assert "height:" in block
    # And the page no longer leans on app.css's .flows-chart rule.
    src = (TEMPLATES / "partials/flows_chart.html").read_text(encoding="utf-8")
    assert 'class="flows-chart"' not in src


# ---------------------------------------------------------------------------
# Recent activity.
# ---------------------------------------------------------------------------


def test_recent_activity_wraps_the_canonical_rows_in_flow_rows(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    body = _page(web_client_factory())
    section = body.split('id="recent-activity-heading"', 1)[1]
    assert '<div class="flow-rows">' in section
    rows = section.split('<div class="flow-rows">', 1)[1]
    assert "data-card-row" in rows
    assert "data-tx-id" in rows
    assert re.search(r'class="tlink[^"]*"\s+href="/transactions', section)

    src = (TEMPLATES / "partials/recent_activity.html").read_text(encoding="utf-8")
    assert '{% include "partials/card_transaction.html" %}' in src
    assert 'class="cards"' not in src


def test_recent_activity_empty_state_names_what_will_appear(
    web_db: sqlite3.Connection, web_client_factory
) -> None:
    body = _page(web_client_factory())
    section = body.split('id="recent-activity-heading"', 1)[1]
    assert "will appear here" in section


# ---------------------------------------------------------------------------
# No Tailwind left in the five templates.
# ---------------------------------------------------------------------------


_TAILWIND = re.compile(
    r"\b(bg-|text-slate|text-sky|text-\[|text-xs|text-sm|text-lg|text-2xl|"
    r"rounded|border-slate|border-dashed|space-y-|mb-\d|mt-\d|p-\d|px-\d|"
    r"hover:|no-underline|font-semibold|tracking-wide|uppercase|"
    r"tabular-nums|items-baseline|justify-between|col-span-full|"
    r"grid-cols-subgrid|sm:|visually-hidden)"
)


def test_no_tailwind_utility_remains_in_the_five_today_templates() -> None:
    offenders: dict[str, list[str]] = {}
    for rel in TODAY_TEMPLATES:
        src = (TEMPLATES / rel).read_text(encoding="utf-8")
        for value in re.findall(r'(?<![-:\w])class="([^"]*)"', src):
            literal = re.sub(r"\{[%{].*?[%}]\}", " ", value)
            hits = [tok for tok in literal.split() if _TAILWIND.search(tok)]
            if hits:
                offenders.setdefault(rel, []).extend(hits)
    assert not offenders, offenders


def test_today_classes_are_prefixed_and_defined_in_today_css() -> None:
    """Every ``today-`` class a template uses exists in today.css, and
    today.css reads signal.css's tokens rather than restating hexes."""
    today = (CSS / "today.css").read_text(encoding="utf-8")
    defined = set(re.findall(r"\.(today-[\w-]+)", today))
    used: set[str] = set()
    for rel in TODAY_TEMPLATES:
        src = (TEMPLATES / rel).read_text(encoding="utf-8")
        used.update(re.findall(r"\btoday-[\w-]+", src))
    assert used, "the page should wear today- classes"
    assert used <= defined, used - defined
    assert "var(--" in today
    rules = re.sub(r"/\*.*?\*/", "", today, flags=re.S)  # prose may cite a hex
    assert not re.search(r"#[0-9a-fA-F]{3,6}\b", rules), "hexes belong in signal.css"
