"""The reports reskin — /monthly, /accounts and /rates inside the SIGNAL shell.

Reskin, not redesign: the same services, the same endpoints, the same
structure per page (the CSS-grid pivot in particular stays exactly what it
was — its real redesign is the next design track). What changes is the
skin: every page opens with ``page_header`` (the question, the one Doto
figure, the meta), the Tailwind utilities are gone from the thirteen
report templates, and everything they used to do lives in ``reports.css``
on signal.css's tokens.

Three silent failure modes these tests exist for:

* a Tailwind class that the vendored, build-less ``tailwind.css`` does not
  carry renders unstyled while every server-side test stays green;
* ``tojson`` inside a quoted attribute truncates the JS while every
  server-side test stays green;
* an ``app.css`` sweep that deletes ``.monthly-pivot`` and friends leaves
  the pivot unstyled unless the reports' own sheet re-declares them.
"""

from __future__ import annotations

import json
import re
import sqlite3
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from finances.db.repos import accounts as accounts_repo
from finances.db.repos import rates as rates_repo
from finances.db.repos import transactions as transactions_repo
from finances.domain.models import (
    Account,
    AccountKind,
    Rate,
    Transaction,
    TransactionKind,
)
from finances.format import fmt_date, fmt_money, fmt_native, fmt_usd

ROOT = Path(__file__).resolve().parents[2]
TEMPLATES = ROOT / "finances" / "web" / "templates"
CSS = ROOT / "finances" / "web" / "static" / "css"

#: The thirteen templates this reskin owns.
OWNED = (
    "pages/monthly.html",
    "pages/monthly_mobile.html",
    "partials/monthly_chart.html",
    "partials/monthly_filters.html",
    "partials/monthly_kind_tabs.html",
    "partials/monthly_mobile_card.html",
    "partials/monthly_mobile_inner.html",
    "partials/monthly_pivot.html",
    "pages/accounts.html",
    "partials/account_card.html",
    "pages/rates.html",
    "partials/rates_chart.html",
    "partials/rates_latest_per_pair.html",
)

_DESKTOP = {"User-Agent": "Mozilla/5.0 desktop"}


def _page(factory, path: str) -> str:
    client: TestClient = factory()
    response = client.get(path, headers=_DESKTOP)
    assert response.status_code == 200, path
    return response.text


def _json_block(html: str, element_id: str) -> dict:
    """The parsed payload of ``<script id=... type="application/json">``."""
    match = re.search(
        rf'<script id="{element_id}" type="application/json">(.*?)</script>',
        html,
        flags=re.DOTALL,
    )
    assert match, f"no application/json block with id {element_id!r}"
    return json.loads(match.group(1))


def _month_str(d: date) -> str:
    return f"{d.year:04d}-{d.month:02d}"


def _add_account(
    conn: sqlite3.Connection,
    *,
    name: str,
    currency: str,
    active: bool = True,
    amount: Decimal | None = None,
) -> Account:
    account = accounts_repo.insert(
        conn,
        Account(
            name=name,
            kind=AccountKind.BANK,
            currency=currency,
            institution="Test",
            active=active,
        ),
    )
    if amount is not None:
        transactions_repo.insert(
            conn,
            Transaction(
                account_id=account.id,
                occurred_at=datetime.now(tz=UTC),
                kind=TransactionKind.EXPENSE if amount < 0 else TransactionKind.INCOME,
                amount=amount,
                currency=currency,
                description=f"{name} seed",
                source="provincial",
                source_ref=f"reskin-{name}",
            ),
        )
    return account


# ---------------------------------------------------------------------------
# Every page opens with its question and ONE answer
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("path", "question"),
    [
        ("/monthly?layout=desktop", "Where did it go?"),
        ("/monthly?layout=mobile", "Where did it go?"),
        ("/accounts", "What do I hold?"),
        ("/rates", "What is a dollar worth?"),
    ],
)
def test_each_page_opens_with_its_question_and_a_single_answer(
    seeded_web_db: sqlite3.Connection,
    web_client_factory,
    path: str,
    question: str,
) -> None:
    html = _page(web_client_factory, path)

    assert html.count('<span class="page-question">') == 1
    assert f'<span class="page-question">{question}</span>' in html
    # One Doto figure per view — a second page-answer is the design broken.
    assert html.count('<h1 class="page-answer">') == 1
    # The header opens the content column, before anything else on the page.
    assert html.index('<header class="page-header">') < html.index("</main>")


# ---------------------------------------------------------------------------
# /monthly
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("kind", ["expense", "income", "net"])
def test_monthly_answer_is_the_grand_total_of_the_active_kind(
    seeded_web_db: sqlite3.Connection, web_client_factory, kind: str
) -> None:
    from finances.web.services.monthly_view import (
        MonthlyFilter,
        MonthlyKind,
        build_pivot,
    )

    pivot = build_pivot(
        seeded_web_db, MonthlyFilter(kind=MonthlyKind(kind)), today=date.today()
    )
    html = _page(web_client_factory, f"/monthly?layout=desktop&kind={kind}")

    expected = fmt_usd(pivot.totals.grand_total_usd)
    assert f'<h1 class="page-answer">{expected}</h1>' in html
    # The meta names the kind as a badge and the pivot's size as a caption.
    assert f'<span class="tbadge" data-kind-badge>{kind.capitalize()}</span>' in html
    rows = len(pivot.rows)
    assert f"{rows} categor{'y' if rows == 1 else 'ies'}" in html
    assert f"{len(pivot.months)} months" in html


def test_monthly_mobile_answer_is_the_month_total(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    from finances.web.services.monthly_view import MonthlyFilter, build_mobile

    today = date.today()
    mobile = build_mobile(
        seeded_web_db, MonthlyFilter(), today=today, month=_month_str(today)
    )
    html = _page(web_client_factory, "/monthly?layout=mobile")

    assert f'<h1 class="page-answer">{fmt_usd(mobile.month_total_usd)}</h1>' in html
    assert '<span class="tbadge" data-kind-badge>Expense</span>' in html
    # The chevrons and the share bars survive the reskin.
    assert f'data-prev-month="{mobile.prev_month}"' in html
    assert f'data-next-month="{mobile.next_month}"' in html
    assert 'id="monthly-mobile"' in html
    assert html.count("data-mobile-card") == len(mobile.categories)
    assert html.count('class="rpt-mcard-bar"') == len(mobile.categories)
    # The Total tile inside the swap target keeps the ASCII form the
    # formatting sweep pins (fmt_money), so a partial swap stays truthful.
    assert fmt_money(mobile.month_total_usd) in html


def test_kind_tabs_keep_their_contract_as_ink_filled_tabs(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    html = _page(web_client_factory, "/monthly?layout=desktop&kind=income")
    tabs = re.findall(r"<a [^>]*data-kind-tab[^>]*>", html)

    assert len(tabs) == 3
    assert sum('data-active="true"' in tab for tab in tabs) == 1
    active = next(tab for tab in tabs if 'data-active="true"' in tab)
    assert "kind=income" in active
    assert "tbtn tbtn-sm rpt-tab is-on" in active
    for tab in tabs:
        assert re.search(r'href="\?range_preset=6m[^"]*&kind=(expense|income|net)', tab)
    assert 'data-monthly-kind-tabs' in html
    # The tabs are the page's own markup now, not the Tailwind macro.
    source = (TEMPLATES / "partials/monthly_kind_tabs.html").read_text(encoding="utf-8")
    assert "kind_tab" not in source


def test_pivot_keeps_month_cols_sticky_classes_and_drill_hrefs(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    html = _page(web_client_factory, "/monthly?layout=desktop")

    assert 'id="monthly-pivot"' in html
    assert 'class="rpt-pivot"' in html
    assert 'style="--month-cols: 6;"' in html
    assert 'class="pivot-head first"' in html
    assert 'class="pivot-row-label"' in html
    assert 'class="pivot-footer first"' in html
    assert re.search(
        r'<a href="/transactions\?date_from=[^"]+" data-cell data-month="\d{4}-\d{2}"',
        html,
    )
    # The empty cell keeps its em dash, in the reports' own class.
    assert '<span class="rpt-pivot-empty">&mdash;</span>' in html

    css = (CSS / "reports.css").read_text(encoding="utf-8")
    grid = css[css.index("\n.rpt-pivot {") :]
    grid = grid[: grid.index("}")]
    assert "display: grid" in grid
    assert "var(--month-cols" in grid
    for selector in (".rpt-pivot .pivot-head", ".rpt-pivot .pivot-row-label", ".rpt-pivot .pivot-footer"):
        # The selector also opens a shared padding rule; one of its blocks
        # must be the sticky one.
        blocks = re.findall(rf"{re.escape(selector)}\s*\{{([^}}]*)\}}", css)
        assert blocks, selector
        assert any("position: sticky" in block for block in blocks), selector
    assert ".rpt-pivot .pivot-row {" in css and "display: contents" in css
    assert ".rpt-pivot .pivot-cell.has-bcv-fallback::before" in css
    assert ".rpt-pivot .pivot-cell.has-needs-review::after" in css


def test_monthly_filters_keep_their_field_names_and_hx_contract(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    html = _page(web_client_factory, "/monthly?layout=desktop&range_preset=3m")
    form = re.search(r'<form[^>]*id="monthly-filters"[^>]*>.*?</form>', html, flags=re.DOTALL)
    assert form, "no #monthly-filters form"
    form_html = form.group(0)

    for attr in (
        'hx-get="/_partial/monthly/pivot"',
        'hx-target="#monthly-pivot"',
        'hx-swap="outerHTML"',
        'hx-push-url="true"',
        'hx-trigger="change"',
    ):
        assert attr in form_html, attr

    # The presets are buttons now — radios inside .tbtn-sm labels — and
    # submit the same single ``range_preset`` value the select did.
    radios = re.findall(r'<input type="radio" name="range_preset" value="(\w+)"[^>]*>', form_html)
    assert radios == ["3m", "6m", "12m", "ytd", "all", "custom"]
    assert form_html.count('name="range_preset"') == 6
    assert re.search(r'<input type="radio" name="range_preset" value="3m"[^>]*checked', form_html)
    assert form_html.count("checked") == 1 + form_html.count('name="include_bcv_fallback" value="true" checked')
    assert form_html.count('class="tbtn tbtn-sm rpt-tab"') == 6

    for field in (
        '<input type="month" name="since" value=""',
        '<input type="month" name="until" value=""',
        '<select name="accounts" multiple',
        '<select name="currencies" multiple',
        'name="include_bcv_fallback"',
        '<input type="hidden" name="kind" value="expense">',
        '<a href="/monthly" data-clear-filters',
    ):
        assert field in form_html, field
    assert 'class="teyebrow"' in form_html
    assert 'class="rpt-input' in form_html


def test_monthly_chart_payload_is_a_json_block_not_an_attribute(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    from finances.web.services.monthly_view import MonthlyFilter, build_chart

    chart = build_chart(seeded_web_db, MonthlyFilter(), today=date.today())
    html = _page(web_client_factory, "/monthly?layout=desktop")

    payload = _json_block(html, "monthly-chart-data")
    assert payload["months"] == chart.months
    assert [s["category"] for s in payload["series"]] == [s.category for s in chart.series]
    assert len(payload["fallback_per_month"]) == len(chart.months)
    assert payload["kind"] == "expense"
    assert "data-chart=" not in html
    assert "<canvas" in html and "data-monthly-chart" in html
    assert 'id="monthly-chart"' in html


# ---------------------------------------------------------------------------
# /accounts
# ---------------------------------------------------------------------------


def test_accounts_answer_is_the_sum_when_every_active_card_is_priced(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    """An INACTIVE unpriced account must not block the sum."""
    from finances.web.services.accounts_view import build_account_cards

    _add_account(seeded_web_db, name="Old COP", currency="COP", active=False, amount=Decimal("-1000"))
    cards = build_account_cards(seeded_web_db, today=date.today())
    active = [c for c in cards if c.active]
    assert all(c.balance_usdt is not None for c in active)
    total = sum((c.balance_usdt for c in active), Decimal("0"))

    html = _page(web_client_factory, "/accounts")

    assert f'<h1 class="page-answer">{fmt_usd(total)}</h1>' in html
    assert "data-unpriced-count" not in html
    assert '<span class="tbadge">1 inactive</span>' in html
    assert "USDT-equivalent via P2P median, never BCV" in html


def test_accounts_answer_falls_back_to_a_count_with_the_unpriced_badge(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    from finances.web.services.accounts_view import build_account_cards

    _add_account(seeded_web_db, name="Bogota COP", currency="COP", amount=Decimal("-50000"))
    cards = build_account_cards(seeded_web_db, today=date.today())
    active = [c for c in cards if c.active]
    assert sum(c.balance_usdt is None for c in active) == 1

    html = _page(web_client_factory, "/accounts")

    assert f'<h1 class="page-answer">{len(active)} accounts</h1>' in html
    assert (
        '<span class="tbadge tbadge-warning tbadge-dot" data-unpriced-count>1 unpriced</span>'
        in html
    )
    card = re.search(r'<a class="rpt-account"[^>]*data-account="Bogota COP".*?</a>', html, flags=re.DOTALL)
    assert card, "no card for the COP account"
    assert 'class="rpt-account-usd is-unpriced"' in card.group(0)
    assert "Unpriced" in card.group(0)
    # The older contract (test_accounts_rates) still reads an em dash for
    # a missing USDT figure.
    assert "&mdash;" in card.group(0)


def test_ves_account_card_shows_bolivares_and_its_usdt_line(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    from finances.web.services.accounts_view import build_account_cards

    cards = build_account_cards(seeded_web_db, today=date.today())
    provincial = next(c for c in cards if c.name == "Provincial")
    assert provincial.currency == "VES" and provincial.balance_usdt is not None

    html = _page(web_client_factory, "/accounts")
    card = re.search(r'<a class="rpt-account"[^>]*data-account="Provincial".*?</a>', html, flags=re.DOTALL)
    assert card, "no Provincial card"
    card_html = card.group(0)

    native = fmt_native(provincial.balance_native, "VES")
    assert native.startswith("Bs. ")
    assert f'<span class="rpt-account-native">{native}</span>' in card_html
    assert f'<span class="rpt-account-usd">{fmt_money(provincial.balance_usdt)} USDT</span>' in card_html
    assert '<span class="rpt-account-inst">Provincial</span>' in card_html


def test_account_cards_keep_their_data_contract_and_inactive_cards_trail(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    from finances.web.services.accounts_view import build_account_cards

    _add_account(seeded_web_db, name="Banesco VES", currency="VES", active=False)
    cards = build_account_cards(seeded_web_db, today=date.today())
    html = _page(web_client_factory, "/accounts")

    anchors = re.findall(r'<a class="rpt-account[^"]*"[^>]*data-account-card[^>]*>', html)
    assert len(anchors) == len(cards)
    for card, anchor in zip(cards, anchors):
        assert f'href="{card.drill_url}"' in anchor
        assert f'data-account="{card.name}"' in anchor
        assert f'data-account-kind="{card.kind}"' in anchor
        assert f'data-active="{"true" if card.active else "false"}"' in anchor
    # Active first, inactive after, as the service sorts them.
    actives = [a for a in anchors if 'data-active="true"' in a]
    assert anchors[: len(actives)] == actives
    inactive = next(a for a in anchors if 'data-active="false"' in a)
    assert "is-inactive" in inactive
    assert '<span class="tbadge" data-inactive-badge>inactive</span>' in html
    # The kind is a mono badge in the reports' own markup, not kind_chip.
    assert '<span class="tbadge" data-kind-badge title="account kind: crypto_spot">spot</span>' in html
    assert 'id="account-cards"' in html and 'class="rpt-accounts"' in html
    source = (TEMPLATES / "partials/account_card.html").read_text(encoding="utf-8")
    assert "kind_chip" not in source and "format_amount" not in source


def test_accounts_empty_state_uses_the_landmark_icon(
    web_db: sqlite3.Connection, web_client_factory
) -> None:
    # The migrations seed two accounts; the empty state needs none at all.
    web_db.execute("DELETE FROM accounts")
    html = _page(web_client_factory, "/accounts")

    assert '<h1 class="page-answer">No accounts</h1>' in html
    empty = re.search(r'<div class="rpt-empty" data-accounts-empty>.*?</div>', html, flags=re.DOTALL)
    assert empty, "no empty state"
    assert 'data-icon="landmark"' in empty.group(0)
    assert 'width="28"' in empty.group(0)


# ---------------------------------------------------------------------------
# /rates
# ---------------------------------------------------------------------------


def test_rates_answer_is_the_latest_p2p_median(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    html = _page(web_client_factory, "/rates")

    assert '<h1 class="page-answer">36.50 Bs./$</h1>' in html
    assert f'<span class="tbadge" data-rate-as-of>{fmt_date(date.today())}</span>' in html
    assert "BCV is reference only (rule-005)" in html
    assert "data-rate-fallback" not in html


def test_rates_answer_falls_back_to_the_latest_of_any_pair_and_says_so(
    web_db: sqlite3.Connection, web_client_factory
) -> None:
    rates_repo.upsert(
        web_db,
        Rate(as_of_date=date(2026, 1, 5), base="USD", quote="VES", rate=Decimal("36.1049"), source="bcv"),
    )
    rates_repo.upsert(
        web_db,
        Rate(as_of_date=date(2026, 1, 9), base="EUR", quote="VES", rate=Decimal("39.20"), source="bcv"),
    )
    html = _page(web_client_factory, "/rates")

    # Newest row of any pair — the EUR one — and an honest badge about it.
    assert '<h1 class="page-answer">39.20 VES per EUR</h1>' in html
    assert '<span class="tbadge tbadge-warning tbadge-dot" data-rate-fallback>no P2P median · bcv</span>' in html


def test_rates_answer_when_there_are_no_rates_at_all(
    web_db: sqlite3.Connection, web_client_factory
) -> None:
    html = _page(web_client_factory, "/rates")

    assert '<h1 class="page-answer">No rates yet</h1>' in html
    assert "data-rate-as-of" not in html
    assert 'data-rates-empty' in html
    assert 'data-icon="percent"' in html


def test_rates_chart_payload_is_a_json_block_on_the_page_and_the_partial(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    from finances.web.services.rates_view import build_rates_chart

    html = _page(web_client_factory, "/rates")
    payload = _json_block(html, "rates-chart-data")
    chart = build_rates_chart(seeded_web_db, range_days=30)
    assert [s["label"] for s in payload["series"]] == [s.label for s in chart.series]
    assert [s["source"] for s in payload["series"]] == [s.source for s in chart.series]
    assert payload["range_days"] == 30

    client = web_client_factory()
    partial = client.get("/_partial/rates/chart", params={"range_days": 7}, headers={"HX-Request": "true"}).text
    assert _json_block(partial, "rates-chart-data")["range_days"] == 7
    assert 'id="rates-chart-canvas"' in partial
    assert "data-chart=" not in partial


def test_rates_range_toggle_keeps_its_hx_contract_as_tabs(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    html = _page(web_client_factory, "/rates")
    buttons = re.findall(r"<button[^>]*data-range-days[^>]*>", html)

    assert [re.search(r'data-range-days="(\d+)"', b).group(1) for b in buttons] == ["7", "30", "90", "365"]
    for button in buttons:
        for attr in (
            'hx-get="/_partial/rates/chart"',
            'hx-target="#rates-chart"',
            'hx-swap="outerHTML"',
            'hx-push-url="true"',
        ):
            assert attr in button, attr
        assert re.search(r"hx-vals='\{\"range_days\": \"\d+\"\}'", button)
        assert "tbtn tbtn-sm rpt-tab" in button
    assert sum("is-on" in b for b in buttons) == 1
    assert "is-on" in next(b for b in buttons if 'data-range-days="30"' in b)


def test_rate_tiles_are_eyebrow_pairs_and_the_bcv_tile_is_reference_only(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    html = _page(web_client_factory, "/rates")
    assert 'class="rpt-rate-tiles"' in html
    # Each tile ends on a sentinel comment, the way triage_row.html does.
    tiles = re.findall(
        r'<div class="rpt-tile rpt-rate-tile"[^>]*data-rate-card.*?<!-- /rpt-rate-tile -->',
        html,
        flags=re.DOTALL,
    )
    assert len(tiles) == 2

    p2p = next(t for t in tiles if 'data-source="binance_p2p_median"' in t)
    bcv = next(t for t in tiles if 'data-source="bcv"' in t)
    assert '<span class="teyebrow">USDT/VES</span>' in p2p
    assert '<span class="rpt-figure">36.5000</span>' in p2p
    assert "reference only" not in p2p
    assert '<span class="teyebrow">USD/VES</span>' in bcv
    assert '<span class="tbadge tbadge-warning">reference only</span>' in bcv
    assert f'data-rate-source="bcv">BCV · {fmt_date(date.today())}</span>' in bcv
    source = (TEMPLATES / "partials/rates_latest_per_pair.html").read_text(encoding="utf-8")
    assert "rate_source_badge" not in source


def test_rates_chart_script_draws_greyscale_and_one_red() -> None:
    """P2P median in ink, BCV in dashed ink-400, anything else in the one red."""
    source = (TEMPLATES / "partials/rates_chart.html").read_text(encoding="utf-8")

    for token in ("--ink-900", "--ink-400", "--red-600", "--border-subtle", "--text-tertiary"):
        assert token in source, token
    assert "borderDash" in source
    assert "JetBrains Mono" in source
    for old in ("#0284c7", "#d97706"):
        assert old not in source
    monthly = (TEMPLATES / "partials/monthly_chart.html").read_text(encoding="utf-8")
    for old in ("#0ea5e9", "#22c55e", "#a855f7", "#f59e0b", "#ef4444", "#64748b"):
        assert old not in monthly


# ---------------------------------------------------------------------------
# Source-level guards over the thirteen templates and the sheet
# ---------------------------------------------------------------------------


def _class_tokens(html: str) -> set[str]:
    used: set[str] = set()
    for value in re.findall(r'(?<![-:\w])class="([^"]*)"', html):
        literal = re.sub(r"\{[%{].*?[%}]\}", " ", value)
        used.update(t for t in literal.split() if t and "{" not in t and "'" not in t)
    return used


def test_no_tailwind_utility_survives_in_the_report_templates() -> None:
    tailwind = (CSS / "tailwind.css").read_text(encoding="utf-8")
    utilities = {m.replace("\\", "") for m in re.findall(r"\.((?:[\w-]|\\.)+)\{", tailwind)}

    offenders: dict[str, list[str]] = {}
    for name in OWNED:
        used = _class_tokens((TEMPLATES / name).read_text(encoding="utf-8"))
        hit = sorted(used & utilities)
        if hit:
            offenders[name] = hit
    assert not offenders, f"Tailwind utilities left in the report templates: {offenders}"


def test_no_tojson_inside_an_attribute_in_the_report_templates() -> None:
    offenders = []
    for name in OWNED:
        source = (TEMPLATES / name).read_text(encoding="utf-8")
        if re.search(r"""=\s*(['"])[^'"]*\{\{[^}]*\|\s*tojson""", source):
            offenders.append(name)
        if re.search(r"""=\s*'\{\{[^}]*\|\s*safe""", source):
            offenders.append(name)
    assert not offenders, offenders


def test_the_report_templates_do_not_lean_on_app_css() -> None:
    """The app.css report block is being swept; nothing here may name it."""
    doomed = (
        "monthly-pivot",
        "monthly-mobile-card",
        "monthly-chart-wrap",
        "account-cards",
        "account-card",
        "rate-cards",
        "rate-card",
        "rates-chart-wrap",
    )
    offenders: dict[str, list[str]] = {}
    for name in OWNED:
        used = _class_tokens((TEMPLATES / name).read_text(encoding="utf-8"))
        hit = sorted(used & set(doomed))
        if hit:
            offenders[name] = hit
    assert not offenders, offenders


def test_reports_css_owns_the_chart_height_and_reads_only_tokens() -> None:
    css = (CSS / "reports.css").read_text(encoding="utf-8")

    chart = css[css.index("\n.rpt-chart {") :]
    chart = chart[: chart.index("}")]
    assert "position: relative" in chart
    assert "height:" in chart
    # Every colour is a token; a raw hex here is a second palette.
    assert re.findall(r"#[0-9a-fA-F]{3,8}\b", css) == []
    # Depth is hairlines and the raised-on-canvas inversion, never a blur.
    assert "blur(" not in css
