"""The /monthly chart gets colour that means something, a hover that explains
the month, and a category filter.

Per rule-011 these land before the implementation. Three separate complaints
from the owner, one surface:

* **Colour.** Six steps of the ink ramp measured ΔE 9.0 apart at the worst
  adjacent pair, against a floor of 15 — below that, full colour vision cannot
  separate them. Five chart-only hues replace them, and the sixth slot stays a
  neutral because "Other" is a remainder, not an entity. The tests here pin the
  contract the drawing code depends on, not the hex values themselves: those
  live in signal.css and are validated outside the suite.

* **Colour follows the entity, never its rank.** ``ink[i % len]`` keyed the
  colour to a series' position, so filtering one category out repainted every
  survivor. That was tolerable while nothing could filter the chart; the
  category filter added below makes it a daily bug, so the slot is assigned
  from the category's identity and is stable under filtering.

* **The hover.** Chart.js' in-canvas tooltip listed the month's series in
  stack order with no totals and no shares. The replacement is a DOM overlay —
  the idiom rates_chart.html already established — ranked by size, and it can
  open "Other" because the tail that fed it is now on the wire.
"""

from __future__ import annotations

import re
import sqlite3
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Callable

from fastapi.testclient import TestClient

from finances.db.repos import accounts as accounts_repo
from finances.db.repos import categories as categories_repo
from finances.db.repos import transactions as transactions_repo
from finances.domain.models import (
    Account,
    AccountKind,
    Transaction,
    TransactionKind,
)

REPO_ROOT = Path(__file__).resolve().parents[2]
SIGNAL_CSS = REPO_ROOT / "finances" / "web" / "static" / "css" / "signal.css"
CHART_HTML = (
    REPO_ROOT / "finances" / "web" / "templates" / "partials" / "monthly_chart.html"
)
FILTERS_HTML = (
    REPO_ROOT / "finances" / "web" / "templates" / "partials" / "monthly_filters.html"
)
TX_FILTERS_HTML = (
    REPO_ROOT
    / "finances"
    / "web"
    / "templates"
    / "partials"
    / "transactions_filters.html"
)
MACROS_HTML = REPO_ROOT / "finances" / "web" / "templates" / "_macros.html"


# ---------------------------------------------------------------------------
# Seeding helper — N expense categories of descending size, one month.
# ---------------------------------------------------------------------------


def _seed_categories(
    conn: sqlite3.Connection, *, count: int
) -> tuple[datetime, list[str]]:
    """Insert ``count`` expense categories of strictly descending totals.

    Returns ``(today, names_in_rank_order)``. Amounts descend so the chart's
    top-N slice is deterministic, which is what makes "is this slot stable
    under filtering" a question with one right answer.
    """
    today = datetime.now(tz=UTC)
    cash = accounts_repo.insert(
        conn, Account(name="Cash USD", kind=AccountKind.CASH, currency="USD")
    )

    names: list[str] = []
    amount = Decimal("500.00")
    idx = 0
    for cat in categories_repo.list_all(conn):
        if cat.kind != TransactionKind.EXPENSE:
            continue
        transactions_repo.insert(
            conn,
            Transaction(
                account_id=cash.id,
                occurred_at=today,
                kind=TransactionKind.EXPENSE,
                amount=amount,
                currency="USD",
                description=f"seed-{idx}",
                category_id=cat.id,
                source="cash_cli",
                source_ref=f"palette-{idx}",
            ),
        )
        names.append(cat.name)
        amount -= Decimal("10")
        idx += 1
        if idx >= count:
            break

    assert len(names) >= count, (
        f"fixture needs {count} expense categories, found {len(names)}"
    )
    return today, names


# ---------------------------------------------------------------------------
# Colour follows the entity.
# ---------------------------------------------------------------------------


def test_series_carry_a_colour_slot(web_db: sqlite3.Connection) -> None:
    """Every chart series names the palette slot it should be drawn in."""
    from finances.web.services.monthly_view import (
        MonthlyFilter,
        MonthlyKind,
        build_chart,
    )

    today, _ = _seed_categories(web_db, count=7)
    chart = build_chart(
        web_db, MonthlyFilter(kind=MonthlyKind.EXPENSE), today=today.date()
    )

    assert chart.series, "expected series"
    for s in chart.series:
        assert isinstance(s.color_slot, int)


def test_colour_slots_are_unique_among_visible_series(
    web_db: sqlite3.Connection,
) -> None:
    """No two series drawn together may share a slot.

    A stable hash alone does not give this — two categories can hash to the
    same slot. The assignment has to resolve that collision among whichever
    series are actually on screen.
    """
    from finances.web.services.monthly_view import (
        MonthlyFilter,
        MonthlyKind,
        build_chart,
    )

    today, _ = _seed_categories(web_db, count=7)
    chart = build_chart(
        web_db, MonthlyFilter(kind=MonthlyKind.EXPENSE), today=today.date()
    )

    slots = [s.color_slot for s in chart.series]
    assert len(slots) == len(set(slots)), f"slots collide: {slots}"


def test_other_takes_the_neutral_slot(web_db: sqlite3.Connection) -> None:
    """"Other" is a remainder, not an entity, so it never takes a hue."""
    from finances.web.services.monthly_view import (
        OTHER_COLOR_SLOT,
        MonthlyFilter,
        MonthlyKind,
        build_chart,
    )

    today, _ = _seed_categories(web_db, count=7)
    chart = build_chart(
        web_db, MonthlyFilter(kind=MonthlyKind.EXPENSE), today=today.date()
    )

    other = [s for s in chart.series if s.category == "Other"]
    assert other, "expected an Other series with 7 categories seeded"
    assert other[0].color_slot == OTHER_COLOR_SLOT

    for s in chart.series:
        if s.category != "Other":
            assert s.color_slot != OTHER_COLOR_SLOT


def test_colour_survives_filtering_another_category_out(
    web_db: sqlite3.Connection,
) -> None:
    """The regression the category filter would otherwise ship.

    Drop the top category from the filter and every survivor moves up one
    rank. Rank-keyed colour repaints all of them; identity-keyed colour does
    not move.
    """
    from finances.web.services.monthly_view import (
        MonthlyFilter,
        MonthlyKind,
        build_chart,
    )

    today, names = _seed_categories(web_db, count=7)

    before = build_chart(
        web_db, MonthlyFilter(kind=MonthlyKind.EXPENSE), today=today.date()
    )
    slots_before = {s.category: s.color_slot for s in before.series}

    # Everything except the largest category.
    kept = names[1:]
    after = build_chart(
        web_db,
        MonthlyFilter(kind=MonthlyKind.EXPENSE, categories=kept),
        today=today.date(),
    )

    for s in after.series:
        if s.category == "Other" or s.category not in slots_before:
            continue
        assert s.color_slot == slots_before[s.category], (
            f"{s.category} was repainted from slot "
            f"{slots_before[s.category]} to {s.color_slot}"
        )


def test_two_visible_categories_never_share_a_slot(
    web_db: sqlite3.Connection,
) -> None:
    """Slots repeat every fifth rank, so a filter can surface a colliding pair.

    Seen live: Dating (rank 3) and Rent (rank 8) both resolved to slot 3 and
    drew in the same colour, which makes the legend a lie. The deeper of the
    two gives way; the one that owns the slot in the stable ordering keeps it,
    so the repair costs at most one category its colour and never the one the
    reader is most likely to be tracking.
    """
    from finances.web.services.monthly_view import (
        CHART_COLOR_SLOTS,
        MonthlyFilter,
        MonthlyKind,
        build_chart,
    )

    today, names = _seed_categories(web_db, count=CHART_COLOR_SLOTS * 2 + 1)

    owner = names[3]
    intruder = names[3 + CHART_COLOR_SLOTS]

    chart = build_chart(
        web_db,
        MonthlyFilter(kind=MonthlyKind.EXPENSE, categories=[owner, intruder]),
        today=today.date(),
    )
    slots = {s.category: s.color_slot for s in chart.series}
    assert slots[owner] != slots[intruder]
    assert slots[owner] == 3, "the slot's owner in the stable ordering keeps it"


# ---------------------------------------------------------------------------
# What is inside "Other".
# ---------------------------------------------------------------------------


def test_other_members_ride_along_with_the_chart(
    web_db: sqlite3.Connection,
) -> None:
    """The tail folded into Other is on the wire, so the overlay can open it.

    Other is routinely the largest block on the owner's real chart — the cap
    hides most of the story unless the hover can say what is in there.
    """
    from finances.web.services.monthly_view import (
        MonthlyFilter,
        MonthlyKind,
        build_chart,
    )

    today, _ = _seed_categories(web_db, count=8)
    chart = build_chart(
        web_db, MonthlyFilter(kind=MonthlyKind.EXPENSE), today=today.date()
    )

    assert chart.other_members, "expected the folded tail on the payload"

    named = [s.category for s in chart.series if s.category != "Other"]
    for member in chart.other_members:
        assert member.category not in named

    # The members must account for exactly the Other series, month by month.
    other = next(s for s in chart.series if s.category == "Other")
    for i in range(len(chart.months)):
        assert sum(
            (m.values[i] for m in chart.other_members), Decimal("0")
        ) == other.values[i]


def test_no_other_members_when_nothing_is_folded(
    web_db: sqlite3.Connection,
) -> None:
    """Under the cap there is no Other, so there is no tail to carry."""
    from finances.web.services.monthly_view import (
        MonthlyFilter,
        MonthlyKind,
        build_chart,
    )

    today, _ = _seed_categories(web_db, count=3)
    chart = build_chart(
        web_db, MonthlyFilter(kind=MonthlyKind.EXPENSE), today=today.date()
    )

    assert all(s.category != "Other" for s in chart.series)
    assert chart.other_members == []


# ---------------------------------------------------------------------------
# The palette lives in the sheet, not in the script.
# ---------------------------------------------------------------------------


def test_signal_declares_the_series_tokens() -> None:
    """Five entity hues plus one neutral, declared once in signal.css."""
    css = SIGNAL_CSS.read_text(encoding="utf-8")
    for n in range(1, 6):
        assert f"--series-{n}:" in css, f"--series-{n} missing from signal.css"
    assert "--series-other:" in css


def test_chart_script_reads_tokens_rather_than_hex() -> None:
    """The drawing code must not carry its own copy of the palette.

    signal.css is the sheet of record; a hex literal in the template is a
    second source of truth that drifts silently.
    """
    html = CHART_HTML.read_text(encoding="utf-8")
    script = html[html.index("<script>") :]
    stray = re.findall(r"#[0-9a-fA-F]{6}\b", script)
    assert not stray, f"hard-coded colours in the chart script: {stray}"

    for n in range(1, 6):
        assert f"--series-{n}" in html
    assert "--series-other" in html


# ---------------------------------------------------------------------------
# The hover overlay.
# ---------------------------------------------------------------------------


def test_chart_renders_an_overlay_node(
    seeded_web_db: sqlite3.Connection,
    web_client_factory: Callable[[], TestClient],
) -> None:
    """A DOM overlay, not Chart.js' in-canvas tooltip.

    The canvas tooltip is what clipped the rate reading on triage; the same
    bubble idiom as rates_chart.html is used instead.
    """
    client = web_client_factory()
    html = client.get("/monthly").text
    assert 'data-monthly-tip' in html


def test_overlay_lives_inside_the_swapped_section() -> None:
    """The pivot partial carries the chart as an out-of-band twin.

    htmx replaces ``#monthly-chart`` wholesale, so an overlay parked outside
    it survives the swap as an orphan and the re-run script binds nothing.
    """
    html = CHART_HTML.read_text(encoding="utf-8")
    start = html.index('id="monthly-chart"')
    end = html.rindex("</section>")
    assert 'data-monthly-tip' in html[start:end]


# ---------------------------------------------------------------------------
# The category filter.
# ---------------------------------------------------------------------------


def test_monthly_offers_a_categories_dropdown(
    seeded_web_db: sqlite3.Connection,
    web_client_factory: Callable[[], TestClient],
) -> None:
    client = web_client_factory()
    html = client.get("/monthly").text
    assert 'data-filter-group="categories"' in html


def test_monthly_category_filter_narrows_the_chart(
    web_db: sqlite3.Connection,
) -> None:
    from finances.web.services.monthly_view import (
        MonthlyFilter,
        MonthlyKind,
        build_chart,
    )

    today, names = _seed_categories(web_db, count=7)
    only = names[2]

    chart = build_chart(
        web_db,
        MonthlyFilter(kind=MonthlyKind.EXPENSE, categories=[only]),
        today=today.date(),
    )
    assert [s.category for s in chart.series] == [only]


def test_monthly_uncategorized_sentinel_selects_unfiled_rows(
    web_db: sqlite3.Connection,
) -> None:
    """``__none__`` means "no category", the same as it does on /transactions.

    Without this the option renders and then matches nothing, which reads as
    a broken filter rather than an empty category.
    """
    from finances.web.services.monthly_view import (
        MonthlyFilter,
        MonthlyKind,
        build_chart,
    )
    from finances.web.services.transactions_query import UNCATEGORIZED

    today, _ = _seed_categories(web_db, count=3)
    cash = accounts_repo.get_by_name(web_db, "Cash USD")
    assert cash is not None
    transactions_repo.insert(
        web_db,
        Transaction(
            account_id=cash.id,
            occurred_at=today,
            kind=TransactionKind.EXPENSE,
            amount=Decimal("7.00"),
            currency="USD",
            description="unfiled",
            source="cash_cli",
            source_ref="palette-unfiled",
        ),
    )

    chart = build_chart(
        web_db,
        MonthlyFilter(kind=MonthlyKind.EXPENSE, categories=[UNCATEGORIZED]),
        today=today.date(),
    )
    assert [s.category for s in chart.series] == ["Uncategorized"]


def test_category_filter_round_trips_through_the_url(
    seeded_web_db: sqlite3.Connection,
    web_client_factory: Callable[[], TestClient],
) -> None:
    """A selected category comes back checked, so the form survives a reload."""
    client = web_client_factory()
    html = client.get("/monthly", params={"categories": "Groceries"}).text
    assert re.search(
        r'name="categories"[^>]*value="Groceries"[^>]*checked', html
    ) or re.search(r'value="Groceries"[^>]*checked[^>]*name="categories"', html)


# ---------------------------------------------------------------------------
# One dropdown, two pages.
# ---------------------------------------------------------------------------


def test_dropdown_macro_is_shared_not_copied() -> None:
    """Both filter forms import one macro instead of keeping a copy each."""
    macros = MACROS_HTML.read_text(encoding="utf-8")
    assert "macro dropdown(" in macros

    for path in (FILTERS_HTML, TX_FILTERS_HTML):
        html = path.read_text(encoding="utf-8")
        assert "macro dropdown(" not in html, f"{path.name} still defines its own"
        assert "_macros.html" in html, f"{path.name} does not import the shared one"
