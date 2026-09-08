"""The /monthly chart draws groups (ADR-023).

A series is now a **group** for grouped categories or a **category** for
ungrouped ones. Groups and ungrouped categories rank together, and the
top-N cap and the Other remainder apply to *that* list. On the owner's
ledger this is the difference between Other being 45.8% of July and the
month's largest true fact — the fixed household costs — being visible.

The seed here mirrors the shape of that July: four household categories
none of which reaches the top five alone, two sociable ones, and a long
tail. ``Home`` must come out on top.
"""

from __future__ import annotations

import json
import re
import sqlite3
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Callable
from urllib.parse import parse_qs, urlsplit

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
from finances.web.services.monthly_view import (
    OTHER_COLOR_SLOT,
    MonthlyChart,
    MonthlyChartSeries,
    MonthlyFilter,
    MonthlyKind,
    build_chart,
)
from finances.web.services.transactions_query import UNCATEGORIZED

REPO_ROOT = Path(__file__).resolve().parents[2]
CHART_HTML = (
    REPO_ROOT / "finances" / "web" / "templates" / "partials" / "monthly_chart.html"
)

# July 2026's shape: Home is the largest fact about the month, and no
# single household category would reach the top five on its own.
JULY = {
    "Rent": "240.00",
    "Utilities": "75.48",
    "Transport": "59.93",
    "Personal Care": "51.84",
    "Going Out": "135.87",
    "Family": "104.75",
    "Groceries": "300.00",
    "Purchases": "200.00",
    "Health": "50.00",
    "Fees": "20.00",
    "Lending": "10.00",
}

HOME = ("Rent", "Utilities", "Transport", "Personal Care")


def _seed(conn: sqlite3.Connection, amounts: dict[str, str]) -> datetime:
    """One expense per category, all on the same day, from a USD account so
    no rate is involved. Returns the day used."""
    today = datetime.now(tz=UTC)
    cash = accounts_repo.insert(
        conn, Account(name="Cash USD", kind=AccountKind.CASH, currency="USD")
    )
    for idx, (name, amount) in enumerate(amounts.items()):
        category = categories_repo.get_by_name(conn, TransactionKind.EXPENSE, name)
        assert category is not None, name
        transactions_repo.insert(
            conn,
            Transaction(
                account_id=cash.id,
                occurred_at=today,
                kind=TransactionKind.EXPENSE,
                amount=Decimal(amount),
                currency="USD",
                description=f"seed-{name}",
                category_id=category.id,
                source="cash_cli",
                source_ref=f"groups-{idx}",
            ),
        )
    return today


def _chart(conn: sqlite3.Connection, today: datetime, **filter_kw) -> MonthlyChart:
    return build_chart(
        conn, MonthlyFilter(kind=MonthlyKind.EXPENSE, **filter_kw), today=today.date()
    )


def _month_index(chart: MonthlyChart, today: datetime) -> int:
    return chart.months.index(f"{today.year:04d}-{today.month:02d}")


def _series(chart: MonthlyChart, label: str) -> MonthlyChartSeries:
    match = [s for s in chart.series if s.category == label]
    assert match, f"no series labelled {label!r} in {[s.category for s in chart.series]}"
    return match[0]


def _query(url: str) -> dict[str, list[str]]:
    return parse_qs(urlsplit(url).query, keep_blank_values=True)


# ---------------------------------------------------------------------------
# What a series is.
# ---------------------------------------------------------------------------


def test_groups_and_ungrouped_categories_rank_together(
    web_db: sqlite3.Connection,
) -> None:
    """Home (427.25) beats Groceries (300) even though no household category
    beats Groceries alone. The cap and the remainder apply to this list."""
    today = _seed(web_db, JULY)
    chart = _chart(web_db, today)

    assert [s.category for s in chart.series] == [
        "Home",
        "Groceries",
        "Social",
        "Purchases",
        "Health",
        "Other",
    ]


def test_a_group_sums_its_categories(web_db: sqlite3.Connection) -> None:
    today = _seed(web_db, JULY)
    chart = _chart(web_db, today)
    i = _month_index(chart, today)

    assert _series(chart, "Home").values[i] == Decimal("427.25")
    assert _series(chart, "Social").values[i] == Decimal("240.62")


def test_a_group_lists_its_categories_as_members_largest_first(
    web_db: sqlite3.Connection,
) -> None:
    """Hovering Home lists Rent, Utilities, Transport, Personal Care."""
    today = _seed(web_db, JULY)
    chart = _chart(web_db, today)
    i = _month_index(chart, today)

    home = _series(chart, "Home")
    assert [m.category for m in home.members] == list(HOME)
    assert sum((m.values[i] for m in home.members), Decimal("0")) == home.values[i]


def test_an_ungrouped_category_has_no_members(web_db: sqlite3.Connection) -> None:
    today = _seed(web_db, JULY)
    chart = _chart(web_db, today)

    assert _series(chart, "Groceries").members == []


def test_a_member_is_drawn_in_its_group_colour(web_db: sqlite3.Connection) -> None:
    """Members are the contents of one block, not blocks of their own."""
    today = _seed(web_db, JULY)
    chart = _chart(web_db, today)

    home = _series(chart, "Home")
    assert home.color_slot != OTHER_COLOR_SLOT
    assert {m.color_slot for m in home.members} == {home.color_slot}


# ---------------------------------------------------------------------------
# One mechanism for opening a series: Other is a series with members too.
# ---------------------------------------------------------------------------


def test_other_lists_what_missed_the_cap_as_members(
    web_db: sqlite3.Connection,
) -> None:
    today = _seed(web_db, JULY)
    chart = _chart(web_db, today)
    i = _month_index(chart, today)

    other = _series(chart, "Other")
    assert [m.category for m in other.members] == ["Fees", "Lending"]
    assert sum((m.values[i] for m in other.members), Decimal("0")) == other.values[i]
    assert {m.color_slot for m in other.members} == {OTHER_COLOR_SLOT}


def test_the_chart_no_longer_carries_a_separate_other_members(
    web_db: sqlite3.Connection,
) -> None:
    """Folded into ``members`` per ADR-023 §2.4 — one mechanism, not two."""
    assert "other_members" not in MonthlyChart.model_fields


def test_a_group_that_misses_the_cap_stays_a_group_inside_other(
    web_db: sqlite3.Connection,
) -> None:
    """The tail is the far end of the same ranking, so what missed the cap
    is a *series*: a small group folds into Other as itself, members and
    all, not as loose categories."""
    small_social = dict(JULY, **{"Going Out": "2.00", "Family": "1.00"})
    today = _seed(web_db, small_social)
    chart = _chart(web_db, today)

    other = _series(chart, "Other")
    labels = [m.category for m in other.members]
    assert "Social" in labels
    assert "Going Out" not in labels and "Family" not in labels
    social = next(m for m in other.members if m.category == "Social")
    assert [m.category for m in social.members] == ["Going Out", "Family"]


# ---------------------------------------------------------------------------
# The category filter narrows a group; it never dissolves it (ADR-023 §2.5).
# ---------------------------------------------------------------------------


def test_filtering_to_one_member_still_draws_the_group(
    web_db: sqlite3.Connection,
) -> None:
    today = _seed(web_db, JULY)
    chart = _chart(web_db, today, categories=["Rent"])
    i = _month_index(chart, today)

    assert [s.category for s in chart.series] == ["Home"]
    home = _series(chart, "Home")
    assert home.values[i] == Decimal("240.00")
    assert [m.category for m in home.members] == ["Rent"]


def test_a_group_keeps_its_colour_when_a_member_is_filtered_out(
    web_db: sqlite3.Connection,
) -> None:
    """Colour follows the series, and the basis ignores the category
    filter, so narrowing Home does not repaint Home or its neighbours."""
    today = _seed(web_db, JULY)
    before = {s.category: s.color_slot for s in _chart(web_db, today).series}

    without_rent = [name for name in JULY if name != "Rent"]
    after = _chart(web_db, today, categories=without_rent)

    for s in after.series:
        if s.category == "Other":
            continue
        assert s.color_slot == before[s.category], (
            f"{s.category} repainted from {before[s.category]} to {s.color_slot}"
        )


def test_colour_slots_rank_series_not_raw_categories(
    web_db: sqlite3.Connection,
) -> None:
    """Ranked by raw category Groceries (300) would own slot 0 and Rent
    (240) slot 1. Ranked by series, Home owns slot 0."""
    today = _seed(web_db, JULY)
    chart = _chart(web_db, today)

    assert _series(chart, "Home").color_slot == 0
    assert _series(chart, "Groceries").color_slot == 1
    assert _series(chart, "Social").color_slot == 2


# ---------------------------------------------------------------------------
# Drilling: one categories= parameter per member.
# ---------------------------------------------------------------------------


def test_a_group_drills_with_one_categories_param_per_member(
    web_db: sqlite3.Connection,
) -> None:
    today = _seed(web_db, JULY)
    chart = _chart(web_db, today)
    i = _month_index(chart, today)

    home = _series(chart, "Home")
    assert len(home.drill_urls) == len(chart.months)
    q = _query(home.drill_urls[i])
    assert set(q["categories"]) == set(HOME)
    assert q["kinds"] == ["expense"]
    assert q["date_from"] == [f"{chart.months[i]}-01"]
    assert q["date_to"][0].startswith(chart.months[i])


def test_an_ungrouped_category_drills_to_itself(web_db: sqlite3.Connection) -> None:
    today = _seed(web_db, JULY)
    chart = _chart(web_db, today)
    i = _month_index(chart, today)

    assert _query(_series(chart, "Groceries").drill_urls[i])["categories"] == ["Groceries"]


def test_other_drills_to_the_tail(web_db: sqlite3.Connection) -> None:
    today = _seed(web_db, JULY)
    chart = _chart(web_db, today)
    i = _month_index(chart, today)

    q = _query(_series(chart, "Other").drill_urls[i])
    assert set(q["categories"]) == {"Fees", "Lending"}


def test_uncategorized_drills_with_the_sentinel(web_db: sqlite3.Connection) -> None:
    """``__none__`` is how /transactions spells "no category"."""
    today = _seed(web_db, {"Groceries": "10.00"})
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
            source_ref="groups-unfiled",
        ),
    )
    chart = _chart(web_db, today)
    i = _month_index(chart, today)

    q = _query(_series(chart, "Uncategorized").drill_urls[i])
    assert q["categories"] == [UNCATEGORIZED]


# ---------------------------------------------------------------------------
# The wire: members and drill URLs ride in the payload; the overlay opens
# any series that has members, not "Other" by name.
# ---------------------------------------------------------------------------


def _json_block(html: str, element_id: str) -> dict:
    m = re.search(
        rf'<script id="{element_id}" type="application/json">(.*?)</script>', html, re.S
    )
    assert m, f"no #{element_id} JSON block"
    return json.loads(m.group(1))


def test_payload_carries_members_and_drill_urls(
    web_db: sqlite3.Connection,
    web_client_factory: Callable[[], TestClient],
) -> None:
    _seed(web_db, JULY)
    html = web_client_factory().get("/monthly?layout=desktop").text
    payload = _json_block(html, "monthly-chart-data")

    assert "other_members" not in payload
    by_label = {s["category"]: s for s in payload["series"]}
    assert [m["category"] for m in by_label["Home"]["members"]] == list(HOME)
    assert [m["category"] for m in by_label["Other"]["members"]] == ["Fees", "Lending"]
    assert by_label["Groceries"]["members"] == []
    assert len(by_label["Home"]["drill_urls"]) == len(payload["months"])
    assert "categories=Rent" in by_label["Home"]["drill_urls"][-1]


def test_overlay_opens_any_series_with_members() -> None:
    """The Other-expansion code is generalised, not duplicated: no second
    path keyed on the label "Other"."""
    html = CHART_HTML.read_text(encoding="utf-8")
    script = html[html.index("<script>") :]

    assert "other_members" not in script
    assert "=== 'Other'" not in script and '=== "Other"' not in script
    assert "members" in script
    assert "is-member" in script


def test_a_bar_click_drills_through_the_series_url() -> None:
    html = CHART_HTML.read_text(encoding="utf-8")
    script = html[html.index("<script>") :]

    assert "drill_urls" in script
