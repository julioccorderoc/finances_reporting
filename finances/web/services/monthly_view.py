"""Pivot / chart / mobile view-models for /monthly (EPIC-023, Phase 2c).

This module is a *projection* over :func:`finances.reports.monthly.build_report`.
Per rule-005 + rule-012 we never reimplement the rate-resolution or
aggregation logic — the canonical totals come from ``MonthlyReport`` and
this module only re-shapes them into:

* :class:`MonthlyPivot` — category × month grid for the desktop view.
* :class:`MonthlyChart` — top-5 + Other stacked-bar series, plus a
  per-month BCV fallback shadow series. A series is a *group* for grouped
  categories and a *category* for ungrouped ones (ADR-026); the pivot and
  the mobile view stay per-category on purpose.
* :class:`MonthlyMobile` — single-month category list for the mobile view.

The DTOs are Pydantic v2 (rule-009) so FastAPI can use them as
``response_model``. Decimal is preserved end-to-end; floats are only used
for the percentage helpers on the mobile view where the precision loss
is harmless and the consumer is the chart bar width.
"""

from __future__ import annotations

import sqlite3
from collections import defaultdict
from datetime import date
from decimal import Decimal
from enum import StrEnum
from typing import Iterable
from urllib.parse import urlencode

from pydantic import BaseModel, ConfigDict, Field

from finances.db.repos import categories as categories_repo
from finances.reports import monthly as monthly_report
from finances.web.services.transactions_query import UNCATEGORIZED

# ---------------------------------------------------------------------------
# Tunable caps. Documented choices, not free knobs.
# ---------------------------------------------------------------------------

#: Cap rows on the desktop pivot to keep it readable. Excess categories
#: collapse into a synthetic "Other" row at the tail. The chart has its
#: own (smaller) cap below — the two are intentionally independent.
PIVOT_TOP_N: int = 25

#: Cap chart series to top 5 + 1 "Other" bucket.
#:
#: This is not a free knob. The palette holds exactly five entity hues
#: (``--series-1``..``--series-5`` in signal.css) because six could not be
#: found that clear the colour-separation floors on every pair. Raising the
#: cap without adding validated hues means two series drawn in the same
#: colour, which is worse than folding one of them into Other. What competes
#: for the slots is a *series* — a group or an ungrouped category (ADR-026),
#: which is how the owner's fixed household costs became visible without a
#: sixth hue. The category filter is the way into the tail; a series'
#: ``members`` is the way to read it without leaving the chart.
CHART_TOP_N: int = 5

#: Number of entity hues in the categorical palette.
CHART_COLOR_SLOTS: int = 5

#: The slot "Other" always takes — a neutral, outside the hue range. Other is
#: a remainder rather than a thing that happened, so giving it a hue would
#: claim an identity it does not have, and would spend one of five scarce
#: slots on the bucket that means least.
OTHER_COLOR_SLOT: int = -1

#: The remainder's label. Stored as a category's ``group_name`` it is the
#: instruction to fold that category into the remainder whatever its rank
#: (ADR-026 §2.6 as amended 2026-09-08 — Lending). Not a group: it never
#: competes for a slot and never holds a palette rank. The remainder is
#: still computed for everything else that misses the cap.
OTHER_LABEL = "Other"

_KIND_INCOME = "income"
_KIND_EXPENSE = "expense"

# A synthetic key for category_name == None so we can differentiate
# "uncategorized" from missing data when grouping. The DTOs preserve
# ``None`` on the wire; this constant is internal only.
_UNCATEGORIZED_KEY = "__uncategorized__"
_UNCATEGORIZED_LABEL = "Uncategorized"


# ---------------------------------------------------------------------------
# Filter model.
# ---------------------------------------------------------------------------


class MonthlyKind(StrEnum):
    EXPENSE = "expense"
    INCOME = "income"
    NET = "net"


class MonthlyRangePreset(StrEnum):
    M3 = "3m"
    M6 = "6m"
    M12 = "12m"
    YTD = "ytd"
    ALL = "all"
    CUSTOM = "custom"


class MonthlyFilter(BaseModel):
    """URL-persistent filter state for /monthly."""

    model_config = ConfigDict(extra="forbid")

    range_preset: MonthlyRangePreset = MonthlyRangePreset.M6
    since: str | None = None  # YYYY-MM, only honoured when preset == CUSTOM
    until: str | None = None
    kind: MonthlyKind = MonthlyKind.EXPENSE
    accounts: list[str] = Field(default_factory=list)
    categories: list[str] = Field(default_factory=list)
    currencies: list[str] = Field(default_factory=list)
    include_bcv_fallback: bool = True


# ---------------------------------------------------------------------------
# Pivot DTOs.
# ---------------------------------------------------------------------------


class PivotCell(BaseModel):
    model_config = ConfigDict(extra="forbid")

    total_usd: Decimal
    fallback_usd: Decimal
    tx_count: int
    needs_review_count: int
    drill_url: str


class PivotRow(BaseModel):
    model_config = ConfigDict(extra="forbid")

    category_name: str | None
    cells_by_month: dict[str, PivotCell]
    row_total_usd: Decimal
    row_fallback_usd: Decimal


class PivotTotals(BaseModel):
    model_config = ConfigDict(extra="forbid")

    column_total_usd: dict[str, Decimal]
    column_fallback_usd: dict[str, Decimal]
    grand_total_usd: Decimal
    grand_fallback_usd: Decimal


class MonthlyPivot(BaseModel):
    model_config = ConfigDict(extra="forbid")

    months: list[str]
    rows: list[PivotRow]
    totals: PivotTotals
    filter: MonthlyFilter


# ---------------------------------------------------------------------------
# Chart DTOs.
# ---------------------------------------------------------------------------


class MonthlyChartSeries(BaseModel):
    model_config = ConfigDict(extra="forbid")

    #: The series' label: a group, an ungrouped category, or ``"Other"``.
    #: A series is a group for grouped categories and a category for
    #: ungrouped ones (ADR-026); the field keeps its original name because
    #: every consumer reads it as "the thing drawn".
    category: str
    values: list[Decimal]
    #: Palette slot, 0-based, or ``OTHER_COLOR_SLOT`` for the remainder.
    #: Decided on a ranking the category filter cannot move, so deselecting
    #: one category does not repaint the rest — see
    #: :func:`_assign_color_slots`.
    color_slot: int = OTHER_COLOR_SLOT
    #: What is inside this series, largest first; empty when nothing is. A
    #: group fills it with its categories; "Other" fills it with the series
    #: that missed the cap. One mechanism for the overlay to open a block
    #: (ADR-026 §2.4) — on a real ledger the block that most needs opening
    #: has been the largest one on the chart. Members carry their parent's
    #: slot: they are the contents of one block, not blocks of their own.
    members: list[MonthlyChartSeries] = Field(default_factory=list)
    #: One /transactions URL per month, aligned with ``values``, carrying
    #: one ``categories=`` parameter per category the series stands for.
    drill_urls: list[str] = Field(default_factory=list)


class MonthlyChart(BaseModel):
    model_config = ConfigDict(extra="forbid")

    months: list[str]
    series: list[MonthlyChartSeries]
    fallback_per_month: list[Decimal]
    filter: MonthlyFilter


# ---------------------------------------------------------------------------
# Mobile DTOs.
# ---------------------------------------------------------------------------


class MonthlyMobileCategory(BaseModel):
    model_config = ConfigDict(extra="forbid")

    category_name: str | None
    total_usd: Decimal
    fallback_usd: Decimal
    needs_review_count: int
    pct_of_month: float
    drill_url: str


class MonthlyMobile(BaseModel):
    model_config = ConfigDict(extra="forbid")

    month: str
    prev_month: str | None
    next_month: str | None
    categories: list[MonthlyMobileCategory]
    month_total_usd: Decimal
    month_fallback_usd: Decimal
    filter: MonthlyFilter


# ---------------------------------------------------------------------------
# Range resolution.
# ---------------------------------------------------------------------------


def _shift_month(year: int, month: int, delta: int) -> tuple[int, int]:
    total = year * 12 + (month - 1) + delta
    return total // 12, (total % 12) + 1


def _month_str(year: int, month: int) -> str:
    return f"{year:04d}-{month:02d}"


def _parse_month(value: str) -> tuple[int, int]:
    y, m = value.split("-")
    return int(y), int(m)


def _months_between(since: str, until: str) -> list[str]:
    """Inclusive list of YYYY-MM strings, oldest -> newest."""
    sy, sm = _parse_month(since)
    uy, um = _parse_month(until)
    out: list[str] = []
    cy, cm = sy, sm
    while (cy, cm) <= (uy, um):
        out.append(_month_str(cy, cm))
        cy, cm = _shift_month(cy, cm, 1)
    return out


def _db_month_bounds(conn: sqlite3.Connection) -> tuple[str, str] | None:
    """Span of months in the transactions table (or ``None`` if empty)."""
    row = conn.execute(
        """
        SELECT MIN(strftime('%Y-%m', occurred_at)) AS lo,
               MAX(strftime('%Y-%m', occurred_at)) AS hi
        FROM transactions
        """
    ).fetchone()
    if row is None or row["lo"] is None:
        return None
    return row["lo"], row["hi"]


def resolve_month_range(
    f: MonthlyFilter,
    *,
    today: date | None = None,
    conn: sqlite3.Connection | None = None,
) -> tuple[str, str]:
    """Compute ``(since, until)`` as ``YYYY-MM`` from a filter + today.

    For ``ALL`` the bounds come from the DB (``conn`` required); for the
    other presets the calendar is enough.
    """
    today = today or date.today()
    cur = _month_str(today.year, today.month)

    preset = f.range_preset
    if preset == MonthlyRangePreset.M3:
        sy, sm = _shift_month(today.year, today.month, -2)
        return _month_str(sy, sm), cur
    if preset == MonthlyRangePreset.M6:
        sy, sm = _shift_month(today.year, today.month, -5)
        return _month_str(sy, sm), cur
    if preset == MonthlyRangePreset.M12:
        sy, sm = _shift_month(today.year, today.month, -11)
        return _month_str(sy, sm), cur
    if preset == MonthlyRangePreset.YTD:
        return _month_str(today.year, 1), cur
    if preset == MonthlyRangePreset.ALL:
        if conn is None:
            # No connection — fall back to a sensible 12m window.
            sy, sm = _shift_month(today.year, today.month, -11)
            return _month_str(sy, sm), cur
        bounds = _db_month_bounds(conn)
        if bounds is None:
            return cur, cur
        return bounds
    # CUSTOM
    if not f.since or not f.until:
        # Soft-fall back to 6m if custom fields are missing — the API
        # surface validates format separately.
        sy, sm = _shift_month(today.year, today.month, -5)
        return _month_str(sy, sm), cur
    return f.since, f.until


# ---------------------------------------------------------------------------
# Internal grouping primitives.
# ---------------------------------------------------------------------------


def _row_matches_filter(
    row: monthly_report.MonthlyRow, f: MonthlyFilter, *, kinds: Iterable[str]
) -> bool:
    if row.kind not in kinds:
        return False
    if f.accounts and row.account_name not in f.accounts:
        return False
    if f.categories:
        # ``__none__`` is the absence of a category, the same sentinel the
        # /transactions WHERE builder reads. Without this the Uncategorized
        # option renders and then matches nothing, which reads as a broken
        # filter rather than an empty category.
        if row.category_name is None:
            if UNCATEGORIZED not in f.categories:
                return False
        elif row.category_name not in f.categories:
            return False
    if f.currencies and row.currency not in f.currencies:
        return False
    return True


def _kinds_for(filter_kind: MonthlyKind) -> tuple[str, ...]:
    if filter_kind == MonthlyKind.EXPENSE:
        return (_KIND_EXPENSE,)
    if filter_kind == MonthlyKind.INCOME:
        return (_KIND_INCOME,)
    return (_KIND_INCOME, _KIND_EXPENSE)


def _signed_total(
    row: monthly_report.MonthlyRow, filter_kind: MonthlyKind
) -> tuple[Decimal, Decimal]:
    """Return ``(total_usd, fallback_usd)`` signed for the active kind.

    For NET, expense contributes negatively and income positively so the
    cell sum reads as ``income - expense``.
    """
    if filter_kind == MonthlyKind.NET and row.kind == _KIND_EXPENSE:
        return -row.total_usd, -row.fallback_usd
    return row.total_usd, row.fallback_usd


def _category_label(name: str | None) -> str:
    return _UNCATEGORIZED_LABEL if name is None else name


def _category_key(name: str | None) -> str:
    return _UNCATEGORIZED_KEY if name is None else name


def _key_to_name(key: str) -> str | None:
    return None if key == _UNCATEGORIZED_KEY else key


def _month_first_day(month: str) -> str:
    return f"{month}-01"


def _month_last_day(month: str) -> str:
    """Last calendar day of the given YYYY-MM (no calendar import needed)."""
    y, m = _parse_month(month)
    if m == 12:
        ny, nm = y + 1, 1
    else:
        ny, nm = y, m + 1
    # The day before the first of the next month.
    from datetime import date as _date, timedelta as _td

    return (_date(ny, nm, 1) - _td(days=1)).isoformat()


def _assign_color_slots(labels: Iterable[str]) -> dict[str, int]:
    """Map categories to palette slots from a filter-independent ordering.

    The property that matters is that **the category filter must not repaint
    the chart**. The old ``ink[i % len]`` keyed colour to a series' position
    among the *visible* ones, so deselecting the largest category shifted a
    colour onto every survivor.

    ``labels`` therefore arrives ranked over the window with the category
    filter lifted, and the slot is that position. Because the ordering does
    not depend on which categories are selected, a category keeps its colour
    however the owner narrows the chart.

    Slots repeat every fifth rank, so a filter can still surface two
    categories that want the same one — Dating at rank 3 and Rent at rank 8
    drew identically on the owner's ledger. :func:`_resolve_slot_collisions`
    settles that among the categories actually on screen; this map is the
    preference it starts from.

    A digest of the name would key colour to identity outright, but hashing
    five categories into five slots leaves them all distinct only about 4% of
    the time, so a filtered chart would collide far more often than a rank
    does.
    """
    return {
        label: index % CHART_COLOR_SLOTS for index, label in enumerate(labels)
    }


def _resolve_slot_collisions(
    visible: list[str], preferred: dict[str, int]
) -> dict[str, int]:
    """Give every visible category its own slot, moving as few as possible.

    Two categories in the same colour makes the legend a lie, so uniqueness
    among what is drawn has to win. The cost is paid by whichever of a
    colliding pair sits deeper in the stable ordering: ``visible`` arrives in
    that order, so the category that owns the slot keeps it and the intruder
    walks to the next free one. The reader tracking the bigger category — the
    likelier one — never sees it move.

    Nothing moves in the default view, where the top five already hold the
    five slots.
    """
    taken: set[int] = set()
    out: dict[str, int] = {}
    for label in visible:
        want = preferred.get(label, 0) % CHART_COLOR_SLOTS
        for step in range(CHART_COLOR_SLOTS):
            slot = (want + step) % CHART_COLOR_SLOTS
            if slot not in taken:
                taken.add(slot)
                out[label] = slot
                break
        else:  # pragma: no cover - CHART_TOP_N == CHART_COLOR_SLOTS
            out[label] = want
    return out


def _drill_url(
    *, month: str, category: str | None, kind: MonthlyKind
) -> str:
    """Build the /transactions drill URL for one pivot or mobile cell.

    ``None`` keeps its long-standing meaning here — no category parameter,
    the month as a whole. The chart's Uncategorized series drills with the
    sentinel instead; see :func:`_filter_value`.
    """
    return _series_drill_url(
        month=month,
        categories=[] if category is None else [category],
        kind=kind,
    )


def _filter_value(category_name: str | None) -> str:
    """The ``categories=`` value that selects a category's rows on
    /transactions: its name, or the sentinel for "no category"."""
    return UNCATEGORIZED if category_name is None else category_name


def _series_drill_url(
    *, month: str, categories: Iterable[str], kind: MonthlyKind
) -> str:
    """Build the /transactions drill URL for one month of one chart series.

    One ``categories=`` parameter per category the series stands for — a
    group's members, a category itself, everything in Other's tail — which
    the repeated-parameter contract on /transactions already accepts
    (ADR-026 §2.5).
    """
    params: list[tuple[str, str]] = [
        ("date_from", _month_first_day(month)),
        ("date_to", _month_last_day(month)),
    ]
    for category in categories:
        params.append(("categories", category))
    # NET cells naturally span both kinds; default the link to expense
    # so the user lands on the most useful drill, but include a hint.
    if kind == MonthlyKind.INCOME:
        params.append(("kinds", "income"))
    elif kind == MonthlyKind.EXPENSE:
        params.append(("kinds", "expense"))
    else:
        params.append(("kinds", "expense"))
        params.append(("kinds", "income"))
    return "/transactions?" + urlencode(params, doseq=True)


# ---------------------------------------------------------------------------
# Pivot.
# ---------------------------------------------------------------------------


def build_pivot(
    conn: sqlite3.Connection,
    f: MonthlyFilter,
    *,
    today: date | None = None,
) -> MonthlyPivot:
    """Build the desktop pivot DTO from the canonical monthly report."""
    since, until = resolve_month_range(f, today=today, conn=conn)
    report = monthly_report.build_report(conn, since=since, until=until)
    months = _months_between(since, until)
    kinds = _kinds_for(f.kind)

    # Aggregator: category_key -> month -> (total_usd, fallback_usd, tx_count, nr_count).
    cells: dict[str, dict[str, list[Decimal | int]]] = defaultdict(
        lambda: defaultdict(lambda: [Decimal("0"), Decimal("0"), 0, 0])
    )
    cat_first_seen: dict[str, str | None] = {}

    for r in report.rows:
        if not _row_matches_filter(r, f, kinds=kinds):
            continue
        key = _category_key(r.category_name)
        cat_first_seen.setdefault(key, r.category_name)
        total, fallback = _signed_total(r, f.kind)
        bucket = cells[key][r.month]
        bucket[0] += total
        bucket[1] += fallback
        bucket[2] += r.tx_count
        bucket[3] += r.needs_review_count

    # Build PivotRow per category with stable cell coverage across all months.
    rows: list[PivotRow] = []
    for key, by_month in cells.items():
        cat_name = cat_first_seen[key]
        cells_by_month: dict[str, PivotCell] = {}
        row_total = Decimal("0")
        row_fallback = Decimal("0")
        for m in months:
            bucket = by_month.get(m)
            if bucket is None:
                cell = PivotCell(
                    total_usd=Decimal("0"),
                    fallback_usd=Decimal("0"),
                    tx_count=0,
                    needs_review_count=0,
                    drill_url=_drill_url(month=m, category=cat_name, kind=f.kind),
                )
            else:
                cell = PivotCell(
                    total_usd=bucket[0],
                    fallback_usd=bucket[1],
                    tx_count=int(bucket[2]),
                    needs_review_count=int(bucket[3]),
                    drill_url=_drill_url(month=m, category=cat_name, kind=f.kind),
                )
            cells_by_month[m] = cell
            row_total += cell.total_usd
            row_fallback += cell.fallback_usd
        rows.append(
            PivotRow(
                category_name=cat_name,
                cells_by_month=cells_by_month,
                row_total_usd=row_total,
                row_fallback_usd=row_fallback,
            )
        )

    # Sort by absolute row_total desc.
    rows.sort(key=lambda r: abs(r.row_total_usd), reverse=True)

    # Cap to top N; tail collapses into a synthetic Other row.
    if len(rows) > PIVOT_TOP_N:
        head = rows[:PIVOT_TOP_N]
        tail = rows[PIVOT_TOP_N:]
        # Aggregate tail into a single Other row.
        other_cells: dict[str, PivotCell] = {}
        for m in months:
            t = sum((r.cells_by_month[m].total_usd for r in tail), Decimal("0"))
            fb = sum((r.cells_by_month[m].fallback_usd for r in tail), Decimal("0"))
            tx = sum(r.cells_by_month[m].tx_count for r in tail)
            nr = sum(r.cells_by_month[m].needs_review_count for r in tail)
            other_cells[m] = PivotCell(
                total_usd=t,
                fallback_usd=fb,
                tx_count=tx,
                needs_review_count=nr,
                drill_url=_drill_url(month=m, category=None, kind=f.kind),
            )
        other_row = PivotRow(
            category_name="Other",
            cells_by_month=other_cells,
            row_total_usd=sum(
                (c.total_usd for c in other_cells.values()), Decimal("0")
            ),
            row_fallback_usd=sum(
                (c.fallback_usd for c in other_cells.values()), Decimal("0")
            ),
        )
        rows = [*head, other_row]

    # Column + grand totals.
    column_total: dict[str, Decimal] = {m: Decimal("0") for m in months}
    column_fallback: dict[str, Decimal] = {m: Decimal("0") for m in months}
    for r in rows:
        for m in months:
            cell = r.cells_by_month[m]
            column_total[m] += cell.total_usd
            column_fallback[m] += cell.fallback_usd
    grand_total = sum(column_total.values(), Decimal("0"))
    grand_fallback = sum(column_fallback.values(), Decimal("0"))

    return MonthlyPivot(
        months=months,
        rows=rows,
        totals=PivotTotals(
            column_total_usd=column_total,
            column_fallback_usd=column_fallback,
            grand_total_usd=grand_total,
            grand_fallback_usd=grand_fallback,
        ),
        filter=f,
    )


# ---------------------------------------------------------------------------
# Chart.
# ---------------------------------------------------------------------------


# A chart series is keyed by what it stands for: ``("group", name)`` for
# the categories that roll up into a group, ``("category", category_key)``
# for the ones that stand alone. Keying on the label alone would let a
# group and a category with the same name silently merge.
_SeriesKey = tuple[str, str]
_GROUP = "group"
_CATEGORY = "category"

_MonthTotals = dict[str, Decimal]


def _group_by_category_id(conn: sqlite3.Connection) -> dict[int, str]:
    """``category_id -> group_name`` for every grouped category (ADR-026).

    Read by id, not name: the report rows carry both, and a name is only
    unique within a kind.
    """
    return {
        c.id: c.group_name
        for c in categories_repo.list_all(conn, include_inactive=True)
        if c.id is not None and c.group_name is not None
    }


def _series_key_for(
    row: monthly_report.MonthlyRow, group_of: dict[int, str]
) -> _SeriesKey:
    group = group_of.get(row.category_id) if row.category_id is not None else None
    if group is not None:
        return (_GROUP, group)
    return (_CATEGORY, _category_key(row.category_name))


def _magnitude(by_month: _MonthTotals) -> Decimal:
    return abs(sum(by_month.values(), Decimal("0")))


def build_chart(
    conn: sqlite3.Connection,
    f: MonthlyFilter,
    *,
    today: date | None = None,
) -> MonthlyChart:
    """Top-5 + Other stacked-bar series + per-month BCV fallback shadow.

    A series is a **group** for grouped categories and a **category** for
    ungrouped ones (ADR-026 §2.3). Groups and ungrouped categories rank
    together, and the cap and the Other remainder apply to that list. The
    category filter still filters *categories* (§2.5): selecting Rent alone
    draws a Home series containing Rent, so the chart never changes shape
    because a filter is on.
    """
    since, until = resolve_month_range(f, today=today, conn=conn)
    report = monthly_report.build_report(conn, since=since, until=until)
    months = _months_between(since, until)
    kinds = _kinds_for(f.kind)
    group_of = _group_by_category_id(conn)

    # series -> month -> total_usd  (signed per kind for NET).
    totals: dict[_SeriesKey, _MonthTotals] = defaultdict(
        lambda: defaultdict(lambda: Decimal("0"))
    )
    # series -> category_key -> month -> total_usd. Every series has at least
    # one category under it; a group has several, and those are its members.
    member_totals: dict[_SeriesKey, dict[str, _MonthTotals]] = defaultdict(
        lambda: defaultdict(lambda: defaultdict(lambda: Decimal("0")))
    )
    # The same accumulation with the *category* filter lifted. Only the colour
    # assignment reads it: a series' slot has to be decided on a basis the
    # category filter cannot move, or deselecting one category repaints the
    # rest. Every other part of the filter still applies, so the basis follows
    # the range, kind, accounts and currencies on screen.
    totals_unfiltered: dict[_SeriesKey, _MonthTotals] = defaultdict(
        lambda: defaultdict(lambda: Decimal("0"))
    )
    labels: dict[_SeriesKey, str] = {}
    category_names: dict[str, str | None] = {}
    fallback_per_month: dict[str, Decimal] = {m: Decimal("0") for m in months}

    palette_filter = f.model_copy(update={"categories": []})
    for r in report.rows:
        if not _row_matches_filter(r, palette_filter, kinds=kinds):
            continue
        key = _series_key_for(r, group_of)
        labels.setdefault(
            key, key[1] if key[0] == _GROUP else _category_label(r.category_name)
        )
        cat_key = _category_key(r.category_name)
        category_names.setdefault(cat_key, r.category_name)
        total, fallback = _signed_total(r, f.kind)
        totals_unfiltered[key][r.month] += total

        if not _row_matches_filter(r, f, kinds=kinds):
            continue
        totals[key][r.month] += total
        member_totals[key][cat_key][r.month] += total
        if r.month in fallback_per_month:
            fallback_per_month[r.month] += abs(fallback)

    # A category stored as Other never competes: it leaves the ranking and
    # the palette basis before either is built, so it holds neither a slot
    # nor a rank. It rejoins as part of the remainder below.
    folded_key: _SeriesKey = (_GROUP, OTHER_LABEL)
    folded = totals.pop(folded_key, None)
    totals_unfiltered.pop(folded_key, None)

    # Rank by absolute window sum and slice top N.
    ranked = sorted(totals.items(), key=lambda kv: _magnitude(kv[1]), reverse=True)
    head = ranked[:CHART_TOP_N]
    tail = ranked[CHART_TOP_N:]

    # Slots are decided on the unfiltered ranking of series, so a group or
    # category keeps its colour whichever of its neighbours — or its own
    # members — the owner deselects.
    palette_ranked = sorted(
        totals_unfiltered.items(), key=lambda kv: _magnitude(kv[1]), reverse=True
    )
    palette_labels = [labels[key] for key, _ in palette_ranked]
    preferred = _assign_color_slots(palette_labels)
    stable_rank = {label: i for i, label in enumerate(palette_labels)}

    head_labels = [labels[key] for key, _ in head]
    # Settle collisions in the stable order, not the filtered one: the
    # series that owns a slot keeps it whoever else is on screen.
    slots = _resolve_slot_collisions(
        sorted(head_labels, key=lambda label: stable_rank.get(label, len(stable_rank))),
        preferred,
    )

    def values_for(by_month: _MonthTotals) -> list[Decimal]:
        return [by_month.get(m, Decimal("0")) for m in months]

    def selects_for(key: _SeriesKey) -> list[str]:
        """The ``categories=`` values that pick this series' rows."""
        return [_filter_value(category_names[cat_key]) for cat_key in member_totals[key]]

    def drill_urls_for(selects: list[str]) -> list[str]:
        return [
            _series_drill_url(month=m, categories=selects, kind=f.kind) for m in months
        ]

    def members_for(key: _SeriesKey, slot: int) -> list[MonthlyChartSeries]:
        """A group's categories, largest first, in the group's own colour:
        the contents of one block, not blocks of their own."""
        out: list[MonthlyChartSeries] = []
        for cat_key, cat_months in sorted(
            member_totals[key].items(), key=lambda kv: _magnitude(kv[1]), reverse=True
        ):
            name = category_names[cat_key]
            out.append(
                MonthlyChartSeries(
                    category=_category_label(name),
                    values=values_for(cat_months),
                    color_slot=slot,
                    drill_urls=drill_urls_for([_filter_value(name)]),
                )
            )
        return out

    def series_for(key: _SeriesKey, by_month: _MonthTotals, slot: int) -> MonthlyChartSeries:
        return MonthlyChartSeries(
            category=labels[key],
            values=values_for(by_month),
            color_slot=slot,
            members=members_for(key, slot) if key[0] == _GROUP else [],
            drill_urls=drill_urls_for(selects_for(key)),
        )

    series: list[MonthlyChartSeries] = []
    for key, by_month in head:
        series.append(series_for(key, by_month, slots[labels[key]]))

    if tail or folded is not None:
        parts: list[_MonthTotals] = [by_month for _, by_month in tail]
        # Ranked already — ``tail`` is the far end of the same sort, so what
        # missed the cap is a series: a small group folds in as itself,
        # members and all. A category stored as Other sits flat beside them
        # — it is not a group, so there is no inner Other to open. Every
        # member shares Other's neutral.
        members = [series_for(key, by_month, OTHER_COLOR_SLOT) for key, by_month in tail]
        selects = [value for key, _ in tail for value in selects_for(key)]
        if folded is not None:
            parts.append(folded)
            members.extend(members_for(folded_key, OTHER_COLOR_SLOT))
            selects.extend(selects_for(folded_key))
        members.sort(key=lambda s: abs(sum(s.values, Decimal("0"))), reverse=True)
        series.append(
            MonthlyChartSeries(
                category=OTHER_LABEL,
                values=[
                    sum((part.get(m, Decimal("0")) for part in parts), Decimal("0"))
                    for m in months
                ],
                color_slot=OTHER_COLOR_SLOT,
                members=members,
                drill_urls=drill_urls_for(selects),
            )
        )

    return MonthlyChart(
        months=months,
        series=series,
        fallback_per_month=[fallback_per_month[m] for m in months],
        filter=f,
    )


# ---------------------------------------------------------------------------
# Mobile.
# ---------------------------------------------------------------------------


def build_mobile(
    conn: sqlite3.Connection,
    f: MonthlyFilter,
    *,
    today: date | None = None,
    month: str | None = None,
) -> MonthlyMobile:
    """Single-month list view for mobile.

    The mobile view only renders one month at a time. ``month`` defaults
    to the current calendar month. ``prev_month`` / ``next_month`` are
    pure calendar arithmetic — they don't probe the DB for data presence.
    """
    today = today or date.today()
    if month is None:
        month = _month_str(today.year, today.month)

    report = monthly_report.build_report(conn, month=month)
    kinds = _kinds_for(f.kind)

    # Aggregate by category for this single month.
    by_cat: dict[str, list[Decimal | int | str | None]] = {}
    cat_first_seen: dict[str, str | None] = {}
    month_total = Decimal("0")
    month_fallback = Decimal("0")
    for r in report.rows:
        if not _row_matches_filter(r, f, kinds=kinds):
            continue
        key = _category_key(r.category_name)
        cat_first_seen.setdefault(key, r.category_name)
        total, fallback = _signed_total(r, f.kind)
        bucket = by_cat.setdefault(key, [Decimal("0"), Decimal("0"), 0])
        bucket[0] = bucket[0] + total
        bucket[1] = bucket[1] + fallback
        bucket[2] = int(bucket[2]) + r.needs_review_count
        month_total += total
        month_fallback += fallback

    # Sort by absolute total desc.
    sorted_cats = sorted(
        by_cat.items(),
        key=lambda kv: abs(kv[1][0]),
        reverse=True,
    )

    # For pct-of-month we use the absolute sum so the bar widths add up
    # cleanly even for NET (where signs differ).
    abs_total = sum((abs(v[0]) for v in by_cat.values()), Decimal("0"))

    cats: list[MonthlyMobileCategory] = []
    for key, bucket in sorted_cats:
        cat_name = cat_first_seen[key]
        total = bucket[0]
        fallback = bucket[1]
        nr = int(bucket[2])
        if abs_total > 0:
            pct = float(abs(total) / abs_total)
        else:
            pct = 0.0
        cats.append(
            MonthlyMobileCategory(
                category_name=cat_name,
                total_usd=total,
                fallback_usd=fallback,
                needs_review_count=nr,
                pct_of_month=pct,
                drill_url=_drill_url(month=month, category=cat_name, kind=f.kind),
            )
        )

    # prev / next via calendar arithmetic.
    y, m = _parse_month(month)
    py, pm = _shift_month(y, m, -1)
    ny, nm = _shift_month(y, m, 1)
    prev_month = _month_str(py, pm)
    next_month = _month_str(ny, nm)

    return MonthlyMobile(
        month=month,
        prev_month=prev_month,
        next_month=next_month,
        categories=cats,
        month_total_usd=month_total,
        month_fallback_usd=month_fallback,
        filter=f,
    )


__all__ = [
    "MonthlyChart",
    "MonthlyChartSeries",
    "MonthlyFilter",
    "MonthlyKind",
    "MonthlyMobile",
    "MonthlyMobileCategory",
    "MonthlyPivot",
    "MonthlyRangePreset",
    "PivotCell",
    "PivotRow",
    "PivotTotals",
    "build_chart",
    "build_mobile",
    "build_pivot",
    "resolve_month_range",
]
