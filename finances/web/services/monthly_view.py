"""Pivot / chart / mobile view-models for /monthly (EPIC-023, Phase 2c).

This module is a *projection* over :func:`finances.reports.monthly.build_report`.
Per rule-005 + rule-012 we never reimplement the rate-resolution or
aggregation logic — the canonical totals come from ``MonthlyReport`` and
this module only re-shapes them into:

* :class:`MonthlyPivot` — category × month grid for the desktop view.
* :class:`MonthlyChart` — top-5 + Other stacked-bar series, plus a
  per-month BCV fallback shadow series.
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

from finances.reports import monthly as monthly_report

# ---------------------------------------------------------------------------
# Tunable caps. Documented choices, not free knobs.
# ---------------------------------------------------------------------------

#: Cap rows on the desktop pivot to keep it readable. Excess categories
#: collapse into a synthetic "Other" row at the tail. The chart has its
#: own (smaller) cap below — the two are intentionally independent.
PIVOT_TOP_N: int = 25

#: Cap chart series to top 5 categories + 1 "Other" bucket.
CHART_TOP_N: int = 5

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

    category: str
    values: list[Decimal]


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
        if row.category_name is None:
            return False
        if row.category_name not in f.categories:
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


def _drill_url(
    *, month: str, category: str | None, kind: MonthlyKind
) -> str:
    """Build the /transactions drill URL for one cell."""
    params: list[tuple[str, str]] = [
        ("date_from", _month_first_day(month)),
        ("date_to", _month_last_day(month)),
    ]
    if category is not None:
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


def build_chart(
    conn: sqlite3.Connection,
    f: MonthlyFilter,
    *,
    today: date | None = None,
) -> MonthlyChart:
    """Top-5 + Other stacked-bar series + per-month BCV fallback shadow."""
    since, until = resolve_month_range(f, today=today, conn=conn)
    report = monthly_report.build_report(conn, since=since, until=until)
    months = _months_between(since, until)
    kinds = _kinds_for(f.kind)

    # category_key -> month -> total_usd  (signed per kind for NET).
    by_cat: dict[str, dict[str, Decimal]] = defaultdict(
        lambda: defaultdict(lambda: Decimal("0"))
    )
    cat_first_seen: dict[str, str | None] = {}
    fallback_per_month: dict[str, Decimal] = {m: Decimal("0") for m in months}

    for r in report.rows:
        if not _row_matches_filter(r, f, kinds=kinds):
            continue
        key = _category_key(r.category_name)
        cat_first_seen.setdefault(key, r.category_name)
        total, fallback = _signed_total(r, f.kind)
        by_cat[key][r.month] += total
        if r.month in fallback_per_month:
            fallback_per_month[r.month] += abs(fallback)

    # Rank by absolute row sum and slice top N.
    ranked = sorted(
        by_cat.items(),
        key=lambda kv: abs(sum(kv[1].values(), Decimal("0"))),
        reverse=True,
    )
    head = ranked[:CHART_TOP_N]
    tail = ranked[CHART_TOP_N:]

    series: list[MonthlyChartSeries] = []
    for key, by_month in head:
        label = _category_label(cat_first_seen[key])
        values = [by_month.get(m, Decimal("0")) for m in months]
        series.append(MonthlyChartSeries(category=label, values=values))

    if tail:
        other_values: list[Decimal] = []
        for m in months:
            other_values.append(
                sum((by_month.get(m, Decimal("0")) for _, by_month in tail), Decimal("0"))
            )
        series.append(MonthlyChartSeries(category="Other", values=other_values))

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
