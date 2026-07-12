"""Single self-contained static HTML report (`finances html`).

The owner's #1 wish: one file he can double-click any time, no server, no
internet, still works if Python breaks next year. This module renders that
file.

Design (see docs/plans/revival/04-static-report.md):

* **Zero duplicated math.** Every number is a projection over the existing
  viewer query services (:mod:`finances.web.services`) and the canonical
  :func:`finances.reports.monthly.build_report`. Report figures equal viewer
  figures by construction (rule-012).
* **Read-only.** :func:`build_report_context` and :func:`export_html` issue
  SELECTs only — a static file can never write to the DB, and neither does
  the export path.
* **Standalone.** One Jinja template with inline CSS, inline JS, embedded
  JSON, and the vendored Chart.js source inlined verbatim. No CDN, no
  ``http(s)://`` anywhere in the output (its license banners are stripped).
* **Headline rate = USDT/P2P**, never BCV (rule-005 / ADR-005 amendment);
  BCV appears only in the reference rate-trend chart.
"""

from __future__ import annotations

import json
import re
import sqlite3
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path

from jinja2 import Environment, FileSystemLoader, select_autoescape
from pydantic import BaseModel, ConfigDict

from finances.format import fmt_date, fmt_money, fmt_month, fmt_number
from finances.reports import monthly as monthly_report
from finances.web.services.dashboard import build_recent_activity
from finances.web.services.monthly_view import (
    MonthlyFilter,
    MonthlyKind,
    build_mobile,
)
from finances.web.services.net_worth import compute_net_worth
from finances.web.services.rates_view import build_rates_chart
from finances.web.services.transactions_query import TransactionCard

# --- tunables (documented choices, not free knobs) -------------------------

#: A source whose newest transaction is older than this many days is flagged
#: stale in the freshness strip. The ritual is weekly; 10 days gives a grace
#: window before the warning fires.
STALE_DAYS = 10
#: Months of income/expense/net history in the header bar chart.
MONTHS_BACK = 12
#: Trailing window for the BCV-vs-P2P rate-trend line chart.
RATE_TREND_DAYS = 90
#: How many recent transactions to list at the foot of the report.
RECENT_LIMIT = 20
#: Cap the category breakdown so a long tail doesn't dominate the page.
CATEGORY_TOP_N = 12

_TEMPLATES_DIR = Path(__file__).resolve().parent / "templates"
_CHARTJS_PATH = (
    Path(__file__).resolve().parent.parent
    / "web"
    / "static"
    / "vendor"
    / "chart.umd.min.js"
)
_UNCATEGORIZED_LABEL = "Uncategorized"


# ---------------------------------------------------------------------------
# View-model (all Decimal; the template formats, the JSON payload floats).
# ---------------------------------------------------------------------------


class AccountBalance(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str
    currency: str
    balance_native: Decimal
    balance_usdt: Decimal | None


class SourceFreshness(BaseModel):
    model_config = ConfigDict(extra="forbid")

    source: str
    last_txn_date: date | None
    days_old: int | None
    stale: bool


class MonthlyFlow(BaseModel):
    model_config = ConfigDict(extra="forbid")

    month: str
    income_usd: Decimal
    expense_usd: Decimal
    net_usd: Decimal


class CategoryAmount(BaseModel):
    model_config = ConfigDict(extra="forbid")

    category: str
    total_usd: Decimal


class RateTrendPoint(BaseModel):
    model_config = ConfigDict(extra="forbid")

    date: date
    rate: Decimal


class RateTrendSeries(BaseModel):
    model_config = ConfigDict(extra="forbid")

    label: str
    points: list[RateTrendPoint]


class RecentTxn(BaseModel):
    model_config = ConfigDict(extra="forbid")

    occurred_at: datetime
    account: str
    description: str
    amount_native: Decimal
    currency: str
    amount_usd: Decimal | None
    kind: str
    category: str | None
    needs_review: bool
    is_bcv_fallback: bool


class ReportContext(BaseModel):
    model_config = ConfigDict(extra="forbid")

    generated_at: datetime
    # Header.
    net_worth_usd: Decimal
    accounts: list[AccountBalance]
    missing_pairs: list[str]
    freshness: list[SourceFreshness]
    stale_sources: list[str]
    # Monthly flows.
    monthly_flows: list[MonthlyFlow]
    month_income_usd: Decimal
    month_expense_usd: Decimal
    # Category breakdown.
    current_month: str
    prev_month: str
    category_current: list[CategoryAmount]
    category_prev: list[CategoryAmount]
    # Rate trend.
    rate_trend: list[RateTrendSeries]
    # Needs review + recent.
    needs_review_count: int
    recent: list[RecentTxn]


# ---------------------------------------------------------------------------
# Month helpers.
# ---------------------------------------------------------------------------


def _months_back(today: date, months_back: int) -> list[str]:
    """``months_back`` ``YYYY-MM`` labels ending with ``today``'s month."""
    pairs: list[tuple[int, int]] = []
    y, m = today.year, today.month
    for _ in range(months_back):
        pairs.append((y, m))
        m -= 1
        if m == 0:
            m, y = 12, y - 1
    pairs.reverse()
    return [f"{yy:04d}-{mm:02d}" for yy, mm in pairs]


def _shift_month(month: str, delta: int) -> str:
    y, m = (int(x) for x in month.split("-"))
    total = y * 12 + (m - 1) + delta
    return f"{total // 12:04d}-{total % 12 + 1:02d}"


def _category_amounts(conn: sqlite3.Connection, month: str) -> list[CategoryAmount]:
    mobile = build_mobile(conn, MonthlyFilter(kind=MonthlyKind.EXPENSE), month=month)
    out = [
        CategoryAmount(
            category=(c.category_name if c.category_name is not None else _UNCATEGORIZED_LABEL),
            total_usd=c.total_usd,
        )
        for c in mobile.categories
    ]
    return out[:CATEGORY_TOP_N]


def _freshness(conn: sqlite3.Connection, today: date) -> list[SourceFreshness]:
    rows = conn.execute(
        """
        SELECT source, MAX(date(occurred_at)) AS last_date
        FROM transactions
        GROUP BY source
        ORDER BY source
        """
    ).fetchall()
    out: list[SourceFreshness] = []
    for row in rows:
        raw = row["last_date"]
        if raw is None:
            out.append(
                SourceFreshness(
                    source=row["source"], last_txn_date=None, days_old=None, stale=True
                )
            )
            continue
        last = raw if isinstance(raw, date) else date.fromisoformat(str(raw))
        days = (today - last).days
        out.append(
            SourceFreshness(
                source=row["source"],
                last_txn_date=last,
                days_old=days,
                stale=days > STALE_DAYS,
            )
        )
    return out


def _recent_txn(card: TransactionCard) -> RecentTxn:
    return RecentTxn(
        occurred_at=card.occurred_at,
        account=card.account_name,
        description=card.description,
        amount_native=card.amount_native,
        currency=card.currency,
        amount_usd=card.amount_usd,
        kind=card.kind,
        category=card.category_name,
        needs_review=card.needs_review,
        is_bcv_fallback=card.is_bcv_fallback,
    )


# ---------------------------------------------------------------------------
# Context builder (read-only).
# ---------------------------------------------------------------------------


def build_report_context(
    conn: sqlite3.Connection, *, now: datetime
) -> ReportContext:
    """Compose every report section from the canonical viewer services.

    Pure read: issues SELECTs only, never mutates ``conn``.
    """
    today = now.date()

    # Header — net worth (USDT headline, never BCV) + per-account.
    nw = compute_net_worth(conn, as_of_date=today)
    accounts = [
        AccountBalance(
            name=c.account_name,
            currency=c.currency,
            balance_native=c.balance_native,
            balance_usdt=c.contribution_usdt,
        )
        for c in nw.contributions
    ]

    # Monthly income/expense/net — last 12 months from the canonical report.
    months = _months_back(today, MONTHS_BACK)
    report = monthly_report.build_report(conn, since=months[0], until=months[-1])
    income: dict[str, Decimal] = {m: Decimal("0") for m in months}
    expense: dict[str, Decimal] = {m: Decimal("0") for m in months}
    for r in report.rows:
        if r.month not in income:
            continue
        if r.kind == "income":
            income[r.month] += r.total_usd
        elif r.kind == "expense":
            expense[r.month] += r.total_usd
    monthly_flows = [
        MonthlyFlow(
            month=m,
            income_usd=income[m],
            expense_usd=expense[m],
            # ``total_usd`` is signed (expenses negative), so net is the signed
            # sum — income + expense, never income - expense.
            net_usd=income[m] + expense[m],
        )
        for m in months
    ]
    current_month = months[-1]
    prev_month = _shift_month(current_month, -1)

    # Category breakdown — current + previous month (expenses).
    category_current = _category_amounts(conn, current_month)
    category_prev = _category_amounts(conn, prev_month)

    # Rate trend — BCV vs P2P, last 90 days (reference chart).
    chart = build_rates_chart(conn, range_days=RATE_TREND_DAYS)
    rate_trend = [
        RateTrendSeries(
            label=s.label,
            points=[RateTrendPoint(date=p.as_of_date, rate=p.rate) for p in s.points],
        )
        for s in chart.series
    ]

    # Needs review + recent activity.
    needs_review_count = int(
        conn.execute(
            "SELECT COUNT(*) AS c FROM transactions WHERE needs_review = 1"
        ).fetchone()["c"]
    )
    recent = [_recent_txn(c) for c in build_recent_activity(conn, limit=RECENT_LIMIT)]

    # Freshness.
    freshness = _freshness(conn, today)
    stale_sources = [f.source for f in freshness if f.stale]

    return ReportContext(
        generated_at=now,
        net_worth_usd=nw.total_usdt,
        accounts=accounts,
        missing_pairs=nw.missing_pairs,
        freshness=freshness,
        stale_sources=stale_sources,
        monthly_flows=monthly_flows,
        month_income_usd=income[current_month],
        month_expense_usd=expense[current_month],
        current_month=current_month,
        prev_month=prev_month,
        category_current=category_current,
        category_prev=category_prev,
        rate_trend=rate_trend,
        needs_review_count=needs_review_count,
        recent=recent,
    )


# ---------------------------------------------------------------------------
# Rendering.
# ---------------------------------------------------------------------------


_ENV: Environment | None = None


def _jinja_env() -> Environment:
    global _ENV
    if _ENV is None:
        env = Environment(
            loader=FileSystemLoader(str(_TEMPLATES_DIR)),
            autoescape=select_autoescape(["html", "j2"], default_for_string=True),
            trim_blocks=True,
            lstrip_blocks=True,
        )
        env.filters.update(
            {
                "fmt_number": fmt_number,
                "fmt_money": fmt_money,
                "fmt_date": fmt_date,
                "fmt_month": fmt_month,
            }
        )
        _ENV = env
    return _ENV


def _chart_data_json(context: ReportContext) -> str:
    """Build the embedded ``REPORT_DATA`` JSON for the two charts.

    Only numbers + fixed series labels go in here (no user text), but we still
    neutralize ``<`` so a value can never close the host ``<script>`` element.
    """
    # Rate trend: unify the x-axis so both series share date labels.
    all_dates = sorted(
        {p.date for s in context.rate_trend for p in s.points}
    )
    labels = [d.isoformat() for d in all_dates]
    rate_series = []
    for s in context.rate_trend:
        by_date = {p.date: float(p.rate) for p in s.points}
        rate_series.append(
            {"label": s.label, "data": [by_date.get(d) for d in all_dates]}
        )

    payload = {
        "monthly": {
            "labels": [fmt_month(f.month) for f in context.monthly_flows],
            "income": [float(f.income_usd) for f in context.monthly_flows],
            "expense": [float(f.expense_usd) for f in context.monthly_flows],
            "net": [float(f.net_usd) for f in context.monthly_flows],
        },
        "rates": {"labels": labels, "series": rate_series},
    }
    return json.dumps(payload, separators=(",", ":")).replace("<", "\\u003c")


def render_html(context: ReportContext, *, chartjs_source: str) -> str:
    """Render the standalone HTML string from a context + inlined Chart.js."""
    template = _jinja_env().get_template("report.html.j2")
    return template.render(
        ctx=context,
        chart_data_json=_chart_data_json(context),
        chartjs_source=chartjs_source,
        stale_days=STALE_DAYS,
    )


def _inline_chartjs() -> str:
    """Return the vendored Chart.js source with URL-bearing banners stripped.

    The minified bundle's only block comments are three license banners, each
    carrying an ``https://`` URL that would violate the no-external-URL gate.
    Removing block comments that contain ``http`` leaves the executable body
    untouched (verified: the body has no other ``/* */`` comments).
    """
    src = _CHARTJS_PATH.read_text(encoding="utf-8")
    return re.sub(
        r"/\*.*?\*/",
        lambda m: "" if "http" in m.group(0) else m.group(0),
        src,
        flags=re.DOTALL,
    )


def export_html(
    conn: sqlite3.Connection,
    output_path: str | Path,
    *,
    now: datetime | None = None,
) -> Path:
    """Build the context, render the file, write it. Never touches the DB write side."""
    now = now or datetime.now(tz=UTC)
    context = build_report_context(conn, now=now)
    html = render_html(context, chartjs_source=_inline_chartjs())
    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(html, encoding="utf-8")
    return out


__all__ = [
    "AccountBalance",
    "CategoryAmount",
    "MonthlyFlow",
    "RECENT_LIMIT",
    "RateTrendPoint",
    "RateTrendSeries",
    "RecentTxn",
    "ReportContext",
    "SourceFreshness",
    "build_report_context",
    "export_html",
    "render_html",
]
