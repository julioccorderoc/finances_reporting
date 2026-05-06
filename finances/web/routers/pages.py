"""Full HTML page routes (EPIC-022 / ADR-012, EPIC-023 Phase 2b/2a/2c/2d).

Phase 1 shipped the placeholder ``/`` route; Phase 2b added ``/transactions``;
Phase 2a wires the real dashboard at ``/``; Phase 2c adds ``/monthly`` (with
UA-driven mobile/desktop split); Phase 2d adds ``/accounts`` and ``/rates``.
Subsequent Phase 2 agents append their own page handlers here without
touching the existing ones.
"""

from __future__ import annotations

import sqlite3
from datetime import UTC, date, datetime
from typing import Literal

from fastapi import APIRouter, Depends, Query, Request

from finances.db.repos import accounts as accounts_repo
from finances.web.deps import get_conn
from finances.web.routers._monthly_filter_dep import monthly_filter_from_query
from finances.web.routers._tx_filter_dep import filter_from_query
from finances.web.services.accounts_view import build_account_cards
from finances.web.services.dashboard import (
    build_kpis,
    build_recent_activity,
    build_spend_trend,
    build_sync_status,
)
from finances.web.services.monthly_view import (
    MonthlyFilter,
    build_chart,
    build_mobile,
    build_pivot,
)
from finances.web.services.rates_view import (
    DEFAULT_RANGE_DAYS,
    build_latest_rates,
    build_rates_chart,
)
from finances.web.services.transactions_query import (
    TransactionsFilter,
    query_transactions,
)
from finances.web.services.triage import (
    TriageType,
    build_queue,
    get_skip_store,
)

router = APIRouter()


def _parse_triage_type(value: str | None) -> TriageType | None:
    if value in (None, "", "all"):
        return None
    try:
        return TriageType(value)
    except ValueError:
        return None


@router.get("/", include_in_schema=False)
def dashboard(
    request: Request,
    conn: sqlite3.Connection = Depends(get_conn),
):
    """Render the dashboard with KPI tiles, sync strip, recent activity, chart."""
    today = datetime.now(tz=UTC).date()
    kpis = build_kpis(conn, today=today)
    chips = build_sync_status(conn)
    recent = build_recent_activity(conn, limit=10)
    trend = build_spend_trend(conn, today=today, months_back=6)
    templates = request.app.state.templates
    return templates.TemplateResponse(
        request,
        "pages/dashboard.html",
        {
            "title": "Finances",
            "kpis": kpis,
            "chips": chips,
            "recent": recent,
            "trend": trend,
        },
    )


@router.get("/transactions", include_in_schema=False)
def transactions_page(
    request: Request,
    f: TransactionsFilter = Depends(filter_from_query),
    conn: sqlite3.Connection = Depends(get_conn),
):
    """Render the full /transactions page (filters + first list page)."""
    page = query_transactions(conn, f)

    # Filter dropdown options. Lightweight — these are short lists.
    accounts_options = [a.name for a in accounts_repo.list_all(conn, include_inactive=True)]
    kinds_options = ["income", "expense", "transfer", "adjustment"]
    currencies_options = sorted(
        {
            row["currency"]
            for row in conn.execute("SELECT DISTINCT currency FROM transactions").fetchall()
        }
    )
    sources_options = sorted(
        {
            row["source"]
            for row in conn.execute("SELECT DISTINCT source FROM transactions").fetchall()
        }
    )

    templates = request.app.state.templates
    return templates.TemplateResponse(
        request,
        "pages/transactions.html",
        {
            "title": "Transactions",
            "page": page,
            "filter": page.filter,
            "accounts_options": accounts_options,
            "kinds_options": kinds_options,
            "currencies_options": currencies_options,
            "sources_options": sources_options,
        },
    )


@router.get("/accounts", include_in_schema=False)
def accounts_page(
    request: Request,
    conn: sqlite3.Connection = Depends(get_conn),
):
    """Render the /accounts card grid (read-only, no filters)."""
    cards = build_account_cards(conn, today=date.today())
    templates = request.app.state.templates
    return templates.TemplateResponse(
        request,
        "pages/accounts.html",
        {"title": "Accounts", "cards": cards},
    )


@router.get("/triage", include_in_schema=False)
def triage_page(
    request: Request,
    type_filter: str | None = Query(default=None),
    conn: sqlite3.Connection = Depends(get_conn),
):
    """Render the /triage page (unified queue + filter chips).

    The page reads the per-app skip store so refreshes preserve any
    items the user already pushed to the bottom this session. The
    skip store is intentionally session-local; see services/triage.py.
    """
    parsed = _parse_triage_type(type_filter)
    skip_store = get_skip_store(request.app)
    queue = build_queue(
        conn,
        type_filter=parsed,
        skipped_ids=set(skip_store) if skip_store else None,
    )
    templates = request.app.state.templates
    return templates.TemplateResponse(
        request,
        "pages/triage.html",
        {
            "title": "Triage",
            "queue": queue,
            "active_filter": parsed.value if parsed is not None else None,
        },
    )


@router.get("/rates", include_in_schema=False)
def rates_page(
    request: Request,
    range_days: int = Query(DEFAULT_RANGE_DAYS, ge=1, le=3650),
    conn: sqlite3.Connection = Depends(get_conn),
):
    """Render the /rates page (chart + latest-per-pair card list)."""
    chart = build_rates_chart(conn, range_days=range_days)
    latest = build_latest_rates(conn)
    templates = request.app.state.templates
    return templates.TemplateResponse(
        request,
        "pages/rates.html",
        {
            "title": "Rates",
            "chart": chart,
            "latest": latest,
            "range_days": range_days,
            "range_options": [7, 30, 90, 365],
        },
    )


# ---------------------------------------------------------------------------
# /monthly — Phase 2c.
#
# This route is the only spot in v1 where we use a User-Agent heuristic to
# pick a template. The mobile and desktop pages have very different
# layouts (pivot vs. single-month list), so we render different templates
# rather than relying on CSS alone. The ``?layout=`` query param lets
# tests + power users force either layout regardless of UA.
# ---------------------------------------------------------------------------


def _is_mobile_layout(
    *, request: Request, layout: str | None
) -> bool:
    if layout == "mobile":
        return True
    if layout == "desktop":
        return False
    ua = request.headers.get("user-agent", "")
    return "mobile" in ua.lower()


@router.get("/monthly", include_in_schema=False)
def monthly_page(
    request: Request,
    layout: Literal["desktop", "mobile"] | None = Query(default=None),
    month: str | None = Query(default=None),
    f: MonthlyFilter = Depends(monthly_filter_from_query),
    conn: sqlite3.Connection = Depends(get_conn),
):
    """Render the /monthly page — desktop pivot or mobile single-month list."""
    templates = request.app.state.templates
    accounts_options = [
        a.name for a in accounts_repo.list_all(conn, include_inactive=True)
    ]
    currencies_options = sorted(
        {
            row["currency"]
            for row in conn.execute(
                "SELECT DISTINCT currency FROM transactions"
            ).fetchall()
        }
    )

    if _is_mobile_layout(request=request, layout=layout):
        mobile = build_mobile(conn, f, month=month)
        return templates.TemplateResponse(
            request,
            "pages/monthly_mobile.html",
            {
                "title": "Monthly",
                "mobile": mobile,
                "filter": f,
                "accounts_options": accounts_options,
                "currencies_options": currencies_options,
            },
        )

    pivot = build_pivot(conn, f)
    chart = build_chart(conn, f)
    return templates.TemplateResponse(
        request,
        "pages/monthly.html",
        {
            "title": "Monthly",
            "pivot": pivot,
            "chart": chart,
            "filter": f,
            "accounts_options": accounts_options,
            "currencies_options": currencies_options,
        },
    )
