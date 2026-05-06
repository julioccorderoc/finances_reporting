"""HTMX fragment endpoints (EPIC-022 / ADR-012, EPIC-023 Phase 2b/2a/2c/2d).

Phase 2b wires the ``/_partial/transactions/list`` swap target. The
fragment shares the same filter dependency as the full page so the URL
state stays the source of truth for both. Phase 2a appends
``/_partial/dashboard/sync-status`` so the dashboard can poll the live
sync state every 60 seconds. Phase 2c adds
``/_partial/monthly/{pivot,chart,mobile}`` swap targets for the
``/monthly`` page. Phase 2d adds the rates chart range-toggle fragment.
"""

from __future__ import annotations

import sqlite3

from fastapi import APIRouter, Depends, Query, Request

from finances.web.deps import get_conn
from finances.web.routers._monthly_filter_dep import monthly_filter_from_query
from finances.web.routers._tx_filter_dep import filter_from_query
from finances.web.services.dashboard import build_sync_status
from finances.web.services.monthly_view import (
    MonthlyFilter,
    build_chart,
    build_mobile,
    build_pivot,
)
from finances.web.services.rates_view import DEFAULT_RANGE_DAYS, build_rates_chart
from finances.web.services.transactions_query import (
    TransactionsFilter,
    query_transactions,
)

router = APIRouter(prefix="/_partial")


@router.get("/transactions/list", include_in_schema=False)
def transactions_list_partial(
    request: Request,
    f: TransactionsFilter = Depends(filter_from_query),
    conn: sqlite3.Connection = Depends(get_conn),
):
    """Return ONLY the list partial — no base.html shell.

    Used by HTMX to swap ``#tx-list`` when filters / sort / page change.
    """
    page = query_transactions(conn, f)
    templates = request.app.state.templates
    return templates.TemplateResponse(
        request,
        "partials/transactions_list.html",
        {"page": page, "filter": page.filter},
    )


@router.get("/dashboard/sync-status", include_in_schema=False)
def dashboard_sync_status_partial(
    request: Request,
    conn: sqlite3.Connection = Depends(get_conn),
):
    """Return ONLY the sync-status strip partial — no base.html shell.

    Polled every 60s by HTMX from the dashboard to keep the strip live.
    """
    chips = build_sync_status(conn)
    templates = request.app.state.templates
    return templates.TemplateResponse(
        request,
        "partials/sync_status_strip.html",
        {"chips": chips},
    )


# ---------------------------------------------------------------------------
# Monthly partials (Phase 2c).
# ---------------------------------------------------------------------------


@router.get("/monthly/pivot", include_in_schema=False)
def monthly_pivot_partial(
    request: Request,
    f: MonthlyFilter = Depends(monthly_filter_from_query),
    conn: sqlite3.Connection = Depends(get_conn),
):
    """Return only the pivot fragment (filter / range / kind change swap)."""
    pivot = build_pivot(conn, f)
    templates = request.app.state.templates
    return templates.TemplateResponse(
        request,
        "partials/monthly_pivot.html",
        {"pivot": pivot, "filter": f},
    )


@router.get("/monthly/chart", include_in_schema=False)
def monthly_chart_partial(
    request: Request,
    f: MonthlyFilter = Depends(monthly_filter_from_query),
    conn: sqlite3.Connection = Depends(get_conn),
):
    """Return only the chart fragment."""
    chart = build_chart(conn, f)
    templates = request.app.state.templates
    return templates.TemplateResponse(
        request,
        "partials/monthly_chart.html",
        {"chart": chart, "filter": f},
    )


@router.get("/monthly/mobile", include_in_schema=False)
def monthly_mobile_partial(
    request: Request,
    month: str | None = Query(default=None),
    f: MonthlyFilter = Depends(monthly_filter_from_query),
    conn: sqlite3.Connection = Depends(get_conn),
):
    """Return only the mobile single-month fragment (chevron / filter swap)."""
    mobile = build_mobile(conn, f, month=month)
    templates = request.app.state.templates
    return templates.TemplateResponse(
        request,
        "partials/monthly_mobile_inner.html",
        {"mobile": mobile, "filter": f},
    )


# ---------------------------------------------------------------------------
# Rates partials (Phase 2d).
# ---------------------------------------------------------------------------


@router.get("/rates/chart", include_in_schema=False)
def rates_chart_partial(
    request: Request,
    range_days: int = Query(DEFAULT_RANGE_DAYS, ge=1, le=3650),
    conn: sqlite3.Connection = Depends(get_conn),
):
    """Return ONLY the rates chart fragment for HTMX range-toggle swap."""
    chart = build_rates_chart(conn, range_days=range_days)
    templates = request.app.state.templates
    return templates.TemplateResponse(
        request,
        "partials/rates_chart.html",
        {
            "chart": chart,
            "range_days": range_days,
            "range_options": [7, 30, 90, 365],
        },
    )
