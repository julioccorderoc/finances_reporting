"""Full HTML page routes (EPIC-022 / ADR-012, EPIC-023 Phase 2b/2d).

Phase 1 ships the placeholder ``/`` route; Phase 2b adds ``/transactions``;
Phase 2d adds ``/accounts`` and ``/rates``. Subsequent Phase 2 agents append
their own page handlers here without touching the existing ones.
"""

from __future__ import annotations

import sqlite3
from datetime import date

from fastapi import APIRouter, Depends, Query, Request

from finances.db.repos import accounts as accounts_repo
from finances.web.deps import get_conn
from finances.web.services.accounts_view import build_account_cards
from finances.web.services.rates_view import (
    DEFAULT_RANGE_DAYS,
    build_latest_rates,
    build_rates_chart,
)
from finances.web.services.transactions_query import (
    TransactionsFilter,
    query_transactions,
)
from finances.web.routers._tx_filter_dep import filter_from_query

router = APIRouter()


@router.get("/", include_in_schema=False)
def dashboard(request: Request):
    templates = request.app.state.templates
    return templates.TemplateResponse(
        request,
        "pages/dashboard.html",
        {"title": "Finances"},
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
