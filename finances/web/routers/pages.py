"""Full HTML page routes (EPIC-022 / ADR-012, EPIC-023 Phase 2b/2c).

Phase 1 ships the placeholder ``/`` route; Phase 2b adds ``/transactions``;
Phase 2c adds ``/monthly`` (with UA-driven mobile/desktop split).
Subsequent Phase 2 agents append their own page handlers here without
touching the existing ones.
"""

from __future__ import annotations

import sqlite3
from typing import Literal

from fastapi import APIRouter, Depends, Query, Request

from finances.db.repos import accounts as accounts_repo
from finances.web.deps import get_conn
from finances.web.routers._monthly_filter_dep import monthly_filter_from_query
from finances.web.services.monthly_view import (
    MonthlyFilter,
    build_chart,
    build_mobile,
    build_pivot,
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
