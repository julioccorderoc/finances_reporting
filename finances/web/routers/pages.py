"""Full HTML page routes (EPIC-022 / ADR-012, EPIC-023 Phase 2b/2a).

Phase 1 shipped the placeholder ``/`` route; Phase 2b added
``/transactions``; Phase 2a wires the real dashboard at ``/``.
Subsequent Phase 2 agents append their own page handlers here without
touching the existing ones.
"""

from __future__ import annotations

import sqlite3
from datetime import UTC, datetime

from fastapi import APIRouter, Depends, Request

from finances.db.repos import accounts as accounts_repo
from finances.web.deps import get_conn
from finances.web.services.dashboard import (
    build_kpis,
    build_recent_activity,
    build_spend_trend,
    build_sync_status,
)
from finances.web.services.transactions_query import (
    TransactionsFilter,
    query_transactions,
)
from finances.web.routers._tx_filter_dep import filter_from_query

router = APIRouter()


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
