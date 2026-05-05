"""HTMX fragment endpoints (EPIC-022 / ADR-012, EPIC-023 Phase 2b/2a).

Phase 2b wires the ``/_partial/transactions/list`` swap target. Phase 2a
appends ``/_partial/dashboard/sync-status`` so the dashboard can poll
the live sync state every 60 seconds without rerendering the whole page.
"""

from __future__ import annotations

import sqlite3

from fastapi import APIRouter, Depends, Request

from finances.web.deps import get_conn
from finances.web.routers._tx_filter_dep import filter_from_query
from finances.web.services.dashboard import build_sync_status
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
