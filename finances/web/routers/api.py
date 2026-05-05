"""JSON API endpoints (EPIC-022 / ADR-012, EPIC-023 Phase 2b/2c).

Phase 2b wires the read-only ``GET /api/transactions`` endpoint. The
shape (``TransactionsPage``) is the foundation for the deferred
EPIC-016 mobile API; the HTMX layer does not depend on it.

Phase 2c wires ``GET /api/monthly`` returning ``MonthlyPivot`` (or
``MonthlyMobile`` when ``?layout=mobile``).
"""

from __future__ import annotations

import sqlite3
from typing import Literal

from fastapi import APIRouter, Depends, Query

from finances.web.deps import get_conn
from finances.web.routers._monthly_filter_dep import monthly_filter_from_query
from finances.web.routers._tx_filter_dep import filter_from_query
from finances.web.services.monthly_view import (
    MonthlyFilter,
    MonthlyMobile,
    MonthlyPivot,
    build_mobile,
    build_pivot,
)
from finances.web.services.transactions_query import (
    TransactionsFilter,
    TransactionsPage,
    query_transactions,
)

router = APIRouter(prefix="/api")


@router.get(
    "/transactions",
    response_model=TransactionsPage,
)
def transactions_list_json(
    f: TransactionsFilter = Depends(filter_from_query),
    conn: sqlite3.Connection = Depends(get_conn),
) -> TransactionsPage:
    """Return paginated, filtered transactions as JSON."""
    return query_transactions(conn, f)


@router.get("/monthly")
def monthly_json(
    layout: Literal["desktop", "mobile"] | None = Query(default=None),
    month: str | None = Query(default=None),
    f: MonthlyFilter = Depends(monthly_filter_from_query),
    conn: sqlite3.Connection = Depends(get_conn),
) -> MonthlyPivot | MonthlyMobile:
    """Return the monthly pivot (default) or mobile DTO when ``layout=mobile``.

    Note we don't use ``response_model`` here because the response shape
    branches on a query parameter; the union return type is enforced by
    the Pydantic models themselves.
    """
    if layout == "mobile":
        return build_mobile(conn, f, month=month)
    return build_pivot(conn, f)
