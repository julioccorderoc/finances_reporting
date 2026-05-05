"""JSON API endpoints (EPIC-022 / ADR-012, EPIC-023 Phase 2b).

Phase 2b wires the read-only ``GET /api/transactions`` endpoint. The
shape (``TransactionsPage``) is the foundation for the deferred
EPIC-016 mobile API; the HTMX layer does not depend on it.
"""

from __future__ import annotations

import sqlite3

from fastapi import APIRouter, Depends

from finances.web.deps import get_conn
from finances.web.routers._tx_filter_dep import filter_from_query
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
