"""JSON API endpoints (EPIC-022 / ADR-012, EPIC-023 Phase 2b/2d).

Phase 2b wires the read-only ``GET /api/transactions`` endpoint. The
shape (``TransactionsPage``) is the foundation for the deferred
EPIC-016 mobile API; the HTMX layer does not depend on it. Phase 2d
adds ``GET /api/accounts`` and ``GET /api/rates`` with the same
JSON-first discipline.
"""

from __future__ import annotations

import sqlite3
from datetime import date

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel, ConfigDict

from finances.web.deps import get_conn
from finances.web.routers._tx_filter_dep import filter_from_query
from finances.web.services.accounts_view import AccountCard, build_account_cards
from finances.web.services.rates_view import (
    DEFAULT_RANGE_DAYS,
    LatestRateCard,
    RatesChart,
    build_latest_rates,
    build_rates_chart,
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


@router.get(
    "/accounts",
    response_model=list[AccountCard],
)
def accounts_list_json(
    conn: sqlite3.Connection = Depends(get_conn),
) -> list[AccountCard]:
    """Return the same card list /accounts renders, as JSON."""
    return build_account_cards(conn, today=date.today())


class RatesResponse(BaseModel):
    """Combined response for /api/rates: chart + latest-per-pair list."""

    model_config = ConfigDict(extra="forbid")

    chart: RatesChart
    latest: list[LatestRateCard]


@router.get(
    "/rates",
    response_model=RatesResponse,
)
def rates_json(
    range_days: int = Query(DEFAULT_RANGE_DAYS, ge=1, le=3650),
    conn: sqlite3.Connection = Depends(get_conn),
) -> RatesResponse:
    """Return both the chart series and the latest-per-pair card list."""
    return RatesResponse(
        chart=build_rates_chart(conn, range_days=range_days),
        latest=build_latest_rates(conn),
    )
