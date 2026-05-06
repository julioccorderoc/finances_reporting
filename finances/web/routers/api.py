"""JSON API endpoints (EPIC-022 / ADR-012, EPIC-023 Phase 2b/2a/2d).

Phase 2b wires ``GET /api/transactions``. Phase 2a appends the dashboard
JSON endpoints used by the deferred mobile API and easy debugging via
``curl``. Phase 2d adds ``GET /api/accounts`` and ``GET /api/rates``
with the same JSON-first discipline. The HTMX layer does not depend on
any of these endpoints.
"""

from __future__ import annotations

import sqlite3
from datetime import UTC, date, datetime

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel, ConfigDict

from finances.web.deps import get_conn
from finances.web.routers._tx_filter_dep import filter_from_query
from finances.web.services.accounts_view import AccountCard, build_account_cards
from finances.web.services.dashboard import (
    KpiTiles,
    SpendTrend,
    build_kpis,
    build_recent_activity,
    build_spend_trend,
)
from finances.web.services.rates_view import (
    DEFAULT_RANGE_DAYS,
    LatestRateCard,
    RatesChart,
    build_latest_rates,
    build_rates_chart,
)
from finances.web.services.transactions_query import (
    TransactionCard,
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


# ---------------------------------------------------------------------------
# Dashboard endpoints (Phase 2a).
# ---------------------------------------------------------------------------


@router.get("/dashboard/kpis", response_model=KpiTiles)
def dashboard_kpis(
    conn: sqlite3.Connection = Depends(get_conn),
) -> KpiTiles:
    """Return the four KPI tiles as JSON."""
    today = datetime.now(tz=UTC).date()
    return build_kpis(conn, today=today)


@router.get("/dashboard/recent", response_model=list[TransactionCard])
def dashboard_recent(
    conn: sqlite3.Connection = Depends(get_conn),
) -> list[TransactionCard]:
    """Return the most recent income/expense transactions as cards."""
    return build_recent_activity(conn, limit=10)


@router.get("/dashboard/spend-trend", response_model=SpendTrend)
def dashboard_spend_trend(
    conn: sqlite3.Connection = Depends(get_conn),
) -> SpendTrend:
    """Return the 6-month stacked-bar dataset for the spend chart."""
    today = datetime.now(tz=UTC).date()
    return build_spend_trend(conn, today=today, months_back=6)


# ---------------------------------------------------------------------------
# Accounts + rates endpoints (Phase 2d).
# ---------------------------------------------------------------------------


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
