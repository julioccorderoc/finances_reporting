"""JSON API endpoints (EPIC-022 / ADR-012, EPIC-023 Phase 2b/2a/2c/2d).

Phase 2b wires ``GET /api/transactions``. Phase 2a appends the dashboard
JSON endpoints used by the deferred mobile API and easy debugging via
``curl``. Phase 2c adds ``GET /api/monthly`` (pivot or mobile DTO).
Phase 2d adds ``GET /api/accounts`` and ``GET /api/rates`` with the
same JSON-first discipline. The HTMX layer does not depend on any of
these endpoints.
"""

from __future__ import annotations

import json
import sqlite3
from datetime import UTC, date, datetime
from typing import Literal

from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response
from pydantic import BaseModel, ConfigDict, Field

from finances.db.repos import categories as categories_repo
from finances.db.repos import transactions as transactions_repo
from finances.web.deps import get_conn
from finances.web.routers._monthly_filter_dep import monthly_filter_from_query
from finances.web.routers._tx_filter_dep import filter_from_query
from finances.web.services.accounts_view import AccountCard, build_account_cards
from finances.web.services.dashboard import (
    KpiTiles,
    SpendTrend,
    build_kpis,
    build_recent_activity,
    build_spend_trend,
)
from finances.web.services.monthly_view import (
    MonthlyFilter,
    MonthlyMobile,
    MonthlyPivot,
    build_mobile,
    build_pivot,
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
from finances.web.services.transactions_write import (
    TransactionEditRequest,
    apply_edit,
)
from finances.web.services.triage import (
    TriageQueue,
    TriageType,
    build_queue,
    confirm_pair,
    get_skip_store,
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


@router.patch(
    "/transactions/{txn_id}",
    response_model=TransactionCard,
)
def transactions_patch(
    txn_id: int,
    req: TransactionEditRequest,
    conn: sqlite3.Connection = Depends(get_conn),
) -> TransactionCard:
    """Apply category / user_rate edits and return the updated card.

    Per ADR-012 the modal Save is atomic per click — one PATCH carries
    all dirty fields. ``needs_review`` is fully derived from
    ``rates.resolve``; the body has no toggle for it.
    """
    try:
        return apply_edit(conn, txn_id=txn_id, req=req)
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


# ---------------------------------------------------------------------------
# Bulk edit (UX overhaul WP4).
# ---------------------------------------------------------------------------


class BulkEditRequest(BaseModel):
    """Body for POST /api/transactions/bulk-edit.

    ``category_id: null`` is an explicit bulk clear — the UI only sends
    it via the picker's "remove category" control, never as a side
    effect of an untouched picker. ``extra="forbid"`` keeps
    ``needs_review`` (derived-only) and everything else out.
    """

    model_config = ConfigDict(extra="forbid")

    ids: list[int] = Field(min_length=1)
    category_id: int | None = None


class BulkEditResponse(BaseModel):
    """JSON shape returned by POST /api/transactions/bulk-edit."""

    model_config = ConfigDict(extra="forbid")

    updated: int


@router.post("/transactions/bulk-edit", response_model=BulkEditResponse)
def transactions_bulk_edit(
    body: BulkEditRequest,
    response: Response,
    conn: sqlite3.Connection = Depends(get_conn),
) -> BulkEditResponse:
    """Assign (or explicitly clear) one category on many transactions.

    Per rule-012 every row goes through the sanctioned
    ``transactions_repo.update()`` — no parallel UPDATE SQL in web code.
    The loop runs inside ONE DB transaction: an unknown id rolls the
    whole batch back and returns 404 (all-or-nothing — a partially
    applied bulk edit would be invisible to the user).

    Category is the only bulk-editable field in v1. ``needs_review`` is
    untouched: it is derived from the rate resolver, and category
    changes cannot affect it (see ADR-005 / ADR-012).
    """
    if body.category_id is not None:
        if categories_repo.get_by_id(conn, body.category_id) is None:
            raise HTTPException(
                status_code=422, detail=f"unknown category id={body.category_id}"
            )

    # deps.get_conn opens autocommit connections (isolation_level=None);
    # explicit BEGIN/COMMIT makes the per-row loop atomic.
    conn.execute("BEGIN")
    try:
        for txn_id in body.ids:
            transactions_repo.update(conn, id=txn_id, category_id=body.category_id)
    except LookupError as exc:
        conn.execute("ROLLBACK")
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception:
        conn.execute("ROLLBACK")
        raise
    conn.execute("COMMIT")

    updated = len(body.ids)
    response.headers["HX-Trigger"] = json.dumps(
        {"toast": {"level": "success", "message": f"{updated} updated"}}
    )
    return BulkEditResponse(updated=updated)


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
# Monthly endpoint (Phase 2c).
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# Triage endpoints (Phase 4 / EPIC-025).
# ---------------------------------------------------------------------------


def _parse_triage_type(value: str | None) -> TriageType | None:
    if value in (None, "", "all"):
        return None
    try:
        return TriageType(value)
    except ValueError as exc:
        raise HTTPException(
            status_code=422, detail=f"unknown type_filter: {value!r}"
        ) from exc


@router.get("/triage", response_model=TriageQueue)
def triage_list_json(
    request: Request,
    type_filter: str | None = Query(default=None),
    conn: sqlite3.Connection = Depends(get_conn),
) -> TriageQueue:
    """Return the unified triage queue as JSON.

    ``type_filter`` filters by ``rate``, ``category``, or ``pair``. The
    ``counts`` dict in the response is always unfiltered so chip badges
    stay accurate.
    """
    parsed = _parse_triage_type(type_filter)
    skip_store = get_skip_store(request.app)
    return build_queue(
        conn,
        type_filter=parsed,
        skipped_ids=set(skip_store) if skip_store else None,
    )


class _TransfersCreateRequest(BaseModel):
    """Body for POST /api/transfers (pair-confirm)."""

    model_config = ConfigDict(extra="forbid")

    deposit_id: int
    sell_id: int


class _TransfersCreateResponse(BaseModel):
    """JSON shape returned by POST /api/transfers."""

    model_config = ConfigDict(extra="forbid")

    transfer_id: str
    from_transaction_id: int
    to_transaction_id: int


@router.post("/transfers", response_model=_TransfersCreateResponse)
def transfers_create(
    body: _TransfersCreateRequest,
    conn: sqlite3.Connection = Depends(get_conn),
) -> _TransfersCreateResponse:
    """Pair a bank deposit + Binance sell as a transfer (rule-002).

    Per ADR-002 / rule-002 this delegates to
    :func:`finances.domain.transfers.create_transfer` (mode 3 — both
    anchors). Returns a 404 if either id is unknown, 422 if either is
    already part of a transfer.
    """
    try:
        result = confirm_pair(
            conn, deposit_id=body.deposit_id, sell_id=body.sell_id
        )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    return _TransfersCreateResponse(**result)
