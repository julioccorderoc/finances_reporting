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
from decimal import Decimal, InvalidOperation

from fastapi import APIRouter, Depends, Form, HTTPException, Query, Request, Response

from finances.db.repos import accounts as accounts_repo
from finances.db.repos import categories as categories_repo
from finances.db.repos import transactions as transactions_repo
from finances.web.deps import get_conn
from finances.web.routers._monthly_filter_dep import monthly_filter_from_query
from finances.web.routers._tx_filter_dep import filter_from_query
from finances.web.services.dashboard import build_sync_status
from finances.web.services.transactions_query import _project_card
from finances.web.services.transactions_write import (
    TransactionEditRequest,
    apply_edit,
)
from finances.web.services.triage import (
    TriageType,
    build_queue,
    confirm_pair,
    get_skip_store,
)
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


# ---------------------------------------------------------------------------
# Transaction edit modal partials (Phase 3 / EPIC-024).
#
# Two endpoints back the row-edit flow:
#
#   GET  /_partial/transactions/{id}/modal — renders the modal markup,
#        targeted at #tx-modal-host on the base layout.
#   POST /_partial/transactions/{id}/edit  — accepts a form-encoded
#        TransactionEditRequest, applies the change via apply_edit,
#        returns the updated card_transaction.html partial, and sets
#        the HX-Trigger: closeModal response header so the Alpine
#        listener on <body> can clear the host div.
#
# Form encoding choice (Phase 3 plan, simpler-of-two):
#   The modal *always* sets ``set_category=true`` and ``set_user_rate=true``
#   so submitting an empty string clears that field. There is no per-field
#   "set_*" checkbox in the rendered HTML — but the API still accepts the
#   ``set_*=false`` shape from JSON callers via the same Pydantic model.
# ---------------------------------------------------------------------------


def _parse_optional_int(value: str | None) -> int | None:
    if value is None:
        return None
    s = value.strip()
    if not s:
        return None
    try:
        return int(s)
    except ValueError as exc:
        raise HTTPException(
            status_code=422, detail=f"category_id must be an integer or empty: {value!r}"
        ) from exc


def _parse_optional_decimal(value: str | None) -> Decimal | None:
    if value is None:
        return None
    s = value.strip()
    if not s:
        return None
    try:
        return Decimal(s)
    except InvalidOperation as exc:
        raise HTTPException(
            status_code=422, detail=f"user_rate must be a decimal or empty: {value!r}"
        ) from exc


def _parse_form_bool(value: str | None) -> bool:
    if value is None:
        return False
    return value.strip().lower() in {"1", "true", "yes", "on"}


@router.get("/transactions/{txn_id}/modal", include_in_schema=False)
def transactions_modal_partial(
    request: Request,
    txn_id: int,
    conn: sqlite3.Connection = Depends(get_conn),
):
    """Render the edit modal for ``txn_id``.

    Returns 404 if the transaction does not exist. Renders the modal
    template with both the read-only provenance block (source,
    source_ref, rate_source, etc.) and the editable form. The form
    targets ``[data-tx-id="{id}"]`` so HTMX can ``outerHTML`` swap the
    row card on save.
    """
    txn = transactions_repo.get_by_id(conn, txn_id)
    if txn is None:
        raise HTTPException(status_code=404, detail=f"transaction id={txn_id} not found")

    # Account name + category name for the read-only header / current
    # selection in the dropdown.
    account_row = conn.execute(
        "SELECT name FROM accounts WHERE id = ?", (txn.account_id,)
    ).fetchone()
    account_name = account_row["name"] if account_row else ""

    category_name: str | None = None
    if txn.category_id is not None:
        cat = categories_repo.get_by_id(conn, txn.category_id)
        category_name = cat.name if cat else None

    card = _project_card(
        conn,
        txn,
        account_name=account_name,
        category_name=category_name,
    )

    categories = categories_repo.list_all(conn)

    templates = request.app.state.templates
    return templates.TemplateResponse(
        request,
        "partials/modal_transaction.html",
        {
            "txn": txn,
            "card": card,
            "categories": categories,
            "account_name": account_name,
        },
    )


@router.post("/transactions/{txn_id}/edit", include_in_schema=False)
def transactions_edit_partial(
    request: Request,
    txn_id: int,
    set_category: str | None = Form(default=None),
    category_id: str | None = Form(default=None),
    set_user_rate: str | None = Form(default=None),
    user_rate: str | None = Form(default=None),
    conn: sqlite3.Connection = Depends(get_conn),
):
    """Apply the modal-form edit and return the updated card partial.

    On success the response carries ``HX-Trigger: closeModal`` so the
    Alpine listener on ``<body>`` (added in base.html) can clear the
    modal host div.
    """
    req = TransactionEditRequest(
        set_category=_parse_form_bool(set_category),
        category_id=_parse_optional_int(category_id),
        set_user_rate=_parse_form_bool(set_user_rate),
        user_rate=_parse_optional_decimal(user_rate),
    )

    try:
        card = apply_edit(conn, txn_id=txn_id, req=req)
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    templates = request.app.state.templates
    response = templates.TemplateResponse(
        request,
        "partials/card_transaction.html",
        {"card": card},
    )
    response.headers["HX-Trigger"] = "closeModal"
    return response


# ---------------------------------------------------------------------------
# Triage partials (Phase 4 / EPIC-025).
# ---------------------------------------------------------------------------


def _parse_triage_type_partial(value: str | None) -> TriageType | None:
    if value in (None, "", "all"):
        return None
    try:
        return TriageType(value)
    except ValueError:
        return None


def _render_queue_partial(request: Request, conn: sqlite3.Connection):
    """Render only the inner queue list (pairs with hx-target=#triage-queue)."""
    skip_store = get_skip_store(request.app)
    queue = build_queue(
        conn,
        type_filter=None,
        skipped_ids=set(skip_store) if skip_store else None,
    )
    templates = request.app.state.templates
    return templates.TemplateResponse(
        request,
        "partials/triage_queue.html",
        {"queue": queue},
    )


@router.get("/triage/queue", include_in_schema=False)
def triage_queue_partial(
    request: Request,
    type_filter: str | None = Query(default=None),
    conn: sqlite3.Connection = Depends(get_conn),
):
    """Return the queue partial — used by filter chips + post-save refresh."""
    parsed = _parse_triage_type_partial(type_filter)
    skip_store = get_skip_store(request.app)
    queue = build_queue(
        conn,
        type_filter=parsed,
        skipped_ids=set(skip_store) if skip_store else None,
    )
    templates = request.app.state.templates
    return templates.TemplateResponse(
        request,
        "partials/triage_queue.html",
        {"queue": queue},
    )


@router.get("/triage/{txn_id}/modal", include_in_schema=False)
def triage_modal_partial(
    request: Request,
    txn_id: int,
    conn: sqlite3.Connection = Depends(get_conn),
):
    """Render the Save & next variant of the txn-edit modal."""
    txn = transactions_repo.get_by_id(conn, txn_id)
    if txn is None:
        raise HTTPException(
            status_code=404, detail=f"transaction id={txn_id} not found"
        )

    account_row = conn.execute(
        "SELECT name FROM accounts WHERE id = ?", (txn.account_id,)
    ).fetchone()
    account_name = account_row["name"] if account_row else ""

    category_name: str | None = None
    if txn.category_id is not None:
        cat = categories_repo.get_by_id(conn, txn.category_id)
        category_name = cat.name if cat else None

    card = _project_card(
        conn,
        txn,
        account_name=account_name,
        category_name=category_name,
    )

    categories = categories_repo.list_all(conn)

    templates = request.app.state.templates
    return templates.TemplateResponse(
        request,
        "partials/modal_transaction_triage.html",
        {
            "txn": txn,
            "card": card,
            "categories": categories,
            "account_name": account_name,
        },
    )


@router.post("/triage/{txn_id}/edit", include_in_schema=False)
def triage_edit_partial(
    request: Request,
    txn_id: int,
    set_category: str | None = Form(default=None),
    category_id: str | None = Form(default=None),
    set_user_rate: str | None = Form(default=None),
    user_rate: str | None = Form(default=None),
    conn: sqlite3.Connection = Depends(get_conn),
):
    """Apply the edit and return a fresh queue partial.

    Sets ``HX-Trigger: closeModal, advanceQueue`` so the base.html
    Alpine listener clears the modal host and the page advances to the
    next item.
    """
    req = TransactionEditRequest(
        set_category=_parse_form_bool(set_category),
        category_id=_parse_optional_int(category_id),
        set_user_rate=_parse_form_bool(set_user_rate),
        user_rate=_parse_optional_decimal(user_rate),
    )

    try:
        apply_edit(conn, txn_id=txn_id, req=req)
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    response = _render_queue_partial(request, conn)
    response.headers["HX-Trigger"] = "closeModal, advanceQueue"
    return response


@router.get(
    "/triage/pair/{deposit_id}/{sell_id}/modal",
    include_in_schema=False,
)
def triage_pair_modal_partial(
    request: Request,
    deposit_id: int,
    sell_id: int,
    conn: sqlite3.Connection = Depends(get_conn),
):
    """Render the pair-confirm modal for the (deposit, sell) pair.

    The modal carries the proposal so the user can confirm or skip.
    """
    from finances.web.services.transactions_query import _project_card
    from finances.web.services.triage import PairProposal

    deposit = transactions_repo.get_by_id(conn, deposit_id)
    sell = transactions_repo.get_by_id(conn, sell_id)
    if deposit is None or sell is None:
        raise HTTPException(
            status_code=404,
            detail=f"transaction id={deposit_id if deposit is None else sell_id} not found",
        )

    deposit_account = conn.execute(
        "SELECT name FROM accounts WHERE id = ?", (deposit.account_id,)
    ).fetchone()
    sell_account = conn.execute(
        "SELECT name FROM accounts WHERE id = ?", (sell.account_id,)
    ).fetchone()
    deposit_card = _project_card(
        conn,
        deposit,
        account_name=deposit_account["name"] if deposit_account else "",
        category_name=None,
    )
    sell_card = _project_card(
        conn,
        sell,
        account_name=sell_account["name"] if sell_account else "",
        category_name=None,
    )

    proposal = PairProposal(
        proposal_id=f"{deposit_id}:{sell_id}",
        deposit=deposit_card,
        sell=sell_card,
        confidence=1.0,
        details={
            "bank_transaction_id": deposit_id,
            "binance_transaction_id": sell_id,
        },
    )

    templates = request.app.state.templates
    return templates.TemplateResponse(
        request,
        "partials/modal_pair_confirm.html",
        {"proposal": proposal},
    )


@router.post(
    "/triage/pair/{deposit_id}/{sell_id}/confirm",
    include_in_schema=False,
)
def triage_pair_confirm_partial(
    request: Request,
    deposit_id: int,
    sell_id: int,
    conn: sqlite3.Connection = Depends(get_conn),
):
    """Confirm the pair → calls :func:`confirm_pair` → fresh queue."""
    try:
        confirm_pair(conn, deposit_id=deposit_id, sell_id=sell_id)
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    response = _render_queue_partial(request, conn)
    response.headers["HX-Trigger"] = "closeModal"
    return response


@router.post("/triage/skip/{item_id:path}", include_in_schema=False)
def triage_skip_partial(
    request: Request,
    item_id: str,
    conn: sqlite3.Connection = Depends(get_conn),
):
    """Push ``item_id`` into the per-app skip store; return queue partial."""
    skip_store = get_skip_store(request.app)
    skip_store.add(item_id)
    response = _render_queue_partial(request, conn)
    response.headers["HX-Trigger"] = "closeModal"
    return response


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
