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

import json
import sqlite3
from decimal import Decimal, InvalidOperation

from fastapi import APIRouter, Depends, Form, HTTPException, Query, Request, Response

from finances.db.repos import accounts as accounts_repo
from finances.db.repos import categories as categories_repo
from finances.db.repos import saved_views as saved_views_repo
from finances.db.repos import transaction_edits as transaction_edits_repo
from finances.db.repos import transactions as transactions_repo
from finances.domain.models import SavedView
from finances.web.deps import get_conn
from finances.web.routers._monthly_filter_dep import monthly_filter_from_query
from finances.web.routers._tx_filter_dep import filter_from_query
from finances.web.services.category_stats import top_categories
from finances.web.services.dashboard import build_sync_status
from finances.web.services.pairing import find_pair_candidates
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
from finances.web.services.rates_view import (
    DEFAULT_RANGE_DAYS,
    build_rates_chart,
    rates_for_day,
)
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
# Form encoding choice (WP2 / ux-overhaul §2):
#   The modals dirty-track their controls with Alpine and submit
#   ``set_category=true`` / ``set_user_rate=true`` only for fields the
#   user actually touched; clearing a category is the explicit
#   "× remove category" control. The API still accepts any ``set_*``
#   combination from JSON callers via the same Pydantic model.
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


def _parse_optional_text(value: str | None) -> str | None:
    """Empty / whitespace-only form input means "clear the field"."""
    if value is None:
        return None
    s = value.strip()
    return s or None


def _hx_trigger_json(*events: str, toast_message: str) -> str:
    """Build an ``HX-Trigger`` header value: named events + a success toast.

    htmx accepts a JSON object in ``HX-Trigger``: each key is dispatched
    as an event, its value as the event detail. The base.html <body>
    listener re-dispatches ``closeModal`` as the ``close-modal`` window
    event and ``toast`` as ``show-toast`` (WP2 toast contract; error
    toasts come from the global htmx:responseError listener instead).
    """
    payload: dict[str, object] = {name: True for name in events}
    payload["toast"] = {"level": "success", "message": toast_message}
    return json.dumps(payload)


_EDIT_FIELD_LABELS = {
    "category_id": "Category",
    "user_rate": "User rate",
    "notes": "Notes",
}


def _build_edit_history(
    conn: sqlite3.Connection, txn_id: int
) -> list[dict[str, object]]:
    """Presentation rows for the modal History section (Wave 2 Thing 3).

    Newest first. ``category_id`` old/new values are resolved to category
    NAMES (inactive categories included so a since-deactivated name still
    resolves); an unknown/deleted id falls back to the raw stored id.
    Returns ``[]`` when there is no history so the template omits the
    section entirely.
    """
    edits = transaction_edits_repo.list_for_transaction(conn, txn_id)
    if not edits:
        return []

    category_names = {
        c.id: c.name
        for c in categories_repo.list_all(conn, include_inactive=True)
    }

    def _display(field: str, value: str | None) -> str | None:
        if value is None:
            return None
        if field == "category_id":
            try:
                cid = int(value)
            except (TypeError, ValueError):
                return value
            return category_names.get(cid, value)
        return value

    return [
        {
            "edited_at": e.edited_at,
            "field_label": _EDIT_FIELD_LABELS.get(e.field, e.field),
            "old_display": _display(e.field, e.old_value),
            "new_display": _display(e.field, e.new_value),
        }
        for e in edits
    ]


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
            "top_categories": top_categories(conn, kind=txn.kind),
            "account_name": account_name,
            "history": _build_edit_history(conn, txn_id),
        },
    )


@router.get("/transactions/{sell_id}/pair-candidates", include_in_schema=False)
def transactions_pair_candidates_partial(
    request: Request,
    sell_id: int,
    window_days: int = Query(default=2, ge=1, le=30),
    conn: sqlite3.Connection = Depends(get_conn),
):
    """Render unpaired bank rows this sell could pair with.

    Read-only. The pick itself is a separate POST so the write path stays
    on confirm_pair → create_transfer (rule-002).
    """
    try:
        data = find_pair_candidates(conn, sell_id=sell_id, window_days=window_days)
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    templates = request.app.state.templates
    return templates.TemplateResponse(
        request,
        "partials/pair_candidates.html",
        {"data": data},
    )


@router.post("/transactions/{sell_id}/pair/{deposit_id}", include_in_schema=False)
def transactions_pair_confirm_partial(
    request: Request,
    sell_id: int,
    deposit_id: int,
    f: TransactionsFilter = Depends(filter_from_query),
    conn: sqlite3.Connection = Depends(get_conn),
):
    """Pair a sell with a hand-picked deposit, then refresh the list.

    Delegates to confirm_pair → create_transfer mode 3, the single write
    path for transfer_id (rule-002). Distinct from the triage confirm
    route only in what it swaps back.
    """
    try:
        confirm_pair(conn, deposit_id=deposit_id, sell_id=sell_id)
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    page = query_transactions(conn, f)
    templates = request.app.state.templates
    response = templates.TemplateResponse(
        request,
        "partials/transactions_list.html",
        {"page": page, "filter": page.filter},
    )
    response.headers["HX-Trigger"] = _hx_trigger_json(
        "closeModal", toast_message="Paired"
    )
    return response


@router.post("/transactions/{txn_id}/edit", include_in_schema=False)
def transactions_edit_partial(
    request: Request,
    txn_id: int,
    set_category: str | None = Form(default=None),
    category_id: str | None = Form(default=None),
    set_user_rate: str | None = Form(default=None),
    user_rate: str | None = Form(default=None),
    set_notes: str | None = Form(default=None),
    notes: str | None = Form(default=None),
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
        set_notes=_parse_form_bool(set_notes),
        notes=_parse_optional_text(notes),
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
        # bulk_select keeps the checkbox cell so the swapped-in card
        # stays aligned with the /transactions subgrid (WP4). This
        # endpoint is only invoked from the /transactions modal.
        {"card": card, "bulk_select": True},
    )
    response.headers["HX-Trigger"] = _hx_trigger_json("closeModal", toast_message="Saved")
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

    # Native-USD rows (USD/USDT/USDC) never consult a rate, so the panel
    # would be pure noise for them — see spec §5.1.
    day_rates = (
        []
        if card.rate_source == "native_usd"
        else rates_for_day(
            conn,
            day=txn.occurred_at.date(),
            winning_source=card.rate_source,
        )
    )

    templates = request.app.state.templates
    return templates.TemplateResponse(
        request,
        "partials/modal_transaction_triage.html",
        {
            "txn": txn,
            "card": card,
            "categories": categories,
            "top_categories": top_categories(conn, kind=txn.kind),
            "account_name": account_name,
            "day_rates": day_rates,
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
    set_notes: str | None = Form(default=None),
    notes: str | None = Form(default=None),
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
        set_notes=_parse_form_bool(set_notes),
        notes=_parse_optional_text(notes),
    )

    try:
        apply_edit(conn, txn_id=txn_id, req=req)
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    response = _render_queue_partial(request, conn)
    response.headers["HX-Trigger"] = _hx_trigger_json(
        "closeModal", "advanceQueue", toast_message="Saved"
    )
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
    response.headers["HX-Trigger"] = _hx_trigger_json(
        "closeModal", toast_message="Pair confirmed"
    )
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


# ---------------------------------------------------------------------------
# Saved filter views (Wave 2 Thing 2 / docs/plans/wave2/02-saved-views.md).
#
# The chip row + inline save form live in partials/saved_views.html and
# swap as one unit (#saved-views, outerHTML). Create/delete re-render the
# row and carry a success toast in HX-Trigger (WP2 contract). Error paths
# (duplicate/blank name, missing id) raise HTTPException so the global
# htmx:response-error listener in base.html surfaces the JSON ``detail``
# as the error toast — same split as the transaction-edit endpoints.
# ---------------------------------------------------------------------------


def _render_saved_views(request: Request, conn: sqlite3.Connection):
    templates = request.app.state.templates
    return templates.TemplateResponse(
        request,
        "partials/saved_views.html",
        {"views": saved_views_repo.list_all(conn)},
    )


@router.get("/views", include_in_schema=False)
def saved_views_partial(
    request: Request,
    conn: sqlite3.Connection = Depends(get_conn),
):
    """Return the saved-views chip row + save form fragment."""
    return _render_saved_views(request, conn)


@router.post("/views", include_in_schema=False)
def saved_views_create_partial(
    request: Request,
    name: str = Form(default=""),
    query_string: str = Form(default=""),
    conn: sqlite3.Connection = Depends(get_conn),
):
    """Save the CURRENT querystring under ``name``; re-render the chip row.

    ``query_string`` arrives from ``window.location.search`` so a leading
    ``?`` is stripped before storage (the DB keeps the bare querystring).
    """
    clean_name = name.strip()
    if not clean_name:
        raise HTTPException(status_code=422, detail="View name must not be empty")
    qs = query_string.strip().removeprefix("?")

    try:
        saved_views_repo.insert(
            conn, SavedView(name=clean_name, query_string=qs)
        )
    except sqlite3.IntegrityError as exc:
        raise HTTPException(
            status_code=422,
            detail=f'A view named "{clean_name}" already exists',
        ) from exc

    response = _render_saved_views(request, conn)
    response.headers["HX-Trigger"] = _hx_trigger_json(
        toast_message=f'View "{clean_name}" saved'
    )
    return response


@router.post("/views/{view_id}/delete", include_in_schema=False)
def saved_views_delete_partial(
    request: Request,
    view_id: int,
    conn: sqlite3.Connection = Depends(get_conn),
):
    """Delete the saved view and re-render the chip row."""
    if not saved_views_repo.delete(conn, view_id):
        raise HTTPException(
            status_code=404, detail=f"saved view id={view_id} not found"
        )
    response = _render_saved_views(request, conn)
    response.headers["HX-Trigger"] = _hx_trigger_json(toast_message="View deleted")
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
