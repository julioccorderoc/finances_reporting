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
from collections.abc import Sequence
from datetime import date, datetime, time
from decimal import Decimal, InvalidOperation
from pathlib import Path

from fastapi import (
    APIRouter,
    Depends,
    File,
    Form,
    HTTPException,
    Query,
    Request,
    Response,
    UploadFile,
)

from finances.db.repos import accounts as accounts_repo
from finances.db.repos import categories as categories_repo
from finances.db.repos import saved_views as saved_views_repo
from finances.db.repos import transaction_edits as transaction_edits_repo
from finances.db.repos import transactions as transactions_repo
from finances.domain import triage_admin
from finances.domain.models import SavedView
from finances.format import fmt_date, fmt_number
from finances.web.deps import dismissed_pairs as _dismissed_pairs, get_conn
from finances.web.routers._monthly_filter_dep import monthly_filter_from_query
from finances.web.routers._tx_filter_dep import filter_from_query
from finances.web.services.category_stats import top_categories
from finances.web.services.dashboard import build_sync_status
from finances.web.services import uploads as uploads_svc
from finances.web.services.categories_view import picker_payload
from finances.web.services.pairing import find_pair_candidates
from finances.web.services.transactions_query import _project_card
from finances.web.services.transactions_write import (
    TransactionEditRequest,
    apply_edit,
)
from finances.web.services.triage_view import build_screen
from finances.web.services.triage import (
    PairProposal,
    TriageAccount,
    TriageItem,
    TriageNeeds,
    TriageType,
    _fetch_txn_with_labels,
    _project_from_row,
    assess_pair,
    build_queue,
    confirm_pair,
    neighbours_of,
    next_item_after,
)
from finances.web.urls import modal_url_for
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
# Provincial statement upload (WP3).
#
# Two steps on purpose. Re-dropping a statement is already harmless —
# UNIQUE(source, source_ref) absorbs it — so the preview exists to catch the
# WRONG file, which dedup cannot.
# ---------------------------------------------------------------------------


def _staging_dir() -> Path:
    """Where unconfirmed uploads wait.

    Read through the module rather than imported by value so tests can point
    it somewhere temporary. It is a directory inside ``inputs/``, which is
    what keeps ``_discover_provincial_files`` from sweeping an unconfirmed
    file into the next ``finances update``.
    """
    from finances import config as _config

    return _config.INPUTS_DIR / uploads_svc.STAGING_DIR_NAME


def _inputs_dir() -> Path:
    from finances import config as _config

    return _config.INPUTS_DIR


@router.post("/uploads/provincial/preview", include_in_schema=False)
def provincial_upload_preview(
    request: Request,
    file: UploadFile = File(...),
    conn: sqlite3.Connection = Depends(get_conn),
):
    """Stage a dropped statement and render what importing it would do.

    Sync, not async, on purpose: ``get_conn`` is a sync dependency, so
    FastAPI resolves it in the threadpool. An ``async def`` endpoint would
    then run on the event-loop thread and every query would fail with
    "SQLite objects created in a thread can only be used in that same
    thread". Statements are ~14 KB, so the blocking read costs nothing.
    """
    templates = request.app.state.templates
    data = file.file.read()
    try:
        staged = uploads_svc.stage_upload(
            data, filename=file.filename or "", staging_dir=_staging_dir()
        )
    except uploads_svc.UploadRejected as exc:
        # 200, not 4xx: this renders into the dropzone as a message the owner
        # reads and acts on, and htmx does not swap error responses.
        return templates.TemplateResponse(
            request,
            "partials/upload_preview.html",
            {"preview": None, "error": str(exc), "result": None},
        )

    preview = uploads_svc.preview_upload(
        conn, staged.token, staging_dir=_staging_dir()
    )
    return templates.TemplateResponse(
        request,
        "partials/upload_preview.html",
        {"preview": preview, "error": preview.error, "result": None},
    )


@router.post("/uploads/provincial/import", include_in_schema=False)
def provincial_upload_import(
    request: Request,
    token: str = Form(...),
    conn: sqlite3.Connection = Depends(get_conn),
):
    """Ingest a previously previewed statement, then archive it."""
    templates = request.app.state.templates
    try:
        result = uploads_svc.commit_upload(
            conn,
            token,
            staging_dir=_staging_dir(),
            inputs_dir=_inputs_dir(),
        )
    except uploads_svc.UploadRejected as exc:
        return templates.TemplateResponse(
            request,
            "partials/upload_preview.html",
            {"preview": None, "error": str(exc), "result": None},
        )

    response = templates.TemplateResponse(
        request,
        "partials/upload_preview.html",
        {"preview": None, "error": None, "result": result},
    )
    response.headers["HX-Trigger"] = _hx_trigger_json(
        toast_message=(
            f"{result.filename}: {result.rows_inserted} new, "
            f"{result.rows_updated} updated"
        )
    )
    return response


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


def _hx_trigger_json(
    *events: str,
    toast_message: str,
    queue_filter: str | None = None,
    resolved: int = 0,
) -> str:
    """Build an ``HX-Trigger`` header value: named events + a success toast.

    htmx accepts a JSON object in ``HX-Trigger``: each key is dispatched
    as an event, its value as the event detail. The base.html <body>
    listener re-dispatches ``closeModal`` as the ``close-modal`` window
    event and ``toast`` as ``show-toast`` (WP2 toast contract; error
    toasts come from the global htmx:responseError listener instead).

    ``queueDirty`` carries the active type filter as its detail rather
    than a bare ``true``. The deferred queue refresh happens long after
    the request that set the filter, and the filter chips render outside
    the swapped region — their ``data-active`` is only ever written by a
    full page load, so reading the filter back off the DOM would refresh
    an unfiltered queue over a filtered one.
    """
    payload: dict[str, object] = {name: True for name in events}
    if "queueDirty" in payload:
        payload["queueDirty"] = {"typeFilter": queue_filter}
    if resolved:
        # The sitting counter is client state — the server knows how many
        # rows a write resolved, never when the sitting began.
        payload["triageResolved"] = {"count": resolved}
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

    # Only the kinds this row can legitimately take: its own, plus the
    # transfer categories that mark money movement. Offering the full list
    # is how 65 rows ended up with a category from another kind.
    categories = categories_repo.list_for_kind(conn, txn.kind)

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
    """Kept for the advance path's internal signature only.

    The redesigned queue has no filter chips: rows are grouped by what is
    wrong with them, and the run walks every group. The parameter survives
    on the write endpoints because ``next_item_after`` needs both queues
    built the same way, and an unfiltered queue is still a queue.
    """
    if value in (None, "", "all"):
        return None
    try:
        return TriageType(value)
    except ValueError:
        return None


def _render_queue_partial(request: Request, conn: sqlite3.Connection):
    """Render the queue screen's body — the target of every swap here.

    The page header, the counts, the integrity banner, the three groups,
    the parked strip and the empty state are all inside it. A count
    rendered in the page shell is written once, by the full page load, and
    is stale from the first save onward.
    """
    templates = request.app.state.templates
    return templates.TemplateResponse(
        request,
        "partials/triage_queue.html",
        {"screen": build_screen(conn, dismissed=_dismissed_pairs(request))},
    )


@router.get("/triage/queue", include_in_schema=False)
def triage_queue_partial(
    request: Request,
    conn: sqlite3.Connection = Depends(get_conn),
):
    """The queue, re-read. Called on modal close, and after a list write."""
    return _render_queue_partial(request, conn)


@router.get("/triage/parked", include_in_schema=False)
def triage_parked_sheet_partial(
    request: Request,
    conn: sqlite3.Connection = Depends(get_conn),
):
    """The parked sheet, built when it opens rather than at page load.

    Its count, its sample and the oldest row it names all move as the
    sitting goes on; a sheet baked into the page shell would show the
    numbers from whenever the page was opened.
    """
    templates = request.app.state.templates
    return templates.TemplateResponse(
        request,
        "partials/triage_sheet_parked.html",
        {"screen": build_screen(conn)},
    )


@router.get("/triage/bulk-sheet", include_in_schema=False)
def triage_bulk_sheet_partial(
    request: Request,
    conn: sqlite3.Connection = Depends(get_conn),
):
    """The bulk sort sheet: the picker at four columns, number keys off.

    Which rows it will touch is decided in the browser, from the
    selection and each row's ``data-needs-category`` — the selection only
    ever lives there (G4).
    """
    templates = request.app.state.templates
    return templates.TemplateResponse(
        request,
        "partials/triage_sheet_bulk.html",
        {"picker": picker_payload(conn)},
    )


def _queue_items(request: Request, conn: sqlite3.Connection) -> list[TriageItem]:
    return build_queue(conn, dismissed=_dismissed_pairs(request)).items


def _synthetic_pair_item(
    conn: sqlite3.Connection, deposit_id: int, sell_id: int
) -> TriageItem:
    """A pair item for two rows the automatic matcher never proposed.

    The matcher's window is ±1 day and ±2% (ADR-002), so nothing it offers
    is ever refusable. The manual path is wider and this is where its
    verdict is rendered — including the refusal, which is the same
    ``assess_pair`` the write raises on, so the button can be greyed out
    and the reason stated before the click rather than after it (H3).
    """
    deposit = transactions_repo.get_by_id(conn, deposit_id)
    sell = transactions_repo.get_by_id(conn, sell_id)
    if deposit is None or sell is None:
        missing = deposit_id if deposit is None else sell_id
        raise HTTPException(
            status_code=404, detail=f"transaction id={missing} not found"
        )

    deposit_card = _project_from_row(conn, _fetch_txn_with_labels(conn, deposit_id))
    sell_card = _project_from_row(conn, _fetch_txn_with_labels(conn, sell_id))
    verdict = assess_pair(deposit, sell)

    account_row = conn.execute(
        "SELECT name, kind, currency, institution FROM accounts WHERE id = ?",
        (deposit.account_id,),
    ).fetchone()

    return TriageItem(
        item_id=f"pair:{deposit_id}:{sell_id}",
        type=TriageType.PAIR,
        sort_key=deposit_card.occurred_at,
        bucket=1,
        needs=TriageNeeds(pair=True),
        account=(
            None
            if account_row is None
            else TriageAccount(
                name=account_row["name"],
                detail=account_row["institution"],
                kind=account_row["kind"],
                currency=account_row["currency"],
            )
        ),
        pair_proposal=PairProposal(
            proposal_id=f"{deposit_id}:{sell_id}",
            deposit=deposit_card,
            sell=sell_card,
            confidence=1.0,
            days_apart=verdict.days_apart,
            drift_pct=verdict.drift_pct,
            implied_rate=verdict.implied_rate,
            refused=verdict.refused,
            refuse_reason=verdict.refuse_reason,
            details={
                "bank_transaction_id": deposit_id,
                "binance_transaction_id": sell_id,
            },
        ),
    )


def _render_modal(
    request: Request,
    conn: sqlite3.Connection,
    item_id: str,
    *,
    queue_items: Sequence[TriageItem] | None = None,
):
    """Render the dialog for ``item_id``.

    One template for all three item types: the left column is always the
    facts and the right column is whatever the row is asking for. The
    position and the arrows come from the item's place in the run, and the
    arrows point straight at their neighbour's own open URL — navigation
    needs no endpoint of its own.

    An item that is not in the run at all is only reachable by URL: a pair
    the automatic matcher never proposed (the manual path) renders as a
    run of one, with both arrows dead. A transaction that is no longer
    queued is a 404 — it has nothing left to ask.
    """
    items = list(_queue_items(request, conn) if queue_items is None else queue_items)
    index = next(
        (n for n, item in enumerate(items) if item.item_id == item_id), None
    )

    if index is not None:
        item = items[index]
        total = len(items)
        # Positional, not a history stack: the arrows are "the row above
        # this one" and "the row below it" in the live run, and both ends
        # answer None, which the template renders as a disabled arrow
        # rather than a missing one (B6).
        prev_item, next_item = neighbours_of(items, item_id)
    elif item_id.startswith("pair:"):
        _, deposit_id, sell_id = item_id.split(":")
        item = _synthetic_pair_item(conn, int(deposit_id), int(sell_id))
        index, total, prev_item, next_item = 0, 1, None, None
    else:
        raise HTTPException(
            status_code=404, detail=f"{item_id} is no longer in the queue"
        )

    if item.txn_card is not None:
        card = item.txn_card
        txn = transactions_repo.get_by_id(conn, card.id)
    else:
        assert item.pair_proposal is not None
        card = item.pair_proposal.deposit
        txn = None

    # Native-USD rows never consult a rate, and a row priced inside a
    # tier's window is not being asked about one, so the panel is built
    # only where it is the question.
    day_rates = (
        rates_for_day(
            conn,
            day=card.occurred_at.date(),
            winning_source=card.rate_source,
            amount_native=card.amount_native,
            currency=card.currency,
        )
        if item.needs.rate and item.txn_card is not None
        else []
    )

    templates = request.app.state.templates
    return templates.TemplateResponse(
        request,
        "partials/triage_modal.html",
        {
            "item": item,
            "card": card,
            "txn": txn,
            "index": index,
            "total": total,
            "prev_url": None if prev_item is None else modal_url_for(prev_item),
            "next_url": None if next_item is None else modal_url_for(next_item),
            # Scoped to the row's own kind: apply_edit refuses a category
            # whose kind contradicts it, so an unscoped picker would put
            # Salary on keyboard shortcut 2 of an expense row.
            "picker": picker_payload(
                conn, kind=txn.kind if txn is not None else None
            ),
            "day_rates": day_rates,
        },
    )


@router.get("/triage/{txn_id}/modal", include_in_schema=False)
def triage_modal_partial(
    request: Request,
    txn_id: int,
    conn: sqlite3.Connection = Depends(get_conn),
):
    """Open the run on one transaction."""
    return _render_modal(request, conn, f"txn:{txn_id}")


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
    """Open the run on a proposed — or manually chosen — pair."""
    return _render_modal(request, conn, f"pair:{deposit_id}:{sell_id}")


def _advance_after_write(
    request: Request,
    conn: sqlite3.Connection,
    *,
    before: list[TriageItem],
    resolved_id: str,
    toast_message: str,
    resolved: int = 1,
):
    """Return the dialog of whichever item took the resolved item's slot.

    The response body IS the next dialog (ADR-012 Amendment 2026-07-26) —
    the list is deliberately absent. Re-rendering every row to replace one
    is what made the queue visibly repaint mid-run, and surgical removal
    is not a legal substitute (resolving one item can invalidate an
    unrelated pair proposal). The list reconciles once, on close, driven
    by ``queueDirty``.

    ``closeModal`` is emitted ONLY when the run is exhausted: the
    base.html handler clears the modal host unconditionally, so a mid-run
    ``closeModal`` would discard the dialog in this very response.
    """
    after = _queue_items(request, conn)
    nxt = next_item_after(before, after, resolved_id)

    if nxt is None:
        response = Response(content="", media_type="text/html")
        response.headers["HX-Trigger"] = _hx_trigger_json(
            "closeModal",
            "queueDirty",
            toast_message=toast_message,
            resolved=resolved,
        )
        return response

    response = _render_modal(request, conn, nxt.item_id, queue_items=after)
    response.headers["HX-Trigger"] = _hx_trigger_json(
        "queueDirty", toast_message=toast_message, resolved=resolved
    )
    return response


def _sorted_toast(
    conn: sqlite3.Connection, req: TransactionEditRequest
) -> str:
    """The specific line, never "Saved" (criterion I10).

    Which of the three shapes it takes is decided by what the write
    actually carried, so a rate saved alongside a category says both.
    """
    label: str | None = None
    if req.set_category and req.category_id is not None:
        category = categories_repo.get_by_id(conn, req.category_id)
        label = category.name if category is not None else None
    rate = req.user_rate if req.set_user_rate else None

    if label is not None:
        message = f"Sorted — {label}."
        if rate is not None:
            message += f" Rate set to {fmt_number(rate)}."
        return message
    if rate is not None:
        return f"Rate set to {fmt_number(rate)}."
    return "Note saved."


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
    """Apply the edit and answer with the NEXT item's dialog.

    Hold-position advance: the queue is snapshotted before the write so
    the successor can be picked by identity from the queue rebuilt after
    it (K12). A partial fix — a rate saved on a row that still lacks a
    category — leaves the row queued and keeps its place.
    """
    req = TransactionEditRequest(
        set_category=_parse_form_bool(set_category),
        category_id=_parse_optional_int(category_id),
        set_user_rate=_parse_form_bool(set_user_rate),
        user_rate=_parse_optional_decimal(user_rate),
        set_notes=_parse_form_bool(set_notes),
        notes=_parse_optional_text(notes),
    )

    before = _queue_items(request, conn)

    try:
        apply_edit(conn, txn_id=txn_id, req=req)
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    return _advance_after_write(
        request,
        conn,
        before=before,
        resolved_id=f"txn:{txn_id}",
        toast_message=_sorted_toast(conn, req),
    )


@router.post("/triage/{txn_id}/accept-guess", include_in_schema=False)
def triage_accept_guess_partial(
    request: Request,
    txn_id: int,
    category_id: int = Form(...),
    conn: sqlite3.Connection = Depends(get_conn),
):
    """Take the queue's own guess, from the list, without a dialog (G6).

    Answers with the refreshed list rather than a dialog: nothing was
    opened, so there is nothing to advance.
    """
    req = TransactionEditRequest(set_category=True, category_id=category_id)
    try:
        apply_edit(conn, txn_id=txn_id, req=req)
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    response = _render_queue_partial(request, conn)
    response.headers["HX-Trigger"] = _hx_trigger_json(
        toast_message=_sorted_toast(conn, req), resolved=1
    )
    return response


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
    """Confirm the pair → :func:`confirm_pair` → the next item's dialog.

    Confirming promotes both legs to ``kind='transfer'``, which can evict
    up to two further rows from the queue — so the successor is picked
    from a queue rebuilt after the write, never assumed.
    """
    before = _queue_items(request, conn)

    try:
        confirm_pair(conn, deposit_id=deposit_id, sell_id=sell_id)
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    return _advance_after_write(
        request,
        conn,
        before=before,
        resolved_id=f"pair:{deposit_id}:{sell_id}",
        toast_message="Paired.",
    )


@router.post(
    "/triage/pair/{deposit_id}/{sell_id}/refuse",
    include_in_schema=False,
)
def triage_pair_refuse_partial(
    request: Request,
    deposit_id: int,
    sell_id: int,
    conn: sqlite3.Connection = Depends(get_conn),
):
    """Say the two rows are not one movement of money, and stop asking.

    Nothing is written: both legs stay exactly as they are, separate rows
    in income and expense (H4). The proposal is dropped for this run — see
    :func:`_dismissed_pairs` for why that is not a database column.
    """
    item_id = f"pair:{deposit_id}:{sell_id}"
    before = _queue_items(request, conn)
    _dismissed_pairs(request).add(item_id)

    return _advance_after_write(
        request,
        conn,
        before=before,
        resolved_id=item_id,
        toast_message="Left unpaired — the legs stay separate rows.",
    )


def _set_parked(
    conn: sqlite3.Connection,
    txn_id: int,
    *,
    parked: bool,
) -> None:
    """Durably defer (or restore) ``txn_id`` via the parked column.

    Per rule-012 the route issues no SQL of its own — the write goes
    through :func:`transactions_repo.update`. Never touches
    ``needs_review``.
    """
    txn = transactions_repo.get_by_id(conn, txn_id)
    if txn is None:
        raise HTTPException(
            status_code=404, detail=f"transaction id={txn_id} not found"
        )

    transactions_repo.update(conn, id=txn_id, parked=parked)


@router.post("/triage/{txn_id}/park", include_in_schema=False)
def triage_park_partial(
    request: Request,
    txn_id: int,
    conn: sqlite3.Connection = Depends(get_conn),
):
    """Defer one row durably, then advance.

    Parking is a decision about a row, same as sorting one, so it hands
    back the next dialog rather than dumping the owner on the list.
    Never touches ``needs_review``.
    """
    before = _queue_items(request, conn)

    _set_parked(conn, txn_id, parked=True)

    return _advance_after_write(
        request,
        conn,
        before=before,
        resolved_id=f"txn:{txn_id}",
        toast_message="Parked. It keeps its money, and stops asking.",
    )


@router.post("/triage/bulk-park", include_in_schema=False)
def triage_bulk_park_partial(
    request: Request,
    ids: str = Form(...),
    conn: sqlite3.Connection = Depends(get_conn),
):
    """Park every selected row in one transaction (F2).

    ``ids`` is a comma-joined list rather than a repeated field: the
    selection is assembled in the browser and one string cannot arrive
    half-parsed.
    """
    wanted = [int(part) for part in ids.split(",") if part.strip()]
    if not wanted:
        raise HTTPException(status_code=422, detail="no rows selected")

    conn.execute("BEGIN")
    try:
        for txn_id in wanted:
            _set_parked(conn, txn_id, parked=True)
    except Exception:
        conn.execute("ROLLBACK")
        raise
    conn.execute("COMMIT")

    response = _render_queue_partial(request, conn)
    count = len(wanted)
    response.headers["HX-Trigger"] = _hx_trigger_json(
        toast_message=(
            "Parked. It keeps its money, and stops asking."
            if count == 1
            else f"{count} rows parked."
        ),
        resolved=count,
    )
    return response


@router.post("/triage/park-before", include_in_schema=False)
def triage_park_before_partial(
    request: Request,
    before: str = Form(...),
    conn: sqlite3.Connection = Depends(get_conn),
):
    """Apply the parked sheet's cutoff (F3).

    Everything uncategorised older than the date is parked in one call,
    through :func:`finances.domain.triage_admin.park_before` — the same
    function the CLI uses, with the same income/expense-only scope.
    """
    try:
        cutoff = date.fromisoformat(before)
    except ValueError as exc:
        raise HTTPException(
            status_code=422, detail=f"not a date: {before!r}"
        ) from exc

    parked = triage_admin.park_before(
        conn, before=datetime.combine(cutoff, time.min)
    )

    response = _render_queue_partial(request, conn)
    response.headers["HX-Trigger"] = _hx_trigger_json(
        "triageCloseSheet",
        toast_message=(
            f"Parking everything uncategorised before {fmt_date(cutoff)}."
        ),
        resolved=parked,
    )
    return response


@router.post("/triage/unpark-all", include_in_schema=False)
def triage_unpark_all_partial(
    request: Request,
    conn: sqlite3.Connection = Depends(get_conn),
):
    """Bring every parked row back, oldest first (F9).

    The queue's own sort is ``(bucket, occurred_at, item_id)``, so
    "oldest first" is what returning them to it means — nothing here
    re-orders anything.
    """
    restored = triage_admin.unpark_all(conn)

    response = _render_queue_partial(request, conn)
    response.headers["HX-Trigger"] = _hx_trigger_json(
        "triageCloseSheet",
        toast_message=f"{restored} rows back in the queue — oldest first.",
    )
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
