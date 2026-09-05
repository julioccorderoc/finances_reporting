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
from datetime import UTC, date, datetime, time
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Literal
from urllib.parse import parse_qs, urlencode, urlsplit

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

from pydantic import ValidationError

from finances.db.repos import accounts as accounts_repo
from finances.db.repos import categories as categories_repo
from finances.db.repos import transaction_edits as transaction_edits_repo
from finances.db.repos import transactions as transactions_repo
from finances.domain import cash_conversion, transfers, triage_admin
from finances.domain.models import Transaction, TransactionKind
from finances.config import CARACAS_TZ
from finances.format import fmt_date, fmt_number
from finances.web.deps import dismissed_pairs as _dismissed_pairs, get_conn
from finances.web.routers._monthly_filter_dep import monthly_filter_from_query
from finances.web.routers._tx_filter_dep import filter_from_query
from finances.web.services.category_stats import top_categories
from finances.web.services.dashboard import build_kpis, build_sync_status
from finances.web.services import uploads as uploads_svc
from finances.web.services.categories_view import picker_payload
from finances.web.services.accounts_view import build_account_cards
from finances.web.services.pairing import find_pair_candidates
from finances.web.services import cash_conversion_view
from finances.web.services import reconcile_view
from finances.web.services.transactions_query import _project_card
from finances.web.services.transactions_write import (
    TransactionEditRequest,
    apply_edit,
    delete_transaction,
    describe_tombstone,
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
from finances.web.services.transaction_add import (
    NewTransactionRequest,
    add_transaction,
    entry_accounts,
)
from finances.web.services.transactions_query import (
    TransactionsFilter,
    count_matching,
    query_transactions,
    row_matches_filter,
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

    An htmx request also gets the page header out of band: the swap
    replaces ``#tx-list`` alone, and the Doto figure above it went stale
    on every filter change. The full page renders the header itself, so
    the twin exists only when htmx asked (see the monthly pivot, which
    has the same shape for the same reason).
    """
    page = query_transactions(conn, f)
    templates = request.app.state.templates
    response = templates.TemplateResponse(
        request,
        "partials/transactions_list.html",
        {"page": page, "filter": page.filter, "header_oob": _is_htmx(request)},
    )
    _push_page_url(request, response, "/transactions")
    return response


def _is_htmx(request: Request) -> bool:
    return request.headers.get("HX-Request") == "true"


def _push_page_url(request: Request, response, page_path: str) -> None:
    """Point the address bar at the PAGE, with this request's query.

    The filter forms, sort chips and pagers say ``hx-push-url="true"``,
    which makes htmx push the request url — the partial's own path — so
    a reload landed on a bare fragment with no shell (2026-09-03). The
    ``HX-Push-Url`` header overrides the attribute; the attribute stays
    for anything that reads it.
    """
    if not _is_htmx(request):
        return
    query = request.url.query
    response.headers["HX-Push-Url"] = f"{page_path}?{query}" if query else page_path


@router.get("/dashboard/kpis", include_in_schema=False)
def dashboard_kpis_partial(
    request: Request,
    conn: sqlite3.Connection = Depends(get_conn),
):
    """Return ONLY the tiles + the plug line — no base.html shell.

    Fetched on ``kpisDirty``, which /accounts fires after writing a
    reconciliation adjustment (ADR-018). That write moves net worth and the
    unexplained total at once, and Today may be open in another tab.
    """
    kpis = build_kpis(conn, today=datetime.now(tz=UTC).date())
    templates = request.app.state.templates
    return templates.TemplateResponse(request, "partials/kpi_tiles.html", {"kpis": kpis})


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
    """Return only the pivot fragment (filter / range / kind change swap).

    An htmx request also gets the page header and the chart, out of band:
    the filter form targets ``#monthly-pivot`` alone, and the total, the
    meta line and the chart above it went stale after every range change.
    The full page includes both itself, so the twins are only rendered
    when htmx asked — two elements with one id is the bug the rail badge
    guard already names.
    """
    pivot = build_pivot(conn, f)
    templates = request.app.state.templates
    response = templates.TemplateResponse(
        request,
        "partials/monthly_pivot.html",
        {
            "pivot": pivot,
            "filter": f,
            "chart": build_chart(conn, f),
            "chart_oob": _is_htmx(request),
            "header_oob": _is_htmx(request),
        },
    )
    _push_page_url(request, response, "/monthly")
    return response


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
    deleted_id: int | None = None,
    toast_href: str | None = None,
    toast_href_label: str | None = None,
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
    if "listDirty" in payload:
        # The id of the row that went, so a surface with no refreshable
        # list (the dashboard's recent activity) can still drop the card
        # it is showing rather than display a row that no longer exists.
        payload["listDirty"] = {"id": deleted_id}
    if resolved:
        # The sitting counter is client state — the server knows how many
        # rows a write resolved, never when the sitting began.
        payload["triageResolved"] = {"count": resolved}
    toast: dict[str, object] = {"level": "success", "message": toast_message}
    if toast_href:
        # A toast that reports something the surface cannot show has to
        # offer the way to it, or the report is a dead end (the add-a-row
        # case: written, but outside the current filter).
        toast["href"] = toast_href
        toast["hrefLabel"] = toast_href_label or "Show it"
    payload["toast"] = toast
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
            # One footer slot, two verbs. Mutually exclusive by construction:
            # can_convert requires an unpaired row, can_unpair a paired one.
            "can_become_cash": cash_conversion_view.can_convert(conn, txn),
            "can_unpair": cash_conversion_view.can_unpair(txn),
        },
    )


@router.get("/transactions/{txn_id}/became-cash", include_in_schema=False)
def transactions_became_cash_panel(
    request: Request,
    txn_id: int,
    conn: sqlite3.Connection = Depends(get_conn),
):
    """The one-field panel: how many dollars came back.

    A GET because it renders, and because the owner must be able to think
    about the number before anything is written. It re-checks
    ``can_convert`` rather than trusting that the button was visible — the
    template is a courtesy, this is the guard.
    """
    txn = transactions_repo.get_by_id(conn, txn_id)
    if txn is None:
        raise HTTPException(status_code=404, detail=f"transaction id={txn_id} not found")
    if not cash_conversion_view.can_convert(conn, txn):
        raise HTTPException(
            status_code=422,
            detail=(
                "this row cannot have become cash: it is money arriving, "
                "already paired, or already cash"
            ),
        )

    templates = request.app.state.templates
    return templates.TemplateResponse(
        request,
        "partials/became_cash_panel.html",
        {
            "txn": txn,
            "suggested_usd": cash_conversion_view.suggested_usd(conn, txn),
            "post_url": f"/_partial/transactions/{txn_id}/became-cash",
            "hx_target": "this",
            "hx_swap": "none",
        },
    )


@router.post("/transactions/{txn_id}/became-cash", include_in_schema=False)
def transactions_became_cash(
    txn_id: int,
    usd_received: str = Form(...),
    conn: sqlite3.Connection = Depends(get_conn),
):
    """Record the conversion: promote the row, insert the Cash USD leg, pair.

    Answers with events rather than markup, exactly as Delete does and for
    the same reason: this modal opens from /transactions AND from the
    dashboard, and htmx will not send a request whose ``hx-target`` matches
    nothing. ``closeModal`` dismisses the dialog, ``listDirty`` asks whatever
    is showing rows to fetch them again, and the toast names the dollars and
    the price they were struck at.
    """
    try:
        amount = Decimal(usd_received)
    except InvalidOperation as exc:
        raise HTTPException(
            status_code=422, detail=f"{usd_received!r} is not an amount"
        ) from exc

    try:
        result = cash_conversion.convert_to_cash(
            conn, transaction_id=txn_id, usd_received=amount
        )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    return Response(
        status_code=200,
        headers={
            "HX-Trigger": _hx_trigger_json(
                "closeModal",
                "listDirty",
                toast_message=cash_conversion_view.describe_conversion(result),
            )
        },
    )


@router.post("/transactions/{txn_id}/unpair", include_in_schema=False)
def transactions_unpair(
    txn_id: int,
    conn: sqlite3.Connection = Depends(get_conn),
):
    """Break this row's transfer back into the two rows it was made from.

    The undo ADR-022 said did not exist: its delete refuses a paired row
    until something can break the pair. Nothing is deleted here — the toast
    says so, because the owner would otherwise assume the row they just took
    back has gone, and a stray Cash USD leg would sit in the ledger
    inflating a balance nobody is looking at.
    """
    txn = transactions_repo.get_by_id(conn, txn_id)
    if txn is None:
        raise HTTPException(status_code=404, detail=f"transaction id={txn_id} not found")
    if txn.transfer_id is None:
        raise HTTPException(status_code=422, detail="this row is not part of a transfer")

    try:
        legs = transfers.unpair(conn, transfer_id=txn.transfer_id)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    names = {
        int(r["id"]): str(r["name"])
        for r in conn.execute("SELECT id, name FROM accounts").fetchall()
    }
    return Response(
        status_code=200,
        headers={
            "HX-Trigger": _hx_trigger_json(
                "closeModal",
                "listDirty",
                toast_message=cash_conversion_view.describe_unpair(legs, names),
            )
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


@router.post("/transactions/{txn_id}/delete", include_in_schema=False)
def transactions_delete_partial(
    txn_id: int,
    reason: str | None = Form(default=None),
    conn: sqlite3.Connection = Depends(get_conn),
):
    """Delete a row for good (ADR-022 §2.4).

    The response swaps nothing. It carries three events instead:
    ``closeModal`` dismisses the dialog the button was in, ``listDirty``
    tells whatever surface is showing rows to fetch them again, and the
    toast names what went — the only place left that can, now that the
    row is gone.

    Why not answer with the refreshed list: this modal opens from
    /transactions AND from the dashboard's recent activity, and htmx
    refuses to even send a request whose ``hx-target`` matches nothing.
    A list-shaped response would make Delete a dead button on the
    dashboard — silently, since no request is sent to fail. The refresh
    is the same deferred pattern ``queueDirty`` already uses for triage,
    and it reads the active filter off the URL.

    A refusal (a paired row, a reconciliation row) comes back as 422 with
    the repo's own words, which base.html's htmx error listener shows as
    an error toast.
    """
    try:
        tomb = delete_transaction(conn, txn_id=txn_id, reason=reason)
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    return Response(
        status_code=200,
        headers={
            "HX-Trigger": _hx_trigger_json(
                "closeModal",
                "listDirty",
                toast_message=describe_tombstone(tomb),
                deleted_id=txn_id,
            )
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
# Adding a transaction by hand (ADR-008 amendment 2026-09-03).
# ---------------------------------------------------------------------------


#: Fields of ``TransactionsFilter`` that take a list of values in the URL.
_FILTER_LIST_FIELDS = frozenset(
    {"accounts", "categories", "kinds", "sources", "currencies"}
)

#: Fields that must reach pydantic as ints. ``page_size`` is
#: ``Literal[25, 50, 100]``, and pydantic will not build a Literal of ints
#: from the string "50" — which the filter form puts in the URL on every
#: request. Left as a string it failed the whole model, and the filter fell
#: back to defaults: a row outside a one-day window came back reported as
#: visible, under the unfiltered count.
_FILTER_INT_FIELDS = frozenset({"page", "page_size"})

_NEW_TXN_FIELD_LABELS = {
    "account_id": "Account",
    "occurred_at": "Date",
    "kind": "Direction",
    "amount": "Amount",
    "description": "Description",
    "category_id": "Category",
    "notes": "Note",
}


def _filter_from_hx_current_url(request: Request) -> TransactionsFilter:
    """Rebuild the list's filter from the page the request came from.

    A write posted from a dialog has no query string of its own, and the
    URL is what /transactions treats as the filter's source of truth (the
    same reasoning base.html's ``refreshList`` follows). htmx sends
    ``HX-Current-URL`` on every request, so the server can answer "would
    your list show this row?" without the dialog having to carry the
    filter in its own form.

    Deliberately forgiving: this decides a courtesy (push the card, or
    toast a link), never a write. Junk in the URL falls back to defaults
    rather than failing an entry the owner already made.
    """
    raw = request.headers.get("HX-Current-URL")
    if not raw:
        return TransactionsFilter()
    params = parse_qs(urlsplit(raw).query)
    data: dict[str, object] = {}
    for key, values in params.items():
        if key not in TransactionsFilter.model_fields or not values:
            continue
        if key in _FILTER_LIST_FIELDS:
            data[key] = values
            continue
        raw = values[-1]
        if key in _FILTER_INT_FIELDS:
            try:
                data[key] = int(raw)
            except ValueError:
                continue
        else:
            data[key] = raw
    try:
        return TransactionsFilter(**data)
    except ValidationError:
        return TransactionsFilter()


#: The dialog's CatPicker columns, and its lack of 1-8 shortcuts. The keys
#: are False here and only here: this dialog has an amount field, and a
#: digit typed into it must stay a digit rather than pick a category.
_NEW_TXN_PICKER_COLUMNS = 2
_NEW_TXN_PICKER_KEYS = False


@router.get("/transactions/new", include_in_schema=False)
def transaction_new_partial(
    request: Request,
    conn: sqlite3.Connection = Depends(get_conn),
):
    """Render the Add-transaction dialog.

    Every ACTIVE account is offered; all but ``Cash USD`` come back
    disabled, each naming what feeds it instead — see
    :mod:`finances.web.services.transaction_add` for why that is the shape
    rather than a one-account dropdown.
    """
    accounts = entry_accounts(conn)
    cash = next(a for a in accounts if a.writable)
    today = datetime.now(tz=CARACAS_TZ).date()

    templates = request.app.state.templates
    return templates.TemplateResponse(
        request,
        "partials/modal_transaction_new.html",
        {
            "accounts": accounts,
            "cash_id": cash.id,
            "cash_currency": cash.currency,
            "today": today,
            "picker": picker_payload(
                conn, kind=TransactionKind.EXPENSE, today=today
            ),
            "picker_columns": _NEW_TXN_PICKER_COLUMNS,
            "picker_keys": _NEW_TXN_PICKER_KEYS,
        },
    )


@router.get("/transactions/new/categories", include_in_schema=False)
def transaction_new_categories_partial(
    request: Request,
    kind: Literal["expense", "income"] = Query(default="expense"),
    conn: sqlite3.Connection = Depends(get_conn),
):
    """Re-scope the dialog's category picker when the direction changes."""
    today = datetime.now(tz=CARACAS_TZ).date()
    templates = request.app.state.templates
    return templates.TemplateResponse(
        request,
        "partials/new_transaction_picker.html",
        {
            "picker": picker_payload(conn, kind=TransactionKind(kind), today=today),
            "picker_columns": _NEW_TXN_PICKER_COLUMNS,
            "picker_keys": _NEW_TXN_PICKER_KEYS,
        },
    )


@router.post("/transactions", include_in_schema=False)
def transaction_add_partial(
    request: Request,
    account_id: str = Form(...),
    occurred_at: str = Form(...),
    kind: str = Form(...),
    amount: str = Form(...),
    description: str = Form(...),
    category_id: str | None = Form(default=None),
    notes: str | None = Form(default=None),
    conn: sqlite3.Connection = Depends(get_conn),
):
    """Write one hand-entered row and answer with what the list should show.

    Two shapes of answer, and the difference is honesty about the filter:

    * the current filter shows the row, and the list is on page 1 → the
      card, swapped into the top of the list, with the match count fixed
      out-of-band;
    * it does not → **no card**, and a toast carrying a link to a view
      that does show it. Prepending a row a filter excludes is how a
      filtered list quietly stops being a filtered list.

    A closed account comes back 422 in the dialog's own words. The
    disabled ``<option>`` is a courtesy; this is the guard (rule-008).
    """
    try:
        req = NewTransactionRequest(
            # Straight to pydantic, not through ``_parse_optional_int``:
            # that helper's refusal is worded for ``category_id``, and it
            # is the only field it is ever used for elsewhere.
            account_id=account_id,
            occurred_at=occurred_at,
            kind=kind,
            amount=amount,
            description=description,
            category_id=_parse_optional_int(category_id),
            notes=notes,
        )
    except ValidationError as exc:
        raise HTTPException(
            status_code=422, detail=_describe_new_txn_errors(exc)
        ) from exc

    try:
        card = add_transaction(conn, req)
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    # The header partial renders its date window off this object, so it
    # has to be the SAME filter the list was built from. Since 2026-09-05
    # that filter invents nothing: no dates in the URL means no window,
    # and the header then draws none.
    f = _filter_from_hx_current_url(request)
    visible = f.page == 1 and row_matches_filter(conn, card.id, f)
    total = count_matching(conn, f)

    if visible:
        toast_message = f"Added “{card.description}”"
        href: str | None = None
    else:
        toast_message = (
            f"Added “{card.description}”. Your current filter does not show it."
        )
        # Where it IS: its account, on its day. Narrow on purpose — a link
        # that lands on a list the row is once again missing from would be
        # worse than no link.
        href = "/transactions?" + urlencode(
            {
                "accounts": card.account_name,
                "date_from": card.occurred_at.date().isoformat(),
                "date_to": card.occurred_at.date().isoformat(),
            }
        )

    templates = request.app.state.templates
    response = templates.TemplateResponse(
        request,
        "partials/transaction_added.html",
        {
            # No card on the hidden path: the response is then nothing but
            # the out-of-band corrections and the dialog closing.
            "card": card if visible else None,
            "total": total,
            "filter": f,
            # The new row is the only match, so what stood there was the
            # empty state. Emitting the OOB delete unconditionally would
            # make htmx log a missing-target error on every other add.
            "list_was_empty": visible and total == 1,
        },
    )
    # No ``closeModal`` here — the dialog closes by an out-of-band swap in
    # the template, which explains why at length. This header carries the
    # toast only.
    response.headers["HX-Trigger"] = _hx_trigger_json(
        toast_message=toast_message,
        toast_href=href,
        toast_href_label="Show it" if href else None,
    )
    return response


def _describe_new_txn_errors(exc: ValidationError) -> str:
    """Pydantic's errors, in the dialog's vocabulary rather than pydantic's."""
    parts: list[str] = []
    for error in exc.errors():
        field = ".".join(str(p) for p in error["loc"]) if error["loc"] else "input"
        parts.append(f"{_NEW_TXN_FIELD_LABELS.get(field, field)}: {error['msg']}")
    return "; ".join(parts) or "That entry could not be read."


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


def _advance_url(item: TriageItem) -> str:
    """A neighbour's own open URL, flagged as a step inside the run.

    The arrows need no endpoint of their own (B6) — they point straight
    at the neighbour's modal route. ``advance=1`` is the only thing that
    distinguishes walking the run from opening it, and it is what tells
    the dialog to skip the entrance animation.
    """
    return f"{modal_url_for(item)}?advance=1"


def _render_modal(
    request: Request,
    conn: sqlite3.Connection,
    item_id: str,
    *,
    queue_items: Sequence[TriageItem] | None = None,
    advancing: bool = False,
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
            # ``advance=1``: an arrow moves INSIDE a run that is already
            # on screen, so the dialog it fetches must not re-play the
            # entrance. The queue's own open URLs stay unflagged.
            "prev_url": None if prev_item is None else _advance_url(prev_item),
            "next_url": None if next_item is None else _advance_url(next_item),
            # Scoped to the row's own kind: apply_edit refuses a category
            # whose kind contradicts it, so an unscoped picker would put
            # Salary on keyboard shortcut 2 of an expense row.
            "picker": picker_payload(
                conn, kind=txn.kind if txn is not None else None
            ),
            "day_rates": day_rates,
            # Only a real row can have become cash: a pair item's subject
            # is a proposal, not a single row with an other side to name.
            "can_become_cash": (
                txn is not None and cash_conversion_view.can_convert(conn, txn)
            ),
            # Whether this dialog is replacing one that is already open.
            # The entrance animation belongs to opening the run, not to
            # walking it: re-playing it on every advance drops the scrim
            # to transparent for a frame and the page blinks through.
            "advancing": advancing,
        },
    )


@router.get("/triage/{txn_id}/modal", include_in_schema=False)
def triage_modal_partial(
    request: Request,
    txn_id: int,
    advance: bool = Query(default=False),
    conn: sqlite3.Connection = Depends(get_conn),
):
    """Open the run on one transaction, or step to it from inside one."""
    return _render_modal(request, conn, f"txn:{txn_id}", advancing=advance)


@router.get(
    "/triage/pair/{deposit_id}/{sell_id}/modal",
    include_in_schema=False,
)
def triage_pair_modal_partial(
    request: Request,
    deposit_id: int,
    sell_id: int,
    advance: bool = Query(default=False),
    conn: sqlite3.Connection = Depends(get_conn),
):
    """Open the run on a proposed — or manually chosen — pair."""
    return _render_modal(
        request, conn, f"pair:{deposit_id}:{sell_id}", advancing=advance
    )


def _convertible_or_refuse(
    conn: sqlite3.Connection, txn_id: int
) -> Transaction:
    """The row, or the reason it cannot have become cash.

    Both triage routes check before doing anything: the hidden button is a
    courtesy, and a crafted POST never sees a template. Same predicate the
    Flow routes use, so the two surfaces cannot disagree about what is
    convertible.
    """
    txn = transactions_repo.get_by_id(conn, txn_id)
    if txn is None:
        raise HTTPException(
            status_code=404, detail=f"transaction id={txn_id} not found"
        )
    if not cash_conversion_view.can_convert(conn, txn):
        raise HTTPException(
            status_code=422,
            detail=(
                "this row cannot have become cash: it is money arriving, "
                "already paired, or already cash"
            ),
        )
    return txn


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

    response = _render_modal(
        request, conn, nxt.item_id, queue_items=after, advancing=True
    )
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


@router.get("/triage/{txn_id}/became-cash", include_in_schema=False)
def triage_became_cash_panel(
    request: Request,
    txn_id: int,
    conn: sqlite3.Connection = Depends(get_conn),
):
    """The same one-field panel Flow reveals, wired to the run.

    Only the panel's post target differs from
    :func:`transactions_became_cash_panel` — the question it asks is the
    same one, so it is the same template and the same guard.
    """
    txn = _convertible_or_refuse(conn, txn_id)

    templates = request.app.state.templates
    return templates.TemplateResponse(
        request,
        "partials/became_cash_panel.html",
        {
            "txn": txn,
            "suggested_usd": cash_conversion_view.suggested_usd(conn, txn),
            "post_url": f"/_partial/triage/{txn_id}/became-cash",
            "hx_target": "#triage-modal-host",
            "hx_swap": "innerHTML",
        },
    )


@router.post("/triage/{txn_id}/became-cash", include_in_schema=False)
def triage_became_cash(
    request: Request,
    txn_id: int,
    usd_received: str = Form(...),
    conn: sqlite3.Connection = Depends(get_conn),
):
    """Record the conversion, then advance — this is a triage decision.

    The write is the Flow route's write, unchanged. The answer is not:
    saying "this became cash" resolves the row the same way sorting it
    does, so the run must hand back the next dialog rather than closing
    on the owner mid-sitting.
    """
    _convertible_or_refuse(conn, txn_id)
    before = _queue_items(request, conn)

    try:
        amount = Decimal(usd_received)
    except InvalidOperation as exc:
        raise HTTPException(
            status_code=422, detail=f"{usd_received!r} is not an amount"
        ) from exc

    try:
        result = cash_conversion.convert_to_cash(
            conn, transaction_id=txn_id, usd_received=amount
        )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    return _advance_after_write(
        request,
        conn,
        before=before,
        resolved_id=f"txn:{txn_id}",
        toast_message=cash_conversion_view.describe_conversion(result),
    )


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


# ---------------------------------------------------------------------------
# Set balance — ADR-018's viewer surface (amendment, 2026-09-03).
# ---------------------------------------------------------------------------


def _parse_actual(raw: str) -> Decimal:
    try:
        return Decimal(raw.strip())
    except (InvalidOperation, AttributeError) as exc:
        raise HTTPException(
            status_code=422, detail=f"{raw!r} is not a number"
        ) from exc


@router.post("/accounts/{account_id}/reconcile/preview", include_in_schema=False)
def account_reconcile_preview_partial(
    request: Request,
    account_id: int,
    actual: str = Form(...),
    conn: sqlite3.Connection = Depends(get_conn),
):
    """The gap, and what could explain it before the owner plugs it.

    Read-only. It exists because a plug absorbs whatever is wrong upstream
    without distinguishing missing history from a bug — the failure ADR-020
    §1.2 records and the ten Binance Pay twins repeated. The panel argues
    against itself first and only then offers a button.
    """
    amount = _parse_actual(actual)
    try:
        preview = reconcile_view.build_preview(
            conn,
            account_id=account_id,
            actual=amount,
            today=datetime.now(tz=CARACAS_TZ).date(),
        )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    templates = request.app.state.templates
    return templates.TemplateResponse(
        request, "partials/reconcile_panel.html", {"preview": preview}
    )


@router.post("/accounts/{account_id}/reconcile", include_in_schema=False)
def account_reconcile_partial(
    request: Request,
    account_id: int,
    actual: str = Form(...),
    note: str = Form(default=""),
    conn: sqlite3.Connection = Depends(get_conn),
):
    """Write the adjustment and answer with the re-rendered card.

    Dated *now*, in Caracas — never the ledger's start, which is ADR-020's
    opening-position claim and has its own CLI. A blank note is a 422: the
    CLI states its reason in a shell history, a click states it nowhere,
    and ``finances doctor`` lists every plug forever after.

    ``kpisDirty`` rides along so a Today page open in another tab refetches
    its tiles — the plug changes net worth and the unexplained-total line.
    """
    amount = _parse_actual(actual)
    try:
        result = reconcile_view.write_adjustment(
            conn,
            account_id=account_id,
            actual=amount,
            note=note,
            now=datetime.now(tz=CARACAS_TZ),
        )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    # Rebuilt AFTER the write, so both the card and the page answer show the
    # reconciled position.
    cards = build_account_cards(conn, today=date.today())
    card = next((c for c in cards if c.id == account_id), None)
    if card is None:  # pragma: no cover - write_adjustment already looked it up
        raise HTTPException(status_code=404, detail=f"account id={account_id} not found")

    if result is None:
        message = f"{card.name} {card.reconcile_currency}: already matches — nothing written"
    else:
        message = (
            f"{card.name} {result.currency}: adjustment "
            f"{result.delta:+f} written, dated today"
        )

    templates = request.app.state.templates
    response = templates.TemplateResponse(
        request,
        "partials/account_slot_swap.html",
        {"card": card, "cards": cards, "header_oob": True},
    )
    response.headers["HX-Trigger"] = _hx_trigger_json(
        "kpisDirty", toast_message=message
    )
    return response
