"""Flow (/transactions) — the SIGNAL reskin of the ledger list (2026-09).

A reskin, not a rethink: the same data, the same endpoints, the same
structure, restyled on signal.css's tokens through ``flow.css``. What these
tests pin is the part of that a server-side suite CAN see:

* the page opens with ``page_header("What happened?", <match count>)`` and
  that count is the one Doto figure on the page;
* the row partial is ``<article class="flow-row">`` and renders inside a
  bare ``<div class="flow-rows">`` — the dashboard's recent activity
  depends on exactly that contract;
* money goes through ``fmt_usd``/``fmt_native``: a credit reads ``+$``, a
  debit uses the U+2212 minus, bolívares lead with ``Bs.``, and the
  provenance chip replaces the old rate badge;
* the upload panel keeps ``id="upload"`` and opens on ``?upload=1``;
* the edit modal keeps every field name and every ``set_*`` sentinel;
* the filters form keeps its id and its htmx wiring;
* no Tailwind utility survives in the ten Flow templates — the vendored
  tailwind.css has no build step, and a class it lacks renders unstyled
  while every server test stays green.

What only a browser can verify (the modal's focus and keyboard handling,
the bulk bar's Alpine state, drag-and-drop onto the dropzone) is listed in
the reskin report for the Playwright walk, not asserted here.
"""

from __future__ import annotations

import re
import sqlite3
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

from finances.web.app import TEMPLATES_DIR, create_app
from finances.web.services.transactions_query import TransactionCard
from finances.web.settings import WebSettings

STATIC = Path(__file__).resolve().parents[2] / "finances" / "web" / "static"
FLOW_CSS = STATIC / "css" / "flow.css"

#: The nine templates the Flow track owns.
FLOW_TEMPLATES = (
    "pages/transactions.html",
    "partials/transactions_list.html",
    "partials/transactions_filters.html",
    "partials/card_transaction.html",
    "partials/modal_transaction.html",
    "partials/category_picker.html",
    "partials/pair_candidates.html",
    "partials/provincial_dropzone.html",
    "partials/upload_preview.html",
)

#: Tailwind utility shapes that used to carry the old viewer's styling.
#: A token matching any of these in a ``class="..."`` attribute is a
#: regression: flow.css owns every class on this surface now.
_TAILWIND_TOKEN = re.compile(
    r"^(?:"
    r"bg-|text-slate|text-\[|text-xs|text-sm|text-lg|text-2xl|text-right|"
    r"text-center|text-left|rounded|px-|py-|p-\d|mt-|mb-|ml-|mr-|space-y-|"
    r"gap-|grid$|grid-cols|col-span|flex$|flex-|items-|justify-|hidden$|"
    r"sm:|lg:|hover:|border$|border-|font-|tabular-nums|truncate|"
    r"uppercase|tracking-|italic|underline|cursor-|w-full|max-h-|"
    r"overflow-|divide-|whitespace-|break-|self-|inline-|ring-|"
    r"text-sky|text-rose|text-emerald|text-amber"
    r")"
)


def _templates():
    return create_app(WebSettings(host="127.0.0.1")).state.templates


def _card(**overrides) -> TransactionCard:
    base = dict(
        id=42,
        occurred_at=datetime(2026, 7, 1, 12, 0, tzinfo=UTC),
        account_name="Provincial",
        description="COM.PAGO probe",
        # Real expenses are NEGATIVE — do not copy seeded_web_db's signs.
        amount_native=Decimal("-1234.56"),
        currency="VES",
        amount_usd=Decimal("-33.82"),
        rate_source="binance_p2p_median",
        is_bcv_fallback=False,
        kind="expense",
        category_name="Groceries",
        needs_review=False,
        rate=Decimal("36.50"),
    )
    base.update(overrides)
    return TransactionCard(**base)


def _render_card(card: TransactionCard, **context) -> str:
    return _templates().get_template("partials/card_transaction.html").render(
        card=card, **context
    )


def _txn_id(conn: sqlite3.Connection, source_ref: str) -> int:
    row = conn.execute(
        "SELECT id FROM transactions WHERE source_ref = ?", (source_ref,)
    ).fetchone()
    assert row is not None, f"seeded txn {source_ref} not present"
    return int(row["id"])


# ---------------------------------------------------------------------------
# The page header: the question, then the one Doto figure.
# ---------------------------------------------------------------------------


def test_page_opens_with_the_question_and_the_match_count(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    client = web_client_factory()
    params = {"date_from": "2000-01-01"}

    body = client.get("/transactions", params=params).text
    total = client.get("/api/transactions", params=params).json()["total"]

    assert total > 1
    assert '<span class="page-question">What happened?</span>' in body
    assert f'<h1 class="page-answer">{total} rows</h1>' in body
    # The one Doto figure on the page.
    assert body.count('class="page-answer"') == 1
    # The live count inside the swap target is what stays truthful across
    # filter changes; the header is written once per full render.
    assert f"{total} matches" in body
    # The old header and its paragraph are gone.
    assert "Browse, filter, and drill into the ledger" not in body
    assert "<h1 class=\"text-2xl" not in body


def test_answer_is_singular_for_one_row(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    client = web_client_factory()

    body = client.get(
        "/transactions", params={"date_from": "2000-01-01", "q": "Earn payout"}
    ).text

    assert '<h1 class="page-answer">1 row</h1>' in body
    assert "1 match<" in body or "1 match\n" in body


def test_meta_names_the_active_window(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    client = web_client_factory()

    body = client.get(
        "/transactions", params={"date_from": "2026-07-07", "date_to": "2026-08-06"}
    ).text

    header = body[body.index('<header class="page-header">') : body.index("</header>")]
    # fmt_date appends the year only when it is not the current one, so the
    # year is optional here; the weekday form is what is pinned (the UX
    # overhaul's locked "weekday dates" decision binds this surface).
    assert re.search(r"Tue, Jul 7(, 2026)? – Thu, Aug 6(, 2026)?", header), header


# ---------------------------------------------------------------------------
# The row contract: <article class="flow-row"> inside <div class="flow-rows">.
# ---------------------------------------------------------------------------


def test_list_wraps_rows_in_a_selectable_flow_rows_grid(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    client = web_client_factory()

    body = client.get(
        "/_partial/transactions/list", params={"date_from": "2000-01-01"}
    ).text

    wrapper = re.search(r'<div class="flow-rows is-selectable[^"]*"', body)
    assert wrapper, "the list does not wrap its rows in .flow-rows.is-selectable"
    assert body.count('<article') == body.count("data-card-row")
    assert body.count('class="flow-row"') == body.count("data-card-row")
    # The column head is a subgrid row of eyebrows, still carrying the
    # select-all control.
    head = re.search(r'<div class="flow-head">.*?</div>\s*\n', body, re.S)
    assert head, "no .flow-head column-header row"
    assert 'class="teyebrow"' in head.group(0)
    assert "data-bulk-select-all" in head.group(0)
    assert "toggleAll($event.target.checked)" in head.group(0)


def test_row_partial_renders_bare_for_the_dashboard() -> None:
    """Agent A wraps recent activity in exactly ``<div class="flow-rows">``."""
    rendered = _render_card(_card())

    assert rendered.lstrip().startswith("<article")
    assert 'class="flow-row"' in rendered
    assert "data-card-row" in rendered
    assert 'data-tx-id="42"' in rendered
    assert 'data-account="Provincial"' in rendered
    assert 'data-kind="expense"' in rendered
    assert 'data-needs-review="false"' in rendered
    assert 'hx-get="/_partial/transactions/42/modal"' in rendered
    assert 'hx-target="#tx-modal-host"' in rendered
    assert 'hx-trigger="click"' in rendered
    assert 'hx-swap="innerHTML"' in rendered
    # No checkbox cell without bulk_select — the dashboard has no bulk bar.
    assert "data-bulk-checkbox" not in rendered
    # The old card-row system and its Tailwind hover are gone (the
    # data-card-row hook stays: it is what every list test counts).
    assert 'class="card-row' not in rendered
    assert "hover:bg-slate-50" not in rendered


def test_row_partial_adds_the_checkbox_cell_under_bulk_select() -> None:
    rendered = _render_card(_card(), bulk_select=True)

    assert 'data-bulk-checkbox value="42"' in rendered
    assert 'x-model="selected"' in rendered
    assert "@click.stop" in rendered
    # The house checkbox, not the browser's.
    assert re.search(r'<input type="checkbox" class="tcheck" data-bulk-checkbox', rendered)


def test_row_partial_honours_the_triage_modal_url_override() -> None:
    rendered = _render_card(_card(), triage_modal_url="/_partial/triage/42/modal")

    assert 'hx-get="/_partial/triage/42/modal"' in rendered


# ---------------------------------------------------------------------------
# Money: fmt_usd / fmt_native, positive is ink with a plus.
# ---------------------------------------------------------------------------


def test_credit_reads_plus_dollar_in_ink_with_a_ves_native_line() -> None:
    rendered = _render_card(
        _card(
            kind="income",
            amount_native=Decimal("36500.00"),
            amount_usd=Decimal("1000.00"),
            description="ABONO nomina",
            category_name="Salary",
        )
    )

    assert '<span class="tmoney-usd">+$1,000.00</span>' in rendered
    assert "Bs. 36,500.00" in rendered
    # The provenance chip replaces the old rate_source_badge...
    assert 'data-prov="binance_p2p_median"' in rendered
    assert "prov-quiet" in rendered
    # ...and the raw tier stays readable on the article for anything that
    # still keys off it.
    assert 'data-rate-source="binance_p2p_median"' in rendered
    # Nothing green, nothing rose: money is ink with a sign.
    assert "text-emerald" not in rendered
    assert "text-rose" not in rendered


def test_debit_uses_the_typographic_minus() -> None:
    rendered = _render_card(_card())

    assert '<span class="tmoney-usd">−$33.82</span>' in rendered
    assert "−Bs. 1,234.56" in rendered


def test_native_usd_row_renders_no_chip() -> None:
    rendered = _render_card(
        _card(
            account_name="Cash USD",
            currency="USD",
            amount_native=Decimal("-12.50"),
            amount_usd=Decimal("-12.50"),
            rate_source="native_usd",
            rate=Decimal("1"),
        )
    )

    assert '<span class="tmoney-usd">−$12.50</span>' in rendered
    assert "data-prov=" not in rendered


def test_unpriced_row_says_so_and_carries_the_review_badge() -> None:
    rendered = _render_card(
        _card(
            amount_usd=None,
            rate_source="needs_review",
            rate=None,
            needs_review=True,
            category_name=None,
        )
    )

    assert "Unpriced" in rendered
    assert 'data-needs-review="true"' in rendered
    assert 'class="tbadge tbadge-warning tbadge-dot"' in rendered
    assert "Needs review" in rendered
    # No category → an em dash in the placeholder colour, never "No ID".
    assert 'class="flow-row-none">—</span>' in rendered


def test_note_becomes_the_mono_second_line() -> None:
    rendered = _render_card(_card(notes="gift for mom, reimburse half"))

    assert re.search(
        r'<span class="flow-row-note" data-note-indicator[^>]*>', rendered
    ), rendered
    assert "gift for mom" in rendered


# ---------------------------------------------------------------------------
# Sort chips, count, empty state.
# ---------------------------------------------------------------------------


def test_sort_chips_are_house_buttons_and_the_active_one_is_ink(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    client = web_client_factory()

    body = client.get(
        "/_partial/transactions/list", params={"date_from": "2000-01-01"}
    ).text

    assert 'class="tbtn tbtn-sm flow-sort is-active"' in body  # Date, the default
    assert 'class="tbtn tbtn-sm flow-sort"' in body  # Amount
    assert re.search(r'<span class="flow-count">\s*[\d,]+ matches\s*</span>', body)
    assert 'hx-vals=\'{"sort": "amount_native", "direction": "desc", "page": "1"}\'' in body


def test_empty_state_offers_a_way_back(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    client = web_client_factory()

    body = client.get(
        "/_partial/transactions/list",
        params={"date_from": "2000-01-01", "q": "nothing-matches-this"},
    ).text

    empty = re.search(r'<div class="flow-empty">.*?</div>', body, re.S)
    assert empty, body
    assert 'data-icon="search"' in empty.group(0)
    assert "No rows match these filters" in empty.group(0)
    assert 'href="/transactions"' in empty.group(0)
    assert "Clear filters" in empty.group(0)


# ---------------------------------------------------------------------------
# Filters.
# ---------------------------------------------------------------------------


def test_filters_form_keeps_its_id_and_htmx_wiring(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    client = web_client_factory()

    body = client.get("/transactions").text

    form = re.search(r'<form\s+id="tx-filters"[^>]*>', body)
    assert form, "the filters form lost its id"
    tag = form.group(0)
    assert 'class="flow-filters"' in tag
    assert 'hx-get="/_partial/transactions/list"' in tag
    assert 'hx-target="#tx-list"' in tag
    assert 'hx-push-url="true"' in tag
    assert "hx-trigger=\"change, search delay:300ms from:input[name='q']\"" in tag

    assert '<input type="hidden" name="sort" value="occurred_at">' in body
    assert '<input type="hidden" name="direction" value="desc">' in body
    assert '<input type="hidden" name="page" value="1">' in body
    assert '<a href="/transactions" data-clear-filters' in body

    # Labels are eyebrows; inputs are the house field.
    assert '<legend class="teyebrow">Accounts</legend>' in body
    assert re.search(
        r'<input\s+type="date"\s+name="date_from"[^>]*class="flow-input"', body
    )
    assert re.search(r'<select name="needs_review" class="flow-input">', body)
    # The chip groups keep the literal classes test_filters_polish pins;
    # flow.css owns them now.
    assert 'class="choice-chips"' in body
    assert 'class="choice-chip"' in body


# ---------------------------------------------------------------------------
# Saved views, bulk bar, picker.
# ---------------------------------------------------------------------------


def test_the_page_carries_no_saved_views_control(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    """Owner decision 2026-09-03: the "Save this view as…" row goes.

    The chip row, the save form and the three ``/_partial/views``
    endpoints existed only for that control; with it gone they are dead
    surface. The ``saved_views`` table, its repo and migration 010 stay —
    the schema is append-only and the data layer is not the viewer's.
    """
    client = web_client_factory()

    body = client.get("/transactions").text

    assert 'id="saved-views"' not in body
    assert "Save this view as" not in body
    assert "/_partial/views" not in body
    assert "flow-views" not in body

    assert client.get("/_partial/views").status_code == 404
    assert client.post("/_partial/views", data={"name": "x", "query_string": ""}).status_code == 404
    assert client.post("/_partial/views/1/delete").status_code == 404

    css = FLOW_CSS.read_text(encoding="utf-8")
    assert ".flow-view" not in css


def test_bulk_bar_is_a_raised_bar_with_the_same_hooks(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    client = web_client_factory()

    body = client.get("/transactions").text

    assert (
        '<div id="bulk-bar" class="flow-bulk" x-show="selected.length > 0" x-cloak>'
        in body
    ), "no raised #bulk-bar with its Alpine hooks"
    assert re.search(
        r'<span class="flow-bulk-count"><strong x-text="selected.length"></strong> selected</span>',
        body,
    )
    apply = re.search(r"<button[^>]*data-bulk-apply[^>]*>", body)
    assert apply
    assert 'class="tbtn tbtn-sm tbtn-primary"' in apply.group(0)
    # The Alpine logic is untouched.
    assert "/api/transactions/bulk-edit" in body
    assert "Pick a category first" in body
    assert "htmx.ajax('GET', '/_partial/transactions/list' + window.location.search" in body
    assert '@htmx:after-swap="selected = []"' in body


def test_category_picker_is_restyled_and_keeps_its_contract() -> None:
    from finances.domain.models import Category, TransactionKind

    cats = [
        Category(id=i + 1, kind=TransactionKind.EXPENSE, name=name, active=True)
        for i, name in enumerate(
            ["Groceries", "Transport", "Health", "Leisure", "Subscriptions",
             "Purchases", "Fees", "Dating", "Gifts"]
        )
    ]
    rendered = _templates().get_template("partials/category_picker.html").render(
        categories=cats, top_categories=cats[:8], picker_selected=3
    )

    assert 'class="flow-cat"' in rendered
    assert rendered.count('class="flow-cat-chip"') == 8
    assert rendered.count("data-chip=") == 8
    assert 'name="set_category" value="false"' in rendered
    assert 'name="category_id" value="3"' in rendered
    assert re.search(r'<input\s+type="text"\s+class="flow-input"\s+data-picker-search', rendered)
    assert "@keydown.enter.prevent" in rendered
    assert rendered.count("data-picker-item=") == len(cats)
    assert 'class="flow-cat-remove"' in rendered
    assert "data-picker-remove" in rendered
    assert "remove category" in rendered
    assert 'x-show="selected !== null"' in rendered
    # Selection is grey and weight, never a ring.
    assert "ring-2" not in rendered


# ---------------------------------------------------------------------------
# The edit modal.
# ---------------------------------------------------------------------------


def test_modal_is_the_signal_dialog_and_keeps_every_field(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    txn_id = _txn_id(seeded_web_db, "prov-3")
    client = web_client_factory()

    body = client.get(f"/_partial/transactions/{txn_id}/modal").text

    assert 'class="flow-modal-over"' in body
    assert 'class="flow-modal" role="dialog" aria-modal="true" tabindex="-1"' in body
    assert f'data-tx-modal data-tx-id="{txn_id}"' in body or (
        "data-tx-modal" in body and f'data-tx-id="{txn_id}"' in body
    )
    # Facts, formatted by the house filters: −3,650 VES at the typed 36.0.
    assert "−$101.39" in body
    assert "−Bs. 3,650.00" in body
    assert 'data-prov="user_rate"' in body
    assert "COM.PAGO grocery" in body
    assert "prov-3" in body
    # The form: same endpoint, same target, same sentinels, same class the
    # <body> dirty-guard in base.html queries.
    form = re.search(r"<form[^>]*class=\"tx-modal-form[^\"]*\"[^>]*>", body)
    assert form, "the edit form lost its tx-modal-form hook"
    assert f'hx-post="/_partial/transactions/{txn_id}/edit"' in form.group(0)
    assert f"hx-target=\"[data-tx-id='{txn_id}']\"" in form.group(0)
    for needle in (
        'name="set_user_rate" value="false"',
        'name="set_notes" value="false"',
        'name="set_category" value="false"',
        'name="category_id"',
        'name="user_rate"',
        'name="notes"',
        "rateDirty ? 'true' : 'false'",
        "notesDirty ? 'true' : 'false'",
        "data-category-picker",
        "data-picker-search",
    ):
        assert needle in body, needle
    # Save is the one red fill; Cancel is a ghost. Both stay verbs.
    assert re.search(r'<button[^>]*type="submit"[^>]*class="tbtn tbtn-primary"[^>]*>\s*Save', body)
    assert re.search(r'<button[^>]*class="tbtn tbtn-ghost"[^>]*>\s*Cancel', body)
    # Keyboard: still handled at the overlay, still inert while typing.
    assert "@keydown.window=" in body
    assert "isTyping($event)" in body
    assert "@keydown.escape.window" in body
    # Nothing from the old sheet survives.
    assert "tx-modal-overlay" not in body
    assert "tx-modal-card" not in body
    assert "tx-modal-section" not in body


def test_every_way_out_of_the_modal_goes_through_the_dirty_guard(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    """Esc, the scrim, Cancel and × all ask before discarding an edit.

    base.html's ``modalDirty()`` reads the set_* sentinels; until now only
    the prev/next arrows (which /transactions does not render) and the
    restart banner's Reload consulted it, so Esc on a half-written note
    closed the dialog and threw the note away (2026-09-03 browser walk).
    """
    txn_id = _txn_id(seeded_web_db, "prov-3")
    client = web_client_factory()

    body = client.get(f"/_partial/transactions/{txn_id}/modal").text

    # One guarded exit, defined on the overlay's own scope.
    guard = re.search(r"requestClose\(\)\s*\{(.*?)\n\s*\}", body, re.S)
    assert guard, "the modal defines no requestClose()"
    assert "this.modalDirty()" in guard.group(1)
    assert "window.confirm('Discard unsaved changes?')" in guard.group(1)
    assert "new CustomEvent('close-modal')" in guard.group(1)

    # Every exit uses it: Esc, the scrim, the × and Cancel.
    assert '@keydown.escape.window="requestClose()"' in body
    assert '@click.self="requestClose()"' in body
    assert body.count('@click="requestClose()"') == 2
    # ...and nothing dispatches close-modal behind the guard's back.
    assert body.count("new CustomEvent('close-modal')") == 1


def test_modal_history_is_a_hairline_list(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    from finances.db.repos import transactions as transactions_repo

    txn_id = _txn_id(seeded_web_db, "prov-1")
    transactions_repo.update(seeded_web_db, id=txn_id, notes="a note")
    client = web_client_factory()

    body = client.get(f"/_partial/transactions/{txn_id}/modal").text

    assert "History (1)" in body
    assert 'class="flow-history"' in body
    assert 'class="flow-history-item"' in body


# ---------------------------------------------------------------------------
# Pair candidates.
# ---------------------------------------------------------------------------


def test_pair_candidates_are_hairline_rows_with_house_buttons(
    web_client_factory,
) -> None:
    from tests.web.test_pairing import pairing_db  # noqa: F401  (fixture reuse)

    # Rendered directly: the view's shape is what is under test, and the
    # pairing fixture's window arithmetic belongs to test_pairing_web.
    candidate = SimpleNamespace(
        card=_card(
            id=7,
            kind="income",
            description="dep-exact",
            amount_native=Decimal("20000.00"),
            amount_usd=Decimal("30.83"),
        ),
        drift_ratio=Decimal("0.004"),
        pairable=True,
        blocked_reason=None,
    )
    blocked = SimpleNamespace(
        card=_card(id=8, description="dep-neg", amount_native=Decimal("-5.00")),
        drift_ratio=None,
        pairable=False,
        blocked_reason="same sign",
    )
    data = SimpleNamespace(
        sell=SimpleNamespace(id=3),
        expected_ves=Decimal("20000.00"),
        window_days=2,
        candidates=[candidate, blocked],
    )
    rendered = _templates().get_template("partials/pair_candidates.html").render(data=data)

    assert 'id="pair-candidates" class="flow-pairs"' in rendered
    assert "Bs. 20,000.00" in rendered  # expected, through fmt_native
    assert rendered.count('class="flow-pair"') == 2
    assert 'hx-post="/_partial/transactions/3/pair/7"' in rendered
    assert re.search(r'<button[^>]*class="tbtn tbtn-sm"[^>]*hx-post="/_partial/transactions/3/pair/7"', rendered)
    assert re.search(r'<button[^>]*class="tbtn tbtn-sm"[^>]*disabled[^>]*title="same sign"', rendered)
    assert 'class="tlink"' in rendered  # widen to ±7 days
    assert "text-emerald" not in rendered


# ---------------------------------------------------------------------------
# Upload.
# ---------------------------------------------------------------------------


def test_upload_details_keeps_its_id_and_opens_on_the_query(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    client = web_client_factory()

    closed = client.get("/transactions").text
    opened = client.get("/transactions", params={"upload": "1"}).text

    assert 'id="upload"' in closed
    assert not re.search(r'<details[^>]*id="upload"[^>]*\sopen', closed)
    assert re.search(r'<details class="flow-upload-panel" id="upload" open>', opened)

    panel = opened[opened.index("data-provincial-dropzone") : opened.index("</details>")]
    assert '<summary class="flow-upload-summary">' in panel
    assert 'data-icon="upload"' in panel
    assert "Import a Provincial statement" in panel
    assert 'id="provincial-upload-form"' in panel
    assert 'hx-post="/_partial/uploads/provincial/preview"' in panel
    assert 'hx-encoding="multipart/form-data"' in panel
    assert 'hx-target="#upload-result"' in panel
    assert 'class="flow-drop"' in panel
    assert "x-bind:class=\"over && 'is-over'\"" in panel
    assert re.search(
        r'<input\s+type="file"\s+name="file"\s+accept="\.csv,\.xls"\s+class="flow-drop-input"\s+x-ref="file"',
        panel,
    )
    assert 'hx-include="closest form"' in panel
    assert '<div id="upload-result" aria-live="polite"></div>' in panel


def test_upload_preview_card_and_primary_import_button() -> None:
    preview = SimpleNamespace(
        token="tok-1",
        filename="statement.csv",
        rows_seen=3,
        rows_new=3,
        rows_known=0,
        date_from=datetime(2026, 7, 14).date(),
        date_to=datetime(2026, 7, 16).date(),
    )
    rendered = _templates().get_template("partials/upload_preview.html").render(
        preview=preview, error=None, result=None
    )

    assert 'class="flow-upload-preview" data-upload-preview' in rendered
    assert "statement.csv" in rendered
    assert 'data-upload-token="tok-1"' in rendered
    assert 'hx-post="/_partial/uploads/provincial/import"' in rendered
    assert re.search(r'<button[^>]*type="submit"[^>]*class="tbtn tbtn-primary"[^>]*>\s*Import 3 new rows', rendered)
    assert "Jul 14" in rendered and "Jul 16" in rendered


def test_upload_error_and_receipt_lines() -> None:
    template = _templates().get_template("partials/upload_preview.html")

    error = template.render(preview=None, error="Only .csv or .xls, please", result=None)
    assert 'class="flow-upload-msg flow-upload-msg-error" data-upload-error' in error
    assert "Only .csv or .xls" in error

    done = template.render(
        preview=None,
        error=None,
        result=SimpleNamespace(filename="statement.csv", rows_inserted=3, rows_updated=0),
    )
    assert 'class="flow-upload-msg" data-upload-done' in done
    assert "3 new" in done
    assert "inputs/processed/" in done


# ---------------------------------------------------------------------------
# No Tailwind left, and flow.css owns the grid.
# ---------------------------------------------------------------------------


def test_no_tailwind_utility_survives_in_the_flow_templates() -> None:
    offenders: dict[str, list[str]] = {}
    for name in FLOW_TEMPLATES:
        source = (TEMPLATES_DIR / name).read_text(encoding="utf-8")
        tokens: set[str] = set()
        for value in re.findall(r'(?<![-:\w])class="([^"]*)"', source):
            literal = re.sub(r"\{[%{].*?[%}]\}", " ", value)
            tokens.update(t for t in literal.split() if "{" not in t)
        bad = sorted(t for t in tokens if _TAILWIND_TOKEN.match(t))
        if bad:
            offenders[name] = bad
    assert not offenders, offenders


def test_flow_css_owns_the_row_grid() -> None:
    css = FLOW_CSS.read_text(encoding="utf-8")

    rows = css[css.index("\n.flow-rows {") :]
    rows = rows[: rows.index("}")]
    assert "display: grid" in rows
    assert "grid-template-columns:" in rows

    row = css[css.index("\n.flow-row {") :]
    row = row[: row.index("}")]
    assert "grid-column: 1 / -1" in row
    assert "grid-template-columns: subgrid" in row
    assert "align-items: center" in row
    assert "min-height: 44px" in row
    assert "border-top: 1px solid var(--border-subtle)" in row

    assert ".flow-rows.is-selectable" in css
    # Selection is grey, never red.
    chip = css[css.index(".choice-chip input:checked + span") :]
    chip = chip[: chip.index("}")]
    assert "var(--surface-selected)" in chip
    assert "red" not in chip


def test_flow_templates_use_no_app_css_families() -> None:
    """The .cards / .card-row / .tx-modal-* / .view-chip / .upload-* rules in
    app.css are on borrowed time; nothing here may lean on them. Two names
    are pinned by tests outside this track and survive as aliases that
    flow.css defines itself: ``cards--selectable`` (test_bulk_ui) and
    ``choice-chip(s)`` (test_filters_polish). ``tx-modal-form`` is the
    hook base.html's dirty guard queries."""
    allowed = {"cards--selectable", "choice-chips", "choice-chip", "tx-modal-form"}
    legacy = re.compile(r"^(?:cards|card-row|tx-modal-|view-chip|upload-|choice-chip)")
    offenders: dict[str, list[str]] = {}
    for name in FLOW_TEMPLATES:
        source = (TEMPLATES_DIR / name).read_text(encoding="utf-8")
        tokens: set[str] = set()
        for value in re.findall(r'(?<![-:\w])class="([^"]*)"', source):
            literal = re.sub(r"\{[%{].*?[%}]\}", " ", value)
            tokens.update(t for t in literal.split() if "{" not in t)
        bad = sorted(t for t in tokens if legacy.match(t) and t not in allowed)
        if bad:
            offenders[name] = bad
    assert not offenders, offenders

    css = FLOW_CSS.read_text(encoding="utf-8")
    for alias in allowed:
        assert f".{alias}" in css, f"flow.css must define the pinned alias .{alias}"
