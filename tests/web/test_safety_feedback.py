"""WP2 — safety + feedback tests (ux-overhaul, docs/plans/ux-overhaul/00-design.md §2).

Covers:

* base.html toast infrastructure (toast host div, JSON HX-Trigger
  parsing, htmx:responseError listener, show-toast plumbing),
* HX-Trigger toast JSON on the edit / triage-edit / pair-confirm POSTs,
* edit-modal dirty tracking (an untouched select must NOT clear the
  category), the explicit "remove category" control, and autofocus on
  the category control — for both modal_transaction.html and
  modal_transaction_triage.html.

House style notes: template behavior is pinned via string markers on the
rendered partials (same approach as tests/web/test_transactions_write.py),
endpoint semantics via form POSTs + repo re-reads. All DB access goes
through the tmp-DB fixtures in tests/web/conftest.py — never the real
finances.db.
"""

from __future__ import annotations

import json
import sqlite3
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from finances.db.repos import transactions as transactions_repo
from finances.domain.models import Transaction, TransactionKind


# ---------------------------------------------------------------------------
# Task 1 — base.html toast infrastructure.
# ---------------------------------------------------------------------------


def test_base_html_has_toast_host(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    client = web_client_factory()
    resp = client.get("/")
    assert resp.status_code == 200
    assert 'id="toast-host"' in resp.text


def test_base_html_parses_json_hx_trigger_and_dispatches_show_toast(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    """The <body> listener must parse JSON HX-Trigger headers (not only the
    legacy comma list) and re-dispatch closeModal / toast as window events."""
    client = web_client_factory()
    body = client.get("/").text
    assert "JSON.parse" in body
    assert "show-toast" in body
    assert "close-modal" in body  # legacy close path must survive


def test_base_html_has_htmx_response_error_listener(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    client = web_client_factory()
    body = client.get("/").text
    assert "htmx:response-error" in body
    # The listener surfaces the server's error body (JSON detail field).
    assert "responseText" in body


def test_app_css_has_toast_styles(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    client = web_client_factory()
    resp = client.get("/static/css/app.css")
    assert resp.status_code == 200
    assert ".toast-host" in resp.text
    assert ".toast-success" in resp.text
    assert ".toast-error" in resp.text


# ---------------------------------------------------------------------------
# Shared helpers (Tasks 2-4).
# ---------------------------------------------------------------------------


def _txn_id(conn: sqlite3.Connection, source_ref: str) -> int:
    row = conn.execute(
        "SELECT id FROM transactions WHERE source_ref = ?", (source_ref,)
    ).fetchone()
    assert row is not None, f"missing seeded txn {source_ref}"
    return int(row["id"])


@pytest.fixture
def pair_candidates(seeded_web_db: sqlite3.Connection) -> tuple[int, int]:
    """Insert an unpaired Provincial deposit + matching Binance sell.

    Mirrors the pair shape proven in tests/web/test_triage.py:
    expected VES = abs(sell amount) * user_rate = 1000 * 36.50 = 36500.
    The sell leg uses a REAL negative amount (expense sign convention —
    do not copy seeded_web_db's positive-amount wart).
    """
    prov_row = seeded_web_db.execute(
        "SELECT id FROM accounts WHERE name = 'Provincial'"
    ).fetchone()
    bin_row = seeded_web_db.execute(
        "SELECT id FROM accounts WHERE name = 'Binance Spot'"
    ).fetchone()
    assert prov_row is not None and bin_row is not None
    yesterday = datetime.now(tz=UTC) - timedelta(days=1)

    transactions_repo.insert(
        seeded_web_db,
        Transaction(
            account_id=int(prov_row["id"]),
            occurred_at=yesterday,
            kind=TransactionKind.INCOME,
            amount=Decimal("36500.00"),
            currency="VES",
            description="ABONO P2P sell",
            source="provincial",
            source_ref="wp2-bank-deposit-1",
        ),
    )
    transactions_repo.insert(
        seeded_web_db,
        Transaction(
            account_id=int(bin_row["id"]),
            occurred_at=yesterday,
            kind=TransactionKind.EXPENSE,
            amount=Decimal("-1000.00"),
            currency="USDT",
            description="P2P sell USDT",
            user_rate=Decimal("36.50"),
            source="binance",
            source_ref="wp2-binance-sell-1",
        ),
    )
    return (
        _txn_id(seeded_web_db, "wp2-bank-deposit-1"),
        _txn_id(seeded_web_db, "wp2-binance-sell-1"),
    )


# ---------------------------------------------------------------------------
# Task 2 — HX-Trigger carries the toast JSON payload.
# ---------------------------------------------------------------------------


def test_edit_endpoint_hx_trigger_carries_toast_json(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    client = web_client_factory()
    txn_id = _txn_id(seeded_web_db, "prov-1")

    resp = client.post(
        f"/_partial/transactions/{txn_id}/edit",
        data={
            "set_category": "false",
            "category_id": "",
            "set_user_rate": "true",
            "user_rate": "36.5",
        },
    )
    assert resp.status_code == 200, resp.text
    payload = json.loads(resp.headers["HX-Trigger"])
    assert payload["closeModal"] is True
    assert payload["toast"] == {"level": "success", "message": "Saved"}


def test_triage_edit_hx_trigger_carries_toast_and_queue_dirty(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    """Mid-run: toast + queueDirty, and pointedly NO closeModal.

    ADR-012 Amendment 2026-07-26 — the response body is the next item's
    modal, and the base.html close handler clears #tx-modal-host
    unconditionally, so closeModal here would discard it.
    """
    client = web_client_factory()
    txn_id = _txn_id(seeded_web_db, "prov-needs-review")

    resp = client.post(
        f"/_partial/triage/{txn_id}/edit",
        data={
            "set_category": "false",
            "category_id": "",
            "set_user_rate": "true",
            "user_rate": "36.5",
        },
    )
    assert resp.status_code == 200, resp.text
    payload = json.loads(resp.headers["HX-Trigger"])
    assert "closeModal" not in payload
    assert payload["queueDirty"] == {"typeFilter": None}
    # Since the triage redesign the copy is specific, never "Saved" (I10).
    assert payload["toast"] == {
        "level": "success",
        "message": "Rate set to 36.50.",
    }


def test_pair_confirm_hx_trigger_carries_toast_json(
    pair_candidates: tuple[int, int], web_client_factory
) -> None:
    deposit_id, sell_id = pair_candidates
    client = web_client_factory()

    resp = client.post(f"/_partial/triage/pair/{deposit_id}/{sell_id}/confirm")
    assert resp.status_code == 200, resp.text
    payload = json.loads(resp.headers["HX-Trigger"])
    assert payload["queueDirty"] == {"typeFilter": None}
    assert payload["toast"] == {"level": "success", "message": "Paired."}


# ---------------------------------------------------------------------------
# Task 3 — transactions edit modal: dirty tracking, remove control, focus.
#
# The wipe bug is a TEMPLATE bug (hard-coded set_category=true sentinels);
# apply_edit already honors set_*=false. So the template-marker tests below
# are the red ones; the endpoint tests pin the (already-correct) server
# contract that the new untouched-form payload relies on.
# ---------------------------------------------------------------------------


def test_modal_no_hardcoded_set_sentinels(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    client = web_client_factory()
    txn_id = _txn_id(seeded_web_db, "prov-1")
    body = client.get(f"/_partial/transactions/{txn_id}/modal").text

    # The old always-true sentinels are gone...
    assert 'name="set_category" value="true"' not in body
    assert 'name="set_user_rate" value="true"' not in body
    # ...replaced by the picker's untouched-default sentinel (WP4) and
    # WP2's rate dirty-tracking, which the picker leaves in place.
    assert 'name="set_category" value="false"' in body
    assert "rateDirty" in body


def test_modal_untouched_fields_do_not_wipe(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    """Payload an untouched dirty-tracked form now submits: both set_*
    false, both values empty. Nothing may be cleared."""
    client = web_client_factory()
    txn_id = _txn_id(seeded_web_db, "prov-3")  # has category AND user_rate
    before = transactions_repo.get_by_id(seeded_web_db, txn_id)
    assert before is not None
    assert before.category_id is not None
    assert before.user_rate is not None

    resp = client.post(
        f"/_partial/transactions/{txn_id}/edit",
        data={
            "set_category": "false",
            "category_id": "",
            "set_user_rate": "false",
            "user_rate": "",
        },
    )
    assert resp.status_code == 200, resp.text

    after = transactions_repo.get_by_id(seeded_web_db, txn_id)
    assert after is not None
    assert after.category_id == before.category_id
    assert after.user_rate == before.user_rate


def test_modal_has_remove_category_control_when_categorized(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    client = web_client_factory()
    txn_id = _txn_id(seeded_web_db, "prov-1")  # categorized (Groceries)
    body = client.get(f"/_partial/transactions/{txn_id}/modal").text
    assert "remove category" in body


def test_modal_hides_remove_category_control_when_uncategorized(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    client = web_client_factory()
    txn_id = _txn_id(seeded_web_db, "cash-1")  # no category
    body = client.get(f"/_partial/transactions/{txn_id}/modal").text
    # WP4's picker always server-renders the control and hides it at
    # runtime via Alpine — assert the binding, not string absence.
    assert 'x-show="selected !== null"' in body


def test_modal_explicit_remove_payload_clears_category(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    """Payload the '× remove category' control produces: set_category=true
    with an empty category_id. This — and only this — clears."""
    client = web_client_factory()
    txn_id = _txn_id(seeded_web_db, "prov-1")
    before = transactions_repo.get_by_id(seeded_web_db, txn_id)
    assert before is not None and before.category_id is not None

    resp = client.post(
        f"/_partial/transactions/{txn_id}/edit",
        data={
            "set_category": "true",
            "category_id": "",
            "set_user_rate": "false",
            "user_rate": "",
        },
    )
    assert resp.status_code == 200, resp.text

    after = transactions_repo.get_by_id(seeded_web_db, txn_id)
    assert after is not None
    assert after.category_id is None


def test_modal_category_control_is_the_shared_picker(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    client = web_client_factory()
    txn_id = _txn_id(seeded_web_db, "prov-1")
    body = client.get(f"/_partial/transactions/{txn_id}/modal").text
    assert "data-category-picker" in body


# ---------------------------------------------------------------------------
# Task 4 — triage modal: same dirty tracking, remove control, focus.
# ---------------------------------------------------------------------------


def test_triage_modal_no_hardcoded_set_sentinels(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    """The dirty-tracked sentinels survived the redesign.

    Their default is still "false" and Alpine still flips them only when
    the owner actually touches the matching control — the triage dialog
    just derives them from the row's draft rather than from a per-field
    flag. ``prov-needs-review`` is used because the redesigned dialog
    opens queue entries, and a fully-resolved row is not one.
    """
    client = web_client_factory()
    txn_id = _txn_id(seeded_web_db, "prov-needs-review")
    body = client.get(f"/_partial/triage/{txn_id}/modal").text

    assert 'name="set_category" value="true"' not in body
    assert 'name="set_user_rate" value="true"' not in body
    assert 'name="set_category" value="false"' in body
    assert 'name="set_user_rate" value="false"' in body


def test_triage_edit_untouched_fields_do_not_wipe(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    """Server-contract pin for the triage edit endpoint: the untouched
    dirty-tracked payload must clear nothing (passes pre-impl; guards
    the payload shape the new template emits)."""
    client = web_client_factory()
    txn_id = _txn_id(seeded_web_db, "prov-3")  # has category AND user_rate
    before = transactions_repo.get_by_id(seeded_web_db, txn_id)
    assert before is not None
    assert before.category_id is not None
    assert before.user_rate is not None

    resp = client.post(
        f"/_partial/triage/{txn_id}/edit",
        data={
            "set_category": "false",
            "category_id": "",
            "set_user_rate": "false",
            "user_rate": "",
        },
    )
    assert resp.status_code == 200, resp.text

    after = transactions_repo.get_by_id(seeded_web_db, txn_id)
    assert after is not None
    assert after.category_id == before.category_id
    assert after.user_rate == before.user_rate


def test_the_triage_dialog_does_not_offer_to_remove_a_category(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    """Un-categorising is the transactions modal's job, not triage's.

    The redesigned dialog opens rows that are MISSING a category; a
    control for taking one away has nothing to act on there, and the
    /transactions modal still carries it for the rows that do.
    """
    client = web_client_factory()
    txn_id = _txn_id(seeded_web_db, "prov-needs-review")
    body = client.get(f"/_partial/triage/{txn_id}/modal").text

    assert "remove category" not in body


def test_triage_modal_category_control_is_the_shared_picker(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    client = web_client_factory()
    txn_id = _txn_id(seeded_web_db, "prov-needs-review")
    body = client.get(f"/_partial/triage/{txn_id}/modal").text
    assert "data-category-picker" in body
