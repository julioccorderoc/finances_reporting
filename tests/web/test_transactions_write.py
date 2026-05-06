"""Phase 3 — transaction edit modal write-path tests (EPIC-024).

Per rule-011 these land before the implementation. Coverage:

* ``finances/db/repos/transactions.py::update`` (new)
  - per-field updates with the ``_UNSET`` sentinel,
  - returns Pydantic ``Transaction`` reflecting the new values,
  - raises on unknown id.
* ``finances/web/services/transactions_write.py::apply_edit``
  - applies category and/or user_rate changes via the repo,
  - re-runs ``rates.resolve`` and reconciles ``needs_review`` per ADR-012,
  - returns the freshly-projected ``TransactionCard``.
* ``PATCH /api/transactions/{id}`` (JSON).
* ``GET /_partial/transactions/{id}/modal`` (HTMX).
* ``POST /_partial/transactions/{id}/edit`` (form-encoded HTMX).
* card_transaction.html now carries the modal-trigger HTMX attributes.
* base.html exposes the modal host div + Alpine listener.
* Phase 2 regression: representative pages still respond 200 with key markers.
"""

from __future__ import annotations

import sqlite3
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from fastapi.testclient import TestClient

from finances.db.repos import categories as categories_repo
from finances.db.repos import transactions as transactions_repo
from finances.domain.models import TransactionKind


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _first_provincial_txn_id(conn: sqlite3.Connection, source_ref: str = "prov-1") -> int:
    row = conn.execute(
        "SELECT id FROM transactions WHERE source_ref = ?", (source_ref,)
    ).fetchone()
    assert row is not None, f"seeded txn {source_ref} not present"
    return int(row["id"])


def _legacy_needs_review_id(conn: sqlite3.Connection) -> int:
    return _first_provincial_txn_id(conn, source_ref="prov-needs-review")


def _user_rate_txn_id(conn: sqlite3.Connection) -> int:
    return _first_provincial_txn_id(conn, source_ref="prov-3")


# ---------------------------------------------------------------------------
# Repo: transactions_repo.update
# ---------------------------------------------------------------------------


def test_repo_update_sets_category_only(seeded_web_db: sqlite3.Connection) -> None:
    txn_id = _first_provincial_txn_id(seeded_web_db)
    before = transactions_repo.get_by_id(seeded_web_db, txn_id)
    assert before is not None

    other = categories_repo.get_by_name(seeded_web_db, TransactionKind.EXPENSE, "Restaurants")
    if other is None:
        # Pick *any* expense category that is not the current one.
        cats = categories_repo.list_all(seeded_web_db)
        other = next(c for c in cats if c.kind == TransactionKind.EXPENSE and c.id != before.category_id)

    updated = transactions_repo.update(
        seeded_web_db,
        id=txn_id,
        category_id=other.id,
    )

    assert updated.category_id == other.id
    assert updated.user_rate == before.user_rate  # untouched
    assert updated.amount == before.amount         # untouched
    assert updated.description == before.description


def test_repo_update_sets_user_rate_only(seeded_web_db: sqlite3.Connection) -> None:
    txn_id = _first_provincial_txn_id(seeded_web_db)
    before = transactions_repo.get_by_id(seeded_web_db, txn_id)
    assert before is not None

    updated = transactions_repo.update(
        seeded_web_db,
        id=txn_id,
        user_rate=Decimal("40.00"),
    )

    assert updated.user_rate == Decimal("40.00")
    assert updated.category_id == before.category_id  # untouched


def test_repo_update_with_unset_does_not_touch_fields(
    seeded_web_db: sqlite3.Connection,
) -> None:
    txn_id = _first_provincial_txn_id(seeded_web_db)
    before = transactions_repo.get_by_id(seeded_web_db, txn_id)
    assert before is not None

    # No fields set → no-op aside from updated_at.
    updated = transactions_repo.update(seeded_web_db, id=txn_id)

    assert updated.category_id == before.category_id
    assert updated.user_rate == before.user_rate
    assert updated.needs_review == before.needs_review


def test_repo_update_returns_updated_pydantic_transaction(
    seeded_web_db: sqlite3.Connection,
) -> None:
    from finances.domain.models import Transaction

    txn_id = _first_provincial_txn_id(seeded_web_db)
    updated = transactions_repo.update(
        seeded_web_db,
        id=txn_id,
        user_rate=Decimal("42.50"),
    )

    assert isinstance(updated, Transaction)
    assert updated.user_rate == Decimal("42.50")

    # updated_at present and >= created_at when both are set.
    refreshed = seeded_web_db.execute(
        "SELECT created_at, updated_at FROM transactions WHERE id = ?", (txn_id,)
    ).fetchone()
    assert refreshed["updated_at"] is not None


def test_repo_update_can_clear_category_with_explicit_none(
    seeded_web_db: sqlite3.Connection,
) -> None:
    txn_id = _first_provincial_txn_id(seeded_web_db)

    updated = transactions_repo.update(
        seeded_web_db,
        id=txn_id,
        category_id=None,
    )

    assert updated.category_id is None


def test_repo_update_can_clear_user_rate_with_explicit_none(
    seeded_web_db: sqlite3.Connection,
) -> None:
    txn_id = _user_rate_txn_id(seeded_web_db)
    before = transactions_repo.get_by_id(seeded_web_db, txn_id)
    assert before is not None and before.user_rate is not None

    updated = transactions_repo.update(
        seeded_web_db,
        id=txn_id,
        user_rate=None,
    )

    assert updated.user_rate is None


def test_repo_update_404_for_unknown_id(seeded_web_db: sqlite3.Connection) -> None:
    with pytest.raises((LookupError, ValueError)):
        transactions_repo.update(seeded_web_db, id=999_999, user_rate=Decimal("1"))


# ---------------------------------------------------------------------------
# Service: transactions_write.apply_edit
# ---------------------------------------------------------------------------


def test_apply_edit_recomputes_needs_review_when_user_rate_set(
    seeded_web_db: sqlite3.Connection,
) -> None:
    from finances.web.services.transactions_write import (
        TransactionEditRequest,
        apply_edit,
    )

    txn_id = _legacy_needs_review_id(seeded_web_db)
    # Seed txn has needs_review=1 by construction.
    before = transactions_repo.get_by_id(seeded_web_db, txn_id)
    assert before is not None and before.needs_review is True

    card = apply_edit(
        seeded_web_db,
        txn_id=txn_id,
        req=TransactionEditRequest(set_user_rate=True, user_rate=Decimal("36.5")),
    )

    assert card.needs_review is False
    assert card.rate_source == "user_rate"

    # And it's persisted.
    after = transactions_repo.get_by_id(seeded_web_db, txn_id)
    assert after is not None
    assert after.user_rate == Decimal("36.5")
    assert after.needs_review is False


def test_apply_edit_flips_needs_review_when_user_rate_cleared(
    seeded_web_db: sqlite3.Connection,
) -> None:
    """Clearing user_rate on the legacy 2010 txn should re-derive needs_review.

    The 2010 txn has no rate available (P2P / BCV both miss); the engine must
    fall through to ``needs_review``. We first set a user_rate (making it
    "ok"), then clear it, and assert needs_review flips back to True.
    """
    from finances.web.services.transactions_write import (
        TransactionEditRequest,
        apply_edit,
    )

    txn_id = _legacy_needs_review_id(seeded_web_db)
    apply_edit(
        seeded_web_db,
        txn_id=txn_id,
        req=TransactionEditRequest(set_user_rate=True, user_rate=Decimal("36.5")),
    )

    card = apply_edit(
        seeded_web_db,
        txn_id=txn_id,
        req=TransactionEditRequest(set_user_rate=True, user_rate=None),
    )

    assert card.needs_review is True
    assert card.rate_source == "needs_review"


def test_apply_edit_setting_category_does_not_touch_needs_review(
    seeded_web_db: sqlite3.Connection,
) -> None:
    from finances.web.services.transactions_write import (
        TransactionEditRequest,
        apply_edit,
    )

    # Use a healthy txn that resolves cleanly via P2P.
    txn_id = _first_provincial_txn_id(seeded_web_db)
    cats = categories_repo.list_all(seeded_web_db)
    expense_cats = [c for c in cats if c.kind == TransactionKind.EXPENSE]
    assert len(expense_cats) >= 2
    target = expense_cats[1]

    card = apply_edit(
        seeded_web_db,
        txn_id=txn_id,
        req=TransactionEditRequest(set_category=True, category_id=target.id),
    )

    assert card.needs_review is False
    assert card.category_name == target.name


def test_apply_edit_unknown_txn_raises(seeded_web_db: sqlite3.Connection) -> None:
    from finances.web.services.transactions_write import (
        TransactionEditRequest,
        apply_edit,
    )

    with pytest.raises((LookupError, ValueError)):
        apply_edit(
            seeded_web_db,
            txn_id=999_999,
            req=TransactionEditRequest(set_user_rate=True, user_rate=Decimal("1")),
        )


# ---------------------------------------------------------------------------
# JSON endpoint: PATCH /api/transactions/{id}
# ---------------------------------------------------------------------------


def test_patch_endpoint_200_returns_updated_card_json(
    seeded_web_db: sqlite3.Connection,
    web_client_factory,
) -> None:
    client: TestClient = web_client_factory()
    txn_id = _first_provincial_txn_id(seeded_web_db)

    resp = client.patch(
        f"/api/transactions/{txn_id}",
        json={"set_user_rate": True, "user_rate": "36.5"},
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["id"] == txn_id
    assert body["rate_source"] == "user_rate"
    assert Decimal(body["amount_usd"]) == (Decimal("365.00") / Decimal("36.5"))


def test_patch_endpoint_404_unknown_id(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    client = web_client_factory()
    resp = client.patch(
        "/api/transactions/999999",
        json={"set_user_rate": True, "user_rate": "1"},
    )
    assert resp.status_code == 404


def test_patch_endpoint_422_invalid_body(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    client = web_client_factory()
    txn_id = _first_provincial_txn_id(seeded_web_db)
    # Extra-forbid model + nonsense field → 422.
    resp = client.patch(
        f"/api/transactions/{txn_id}",
        json={"description": "you can't change this"},
    )
    assert resp.status_code == 422


# ---------------------------------------------------------------------------
# Modal partial: GET /_partial/transactions/{id}/modal
# ---------------------------------------------------------------------------


def test_modal_partial_renders(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    client = web_client_factory()
    txn_id = _user_rate_txn_id(seeded_web_db)

    resp = client.get(f"/_partial/transactions/{txn_id}/modal")
    assert resp.status_code == 200
    body = resp.text

    # Provenance block
    assert "source" in body.lower()
    assert "source_ref" in body or "prov-3" in body
    # Description shown un-truncated
    assert "COM.PAGO grocery" in body
    # Editable form pieces
    assert 'name="category_id"' in body
    assert 'name="user_rate"' in body
    assert "Save" in body
    assert "Cancel" in body
    # Targets the row card for swap.
    assert f"data-tx-id='{txn_id}'" in body or f'data-tx-id="{txn_id}"' in body


def test_modal_partial_includes_all_categories_in_dropdown(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    client = web_client_factory()
    txn_id = _first_provincial_txn_id(seeded_web_db)

    resp = client.get(f"/_partial/transactions/{txn_id}/modal")
    assert resp.status_code == 200
    body = resp.text

    cats = categories_repo.list_all(seeded_web_db)
    assert len(cats) >= 5  # sanity
    for cat in cats:
        # name should appear at least once in an <option> tag.
        assert cat.name in body


def test_modal_partial_404_unknown_id(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    client = web_client_factory()
    resp = client.get("/_partial/transactions/999999/modal")
    assert resp.status_code == 404


# ---------------------------------------------------------------------------
# Edit form POST: /_partial/transactions/{id}/edit
# ---------------------------------------------------------------------------


def test_edit_form_post_swaps_card_and_triggers_close(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    client = web_client_factory()
    txn_id = _first_provincial_txn_id(seeded_web_db)

    resp = client.post(
        f"/_partial/transactions/{txn_id}/edit",
        data={
            "set_category": "true",
            "category_id": "",
            "set_user_rate": "true",
            "user_rate": "36.5",
        },
    )
    assert resp.status_code == 200, resp.text
    body = resp.text
    # Updated card in response.
    assert "data-card-row" in body
    assert f'data-tx-id="{txn_id}"' in body
    # No full HTML shell (partial).
    assert "<html" not in body.lower()
    # HX-Trigger header signals modal close.
    trigger = resp.headers.get("HX-Trigger", "")
    assert "closeModal" in trigger


def test_edit_form_clears_user_rate_when_empty_string(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    client = web_client_factory()
    txn_id = _user_rate_txn_id(seeded_web_db)

    # First confirm baseline user_rate is populated.
    before = transactions_repo.get_by_id(seeded_web_db, txn_id)
    assert before is not None and before.user_rate is not None

    resp = client.post(
        f"/_partial/transactions/{txn_id}/edit",
        data={
            "set_category": "false",
            "category_id": "",
            "set_user_rate": "true",
            "user_rate": "",
        },
    )
    assert resp.status_code == 200, resp.text

    after = transactions_repo.get_by_id(seeded_web_db, txn_id)
    assert after is not None
    assert after.user_rate is None


def test_edit_form_clears_category_when_empty_string(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    client = web_client_factory()
    txn_id = _first_provincial_txn_id(seeded_web_db)
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


def test_edit_form_404_for_unknown_id(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    client = web_client_factory()
    resp = client.post(
        "/_partial/transactions/999999/edit",
        data={
            "set_user_rate": "true",
            "user_rate": "1",
        },
    )
    assert resp.status_code == 404


# ---------------------------------------------------------------------------
# Card partial — modal trigger wiring.
# ---------------------------------------------------------------------------


def test_card_partial_outer_article_has_modal_trigger_attributes(
    web_client_factory,
) -> None:
    """Direct render of card_transaction.html should embed HTMX modal-open hooks."""
    from finances.web.app import create_app
    from finances.web.services.transactions_query import TransactionCard
    from finances.web.settings import WebSettings

    app = create_app(WebSettings(host="127.0.0.1"))
    templates = app.state.templates

    card = TransactionCard(
        id=42,
        occurred_at=datetime(2026, 5, 1, 12, 0, tzinfo=UTC),
        account_name="Provincial",
        description="probe",
        amount_native=Decimal("100.00"),
        currency="VES",
        amount_usd=Decimal("2.74"),
        rate_source="binance_p2p_median",
        is_bcv_fallback=False,
        kind="expense",
        category_name="Food",
        needs_review=False,
    )
    rendered = templates.get_template("partials/card_transaction.html").render(card=card)

    assert "/_partial/transactions/42/modal" in rendered
    # Must use hx-get for the modal load.
    assert "hx-get=" in rendered


def test_card_partial_modal_trigger_targets_host_div(
    web_client_factory,
) -> None:
    from finances.web.app import create_app
    from finances.web.services.transactions_query import TransactionCard
    from finances.web.settings import WebSettings

    app = create_app(WebSettings(host="127.0.0.1"))
    templates = app.state.templates

    card = TransactionCard(
        id=7,
        occurred_at=datetime(2026, 5, 1, 12, 0, tzinfo=UTC),
        account_name="Provincial",
        description="probe",
        amount_native=Decimal("1.00"),
        currency="USD",
        amount_usd=Decimal("1.00"),
        rate_source="native_usd",
        is_bcv_fallback=False,
        kind="expense",
        category_name=None,
        needs_review=False,
    )
    rendered = templates.get_template("partials/card_transaction.html").render(card=card)

    assert "#tx-modal-host" in rendered


def test_base_html_has_modal_host_div(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    """The dashboard renders base.html — verify the modal host div is in place."""
    client = web_client_factory()
    resp = client.get("/")
    assert resp.status_code == 200
    body = resp.text
    assert 'id="tx-modal-host"' in body
    # Alpine listener for HX-Trigger: closeModal
    assert "closeModal" in body or "close-modal" in body


# ---------------------------------------------------------------------------
# Phase 2 regression — representative pages still 200 with key markers.
# ---------------------------------------------------------------------------


def test_phase_2_tests_still_pass(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    client = web_client_factory()
    today = datetime.now(tz=UTC).date()
    month = today.strftime("%Y-%m")

    r = client.get("/")
    assert r.status_code == 200
    assert "Finances" in r.text

    r = client.get("/transactions", params={"date_from": "2000-01-01"})
    assert r.status_code == 200
    assert "data-card-row" in r.text

    r = client.get("/monthly", params={"range_preset": "3m"})
    assert r.status_code == 200

    r = client.get("/accounts")
    assert r.status_code == 200

    r = client.get("/rates")
    assert r.status_code == 200
