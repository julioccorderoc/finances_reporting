"""WP4 — both edit modals render the shared category picker (tests first).

The 26-option native ``category_select`` macro is replaced by
partials/category_picker.html in modal_transaction.html AND
modal_transaction_triage.html. The server passes ``top_categories``
computed per the transaction's kind.

Two tests here are regression GUARDS and pass before the impl commit
(full category list still present; untouched picker never wipes) — the
other three fail first, per rule-011.
"""

from __future__ import annotations

import sqlite3

from finances.db.repos import categories as categories_repo
from finances.db.repos import transactions as transactions_repo
from finances.domain.models import TransactionKind


def _txn_id(conn: sqlite3.Connection, source_ref: str) -> int:
    row = conn.execute(
        "SELECT id FROM transactions WHERE source_ref = ?", (source_ref,)
    ).fetchone()
    assert row is not None, f"seeded txn {source_ref} not present"
    return int(row["id"])


def test_edit_modal_renders_picker_not_native_select(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    client = web_client_factory()
    txn_id = _txn_id(seeded_web_db, "prov-1")

    resp = client.get(f"/_partial/transactions/{txn_id}/modal")
    assert resp.status_code == 200
    body = resp.text

    assert "data-category-picker" in body
    assert "data-picker-search" in body
    assert 'name="category_id"' in body
    # The native select's empty-option label was unique to it — gone now.
    assert "— no category —" not in body


def test_edit_modal_picker_has_top_chips(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    client = web_client_factory()
    txn_id = _txn_id(seeded_web_db, "prov-1")  # expense txn

    resp = client.get(f"/_partial/transactions/{txn_id}/modal")
    assert resp.status_code == 200
    body = resp.text

    # Groceries is the most-used expense category in seeded_web_db → a chip.
    assert 'data-chip="1"' in body
    assert "Groceries" in body


def test_triage_modal_renders_picker(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    client = web_client_factory()
    txn_id = _txn_id(seeded_web_db, "prov-needs-review")

    resp = client.get(f"/_partial/triage/{txn_id}/modal")
    assert resp.status_code == 200
    body = resp.text

    assert "data-category-picker" in body
    assert "data-picker-search" in body
    assert "— no category —" not in body


def test_edit_modal_lists_every_category_the_row_can_take(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    """GUARD: the picker survives the swap, minus the kinds it must not offer.

    ``prov-1`` is an expense, so the modal offers expense and transfer
    categories. It must not offer income or adjustment ones — that is how
    65 live rows acquired a category from a contradicting kind.
    """
    client = web_client_factory()
    txn_id = _txn_id(seeded_web_db, "prov-1")

    body = client.get(f"/_partial/transactions/{txn_id}/modal").text

    for cat in categories_repo.list_for_kind(seeded_web_db, TransactionKind.EXPENSE):
        assert cat.name in body

    salary = categories_repo.get_by_name(
        seeded_web_db, TransactionKind.INCOME, "Salary"
    )
    assert salary is not None
    assert salary.name not in body


def test_untouched_picker_submission_preserves_category(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    """GUARD: the picker's default form shape (set_category=false) must not wipe."""
    client = web_client_factory()
    txn_id = _txn_id(seeded_web_db, "prov-1")
    before = transactions_repo.get_by_id(seeded_web_db, txn_id)
    assert before is not None and before.category_id is not None

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

    after = transactions_repo.get_by_id(seeded_web_db, txn_id)
    assert after is not None
    assert after.category_id == before.category_id
