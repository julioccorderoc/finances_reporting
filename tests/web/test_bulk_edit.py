"""WP4 — POST /api/transactions/bulk-edit (tests precede impl per rule-011).

Contract:
* Pydantic ``BulkEditRequest`` — ids: list[int] (min length 1),
  category_id: int | None (None = explicit bulk clear),
* handler loops the sanctioned ``transactions_repo.update()`` per id
  inside ONE DB transaction (rule-012 — no parallel UPDATE SQL),
* unknown txn id → 404 and the whole batch rolls back (all-or-nothing),
* unknown category id → 422, empty ids → 422,
* JSON response {"updated": N} + HX-Trigger toast header (WP2 contract),
* needs_review untouched (derived-only, never a manual toggle).

The rule-012 tripwire test passes pre-impl (guard); everything else
fails first because the route does not exist yet (404 vs expected).
"""

from __future__ import annotations

import json
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


def _transport_id(conn: sqlite3.Connection) -> int:
    cat = categories_repo.get_by_name(conn, TransactionKind.EXPENSE, "Transport")
    assert cat is not None and cat.id is not None
    return cat.id


def test_bulk_assign_happy_path(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    client = web_client_factory()
    ids = [_txn_id(seeded_web_db, r) for r in ("prov-1", "prov-2", "cash-1")]
    target = _transport_id(seeded_web_db)

    resp = client.post(
        "/api/transactions/bulk-edit", json={"ids": ids, "category_id": target}
    )

    assert resp.status_code == 200, resp.text
    assert resp.json() == {"updated": 3}
    for txn_id in ids:
        txn = transactions_repo.get_by_id(seeded_web_db, txn_id)
        assert txn is not None and txn.category_id == target


def test_bulk_edit_sends_hx_trigger_toast(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    client = web_client_factory()
    ids = [_txn_id(seeded_web_db, "prov-1")]

    resp = client.post(
        "/api/transactions/bulk-edit",
        json={"ids": ids, "category_id": _transport_id(seeded_web_db)},
    )

    assert resp.status_code == 200
    trigger = json.loads(resp.headers["HX-Trigger"])
    assert trigger == {"toast": {"level": "success", "message": "1 updated"}}


def test_bulk_edit_empty_ids_is_422(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    client = web_client_factory()

    resp = client.post(
        "/api/transactions/bulk-edit", json={"ids": [], "category_id": 1}
    )

    assert resp.status_code == 422


def test_bulk_edit_unknown_txn_rolls_back_whole_batch(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    client = web_client_factory()
    good = _txn_id(seeded_web_db, "prov-1")
    before = transactions_repo.get_by_id(seeded_web_db, good)
    assert before is not None
    target = _transport_id(seeded_web_db)
    assert target != before.category_id

    resp = client.post(
        "/api/transactions/bulk-edit",
        json={"ids": [good, 999_999], "category_id": target},
    )

    assert resp.status_code == 404
    after = transactions_repo.get_by_id(seeded_web_db, good)
    assert after is not None
    assert after.category_id == before.category_id  # rolled back, all-or-nothing


def test_bulk_edit_unknown_category_is_422(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    client = web_client_factory()
    good = _txn_id(seeded_web_db, "prov-1")

    resp = client.post(
        "/api/transactions/bulk-edit", json={"ids": [good], "category_id": 999_999}
    )

    assert resp.status_code == 422


def test_bulk_clear_with_null_category(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    client = web_client_factory()
    good = _txn_id(seeded_web_db, "prov-1")
    before = transactions_repo.get_by_id(seeded_web_db, good)
    assert before is not None and before.category_id is not None

    resp = client.post(
        "/api/transactions/bulk-edit", json={"ids": [good], "category_id": None}
    )

    assert resp.status_code == 200
    after = transactions_repo.get_by_id(seeded_web_db, good)
    assert after is not None and after.category_id is None


def test_bulk_edit_never_touches_needs_review(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    client = web_client_factory()
    flagged = _txn_id(seeded_web_db, "prov-needs-review")

    resp = client.post(
        "/api/transactions/bulk-edit",
        json={"ids": [flagged], "category_id": _transport_id(seeded_web_db)},
    )

    assert resp.status_code == 200
    after = transactions_repo.get_by_id(seeded_web_db, flagged)
    assert after is not None and after.needs_review is True  # still derived, still flagged


def test_bulk_edit_rejects_extra_fields(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    client = web_client_factory()
    good = _txn_id(seeded_web_db, "prov-1")

    resp = client.post(
        "/api/transactions/bulk-edit",
        json={"ids": [good], "category_id": 1, "needs_review": False},
    )

    assert resp.status_code == 422  # extra=forbid; needs_review is never a manual toggle


def test_web_router_contains_no_raw_transaction_update_sql() -> None:
    """GUARD (rule-012 tripwire): the API router never writes UPDATE SQL."""
    import inspect

    from finances.web.routers import api as api_module

    src = inspect.getsource(api_module)
    assert "UPDATE transactions" not in src
