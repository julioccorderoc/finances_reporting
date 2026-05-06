"""Phase 4 — /triage unified queue + pair-confirm modal tests (EPIC-025).

Tests precede implementation per rule-011. Coverage:

* ``finances/web/services/triage.py``
  - ``build_queue`` collects RATE / CATEGORY / PAIR items,
  - merges a single txn with both rate + category issues into one item
    with two badges,
  - sorts oldest-first by ``sort_key``,
  - filters by ``type_filter`` while keeping ``counts`` unfiltered,
  - moves session-skipped items to the bottom,
  - surfaces unreconciled-transfer integrity warnings,
  - excludes transfer/adjustment from CATEGORY items.
  - ``confirm_pair`` calls ``create_transfer`` with the right anchors
    and rejects already-paired ids.
* ``GET /triage`` renders the page with cards.
* ``GET /api/triage`` returns JSON, supports ``?type_filter=``.
* ``POST /api/transfers`` pair-confirm endpoint.
* ``GET /_partial/triage/{id}/modal`` exposes the Save & next variant.
* ``POST /_partial/triage/{id}/edit`` advances the queue + sets
  ``HX-Trigger`` headers.
* ``GET /_partial/triage/pair/{deposit_id}/{sell_id}/modal`` shows
  side-by-side mini-cards + confidence.
* ``POST /_partial/triage/pair/{deposit_id}/{sell_id}/confirm``
  creates the transfer.
* ``POST /_partial/triage/skip/{item_id}`` floats the item.
* Triage card partial uses /_partial/triage/{id}/modal as modal_url.
* REGRESSION: /transactions card_transaction.html still uses the
  Phase 3 modal endpoint (no Phase 4 leakage).
* Sanity: Phase 2 pages still respond 200.
"""

from __future__ import annotations

import sqlite3
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from finances.db.repos import accounts as accounts_repo
from finances.db.repos import categories as categories_repo
from finances.db.repos import rates as rates_repo
from finances.db.repos import transactions as transactions_repo
from finances.domain.models import (
    Account,
    AccountKind,
    Rate,
    Transaction,
    TransactionKind,
)


# ---------------------------------------------------------------------------
# Local triage fixture: deterministic shape that exercises every code path.
# ---------------------------------------------------------------------------


def _aware(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=UTC)
    return dt


@pytest.fixture
def triage_seeded_db(web_db: sqlite3.Connection) -> sqlite3.Connection:
    """Build a queue exercising:

    * 2 needs_review (RATE) txns,
    * 2 category-null (CATEGORY) txns — one of which also has
      needs_review=1 (so it must merge into one item with 2 badges),
    * a Provincial deposit + matching Binance sell (PAIR proposal),
    * a transfer leg with transfer_id but no paired peer (unreconciled
      → integrity warning).
    """
    today = datetime.now(tz=UTC)
    ago_3 = today - timedelta(days=3)
    ago_10 = today - timedelta(days=10)
    ago_30 = today - timedelta(days=30)
    legacy = datetime(2010, 1, 1, tzinfo=UTC)

    # Accounts
    provincial = accounts_repo.insert(
        web_db,
        Account(
            name="Provincial",
            kind=AccountKind.BANK,
            currency="VES",
            institution="Provincial",
        ),
    )
    binance = accounts_repo.insert(
        web_db,
        Account(
            name="Binance Spot",
            kind=AccountKind.CRYPTO_SPOT,
            currency="USDT",
            institution="Binance",
        ),
    )
    cash = accounts_repo.insert(
        web_db,
        Account(name="Cash USD", kind=AccountKind.CASH, currency="USD"),
    )

    # P2P median for ~today so projection has a rate.
    rates_repo.upsert(
        web_db,
        Rate(
            as_of_date=date.today(),
            base="USDT",
            quote="VES",
            rate=Decimal("36.50"),
            source="binance_p2p_median",
        ),
    )

    # Categories
    food = categories_repo.get_by_name(
        web_db, TransactionKind.EXPENSE, "Groceries"
    )
    assert food is not None

    # 1) RATE-only item: needs_review with category set.
    rate_only = transactions_repo.insert(
        web_db,
        Transaction(
            account_id=provincial.id,
            occurred_at=_aware(legacy),
            kind=TransactionKind.EXPENSE,
            amount=Decimal("123.45"),
            currency="VES",
            description="LEGACY needs review",
            category_id=food.id,
            source="provincial",
            source_ref="rate-only-1",
            needs_review=True,
        ),
    )

    # 2) RATE + CATEGORY merged item (needs_review and category_id NULL).
    rate_and_category = transactions_repo.insert(
        web_db,
        Transaction(
            account_id=provincial.id,
            occurred_at=_aware(ago_30),
            kind=TransactionKind.EXPENSE,
            amount=Decimal("234.56"),
            currency="VES",
            description="legacy uncategorised",
            source="provincial",
            source_ref="rate-and-cat-1",
            needs_review=True,
        ),
    )

    # 3) CATEGORY-only item: category_id NULL, kind=expense, has rate (today).
    cat_only = transactions_repo.insert(
        web_db,
        Transaction(
            account_id=provincial.id,
            occurred_at=_aware(ago_10),
            kind=TransactionKind.EXPENSE,
            amount=Decimal("365.00"),
            currency="VES",
            description="needs category",
            source="provincial",
            source_ref="cat-only-1",
        ),
    )

    # 4) RATE-only item #2 (later than #1, earlier than ago_3): older->oldest first.
    rate_only_2 = transactions_repo.insert(
        web_db,
        Transaction(
            account_id=provincial.id,
            occurred_at=_aware(ago_3),
            kind=TransactionKind.EXPENSE,
            amount=Decimal("987.65"),
            currency="VES",
            description="recent needs rate",
            category_id=food.id,
            source="provincial",
            source_ref="rate-only-2",
            needs_review=True,
        ),
    )

    # 5) Transfer-kind, category_id NULL: must be EXCLUDED from CATEGORY items.
    transferish = transactions_repo.insert(
        web_db,
        Transaction(
            account_id=cash.id,
            occurred_at=_aware(ago_10),
            kind=TransactionKind.TRANSFER,
            amount=Decimal("-50.00"),
            currency="USD",
            description="lone transfer leg",
            transfer_id="orphan-transfer-id-1",
            source="manual",
            source_ref="orphan-1",
        ),
    )

    # 6) PAIR candidate: provincial bank deposit (income, +amount).
    deposit = transactions_repo.insert(
        web_db,
        Transaction(
            account_id=provincial.id,
            occurred_at=_aware(today - timedelta(days=1)),
            kind=TransactionKind.INCOME,
            amount=Decimal("36500.00"),
            currency="VES",
            description="ABONO P2P sell",
            category_id=food.id,  # category to keep it OUT of CATEGORY items
            source="provincial",
            source_ref="bank-deposit-1",
        ),
    )

    # 7) PAIR candidate: matching Binance sell (negative, with user_rate).
    #    expected = abs(amount) * user_rate = 1000 * 36.50 = 36500.
    sell = transactions_repo.insert(
        web_db,
        Transaction(
            account_id=binance.id,
            occurred_at=_aware(today - timedelta(days=1)),
            kind=TransactionKind.EXPENSE,
            amount=Decimal("-1000.00"),
            currency="USDT",
            description="P2P sell USDT",
            category_id=food.id,
            user_rate=Decimal("36.50"),
            source="binance",
            source_ref="binance-sell-1",
        ),
    )

    return web_db


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _txn_id(conn: sqlite3.Connection, source_ref: str) -> int:
    row = conn.execute(
        "SELECT id FROM transactions WHERE source_ref = ?", (source_ref,)
    ).fetchone()
    assert row is not None, f"missing seeded txn {source_ref}"
    return int(row["id"])


# ---------------------------------------------------------------------------
# services/triage.py — unit tests against build_queue
# ---------------------------------------------------------------------------


def test_triage_queue_includes_rate_items(
    triage_seeded_db: sqlite3.Connection,
) -> None:
    from finances.web.services.triage import TriageType, build_queue

    queue = build_queue(triage_seeded_db)

    rate_only_id = _txn_id(triage_seeded_db, "rate-only-1")
    rate_only_2_id = _txn_id(triage_seeded_db, "rate-only-2")

    rate_card_ids = {
        item.txn_card.id
        for item in queue.items
        if item.type == TriageType.RATE and item.txn_card is not None
    }

    # Every needs_review row surfaces as a RATE-typed item (a merged
    # rate+category row also satisfies the RATE half).
    assert rate_only_id in rate_card_ids
    assert rate_only_2_id in rate_card_ids


def test_triage_queue_includes_category_items(
    triage_seeded_db: sqlite3.Connection,
) -> None:
    from finances.web.services.triage import TriageType, build_queue

    queue = build_queue(triage_seeded_db)

    cat_only_id = _txn_id(triage_seeded_db, "cat-only-1")
    merged_id = _txn_id(triage_seeded_db, "rate-and-cat-1")

    cat_card_ids = {
        item.txn_card.id
        for item in queue.items
        if item.type == TriageType.CATEGORY and item.txn_card is not None
    }

    assert cat_only_id in cat_card_ids
    # The merged one is a different item type (single merged row).
    assert merged_id not in cat_card_ids


def test_triage_queue_excludes_transfer_kind_from_category_items(
    triage_seeded_db: sqlite3.Connection,
) -> None:
    from finances.web.services.triage import build_queue

    queue = build_queue(triage_seeded_db)

    transferish_id = _txn_id(triage_seeded_db, "orphan-1")

    surfaced_ids = {
        item.txn_card.id
        for item in queue.items
        if item.txn_card is not None
    }
    # Lone transfer leg (category_id NULL but kind=transfer) must NOT show up.
    assert transferish_id not in surfaced_ids


def test_triage_queue_merges_rate_and_category_into_one_item(
    triage_seeded_db: sqlite3.Connection,
) -> None:
    from finances.web.services.triage import build_queue

    queue = build_queue(triage_seeded_db)

    merged_id = _txn_id(triage_seeded_db, "rate-and-cat-1")
    matches = [
        item
        for item in queue.items
        if item.txn_card is not None and item.txn_card.id == merged_id
    ]
    assert len(matches) == 1, "merged txn must appear ONCE, not twice"

    item = matches[0]
    badges = set(item.txn_issue_badges)
    assert "rate" in badges and "category" in badges


def test_triage_queue_includes_pair_items_from_strategy(
    triage_seeded_db: sqlite3.Connection,
) -> None:
    from finances.web.services.triage import TriageType, build_queue

    queue = build_queue(triage_seeded_db)

    pair_items = [item for item in queue.items if item.type == TriageType.PAIR]
    assert len(pair_items) >= 1

    deposit_id = _txn_id(triage_seeded_db, "bank-deposit-1")
    sell_id = _txn_id(triage_seeded_db, "binance-sell-1")

    proposal = pair_items[0].pair_proposal
    assert proposal is not None
    assert proposal.deposit.id == deposit_id
    assert proposal.sell.id == sell_id


def test_triage_queue_sorted_oldest_first(
    triage_seeded_db: sqlite3.Connection,
) -> None:
    from finances.web.services.triage import build_queue

    queue = build_queue(triage_seeded_db)
    sort_keys = [item.sort_key for item in queue.items]
    assert sort_keys == sorted(sort_keys), (
        "items must come back oldest-first by sort_key"
    )


def test_triage_queue_counts_unfiltered(
    triage_seeded_db: sqlite3.Connection,
) -> None:
    from finances.web.services.triage import TriageType, build_queue

    queue_all = build_queue(triage_seeded_db)
    queue_rate = build_queue(triage_seeded_db, type_filter=TriageType.RATE)

    # Counts dict reflects total per type regardless of filter.
    assert queue_all.counts == queue_rate.counts
    assert queue_all.counts[TriageType.RATE] >= 2
    assert queue_all.counts[TriageType.CATEGORY] >= 1
    assert queue_all.counts[TriageType.PAIR] >= 1


def test_triage_queue_type_filter(
    triage_seeded_db: sqlite3.Connection,
) -> None:
    from finances.web.services.triage import TriageType, build_queue

    queue = build_queue(triage_seeded_db, type_filter=TriageType.RATE)

    assert all(item.type == TriageType.RATE for item in queue.items)
    assert len(queue.items) == queue.counts[TriageType.RATE]


def test_triage_queue_skipped_items_at_bottom(
    triage_seeded_db: sqlite3.Connection,
) -> None:
    from finances.web.services.triage import build_queue

    queue = build_queue(triage_seeded_db)
    assert queue.items, "fixture must produce at least one item"
    first = queue.items[0]

    skipped = build_queue(triage_seeded_db, skipped_ids={first.item_id})

    # The previously-first item is NOT first anymore (it sank).
    assert skipped.items[0].item_id != first.item_id
    # And it appears at the END of the returned queue.
    assert skipped.items[-1].item_id == first.item_id


def test_triage_integrity_warnings_when_unreconciled_transfers_exist(
    triage_seeded_db: sqlite3.Connection,
) -> None:
    from finances.web.services.triage import build_queue

    queue = build_queue(triage_seeded_db)
    assert any(
        "orphan-transfer-id-1" in w or "transfer" in w.lower()
        for w in queue.integrity_warnings
    ), f"expected an integrity warning, got {queue.integrity_warnings!r}"


# ---------------------------------------------------------------------------
# Routes — page + JSON.
# ---------------------------------------------------------------------------


def test_triage_page_renders(
    triage_seeded_db: sqlite3.Connection, web_client_factory
) -> None:
    client: TestClient = web_client_factory()
    resp = client.get("/triage")
    assert resp.status_code == 200, resp.text
    body = resp.text
    assert "Triage" in body
    # At least one card-row.
    assert "data-card-row" in body or "data-pair-row" in body


def test_api_triage_returns_json_queue(
    triage_seeded_db: sqlite3.Connection, web_client_factory
) -> None:
    client = web_client_factory()
    resp = client.get("/api/triage")
    assert resp.status_code == 200, resp.text
    payload = resp.json()
    assert "items" in payload and "counts" in payload
    # counts is a dict keyed by type strings.
    assert "rate" in payload["counts"]
    assert "category" in payload["counts"]
    assert "pair" in payload["counts"]


def test_api_triage_supports_type_filter(
    triage_seeded_db: sqlite3.Connection, web_client_factory
) -> None:
    client = web_client_factory()
    resp = client.get("/api/triage", params={"type_filter": "pair"})
    assert resp.status_code == 200, resp.text
    payload = resp.json()
    for item in payload["items"]:
        assert item["type"] == "pair"


# ---------------------------------------------------------------------------
# POST /api/transfers — pair-confirm endpoint.
# ---------------------------------------------------------------------------


def test_post_transfers_pairs_two_transactions(
    triage_seeded_db: sqlite3.Connection, web_client_factory
) -> None:
    client = web_client_factory()
    deposit_id = _txn_id(triage_seeded_db, "bank-deposit-1")
    sell_id = _txn_id(triage_seeded_db, "binance-sell-1")

    resp = client.post(
        "/api/transfers",
        json={"deposit_id": deposit_id, "sell_id": sell_id},
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body.get("transfer_id")

    # Both rows have transfer_id set in the DB.
    rows = triage_seeded_db.execute(
        "SELECT id, transfer_id FROM transactions WHERE id IN (?, ?)",
        (deposit_id, sell_id),
    ).fetchall()
    transfer_ids = {row["transfer_id"] for row in rows}
    assert len(transfer_ids) == 1 and None not in transfer_ids


def test_post_transfers_400_for_already_paired(
    triage_seeded_db: sqlite3.Connection, web_client_factory
) -> None:
    client = web_client_factory()
    deposit_id = _txn_id(triage_seeded_db, "bank-deposit-1")
    sell_id = _txn_id(triage_seeded_db, "binance-sell-1")

    # First call succeeds.
    r1 = client.post(
        "/api/transfers",
        json={"deposit_id": deposit_id, "sell_id": sell_id},
    )
    assert r1.status_code == 200, r1.text

    # Second call must fail (both already paired).
    r2 = client.post(
        "/api/transfers",
        json={"deposit_id": deposit_id, "sell_id": sell_id},
    )
    assert 400 <= r2.status_code < 500


def test_post_transfers_404_for_unknown_id(
    triage_seeded_db: sqlite3.Connection, web_client_factory
) -> None:
    client = web_client_factory()
    deposit_id = _txn_id(triage_seeded_db, "bank-deposit-1")

    resp = client.post(
        "/api/transfers",
        json={"deposit_id": deposit_id, "sell_id": 999_999},
    )
    assert resp.status_code == 404


# ---------------------------------------------------------------------------
# Modal partials.
# ---------------------------------------------------------------------------


def test_triage_modal_endpoint_renders_save_and_next_button(
    triage_seeded_db: sqlite3.Connection, web_client_factory
) -> None:
    client = web_client_factory()
    rate_only_id = _txn_id(triage_seeded_db, "rate-only-1")

    resp = client.get(f"/_partial/triage/{rate_only_id}/modal")
    assert resp.status_code == 200, resp.text
    body = resp.text

    assert "Save" in body and "next" in body.lower()
    assert "Skip" in body
    assert "Cancel" in body


def test_triage_modal_form_post_advances_queue(
    triage_seeded_db: sqlite3.Connection, web_client_factory
) -> None:
    client = web_client_factory()
    rate_only_id = _txn_id(triage_seeded_db, "rate-only-1")

    resp = client.post(
        f"/_partial/triage/{rate_only_id}/edit",
        data={
            "set_category": "true",
            "category_id": "",
            "set_user_rate": "true",
            "user_rate": "36.5",
        },
    )
    assert resp.status_code == 200, resp.text
    # Returned partial = the triage_queue refresh.
    assert "triage-queue" in resp.text or "data-card-row" in resp.text or "data-pair-row" in resp.text


def test_triage_modal_save_triggers_close_and_advance_headers(
    triage_seeded_db: sqlite3.Connection, web_client_factory
) -> None:
    client = web_client_factory()
    rate_only_id = _txn_id(triage_seeded_db, "rate-only-1")

    resp = client.post(
        f"/_partial/triage/{rate_only_id}/edit",
        data={
            "set_category": "true",
            "category_id": "",
            "set_user_rate": "true",
            "user_rate": "36.5",
        },
    )
    assert resp.status_code == 200, resp.text
    trigger = resp.headers.get("HX-Trigger", "")
    assert "closeModal" in trigger
    assert "advanceQueue" in trigger


# ---------------------------------------------------------------------------
# Pair-confirm modal flow.
# ---------------------------------------------------------------------------


def test_triage_pair_modal_renders_two_cards_and_confidence(
    triage_seeded_db: sqlite3.Connection, web_client_factory
) -> None:
    client = web_client_factory()
    deposit_id = _txn_id(triage_seeded_db, "bank-deposit-1")
    sell_id = _txn_id(triage_seeded_db, "binance-sell-1")

    resp = client.get(f"/_partial/triage/pair/{deposit_id}/{sell_id}/modal")
    assert resp.status_code == 200, resp.text
    body = resp.text

    # Both descriptions present.
    assert "ABONO P2P sell" in body
    assert "P2P sell USDT" in body
    # Confidence indicator (numeric/string).
    assert "confidence" in body.lower() or "Confidence" in body


def test_triage_pair_confirm_creates_transfer(
    triage_seeded_db: sqlite3.Connection, web_client_factory
) -> None:
    client = web_client_factory()
    deposit_id = _txn_id(triage_seeded_db, "bank-deposit-1")
    sell_id = _txn_id(triage_seeded_db, "binance-sell-1")

    resp = client.post(
        f"/_partial/triage/pair/{deposit_id}/{sell_id}/confirm"
    )
    assert resp.status_code == 200, resp.text

    rows = triage_seeded_db.execute(
        "SELECT id, transfer_id FROM transactions WHERE id IN (?, ?)",
        (deposit_id, sell_id),
    ).fetchall()
    transfer_ids = {row["transfer_id"] for row in rows}
    assert len(transfer_ids) == 1 and None not in transfer_ids


# ---------------------------------------------------------------------------
# Skip-store endpoint.
# ---------------------------------------------------------------------------


def test_triage_skip_endpoint_pushes_item_to_skip_set(
    triage_seeded_db: sqlite3.Connection, web_client_factory
) -> None:
    client = web_client_factory()
    rate_only_id = _txn_id(triage_seeded_db, "rate-only-1")
    item_id = f"txn:{rate_only_id}"

    # Confirm it's currently first (oldest needs_review legacy date).
    pre = client.get("/api/triage").json()
    assert pre["items"], "queue must be non-empty pre-skip"

    resp = client.post(f"/_partial/triage/skip/{item_id}")
    assert resp.status_code == 200, resp.text

    # Subsequent queue must have the item at the bottom.
    post = client.get("/api/triage").json()
    item_ids = [it["item_id"] for it in post["items"]]
    if item_id in item_ids:
        assert item_ids[-1] == item_id


# ---------------------------------------------------------------------------
# Card partial wiring.
# ---------------------------------------------------------------------------


def test_triage_card_partial_uses_canonical_card_with_triage_modal_url(
    triage_seeded_db: sqlite3.Connection, web_client_factory
) -> None:
    """Triage card partial must point its modal-trigger at the triage modal."""
    client = web_client_factory()
    resp = client.get("/triage")
    assert resp.status_code == 200, resp.text
    body = resp.text
    # At least one /_partial/triage/{id}/modal hx-get appears.
    assert "/_partial/triage/" in body
    assert "/modal" in body


def test_phase_3_modal_still_uses_transactions_endpoint(
    triage_seeded_db: sqlite3.Connection, web_client_factory
) -> None:
    """Regression: /transactions cards still point at the Phase 3 modal."""
    client = web_client_factory()
    resp = client.get("/transactions", params={"date_from": "2000-01-01"})
    assert resp.status_code == 200, resp.text
    body = resp.text
    assert "/_partial/transactions/" in body
    assert "/modal" in body


# ---------------------------------------------------------------------------
# Phase 2 sanity.
# ---------------------------------------------------------------------------


def test_full_phase_2_pages_still_render(
    triage_seeded_db: sqlite3.Connection, web_client_factory
) -> None:
    client = web_client_factory()

    r = client.get("/")
    assert r.status_code == 200

    r = client.get("/transactions", params={"date_from": "2000-01-01"})
    assert r.status_code == 200

    r = client.get("/monthly", params={"range_preset": "3m"})
    assert r.status_code == 200

    r = client.get("/accounts")
    assert r.status_code == 200

    r = client.get("/rates")
    assert r.status_code == 200


# ---------------------------------------------------------------------------
# confirm_pair (service-level) — direct unit test.
# ---------------------------------------------------------------------------


def test_confirm_pair_creates_transfer(
    triage_seeded_db: sqlite3.Connection,
) -> None:
    from finances.web.services.triage import confirm_pair

    deposit_id = _txn_id(triage_seeded_db, "bank-deposit-1")
    sell_id = _txn_id(triage_seeded_db, "binance-sell-1")

    result = confirm_pair(
        triage_seeded_db, deposit_id=deposit_id, sell_id=sell_id
    )

    assert "transfer_id" in result
    rows = triage_seeded_db.execute(
        "SELECT transfer_id FROM transactions WHERE id IN (?, ?)",
        (deposit_id, sell_id),
    ).fetchall()
    transfer_ids = {row["transfer_id"] for row in rows}
    assert len(transfer_ids) == 1 and None not in transfer_ids


def test_confirm_pair_raises_for_already_paired(
    triage_seeded_db: sqlite3.Connection,
) -> None:
    from finances.web.services.triage import confirm_pair

    deposit_id = _txn_id(triage_seeded_db, "bank-deposit-1")
    sell_id = _txn_id(triage_seeded_db, "binance-sell-1")

    confirm_pair(triage_seeded_db, deposit_id=deposit_id, sell_id=sell_id)

    with pytest.raises(ValueError):
        confirm_pair(
            triage_seeded_db, deposit_id=deposit_id, sell_id=sell_id
        )
