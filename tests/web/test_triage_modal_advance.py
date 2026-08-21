"""Hold-position advance + modal-as-response (ADR-012 Amendment 2026-07-26).

The old flow advanced by re-rendering the whole queue and clicking the
first modal trigger left in the DOM. Two consequences the owner hit daily:

* the parked ``<details>`` group renders ABOVE ``queue.items``, so "first
  trigger in the DOM" is a PARKED row whenever any exist — every save
  dropped the owner into an item they had deliberately deferred;
* the advance always landed on position 0, never on the neighbour of the
  item just resolved.

The replacement: the server picks the successor by IDENTITY — the nearest
item that was below the resolved one and is still queued — and returns
that item's modal as the response body. Nothing clicks anything, and the
queue list is not part of the response at all.
"""

from __future__ import annotations

import json
import sqlite3
from datetime import UTC, datetime
from decimal import Decimal

import pytest
from fastapi.testclient import TestClient
from hypothesis import given
from hypothesis import strategies as st

from finances.db.repos import accounts as accounts_repo
from finances.db.repos import categories as categories_repo
from finances.db.repos import transactions as transactions_repo
from finances.domain.models import (
    Account,
    AccountKind,
    Transaction,
    TransactionKind,
)
from finances.web.services.triage import (
    TriageItem,
    TriageType,
    build_queue,
    next_item_after,
)


# ---------------------------------------------------------------------------
# Pure successor selection.
# ---------------------------------------------------------------------------


def _item(item_id: str) -> TriageItem:
    return TriageItem(
        item_id=item_id,
        type=TriageType.CATEGORY,
        sort_key=datetime(2026, 1, 1, tzinfo=UTC),
        bucket=0,
    )


def _queue(*ids: str) -> list[TriageItem]:
    return [_item(i) for i in ids]


def test_middle_item_advances_to_the_one_below() -> None:
    before = _queue("a", "b", "c", "d")
    after = _queue("a", "c", "d")

    assert next_item_after(before, after, "b").item_id == "c"


def test_first_item_advances_to_the_one_below() -> None:
    before = _queue("a", "b", "c")
    after = _queue("b", "c")

    assert next_item_after(before, after, "a").item_id == "b"


def test_last_item_steps_up_to_the_new_last() -> None:
    """Nothing below the bottom row — hold-position falls back one slot."""
    before = _queue("a", "b", "c")
    after = _queue("a", "b")

    assert next_item_after(before, after, "c").item_id == "b"


def test_only_item_advances_to_nothing() -> None:
    assert next_item_after(_queue("a"), [], "a") is None


def test_unknown_id_advances_to_nothing() -> None:
    """The resolved item was not in the list the owner was looking at."""
    assert next_item_after(_queue("a", "b"), _queue("a", "b"), "zz") is None


def test_a_partial_fix_does_not_reopen_the_same_item() -> None:
    """Saving a rate on a row that still lacks a category keeps it queued.

    Re-opening the modal the owner just dismissed reads as a failed save.
    The row keeps its place in the queue for a later pass; the advance
    moves on.
    """
    before = _queue("a", "b", "c")
    after = _queue("b", "a", "c")  # 'b' survived and moved (bucket 1 -> 0)

    assert next_item_after(before, after, "b").item_id == "c"


def test_items_removed_above_the_slot_do_not_shift_the_successor() -> None:
    """A write can evict rows ABOVE the one being resolved.

    Confirming a pair promotes both legs to ``kind='transfer'``, which
    drops them from the CATEGORY surface. Legs are bucket 0/1 and pairs
    are bucket 2, so those two removals always land above the resolved
    slot. Indexing ``after`` with the ``before`` index therefore
    overshoots by one per evicted row and silently skips proposals.
    """
    before = _queue("leg-a", "leg-b", "pair-1", "pair-2", "pair-3")
    after = _queue("pair-2", "pair-3")  # both legs AND pair-1 are gone

    assert next_item_after(before, after, "pair-1").item_id == "pair-2"


def test_successor_is_the_post_write_instance_not_the_snapshot() -> None:
    """The advance must render current state, not the stale snapshot."""
    before = _queue("a", "b")
    fresh = _item("b")
    fresh.bucket = 1  # 'b' mutated while 'a' was being resolved

    result = next_item_after(before, [fresh], "a")

    assert result is fresh


def test_advance_never_returns_a_parked_item() -> None:
    """Parked rows live in ``parked_items``, never in ``items``.

    Selection reads the successor out of the live list only, so parking
    cannot be undone by an advance no matter where the group renders.
    """
    before = _queue("a", "b")
    after = _queue("b")

    result = next_item_after(before, after, "a")
    assert result is not None and result.item_id == "b"


@given(
    size=st.integers(min_value=1, max_value=12),
    index=st.integers(min_value=0, max_value=11),
)
def test_hold_position_property(size: int, index: int) -> None:
    """For a pure removal: land on the slot the resolved item vacated."""
    index = index % size
    ids = [f"i{n}" for n in range(size)]
    before = _queue(*ids)
    resolved = ids[index]
    after = _queue(*[i for i in ids if i != resolved])

    result = next_item_after(before, after, resolved)

    if size == 1:
        assert result is None
    elif index == size - 1:
        assert result.item_id == ids[index - 1]  # stepped up
    else:
        assert result.item_id == ids[index + 1]  # the one below


# ---------------------------------------------------------------------------
# Endpoint contract: the response body IS the next modal.
# ---------------------------------------------------------------------------


@pytest.fixture
def advance_db(web_db: sqlite3.Connection) -> sqlite3.Connection:
    """Four category-only USD rows (ids 1-4) plus one PARKED older row.

    USD keeps ``rates.resolve`` on the native path, so setting a category
    fully resolves a row and drops it out of the queue — which is what
    makes the successor assertions deterministic.

    The parked row is dated EARLIEST on purpose: under the old DOM-order
    advance it rendered first and was always the item the owner landed on.
    """
    cash = accounts_repo.insert(
        web_db, Account(name="Cash USD", kind=AccountKind.CASH, currency="USD")
    )

    def _txn(day: int, ref: str, *, parked: bool = False) -> int:
        row = transactions_repo.insert(
            web_db,
            Transaction(
                account_id=cash.id,
                occurred_at=datetime(2026, 5, day, tzinfo=UTC),
                kind=TransactionKind.EXPENSE,
                amount=Decimal("-10.00"),
                currency="USD",
                description=ref,
                source="cash",
                source_ref=ref,
            ),
        )
        if parked:
            transactions_repo.update(web_db, id=row.id, parked=True)
        return row.id

    for day, ref in ((10, "one"), (11, "two"), (12, "three"), (13, "four")):
        _txn(day, ref)
    _txn(1, "parked-oldest", parked=True)
    return web_db


def _groceries_id(conn: sqlite3.Connection) -> int:
    cat = categories_repo.get_by_name(conn, TransactionKind.EXPENSE, "Groceries")
    assert cat is not None
    return cat.id


def _save(client: TestClient, txn_id: int, category_id: int, **params):
    return client.post(
        f"/_partial/triage/{txn_id}/edit",
        params=params,
        data={
            "set_category": "true",
            "category_id": str(category_id),
            "set_user_rate": "false",
            "user_rate": "",
            "set_notes": "false",
            "notes": "",
        },
    )


def test_save_returns_the_next_items_modal(
    advance_db: sqlite3.Connection, web_client_factory
) -> None:
    """Queue is [1,2,3,4]; resolving 2 must hand back 3's modal."""
    client = web_client_factory()

    resp = _save(client, 2, _groceries_id(advance_db))

    assert resp.status_code == 200, resp.text
    assert 'data-tx-modal="triage"' in resp.text
    assert 'data-tx-id="3"' in resp.text


def test_save_response_is_not_the_queue_partial(
    advance_db: sqlite3.Connection, web_client_factory
) -> None:
    """~400 KB of cards per save is what made the list visibly repaint."""
    client = web_client_factory()

    resp = _save(client, 2, _groceries_id(advance_db))

    assert "data-triage-queue" not in resp.text
    assert "data-triage-bucket-header" not in resp.text


def test_save_on_the_first_item_advances_down_not_to_a_parked_row(
    advance_db: sqlite3.Connection, web_client_factory
) -> None:
    """The regression the owner reported: landing on a deferred item."""
    client = web_client_factory()
    parked_id = advance_db.execute(
        "SELECT id FROM transactions WHERE source_ref = 'parked-oldest'"
    ).fetchone()["id"]

    resp = _save(client, 1, _groceries_id(advance_db))

    assert f'data-tx-id="{parked_id}"' not in resp.text
    assert 'data-tx-id="2"' in resp.text


def test_save_on_the_last_item_steps_up(
    advance_db: sqlite3.Connection, web_client_factory
) -> None:
    client = web_client_factory()

    resp = _save(client, 4, _groceries_id(advance_db))

    assert 'data-tx-id="3"' in resp.text


def test_save_on_the_final_remaining_item_closes_the_modal(
    advance_db: sqlite3.Connection, web_client_factory
) -> None:
    """Empty body clears the host; closeModal is safe only here."""
    client = web_client_factory()
    groceries = _groceries_id(advance_db)
    for txn_id in (1, 2, 3):
        _save(client, txn_id, groceries)

    resp = _save(client, 4, groceries)

    assert resp.status_code == 200, resp.text
    assert resp.text.strip() == ""
    payload = json.loads(resp.headers["HX-Trigger"])
    assert payload["closeModal"] is True
    assert payload["queueDirty"] == {"typeFilter": None}


def test_mid_run_save_must_not_send_closeModal(
    advance_db: sqlite3.Connection, web_client_factory
) -> None:
    """base.html clears #tx-modal-host on close-modal — it would wipe the
    modal this very response is delivering."""
    client = web_client_factory()

    resp = _save(client, 2, _groceries_id(advance_db))

    payload = json.loads(resp.headers["HX-Trigger"])
    assert "closeModal" not in payload
    assert payload["queueDirty"] == {"typeFilter": None}
    assert payload["toast"] == {"level": "success", "message": "Saved"}


def test_park_advances_to_the_next_item(
    advance_db: sqlite3.Connection, web_client_factory
) -> None:
    client = web_client_factory()

    resp = client.post("/_partial/triage/2/park")

    assert resp.status_code == 200, resp.text
    assert 'data-tx-id="3"' in resp.text
    payload = json.loads(resp.headers["HX-Trigger"])
    assert "closeModal" not in payload
    assert payload["queueDirty"] == {"typeFilter": None}


def test_unpark_still_returns_the_queue_partial(
    advance_db: sqlite3.Connection, web_client_factory
) -> None:
    """Unpark is a LIST action with no modal open — it must not advance."""
    client = web_client_factory()
    parked_id = advance_db.execute(
        "SELECT id FROM transactions WHERE source_ref = 'parked-oldest'"
    ).fetchone()["id"]

    resp = client.post(f"/_partial/triage/{parked_id}/unpark")

    assert resp.status_code == 200, resp.text
    assert "data-triage-queue" in resp.text


def test_advance_respects_an_active_type_filter(
    web_db: sqlite3.Connection, web_client_factory
) -> None:
    """Advancing into an item the filter hides would strand the owner.

    Bolívar rows with an empty ``rates`` table, because since ADR-021 a
    rate item is the projection's answer and a USD row is always priced —
    the previous USD fixture produced no rate items at all.
    """
    account = accounts_repo.insert(
        web_db,
        Account(name="Provincial", kind=AccountKind.BANK, currency="VES"),
    )
    cat = categories_repo.get_by_name(web_db, TransactionKind.EXPENSE, "Groceries")
    assert cat is not None

    def _txn(day: int, ref: str, *, category_id, needs_review: bool) -> int:
        return transactions_repo.insert(
            web_db,
            Transaction(
                account_id=account.id,
                occurred_at=datetime(2026, 5, day, tzinfo=UTC),
                kind=TransactionKind.EXPENSE,
                amount=Decimal("-10.00"),
                currency="VES",
                description=ref,
                category_id=category_id,
                source="provincial",
                source_ref=ref,
                needs_review=needs_review,
            ),
        )

    # Two RATE rows (ids 1, 2) and one CATEGORY row (id 3) between them by
    # date — under ?type_filter=rate the successor of 1 must be 2, not 3.
    _txn(10, "rate-a", category_id=cat.id, needs_review=True)
    _txn(12, "rate-b", category_id=cat.id, needs_review=True)
    _txn(11, "cat-only", category_id=None, needs_review=False)

    client = web_client_factory()
    resp = client.post(
        "/_partial/triage/1/edit",
        params={"type_filter": "rate"},
        data={
            "set_category": "false",
            "category_id": "",
            "set_user_rate": "true",
            "user_rate": "1.00",
            "set_notes": "false",
            "notes": "",
        },
    )

    assert resp.status_code == 200, resp.text
    assert 'data-tx-id="2"' in resp.text
    assert 'data-tx-id="3"' not in resp.text
    # The filter must survive into the next modal's own actions...
    assert "type_filter=rate" in resp.text
    # ...and into the deferred queue refresh, which happens long after
    # the request that applied the filter. The chips render outside the
    # swapped region, so their data-active cannot be trusted for this.
    payload = json.loads(resp.headers["HX-Trigger"])
    assert payload["queueDirty"] == {"typeFilter": "rate"}


# ---------------------------------------------------------------------------
# Pair confirm advances the same way.
# ---------------------------------------------------------------------------


@pytest.fixture
def pair_advance_db(web_db: sqlite3.Connection) -> sqlite3.Connection:
    """One pair proposal (bucket 2, sorts last) after two category rows."""
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
        web_db, Account(name="Cash USD", kind=AccountKind.CASH, currency="USD")
    )
    when = datetime(2026, 5, 10, 12, 0, tzinfo=UTC)

    # ids 1, 2 — plain category-only rows (bucket 0).
    for day, ref in ((8, "cat-a"), (9, "cat-b")):
        transactions_repo.insert(
            web_db,
            Transaction(
                account_id=cash.id,
                occurred_at=datetime(2026, 5, day, tzinfo=UTC),
                kind=TransactionKind.EXPENSE,
                amount=Decimal("-10.00"),
                currency="USD",
                description=ref,
                source="cash",
                source_ref=ref,
            ),
        )

    # id 3 — bank deposit; id 4 — matching Binance sell (needs user_rate).
    transactions_repo.insert(
        web_db,
        Transaction(
            account_id=provincial.id,
            occurred_at=when,
            kind=TransactionKind.INCOME,
            amount=Decimal("3650.00"),
            currency="VES",
            description="ABONO P2P sell",
            source="provincial",
            source_ref="bank-deposit-1",
        ),
    )
    transactions_repo.insert(
        web_db,
        Transaction(
            account_id=binance.id,
            occurred_at=when,
            kind=TransactionKind.EXPENSE,
            amount=Decimal("-100.00"),
            currency="USDT",
            description="P2P sell USDT",
            source="binance",
            source_ref="p2p:sell-1",
            user_rate=Decimal("36.50"),
        ),
    )
    return web_db


def test_pair_confirm_advances_like_a_save(
    pair_advance_db: sqlite3.Connection, web_client_factory
) -> None:
    """The pair sits in bucket 2 (last), so confirming steps up."""
    queue = build_queue(pair_advance_db)
    assert queue.items[-1].type == TriageType.PAIR, [
        i.item_id for i in queue.items
    ]
    client = web_client_factory()

    resp = client.post("/_partial/triage/pair/3/4/confirm")

    assert resp.status_code == 200, resp.text
    assert 'data-tx-modal="triage"' in resp.text
    payload = json.loads(resp.headers["HX-Trigger"])
    assert "closeModal" not in payload
    assert payload["queueDirty"]["typeFilter"] is None


@pytest.fixture
def multi_pair_db(web_db: sqlite3.Connection) -> sqlite3.Connection:
    """Three pair proposals whose six legs also sit in the queue.

    Confirming the first pair promotes its two legs to ``transfer``,
    evicting them from the CATEGORY surface — two removals ABOVE the
    resolved pair, because legs are bucket 0 and pairs are bucket 2.
    """
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

    for n in range(3):
        when = datetime(2026, 5, 10 + n, 12, 0, tzinfo=UTC)
        amount = Decimal("100.00") * (n + 1)
        transactions_repo.insert(
            web_db,
            Transaction(
                account_id=provincial.id,
                occurred_at=when,
                kind=TransactionKind.INCOME,
                amount=amount * Decimal("36.5"),
                currency="VES",
                description=f"ABONO P2P sell {n}",
                source="provincial",
                source_ref=f"bank-deposit-{n}",
            ),
        )
        transactions_repo.insert(
            web_db,
            Transaction(
                account_id=binance.id,
                occurred_at=when,
                kind=TransactionKind.EXPENSE,
                amount=-amount,
                currency="USDT",
                description=f"P2P sell USDT {n}",
                source="binance",
                source_ref=f"p2p:sell-{n}",
                user_rate=Decimal("36.50"),
            ),
        )
    return web_db


def test_confirming_a_pair_does_not_skip_the_next_proposal(
    multi_pair_db: sqlite3.Connection, web_client_factory
) -> None:
    """Regression: the two evicted legs must not shift the successor.

    Queue is [6 legs as category rows..., pair 1, pair 2, pair 3].
    Confirming pair 1 removes THREE items, two of them above it. Indexing
    the rebuilt queue with the old position lands on pair 3 and silently
    skips pair 2 — and since the list no longer refreshes mid-run, the
    owner never sees that it happened.
    """
    queue = build_queue(multi_pair_db)
    pairs = [i.item_id for i in queue.items if i.type == TriageType.PAIR]
    assert pairs == ["pair:1:2", "pair:3:4", "pair:5:6"], pairs

    client = web_client_factory()
    resp = client.post("/_partial/triage/pair/1/2/confirm")

    assert resp.status_code == 200, resp.text
    assert 'data-tx-modal="pair"' in resp.text
    assert 'data-deposit-id="3"' in resp.text and 'data-sell-id="4"' in resp.text


# ---------------------------------------------------------------------------
# base.html wiring.
# ---------------------------------------------------------------------------


def _page(web_client_factory) -> str:
    return web_client_factory().get("/triage").text


def test_the_dom_order_advance_is_gone(
    advance_db: sqlite3.Connection, web_client_factory
) -> None:
    """This selector is the parked-jump bug. It must not come back."""
    body = _page(web_client_factory)

    assert "[hx-get*='/modal']" not in body
    assert "[hx-get*=\\'/modal\\']" not in body
    assert "advanceNext" not in body
    assert "advanceQueue" not in body


def test_close_modal_refreshes_a_dirty_queue(
    advance_db: sqlite3.Connection, web_client_factory
) -> None:
    """The list reconciles once, when the run ends — not on every save."""
    body = _page(web_client_factory)

    assert "queueDirty" in body
    assert "/_partial/triage/queue" in body
    assert "htmx.ajax" in body


def test_refresh_filter_comes_from_the_server_not_the_chips(
    advance_db: sqlite3.Connection, web_client_factory
) -> None:
    """data-active is only ever written by a full page load.

    Clicking a chip swaps #triage-queue, which the chip <nav> sits
    outside of — so reading the filter back off the chip would refresh an
    unfiltered queue over the filtered one the owner was working in.
    """
    body = _page(web_client_factory)

    assert "queueFilter" in body
    assert "typeFilter" in body
    assert "data-active=" not in body.split("<body", 1)[1].split("</nav>", 1)[0]


def test_a_dismissed_modal_is_not_resurrected_by_a_late_response(
    advance_db: sqlite3.Connection, web_client_factory
) -> None:
    """Escape during an in-flight save must stay closed.

    The response body is the NEXT modal; swapping it in after the owner
    dismissed the dialog pops a different transaction back up.
    """
    body = _page(web_client_factory)

    assert "modalDismissed" in body
    assert "htmx:before-swap.window" in body
    assert "shouldSwap" in body


def test_the_modal_guards_against_key_repeat_and_double_submit(
    advance_db: sqlite3.Connection, web_client_factory
) -> None:
    """Held Enter would submit each modal the advance swaps in."""
    client = web_client_factory()
    modal = client.get("/_partial/triage/1/modal").text

    assert "$event.repeat" in modal
    assert 'hx-disabled-elt="find button[type=submit]"' in modal


def test_the_pair_modal_takes_focus_when_advanced_into(
    pair_advance_db: sqlite3.Connection, web_client_factory
) -> None:
    """Pairs sort last, so a run normally ENDS by advancing into one."""
    client = web_client_factory()
    modal = client.get("/_partial/triage/pair/3/4/modal").text

    assert 'tabindex="-1"' in modal
    assert "x-init" in modal and ".focus()" in modal


def test_modal_actions_target_the_modal_host(
    advance_db: sqlite3.Connection, web_client_factory
) -> None:
    """Save/park must swap the modal in place, never the queue region."""
    client = web_client_factory()
    modal = client.get("/_partial/triage/1/modal").text

    assert 'hx-target="#tx-modal-host"' in modal
    assert 'hx-target="#triage-queue"' not in modal
