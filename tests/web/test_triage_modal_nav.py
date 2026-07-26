"""Prev/next navigation in the triage modal (ADR-012 Amendment 2026-07-21).

That amendment promised an action row of ``[← →] [Park] [Cancel] [Save &
next]``; only ``Save & next`` was ever built. The 2026-07-26 amendment
superseded the clause's *rationale* -- it assumed a saved item stays in
the queue, and it does not -- but the arrows themselves were never built.

Semantics decided with the owner: ← is **positional**, the item above me
in the live queue, not a history stack. A history stack would reach rows
already resolved, and saving from one ends the run: ``next_item_after``
returns ``None`` for a ``resolved_id`` absent from ``before``, which is
the exhausted-queue signal.

There is no new endpoint. The neighbour is known at render time, so an
arrow simply points at that item's existing modal URL, and renders
``disabled`` when there is no neighbour in that direction.
"""

from __future__ import annotations

import re
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
from finances.web.routers.partials import _modal_url_for
from finances.web.services.triage import (
    PairProposal,
    TriageItem,
    TriageType,
    neighbours_of,
    next_item_after,
)
from finances.web.services.transactions_query import TransactionCard


# ---------------------------------------------------------------------------
# Pure neighbour selection.
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


def _ids(pair) -> tuple[str | None, str | None]:
    prev, nxt = pair
    return (
        None if prev is None else prev.item_id,
        None if nxt is None else nxt.item_id,
    )


def test_a_middle_item_has_both_neighbours() -> None:
    assert _ids(neighbours_of(_queue("a", "b", "c"), "b")) == ("a", "c")


def test_the_first_item_has_no_previous() -> None:
    assert _ids(neighbours_of(_queue("a", "b", "c"), "a")) == (None, "b")


def test_the_last_item_has_no_next() -> None:
    assert _ids(neighbours_of(_queue("a", "b", "c"), "c")) == ("b", None)


def test_a_lone_item_has_neither() -> None:
    assert _ids(neighbours_of(_queue("a"), "a")) == (None, None)


def test_an_unknown_item_has_neither() -> None:
    """The open modal is not in this queue — arrows go dead, not wrong."""
    assert _ids(neighbours_of(_queue("a", "b"), "zz")) == (None, None)


def test_an_empty_queue_has_neither() -> None:
    assert _ids(neighbours_of([], "a")) == (None, None)


def test_neighbours_are_the_live_instances() -> None:
    """Same rule as the advance: render current state, not a snapshot."""
    items = _queue("a", "b", "c")
    prev, nxt = neighbours_of(items, "b")

    assert prev is items[0]
    assert nxt is items[2]


@given(
    size=st.integers(min_value=1, max_value=12),
    index=st.integers(min_value=0, max_value=11),
)
def test_neighbours_are_the_adjacent_slots(size: int, index: int) -> None:
    index = index % size
    ids = [f"i{n}" for n in range(size)]

    prev, nxt = _ids(neighbours_of(_queue(*ids), ids[index]))

    assert prev == (ids[index - 1] if index > 0 else None)
    assert nxt == (ids[index + 1] if index < size - 1 else None)


def test_the_advance_still_falls_back_past_a_gap() -> None:
    """Guard on the refactor: navigation walks one slot, the advance hunts.

    ``next_item_after`` must keep scanning past rows a write removed;
    ``neighbours_of`` must not, because nothing was removed.
    """
    before = _queue("a", "b", "c", "d")
    after = _queue("a", "d")

    assert next_item_after(before, after, "b").item_id == "d"


# ---------------------------------------------------------------------------
# Modal URLs.
# ---------------------------------------------------------------------------


def _pair_item(deposit_id: int, sell_id: int) -> TriageItem:
    def _card(txn_id: int) -> TransactionCard:
        return TransactionCard(
            id=txn_id,
            occurred_at=datetime(2026, 1, 1, tzinfo=UTC),
            account_name="Provincial",
            kind="transfer",
            description="leg",
            category_name=None,
            amount_native=Decimal("-1.00"),
            currency="VES",
            amount_usd=None,
            rate_source="needs_review",
            is_bcv_fallback=False,
            needs_review=True,
            notes=None,
        )

    return TriageItem(
        item_id=f"pair:{deposit_id}:{sell_id}",
        type=TriageType.PAIR,
        sort_key=datetime(2026, 1, 1, tzinfo=UTC),
        bucket=2,
        pair_proposal=PairProposal(
            proposal_id=f"{deposit_id}:{sell_id}",
            deposit=_card(deposit_id),
            sell=_card(sell_id),
            confidence=1.0,
            details={
                "bank_transaction_id": deposit_id,
                "binance_transaction_id": sell_id,
            },
        ),
    )


def test_a_txn_item_points_at_the_txn_modal() -> None:
    assert _modal_url_for(_item("txn:7"), None) == "/_partial/triage/7/modal"


def test_a_pair_item_points_at_the_pair_modal() -> None:
    assert (
        _modal_url_for(_pair_item(4, 9), None)
        == "/_partial/triage/pair/4/9/modal"
    )


def test_the_active_filter_rides_along() -> None:
    """An arrow must land in the same filtered queue the owner is working."""
    assert (
        _modal_url_for(_item("txn:7"), "rate")
        == "/_partial/triage/7/modal?type_filter=rate"
    )
    assert (
        _modal_url_for(_pair_item(4, 9), "pair")
        == "/_partial/triage/pair/4/9/modal?type_filter=pair"
    )


# ---------------------------------------------------------------------------
# Endpoint contract.
# ---------------------------------------------------------------------------


@pytest.fixture
def nav_db(web_db: sqlite3.Connection) -> sqlite3.Connection:
    """Four category-only USD rows (ids 1-4) and one PARKED older row.

    Same shape as the advance suite's fixture: USD keeps ``rates.resolve``
    on the native path, so the queue order is exactly 1,2,3,4 and the
    parked row is never a navigation target.
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


def _nav_button(html: str, which: str) -> str:
    """Attribute text of the arrow button itself.

    The marker also appears inside the keydown handler's
    ``[data-nav-prev]:not([disabled])`` selector, which sits earlier in
    the document, so match only an occurrence that is actually inside a
    ``<button`` tag.
    """
    marker = f"data-nav-{which}"
    assert marker in html, f"{which} arrow not rendered"

    at = html.find(marker)
    while at != -1:
        start = html.rfind("<button", 0, at)
        if start != -1 and ">" not in html[start:at]:
            return html[start : html.index(">", at)]
        at = html.find(marker, at + 1)

    raise AssertionError(f"{which} arrow marker never appears inside a <button")


def _is_disabled(attrs: str) -> bool:
    """The bare ``disabled`` attribute, not Tailwind's ``disabled:`` variant."""
    return re.search(r"\sdisabled(?=[\s>]|$)", attrs) is not None


def _groceries_id(conn: sqlite3.Connection) -> int:
    cat = categories_repo.get_by_name(conn, TransactionKind.EXPENSE, "Groceries")
    assert cat is not None
    return cat.id


def test_a_middle_modal_offers_both_arrows(
    nav_db: sqlite3.Connection, web_client_factory
) -> None:
    client: TestClient = web_client_factory()

    body = client.get("/_partial/triage/2/modal").text

    assert "/_partial/triage/1/modal" in _nav_button(body, "prev")
    assert "/_partial/triage/3/modal" in _nav_button(body, "next")
    assert not _is_disabled(_nav_button(body, "prev"))
    assert not _is_disabled(_nav_button(body, "next"))


def test_the_first_modal_disables_prev(
    nav_db: sqlite3.Connection, web_client_factory
) -> None:
    """Item 1 is the top of the live queue — the parked row is not above it."""
    client: TestClient = web_client_factory()

    body = client.get("/_partial/triage/1/modal").text

    assert _is_disabled(_nav_button(body, "prev"))
    assert "/_partial/triage/2/modal" in _nav_button(body, "next")


def test_the_last_modal_disables_next(
    nav_db: sqlite3.Connection, web_client_factory
) -> None:
    client: TestClient = web_client_factory()

    body = client.get("/_partial/triage/4/modal").text

    assert "/_partial/triage/3/modal" in _nav_button(body, "prev")
    assert _is_disabled(_nav_button(body, "next"))


def test_arrows_never_target_a_parked_row(
    nav_db: sqlite3.Connection, web_client_factory
) -> None:
    """Parked rows live outside ``queue.items``; navigation reads items."""
    client: TestClient = web_client_factory()
    parked_id = nav_db.execute(
        "SELECT id FROM transactions WHERE source_ref = 'parked-oldest'"
    ).fetchone()["id"]

    body = client.get("/_partial/triage/1/modal").text

    assert f"/_partial/triage/{parked_id}/modal" not in body


def test_arrow_urls_carry_the_active_filter(
    nav_db: sqlite3.Connection, web_client_factory
) -> None:
    client: TestClient = web_client_factory()

    body = client.get(
        "/_partial/triage/2/modal", params={"type_filter": "category"}
    ).text

    assert "/_partial/triage/1/modal?type_filter=category" in _nav_button(
        body, "prev"
    )
    assert "/_partial/triage/3/modal?type_filter=category" in _nav_button(
        body, "next"
    )


def test_the_advanced_into_modal_has_fresh_arrows(
    nav_db: sqlite3.Connection, web_client_factory
) -> None:
    """Resolving 2 advances to 3, whose prev must be 1 — 2 is gone."""
    client: TestClient = web_client_factory()

    resp = client.post(
        "/_partial/triage/2/edit",
        data={
            "set_category": "true",
            "category_id": str(_groceries_id(nav_db)),
            "set_user_rate": "false",
            "user_rate": "",
            "set_notes": "false",
            "notes": "",
        },
    )

    assert 'data-tx-id="3"' in resp.text
    assert "/_partial/triage/1/modal" in _nav_button(resp.text, "prev")
    assert "/_partial/triage/2/modal" not in resp.text
    assert "/_partial/triage/4/modal" in _nav_button(resp.text, "next")


def test_the_advance_response_is_still_only_a_modal(
    nav_db: sqlite3.Connection, web_client_factory
) -> None:
    """Arrows must not drag the queue list back into a save response."""
    client: TestClient = web_client_factory()

    resp = client.post(
        "/_partial/triage/2/edit",
        data={
            "set_category": "true",
            "category_id": str(_groceries_id(nav_db)),
            "set_user_rate": "false",
            "user_rate": "",
            "set_notes": "false",
            "notes": "",
        },
    )

    assert "data-triage-queue" not in resp.text
    assert "data-triage-filter" not in resp.text


# ---------------------------------------------------------------------------
# Client wiring.
# ---------------------------------------------------------------------------


def test_the_arrows_navigate_through_the_shared_handler(
    nav_db: sqlite3.Connection, web_client_factory
) -> None:
    """Pins the JS quoting, not just the handler name.

    ``{{ url | tojson }}`` emits double quotes, which terminate the
    double-quoted ``@click`` attribute and truncate the expression to
    ``navigateModal(`` — Alpine then throws SyntaxError at runtime while
    a substring assertion on "navigateModal" still passes.
    """
    client: TestClient = web_client_factory()

    body = client.get("/_partial/triage/2/modal").text

    assert "navigateModal('/_partial/triage/1/modal')" in _nav_button(body, "prev")
    assert "navigateModal('/_partial/triage/3/modal')" in _nav_button(body, "next")
    assert '"' not in _nav_button(body, "prev").split("@click=", 1)[1][1:].split(
        '"', 1
    )[0]


def test_arrow_keys_click_the_arrow_buttons(
    nav_db: sqlite3.Connection, web_client_factory
) -> None:
    """Keyboard parity with Enter (save) and s (park), which also click.

    Going through the button rather than re-inlining the URL means the
    disabled state at the ends is honoured in one place.
    """
    client: TestClient = web_client_factory()

    body = client.get("/_partial/triage/2/modal").text

    assert "ArrowLeft" in body
    assert "ArrowRight" in body
    assert "[data-nav-prev]:not([disabled])" in body
    assert "[data-nav-next]:not([disabled])" in body
    # The existing isTyping() guard keeps arrows out of the rate field.
    assert "isTyping" in body


def test_navigation_warns_only_when_the_modal_is_dirty(
    nav_db: sqlite3.Connection, web_client_factory
) -> None:
    """The guard reads the hidden set_* sentinels, not ``catDirty``.

    ``catDirty`` in the form's x-data is dead — the WP4 picker tracks its
    own ``dirty`` in its own scope and writes ``set_category``. Reading
    the sentinels catches all three fields wherever they are tracked.
    """
    client: TestClient = web_client_factory()

    page = client.get("/triage").text

    assert "modalDirty" in page
    assert "set_category" in page
    assert "set_user_rate" in page
    assert "set_notes" in page
    assert "Discard unsaved changes?" in page


def test_navigation_reuses_the_existing_ajax_swap(
    nav_db: sqlite3.Connection, web_client_factory
) -> None:
    """Same htmx.ajax into #tx-modal-host the queue refresh already uses."""
    client: TestClient = web_client_factory()

    page = client.get("/triage").text
    handler = page[page.index("navigateModal") : page.index("navigateModal") + 600]

    assert "htmx.ajax" in handler
    assert "tx-modal-host" in handler
