"""Filter-chip counts: truthful totals, and fresh after a triage run.

Two defects this locks down (both listed as "Not addressed here" in the
ADR-012 Amendment 2026-07-26):

* The "All" chip and the header count both rendered ``queue.items |
  length``, which under an active filter is the FILTERED count — so
  "All" disagreed with ``queue.counts``, which is always computed on the
  unfiltered set.
* The chip ``<nav>`` rendered OUTSIDE ``#triage-queue``. Every swap in
  the triage screen (chip click, unpark, the deferred ``queueDirty``
  refresh) targets ``#triage-queue``, so the chip counts and their
  ``data-active`` were frozen at page load and went stale the moment the
  owner resolved anything.

The fix is placement, not machinery: the chip row and the counts line
move inside the swapped region, the same way the bucket-count header
already did. No out-of-band swap, no second request, no JS change.
``data-active`` becomes accurate again as a side effect but stays
display-only — the refresh filter still travels in the ``queueDirty``
payload, never read back off the DOM.
"""

from __future__ import annotations

import sqlite3
from datetime import UTC, datetime
from decimal import Decimal

import pytest
from fastapi.testclient import TestClient

from finances.db.repos import accounts as accounts_repo
from finances.db.repos import categories as categories_repo
from finances.db.repos import transactions as transactions_repo
from finances.domain.models import (
    Account,
    AccountKind,
    Transaction,
    TransactionKind,
)
from finances.web.services.triage import TriageType, build_queue


# ---------------------------------------------------------------------------
# Fixture: a queue whose filtered and unfiltered counts differ.
# ---------------------------------------------------------------------------


@pytest.fixture
def chips_db(web_db: sqlite3.Connection) -> sqlite3.Connection:
    """Three live items: 2 CATEGORY + 1 RATE. No pairs.

    The two category rows are USD, so setting a category fully resolves
    them (``rates.resolve`` takes the native path) and they leave the
    queue — which is what makes the post-run count assertions
    deterministic.

    The rate row is VES dated 2010 with a category already set, so it
    lands on the RATE surface only: no rate exists that far back, and it
    is not also missing a category.
    """
    cash = accounts_repo.insert(
        web_db, Account(name="Cash USD", kind=AccountKind.CASH, currency="USD")
    )
    provincial = accounts_repo.insert(
        web_db,
        Account(
            name="Provincial",
            kind=AccountKind.BANK,
            currency="VES",
            institution="Provincial",
        ),
    )
    groceries = categories_repo.get_by_name(
        web_db, TransactionKind.EXPENSE, "Groceries"
    )
    assert groceries is not None

    for day, ref in ((10, "cat-one"), (11, "cat-two")):
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

    transactions_repo.insert(
        web_db,
        Transaction(
            account_id=provincial.id,
            occurred_at=datetime(2010, 1, 1, tzinfo=UTC),
            kind=TransactionKind.EXPENSE,
            amount=Decimal("-999.00"),
            currency="VES",
            description="LEGACY needs review",
            category_id=groceries.id,
            source="provincial",
            source_ref="rate-one",
            needs_review=True,
        ),
    )
    return web_db


def _groceries_id(conn: sqlite3.Connection) -> int:
    cat = categories_repo.get_by_name(conn, TransactionKind.EXPENSE, "Groceries")
    assert cat is not None
    return cat.id


def _cat_txn_id(conn: sqlite3.Connection, ref: str) -> int:
    row = conn.execute(
        "SELECT id FROM transactions WHERE source_ref = ?", (ref,)
    ).fetchone()
    assert row is not None
    return int(row["id"])


def _chip_attrs(html: str, value: str) -> str:
    """Return the attribute text of one chip button.

    Slices from the chip's own marker attribute back to the opening
    ``<button`` and forward to the closing ``>`` so an assertion about
    one chip cannot accidentally read a neighbour's.
    """
    marker = f'data-filter-chip="{value}"'
    assert marker in html, f"chip {value!r} not rendered"
    at = html.index(marker)
    start = html.rindex("<button", 0, at)
    end = html.index(">", at)
    return html[start:end]


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


# ---------------------------------------------------------------------------
# TriageQueue.total — the unfiltered size, available under any filter.
# ---------------------------------------------------------------------------


def test_total_is_the_unfiltered_count(chips_db: sqlite3.Connection) -> None:
    queue = build_queue(chips_db)

    assert queue.total == 3
    assert queue.total == len(queue.items)


def test_total_ignores_the_type_filter(chips_db: sqlite3.Connection) -> None:
    """The whole point: "All" must not shrink when a chip is active."""
    queue = build_queue(chips_db, type_filter=TriageType.CATEGORY)

    assert len(queue.items) == 2
    assert queue.total == 3


def test_total_agrees_with_counts(chips_db: sqlite3.Connection) -> None:
    """``counts`` is per-type on the unfiltered set; ``total`` is its sum."""
    queue = build_queue(chips_db, type_filter=TriageType.RATE)

    assert queue.total == sum(queue.counts.values())


def test_total_excludes_parked_rows(chips_db: sqlite3.Connection) -> None:
    """Parked is a separate surface with its own count, never part of items."""
    transactions_repo.update(
        chips_db, id=_cat_txn_id(chips_db, "cat-one"), parked=True
    )

    queue = build_queue(chips_db)

    assert queue.total == 2
    assert queue.parked_count == 1


# ---------------------------------------------------------------------------
# Page render: the chips tell the truth under a filter.
# ---------------------------------------------------------------------------


def test_all_chip_shows_the_true_total_under_a_filter(
    chips_db: sqlite3.Connection, web_client_factory
) -> None:
    client = web_client_factory()

    body = client.get("/triage", params={"type_filter": "category"}).text

    assert "All (3)" in body
    assert "All (2)" not in body


def test_per_type_chips_are_unchanged_by_the_filter(
    chips_db: sqlite3.Connection, web_client_factory
) -> None:
    client = web_client_factory()

    body = client.get("/triage", params={"type_filter": "rate"}).text

    assert "Rates (1)" in body
    assert "Categories (2)" in body
    assert "Pairs (0)" in body


def test_counts_line_shows_filtered_of_total(
    chips_db: sqlite3.Connection, web_client_factory
) -> None:
    client = web_client_factory()

    body = client.get("/triage", params={"type_filter": "category"}).text

    assert "Showing 2 of 3 items" in body


def test_counts_line_is_a_plain_total_when_unfiltered(
    chips_db: sqlite3.Connection, web_client_factory
) -> None:
    client = web_client_factory()

    body = client.get("/triage").text

    assert "3 items" in body
    assert "Showing" not in body


def test_the_header_carries_no_count_of_its_own(
    chips_db: sqlite3.Connection, web_client_factory
) -> None:
    """A number outside ``#triage-queue`` can never be refreshed.

    The h1 count was the original lie: it rendered the FILTERED length
    while claiming to describe the queue. It is gone, not relocated
    outside the swap.
    """
    client = web_client_factory()

    body = client.get("/triage", params={"type_filter": "category"}).text
    head = body.split('id="triage-queue"', 1)[0]

    assert "items" not in head.split("<h1", 1)[1]


# ---------------------------------------------------------------------------
# Placement: the chips live inside the swapped region.
# ---------------------------------------------------------------------------


def test_the_chip_row_is_inside_the_swapped_region(
    chips_db: sqlite3.Connection, web_client_factory
) -> None:
    """Every triage swap targets #triage-queue; the chips must ride along."""
    client = web_client_factory()

    body = client.get("/_partial/triage/queue").text

    assert "data-triage-filter" in body
    assert 'data-filter-chip="all"' in body


def test_the_page_renders_exactly_one_chip_row(
    chips_db: sqlite3.Connection, web_client_factory
) -> None:
    """Moving the nav must not leave a duplicate behind in the shell."""
    client = web_client_factory()

    body = client.get("/triage").text

    assert body.count("data-triage-filter") == 1


def test_the_partial_marks_the_active_chip(
    chips_db: sqlite3.Connection, web_client_factory
) -> None:
    """``data-active`` is display-only, but it must stop being a lie."""
    client = web_client_factory()

    body = client.get("/_partial/triage/queue", params={"type_filter": "rate"}).text

    assert 'data-active="true"' in _chip_attrs(body, "rate")
    assert 'data-active="false"' in _chip_attrs(body, "all")


def test_the_partial_marks_all_active_when_unfiltered(
    chips_db: sqlite3.Connection, web_client_factory
) -> None:
    client = web_client_factory()

    body = client.get("/_partial/triage/queue").text

    assert 'data-active="true"' in _chip_attrs(body, "all")
    assert 'data-active="false"' in _chip_attrs(body, "category")


def test_chips_carry_the_active_filter_in_their_urls(
    chips_db: sqlite3.Connection, web_client_factory
) -> None:
    """A chip swap must be able to switch filters, including back to all."""
    client = web_client_factory()

    body = client.get("/_partial/triage/queue", params={"type_filter": "rate"}).text

    assert 'hx-get="/_partial/triage/queue"' in _chip_attrs(body, "all")
    assert (
        'hx-get="/_partial/triage/queue?type_filter=category"'
        in _chip_attrs(body, "category")
    )


# ---------------------------------------------------------------------------
# Freshness: counts move after a triage run.
# ---------------------------------------------------------------------------


def test_chip_counts_refresh_after_a_resolved_item(
    chips_db: sqlite3.Connection, web_client_factory
) -> None:
    """The deferred queueDirty refresh must bring the chips with it."""
    client = web_client_factory()

    _save(client, _cat_txn_id(chips_db, "cat-one"), _groceries_id(chips_db))
    body = client.get("/_partial/triage/queue").text

    assert "All (2)" in body
    assert "Categories (1)" in body
    assert "2 items" in body


def test_chip_counts_refresh_under_an_active_filter(
    chips_db: sqlite3.Connection, web_client_factory
) -> None:
    """Resolving under a filter updates "All" too, not just the active chip."""
    client = web_client_factory()

    _save(
        client,
        _cat_txn_id(chips_db, "cat-one"),
        _groceries_id(chips_db),
        type_filter="category",
    )
    body = client.get(
        "/_partial/triage/queue", params={"type_filter": "category"}
    ).text

    assert "All (2)" in body
    assert "Categories (1)" in body
    assert "Showing 1 of 2 items" in body


def test_unpark_refresh_carries_the_chips(
    chips_db: sqlite3.Connection, web_client_factory
) -> None:
    """Unpark is the other route that swaps the queue partial directly."""
    client = web_client_factory()
    parked_id = _cat_txn_id(chips_db, "cat-one")
    transactions_repo.update(chips_db, id=parked_id, parked=True)

    body = client.post(f"/_partial/triage/{parked_id}/unpark").text

    assert "All (3)" in body
    assert 'data-filter-chip="all"' in body


def test_a_save_response_still_carries_no_chip_row(
    chips_db: sqlite3.Connection, web_client_factory
) -> None:
    """The save response is the next modal — the queue is not in it.

    Sibling of ``test_save_response_is_not_the_queue_partial``: the chips
    now live inside the queue partial, so they must not sneak the 400 KB
    list back into a per-save response.
    """
    client = web_client_factory()

    resp = _save(client, _cat_txn_id(chips_db, "cat-one"), _groceries_id(chips_db))

    assert "data-triage-filter" not in resp.text
    assert "data-triage-queue" not in resp.text
