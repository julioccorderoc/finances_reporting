"""Difficulty-first queue ordering (spec §5.5, ADR-012 Amendment).

The owner's ask: "first run MOST of the ones I have, and tackle the
ambiguous ones at the end." Chronological order interleaves one-click rows
with rows requiring recall of an eight-month-old exchange rate. Buckets put
the cheap work first without giving up oldest-first inside each bucket.
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
from finances.web.services.triage import build_queue


@pytest.fixture
def bucket_db(web_db: sqlite3.Connection) -> sqlite3.Connection:
    """A rate-issue row dated EARLIER than a category-only row.

    Under pure oldest-first this returns [rate, category]. Under
    difficulty-first it must return [category, rate]. That inversion is the
    whole point of the change.
    """
    account = accounts_repo.insert(
        web_db, Account(name="Provincial", kind=AccountKind.BANK, currency="VES")
    )
    groceries = categories_repo.get_by_name(
        web_db, TransactionKind.EXPENSE, "Groceries"
    )
    assert groceries is not None

    def _txn(day: int, ref: str, *, category_id, needs_review: bool) -> None:
        transactions_repo.insert(
            web_db,
            Transaction(
                account_id=account.id,
                occurred_at=datetime(2026, 5, day, tzinfo=UTC),
                kind=TransactionKind.EXPENSE,
                amount=Decimal("-100.00"),
                currency="VES",
                description=ref,
                category_id=category_id,
                source="provincial",
                source_ref=ref,
                needs_review=needs_review,
            ),
        )

    # id 1 — OLDER, but a rate issue -> bucket 1
    _txn(1, "old-rate", category_id=groceries.id, needs_review=True)
    # id 2 — NEWER, category only -> bucket 0
    _txn(9, "new-category", category_id=None, needs_review=False)
    # id 3 — both issues -> bucket 1, because a missing rate dominates
    _txn(5, "both", category_id=None, needs_review=True)
    return web_db


def test_difficulty_beats_chronology(bucket_db: sqlite3.Connection) -> None:
    """The two rows that block the sitting lead, oldest of them first.

    The redesign reordered the buckets — 0 category, 1 pairs, 2 priced
    roughly — so the rate-only row now goes last instead of second. The
    principle is unchanged and stronger: an approximate rate does not
    block a sitting at all (criteria A8/D6), so it walks after everything
    that does.
    """
    queue = build_queue(bucket_db)

    # txn:3 and txn:2 both need a category; txn:1 only needs a rate.
    assert [i.item_id for i in queue.items] == ["txn:3", "txn:2", "txn:1"]


def test_bucket_assignment(bucket_db: sqlite3.Connection) -> None:
    by_id = {i.item_id: i for i in build_queue(bucket_db).items}

    assert by_id["txn:2"].bucket == 0          # category only
    assert by_id["txn:1"].bucket == 2          # priced roughly
    assert by_id["txn:3"].bucket == 0          # both -> the category blocks


def test_oldest_first_survives_inside_a_bucket(
    bucket_db: sqlite3.Connection,
) -> None:
    """Plan 1's guarantee must not be lost to the new leading sort key."""
    bucket_0 = [i for i in build_queue(bucket_db).items if i.bucket == 0]

    assert [i.item_id for i in bucket_0] == ["txn:3", "txn:2"]
    assert bucket_0[0].sort_key < bucket_0[1].sort_key


def test_tied_timestamps_still_break_on_item_id(
    web_db: sqlite3.Connection,
) -> None:
    """204 of 243 live items share a timestamp — the tiebreak still matters."""
    account = accounts_repo.insert(
        web_db, Account(name="P", kind=AccountKind.BANK, currency="VES")
    )
    for n in range(3):
        transactions_repo.insert(
            web_db,
            Transaction(
                account_id=account.id,
                occurred_at=datetime(2026, 5, 1, tzinfo=UTC),
                kind=TransactionKind.EXPENSE,
                amount=Decimal("-1.00"),
                currency="VES",
                description=f"tied {n}",
                source="provincial",
                source_ref=f"tied-{n}",
                needs_review=True,
            ),
        )

    assert [i.item_id for i in build_queue(web_db).items] == [
        "txn:1",
        "txn:2",
        "txn:3",
    ]


def test_bucket_counts_sum_to_the_item_count(
    bucket_db: sqlite3.Connection,
) -> None:
    queue = build_queue(bucket_db)

    assert sum(queue.bucket_counts.values()) == len(queue.items)
    assert queue.bucket_counts[0] == 2
    assert queue.bucket_counts[2] == 1
    # And the named counts the header reads agree with them.
    assert queue.category_count == 2
    assert queue.approximate_count == 1
    assert queue.blocking_count == 2


def test_header_renders_counts_inside_the_swapped_region(
    bucket_db: sqlite3.Connection, web_client_factory
) -> None:
    """Counts outside #triage-queue would go stale on every save or park."""
    client: TestClient = web_client_factory()
    html = client.get("/triage").text

    queue_region = html.split('id="triage-queue"', 1)[1]
    # The group heads that have rows, and the header's own answer. The
    # empty group renders nothing at all (A7), so its absence here is the
    # contract rather than a gap.
    assert "Needs a category" in queue_region
    assert "Priced roughly" in queue_region
    assert "Proposed pairs" not in queue_region
    assert "rows need you" in queue_region
    assert "category" in queue_region
    assert "approximate rates" in queue_region


# ---------------------------------------------------------------------------
# The queue partial after a list write.
# ---------------------------------------------------------------------------


def test_a_list_write_answers_with_the_whole_live_queue(
    web_db: sqlite3.Connection, web_client_factory
) -> None:
    """The redesign has no filter chips, so there is no filter to carry.

    This used to pin ``_render_queue_partial`` against a hardcoded
    ``type_filter=None`` swapping an unfiltered queue over a filtered
    one. Rows are grouped by what is wrong with them now and the run
    walks every group, so the honest replacement is: a write from the
    list answers with every live row, and only the resolved one is gone.
    """
    account = accounts_repo.insert(
        web_db, Account(name="Provincial", kind=AccountKind.BANK, currency="VES")
    )
    groceries = categories_repo.get_by_name(
        web_db, TransactionKind.EXPENSE, "Groceries"
    )
    assert groceries is not None

    def _txn(ref: str, *, category_id, needs_review: bool) -> None:
        transactions_repo.insert(
            web_db,
            Transaction(
                account_id=account.id,
                occurred_at=datetime(2026, 5, 1, tzinfo=UTC),
                kind=TransactionKind.EXPENSE,
                amount=Decimal("-50.00"),
                currency="VES",
                description=ref,
                category_id=category_id,
                source="provincial",
                source_ref=ref,
                needs_review=needs_review,
            ),
        )

    _txn("RATE-A-MARKER", category_id=groceries.id, needs_review=True)  # id 1
    _txn("RATE-B-MARKER", category_id=groceries.id, needs_review=True)  # id 2
    _txn("CAT-ONLY-MARKER", category_id=None, needs_review=False)  # id 3

    client: TestClient = web_client_factory()

    resp = client.post("/_partial/triage/bulk-park", data={"ids": "2"})

    assert resp.status_code == 200, resp.text
    # It really is the queue partial, not some other fragment.
    assert "data-triage-row" in resp.text
    assert "RATE-A-MARKER" in resp.text
    assert "CAT-ONLY-MARKER" in resp.text
    assert "RATE-B-MARKER" not in resp.text
