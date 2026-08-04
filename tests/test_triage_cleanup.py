"""Cutting the triage queue down to the rows that actually need a human.

Three separate causes had piled 128 rows into the queue:

* **P2P sells filed as spending.** Selling USDT for bolívares is currency
  movement, never an expense, but the Binance ingest writes it as one with no
  category. 97 rows.
* **Salary arriving as an uncategorised deposit.** The owner's rule is
  "Binance deposits over $1,000 are salary", which the rules engine could not
  express — it matched on description text only.
* **2025 rows the owner has decided not to triage.** ``parked`` already
  exists for exactly this (durable deferral, survives re-ingest), but there
  was no way to apply it in bulk and ``finances doctor`` still counted parked
  rows as outstanding.
"""

from __future__ import annotations

import sqlite3
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from finances.db.repos import accounts as accounts_repo
from finances.db.repos import transactions as txn_repo
from finances.domain import integrity
from finances.domain.categorization import CategorizationRequest, suggest
from finances.domain.models import Account, AccountKind, Transaction, TransactionKind
from finances.domain.triage_admin import park_before

WHEN_2025 = datetime(2025, 11, 15, 12, 0, tzinfo=UTC)
WHEN_2026 = datetime(2026, 5, 15, 12, 0, tzinfo=UTC)


@pytest.fixture
def spot(in_memory_db: sqlite3.Connection):
    conn = in_memory_db
    account = accounts_repo.insert(
        conn,
        Account(
            name="Binance Spot",
            kind=AccountKind.CRYPTO_SPOT,
            currency="USDT",
            institution="Binance",
        ),
    )
    assert account.id is not None
    return conn, account


def _row(
    conn: sqlite3.Connection,
    account,
    *,
    amount: str,
    description: str,
    ref: str,
    kind: TransactionKind = TransactionKind.EXPENSE,
    occurred_at: datetime = WHEN_2026,
) -> int:
    txn = txn_repo.insert(
        conn,
        Transaction(
            account_id=account.id,
            occurred_at=occurred_at,
            kind=kind,
            amount=Decimal(amount),
            currency="USDT",
            description=description,
            source="binance",
            source_ref=ref,
        ),
    )
    assert txn.id is not None
    return txn.id


def _category(conn: sqlite3.Connection, name: str) -> int:
    row = conn.execute("SELECT id FROM categories WHERE name = ?", (name,)).fetchone()
    assert row is not None, f"seed missing category {name!r}"
    return int(row[0])


# ---------------------------------------------------------------------------
# A P2P sell is movement, not spending
# ---------------------------------------------------------------------------


def test_a_p2p_sell_is_categorised_as_movement(spot):
    conn, _ = spot
    match = suggest(
        conn,
        CategorizationRequest(
            description="P2P SELL USDT @ 630.51 VES (order 22890001)",
            source="binance",
            amount=Decimal("-100"),
        ),
    )

    assert match is not None
    assert match.category_id == _category(conn, "Internal Transfer")


def test_a_p2p_buy_is_not_swept_up_by_the_sell_rule(spot):
    conn, _ = spot
    match = suggest(
        conn,
        CategorizationRequest(
            description="P2P BUY USDT @ 630.51 VES (order 22890002)",
            source="binance",
            amount=Decimal("100"),
        ),
    )

    assert match is None or match.category_id != _category(conn, "Internal Transfer")


# ---------------------------------------------------------------------------
# Rules can match on amount, so "deposits over $1,000 are salary" is expressible
# ---------------------------------------------------------------------------


def test_a_large_deposit_is_salary(spot):
    conn, _ = spot
    match = suggest(
        conn,
        CategorizationRequest(
            description="Binance deposit USDC",
            source="binance",
            amount=Decimal("2040"),
        ),
    )

    assert match is not None
    assert match.category_id == _category(conn, "Salary")


def test_a_small_deposit_is_not_salary(spot):
    """The owner's rule is a threshold, and it has to hold at the boundary."""
    conn, _ = spot
    match = suggest(
        conn,
        CategorizationRequest(
            description="Binance deposit USDC",
            source="binance",
            amount=Decimal("100"),
        ),
    )

    assert match is None or match.category_id != _category(conn, "Salary")


def test_an_amount_scoped_rule_is_skipped_when_no_amount_is_supplied(spot):
    """Callers that cannot supply an amount must not match by accident."""
    conn, _ = spot
    match = suggest(
        conn,
        CategorizationRequest(description="Binance deposit USDC", source="binance"),
    )

    assert match is None or match.category_id != _category(conn, "Salary")


# ---------------------------------------------------------------------------
# Parking a period the owner has decided not to triage
# ---------------------------------------------------------------------------


def test_park_before_defers_only_earlier_rows(spot):
    conn, account = spot
    old = _row(
        conn, account, amount="-10", description="old", ref="a", occurred_at=WHEN_2025
    )
    recent = _row(
        conn, account, amount="-10", description="new", ref="b", occurred_at=WHEN_2026
    )

    parked = park_before(conn, before=datetime(2026, 1, 1, tzinfo=UTC))

    assert parked == 1
    assert txn_repo.get_by_id(conn, old).parked is True
    assert txn_repo.get_by_id(conn, recent).parked is False


def test_park_before_leaves_categorised_rows_alone(spot):
    """Parking is for outstanding work; a finished row is not outstanding."""
    conn, account = spot
    done = _row(
        conn, account, amount="-10", description="done", ref="c", occurred_at=WHEN_2025
    )
    conn.execute(
        "UPDATE transactions SET category_id = ? WHERE id = ?",
        (_category(conn, "Groceries"), done),
    )

    parked = park_before(conn, before=datetime(2026, 1, 1, tzinfo=UTC))

    assert parked == 0
    assert txn_repo.get_by_id(conn, done).parked is False


def test_park_before_is_idempotent(spot):
    conn, account = spot
    _row(
        conn, account, amount="-10", description="old", ref="d", occurred_at=WHEN_2025
    )

    first = park_before(conn, before=datetime(2026, 1, 1, tzinfo=UTC))
    second = park_before(conn, before=datetime(2026, 1, 1, tzinfo=UTC))

    assert first == 1
    assert second == 0


def test_doctor_stops_counting_parked_rows_as_outstanding(spot):
    """Park is an answer to "do you want to categorise this?" — it is "no"."""
    conn, account = spot
    _row(
        conn, account, amount="-10", description="old", ref="e", occurred_at=WHEN_2025
    )

    before = integrity.run_checks(conn)
    assert any(f.check == "uncategorized_not_flagged" for f in before.findings)

    park_before(conn, before=datetime(2026, 1, 1, tzinfo=UTC))

    after = integrity.run_checks(conn)
    assert not any(f.check == "uncategorized_not_flagged" for f in after.findings)
