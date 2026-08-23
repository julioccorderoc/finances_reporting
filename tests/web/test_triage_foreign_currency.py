"""A row in a currency the ladder has no tier for is a *rate* item.

The companion to ``tests/test_rates_currency_scope.py`` on the surface
that consumes the resolver. Before the ADR-021 §2.5 guard a COP row was
divided by a bolívar rate, so it priced "fine" and the queue never asked
about it — the worst outcome available: a confident wrong number, silently.

It must now behave exactly like the one state the design already renders
(criterion D5): unpriceable, no dollar figure, bucket 2, non-blocking.
"""

from __future__ import annotations

import sqlite3
from datetime import UTC, date, datetime
from decimal import Decimal

import pytest

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
from finances.web.services.triage import build_queue


DAY = datetime(2026, 5, 20, 12, 0, tzinfo=UTC)


@pytest.fixture
def cop_db(web_db: sqlite3.Connection) -> sqlite3.Connection:
    """One categorised COP expense, with the VES ladder fully stocked."""
    for base, quote, source in (
        ("USDT", "VES", "binance_p2p_realized"),
        ("USDT", "VES", "binance_p2p_median"),
        ("USD", "VES", "bcv"),
    ):
        rates_repo.upsert(
            web_db,
            Rate(
                as_of_date=DAY.date(),
                base=base,
                quote=quote,
                rate=Decimal("165.40"),
                source=source,
            ),
        )
    account = accounts_repo.insert(
        web_db, Account(name="Bancolombia", kind=AccountKind.BANK, currency="COP")
    )
    groceries = categories_repo.get_by_name(
        web_db, TransactionKind.EXPENSE, "Groceries"
    )
    assert groceries is not None
    transactions_repo.insert(
        web_db,
        Transaction(
            account_id=account.id,
            occurred_at=DAY,
            kind=TransactionKind.EXPENSE,
            amount=Decimal("-120000.00"),
            currency="COP",
            description="MERCADO",
            category_id=groceries.id,
            source="manual",
            source_ref="cop-1",
        ),
    )
    return web_db


def test_a_cop_row_is_queued_as_an_unpriceable_rate_item(
    cop_db: sqlite3.Connection,
) -> None:
    queue = build_queue(cop_db)

    assert [i.item_id for i in queue.items] == ["txn:1"]
    item = queue.items[0]
    assert item.bucket == 2
    assert item.needs.rate is True
    assert item.needs.cat is False


def test_the_queue_shows_no_dollar_figure_for_a_cop_row(
    cop_db: sqlite3.Connection,
) -> None:
    """A bolívar rate would have reported $725.51 for 120 000 pesos."""
    item = build_queue(cop_db).items[0]

    assert item.txn_card.amount_usd is None
    assert item.txn_card.approximate is False
    assert item.rough is None
