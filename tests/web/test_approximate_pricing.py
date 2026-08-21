"""ADR-021 across the surfaces that read the resolver.

The resolver's new terminal branch is only useful if the answer travels.
``rate_source`` is the carrier — the ``_nearest`` suffix *is* the
provenance — and every surface derives from it in exactly one place:

* ``TransactionCard.approximate`` (the pinned cross-session contract
  field) and ``TransactionCard.rate``,
* ``ConsolidatedRow.is_approximate`` (in ``test_reports_consolidated_usd``),
* the triage modal's rate panel, which now offers the nearest rate per
  tier instead of an empty row,
* net worth, which bars a BCV answer and must keep barring ``bcv_nearest``.
"""

from __future__ import annotations

import sqlite3
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

import pytest

from finances.db.repos import accounts as accounts_repo
from finances.db.repos import rates as rates_repo
from finances.db.repos import transactions as transactions_repo
from finances.domain.models import (
    Account,
    AccountKind,
    Rate,
    Transaction,
    TransactionKind,
)
from finances.web.services.net_worth import usdt_value
from finances.web.services.rates_view import rates_for_day
from finances.web.services.transactions_query import (
    TXN_QUERY_BASE,
    _project_card,
    _row_to_transaction,
)

DAY = date(2026, 4, 23)
WHEN = datetime(2026, 4, 23, 12, 0, tzinfo=UTC)


def _seed_rate(
    conn: sqlite3.Connection,
    source: str,
    day: date,
    rate: str,
    *,
    base: str = "USDT",
    quote: str = "VES",
) -> None:
    rates_repo.upsert(
        conn,
        Rate(
            as_of_date=day, base=base, quote=quote, rate=Decimal(rate), source=source
        ),
    )


@pytest.fixture
def priced_db(web_db: sqlite3.Connection) -> sqlite3.Connection:
    accounts_repo.insert(
        web_db,
        Account(
            name="Provincial",
            kind=AccountKind.BANK,
            currency="VES",
            institution="Provincial",
        ),
    )
    accounts_repo.insert(
        web_db, Account(name="Cash USD", kind=AccountKind.CASH, currency="USD")
    )
    return web_db


def _card(conn: sqlite3.Connection, txn_id: int):
    row = conn.execute(TXN_QUERY_BASE + " WHERE t.id = ?", (txn_id,)).fetchone()
    return _project_card(
        conn,
        _row_to_transaction(row),
        account_name=row["account_name"] or "",
        category_name=row["category_name"],
    )


def _insert(
    conn: sqlite3.Connection,
    *,
    amount: str,
    currency: str = "VES",
    account_id: int = 1,
    ref: str = "ref-1",
    **overrides,
) -> int:
    txn = transactions_repo.insert(
        conn,
        Transaction(
            account_id=account_id,
            occurred_at=WHEN,
            kind=TransactionKind.EXPENSE,
            amount=Decimal(amount),
            currency=currency,
            description="COM.PAGO bodega",
            source="provincial",
            source_ref=ref,
            **overrides,
        ),
    )
    assert txn.id is not None
    return txn.id


# ---------------------------------------------------------------------------
# TransactionCard — the pinned contract.
# ---------------------------------------------------------------------------


def test_card_marks_an_approximated_row(priced_db: sqlite3.Connection) -> None:
    _seed_rate(priced_db, "bcv", DAY - timedelta(days=90), "36.00", base="USD")
    txn_id = _insert(priced_db, amount="-3600.00")

    card = _card(priced_db, txn_id)

    assert card.approximate is True
    assert card.rate_source == "bcv_nearest"
    assert card.rate == Decimal("36.00")
    assert card.amount_usd == Decimal("-100")
    assert card.is_bcv_fallback is True


def test_card_does_not_mark_an_in_window_row(priced_db: sqlite3.Connection) -> None:
    _seed_rate(priced_db, "binance_p2p_median", DAY, "800.00")
    txn_id = _insert(priced_db, amount="-8000.00")

    card = _card(priced_db, txn_id)

    assert card.approximate is False
    assert card.rate == Decimal("800.00")
    assert card.rate_source == "binance_p2p_median"


def test_card_of_an_unpriceable_row(priced_db: sqlite3.Connection) -> None:
    """No rates at all: the one case that still has no dollar figure."""
    txn_id = _insert(priced_db, amount="-3600.00")

    card = _card(priced_db, txn_id)

    assert card.amount_usd is None
    assert card.rate is None
    assert card.approximate is False
    assert card.needs_review is True


def test_card_of_a_native_row_is_never_approximate(
    priced_db: sqlite3.Connection,
) -> None:
    txn_id = _insert(
        priced_db, amount="-12.50", currency="USD", account_id=2, ref="cash-1"
    )

    card = _card(priced_db, txn_id)

    assert card.rate_source == "native_usd"
    assert card.approximate is False
    assert card.amount_usd == Decimal("-12.50")


def test_card_carries_source_ref(priced_db: sqlite3.Connection) -> None:
    """The queue needs it to name a row; the projection is where it lives."""
    _seed_rate(priced_db, "binance_p2p_median", DAY, "800.00")
    txn_id = _insert(priced_db, amount="-8000.00", ref="prov:2026-04-23:1")

    assert _card(priced_db, txn_id).source_ref == "prov:2026-04-23:1"


# ---------------------------------------------------------------------------
# The modal's rate panel.
# ---------------------------------------------------------------------------


def _series(conn: sqlite3.Connection, winning_source: str):
    return rates_for_day(
        conn,
        day=DAY,
        winning_source=winning_source,
        amount_native=Decimal("20000.00"),
        currency="VES",
    )


def _tier(conn: sqlite3.Connection, source: str, winning_source: str = "bcv"):
    return next(s for s in _series(conn, winning_source) if s.source == source)


def test_panel_marks_the_winner_through_the_nearest_suffix(
    web_db: sqlite3.Connection,
) -> None:
    """``bcv_nearest`` is still BCV winning — the panel must say so.

    It strips ``_carry`` already; a suffix it does not know about leaves the
    panel showing three tiers and no winner.
    """
    _seed_rate(web_db, "bcv", DAY - timedelta(days=90), "36.00", base="USD")

    winners = [s for s in _series(web_db, "bcv_nearest") if s.is_winner]

    assert [s.source for s in winners] == ["bcv"]


def test_panel_offers_an_expired_tier_as_an_approximation(
    web_db: sqlite3.Connection,
) -> None:
    """Design criterion D9: each suggestion shows its resulting USD.

    ADR-016 §2.1 suppressed the figure for an expired tier, on the grounds
    that the chain had refused the rate. ADR-021 is what changed: the chain
    no longer refuses it, it approximates with it, and the panel exists so
    the owner can take that number or type a better one.
    """
    stale = DAY - timedelta(days=60)
    _seed_rate(web_db, "binance_p2p_median", stale, "633.52")

    median = _tier(web_db, "binance_p2p_median")

    assert median.is_expired is True
    assert median.is_approximate is True
    assert median.amount_usd == Decimal("20000.00") / Decimal("633.52")
    assert median.age_days == 60


def test_panel_offers_a_future_rate_with_a_negative_age(
    web_db: sqlite3.Connection,
) -> None:
    """"BCV, 3 days later" needs a direction, and a signed age is it."""
    _seed_rate(web_db, "bcv", DAY + timedelta(days=3), "36.00", base="USD")

    bcv = _tier(web_db, "bcv")

    assert bcv.as_of_date == DAY + timedelta(days=3)
    assert bcv.age_days == -3
    assert bcv.is_approximate is True
    assert bcv.is_carry is False
    assert bcv.amount_usd == Decimal("20000.00") / Decimal("36.00")


def test_panel_leaves_an_empty_tier_empty(web_db: sqlite3.Connection) -> None:
    realized = _tier(web_db, "binance_p2p_realized")

    assert realized.rate is None
    assert realized.amount_usd is None
    assert realized.is_approximate is False


def test_panel_does_not_mark_an_in_window_carry_as_approximate(
    web_db: sqlite3.Connection,
) -> None:
    _seed_rate(web_db, "binance_p2p_median", DAY - timedelta(days=3), "800.00")

    median = _tier(web_db, "binance_p2p_median")

    assert median.is_carry is True
    assert median.is_approximate is False
    assert median.is_expired is False


def test_panel_applies_the_realized_cap_it_used_to_ignore(
    web_db: sqlite3.Connection,
) -> None:
    """The realized cap lived inline in ``resolve``, so the panel never saw it.

    A 30-day-old realized rate is one the resolver refuses; the panel used
    to price from it as if it had won.
    """
    _seed_rate(
        web_db, "binance_p2p_realized", DAY - timedelta(days=30), "690.00"
    )

    realized = _tier(web_db, "binance_p2p_realized")

    assert realized.is_expired is True
    assert realized.is_approximate is True


# ---------------------------------------------------------------------------
# Net worth — the headline that must never lean on BCV.
# ---------------------------------------------------------------------------


def test_net_worth_still_refuses_an_approximated_bcv_rate(
    web_db: sqlite3.Connection,
) -> None:
    _seed_rate(web_db, "bcv", DAY - timedelta(days=200), "36.00", base="USD")

    assert (
        usdt_value(
            web_db, currency="VES", amount_native=Decimal("3600"), as_of_date=DAY
        )
        is None
    )


def test_net_worth_accepts_an_approximated_market_rate(
    web_db: sqlite3.Connection,
) -> None:
    """Only BCV is barred. An approximation of a market tier is a number the
    tile can show, and showing nothing was the old defect."""
    _seed_rate(
        web_db, "binance_p2p_median", DAY - timedelta(days=200), "800.00"
    )

    assert usdt_value(
        web_db, currency="VES", amount_native=Decimal("8000"), as_of_date=DAY
    ) == Decimal("10")
