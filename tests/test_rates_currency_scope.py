"""The fallback ladder is a *bolívar* ladder (ADR-021 §2.5).

``resolve``'s tiers are ``USDT/VES`` and ``USD/VES``. Nothing used to
compare a tier's quote currency against the transaction's own, so a row
denominated in any other non-native currency — COP, EUR — was divided by
a bolívar rate and reported as a confident dollar figure.

Inert on the live ledger, which holds only VES/USDT/USDC/USD, and found
while seeding a genuinely unpriceable row for the Triage redesign's
criterion D5. These tests pin the guard: outside the ladder's own quote
currency the chain resolves to *unpriceable*, which is the same state an
empty rates table produces and which the triage surface already renders.
"""

from __future__ import annotations

import sqlite3
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import Any

from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from finances.db.repos import rates as rates_repo
from finances.domain import money
from finances.domain import rates as rates_engine
from finances.domain.models import Rate, Transaction
from tests.conftest import RateFactory, TransactionFactory


DAY = date(2026, 5, 20)

#: Every quote currency the ladder actually holds a tier for. Read off the
#: tier table rather than spelled out, so a future tier in a new currency
#: joins these tests instead of silently escaping them.
LADDER_QUOTES = frozenset(quote for _base, quote, _source in rates_engine._FALLBACK_TIERS)

#: A currency the ledger has no rate pair for. Colombian pesos are the real
#: case: the owner's ledger could grow one, and a COP row must not be
#: priced by a VES rate.
FOREIGN = "COP"


def _txn_on(day: date, **overrides: Any) -> Transaction:
    return TransactionFactory.build(
        occurred_at=datetime(day.year, day.month, day.day, 12, 0, tzinfo=UTC),
        **overrides,
    )


def _upsert_rate(conn: sqlite3.Connection, **overrides: Any) -> Rate:
    # ``upsert``, not ``insert``: hypothesis reuses the function-scoped
    # connection across examples, and a second insert of the same tier/day
    # trips the ``(as_of_date, base, quote, source)`` unique index.
    return rates_repo.upsert(conn, RateFactory.build(**overrides))


def _seed_full_ladder(conn: sqlite3.Connection, day: date = DAY) -> None:
    """One rate on every tier, on ``day`` — the ladder at its strongest."""
    for base, quote, source in rates_engine._FALLBACK_TIERS:
        _upsert_rate(
            conn,
            as_of_date=day,
            base=base,
            quote=quote,
            source=source,
            rate=Decimal("165.40"),
        )


# ---------------------------------------------------------------------------
# The guard itself.
# ---------------------------------------------------------------------------


def test_a_foreign_currency_row_is_not_priced_by_the_ves_ladder(
    in_memory_db: sqlite3.Connection,
) -> None:
    _seed_full_ladder(in_memory_db)
    txn = _txn_on(DAY, currency=FOREIGN)

    rate, source = rates_engine.resolve(in_memory_db, txn)

    assert rate is None
    assert source == rates_engine.NEEDS_REVIEW_SOURCE
    assert txn.needs_review is True


def test_a_foreign_currency_row_never_reaches_the_nearest_branch(
    in_memory_db: sqlite3.Connection,
) -> None:
    """ADR-021's terminal branch is part of the ladder, not an escape from it."""
    _seed_full_ladder(in_memory_db, DAY - timedelta(days=400))
    txn = _txn_on(DAY, currency=FOREIGN)

    rate, source = rates_engine.resolve(in_memory_db, txn)

    assert rate is None
    assert source == rates_engine.NEEDS_REVIEW_SOURCE
    assert not money.is_approximate(source)


def test_user_rate_does_not_price_a_foreign_currency_row(
    in_memory_db: sqlite3.Connection,
) -> None:
    """Branch 1 is VES-scoped too.

    ``user_rate`` is quote units per dollar (ADR-015), and the quote unit
    the ledger means by it is the bolívar. On a currency the chain has no
    tier for, the number's unit is unverified — the same reasoning that
    keeps a P2P fill's provenance off a native-USD row (ADR-021 §2.3).
    """
    txn = _txn_on(DAY, currency=FOREIGN, user_rate=Decimal("4000"))

    rate, source = rates_engine.resolve(in_memory_db, txn)

    assert rate is None
    assert source == rates_engine.NEEDS_REVIEW_SOURCE


def test_user_rate_still_prices_a_ladder_currency_row(
    in_memory_db: sqlite3.Connection,
) -> None:
    """The guard narrows branch 1; it must not disable it."""
    txn = _txn_on(DAY, currency="VES", user_rate=Decimal("42.50"))

    assert rates_engine.resolve(in_memory_db, txn) == (
        Decimal("42.50"),
        rates_engine.USER_RATE_SOURCE,
    )


def test_every_ladder_quote_currency_is_still_served(
    in_memory_db: sqlite3.Connection,
) -> None:
    """Structural: the guard is the tier table, not a second literal.

    Every quote currency the tiers define must resolve from the ladder. A
    tier added in a new currency passes here for free; a guard hard-coded
    to ``"VES"`` would fail the moment one is.
    """
    _seed_full_ladder(in_memory_db)

    for quote in sorted(LADDER_QUOTES):
        txn = _txn_on(DAY, currency=quote)

        rate, source = rates_engine.resolve(in_memory_db, txn)

        assert rate is not None, quote
        assert source != rates_engine.NEEDS_REVIEW_SOURCE, quote


# ---------------------------------------------------------------------------
# The arithmetic on top of it.
# ---------------------------------------------------------------------------


def test_to_usd_leaves_a_foreign_currency_row_unpriced(
    in_memory_db: sqlite3.Connection,
) -> None:
    """The one conversion path reports a blank, not an invented figure."""
    _seed_full_ladder(in_memory_db)
    txn = _txn_on(DAY, currency=FOREIGN, amount=Decimal("-120000.00"))

    amount_usd, source = money.to_usd(in_memory_db, txn)

    assert amount_usd is None
    assert source == rates_engine.NEEDS_REVIEW_SOURCE


def test_resolve_detail_reports_no_date_for_a_foreign_row(
    in_memory_db: sqlite3.Connection,
) -> None:
    _seed_full_ladder(in_memory_db)

    resolution = rates_engine.resolve_detail(
        in_memory_db, _txn_on(DAY, currency=FOREIGN)
    )

    assert resolution.as_of_date is None
    assert resolution.age_days is None
    assert resolution.approximate is False


# ---------------------------------------------------------------------------
# Property (rule-011 requires hypothesis on the rate logic).
# ---------------------------------------------------------------------------


@given(
    currency=st.text(alphabet="ABCDEFGHIJKLMNOPQRSTUVWXYZ", min_size=3, max_size=3)
    .filter(lambda c: c not in money.NATIVE_USD_CURRENCIES and c not in LADDER_QUOTES),
    offset=st.integers(min_value=-400, max_value=400),
    user_rate=st.one_of(st.none(), st.decimals(min_value=1, max_value=5000, places=2)),
)
@settings(max_examples=50, deadline=None, suppress_health_check=[HealthCheck.function_scoped_fixture])
def test_property_no_currency_outside_the_ladder_is_ever_priced(
    in_memory_db: sqlite3.Connection,
    currency: str,
    offset: int,
    user_rate: Decimal | None,
) -> None:
    """Outside ``native ∪ ladder quotes`` the chain never returns a rate."""
    _seed_full_ladder(in_memory_db)
    txn = _txn_on(DAY + timedelta(days=offset), currency=currency, user_rate=user_rate)

    rate, source = rates_engine.resolve(in_memory_db, txn)

    assert rate is None
    assert source == rates_engine.NEEDS_REVIEW_SOURCE
