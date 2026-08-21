"""ADR-021 — every tier expires, and an expired chain prices approximately.

Three changes, tested here:

1. ``max_age_days`` describes *every* tier (realized and BCV joined the
   median), so a surface doing its own lookup gets the resolver's own
   bound by asking rather than by guessing.
2. A terminal branch below tier 4: when nothing resolves inside its
   window, the row is priced from the **nearest** rate in the table —
   either direction — and labelled ``<source>_nearest``. ``needs_review``
   now means only "the table holds nothing for this pair".
3. ``resolve`` short-circuits a native-USD currency *above* branch 1, so
   the 142 live USDT rows carrying ``user_rate`` as provenance can never
   have that bolívar price read as a conversion factor.
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


REALIZED = rates_engine.REALIZED_SOURCE
MEDIAN = rates_engine.BINANCE_P2P_SOURCE
BCV = rates_engine.BCV_SOURCE
NEAREST = rates_engine.NEAREST_SUFFIX


def _txn_on(day: date, **overrides: Any) -> Transaction:
    """A VES transaction on ``day`` — the currency the ladder exists for."""
    overrides.setdefault("currency", "VES")
    return TransactionFactory.build(
        occurred_at=datetime(day.year, day.month, day.day, 12, 0, tzinfo=UTC),
        **overrides,
    )


def _insert_rate(conn: sqlite3.Connection, **overrides: Any) -> Rate:
    return rates_repo.insert(conn, RateFactory.build(**overrides))


def _realized(conn: sqlite3.Connection, day: date, value: str) -> Rate:
    return _insert_rate(
        conn,
        as_of_date=day,
        base="USDT",
        quote="VES",
        source=REALIZED,
        rate=Decimal(value),
    )


def _median(conn: sqlite3.Connection, day: date, value: str) -> Rate:
    return _insert_rate(
        conn,
        as_of_date=day,
        base="USDT",
        quote="VES",
        source=MEDIAN,
        rate=Decimal(value),
    )


def _bcv(conn: sqlite3.Connection, day: date, value: str) -> Rate:
    return _insert_rate(
        conn,
        as_of_date=day,
        base="USD",
        quote="VES",
        source=BCV,
        rate=Decimal(value),
    )


DAY = date(2026, 5, 20)


# ---------------------------------------------------------------------------
# 1 — one table, every tier.
# ---------------------------------------------------------------------------


def test_max_age_days_covers_every_fallback_tier() -> None:
    """The realized cap was enforced inline and BCV had none at all.

    ``rates_view.rates_for_day`` reads this function per tier; a tier that
    answers ``None`` is a tier the panel will happily price from at any
    age, which is the divergence ADR-016 §2.1 closed for the median.
    """
    assert rates_engine.max_age_days(REALIZED) == rates_engine.REALIZED_MAX_AGE_DAYS
    assert rates_engine.max_age_days(MEDIAN) == rates_engine.MEDIAN_MAX_AGE_DAYS
    assert rates_engine.max_age_days(BCV) == rates_engine.BCV_MAX_AGE_DAYS
    assert rates_engine.max_age_days("user_rate") is None


def test_every_tier_shares_one_bound() -> None:
    caps = {
        rates_engine.REALIZED_MAX_AGE_DAYS,
        rates_engine.MEDIAN_MAX_AGE_DAYS,
        rates_engine.BCV_MAX_AGE_DAYS,
    }
    assert caps == {14}


def test_bcv_still_carries_inside_its_window(in_memory_db: sqlite3.Connection) -> None:
    _bcv(in_memory_db, DAY - timedelta(days=rates_engine.BCV_MAX_AGE_DAYS), "100")

    rate, source = rates_engine.resolve(in_memory_db, _txn_on(DAY))

    assert rate == Decimal("100")
    assert source == BCV + rates_engine.CARRY_SUFFIX


def test_bcv_expires_past_its_window(in_memory_db: sqlite3.Connection) -> None:
    """The floor of the chain now has a floor of its own.

    Past the cap the value is still used — there is nothing better — but it
    is reported as an approximation, not as a carry.
    """
    old = DAY - timedelta(days=rates_engine.BCV_MAX_AGE_DAYS + 1)
    _bcv(in_memory_db, old, "100")

    rate, source = rates_engine.resolve(in_memory_db, _txn_on(DAY))

    assert rate == Decimal("100")
    assert source == BCV + NEAREST


# ---------------------------------------------------------------------------
# 2 — the nearest-rate branch.
# ---------------------------------------------------------------------------


def test_nearest_looks_forward_too(in_memory_db: sqlite3.Connection) -> None:
    """A rate published the day *after* the row still prices it.

    ``latest_on_or_before`` cannot see it, so before this branch the row was
    unpriceable while a rate sat one day away.
    """
    _bcv(in_memory_db, DAY + timedelta(days=1), "142.50")

    rate, source = rates_engine.resolve(in_memory_db, _txn_on(DAY))

    assert rate == Decimal("142.50")
    assert source == BCV + NEAREST


def test_a_future_rate_is_never_a_carry(in_memory_db: sqlite3.Connection) -> None:
    """Hindsight is a different claim from staleness, even one day out."""
    _median(in_memory_db, DAY + timedelta(days=1), "800")

    _rate, source = rates_engine.resolve(in_memory_db, _txn_on(DAY))

    assert source == MEDIAN + NEAREST
    assert rates_engine.CARRY_SUFFIX not in source


def test_nearest_picks_the_closest_rate_in_either_direction(
    in_memory_db: sqlite3.Connection,
) -> None:
    _bcv(in_memory_db, DAY - timedelta(days=40), "100")
    _bcv(in_memory_db, DAY + timedelta(days=20), "300")

    rate, source = rates_engine.resolve(in_memory_db, _txn_on(DAY))

    assert rate == Decimal("300")
    assert source == BCV + NEAREST


def test_nearest_prefers_the_higher_tier_at_equal_distance(
    in_memory_db: sqlite3.Connection,
) -> None:
    """Distance first, then the resolver's own priority order."""
    far = timedelta(days=30)
    _bcv(in_memory_db, DAY - far, "100")
    _median(in_memory_db, DAY - far, "700")
    _realized(in_memory_db, DAY - far, "690")

    rate, source = rates_engine.resolve(in_memory_db, _txn_on(DAY))

    assert rate == Decimal("690")
    assert source == REALIZED + NEAREST


def test_distance_beats_priority(in_memory_db: sqlite3.Connection) -> None:
    """A closer BCV print wins over a much older realized rate."""
    _realized(in_memory_db, DAY - timedelta(days=90), "690")
    _bcv(in_memory_db, DAY - timedelta(days=20), "620")

    rate, source = rates_engine.resolve(in_memory_db, _txn_on(DAY))

    assert rate == Decimal("620")
    assert source == BCV + NEAREST


def test_an_in_window_tier_always_beats_the_nearest_branch(
    in_memory_db: sqlite3.Connection,
) -> None:
    """The new branch is terminal: it never pre-empts a usable tier."""
    _median(in_memory_db, DAY - timedelta(days=2), "800")
    _bcv(in_memory_db, DAY, "620")

    rate, source = rates_engine.resolve(in_memory_db, _txn_on(DAY))

    assert rate == Decimal("800")
    assert source == MEDIAN + rates_engine.CARRY_SUFFIX


def test_needs_review_only_when_the_table_is_empty(
    in_memory_db: sqlite3.Connection,
) -> None:
    txn = _txn_on(DAY)

    rate, source = rates_engine.resolve(in_memory_db, txn)

    assert rate is None
    assert source == rates_engine.NEEDS_REVIEW_SOURCE
    assert txn.needs_review is True


def test_an_approximated_row_is_not_needs_review(
    in_memory_db: sqlite3.Connection,
) -> None:
    """D6: an approximate rate never blocks the sitting."""
    _bcv(in_memory_db, DAY - timedelta(days=200), "100")
    txn = _txn_on(DAY)

    _rate, source = rates_engine.resolve(in_memory_db, txn)

    assert source != rates_engine.NEEDS_REVIEW_SOURCE
    assert txn.needs_review is False


def test_rates_of_another_pair_do_not_rescue_the_row(
    in_memory_db: sqlite3.Connection,
) -> None:
    """Nearest is scoped per tier, not "any row in the table"."""
    _insert_rate(
        in_memory_db,
        as_of_date=DAY,
        base="USD",
        quote="COP",
        source="bcv",
        rate=Decimal("4000"),
    )
    txn = _txn_on(DAY)

    rate, source = rates_engine.resolve(in_memory_db, txn)

    assert rate is None
    assert source == rates_engine.NEEDS_REVIEW_SOURCE


# ---------------------------------------------------------------------------
# is_approximate — the one reading of the suffix.
# ---------------------------------------------------------------------------


def test_is_approximate_reads_the_suffix() -> None:
    assert money.is_approximate(BCV + NEAREST)
    assert money.is_approximate(MEDIAN + NEAREST)
    assert money.is_approximate(REALIZED + NEAREST)
    assert not money.is_approximate(BCV)
    assert not money.is_approximate(BCV + rates_engine.CARRY_SUFFIX)
    assert not money.is_approximate(money.NATIVE_USD_SOURCE)
    assert not money.is_approximate(rates_engine.NEEDS_REVIEW_SOURCE)


def test_a_bcv_approximation_is_still_bcv_sourced() -> None:
    """Net worth and the headline bar BCV; the suffix must not smuggle it in."""
    assert money.is_bcv_sourced(BCV + NEAREST)


# ---------------------------------------------------------------------------
# 3 — the currency guard.
# ---------------------------------------------------------------------------


def test_native_currency_short_circuits_above_user_rate(
    in_memory_db: sqlite3.Connection,
) -> None:
    """The 142 USDT rows whose ``user_rate`` is provenance, not a factor.

    A P2P fill records the bolívar price it was struck at. Read as a
    conversion factor it would report 200 USDT as $1.21.
    """
    txn = _txn_on(DAY, currency="USDT", user_rate=Decimal("165.40"))

    rate, source = rates_engine.resolve(in_memory_db, txn)

    assert source == money.NATIVE_USD_SOURCE
    assert rate == Decimal("1")
    assert money.to_usd(in_memory_db, txn) == (txn.amount, money.NATIVE_USD_SOURCE)


def test_every_native_currency_is_guarded(in_memory_db: sqlite3.Connection) -> None:
    _bcv(in_memory_db, DAY, "620")
    for code in money.NATIVE_USD_CURRENCIES:
        txn = _txn_on(DAY, currency=code)

        rate, source = rates_engine.resolve(in_memory_db, txn)

        assert (rate, source) == (Decimal("1"), money.NATIVE_USD_SOURCE), code
        assert txn.needs_review is False


def test_a_native_row_never_reaches_the_nearest_branch(
    in_memory_db: sqlite3.Connection,
) -> None:
    """An empty rates table must not make a dollar row approximate."""
    txn = _txn_on(DAY, currency="USD")

    _rate, source = rates_engine.resolve(in_memory_db, txn)

    assert source == money.NATIVE_USD_SOURCE
    assert not money.is_approximate(source)
    assert txn.needs_review is False


# ---------------------------------------------------------------------------
# Repo primitive.
# ---------------------------------------------------------------------------


def test_repo_nearest_scans_both_directions(
    in_memory_db: sqlite3.Connection,
) -> None:
    _bcv(in_memory_db, DAY - timedelta(days=9), "100")
    _bcv(in_memory_db, DAY + timedelta(days=3), "300")

    found = rates_repo.nearest(
        in_memory_db, as_of_date=DAY, base="USD", quote="VES", source=BCV
    )

    assert found is not None
    assert found.as_of_date == DAY + timedelta(days=3)


def test_repo_nearest_prefers_the_earlier_row_on_a_tie(
    in_memory_db: sqlite3.Connection,
) -> None:
    """A carried rate is a weaker claim than a same-day one but a stronger
    one than hindsight, so equal distance resolves backwards."""
    _bcv(in_memory_db, DAY - timedelta(days=4), "100")
    _bcv(in_memory_db, DAY + timedelta(days=4), "300")

    found = rates_repo.nearest(
        in_memory_db, as_of_date=DAY, base="USD", quote="VES", source=BCV
    )

    assert found is not None
    assert found.rate == Decimal("100")


def test_repo_nearest_returns_none_for_an_empty_tier(
    in_memory_db: sqlite3.Connection,
) -> None:
    _bcv(in_memory_db, DAY, "100")

    assert (
        rates_repo.nearest(
            in_memory_db, as_of_date=DAY, base="USDT", quote="VES", source=MEDIAN
        )
        is None
    )


# ---------------------------------------------------------------------------
# resolve_detail — the resolution, with its provenance attached.
# ---------------------------------------------------------------------------


def test_resolve_detail_reports_the_rate_date_and_signed_age(
    in_memory_db: sqlite3.Connection,
) -> None:
    """Signed: positive when the rate predates the row, negative for hindsight.

    The design's rate panel says "BCV, 3 days later" — it cannot without a
    direction, and re-deriving one outside the resolver is a second chain.
    """
    _bcv(in_memory_db, DAY + timedelta(days=3), "142.50")

    res = rates_engine.resolve_detail(in_memory_db, _txn_on(DAY))

    assert res.source == BCV + NEAREST
    assert res.rate == Decimal("142.50")
    assert res.as_of_date == DAY + timedelta(days=3)
    assert res.age_days == -3
    assert res.approximate is True


def test_resolve_detail_and_resolve_never_disagree(
    in_memory_db: sqlite3.Connection,
) -> None:
    _realized(in_memory_db, DAY - timedelta(days=1), "690")
    txn = _txn_on(DAY)

    res = rates_engine.resolve_detail(in_memory_db, txn)

    assert (res.rate, res.source) == rates_engine.resolve(in_memory_db, txn)
    assert res.age_days == 1
    assert res.approximate is False


def test_resolve_detail_on_a_user_rate_row(in_memory_db: sqlite3.Connection) -> None:
    """A typed rate has no date of its own — it is the row's own number."""
    res = rates_engine.resolve_detail(
        in_memory_db, _txn_on(DAY, user_rate=Decimal("42.50"))
    )

    assert res.source == rates_engine.USER_RATE_SOURCE
    assert res.rate == Decimal("42.50")
    assert res.as_of_date is None
    assert res.age_days is None
    assert res.approximate is False


# ---------------------------------------------------------------------------
# Properties (rule-011 requires hypothesis on the rate logic).
# ---------------------------------------------------------------------------

_RATE_VALUES = st.decimals(
    min_value=Decimal("0.01"),
    max_value=Decimal("999999"),
    places=4,
    allow_nan=False,
    allow_infinity=False,
)
_DAYS = st.dates(min_value=date(2022, 1, 1), max_value=date(2030, 12, 31))


@given(
    offset=st.integers(min_value=-400, max_value=400),
    value=_RATE_VALUES,
    day=_DAYS,
)
@settings(
    max_examples=60,
    deadline=None,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
def test_property_one_bcv_row_prices_every_day(
    in_memory_db: sqlite3.Connection, offset: int, value: Decimal, day: date
) -> None:
    """With any rate in the table, no VES row is ever unpriceable.

    And the label tells the truth about which of the three states produced
    it: exact, carried inside the window, or approximated.
    """
    in_memory_db.execute("DELETE FROM rates")
    _bcv(in_memory_db, day + timedelta(days=offset), str(value))
    txn = _txn_on(day)

    rate, source = rates_engine.resolve(in_memory_db, txn)

    assert rate == value
    assert txn.needs_review is False
    if offset == 0:
        assert source == BCV
    elif -rates_engine.BCV_MAX_AGE_DAYS <= offset < 0:
        assert source == BCV + rates_engine.CARRY_SUFFIX
    else:
        assert source == BCV + NEAREST


@given(
    realized_offset=st.integers(min_value=-300, max_value=300),
    bcv_offset=st.integers(min_value=-300, max_value=300),
    day=_DAYS,
)
@settings(
    max_examples=60,
    deadline=None,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
def test_property_window_beats_distance_beats_priority(
    in_memory_db: sqlite3.Connection,
    realized_offset: int,
    bcv_offset: int,
    day: date,
) -> None:
    """The full ordering, stated once.

    A tier inside its window always wins, in priority order. Only when both
    are outside does distance decide, and only a tie there falls back to
    priority.
    """
    in_memory_db.execute("DELETE FROM rates")
    _realized(in_memory_db, day + timedelta(days=realized_offset), "690")
    _bcv(in_memory_db, day + timedelta(days=bcv_offset), "620")

    _rate, source = rates_engine.resolve(in_memory_db, _txn_on(day))

    cap = rates_engine.REALIZED_MAX_AGE_DAYS
    realized_usable = -cap <= realized_offset <= 0
    bcv_usable = -rates_engine.BCV_MAX_AGE_DAYS <= bcv_offset <= 0

    if realized_usable:
        assert source.startswith(REALIZED)
        assert not money.is_approximate(source)
    elif bcv_usable:
        assert source.startswith(BCV)
        assert not money.is_approximate(source)
    else:
        assert money.is_approximate(source)
        expected = (
            REALIZED
            if abs(realized_offset) <= abs(bcv_offset)
            else BCV
        )
        assert source == expected + NEAREST


@given(
    offsets=st.lists(
        st.integers(min_value=-200, max_value=200),
        min_size=1,
        max_size=6,
        unique=True,
    ),
    day=_DAYS,
)
@settings(
    max_examples=50,
    deadline=None,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
def test_property_nearest_is_the_minimum_distance(
    in_memory_db: sqlite3.Connection, offsets: list[int], day: date
) -> None:
    in_memory_db.execute("DELETE FROM rates")
    for n, offset in enumerate(offsets):
        _bcv(in_memory_db, day + timedelta(days=offset), str(100 + n))

    found = rates_repo.nearest(
        in_memory_db, as_of_date=day, base="USD", quote="VES", source=BCV
    )

    assert found is not None
    best = min(abs(o) for o in offsets)
    closest = {o for o in offsets if abs(o) == best}
    # Ties resolve backwards: a carry outranks hindsight.
    expected = day - timedelta(days=best) if -best in closest else day + timedelta(days=best)
    assert found.as_of_date == expected
