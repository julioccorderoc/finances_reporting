"""Tests for finances.domain.rates (EPIC-005, ADR-005).

Covers every branch of the priority chain documented in ADR-005 and
rule-005: user_rate -> binance_p2p_median (with carry-forward) -> bcv
(with carry-forward) -> needs_review. Includes hypothesis property
tests exercising the invariants end-to-end per rule-011.
"""

from __future__ import annotations

import sqlite3
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import Any

from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from finances.db.repos import rates as rates_repo
from finances.domain import rates as rates_engine
from finances.domain.models import Rate, Transaction
from tests.conftest import RateFactory, TransactionFactory


def _txn_on(day: date, **overrides: Any) -> Transaction:
    return TransactionFactory.build(
        occurred_at=datetime(day.year, day.month, day.day, 12, 0, tzinfo=UTC),
        **overrides,
    )


def _insert_rate(conn: sqlite3.Connection, **overrides: Any) -> Rate:
    return rates_repo.insert(conn, RateFactory.build(**overrides))


# ---------------------------------------------------------------------------
# Happy paths — one per branch of the priority chain.
# ---------------------------------------------------------------------------


def test_user_rate_wins_when_set(in_memory_db: sqlite3.Connection) -> None:
    txn = _txn_on(date(2025, 6, 15), user_rate=Decimal("42.50"))

    rate, source = rates_engine.resolve(in_memory_db, txn)

    assert rate == Decimal("42.50")
    assert source == "user_rate"
    assert txn.needs_review is False


def test_user_rate_bypasses_market_data(in_memory_db: sqlite3.Connection) -> None:
    day = date(2025, 6, 15)
    _insert_rate(
        in_memory_db,
        as_of_date=day,
        base="USDT",
        quote="VES",
        source="binance_p2p_median",
        rate=Decimal("100"),
    )
    _insert_rate(
        in_memory_db,
        as_of_date=day,
        base="USD",
        quote="VES",
        source="bcv",
        rate=Decimal("50"),
    )
    txn = _txn_on(day, user_rate=Decimal("42.50"))

    rate, source = rates_engine.resolve(in_memory_db, txn)

    assert rate == Decimal("42.50")
    assert source == "user_rate"


def test_resolves_binance_p2p_on_exact_date(in_memory_db: sqlite3.Connection) -> None:
    day = date(2025, 6, 15)
    _insert_rate(
        in_memory_db,
        as_of_date=day,
        base="USDT",
        quote="VES",
        source="binance_p2p_median",
        rate=Decimal("87.25"),
    )
    txn = _txn_on(day)

    rate, source = rates_engine.resolve(in_memory_db, txn)

    assert rate == Decimal("87.25")
    assert source == "binance_p2p_median"
    assert txn.needs_review is False


def test_carries_forward_binance_p2p_from_earlier_day(
    in_memory_db: sqlite3.Connection,
) -> None:
    _insert_rate(
        in_memory_db,
        as_of_date=date(2025, 6, 12),
        base="USDT",
        quote="VES",
        source="binance_p2p_median",
        rate=Decimal("87.25"),
    )
    txn = _txn_on(date(2025, 6, 15))

    rate, source = rates_engine.resolve(in_memory_db, txn)

    assert rate == Decimal("87.25")
    assert source == "binance_p2p_median_carry"
    assert txn.needs_review is False


def test_falls_back_to_bcv_when_no_binance(in_memory_db: sqlite3.Connection) -> None:
    day = date(2025, 6, 15)
    _insert_rate(
        in_memory_db,
        as_of_date=day,
        base="USD",
        quote="VES",
        source="bcv",
        rate=Decimal("36.10"),
    )
    txn = _txn_on(day)

    rate, source = rates_engine.resolve(in_memory_db, txn)

    assert rate == Decimal("36.10")
    assert source == "bcv"
    assert txn.needs_review is False


def test_carries_forward_bcv_when_no_binance(in_memory_db: sqlite3.Connection) -> None:
    _insert_rate(
        in_memory_db,
        as_of_date=date(2025, 6, 10),
        base="USD",
        quote="VES",
        source="bcv",
        rate=Decimal("36.10"),
    )
    txn = _txn_on(date(2025, 6, 15))

    rate, source = rates_engine.resolve(in_memory_db, txn)

    assert rate == Decimal("36.10")
    assert source == "bcv_carry"
    assert txn.needs_review is False


def test_flags_needs_review_when_no_rates_available(
    in_memory_db: sqlite3.Connection,
) -> None:
    txn = _txn_on(date(2025, 6, 15))
    assert txn.needs_review is False

    rate, source = rates_engine.resolve(in_memory_db, txn)

    assert rate is None
    assert source == "needs_review"
    assert txn.needs_review is True


# ---------------------------------------------------------------------------
# Priority ordering — ensures the chain does not short-circuit wrong.
# ---------------------------------------------------------------------------


def test_p2p_exact_beats_bcv_exact_same_day(in_memory_db: sqlite3.Connection) -> None:
    day = date(2025, 6, 15)
    _insert_rate(
        in_memory_db,
        as_of_date=day,
        base="USDT",
        quote="VES",
        source="binance_p2p_median",
        rate=Decimal("87.25"),
    )
    _insert_rate(
        in_memory_db,
        as_of_date=day,
        base="USD",
        quote="VES",
        source="bcv",
        rate=Decimal("36.10"),
    )
    txn = _txn_on(day)

    rate, source = rates_engine.resolve(in_memory_db, txn)

    assert rate == Decimal("87.25")
    assert source == "binance_p2p_median"


def test_p2p_carry_beats_bcv_exact(in_memory_db: sqlite3.Connection) -> None:
    day = date(2025, 6, 15)
    _insert_rate(
        in_memory_db,
        as_of_date=date(2025, 6, 10),
        base="USDT",
        quote="VES",
        source="binance_p2p_median",
        rate=Decimal("87.25"),
    )
    _insert_rate(
        in_memory_db,
        as_of_date=day,
        base="USD",
        quote="VES",
        source="bcv",
        rate=Decimal("36.10"),
    )
    txn = _txn_on(day)

    rate, source = rates_engine.resolve(in_memory_db, txn)

    assert rate == Decimal("87.25")
    assert source == "binance_p2p_median_carry"


def test_latest_p2p_selected_when_multiple_earlier_days(
    in_memory_db: sqlite3.Connection,
) -> None:
    _insert_rate(
        in_memory_db,
        as_of_date=date(2025, 6, 10),
        base="USDT",
        quote="VES",
        source="binance_p2p_median",
        rate=Decimal("80"),
    )
    _insert_rate(
        in_memory_db,
        as_of_date=date(2025, 6, 14),
        base="USDT",
        quote="VES",
        source="binance_p2p_median",
        rate=Decimal("85"),
    )
    _insert_rate(
        in_memory_db,
        as_of_date=date(2025, 6, 11),
        base="USDT",
        quote="VES",
        source="binance_p2p_median",
        rate=Decimal("82"),
    )
    txn = _txn_on(date(2025, 6, 15))

    rate, source = rates_engine.resolve(in_memory_db, txn)

    assert rate == Decimal("85")
    assert source == "binance_p2p_median_carry"


# ---------------------------------------------------------------------------
# Non-raising contract + defensive semantics.
# ---------------------------------------------------------------------------


def test_does_not_raise_on_empty_rates_table(
    in_memory_db: sqlite3.Connection,
) -> None:
    txn = _txn_on(date(2025, 6, 15))

    rate, source = rates_engine.resolve(in_memory_db, txn)

    assert rate is None
    assert source == "needs_review"


def test_ignores_future_dated_rates(in_memory_db: sqlite3.Connection) -> None:
    _insert_rate(
        in_memory_db,
        as_of_date=date(2025, 6, 20),
        base="USDT",
        quote="VES",
        source="binance_p2p_median",
        rate=Decimal("99"),
    )
    txn = _txn_on(date(2025, 6, 15))

    rate, source = rates_engine.resolve(in_memory_db, txn)

    assert rate is None
    assert source == "needs_review"
    assert txn.needs_review is True


def test_does_not_mutate_needs_review_when_resolved(
    in_memory_db: sqlite3.Connection,
) -> None:
    day = date(2025, 6, 15)
    _insert_rate(
        in_memory_db,
        as_of_date=day,
        base="USDT",
        quote="VES",
        source="binance_p2p_median",
        rate=Decimal("87.25"),
    )
    txn = _txn_on(day, needs_review=False)

    rates_engine.resolve(in_memory_db, txn)

    assert txn.needs_review is False


def test_ignores_unrelated_sources(in_memory_db: sqlite3.Connection) -> None:
    day = date(2025, 6, 15)
    _insert_rate(
        in_memory_db,
        as_of_date=day,
        base="USDT",
        quote="VES",
        source="some_other_source",
        rate=Decimal("99"),
    )
    txn = _txn_on(day)

    rate, source = rates_engine.resolve(in_memory_db, txn)

    assert rate is None
    assert source == "needs_review"


# ---------------------------------------------------------------------------
# Hypothesis property tests (mandatory per rule-011).
# ---------------------------------------------------------------------------


_RATE_VALUES = st.decimals(
    min_value=Decimal("0.000001"),
    max_value=Decimal("1000000"),
    places=6,
    allow_nan=False,
    allow_infinity=False,
)
_DATES = st.dates(min_value=date(2020, 1, 1), max_value=date(2030, 12, 31))


@given(user_rate=_RATE_VALUES, day=_DATES)
@settings(
    max_examples=50,
    deadline=None,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
def test_property_user_rate_always_wins(
    in_memory_db: sqlite3.Connection, user_rate: Decimal, day: date
) -> None:
    txn = _txn_on(day, user_rate=user_rate)

    rate, source = rates_engine.resolve(in_memory_db, txn)

    assert rate == user_rate
    assert source == "user_rate"
    assert txn.needs_review is False


@given(
    p2p_offset=st.integers(min_value=0, max_value=60),
    rate_value=_RATE_VALUES,
    txn_day=st.dates(min_value=date(2021, 1, 1), max_value=date(2030, 12, 31)),
)
@settings(
    max_examples=40,
    deadline=None,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
def test_property_p2p_source_suffix_matches_date_offset(
    in_memory_db: sqlite3.Connection,
    p2p_offset: int,
    rate_value: Decimal,
    txn_day: date,
) -> None:
    in_memory_db.execute("DELETE FROM rates")
    rate_day = txn_day - timedelta(days=p2p_offset)
    _insert_rate(
        in_memory_db,
        as_of_date=rate_day,
        base="USDT",
        quote="VES",
        source="binance_p2p_median",
        rate=rate_value,
    )
    txn = _txn_on(txn_day)

    rate, source = rates_engine.resolve(in_memory_db, txn)

    assert rate == rate_value
    expected = (
        "binance_p2p_median_carry" if p2p_offset > 0 else "binance_p2p_median"
    )
    assert source == expected
    assert txn.needs_review is False


@given(
    p2p_offset=st.integers(min_value=0, max_value=30),
    bcv_offset=st.integers(min_value=0, max_value=30),
    txn_day=st.dates(min_value=date(2022, 1, 1), max_value=date(2030, 12, 31)),
)
@settings(
    max_examples=40,
    deadline=None,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
def test_property_p2p_always_beats_bcv_when_both_present(
    in_memory_db: sqlite3.Connection,
    p2p_offset: int,
    bcv_offset: int,
    txn_day: date,
) -> None:
    in_memory_db.execute("DELETE FROM rates")
    _insert_rate(
        in_memory_db,
        as_of_date=txn_day - timedelta(days=p2p_offset),
        base="USDT",
        quote="VES",
        source="binance_p2p_median",
        rate=Decimal("100"),
    )
    _insert_rate(
        in_memory_db,
        as_of_date=txn_day - timedelta(days=bcv_offset),
        base="USD",
        quote="VES",
        source="bcv",
        rate=Decimal("50"),
    )
    txn = _txn_on(txn_day)

    rate, source = rates_engine.resolve(in_memory_db, txn)

    assert rate == Decimal("100")
    assert source in {"binance_p2p_median", "binance_p2p_median_carry"}


@given(
    txn_day=st.dates(min_value=date(2022, 1, 1), max_value=date(2030, 12, 31)),
    bcv_offset=st.integers(min_value=0, max_value=30),
)
@settings(
    max_examples=30,
    deadline=None,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
def test_property_bcv_used_only_when_no_p2p_available(
    in_memory_db: sqlite3.Connection,
    txn_day: date,
    bcv_offset: int,
) -> None:
    in_memory_db.execute("DELETE FROM rates")
    _insert_rate(
        in_memory_db,
        as_of_date=txn_day - timedelta(days=bcv_offset),
        base="USD",
        quote="VES",
        source="bcv",
        rate=Decimal("36.50"),
    )
    txn = _txn_on(txn_day)

    rate, source = rates_engine.resolve(in_memory_db, txn)

    assert rate == Decimal("36.50")
    expected = "bcv_carry" if bcv_offset > 0 else "bcv"
    assert source == expected
