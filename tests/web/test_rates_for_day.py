"""Per-day candidate rate series for the triage modal (spec §5.1).

The panel must show all three tiers the resolver can draw from, mark which
one actually produced the dollar figure, and disclose carry-forward. It must
never re-derive the winner — it is told, via ``winning_source``.
"""

from __future__ import annotations

import sqlite3
from datetime import date, timedelta
from decimal import Decimal

import pytest

from finances.db.repos import rates as rates_repo
from finances.domain import rates as rates_domain
from finances.domain.models import Rate
from finances.web.services.rates_view import rates_for_day

DAY = date(2026, 4, 23)
AMOUNT = Decimal("20000.00")
CURRENCY = "VES"


def _seed(conn: sqlite3.Connection, source: str, day: date, rate: str,
          base: str = "USDT", quote: str = "VES") -> None:
    rates_repo.upsert(
        conn,
        Rate(as_of_date=day, base=base, quote=quote,
             rate=Decimal(rate), source=source),
    )


def _series(
    conn: sqlite3.Connection,
    winning_source: str,
    *,
    amount_native: Decimal = AMOUNT,
    currency: str = CURRENCY,
):
    """Call the service for ``DAY`` with a default priced amount."""
    return rates_for_day(
        conn,
        day=DAY,
        winning_source=winning_source,
        amount_native=amount_native,
        currency=currency,
    )


def test_returns_three_series_in_resolver_priority_order(
    web_db: sqlite3.Connection,
) -> None:
    series = _series(web_db, "bcv")

    assert [s.source for s in series] == [
        "binance_p2p_realized",
        "binance_p2p_median",
        "bcv",
    ]


def test_missing_series_renders_as_none_not_error(
    web_db: sqlite3.Connection,
) -> None:
    """binance_p2p_realized has no rows on this base — that is expected."""
    series = _series(web_db, "needs_review")
    realized = series[0]

    assert realized.rate is None
    assert realized.as_of_date is None
    assert realized.is_carry is False
    assert realized.is_winner is False


def test_exact_day_match_is_not_carry(web_db: sqlite3.Connection) -> None:
    _seed(web_db, "binance_p2p_median", DAY, "483.31")

    series = _series(web_db, "binance_p2p_median")
    p2p = series[1]

    assert p2p.rate == Decimal("483.31")
    assert p2p.as_of_date == DAY
    assert p2p.is_carry is False
    assert p2p.is_winner is True


def test_older_rate_is_carried_and_flagged(web_db: sqlite3.Connection) -> None:
    _seed(web_db, "binance_p2p_median", date(2026, 4, 21), "481.00")

    series = _series(web_db, "binance_p2p_median_carry")
    p2p = series[1]

    assert p2p.rate == Decimal("481.00")
    assert p2p.as_of_date == date(2026, 4, 21)
    assert p2p.is_carry is True


def test_carry_suffix_still_matches_the_winner(
    web_db: sqlite3.Connection,
) -> None:
    """'bcv_carry' must mark the 'bcv' series, not fall through to no winner."""
    _seed(web_db, "bcv", date(2026, 4, 20), "36.55", base="USD")

    series = _series(web_db, "bcv_carry")

    assert [s.is_winner for s in series] == [False, False, True]


def test_bcv_is_flagged_reference_only(web_db: sqlite3.Connection) -> None:
    """ADR-005: BCV is never a headline figure, even when it is the winner."""
    series = _series(web_db, "bcv")

    assert [s.is_reference_only for s in series] == [False, False, True]


@pytest.mark.parametrize("source", ["user_rate", "native_usd", "needs_review"])
def test_non_series_winners_mark_nothing(
    web_db: sqlite3.Connection, source: str
) -> None:
    """user_rate / native_usd / needs_review are not table-backed series."""
    _seed(web_db, "binance_p2p_median", DAY, "483.31")

    series = _series(web_db, source)

    assert not any(s.is_winner for s in series)


def test_bcv_series_reads_usd_ves_not_usdt_ves(
    web_db: sqlite3.Connection,
) -> None:
    """The BCV pair is USD/VES; a USDT/VES bcv row must not be picked up."""
    _seed(web_db, "bcv", DAY, "99.99", base="USDT")

    series = _series(web_db, "bcv")

    assert series[2].rate is None


# ---------------------------------------------------------------------------
# Per-tier USD pricing: what the amount WOULD be worth under each rate.
# ---------------------------------------------------------------------------


def test_each_series_is_priced_in_usd_at_its_own_rate(
    web_db: sqlite3.Connection,
) -> None:
    """The panel answers "what if?" for the tiers that did NOT win."""
    _seed(web_db, "binance_p2p_realized", DAY, "500.00")
    _seed(web_db, "binance_p2p_median", DAY, "400.00")
    _seed(web_db, "bcv", DAY, "250.00", base="USD")

    series = _series(web_db, "binance_p2p_realized")

    assert [s.amount_usd for s in series] == [
        Decimal("40.00"),
        Decimal("50.00"),
        Decimal("80.00"),
    ]


def test_a_missing_series_has_no_usd_figure(web_db: sqlite3.Connection) -> None:
    _seed(web_db, "binance_p2p_median", DAY, "400.00")

    series = _series(web_db, "binance_p2p_median")

    assert series[0].rate is None
    assert series[0].amount_usd is None
    assert series[1].amount_usd == Decimal("50.00")


def test_usd_keeps_the_sign_of_the_native_amount(
    web_db: sqlite3.Connection,
) -> None:
    """Expenses are negative (project_expense_sign_convention)."""
    _seed(web_db, "bcv", DAY, "250.00", base="USD")

    series = _series(web_db, "bcv", amount_native=Decimal("-20000.00"))

    assert series[2].amount_usd == Decimal("-80.00")


def test_a_non_ves_amount_is_not_priced_by_a_ves_series(
    web_db: sqlite3.Connection,
) -> None:
    """Dividing a COP amount by a VES/USDT rate would invent a number."""
    _seed(web_db, "binance_p2p_median", DAY, "400.00")

    series = _series(web_db, "needs_review", currency="COP")

    assert series[1].rate == Decimal("400.00"), "the rate is still disclosed"
    assert all(s.amount_usd is None for s in series)


@pytest.mark.parametrize(
    "base,quote,source",
    list(rates_domain._FALLBACK_TIERS),
    ids=[tier[2] for tier in rates_domain._FALLBACK_TIERS],
)
@pytest.mark.parametrize(
    "suffix", ["", rates_domain.CARRY_SUFFIX], ids=["exact", "carry"]
)
def test_every_resolver_tier_has_exactly_one_modal_winner(
    web_db: sqlite3.Connection, base: str, quote: str, source: str, suffix: str
) -> None:
    """Cross-module invariant pinning ``rates_view`` to ``rates.resolve``.

    Parametrized straight off ``finances.domain.rates._FALLBACK_TIERS`` — the
    resolver's own priority chain — rather than a hand-copied list of source
    names. If the resolver ever grows a tier (or renames one) that
    ``_MODAL_SERIES_SPEC`` in ``rates_view.py`` doesn't also know about, this
    test must fail (zero winners, not a skip) rather than let the modal
    silently render three rates with no winner marked.
    """
    winning_source = source + suffix

    series = _series(web_db, winning_source)

    winners = [s for s in series if s.is_winner]
    assert len(winners) == 1, (
        f"expected exactly one modal series to win for "
        f"winning_source={winning_source!r}, got {[s.source for s in winners]!r} "
        f"— rates_view._MODAL_SERIES_SPEC is out of sync with "
        f"rates.resolve's _FALLBACK_TIERS"
    )
    assert winners[0].source == source


# ---------------------------------------------------------------------------
# ADR-014: the panel applies the same max age the resolver does.
#
# The panel performs its own latest_on_or_before lookup per tier, so without
# this it would keep displaying a rate the resolver has stopped using. An
# expired tier is shown-but-marked rather than hidden: the owner must be able
# to tell "no P2P data for this period" from "P2P data exists, rejected as
# stale", and that ambiguity is what made the original bug invisible.
# ---------------------------------------------------------------------------


def test_expired_median_is_marked_expired(web_db: sqlite3.Connection) -> None:
    stale_day = DAY - timedelta(days=rates_domain.MEDIAN_MAX_AGE_DAYS + 1)
    _seed(web_db, "binance_p2p_median", stale_day, "633.52")

    median = next(
        s for s in _series(web_db, "bcv") if s.source == "binance_p2p_median"
    )

    assert median.is_expired is True
    assert median.age_days == rates_domain.MEDIAN_MAX_AGE_DAYS + 1


def test_expired_median_is_shown_not_hidden(web_db: sqlite3.Connection) -> None:
    """The stale number stays visible so the owner can see it was rejected."""
    stale_day = DAY - timedelta(days=60)
    _seed(web_db, "binance_p2p_median", stale_day, "633.52")

    median = next(
        s for s in _series(web_db, "bcv") if s.source == "binance_p2p_median"
    )

    assert median.rate == Decimal("633.52")
    assert median.as_of_date == stale_day


def test_expired_median_carries_no_dollar_figure(
    web_db: sqlite3.Connection,
) -> None:
    """No USD may be rendered from a rate the chain refused."""
    stale_day = DAY - timedelta(days=60)
    _seed(web_db, "binance_p2p_median", stale_day, "633.52")

    median = next(
        s for s in _series(web_db, "bcv") if s.source == "binance_p2p_median"
    )

    assert median.amount_usd is None


def test_median_on_final_day_of_window_is_priced_normally(
    web_db: sqlite3.Connection,
) -> None:
    fresh_enough = DAY - timedelta(days=rates_domain.MEDIAN_MAX_AGE_DAYS)
    _seed(web_db, "binance_p2p_median", fresh_enough, "800.00")

    median = next(
        s for s in _series(web_db, "bcv") if s.source == "binance_p2p_median"
    )

    assert median.is_expired is False
    assert median.amount_usd == AMOUNT / Decimal("800.00")


def test_bcv_never_expires(web_db: sqlite3.Connection) -> None:
    """ADR-014 caps the median only; BCV is the floor of the chain."""
    ancient = DAY - timedelta(days=400)
    _seed(web_db, "bcv", ancient, "36.00", base="USD", quote="VES")

    bcv = next(s for s in _series(web_db, "bcv") if s.source == "bcv")

    assert bcv.is_expired is False
    assert bcv.amount_usd == AMOUNT / Decimal("36.00")
