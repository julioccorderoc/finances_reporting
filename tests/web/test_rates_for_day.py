"""Per-day candidate rate series for the triage modal (spec §5.1).

The panel must show all three tiers the resolver can draw from, mark which
one actually produced the dollar figure, and disclose carry-forward. It must
never re-derive the winner — it is told, via ``winning_source``.
"""

from __future__ import annotations

import sqlite3
from datetime import date
from decimal import Decimal

import pytest

from finances.db.repos import rates as rates_repo
from finances.domain import rates as rates_domain
from finances.domain.models import Rate
from finances.web.services.rates_view import rates_for_day

DAY = date(2026, 4, 23)


def _seed(conn: sqlite3.Connection, source: str, day: date, rate: str,
          base: str = "USDT", quote: str = "VES") -> None:
    rates_repo.upsert(
        conn,
        Rate(as_of_date=day, base=base, quote=quote,
             rate=Decimal(rate), source=source),
    )


def test_returns_three_series_in_resolver_priority_order(
    web_db: sqlite3.Connection,
) -> None:
    series = rates_for_day(web_db, day=DAY, winning_source="bcv")

    assert [s.source for s in series] == [
        "binance_p2p_realized",
        "binance_p2p_median",
        "bcv",
    ]


def test_missing_series_renders_as_none_not_error(
    web_db: sqlite3.Connection,
) -> None:
    """binance_p2p_realized has no rows on this base — that is expected."""
    series = rates_for_day(web_db, day=DAY, winning_source="needs_review")
    realized = series[0]

    assert realized.rate is None
    assert realized.as_of_date is None
    assert realized.is_carry is False
    assert realized.is_winner is False


def test_exact_day_match_is_not_carry(web_db: sqlite3.Connection) -> None:
    _seed(web_db, "binance_p2p_median", DAY, "483.31")

    series = rates_for_day(web_db, day=DAY, winning_source="binance_p2p_median")
    p2p = series[1]

    assert p2p.rate == Decimal("483.31")
    assert p2p.as_of_date == DAY
    assert p2p.is_carry is False
    assert p2p.is_winner is True


def test_older_rate_is_carried_and_flagged(web_db: sqlite3.Connection) -> None:
    _seed(web_db, "binance_p2p_median", date(2026, 4, 21), "481.00")

    series = rates_for_day(web_db, day=DAY, winning_source="binance_p2p_median_carry")
    p2p = series[1]

    assert p2p.rate == Decimal("481.00")
    assert p2p.as_of_date == date(2026, 4, 21)
    assert p2p.is_carry is True


def test_carry_suffix_still_matches_the_winner(
    web_db: sqlite3.Connection,
) -> None:
    """'bcv_carry' must mark the 'bcv' series, not fall through to no winner."""
    _seed(web_db, "bcv", date(2026, 4, 20), "36.55", base="USD")

    series = rates_for_day(web_db, day=DAY, winning_source="bcv_carry")

    assert [s.is_winner for s in series] == [False, False, True]


def test_bcv_is_flagged_reference_only(web_db: sqlite3.Connection) -> None:
    """ADR-005: BCV is never a headline figure, even when it is the winner."""
    series = rates_for_day(web_db, day=DAY, winning_source="bcv")

    assert [s.is_reference_only for s in series] == [False, False, True]


@pytest.mark.parametrize("source", ["user_rate", "native_usd", "needs_review"])
def test_non_series_winners_mark_nothing(
    web_db: sqlite3.Connection, source: str
) -> None:
    """user_rate / native_usd / needs_review are not table-backed series."""
    _seed(web_db, "binance_p2p_median", DAY, "483.31")

    series = rates_for_day(web_db, day=DAY, winning_source=source)

    assert not any(s.is_winner for s in series)


def test_bcv_series_reads_usd_ves_not_usdt_ves(
    web_db: sqlite3.Connection,
) -> None:
    """The BCV pair is USD/VES; a USDT/VES bcv row must not be picked up."""
    _seed(web_db, "bcv", DAY, "99.99", base="USDT")

    series = rates_for_day(web_db, day=DAY, winning_source="bcv")

    assert series[2].rate is None


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

    series = rates_for_day(web_db, day=DAY, winning_source=winning_source)

    winners = [s for s in series if s.is_winner]
    assert len(winners) == 1, (
        f"expected exactly one modal series to win for "
        f"winning_source={winning_source!r}, got {[s.source for s in winners]!r} "
        f"— rates_view._MODAL_SERIES_SPEC is out of sync with "
        f"rates.resolve's _FALLBACK_TIERS"
    )
    assert winners[0].source == source
