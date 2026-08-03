"""Rate-page DTO builders for /rates (EPIC-023, Phase 2d).

The Rates page has two reads:

* ``build_rates_chart`` — daily-granularity history for two specific
  series (USDT/VES P2P median + USD/VES BCV) over a configurable
  trailing window. The chart is reference-only; per ADR-005 BCV is
  never used as a headline figure but it is informative to plot.
* ``build_latest_rates`` — the most recent rate per
  ``(base, quote, source)`` tuple, broader than the chart.

All reads stay in this module; the route layer only adapts FastAPI
plumbing around them.
"""

from __future__ import annotations

import sqlite3
from datetime import date, timedelta
from decimal import Decimal

from pydantic import BaseModel, ConfigDict

from finances.db.repos import rates as rates_repo
from finances.domain import money
from finances.domain import rates as rates_domain
from finances.domain.rates import CARRY_SUFFIX

DEFAULT_RANGE_DAYS = 30


class RatePoint(BaseModel):
    model_config = ConfigDict(extra="forbid")

    as_of_date: date
    rate: Decimal


class RateSeries(BaseModel):
    model_config = ConfigDict(extra="forbid")

    label: str
    base: str
    quote: str
    source: str
    points: list[RatePoint]


class RatesChart(BaseModel):
    model_config = ConfigDict(extra="forbid")

    series: list[RateSeries]
    range_days: int


class LatestRateCard(BaseModel):
    model_config = ConfigDict(extra="forbid")

    base: str
    quote: str
    source: str
    rate: Decimal
    as_of_date: date


# (base, quote, source, label) — the two series the chart pins.
_CHART_SERIES_SPEC: tuple[tuple[str, str, str, str], ...] = (
    ("USDT", "VES", "binance_p2p_median", "USDT/VES P2P"),
    ("USD", "VES", "bcv", "USD/VES BCV"),
)

# The triage modal shows every tier ``rates.resolve`` can draw from, in the
# resolver's own priority order, so the owner can see what was NOT used as
# well as what was. Kept separate from _CHART_SERIES_SPEC on purpose: the
# realized series has no rows on this base (spec §3.1a) and would draw a
# permanently empty line on the /rates chart.
_MODAL_SERIES_SPEC: tuple[tuple[str, str, str, str], ...] = (
    ("USDT", "VES", "binance_p2p_realized", "Realized"),
    ("USDT", "VES", "binance_p2p_median", "USDT P2P"),
    ("USD", "VES", "bcv", "BCV"),
)

# ADR-005: BCV is reference-only and never a headline figure.
_REFERENCE_ONLY_SOURCES = frozenset({"bcv"})


class DayRate(BaseModel):
    """One candidate rate for a transaction's day, as the modal shows it.

    ``amount_usd`` is the counterfactual: what the transaction's native
    amount would be worth priced at THIS tier, whether or not this tier
    won. ``None`` when the tier has no rate for the day, when the
    transaction's currency is not the tier's quote currency, or when the
    tier has expired (ADR-016) — no dollar figure may be rendered from a
    rate the chain refused.

    ``is_expired`` marks a rate older than its tier's carry-forward bound.
    Such a row is still rendered, with ``age_days``, rather than hidden:
    "no data for this period" and "data exists, rejected as stale" are
    different facts and the owner needs to tell them apart.
    """

    model_config = ConfigDict(extra="forbid")

    label: str
    source: str
    rate: Decimal | None
    as_of_date: date | None
    amount_usd: Decimal | None
    is_carry: bool
    is_winner: bool
    is_reference_only: bool
    is_expired: bool
    age_days: int | None


def _series_points(
    conn: sqlite3.Connection,
    *,
    base: str,
    quote: str,
    source: str,
    since: date,
) -> list[RatePoint]:
    rows = conn.execute(
        """
        SELECT as_of_date, rate
        FROM rates
        WHERE base = ? AND quote = ? AND source = ? AND as_of_date >= ?
        ORDER BY as_of_date ASC
        """,
        (base, quote, source, since.isoformat()),
    ).fetchall()
    points: list[RatePoint] = []
    for row in rows:
        as_of = row["as_of_date"]
        if not isinstance(as_of, date):
            as_of = date.fromisoformat(str(as_of))
        rate_value = row["rate"]
        if not isinstance(rate_value, Decimal):
            rate_value = Decimal(str(rate_value))
        points.append(RatePoint(as_of_date=as_of, rate=rate_value))
    return points


def build_rates_chart(
    conn: sqlite3.Connection, *, range_days: int = DEFAULT_RANGE_DAYS
) -> RatesChart:
    """Build a ``RatesChart`` for the trailing ``range_days`` window."""
    if range_days <= 0:
        range_days = DEFAULT_RANGE_DAYS
    since = date.today() - timedelta(days=range_days - 1)

    series: list[RateSeries] = []
    for base, quote, source, label in _CHART_SERIES_SPEC:
        points = _series_points(
            conn, base=base, quote=quote, source=source, since=since
        )
        series.append(
            RateSeries(
                label=label,
                base=base,
                quote=quote,
                source=source,
                points=points,
            )
        )
    return RatesChart(series=series, range_days=range_days)


def build_latest_rates(conn: sqlite3.Connection) -> list[LatestRateCard]:
    """Return one card per ``(base, quote, source)`` with the latest row."""
    rows = conn.execute(
        """
        SELECT base, quote, source, rate, as_of_date
        FROM rates r
        WHERE as_of_date = (
            SELECT MAX(as_of_date)
            FROM rates r2
            WHERE r2.base = r.base AND r2.quote = r.quote AND r2.source = r.source
        )
        GROUP BY base, quote, source
        ORDER BY base, quote, source
        """
    ).fetchall()

    cards: list[LatestRateCard] = []
    for row in rows:
        as_of = row["as_of_date"]
        if not isinstance(as_of, date):
            as_of = date.fromisoformat(str(as_of))
        rate_value = row["rate"]
        if not isinstance(rate_value, Decimal):
            rate_value = Decimal(str(rate_value))
        cards.append(
            LatestRateCard(
                base=row["base"],
                quote=row["quote"],
                source=row["source"],
                rate=rate_value,
                as_of_date=as_of,
            )
        )
    return cards


def rates_for_day(
    conn: sqlite3.Connection,
    *,
    day: date,
    winning_source: str,
    amount_native: Decimal,
    currency: str,
) -> list[DayRate]:
    """Return the three candidate rate series for ``day``, each priced.

    ``winning_source`` is the ``rate_source`` already computed by
    ``rates.resolve`` via ``_project_card``. This function NEVER re-derives
    the winner — duplicating resolver logic here is exactly what rule-012
    forbids. Sources with no table-backed series (``user_rate``,
    ``native_usd``, ``needs_review``) simply mark nothing.

    ``amount_native``/``currency`` are the transaction's own, and are
    required rather than defaulted: a caller that forgets them would
    silently strip every dollar figure off the panel. Each series is
    priced the same way ``_project_card`` prices the winner —
    ``amount / rate``, no quantize, formatting left to ``fmt_money`` — so
    the winner's row and the modal header cannot disagree. A series whose
    quote currency is not the transaction's is left unpriced: dividing,
    say, COP by a VES rate would invent a number.
    """
    winner = winning_source.removesuffix(CARRY_SUFFIX)

    series: list[DayRate] = []
    for base, quote, source, label in _MODAL_SERIES_SPEC:
        found = rates_repo.latest_on_or_before(
            conn, as_of_date=day, base=base, quote=quote, source=source
        )
        age_days = (day - found.as_of_date).days if found is not None else None
        max_age = rates_domain.max_age_days(source)
        is_expired = (
            age_days is not None and max_age is not None and age_days > max_age
        )
        # Priced through the shared helper, so the panel and the winning
        # row cannot disagree about the arithmetic. A series whose quote
        # currency is not the transaction's stays unpriced: dividing, say,
        # COP by a VES rate would invent a number.
        amount_usd = (
            money.to_usd_at(amount_native, currency, found.rate)
            if found is not None and currency == quote and not is_expired
            else None
        )
        series.append(
            DayRate(
                label=label,
                source=source,
                rate=found.rate if found is not None else None,
                as_of_date=found.as_of_date if found is not None else None,
                amount_usd=amount_usd,
                is_carry=found is not None and found.as_of_date < day,
                is_winner=source == winner,
                is_reference_only=source in _REFERENCE_ONLY_SOURCES,
                is_expired=is_expired,
                age_days=age_days,
            )
        )
    return series


__all__ = [
    "DEFAULT_RANGE_DAYS",
    "DayRate",
    "LatestRateCard",
    "RatePoint",
    "RateSeries",
    "RatesChart",
    "build_latest_rates",
    "build_rates_chart",
    "rates_for_day",
]
