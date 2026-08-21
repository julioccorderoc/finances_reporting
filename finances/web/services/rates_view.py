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
from finances.domain.rates import CARRY_SUFFIX, NEAREST_SUFFIX

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
    won. ``None`` only when the tier has no rate at all, or when the
    transaction's currency is not the tier's quote currency.

    ADR-016 additionally suppressed the figure for an expired tier, on the
    grounds that no dollar amount may be rendered from a rate the chain
    refused. ADR-021 is what changed: the chain no longer refuses it, it
    *approximates* with it, and this panel is where the owner accepts that
    number or types a better one (design criterion D9). Blanking it would
    hide the offer.

    ``is_expired`` marks a rate older than its tier's carry-forward bound.
    ``is_approximate`` is the wider fact — expired **or** dated after the
    transaction — and is what the resolver's ``_nearest`` suffix means.
    Such a row is still rendered, with ``age_days``, rather than hidden:
    "no data for this period" and "data exists, out of window" are
    different facts and the owner needs to tell them apart.

    ``age_days`` is **signed**, like ``RateResolution.age_days``: positive
    for a rate that predates the transaction, negative for one published
    after it. "BCV, 3 days later" cannot be said without the direction.
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
    is_approximate: bool
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
    forbids. Both of the resolver's suffixes are stripped before matching;
    a suffix the panel does not know about would leave three tiers and no
    winner marked. Sources with no table-backed series (``user_rate``,
    ``native_usd``, ``needs_review``) simply mark nothing.

    Each tier offers its **best available** rate, in the resolver's own
    order of preference: the in-window backward answer if it has one, else
    the nearest row in either direction (ADR-021), which is the same row
    the resolver's terminal branch would have used. The bound comes from
    ``rates.max_age_days`` rather than a second copy — that is the whole
    point of ``max_age_days`` existing.

    ``amount_native``/``currency`` are the transaction's own, and are
    required rather than defaulted: a caller that forgets them would
    silently strip every dollar figure off the panel. Each series is
    priced through ``money.to_usd_at``, the same helper behind every other
    USD figure — no quantize, formatting left to ``fmt_money`` — so the
    winner's row and the modal header cannot disagree. A series whose
    quote currency is not the transaction's is left unpriced: dividing,
    say, COP by a VES rate would invent a number.
    """
    winner = winning_source.removesuffix(CARRY_SUFFIX).removesuffix(NEAREST_SUFFIX)

    series: list[DayRate] = []
    for base, quote, source, label in _MODAL_SERIES_SPEC:
        found = rates_repo.latest_on_or_before(
            conn, as_of_date=day, base=base, quote=quote, source=source
        )
        max_age = rates_domain.max_age_days(source)
        age_days = (day - found.as_of_date).days if found is not None else None
        is_expired = (
            age_days is not None and max_age is not None and age_days > max_age
        )
        if found is None or is_expired:
            # Nothing usable behind the transaction: fall to the same row
            # the resolver's terminal branch would take, which may well be
            # the expired one it just rejected — or a later one it could
            # not see.
            nearest = rates_repo.nearest(
                conn, as_of_date=day, base=base, quote=quote, source=source
            )
            if nearest is not None:
                found = nearest
                age_days = (day - found.as_of_date).days
                is_expired = max_age is not None and age_days > max_age

        is_approximate = age_days is not None and (is_expired or age_days < 0)
        # Priced through the shared helper, so the panel and the winning
        # row cannot disagree about the arithmetic. A series whose quote
        # currency is not the transaction's stays unpriced: dividing, say,
        # COP by a VES rate would invent a number.
        amount_usd = (
            money.to_usd_at(amount_native, currency, found.rate)
            if found is not None and currency == quote
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
                is_approximate=is_approximate,
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
