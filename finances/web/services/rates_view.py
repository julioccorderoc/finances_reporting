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


__all__ = [
    "DEFAULT_RANGE_DAYS",
    "LatestRateCard",
    "RatePoint",
    "RateSeries",
    "RatesChart",
    "build_latest_rates",
    "build_rates_chart",
]
