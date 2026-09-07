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

from pydantic import BaseModel, ConfigDict, Field

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
    """The plotted series plus the axis they are plotted against.

    ``labels`` is the shared date domain: every day any series has a row
    for, ascending. It is built here rather than in the browser because
    Chart.js's ``category`` scale places points by *index* unless it is
    handed an explicit label list — so a 13-row median and a 245-row BCV
    series end up drawn against different days. That defect is invisible
    to a server test if the axis is assembled in JavaScript, and visible
    to one if it is assembled here.

    The domain is not clamped to today: BCV publishes tomorrow's rate
    today, and an axis that stopped at ``date.today()`` would drop a rate
    the ledger already holds.
    """

    model_config = ConfigDict(extra="forbid")

    series: list[RateSeries]
    labels: list[date]
    range_days: int


class LatestRateCard(BaseModel):
    model_config = ConfigDict(extra="forbid")

    base: str
    quote: str
    source: str
    rate: Decimal
    as_of_date: date


class RateLogRow(BaseModel):
    """One recorded rate, as the history log lists it.

    Flat rather than pivoted: the pivot this replaced showed one row per
    day and one column per stream, which meant a wall of em-dashes as soon
    as the range was wide enough to be interesting — BCV publishes daily,
    the P2P median a dozen times a year. Here every row carries a rate.
    """

    model_config = ConfigDict(extra="forbid")

    as_of_date: date
    base: str
    quote: str
    pair: str
    source: str
    label: str
    rate: Decimal
    is_reference_only: bool
    is_future: bool


class RatesLogFilter(BaseModel):
    """URL-persistent filter state for the /rates history log.

    Every field defaults to "no constraint", dates included: a bare
    /rates lists every rate the ledger holds. The pivot's window was the
    chart's range toggle, which made the table a view of the plot rather
    than of the data. ``TransactionsFilter`` carries the same warning for
    the same reason — an invented default window makes an unfiltered
    search silently local.
    """

    model_config = ConfigDict(extra="forbid")

    date_from: date | None = None
    date_to: date | None = None
    pairs: list[str] = Field(default_factory=list)
    sources: list[str] = Field(default_factory=list)
    page: int = 1
    page_size: int = 50


class RatesLogPage(BaseModel):
    """Paginated result for the log fragment."""

    model_config = ConfigDict(extra="forbid")

    rows: list[RateLogRow]
    total: int
    page: int
    page_size: int
    total_pages: int
    filter: RatesLogFilter


class RatesLogOptions(BaseModel):
    """What the two dropdowns offer, read off the data.

    Not a hard-coded list: ``rates.source`` is TEXT and open-ended, the
    same as ``transactions.source``, so a stream ingested later earns a
    filter entry with no edit here.
    """

    model_config = ConfigDict(extra="forbid")

    pairs: list[str]
    sources: list[str]


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

# Display names for the sources the ledger records today. Open-ended on
# purpose (``rates.source`` is TEXT): a source with no entry here shows
# its raw value rather than being dropped or renamed.
SOURCE_LABELS: dict[str, str] = {
    "user_rate": "user rate",
    "binance_p2p_realized": "realized",
    "binance_p2p_median": "P2P median",
    "binance_p2p_median_buy": "P2P buy",
    "binance_p2p_median_sell": "P2P sell",
    "bcv": "BCV",
    "native_usd": "native",
}


def source_label(source: str) -> str:
    """Human name for ``source``, falling back to the stored value."""
    return SOURCE_LABELS.get(source, source)


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
    return RatesChart(
        series=series,
        labels=_date_axis(series, since=since),
        range_days=range_days,
    )


def _date_axis(series: list[RateSeries], *, since: date) -> list[date]:
    """Every calendar day from ``since`` to the last day worth showing.

    Every day, not only the days with rows: the gaps are the point. A P2P
    median published nine times in a month should look like nine marks
    across the month, not nine consecutive ones.

    The right edge is today, or later if a series carries a forward-dated
    row — BCV publishes ahead, and an axis stopping at ``today`` would
    silently drop a rate the ledger already holds.
    """
    last = max(
        [date.today()]
        + [point.as_of_date for s in series for point in s.points]
    )
    if last < since:
        return []
    return [since + timedelta(days=n) for n in range((last - since).days + 1)]


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


def _ladder_rank(base: str, quote: str, source: str) -> int:
    """Where ``(base, quote, source)`` sits in the resolver's chain.

    Streams the ladder does not price at all — BCV in euros, say — sort
    after every one it does. Read off ``rates.ladder_tiers`` rather than
    restated, so a re-ordering of the chain re-orders every surface that
    lists tiers (rule-012).
    """
    ladder = list(rates_domain.ladder_tiers())
    triple = (base, quote, source)
    return ladder.index(triple) if triple in ladder else len(ladder)


def _log_where(f: RatesLogFilter) -> tuple[str, list[object]]:
    """The WHERE clause for ``f``, and its parameters.

    An empty filter yields no constraints at all — that is what makes a
    bare /rates the whole ledger rather than a window onto it.
    """
    clauses: list[str] = []
    params: list[object] = []

    if f.date_from is not None:
        clauses.append("as_of_date >= ?")
        params.append(f.date_from.isoformat())
    if f.date_to is not None:
        clauses.append("as_of_date <= ?")
        params.append(f.date_to.isoformat())
    if f.pairs:
        marks = ", ".join("?" for _ in f.pairs)
        clauses.append(f"(base || '/' || quote) IN ({marks})")
        params.extend(f.pairs)
    if f.sources:
        marks = ", ".join("?" for _ in f.sources)
        clauses.append(f"source IN ({marks})")
        params.extend(f.sources)

    where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
    return where, params


def count_matching_rates(conn: sqlite3.Connection, f: RatesLogFilter) -> int:
    """How many rows ``f`` matches — the "N rates" line, without the page."""
    where, params = _log_where(f)
    row = conn.execute(f"SELECT COUNT(*) FROM rates{where}", params).fetchone()
    return int(row[0])


def build_rates_log(
    conn: sqlite3.Connection, f: RatesLogFilter | None = None
) -> RatesLogPage:
    """Run the filtered, paginated history query and return a page.

    One row per recorded rate. The pivot this replaced showed one row per
    day and one column per stream, which meant a grid that was mostly
    em-dashes as soon as the range was wide enough to be interesting.

    Ordering is date descending, then the resolver's ladder within a day,
    so the tier that would have priced a transaction reads above the one
    that would not. The ladder rank is computed in Python rather than in
    SQL: expressing it as a CASE would be a second copy of the chain.
    """
    f = f or RatesLogFilter()
    where, params = _log_where(f)

    total = count_matching_rates(conn, f)
    page_size = max(1, f.page_size)
    total_pages = max(1, -(-total // page_size))
    page_number = min(max(1, f.page), total_pages)
    offset = (page_number - 1) * page_size

    # Ordered in SQL by date and pair so the page boundary is stable, then
    # re-ordered by ladder rank within each day below. Sorting the whole
    # table in Python would mean reading every row to render fifty.
    rows = conn.execute(
        f"""
        SELECT as_of_date, base, quote, source, rate
        FROM rates{where}
        ORDER BY as_of_date DESC, base, quote, source
        LIMIT ? OFFSET ?
        """,
        [*params, page_size, offset],
    ).fetchall()

    today = date.today()
    log_rows: list[RateLogRow] = []
    for row in rows:
        as_of = row["as_of_date"]
        if not isinstance(as_of, date):
            as_of = date.fromisoformat(str(as_of))
        rate_value = row["rate"]
        if not isinstance(rate_value, Decimal):
            rate_value = Decimal(str(rate_value))
        log_rows.append(
            RateLogRow(
                as_of_date=as_of,
                base=row["base"],
                quote=row["quote"],
                pair=f"{row['base']}/{row['quote']}",
                source=row["source"],
                label=source_label(row["source"]),
                rate=rate_value,
                is_reference_only=row["source"] in _REFERENCE_ONLY_SOURCES,
                is_future=as_of > today,
            )
        )

    # Within a day, the chain's order. Python's sort is stable, so the
    # SQL ordering survives as the tie-break among equal ranks.
    log_rows.sort(
        key=lambda r: (-r.as_of_date.toordinal(), _ladder_rank(r.base, r.quote, r.source))
    )

    return RatesLogPage(
        rows=log_rows,
        total=total,
        page=page_number,
        page_size=page_size,
        total_pages=total_pages,
        filter=f,
    )


def rates_log_options(conn: sqlite3.Connection) -> RatesLogOptions:
    """The pairs and sources the dropdowns offer, read off the table."""
    pairs = [
        row[0]
        for row in conn.execute(
            "SELECT DISTINCT base || '/' || quote FROM rates ORDER BY 1"
        ).fetchall()
    ]
    sources = [
        row[0]
        for row in conn.execute(
            "SELECT DISTINCT source FROM rates ORDER BY 1"
        ).fetchall()
    ]
    return RatesLogOptions(pairs=pairs, sources=sources)


def build_chart_details(
    conn: sqlite3.Connection, *, range_days: int = DEFAULT_RANGE_DAYS
) -> dict[date, list[RateLogRow]]:
    """Every rate recorded on each day of the chart's window, by day.

    Feeds the chart's hover overlay, which answers "what was recorded that
    day" — all of it, not only the two series the plot plots. Bounded by
    the chart's range because that is the only place it is read, and
    carrying a year of detail into a page that plots a week would be
    payload for nothing.
    """
    if range_days <= 0:
        range_days = DEFAULT_RANGE_DAYS
    since = date.today() - timedelta(days=range_days - 1)

    page = build_rates_log(
        conn,
        RatesLogFilter(date_from=since, page=1, page_size=_DETAIL_ROW_CAP),
    )

    by_day: dict[date, list[RateLogRow]] = {}
    for row in page.rows:
        by_day.setdefault(row.as_of_date, []).append(row)
    return by_day


# A year of daily BCV in two currencies plus the P2P streams is well under
# this; the cap only exists so a pathological table cannot put an unbounded
# payload on the page.
_DETAIL_ROW_CAP = 5000


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
    "RateLogRow",
    "RatePoint",
    "RateSeries",
    "RatesChart",
    "RatesLogFilter",
    "RatesLogOptions",
    "RatesLogPage",
    "SOURCE_LABELS",
    "build_chart_details",
    "build_latest_rates",
    "build_rates_chart",
    "build_rates_log",
    "count_matching_rates",
    "rates_for_day",
    "rates_log_options",
    "source_label",
]
