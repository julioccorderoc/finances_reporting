"""Realized VES cost-basis rates derived from Binance P2P sells (ADR-013).

A P2P **sell** (USDT out, bolívars in) is the moment bolívars are acquired,
and ``transactions.user_rate`` on that row already holds the exact realized
VES/USDT price of the fill. This module rolls those fills up into one
volume-weighted rate per day and materializes them into the ``rates`` table
under :data:`REALIZED_SOURCE`, so the resolver can price bolívar spending at
what it actually cost rather than at the spend-day market median.

Materializing (rather than querying transactions live) lets
:func:`finances.domain.rates.resolve` reuse ``rates_repo.latest_on_or_before``
verbatim — including the ``as_of_date`` the staleness guard needs.

Selection deliberately keys off ``source_ref LIKE 'p2p:%'`` plus a negative
amount rather than ``kind``: bank-anchored pairing (EPIC-006) promotes these
rows to ``kind='transfer'`` after the fact, so ``kind`` is not stable.
"""

from __future__ import annotations

import logging
import re
import sqlite3
from datetime import date
from decimal import Decimal

from finances.db.repos import rates as rates_repo
from finances.domain.models import Rate

logger = logging.getLogger(__name__)

REALIZED_SOURCE = "binance_p2p_realized"
BASE_ASSET = "USDT"
QUOTE_FIAT = "VES"

# P2P fills are described by our own ingester with a deterministic shape:
#   "P2P SELL USDT @ 41.33 VES (order 12345)"
# The fiat is not stored as a column, so it is recovered from that text. A
# fill whose fiat is anything other than VES must never be folded into the
# VES average; see ADR-013 §4.2.
_FIAT_RE = re.compile(r"@\s+\S+\s+([A-Za-z]{2,10})\s+\(order", re.IGNORECASE)

SQL_P2P_SELLS = """
    SELECT DATE(occurred_at) AS day,
           amount,
           user_rate,
           description
    FROM transactions
    WHERE source_ref LIKE 'p2p:%'
      AND CAST(amount AS REAL) < 0
      AND user_rate IS NOT NULL
    ORDER BY day ASC
"""


def _to_decimal(value: object) -> Decimal:
    """Coerce a sqlite value to Decimal without passing through float."""
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


def _fiat_of(description: str | None) -> str:
    """Recover the fill's fiat from its generated description.

    Falls back to :data:`QUOTE_FIAT` when the description is absent or does
    not match the ingester's shape — the ledger is VES-only in practice, so
    an unparseable row is far more likely to be VES than a second currency.
    """
    if not description:
        return QUOTE_FIAT
    match = _FIAT_RE.search(description)
    if match is None:
        return QUOTE_FIAT
    return match.group(1).upper()


def compute_realized_rates(conn: sqlite3.Connection) -> list[Rate]:
    """Derive one volume-weighted realized VES rate per day of P2P sells.

    Pure: reads transactions, writes nothing. Returns rates ordered by date.
    Days whose fills sum to zero volume are skipped rather than dividing by
    zero.
    """
    weighted: dict[str, Decimal] = {}
    volume: dict[str, Decimal] = {}

    for row in conn.execute(SQL_P2P_SELLS):
        fiat = _fiat_of(row["description"])
        if fiat != QUOTE_FIAT:
            logger.info(
                "skipping non-%s P2P fill on %s (fiat=%s)", QUOTE_FIAT, row["day"], fiat
            )
            continue

        day = row["day"]
        size = abs(_to_decimal(row["amount"]))
        rate = _to_decimal(row["user_rate"])

        weighted[day] = weighted.get(day, Decimal(0)) + size * rate
        volume[day] = volume.get(day, Decimal(0)) + size

    results: list[Rate] = []
    for day in sorted(weighted):
        total_volume = volume[day]
        if total_volume == 0:
            logger.warning("skipping %s: P2P fills sum to zero volume", day)
            continue
        results.append(
            Rate(
                as_of_date=date.fromisoformat(day),
                base=BASE_ASSET,
                quote=QUOTE_FIAT,
                rate=weighted[day] / total_volume,
                source=REALIZED_SOURCE,
            )
        )
    return results


def rebuild(conn: sqlite3.Connection) -> int:
    """Recompute realized rates from P2P history and upsert them.

    Idempotent: ``rates`` carries ``UNIQUE(as_of_date, base, quote, source)``,
    so re-running updates in place. Safe to call after every ingest, and the
    recovery path when a P2P transaction is edited or deleted.

    Returns the number of daily rates written.
    """
    computed = compute_realized_rates(conn)
    for rate in computed:
        rates_repo.upsert(conn, rate)
    return len(computed)


__all__ = [
    "BASE_ASSET",
    "QUOTE_FIAT",
    "REALIZED_SOURCE",
    "compute_realized_rates",
    "rebuild",
]
