"""Rate resolution engine (EPIC-005, ADR-005).

The single auditable entry point for converting a transaction's native
amount to USD. Every USD-equivalence calculation in the codebase must
route through ``resolve`` — see ``docs/architecture/rules/rule-005``.

Priority chain (locked by ADR-005):

    1. ``Transaction.user_rate``                           (user_rate)
    2. ``rates(USDT, VES, day, 'binance_p2p_median')``    (exact or _carry)
    3. ``rates(USD,  VES, day, 'bcv')``                    (exact or _carry)
    4. none -> sets ``transaction.needs_review = True``   (needs_review)

``resolve`` never raises on missing data; gaps are surfaced via the
``needs_review`` flag and the returned source label instead.
"""

from __future__ import annotations

import sqlite3
from decimal import Decimal

from finances.db.repos import rates as rates_repo
from finances.domain.models import Transaction

USER_RATE_SOURCE = "user_rate"
BINANCE_P2P_SOURCE = "binance_p2p_median"
BCV_SOURCE = "bcv"
NEEDS_REVIEW_SOURCE = "needs_review"
CARRY_SUFFIX = "_carry"

# Ordered fallback tiers consulted after ``user_rate``. Each tuple is
# (base, quote, source). Order is load-bearing: ADR-005 mandates P2P
# first, BCV second, with carry-forward applied within each tier before
# falling through to the next.
_FALLBACK_TIERS: tuple[tuple[str, str, str], ...] = (
    ("USDT", "VES", BINANCE_P2P_SOURCE),
    ("USD", "VES", BCV_SOURCE),
)


def resolve(
    conn: sqlite3.Connection, txn: Transaction
) -> tuple[Decimal | None, str]:
    """Resolve ``txn``'s exchange rate via the ADR-005 priority chain.

    Returns ``(rate, source)``. When no rate is available, returns
    ``(None, 'needs_review')`` and sets ``txn.needs_review = True`` as a
    side effect so downstream persistence layers can flag the row.
    """
    if txn.user_rate is not None:
        return txn.user_rate, USER_RATE_SOURCE

    as_of = txn.occurred_at.date()
    for base, quote, source in _FALLBACK_TIERS:
        rate = rates_repo.latest_on_or_before(
            conn, as_of_date=as_of, base=base, quote=quote, source=source
        )
        if rate is None:
            continue
        if rate.as_of_date == as_of:
            return rate.rate, source
        return rate.rate, source + CARRY_SUFFIX

    txn.needs_review = True
    return None, NEEDS_REVIEW_SOURCE


__all__ = [
    "BCV_SOURCE",
    "BINANCE_P2P_SOURCE",
    "CARRY_SUFFIX",
    "NEEDS_REVIEW_SOURCE",
    "USER_RATE_SOURCE",
    "resolve",
]
