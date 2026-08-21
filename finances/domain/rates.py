"""Rate resolution engine (EPIC-005, ADR-005).

The single auditable entry point for converting a transaction's native
amount to USD. Every USD-equivalence calculation in the codebase must
route through ``resolve`` — see ``docs/architecture/rules/rule-005``.

Priority chain (locked by ADR-005, extended by ADR-013, ADR-016 and
ADR-021):

    0. native-USD currency                                 (native_usd)
    1. ``Transaction.user_rate``                           (user_rate)
    2. ``rates(USDT, VES, day, 'binance_p2p_realized')``  (exact or _carry,
       while no older than :data:`REALIZED_MAX_AGE_DAYS`)
    3. ``rates(USDT, VES, day, 'binance_p2p_median')``    (exact or _carry,
       while no older than :data:`MEDIAN_MAX_AGE_DAYS`)
    4. ``rates(USD,  VES, day, 'bcv')``                    (exact or _carry,
       while no older than :data:`BCV_MAX_AGE_DAYS`)
    5. the nearest rate in the table, either direction    (_nearest)
    6. none at all -> sets ``transaction.needs_review``    (needs_review)

Branch 0 is a guard, not a tier: a native-USD row has nothing to convert,
and its ``user_rate`` — when it has one — is the bolívar price a P2P fill
was struck at, recorded as provenance. Reading that as a conversion factor
would report a 200 USDT row as $1.21. It sits *above* branch 1 for exactly
that reason (ADR-021 §2.3).

Tier 2 is the *cost basis*: what the bolívars actually cost when they were
acquired, rather than what they were worth on the day they were spent.

Every tier is age-capped, at one shared number, and every cap lives in
:data:`_TIER_MAX_AGE_DAYS` where :func:`max_age_days` can read it — a
surface that keeps its own copy is the defect ADR-016 §2.1 fixed for the
median tier and ADR-021 §2.1 fixed for the other two. A stale rate
misprices spending badly in a fast-moving currency, and carried far
enough, any tier converges on BCV.

Branch 5 is what makes an expiry safe: past every window the chain still
prices the row, from the closest rate the table holds in either direction,
and says so in the label. ``needs_review`` therefore now means one thing —
the table holds nothing at all for this pair.

``resolve`` never raises on missing data; gaps are surfaced via the
``needs_review`` flag and the returned source label instead.
"""

from __future__ import annotations

import sqlite3
from datetime import date
from decimal import Decimal

from pydantic import BaseModel, ConfigDict

from finances.db.repos import rates as rates_repo
from finances.domain.models import Transaction

USER_RATE_SOURCE = "user_rate"
REALIZED_SOURCE = "binance_p2p_realized"
BINANCE_P2P_SOURCE = "binance_p2p_median"
BCV_SOURCE = "bcv"
NEEDS_REVIEW_SOURCE = "needs_review"
CARRY_SUFFIX = "_carry"

# Appended to a tier's source label when the rate came from ADR-021's
# terminal branch: the closest row in the table rather than that tier's
# own in-window answer. ``money.is_approximate`` is the one reader.
NEAREST_SUFFIX = "_nearest"

# How long a realized acquisition rate may be carried onto later spending
# before it is considered stale (ADR-013). Inclusive: a rate exactly this
# many days old still applies.
REALIZED_MAX_AGE_DAYS = 14

# The same bound for the market median (ADR-016) and for BCV (ADR-021).
# Held equal on purpose: one number governs the whole chain.
MEDIAN_MAX_AGE_DAYS = 14
BCV_MAX_AGE_DAYS = 14

# Ordered tiers consulted after the ``user_rate`` override. Each tuple is
# (base, quote, source). Order is load-bearing twice over: it is the
# ADR-005 priority chain, and it is also the tie-break when two tiers are
# equally far from the transaction's day in branch 5.
_FALLBACK_TIERS: tuple[tuple[str, str, str], ...] = (
    ("USDT", "VES", REALIZED_SOURCE),
    ("USDT", "VES", BINANCE_P2P_SOURCE),
    ("USD", "VES", BCV_SOURCE),
)

# Per-tier carry-forward bound, keyed by source. Since ADR-021 every tier
# is here; a source absent from this map carries without limit, and none
# of the chain's own tiers is.
_TIER_MAX_AGE_DAYS: dict[str, int] = {
    REALIZED_SOURCE: REALIZED_MAX_AGE_DAYS,
    BINANCE_P2P_SOURCE: MEDIAN_MAX_AGE_DAYS,
    BCV_SOURCE: BCV_MAX_AGE_DAYS,
}


class RateResolution(BaseModel):
    """One answer from the chain, with the provenance that produced it.

    ``resolve`` returns only ``(rate, source)`` because that is all its
    forty-odd callers need. The triage queue and the modal's rate panel
    need two more facts to say "BCV, 3 days later" instead of a bare
    "approximate", and deriving those outside the resolver would be the
    second chain rule-005 forbids.

    ``age_days`` is **signed**: positive when the rate predates the
    transaction (a carry), negative when it postdates it (hindsight),
    zero on the day itself. ``None`` for answers with no date of their
    own — ``user_rate``, ``native_usd``, ``needs_review``.
    """

    model_config = ConfigDict(extra="forbid")

    rate: Decimal | None
    source: str
    as_of_date: date | None = None
    age_days: int | None = None
    approximate: bool = False


def _native_usd() -> tuple[frozenset[str], str]:
    """The native-USD currency set and its source label.

    Imported inside the function because :mod:`finances.domain.money`
    imports this module. The alternative — a second literal set here — is
    precisely what ``tests/test_money_is_the_only_definition.py`` exists to
    forbid, and what the 2026-08-03 review found four copies of.
    """
    from finances.domain.money import NATIVE_USD_CURRENCIES, NATIVE_USD_SOURCE

    return NATIVE_USD_CURRENCIES, NATIVE_USD_SOURCE


def max_age_days(source: str) -> int | None:
    """Return the carry-forward bound for ``source``, or ``None`` if uncapped.

    Public so the triage modal's rate panel can apply the resolver's own
    bound instead of hard-coding a second copy of it. The panel does its own
    ``latest_on_or_before`` lookup per tier, and a panel that disagreed with
    the resolver about staleness is precisely the bug ADR-016 fixes.
    """
    return _TIER_MAX_AGE_DAYS.get(source)


def _in_window_tier(
    conn: sqlite3.Connection, as_of: date
) -> RateResolution | None:
    """Branches 2-4: the first tier with a rate inside its own window."""
    for base, quote, source in _FALLBACK_TIERS:
        found = rates_repo.latest_on_or_before(
            conn, as_of_date=as_of, base=base, quote=quote, source=source
        )
        if found is None:
            continue
        age_days = (as_of - found.as_of_date).days
        max_age = max_age_days(source)
        if max_age is not None and age_days > max_age:
            continue
        suffix = CARRY_SUFFIX if age_days else ""
        return RateResolution(
            rate=found.rate,
            source=source + suffix,
            as_of_date=found.as_of_date,
            age_days=age_days,
        )
    return None


def _nearest_tier(conn: sqlite3.Connection, as_of: date) -> RateResolution | None:
    """Branch 5: the closest rate any tier holds, in either direction.

    Distance decides; the chain's own priority breaks a tie. Scanning the
    tiers in order and keeping the first strict improvement is what
    implements that, so an equidistant realized rate beats a median and a
    median beats BCV.
    """
    best: RateResolution | None = None
    best_distance: int | None = None

    for base, quote, source in _FALLBACK_TIERS:
        found = rates_repo.nearest(
            conn, as_of_date=as_of, base=base, quote=quote, source=source
        )
        if found is None:
            continue
        age_days = (as_of - found.as_of_date).days
        distance = abs(age_days)
        if best_distance is not None and distance >= best_distance:
            continue
        best_distance = distance
        best = RateResolution(
            rate=found.rate,
            source=source + NEAREST_SUFFIX,
            as_of_date=found.as_of_date,
            age_days=age_days,
            approximate=True,
        )
    return best


def resolve_detail(
    conn: sqlite3.Connection, txn: Transaction
) -> RateResolution:
    """Resolve ``txn``'s rate and report how the answer was reached.

    The full implementation of the ADR-005 chain; :func:`resolve` is the
    two-value view of it. Sets ``txn.needs_review = True`` as a side effect
    when — and only when — the rates table holds nothing for the pair.
    """
    native_currencies, native_source = _native_usd()
    if (txn.currency or "").upper() in native_currencies:
        # One quote unit per dollar, which is what a dollar is: a caller
        # that divides gets the right answer without a special case.
        return RateResolution(rate=Decimal(1), source=native_source)

    if txn.user_rate is not None:
        return RateResolution(rate=txn.user_rate, source=USER_RATE_SOURCE)

    as_of = txn.occurred_at.date()

    resolution = _in_window_tier(conn, as_of) or _nearest_tier(conn, as_of)
    if resolution is not None:
        return resolution

    txn.needs_review = True
    return RateResolution(rate=None, source=NEEDS_REVIEW_SOURCE)


def resolve(
    conn: sqlite3.Connection, txn: Transaction
) -> tuple[Decimal | None, str]:
    """Resolve ``txn``'s exchange rate via the ADR-005 priority chain.

    Returns ``(rate, source)``. When the rates table holds nothing for the
    pair, returns ``(None, 'needs_review')`` and sets
    ``txn.needs_review = True`` as a side effect so downstream persistence
    layers can flag the row.
    """
    resolution = resolve_detail(conn, txn)
    return resolution.rate, resolution.source


__all__ = [
    "BCV_MAX_AGE_DAYS",
    "BCV_SOURCE",
    "BINANCE_P2P_SOURCE",
    "CARRY_SUFFIX",
    "MEDIAN_MAX_AGE_DAYS",
    "NEAREST_SUFFIX",
    "NEEDS_REVIEW_SOURCE",
    "REALIZED_MAX_AGE_DAYS",
    "REALIZED_SOURCE",
    "RateResolution",
    "USER_RATE_SOURCE",
    "max_age_days",
    "resolve",
    "resolve_detail",
]
