"""One definition of "a dollar", and one of "this is not spending".

Two facts were previously written down in several places each, and both had
already drifted by the time anyone looked:

* **Which currencies are already dollars** was defined four times, once per
  surface, and the fourth copy carried a different name
  (``_NATIVE_USDT_CURRENCIES`` in ``web/services/net_worth.py``).
* **How to convert a transaction to USD** was written five times. The fifth
  (net worth) never consulted the resolver at all and priced bolívars off a
  source with eight rows in fifteen months.

rule-005 already forbids a second rate chain. This module is where the rest
of the arithmetic that surrounds the chain lives, so a surface can be wrong
about currency handling only by declining to import from here.

The second half of the module answers a different question the ledger got
wrong for months: **what counts as spending?**

``reports`` decided that with a single predicate — ``kind <> 'transfer'``.
But a Binance USDC→USDT convert, a P2P sell whose bank deposit was never
imported, and a USDT send swapped for physical cash are all *currency
movement*, and none of them is ``kind='transfer'``: the first because both
legs live on one account, the second because its counterpart is missing,
the third because its destination account has no rows.

The owner had already marked 46 such rows ``Internal Transfer`` /
``External Transfer`` by hand. Nothing read the column, so $7 526 of money
that only moved between the owner's own accounts was reported as spending
and income. :data:`SQL_NOT_CURRENCY_MOVEMENT` is that judgement, made
load-bearing.
"""

from __future__ import annotations

import sqlite3
from decimal import Decimal

from finances.domain import rates as rates_engine
from finances.domain.models import Transaction, TransactionKind

# Currencies held 1:1 with USD. A transaction in one of these bypasses the
# resolver entirely — there is nothing to convert.
NATIVE_USD_CURRENCIES = frozenset({"USD", "USDT", "USDC"})

# The ``rate_source`` label reported for that bypass.
NATIVE_USD_SOURCE = "native_usd"

# Sources whose USD value is BCV-derived and therefore barred from any
# headline figure (ADR-005 amendment 2026-04-19). Matched by prefix so
# ``bcv`` and ``bcv_carry`` are both caught.
BCV_SOURCE_PREFIX = "bcv"

# A category of this kind, applied to a row that is *not* kind='transfer',
# is the owner asserting "this money moved, it was not spent".
MOVEMENT_CATEGORY_KIND = TransactionKind.TRANSFER

# The predicate every income/expense aggregate must apply. Kept as SQL
# rather than a Python filter so the report modules share one string
# instead of one convention.
#
# ``kind <> 'transfer'`` alone was the old rule and is still half of this:
# a properly paired transfer is excluded by kind. The second clause adds
# the rows that are movement but could never be paired.
SQL_NOT_CURRENCY_MOVEMENT = """
    kind <> 'transfer'
    AND kind <> 'adjustment'
    AND (
        category_id IS NULL
        OR category_id NOT IN (SELECT id FROM categories WHERE kind = 'transfer')
    )
"""


def movement_category_ids(conn: sqlite3.Connection) -> frozenset[int]:
    """Category ids that mean "currency movement, not spending".

    Fetched once per report rather than joined per row; there are two of
    them and a report iterates thousands of transactions.
    """
    rows = conn.execute(
        "SELECT id FROM categories WHERE kind = ?",
        (MOVEMENT_CATEGORY_KIND.value,),
    ).fetchall()
    return frozenset(int(r["id"]) for r in rows)


def is_currency_movement(txn: Transaction, movement_ids: frozenset[int]) -> bool:
    """Whether ``txn`` moved money rather than earning or spending it.

    True for a paired transfer, and for any row the owner has categorised
    under a transfer-kind category. ``movement_ids`` comes from
    :func:`movement_category_ids`.
    """
    if txn.kind is TransactionKind.TRANSFER:
        return True
    return txn.category_id is not None and txn.category_id in movement_ids


def to_usd_detail(
    conn: sqlite3.Connection, txn: Transaction
) -> tuple[Decimal | None, rates_engine.RateResolution]:
    """Convert ``txn`` to USD, keeping the resolution that produced it.

    The only implementation of this arithmetic. ``amount_usd`` is ``None``
    exactly when the chain could not price the row at all, in which case the
    resolution's source is ``needs_review``.

    A native-USD currency passes through *unchanged* rather than being
    divided by the resolver's rate of one: division renormalises a Decimal's
    exponent, and a ledger that turns ``-12.50`` into ``-12.5`` on its way to
    a report is doing arithmetic nobody asked for.

    The guard against reading a P2P fill's ``user_rate`` — the bolívar price
    the fill was struck at — as a conversion factor now lives in the
    resolver itself (ADR-021 §2.3), so every caller has it, not just this
    one.

    Callers that need only the label use :func:`to_usd`. The card projection
    and the triage queue need the rate and its date too, to say *how*
    approximate an approximation is.

    All arithmetic is ``Decimal``; floats never enter the path (ADR-009).
    """
    resolution = rates_engine.resolve_detail(conn, txn)
    if resolution.source == NATIVE_USD_SOURCE:
        return txn.amount, resolution
    if resolution.rate is None:
        return None, resolution
    return txn.amount / resolution.rate, resolution


def to_usd(
    conn: sqlite3.Connection, txn: Transaction
) -> tuple[Decimal | None, str]:
    """Convert ``txn`` to USD, as ``(amount_usd, rate_source)``.

    The two-value view of :func:`to_usd_detail`, which is what most callers
    want. ``rate_source`` carries the full provenance, including ADR-021's
    ``_nearest`` suffix — read it with :func:`is_approximate` and
    :func:`is_bcv_sourced`, never by re-deriving it from dates.
    """
    amount_usd, resolution = to_usd_detail(conn, txn)
    return amount_usd, resolution.source


def to_usd_at(
    amount: Decimal, currency: str, rate: Decimal | None
) -> Decimal | None:
    """Express ``amount`` in USD at a rate the caller already holds.

    The companion to :func:`to_usd`, for the two callers that are not
    pricing "this transaction, however the resolver would price it":

    * ``transfers.validate`` prices each leg of a pair at that leg's own
      recorded ``user_rate``.
    * the triage modal's rate panel prices one amount at *every* tier, to
      show the owner what each would have been worth.

    Per ADR-015 ``user_rate`` is quote units per dollar — bolívares per
    USDT — so a non-USD amount **divides** by it. Multiplying, which the
    original ``validate`` did, yields VES²/USD: a quantity with no meaning,
    and the reason all 107 cross-currency transfers reported invalid.

    Returns ``None`` when the amount cannot be priced, rather than guessing.
    """
    if currency.upper() in NATIVE_USD_CURRENCIES:
        return amount
    if rate is None or rate <= 0:
        return None
    return amount / rate


def is_bcv_sourced(source: str) -> bool:
    """Whether a ``rate_source`` label came from the BCV fallback tier.

    Prefix-matched, so ``bcv``, ``bcv_carry`` and ADR-021's ``bcv_nearest``
    are all caught: an approximation of a BCV rate is still a BCV claim and
    stays barred from every headline and from net worth.
    """
    return source.startswith(BCV_SOURCE_PREFIX)


def is_approximate(source: str) -> bool:
    """Whether a ``rate_source`` came from ADR-021's nearest-rate branch.

    The suffix is the whole provenance: no consumer may re-derive
    "is this an approximation" from dates or amounts (rule-005). This is
    the one reader, and ``TransactionCard.approximate`` /
    ``ConsolidatedRow.is_approximate`` are the one derivation each.

    Independent of :func:`is_bcv_sourced`. A row can be both (``bcv_nearest``),
    either, or neither, and collapsing the two axes would lose a fact:
    "priced off the official floor" and "priced off a rate from outside
    every window" are different weaknesses.
    """
    return source.endswith(rates_engine.NEAREST_SUFFIX)


__all__ = [
    "BCV_SOURCE_PREFIX",
    "MOVEMENT_CATEGORY_KIND",
    "NATIVE_USD_CURRENCIES",
    "NATIVE_USD_SOURCE",
    "SQL_NOT_CURRENCY_MOVEMENT",
    "is_approximate",
    "is_bcv_sourced",
    "is_currency_movement",
    "movement_category_ids",
    "to_usd",
    "to_usd_detail",
    "to_usd_at",
]
