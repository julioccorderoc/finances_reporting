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


def to_usd(
    conn: sqlite3.Connection, txn: Transaction
) -> tuple[Decimal | None, str]:
    """Convert ``txn`` to USD. The only implementation of this arithmetic.

    Returns ``(amount_usd, rate_source)``. ``amount_usd`` is ``None`` exactly
    when the resolver could not price the row, in which case ``rate_source``
    is ``needs_review``.

    A native-USD currency short-circuits before the resolver is consulted,
    which is why a P2P fill's ``user_rate`` — the realized VES price of that
    fill, recorded as provenance — is never mistaken for a conversion factor
    on the USDT row that carries it.

    All arithmetic is ``Decimal``; floats never enter the path (ADR-009).
    """
    if txn.currency in NATIVE_USD_CURRENCIES:
        return txn.amount, NATIVE_USD_SOURCE

    rate, source = rates_engine.resolve(conn, txn)
    if rate is None:
        return None, source
    return txn.amount / rate, source


def is_bcv_sourced(source: str) -> bool:
    """Whether a ``rate_source`` label came from the BCV fallback tier."""
    return source.startswith(BCV_SOURCE_PREFIX)


__all__ = [
    "BCV_SOURCE_PREFIX",
    "MOVEMENT_CATEGORY_KIND",
    "NATIVE_USD_CURRENCIES",
    "NATIVE_USD_SOURCE",
    "SQL_NOT_CURRENCY_MOVEMENT",
    "is_bcv_sourced",
    "is_currency_movement",
    "movement_category_ids",
    "to_usd",
]
