"""Changing money into dollar bills (ADR-008 amendment 2026-09-04).

The owner sends USDT, or pays out bolívares, and receives physical dollars.
The ledger already knows this shape — rows 859/5740 and 863/5741 are the two
2025 conversions — but until now the only way to record one was a script.

What it is, in ledger terms: a double-entry transfer (rule-002). The outgoing
row is promoted to a leg, a ``Cash USD`` leg is inserted carrying the dollars
actually received, and the two share a ``transfer_id``. Nothing is spent —
:mod:`finances.domain.money` already excludes currency movement from every
income and expense aggregate, which is precisely why recording it as an
expense (what it looks like today) overstates spending by the whole amount.

**Why not** ``create_transfer``'s anchor-only mode: it copies one amount to
both legs. That is right for a same-currency move and quietly wrong here,
where 36,000 bolívares become 40 dollars. So the cash leg is inserted first
and the two rows are paired in both-anchors mode, which also means both legs
pass through ``_promote_to_transfer`` and leave a pre-image — so
:func:`finances.domain.transfers.unpair` can take the whole thing back.

**The rate.** When the legs disagree in value the outgoing row gets a
``user_rate`` holding the price the exchange was actually struck at (ADR-015:
quote units per dollar), which is what makes ``transfers.validate`` see the
pair sum to zero. It is written through ``transactions_repo.update`` rather
than raw SQL so it lands in the owner's edit history like any other rate.

**ADR-008.** ``Cash USD`` is written by the cash module and by transfer
pairing — from the CLI or the viewer, never by an importer. This is the
pairing half of that sentence. The leg keeps ``source='internal'`` and a
deterministic ref (rule-010), matching the 2025 rows; it does not route
through ``cash_cli``, whose UUIDv4 refs are right for a hand-typed entry and
wrong for a leg derived from a row that already exists.
"""

from __future__ import annotations

import sqlite3
from decimal import ROUND_HALF_UP, Decimal

from pydantic import BaseModel, ConfigDict

from finances.db.repos import transactions as txn_repo
from finances.domain import transfers
from finances.domain.models import Transaction, TransactionKind

#: Where a conversion's dollars land. Resolved by kind, not by id: rule-008
#: allows exactly one cash account in v1, and hardcoding 5 would make this
#: module wrong on any database seeded in a different order.
_CASH_KIND = "cash"

#: The struck rate's precision, in quote units per dollar. Four places is
#: what the ledger stores elsewhere and is finer than any counterparty quotes.
_RATE_QUANTUM = Decimal("0.0001")

#: Sources whose rows are the ledger's own corrections. Same set ADR-022's
#: delete refuses (§2.3) — a plug is not a payment that became cash.
_REFUSED_SOURCES: frozenset[str] = transfers.UNBREAKABLE_PAIR_SOURCES


class CashConversion(BaseModel):
    """What a recorded conversion produced, for the caller to name."""

    model_config = ConfigDict(frozen=True)

    transfer_id: str
    anchor_transaction_id: int
    cash_transaction_id: int
    usd_received: Decimal
    struck_rate: Decimal | None
    """Quote units per dollar, or ``None`` when the legs were already 1:1."""


def cash_account_id(conn: sqlite3.Connection) -> int:
    """The one account physical dollars live in (rule-008)."""
    rows = conn.execute(
        "SELECT id FROM accounts WHERE kind = ? AND active = 1 ORDER BY id",
        (_CASH_KIND,),
    ).fetchall()
    if len(rows) != 1:
        raise ValueError(
            f"rule-008 expects exactly one active cash account; found {len(rows)}"
        )
    return int(rows[0]["id"])


def _struck_rate(anchor_amount: Decimal, usd_received: Decimal) -> Decimal | None:
    """Quote units per dollar, or ``None`` when the legs already agree.

    A dollar-equivalent row whose whole amount arrived in bills is 1:1 by
    ADR-015 and needs no rate — rows 859 and 863 carry none. Anything else
    does, including a dollar move that lost something on the way: 580 USDT
    for $575 is not 1:1 however alike the units look.
    """
    rate = (abs(anchor_amount) / usd_received).quantize(
        _RATE_QUANTUM, rounding=ROUND_HALF_UP
    )
    return None if rate == Decimal(1) else rate


def convert_to_cash(
    conn: sqlite3.Connection,
    *,
    transaction_id: int,
    usd_received: Decimal,
    description: str | None = None,
) -> CashConversion:
    """Record that ``transaction_id`` is money the owner changed into dollars.

    ``usd_received`` is what actually came back in bills — the one thing the
    ledger cannot derive, because the rate a counterparty struck in a doorway
    is not the rate any feed publishes.

    Raises :class:`LookupError` if the row is gone, :class:`ValueError` for
    every row this is not a sensible thing to say about.
    """
    anchor = txn_repo.get_by_id(conn, transaction_id)
    if anchor is None:
        raise LookupError(f"transaction id={transaction_id} not found")

    if usd_received <= 0:
        raise ValueError("dollars received must be positive")
    if anchor.transfer_id is not None:
        raise ValueError(
            "this row is already half of a transfer — break the pair first if "
            "it is wrong"
        )
    if anchor.amount >= 0:
        raise ValueError(
            "a conversion records money leaving an account to come back as "
            "bills; this row is money arriving"
        )
    if anchor.source in _REFUSED_SOURCES:
        raise ValueError(
            f"a '{anchor.source}' row is the ledger's own correction, not a "
            "payment that became cash (ADR-018 / ADR-020)"
        )

    cash_id = cash_account_id(conn)
    if anchor.account_id == cash_id:
        raise ValueError("this row is already cash; nothing would move")

    rate = _struck_rate(anchor.amount, usd_received)

    conn.execute("SAVEPOINT convert_to_cash")
    try:
        if rate is not None:
            txn_repo.update(conn, id=anchor.id, user_rate=rate)

        cash_leg = txn_repo.insert(
            conn,
            Transaction(
                account_id=cash_id,
                occurred_at=anchor.occurred_at,
                # Born a transfer, so unpairing leaves an orphan half-transfer
                # rather than a phantom income row inflating every report.
                kind=TransactionKind.TRANSFER,
                amount=usd_received,
                currency="USD",
                description=description or _default_description(anchor),
                source="internal",
                source_ref=f"cash:{anchor.source}:{anchor.id}",
                needs_review=False,
            ),
        )
        assert cash_leg.id is not None

        pair = transfers.create_transfer(
            conn,
            anchor_transaction_id=anchor.id,
            counterpart_transaction_id=cash_leg.id,
        )
    except Exception:
        conn.execute("ROLLBACK TO convert_to_cash")
        raise
    finally:
        conn.execute("RELEASE convert_to_cash")

    return CashConversion(
        transfer_id=pair.transfer_id,
        anchor_transaction_id=anchor.id,
        cash_transaction_id=cash_leg.id,
        usd_received=usd_received,
        struck_rate=rate,
    )


def _default_description(anchor: Transaction) -> str:
    """Names the row the bills came from, the way the 2025 legs do.

    Rows 5740/5741 read "Cash received — cambio $700 efectivo Jorge": the
    words that made the original row recognisable, carried across. The
    machine-readable half stays on the anchor.
    """
    origin = (anchor.description or "").strip()
    return f"Cash received — {origin}" if origin else "Cash received"


__all__ = [
    "CashConversion",
    "cash_account_id",
    "convert_to_cash",
]
