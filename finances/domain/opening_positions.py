"""Opening positions for history the custodian no longer serves (ADR-020).

ADR-018 closed these gaps with an adjustment dated the day the reconciliation
ran. That row corrects the balance on its date and then freezes into history,
so it never repairs the hole underneath and every later repair to historical
rows silently invalidates it. Worse, a residual absorbs whatever is wrong
upstream: the 2026-08-04 plugs were sized nine minutes after a re-sync had
duplicated 105 events, fit that corruption exactly, and left ``finances
doctor`` reporting a healthy ledger.

An opening position is dated at the ledger's start instead, and carries a
**stable** ``source_ref``, so restating it replaces the prior statement rather
than layering another correction on top. One opening position per
``(account, currency)``, however many times it is restated.

Two shapes, and choosing between them is the whole design:

**Transfer** — when the ledger *overstates* a position. It does not need a
negative opening balance; it needs the outbound movement nobody recorded.
Binance stops serving internal-transfer history after six months, so the
Spot↔Funding movements explaining the split cannot be recovered row by row —
but that they happened, and in what net amount, is computable.

**Balance** — when the ledger *understates* a position: value held before the
books began, whose arrival predates any servable history.

Because the overstated side is expressed as movement, every opening balance is
strictly positive. ADR-018's objection — that a negative opening balance
asserts the owner began with less than nothing — becomes an invariant this
module enforces rather than an argument against the approach: a gap that would
require one is refused.

The custodian figure is always an input (ADR-018 §2.3, carried forward). A
ledger reconciled against the API that filled it agrees with itself.
"""
from __future__ import annotations

import enum
import sqlite3
from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel, ConfigDict

from finances.config import CARACAS_TZ
from finances.db.repos import transactions as txn_repo
from finances.domain.models import Transaction, TransactionKind
from finances.domain.reconciliation_adjustments import (
    NEGLIGIBLE,
    position_balance,
)
from finances.domain.transfers import create_transfer

SOURCE = "opening_balance"

SQL_LEDGER_START = "SELECT MIN(occurred_at) AS start FROM transactions"


class OpeningShape(str, enum.Enum):
    """Which of the two honest readings of a gap was used."""

    BALANCE = "balance"
    TRANSFER = "transfer"


class OpeningResult(BaseModel):
    """What one restatement did."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    account_id: int
    currency: str
    shape: OpeningShape
    ledger_balance: Decimal
    actual_balance: Decimal
    delta: Decimal
    transaction_ids: tuple[int, ...]
    counterpart_account_id: int | None = None


def ledger_start(conn: sqlite3.Connection) -> datetime:
    """The earliest instant the ledger knows about.

    Opening rows are dated here so they fall inside every reporting window —
    which is what makes period balances reconcile, and is precisely what a
    plug dated part-way through the history cannot do.
    """
    row = conn.execute(SQL_LEDGER_START).fetchone()
    raw = row["start"] if row is not None else None
    if raw is None:
        return datetime.now(tz=CARACAS_TZ)
    if isinstance(raw, datetime):
        parsed = raw
    else:
        parsed = datetime.fromisoformat(str(raw))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=CARACAS_TZ)
    return parsed


def _owned_refs(account_id: int, currency: str) -> tuple[str, str]:
    """The ``source_ref`` patterns a position is responsible for.

    A position owns its own opening balance, and it owns any opening transfer
    it *emits* — the overstated side is the one being corrected, so the
    transfer belongs to it. Restating the receiving side must not silently
    undo a neighbour's correction, which is why this is not simply
    ``source = 'opening_balance'``.
    """
    currency = currency.upper()
    return (
        f"opening:{account_id}:{currency}",
        f"opening-transfer:{account_id}:%:{currency}:%",
    )


def clear_opening(
    conn: sqlite3.Connection, *, account_id: int, currency: str
) -> int:
    """Delete the opening rows this position owns. Returns rows removed.

    Transfer legs are removed as a pair: deleting one half would leave the
    other stranded and break rule-002's leg count.
    """
    balance_ref, transfer_pattern = _owned_refs(account_id, currency)

    transfer_ids = [
        row["transfer_id"]
        for row in conn.execute(
            "SELECT DISTINCT transfer_id FROM transactions"
            " WHERE source = ? AND source_ref LIKE ? AND transfer_id IS NOT NULL",
            (SOURCE, transfer_pattern),
        ).fetchall()
    ]

    removed = conn.execute(
        "DELETE FROM transactions WHERE source = ? AND source_ref = ?",
        (SOURCE, balance_ref),
    ).rowcount

    for transfer_id in transfer_ids:
        removed += conn.execute(
            "DELETE FROM transactions WHERE source = ? AND transfer_id = ?",
            (SOURCE, transfer_id),
        ).rowcount

    return removed


def record_opening(
    conn: sqlite3.Connection,
    *,
    account_id: int,
    currency: str,
    actual: Decimal,
    counterpart_account_id: int | None = None,
    occurred_at: datetime | None = None,
) -> OpeningResult | None:
    """State what a position held before the ledger began. ``None`` if exact.

    Any prior opening rows this position owns are replaced, so calling this
    twice with different figures restates rather than accumulates.

    Raises ``ValueError`` when the ledger overstates the position and no
    ``counterpart_account_id`` is given: the arithmetic would demand a
    negative opening balance, and the gap has been mis-modelled.
    """
    currency = currency.upper()
    when = occurred_at or ledger_start(conn)

    # Clear first so the delta is measured against the ledger *without* any
    # previous statement — otherwise a restatement would correct a correction.
    clear_opening(conn, account_id=account_id, currency=currency)

    ledger = position_balance(conn, account_id=account_id, currency=currency)
    delta = actual - ledger

    if abs(delta) < NEGLIGIBLE:
        return None

    if delta < 0:
        if counterpart_account_id is None:
            raise ValueError(
                f"account {account_id} {currency} is overstated by {-delta}. "
                "Closing that with an opening balance would require a "
                "negative one, which asserts the owner began with less than "
                "nothing. Supply the account the value moved to so it can be "
                "recorded as the transfer it was (ADR-020 §2.1)."
            )
        return _record_transfer(
            conn,
            account_id=account_id,
            counterpart_account_id=counterpart_account_id,
            currency=currency,
            ledger=ledger,
            actual=actual,
            delta=delta,
            when=when,
        )

    return _record_balance(
        conn,
        account_id=account_id,
        currency=currency,
        ledger=ledger,
        actual=actual,
        delta=delta,
        when=when,
    )


def _record_balance(
    conn: sqlite3.Connection,
    *,
    account_id: int,
    currency: str,
    ledger: Decimal,
    actual: Decimal,
    delta: Decimal,
    when: datetime,
) -> OpeningResult:
    inserted = txn_repo.insert(
        conn,
        Transaction(
            account_id=account_id,
            occurred_at=when,
            kind=TransactionKind.ADJUSTMENT,
            amount=delta,
            currency=currency,
            description=(
                f"Opening balance {currency}: value held before this ledger "
                "began, whose arrival predates any history the custodian "
                f"still serves. Ledger {ledger}, custodian {actual}."
            ),
            source=SOURCE,
            source_ref=f"opening:{account_id}:{currency}",
            needs_review=False,
        ),
    )
    assert inserted.id is not None  # insert() always populates id
    return OpeningResult(
        account_id=account_id,
        currency=currency,
        shape=OpeningShape.BALANCE,
        ledger_balance=ledger,
        actual_balance=actual,
        delta=delta,
        transaction_ids=(inserted.id,),
    )


def _record_transfer(
    conn: sqlite3.Connection,
    *,
    account_id: int,
    counterpart_account_id: int,
    currency: str,
    ledger: Decimal,
    actual: Decimal,
    delta: Decimal,
    when: datetime,
) -> OpeningResult:
    moved = -delta  # delta < 0, so this is what left the position
    pair = create_transfer(
        conn,
        from_account_id=account_id,
        to_account_id=counterpart_account_id,
        amount=moved,
        currency=currency,
        occurred_at=when,
        description=(
            f"Opening transfer {currency}: movement between the owner's own "
            "positions that the custodian no longer serves history for. "
            f"Ledger {ledger}, custodian {actual}. Recorded once, at ledger "
            "start."
        ),
        source=SOURCE,
        source_ref_from=(
            f"opening-transfer:{account_id}:{counterpart_account_id}:{currency}:from"
        ),
        source_ref_to=(
            f"opening-transfer:{account_id}:{counterpart_account_id}:{currency}:to"
        ),
    )
    return OpeningResult(
        account_id=account_id,
        currency=currency,
        shape=OpeningShape.TRANSFER,
        ledger_balance=ledger,
        actual_balance=actual,
        delta=delta,
        transaction_ids=(pair.from_transaction_id, pair.to_transaction_id),
        counterpart_account_id=counterpart_account_id,
    )


__all__ = [
    "SOURCE",
    "OpeningResult",
    "OpeningShape",
    "clear_opening",
    "ledger_start",
    "record_opening",
]
