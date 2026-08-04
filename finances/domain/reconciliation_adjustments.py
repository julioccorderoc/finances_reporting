"""Reconciling a position against the custodian that holds it (ADR-018).

Binance serves internal-transfer history for six months. This ledger's
history starts in October 2025, so the Spot↔Funding movements that would
explain most of its Binance imbalance no longer exist anywhere: a query
starting six months back succeeds, seven months back fails with ``-5026``.
The ingest is not at fault — it chunks well inside the documented range cap,
logs the refusal and declines to advance its watermark. There is simply
nothing left to read.

A ledger that says ``-10,084 USDT`` is not inaccurate, it is impossible. You
cannot spend an asset you never received, and ``finances doctor`` calls that
an error precisely so it cannot pass unnoticed.

The ordinary bookkeeping answer to unrecoverable history is an adjusting
entry, and that is what ``kind='adjustment'`` — permitted by the schema since
migration 001 and unused until now — was held in reserve for. An adjustment
asserts that a previous record is incomplete, which is exactly the claim.

**The custodian figure is always an input.** Reading it from the API would
make the ledger agree with the API by construction, which is a tautology
rather than a reconciliation. The owner reads the balance and states it.
"""

from __future__ import annotations

import sqlite3
import uuid
from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel, ConfigDict

from finances.db.repos import transactions as txn_repo
from finances.domain.models import Transaction, TransactionKind

SOURCE = "reconciliation"

# Below this the difference is not worth a permanent row: it is rounding in
# the custodian's own display, not a hole in the history.
NEGLIGIBLE = Decimal("0.00000001")

SQL_POSITION_BALANCE = """
    SELECT COALESCE(SUM(CAST(amount AS TEXT)), '0') AS ignored
      FROM transactions
     WHERE account_id = :account_id AND currency = :currency
"""


class AdjustmentResult(BaseModel):
    """What one reconciliation did."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    transaction_id: int
    account_id: int
    currency: str
    ledger_balance: Decimal
    actual_balance: Decimal
    delta: Decimal


def position_balance(
    conn: sqlite3.Connection,
    *,
    account_id: int,
    currency: str,
) -> Decimal:
    """Sum one ``(account, currency)`` position in ``Decimal``.

    Summed in Python rather than SQL: ``amount`` is TEXT and ``SUM`` would
    coerce it through a float, which is the one thing ADR-009 forbids of
    money. The row counts here are small — one account's holdings of one
    asset — so the cost is irrelevant next to the correctness.
    """
    rows = conn.execute(
        "SELECT amount FROM transactions WHERE account_id = ? AND currency = ?",
        (account_id, currency),
    ).fetchall()
    total = Decimal("0")
    for row in rows:
        raw = row["amount"]
        total += raw if isinstance(raw, Decimal) else Decimal(str(raw))
    return total


def record_adjustment(
    conn: sqlite3.Connection,
    *,
    account_id: int,
    currency: str,
    actual: Decimal,
    occurred_at: datetime,
) -> AdjustmentResult | None:
    """Bring one position to the custodian's figure. Returns None if it agrees.

    The row is dated ``occurred_at`` — the day the reconciliation was
    performed — not the day the ledger begins. The gap being closed is mostly
    *mid-history* movement, so attributing it to an opening position would
    assert something untrue, and for a position the ledger overstates the
    implied opening balance would have to be negative (ADR-018 §2.1).

    Only call this for a gap with no remaining source. A gap an unrun ingest
    would close is a sync that has not happened, and adjusting it
    double-counts the moment those rows arrive.
    """
    currency = currency.upper()
    ledger = position_balance(conn, account_id=account_id, currency=currency)
    delta = actual - ledger

    if abs(delta) < NEGLIGIBLE:
        return None

    when = occurred_at.date().isoformat()
    description = (
        f"Reconciliation to custodian balance: ledger {_plain(ledger)}, "
        f"custodian {_plain(actual)} {currency} as of {when}. "
        "Source history unavailable."
    )

    # A stable ref would collide on a second reconciliation of the same
    # position, which is a legitimate act — positions drift again. A uuid
    # keeps each reconciliation its own event while still satisfying
    # rule-010's requirement that a source_ref always exists.
    source_ref = f"reconcile:{account_id}:{currency}:{uuid.uuid4()}"

    inserted = txn_repo.insert(
        conn,
        Transaction(
            account_id=account_id,
            occurred_at=occurred_at,
            kind=TransactionKind.ADJUSTMENT,
            amount=delta,
            currency=currency,
            description=description,
            source=SOURCE,
            source_ref=source_ref,
            needs_review=False,
        ),
    )
    assert inserted.id is not None  # insert() always populates id

    return AdjustmentResult(
        transaction_id=inserted.id,
        account_id=account_id,
        currency=currency,
        ledger_balance=ledger,
        actual_balance=actual,
        delta=delta,
    )


def _plain(value: Decimal) -> str:
    """Render a Decimal without exponent notation."""
    return format(value, "f")


__all__ = [
    "AdjustmentResult",
    "position_balance",
    "record_adjustment",
]
