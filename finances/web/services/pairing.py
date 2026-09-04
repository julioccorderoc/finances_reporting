"""Manual pair candidates: a Binance P2P sell → its bank deposit.

:class:`~finances.domain.transfers.BankAnchoredP2pPairing` only proposes a
pairing when exactly one candidate survives its amount tolerance. Round
20 000 Bs deposits repeat often enough that the uniqueness gate skips
them, so the remaining backlog needs a human. This module is the
read-only half of that: it lists what a sell *could* pair with and scores
each option. Writing stays with :func:`~finances.web.services.triage.confirm_pair`
→ :func:`~finances.domain.transfers.create_transfer` (rule-002).

Read-only. Nothing here mutates the database.
"""

from __future__ import annotations

import sqlite3
from datetime import timedelta
from decimal import Decimal

from pydantic import BaseModel, ConfigDict

from finances.web.services.transactions_query import (
    TXN_QUERY_BASE,
    TransactionCard,
    _project_card,
    _row_to_transaction,
)

# What the wrong-side row would have to be. A sell is looking for money
# arriving in the bank, a buy for money leaving it — so the reason has to
# name the side being looked for, or the picker tells the owner a deposit
# is "not a deposit".
_SAME_SIGN_REASON = "same sign — not a deposit"
_SAME_SIGN_REASON_BUY = "same sign — not a debit"

# Both income and expense kinds are returned deliberately: a deposit
# recorded under the wrong kind must stay visible to the human, who is
# the only one who can recognize it.
_CANDIDATE_SQL = (
    TXN_QUERY_BASE
    + """
    WHERE t.source = :bank_source
      AND t.transfer_id IS NULL
      AND t.occurred_at BETWEEN :start AND :end
    ORDER BY t.occurred_at ASC, t.id ASC
"""
)


class PairCandidate(BaseModel):
    """One row a sell could be paired with, plus how well it fits."""

    model_config = ConfigDict(extra="forbid")

    card: TransactionCard
    drift_ratio: Decimal | None
    pairable: bool
    blocked_reason: str | None = None


class PairCandidates(BaseModel):
    """Everything the pair-picker partial needs to render."""

    model_config = ConfigDict(extra="forbid")

    sell: TransactionCard
    expected_ves: Decimal | None
    window_days: int
    candidates: list[PairCandidate]


def find_pair_candidates(
    conn: sqlite3.Connection,
    *,
    sell_id: int,
    window_days: int = 2,
    bank_source: str = "provincial",
) -> PairCandidates:
    """List unpaired bank rows near ``sell_id``, closest amount first.

    Raises ``LookupError`` when ``sell_id`` does not exist.
    """
    sell_row = conn.execute(TXN_QUERY_BASE + " WHERE t.id = ?", (sell_id,)).fetchone()
    if sell_row is None:
        raise LookupError(f"transaction id={sell_id} not found")

    sell_txn = _row_to_transaction(sell_row)
    sell_card = _project_card(
        conn,
        sell_txn,
        account_name=sell_row["account_name"] or "",
        category_name=sell_row["category_name"],
    )

    # The sell is in USDT; user_rate is VES per USDT. Without it there is
    # nothing to score against, but the rows are still worth showing.
    expected_ves: Decimal | None = None
    if sell_txn.user_rate is not None and sell_txn.user_rate > 0:
        expected_ves = abs(sell_txn.amount) * sell_txn.user_rate

    start = sell_txn.occurred_at - timedelta(days=window_days)
    end = sell_txn.occurred_at + timedelta(days=window_days)
    rows = conn.execute(
        _CANDIDATE_SQL,
        {
            "bank_source": bank_source,
            "start": start.isoformat(),
            "end": end.isoformat(),
        },
    ).fetchall()

    sell_is_negative = sell_txn.amount < 0

    candidates: list[PairCandidate] = []
    for row in rows:
        txn = _row_to_transaction(row)
        card = _project_card(
            conn,
            txn,
            account_name=row["account_name"] or "",
            category_name=row["category_name"],
        )

        drift: Decimal | None = None
        if expected_ves is not None and txn.amount != 0:
            drift = abs(abs(txn.amount) - expected_ves) / abs(txn.amount)

        # create_transfer infers the from/to leg from the two signs and
        # rejects a pair that shares one. Surface that up front instead of
        # letting the click 422.
        pairable = (txn.amount < 0) != sell_is_negative
        blocked = _SAME_SIGN_REASON if sell_is_negative else _SAME_SIGN_REASON_BUY

        candidates.append(
            PairCandidate(
                card=card,
                drift_ratio=drift,
                pairable=pairable,
                blocked_reason=None if pairable else blocked,
            )
        )

    # Closest amount first; unscored rows last; date as the tiebreaker.
    candidates.sort(
        key=lambda c: (
            c.drift_ratio is None,
            c.drift_ratio if c.drift_ratio is not None else Decimal(0),
            c.card.occurred_at,
        )
    )

    return PairCandidates(
        sell=sell_card,
        expected_ves=expected_ves,
        window_days=window_days,
        candidates=candidates,
    )


__all__ = ["PairCandidate", "PairCandidates", "find_pair_candidates"]
