"""Bank reversal pairing (ADR-019).

When Provincial rejects a payment it returns the money under a named
marker (``REVERSO CARGO``) — for the payment and again for its
commission — and the owner retries. The reversal and the charge it
undoes are a zero-sum pair on one account: money moved, nothing was
spent. Pairing them under a shared ``transfer_id`` (rule-002 mechanism)
removes both from spending and income everywhere without deleting
statement rows or touching re-ingest idempotency.

``create_transfer`` cannot host this: its same-account/same-currency
guard is correct for transfers ("nothing moved") and precisely wrong
here, where same account, same currency, and an exact zero sum are the
*requirements*.
"""

from __future__ import annotations

import sqlite3
import uuid
from datetime import date, datetime
from decimal import Decimal

from finances.db.repos import transactions as txn_repo
from finances.domain.reconciliation import MatchProposal
from finances.domain.transfers import _promote_to_transfer

# Statement wordings that mark a bank-side return. Extend as new ones
# appear on real statements.
REVERSAL_MARKERS: tuple[str, ...] = ("REVERSO CARGO",)

DEFAULT_WINDOW_DAYS = 4


def _day(value: str | datetime) -> date:
    if isinstance(value, datetime):
        return value.date()
    return datetime.fromisoformat(str(value)).date()


def pair_reversal(
    conn: sqlite3.Connection,
    *,
    charge_transaction_id: int,
    reversal_transaction_id: int,
) -> str:
    """Promote a charge and its reversal into a zero-sum pair.

    Validations are the mirror image of a transfer's: both rows must sit
    on the *same* account in the *same* currency and sum exactly to
    zero. Returns the shared ``transfer_id``.

    The reversal leg sheds its category — historically rule 27 stamped
    it "Fees", which is not income of any kind. The charge leg keeps its
    category (it may be hand triage; reports ignore categories on
    ``kind='transfer'`` rows either way).
    """
    charge = txn_repo.get_by_id(conn, charge_transaction_id)
    reversal = txn_repo.get_by_id(conn, reversal_transaction_id)
    if charge is None or reversal is None:
        raise ValueError("both the charge and the reversal must exist")
    if charge.account_id != reversal.account_id:
        raise ValueError("a reversal undoes a charge on its own account")
    if charge.currency != reversal.currency:
        raise ValueError("a reversal repays the charge's own currency")
    if charge.amount + reversal.amount != 0:
        raise ValueError(
            f"charge {charge.amount} and reversal {reversal.amount} must sum to zero"
        )
    for row in (charge, reversal):
        if row.transfer_id is not None:
            raise ValueError(f"transaction {row.id} is already paired")

    tid = str(uuid.uuid4())
    _promote_to_transfer(conn, transaction_id=charge_transaction_id, transfer_id=tid)
    _promote_to_transfer(conn, transaction_id=reversal_transaction_id, transfer_id=tid)
    conn.execute(
        "UPDATE transactions SET category_id = NULL WHERE id = ?",
        (reversal_transaction_id,),
    )
    return tid


class BankReversalPairing:
    """Strategy: pair each bank reversal with the charge it undoes.

    Candidates for a reversal are unpaired expenses on the same account
    with the exact opposite amount, dated at most ``window_days`` before
    (or on) the reversal's day. Claims are greedy and each charge is
    consumed at most once, per the ADR-002 amendment's reasoning: when a
    failed attempt and its successful retry both qualify (same amount,
    same day), any assignment yields identical totals. Preference order
    keeps the most information: an uncategorized charge is claimed
    before a hand-triaged one, then the closest day, then the newest
    row. Ambiguity is therefore never a reason to strand a reversal as
    phantom income.
    """

    name: str = "bank_reversal_pairing"

    def __init__(
        self,
        conn: sqlite3.Connection,
        *,
        window_days: int = DEFAULT_WINDOW_DAYS,
        source: str = "provincial",
    ) -> None:
        self._conn = conn
        self._window_days = window_days
        self._source = source

    def match(self) -> list[MatchProposal]:
        marker_sql = " OR ".join(
            "description LIKE ?" for _ in REVERSAL_MARKERS
        )
        reversals = self._conn.execute(
            f"""
            SELECT id, account_id, occurred_at, amount, description
            FROM transactions
            WHERE source = ?
              AND kind = 'income'
              AND transfer_id IS NULL
              AND ({marker_sql})
            ORDER BY occurred_at ASC, id ASC
            """,
            (self._source, *[f"%{m}%" for m in REVERSAL_MARKERS]),
        ).fetchall()

        proposals: list[MatchProposal] = []
        claimed: set[int] = set()
        for rev in reversals:
            rev_amount = Decimal(str(rev["amount"]))
            rev_day = _day(rev["occurred_at"])
            candidates = self._conn.execute(
                """
                SELECT id, occurred_at, amount, category_id
                FROM transactions
                WHERE source = ?
                  AND account_id = ?
                  AND kind = 'expense'
                  AND transfer_id IS NULL
                  AND DATE(occurred_at) BETWEEN DATE(?, ?) AND DATE(?)
                """,
                (
                    self._source,
                    rev["account_id"],
                    rev["occurred_at"],
                    f"-{self._window_days} days",
                    rev["occurred_at"],
                ),
            ).fetchall()

            eligible = [
                c
                for c in candidates
                if c["id"] not in claimed
                and Decimal(str(c["amount"])) + rev_amount == 0
            ]
            if not eligible:
                continue
            eligible.sort(
                key=lambda c: (
                    0 if c["category_id"] is None else 1,
                    (rev_day - _day(c["occurred_at"])).days,
                    -c["id"],
                )
            )
            chosen = eligible[0]
            claimed.add(chosen["id"])
            proposals.append(
                MatchProposal(
                    strategy=self.name,
                    details={
                        "charge_transaction_id": chosen["id"],
                        "reversal_transaction_id": rev["id"],
                    },
                )
            )
        return proposals

    def apply(self, proposal: MatchProposal) -> None:
        pair_reversal(
            self._conn,
            charge_transaction_id=proposal.details["charge_transaction_id"],
            reversal_transaction_id=proposal.details["reversal_transaction_id"],
        )


__all__ = [
    "DEFAULT_WINDOW_DAYS",
    "REVERSAL_MARKERS",
    "BankReversalPairing",
    "pair_reversal",
]
