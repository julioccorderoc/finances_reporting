"""«Became cash» and «Unpair», as the viewer needs them.

The domain owns both writes (:mod:`finances.domain.cash_conversion`,
:func:`finances.domain.transfers.unpair`). This module owns the three things
only the viewer cares about:

* :func:`can_convert` — whether the footer shows the control. It is the same
  predicate the panel route enforces, so a hidden button and a refused POST
  can never disagree; the hiding is a courtesy, the refusal is the guard.
* :func:`suggested_usd` — what the amount field is pre-filled with.
* the two ``describe_*`` functions — the toast, which is the only place that
  can say what happened once the modal has closed.

Nothing here runs SQL against ``transactions`` (rule-012).
"""

from __future__ import annotations

import sqlite3
from decimal import ROUND_HALF_UP, Decimal

from finances.domain import cash_conversion, money, transfers
from finances.domain.models import Transaction
from finances.format import fmt_native


def can_convert(conn: sqlite3.Connection, txn: Transaction) -> bool:
    """Whether "this became cash" is a sensible thing to say about ``txn``.

    Money must be leaving, it must not already be paired, and it must not
    already be cash. Mirrors the refusals in
    :func:`~finances.domain.cash_conversion.convert_to_cash` — which still
    re-checks every one of them, because a crafted POST never sees a template.
    """
    if txn.transfer_id is not None or txn.amount >= 0:
        return False
    if txn.source in transfers.UNBREAKABLE_PAIR_SOURCES:
        return False
    try:
        return txn.account_id != cash_conversion.cash_account_id(conn)
    except ValueError:
        # No single cash account (rule-008 violated, or a partial fixture):
        # offering a control that cannot work is worse than offering none.
        return False


def can_unpair(txn: Transaction) -> bool:
    """Whether the footer offers to break this row's pair.

    Only that the row *is* paired. Whether a pre-image exists to replay is
    the domain's question, and its refusal is worth showing: "this pair was
    made before the ledger started recording how" is information, where a
    silently missing button is not.
    """
    return txn.transfer_id is not None


def suggested_usd(conn: sqlite3.Connection, txn: Transaction) -> Decimal | None:
    """The amount field's pre-fill: what the ledger already prices the row at.

    A guess, and named as one in the template. The dollars that actually came
    back are the one number the ledger cannot derive — the rate struck in a
    doorway is not the rate any feed publishes — so this only saves typing
    when the guess happens to be right, and must never be submitted unread.
    """
    amount_usd, _ = money.to_usd(conn, txn)
    if amount_usd is None:
        return None
    return abs(amount_usd).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def describe_conversion(result: cash_conversion.CashConversion) -> str:
    """One line naming the dollars and the price they were struck at."""
    dollars = fmt_native(result.usd_received, "USD")
    if result.struck_rate is None:
        return f"Recorded as cash — {dollars}"
    return f"Recorded as cash — {dollars} at {result.struck_rate}"


def describe_unpair(legs: list[Transaction], account_names: dict[int, str]) -> str:
    """One line saying the pair is broken and both rows are still there.

    Unpair deletes nothing on purpose, so the toast has to say so — otherwise
    the owner reasonably assumes the row they just took back has gone, and a
    stray Cash USD leg sits in the ledger inflating a balance nobody is
    looking at.
    """
    names = [account_names.get(leg.account_id, "row") for leg in legs]
    listed = " and ".join(dict.fromkeys(names)) or "both rows"
    return f"Pair broken — the {listed} rows are still here; delete one if it should not exist"
