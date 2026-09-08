"""Move P2P and Pay rows onto the wallet that funded them (ADR-024).

The ingest hardcoded Spot for both classes for about a year. Binance
settles P2P in the Funding wallet and spends Binance Pay from it, so 173
rows and −13,748.63 USDT sat on a wallet that never held them, while the
ledger's own transfers put 9,842.05 into Funding that nothing there ever
spent.

This is a repair of history, run explicitly. It is deliberately *not* a
migration: every ``finances`` command applies pending migrations against
the live ledger, so a migration would move 173 rows the first time the
owner typed anything, with no dry run and nothing read first.

Only ``account_id`` changes. ``source_ref`` in particular is untouched —
it is the dedup key (ADR-010), and rewriting it would make every repaired
row re-import as a new event on the next sync.
"""

from __future__ import annotations

import sqlite3
from decimal import Decimal

from pydantic import BaseModel, ConfigDict

from finances.db.repos import accounts as accounts_repo

SPOT_ACCOUNT_NAME = "Binance Spot"
FUNDING_ACCOUNT_NAME = "Binance Funding"

# The two event classes Binance settles in the Funding wallet. Matched on
# source_ref prefix, which is what the ingest writes and what the backfill
# reproduced: ``p2p:<orderNumber>`` and ``pay:<orderId>``.
FUNDING_SOURCE_REF_PREFIXES: tuple[str, ...] = ("p2p:", "pay:")


class WalletRepairReport(BaseModel):
    """What the repair did, or would do under ``dry_run``.

    ``totals_by_currency`` exists because a count is not checkable. A
    repair of this size is read against the exchange's own figure before
    it is trusted, and that comparison needs the amount.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    moved: int
    totals_by_currency: dict[str, Decimal]
    dry_run: bool = False


def _account_id(conn: sqlite3.Connection, name: str) -> int:
    account = accounts_repo.get_by_name(conn, name)
    if account is None or account.id is None:
        raise RuntimeError(f"account {name!r} must exist before repairing wallets")
    return account.id


def repair_wallet_attribution(
    conn: sqlite3.Connection, *, dry_run: bool = False
) -> WalletRepairReport:
    """Move every P2P and Pay row off Spot and onto Funding.

    Defined by where a row *is*, not by what it is: rows the backfill
    already placed on Funding are left alone, which is also what makes
    re-running this a no-op.
    """
    spot_id = _account_id(conn, SPOT_ACCOUNT_NAME)
    funding_id = _account_id(conn, FUNDING_ACCOUNT_NAME)

    predicate = " OR ".join("source_ref LIKE ?" for _ in FUNDING_SOURCE_REF_PREFIXES)
    params = [f"{prefix}%" for prefix in FUNDING_SOURCE_REF_PREFIXES]

    rows = conn.execute(
        f"""
        SELECT id, currency, amount FROM transactions
         WHERE account_id = ? AND ({predicate})
         ORDER BY id
        """,
        (spot_id, *params),
    ).fetchall()

    totals: dict[str, Decimal] = {}
    for row in rows:
        currency = str(row["currency"])
        totals[currency] = totals.get(currency, Decimal(0)) + Decimal(
            str(row["amount"])
        )

    if rows and not dry_run:
        conn.executemany(
            "UPDATE transactions SET account_id = ? WHERE id = ?",
            [(funding_id, int(row["id"])) for row in rows],
        )

    return WalletRepairReport(
        moved=len(rows), totals_by_currency=totals, dry_run=dry_run
    )


__all__ = [
    "FUNDING_SOURCE_REF_PREFIXES",
    "WalletRepairReport",
    "repair_wallet_attribution",
]
