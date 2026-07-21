"""Triage queue builder + pair-confirm wrapper (EPIC-025 / Phase 4 / ADR-012).

Per rule-012, this module reuses existing domain primitives:

* RATE / CATEGORY items are surfaced by reading ``transactions`` rows
  via the canonical ``transactions`` repo + projecting through the
  Phase 2/3 ``_project_card`` helper. We do not re-implement
  rate-resolver logic here; the projection delegates to
  :func:`finances.domain.rates.resolve`.
* PAIR items wrap :class:`finances.domain.transfers.BankAnchoredP2pPairing`
  proposals; we don't re-implement matching logic.
* :func:`confirm_pair` delegates to :func:`finances.domain.transfers.create_transfer`
  (mode 3 — both anchors). The web layer never executes its own
  INSERT/UPDATE on ``transactions``.

The "Skip → bottom" affordance is intentionally session-local. Items are
stored in ``app.state.skipped_triage_ids`` (a per-process ``set[str]``)
and reset on app restart. Multi-worker uvicorn deployments do not sync
these sets, which is acceptable for v1 (single-user, single-worker).
"""

from __future__ import annotations

import sqlite3
from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from finances.db.repos import categories as categories_repo
from finances.domain import transfers as transfers_domain
from finances.domain.transfers import BankAnchoredP2pPairing, create_transfer
from finances.web.services.transactions_query import (
    TransactionCard,
    _project_card,
    _row_to_transaction,
)


# ---------------------------------------------------------------------------
# Public types
# ---------------------------------------------------------------------------


class TriageType(StrEnum):
    """Discriminator for unified-queue items."""

    RATE = "rate"
    CATEGORY = "category"
    PAIR = "pair"


class PairProposal(BaseModel):
    """One BankAnchoredP2pPairing proposal projected for the UI."""

    model_config = ConfigDict(extra="forbid")

    proposal_id: str
    deposit: TransactionCard
    sell: TransactionCard
    confidence: float
    details: dict[str, Any]


class TriageItem(BaseModel):
    """One item in the unified queue.

    Type-discriminated payload: only ``txn_card`` (for RATE/CATEGORY) or
    ``pair_proposal`` (for PAIR) is populated per item.
    """

    model_config = ConfigDict(extra="forbid")

    item_id: str
    type: TriageType
    sort_key: datetime
    txn_card: TransactionCard | None = None
    txn_issue_badges: list[str] = Field(default_factory=list)
    pair_proposal: PairProposal | None = None


class TriageQueue(BaseModel):
    """Full unified-queue payload."""

    model_config = ConfigDict(extra="forbid")

    items: list[TriageItem]
    counts: dict[TriageType, int]
    integrity_warnings: list[str]


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


_TXN_QUERY_BASE = """
    SELECT
        t.id, t.account_id, t.occurred_at, t.kind, t.amount, t.currency,
        t.description, t.category_id, t.transfer_id, t.user_rate,
        t.source, t.source_ref, t.needs_review, t.notes,
        a.name AS account_name,
        c.name AS category_name
    FROM transactions t
    LEFT JOIN accounts a ON a.id = t.account_id
    LEFT JOIN categories c ON c.id = t.category_id
"""


def _fetch_txn_with_labels(conn: sqlite3.Connection, txn_id: int):
    return conn.execute(
        _TXN_QUERY_BASE + " WHERE t.id = ?", (txn_id,)
    ).fetchone()


def _project_from_row(conn: sqlite3.Connection, row) -> TransactionCard:
    txn = _row_to_transaction(row)
    return _project_card(
        conn,
        txn,
        account_name=row["account_name"] or "",
        category_name=row["category_name"],
    )


# ---------------------------------------------------------------------------
# build_queue
# ---------------------------------------------------------------------------


def _collect_txn_items(
    conn: sqlite3.Connection,
) -> list[TriageItem]:
    """Read needs_review + missing-category rows and merge into items.

    A single transaction with both issues becomes ONE item with two
    badges. Transfer/adjustment rows are excluded from the CATEGORY
    surface — those rarely carry a category by design and surfacing them
    would noise the queue per the Q9 spec.
    """
    rate_rows = conn.execute(
        _TXN_QUERY_BASE
        + """
        WHERE t.needs_review = 1
        ORDER BY t.occurred_at, t.id
        """
    ).fetchall()

    cat_rows = conn.execute(
        _TXN_QUERY_BASE
        + """
        WHERE t.category_id IS NULL
          AND t.kind NOT IN ('transfer', 'adjustment')
        ORDER BY t.occurred_at, t.id
        """
    ).fetchall()

    # Index both sets by id so a single txn that hits both can merge.
    by_id: dict[int, dict[str, Any]] = {}
    for row in rate_rows:
        by_id.setdefault(int(row["id"]), {"row": row, "badges": []})[
            "badges"
        ].append("rate")
    for row in cat_rows:
        entry = by_id.setdefault(int(row["id"]), {"row": row, "badges": []})
        entry["badges"].append("category")

    items: list[TriageItem] = []
    for txn_id, entry in by_id.items():
        row = entry["row"]
        badges: list[str] = entry["badges"]
        # Pick discriminator type: a merged item picks the more "blocking"
        # of the two. RATE wins because a missing rate yields a None USD
        # amount (more visible to triage).
        item_type = TriageType.RATE if "rate" in badges else TriageType.CATEGORY
        card = _project_from_row(conn, row)
        items.append(
            TriageItem(
                item_id=f"txn:{txn_id}",
                type=item_type,
                sort_key=card.occurred_at,
                txn_card=card,
                txn_issue_badges=sorted(set(badges)),
                pair_proposal=None,
            )
        )

    return items


def _collect_pair_items(conn: sqlite3.Connection) -> list[TriageItem]:
    """Run BankAnchoredP2pPairing and project proposals."""
    strategy = BankAnchoredP2pPairing(conn)
    proposals = strategy.match()

    items: list[TriageItem] = []
    for proposal in proposals:
        # Per finances/domain/transfers.py, the strategy returns
        # ``bank_transaction_id`` and ``binance_transaction_id`` keys.
        deposit_id = int(proposal.details["bank_transaction_id"])
        sell_id = int(proposal.details["binance_transaction_id"])

        deposit_row = _fetch_txn_with_labels(conn, deposit_id)
        sell_row = _fetch_txn_with_labels(conn, sell_id)
        if deposit_row is None or sell_row is None:
            # Defensive: strategy emitted ids that don't exist anymore.
            continue

        deposit_card = _project_from_row(conn, deposit_row)
        sell_card = _project_from_row(conn, sell_row)

        proposal_id = f"{deposit_id}:{sell_id}"
        items.append(
            TriageItem(
                item_id=f"pair:{proposal_id}",
                type=TriageType.PAIR,
                sort_key=deposit_card.occurred_at,
                txn_card=None,
                txn_issue_badges=[],
                pair_proposal=PairProposal(
                    proposal_id=proposal_id,
                    deposit=deposit_card,
                    sell=sell_card,
                    confidence=proposal.confidence,
                    details=dict(proposal.details),
                ),
            )
        )

    return items


def _build_integrity_warnings(conn: sqlite3.Connection) -> list[str]:
    """Surface unreconciled transfers as human-readable warnings.

    Read straight from :func:`finances.domain.transfers.find_unreconciled`
    so the wording stays under the canonical view.
    """
    rows = transfers_domain.find_unreconciled(conn)
    warnings: list[str] = []
    for row in rows:
        tid = row.get("transfer_id")
        leg_count = row.get("leg_count")
        if tid is None:
            warnings.append(
                f"{leg_count} transfer leg(s) carry NULL transfer_id "
                "(missing pairing)"
            )
        else:
            warnings.append(
                f"transfer_id={tid} has only {leg_count} leg(s) (expected 2)"
            )
    return warnings


def build_queue(
    conn: sqlite3.Connection,
    *,
    type_filter: TriageType | None = None,
    skipped_ids: set[str] | None = None,
) -> TriageQueue:
    """Assemble the unified triage queue.

    Order of operations:
      1) Collect txn-issue items (RATE / CATEGORY, merging duplicates).
      2) Collect pair items (BankAnchoredP2pPairing.match).
      3) Sort all items by (sort_key, item_id) — oldest-first, with item_id
         as a mandatory tiebreak for the many rows sharing a timestamp.
      4) Compute counts on the unfiltered set.
      5) Apply ``type_filter`` if provided.
      6) Move items in ``skipped_ids`` to the bottom (preserve relative
         order).
      7) Build integrity warnings from unreconciled transfers.
    """
    txn_items = _collect_txn_items(conn)
    pair_items = _collect_pair_items(conn)
    all_items = txn_items + pair_items
    # (sort_key, item_id) is a TOTAL order. sort_key alone is not: 204 of 243
    # live items share a timestamp, so ties would fall through to SQLite's
    # row order, which changes with the query plan. item_id sorts as a string
    # ("txn:9" after "txn:10"), which is arbitrary but stable — and stability,
    # not numeric ordering, is what prev/next navigation needs.
    all_items.sort(key=lambda it: (it.sort_key, it.item_id))

    # Counts always reflect the UNFILTERED queue, so the chip badges stay
    # accurate when the user selects a single type.
    counts = {t: 0 for t in TriageType}
    for item in all_items:
        counts[item.type] += 1

    if type_filter is not None:
        filtered = [it for it in all_items if it.type == type_filter]
    else:
        filtered = list(all_items)

    if skipped_ids:
        kept = [it for it in filtered if it.item_id not in skipped_ids]
        sunk = [it for it in filtered if it.item_id in skipped_ids]
        ordered = kept + sunk
    else:
        ordered = filtered

    warnings = _build_integrity_warnings(conn)

    return TriageQueue(
        items=ordered,
        counts=counts,
        integrity_warnings=warnings,
    )


# ---------------------------------------------------------------------------
# confirm_pair — wraps create_transfer
# ---------------------------------------------------------------------------


def confirm_pair(
    conn: sqlite3.Connection,
    *,
    deposit_id: int,
    sell_id: int,
) -> dict[str, Any]:
    """Pair a bank deposit + Binance sell via :func:`create_transfer`.

    Validates that both transactions exist and neither already carries a
    transfer_id; otherwise raises ``LookupError`` (404 surface) or
    ``ValueError`` (400 surface).

    Returns a dict ready for JSON serialization::

        {"transfer_id": str, "from_transaction_id": int, "to_transaction_id": int}
    """
    from finances.db.repos import transactions as transactions_repo

    deposit = transactions_repo.get_by_id(conn, deposit_id)
    sell = transactions_repo.get_by_id(conn, sell_id)

    if deposit is None:
        raise LookupError(f"transaction id={deposit_id} not found")
    if sell is None:
        raise LookupError(f"transaction id={sell_id} not found")

    if deposit.transfer_id is not None:
        raise ValueError(
            f"transaction id={deposit_id} is already part of a transfer "
            f"(transfer_id={deposit.transfer_id})"
        )
    if sell.transfer_id is not None:
        raise ValueError(
            f"transaction id={sell_id} is already part of a transfer "
            f"(transfer_id={sell.transfer_id})"
        )

    pair = create_transfer(
        conn,
        anchor_transaction_id=deposit_id,
        counterpart_transaction_id=sell_id,
    )

    return {
        "transfer_id": pair.transfer_id,
        "from_transaction_id": pair.from_transaction_id,
        "to_transaction_id": pair.to_transaction_id,
    }


# ---------------------------------------------------------------------------
# Skip-store helpers (session-local, in-memory)
# ---------------------------------------------------------------------------


def get_skip_store(app) -> set[str]:
    """Return (creating if missing) the per-app skipped-ids set."""
    store = getattr(app.state, "skipped_triage_ids", None)
    if store is None:
        store = set()
        app.state.skipped_triage_ids = store
    return store


__all__ = [
    "PairProposal",
    "TriageItem",
    "TriageQueue",
    "TriageType",
    "build_queue",
    "confirm_pair",
    "get_skip_store",
]
