"""Transaction edit-modal write flow (EPIC-024 / Phase 3 / ADR-012).

This module is the *only* write path the web viewer uses for editing a
transaction. Per rule-012 it must:

* validate input via Pydantic v2 (``TransactionEditRequest``),
* delegate the SQL UPDATE to ``finances.db.repos.transactions.update``,
* re-run :func:`finances.domain.rates.resolve` so ``needs_review`` is
  re-derived (never set manually) per ADR-005 / ADR-012,
* return a freshly-projected ``TransactionCard`` so the caller can swap
  the row card directly.

The two-step "set then reconcile" pattern mirrors the dashboard handler
that already calls ``rates.resolve`` after a write.
"""

from __future__ import annotations

import sqlite3
from decimal import Decimal

from pydantic import BaseModel, ConfigDict

from finances.db.repos import categories as categories_repo
from finances.db.repos import transactions as transactions_repo
from finances.domain import rates as rates_engine
from finances.web.services.transactions_query import (
    TransactionCard,
    _project_card,
)


class TransactionEditRequest(BaseModel):
    """Modal save payload.

    The two ``set_*`` flags disambiguate "field omitted → leave alone"
    from "field present with value None → clear it". Since WP2
    (ux-overhaul) the modals dirty-track their controls and submit
    ``set_*=True`` only for fields the user actually touched; clearing
    a category is an explicit control. JSON callers may still send any
    ``set_*`` combination for partial updates.
    """

    model_config = ConfigDict(extra="forbid")

    set_category: bool = False
    category_id: int | None = None
    set_user_rate: bool = False
    user_rate: Decimal | None = None


def apply_edit(
    conn: sqlite3.Connection,
    *,
    txn_id: int,
    req: TransactionEditRequest,
) -> TransactionCard:
    """Apply ``req`` to txn ``txn_id`` and return the updated card.

    Steps (ADR-005, ADR-012, rule-005, rule-012):

    1. Apply ``category_id`` and/or ``user_rate`` updates via
       ``transactions_repo.update``.
    2. Re-fetch the updated ``Transaction``.
    3. Call ``rates.resolve(conn, txn)`` to derive ``(rate, source)``.
    4. Compute ``needs_review = (source == NEEDS_REVIEW_SOURCE)``.
    5. If it changed, call the repo a second time with only that field.
    6. Project a card via the existing ``_project_card`` helper.
    """
    existing = transactions_repo.get_by_id(conn, txn_id)
    if existing is None:
        raise LookupError(f"transaction id={txn_id} not found")

    if req.set_category and req.category_id is not None:
        category = categories_repo.get_by_id(conn, req.category_id)
        if category is None:
            raise ValueError(f"unknown category id={req.category_id}")

    update_kwargs: dict[str, object] = {}
    if req.set_category:
        update_kwargs["category_id"] = req.category_id
    if req.set_user_rate:
        update_kwargs["user_rate"] = req.user_rate

    if update_kwargs:
        transactions_repo.update(conn, id=txn_id, **update_kwargs)

    # Step 2 — re-fetch.
    txn = transactions_repo.get_by_id(conn, txn_id)
    if txn is None:  # pragma: no cover - defensive
        raise LookupError(f"transaction id={txn_id} not found after update")

    # Step 3 — re-derive rate/source. ``resolve`` may flip needs_review on
    # the in-memory object as a side effect; we don't depend on that.
    _rate, source = rates_engine.resolve(conn, txn)
    derived_needs_review = source == rates_engine.NEEDS_REVIEW_SOURCE

    # Step 4-5 — reconcile DB state.
    if derived_needs_review != txn.needs_review:
        transactions_repo.update(
            conn, id=txn_id, needs_review=derived_needs_review
        )
        txn = transactions_repo.get_by_id(conn, txn_id)
        assert txn is not None  # narrows for the type checker

    # Step 6 — project the card.
    account_row = conn.execute(
        "SELECT name FROM accounts WHERE id = ?", (txn.account_id,)
    ).fetchone()
    account_name = account_row["name"] if account_row else ""

    category_name: str | None = None
    if txn.category_id is not None:
        cat = categories_repo.get_by_id(conn, txn.category_id)
        category_name = cat.name if cat else None

    return _project_card(
        conn,
        txn,
        account_name=account_name,
        category_name=category_name,
    )


__all__ = [
    "TransactionEditRequest",
    "apply_edit",
]
