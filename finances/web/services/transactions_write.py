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
from finances.domain import realized_rates
from finances.domain.money import MOVEMENT_CATEGORY_KIND
from finances.domain.models import Category, Transaction, TransactionKind
from finances.web.services.transactions_query import (
    TransactionCard,
    _project_card,
)


class TransactionEditRequest(BaseModel):
    """Modal save payload.

    The ``set_*`` flags disambiguate "field omitted → leave alone"
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
    set_notes: bool = False
    notes: str | None = None


def _is_p2p_sell(txn: Transaction) -> bool:
    """Does this row feed the realized cost basis?

    Mirrors the selection in ``realized_rates.SQL_P2P_SELLS`` — minus its
    ``user_rate IS NOT NULL`` clause, because clearing a rate must also
    trigger a rebuild (it removes a fill from the day's average).

    Keyed off ``source_ref`` and sign rather than ``kind`` for the reason
    ADR-013 gives: bank-anchored pairing promotes these rows to
    ``kind='transfer'`` after the fact, so ``kind`` is not stable.
    """
    return txn.source_ref.startswith("p2p:") and txn.amount < 0


def category_fits(txn_kind: TransactionKind, category_kind: TransactionKind) -> bool:
    """Whether a category of ``category_kind`` may be applied to a row.

    The predicate behind :func:`_reject_contradictory_category`, exposed so
    the triage queue can apply it to a *guess* before offering it. A guess
    the save path would refuse is a 422 waiting for a click, and the
    accept-the-guess chip resolves a row without opening the modal.

    Asymmetric on purpose — see :func:`_reject_contradictory_category`.
    """
    return category_kind is MOVEMENT_CATEGORY_KIND or category_kind is txn_kind


def _reject_contradictory_category(txn: Transaction, category: Category) -> None:
    """Refuse a category whose kind contradicts the row's kind.

    ``categories.kind`` existed from the first migration and nothing ever
    compared it to ``transactions.kind``; the picker offered every category
    on every row. The live ledger accumulated 65 contradictions, among them
    6 income rows filed under ``Fees``.

    The rule is asymmetric on purpose. A **transfer**-kind category on an
    income or expense row is meaningful — it is the owner asserting that the
    money moved rather than being spent, which
    :mod:`finances.domain.money` now acts on. Only an expense category on an
    income row, or the reverse, is nonsense, and only that is refused.

    Raises ``ValueError`` (the 400 surface) before anything is written, so a
    rejected save leaves every field of the row alone.
    """
    if category_fits(txn.kind, category.kind):
        return
    raise ValueError(
        f"category {category.name!r} is a {category.kind.value} category and "
        f"cannot be applied to a {txn.kind.value} transaction"
    )


def apply_edit(
    conn: sqlite3.Connection,
    *,
    txn_id: int,
    req: TransactionEditRequest,
) -> TransactionCard:
    """Apply ``req`` to txn ``txn_id`` and return the updated card.

    Steps (ADR-005, ADR-012, ADR-013, rule-005, rule-012):

    1. Apply ``category_id``, ``user_rate`` and/or ``notes`` updates via
       ``transactions_repo.update``.
    2. Re-fetch the updated ``Transaction``.
    3. If the edit changed ``user_rate`` on a P2P sell, rebuild the
       realized cost basis.
    4. Call ``rates.resolve(conn, txn)`` to derive ``(rate, source)``.
    5. Compute ``needs_review = (source == NEEDS_REVIEW_SOURCE)``.
    6. If it changed, call the repo a second time with only that field.
    7. Project a card via the existing ``_project_card`` helper.

    Step 3 exists because this function is the single choke point for
    every web ``user_rate`` write — the triage modal, the
    ``/transactions`` modal and ``PATCH /api/transactions/{id}``. A P2P
    sell's ``user_rate`` is an *input* to the materialised
    ``binance_p2p_realized`` tier, so editing one without recomputing
    leaves every bolívar row priced off a stale basis until the next
    Binance ingest silently re-prices all of them (ADR-013 Amendment
    2026-07-26).
    """
    existing = transactions_repo.get_by_id(conn, txn_id)
    if existing is None:
        raise LookupError(f"transaction id={txn_id} not found")

    if req.set_category and req.category_id is not None:
        category = categories_repo.get_by_id(conn, req.category_id)
        if category is None:
            raise ValueError(f"unknown category id={req.category_id}")
        _reject_contradictory_category(existing, category)

    update_kwargs: dict[str, object] = {}
    if req.set_category:
        update_kwargs["category_id"] = req.category_id
    if req.set_user_rate:
        update_kwargs["user_rate"] = req.user_rate
    if req.set_notes:
        update_kwargs["notes"] = req.notes

    if update_kwargs:
        transactions_repo.update(conn, id=txn_id, **update_kwargs)

    # Step 2 — re-fetch.
    txn = transactions_repo.get_by_id(conn, txn_id)
    if txn is None:  # pragma: no cover - defensive
        raise LookupError(f"transaction id={txn_id} not found after update")

    # Step 3 — the edited row may be an input to the realized tier.
    # Narrow on purpose: a category- or notes-only save changes nothing
    # the basis reads, and re-saving the same rate is a keystroke, not a
    # change of basis. Measured at ~5 ms on the live ledger (123 fills →
    # 101 daily rates), against the ~16 ms the triage caller already
    # spends rebuilding the queue twice around this call.
    #
    # Ordered BEFORE the resolve below so nothing in the response is
    # derived from a basis this request already invalidated. Defensive
    # today — every fill in the ledger is USDT-denominated, so the edited
    # row itself takes the native_usd path.
    if (
        req.set_user_rate
        and _is_p2p_sell(txn)
        and txn.user_rate != existing.user_rate
    ):
        realized_rates.rebuild(conn)

    # Step 4 — re-derive rate/source. ``resolve`` may flip needs_review on
    # the in-memory object as a side effect; we don't depend on that.
    #
    # This used to run the whole VES ladder against native-USD rows, which
    # was harmless only by luck: the guard is now inside the resolver
    # (ADR-021 §2.3), above the ``user_rate`` branch, so a USDT row whose
    # ``user_rate`` records the bolívar price of a P2P fill cannot have that
    # price read as a conversion factor here or anywhere else.
    #
    # ``needs_review`` now means "the rates table holds nothing for this
    # pair". A row the chain can only approximate resolves to a
    # ``*_nearest`` source and is NOT flagged — an approximate rate must
    # never block a triage sitting (design criterion D6).
    _rate, source = rates_engine.resolve(conn, txn)
    derived_needs_review = source == rates_engine.NEEDS_REVIEW_SOURCE

    # Step 5-6 — reconcile DB state.
    if derived_needs_review != txn.needs_review:
        transactions_repo.update(
            conn, id=txn_id, needs_review=derived_needs_review
        )
        txn = transactions_repo.get_by_id(conn, txn_id)
        assert txn is not None  # narrows for the type checker

    # Step 7 — project the card.
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
    "category_fits",
]
