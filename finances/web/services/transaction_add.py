"""Adding a transaction by hand, from the viewer (ADR-008 amendment 2026-09-03).

Built for every account, open for one. The dialog lists **every** active
account and disables all but ``Cash USD``; this module owns both halves of
that sentence, so the ``<option disabled>`` and the server's refusal can
never disagree:

* :func:`entry_accounts` — what the select offers, which entries are
  writable, and what feeds each one that is not.
* :func:`add_transaction` — the write, refusing anything
  :func:`entry_accounts` marked closed. The disabled attribute is a
  courtesy; this is the guard.

Why only cash: every other account in this ledger is a *mirror* of an
outside record — a bank statement, an exchange API. A hand-written row on
one of those does not correct the mirror, it makes the mirror disagree with
the thing it mirrors, and the next ingest cannot tell the hand-written row
from a missed one. Cash has no outside record; the owner IS the source.
When another account acquires a real manual case, one predicate here
changes and its option lights up (rule-008 wants a new ADR first).

The write itself is not implemented here. It routes through
:mod:`finances.ingest.cash_cli` — the same functions ``finances cash add``
calls — so there stays exactly one manual write path in the system, with
one ``source``, one ``source_ref`` scheme (rule-010) and one place the
expense sign is applied.
"""

from __future__ import annotations

import sqlite3
from datetime import date, datetime, time
from decimal import Decimal
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

from finances.config import CARACAS_TZ
from finances.db.repos import accounts as accounts_repo
from finances.db.repos import categories as categories_repo
from finances.domain.models import Account, AccountKind, TransactionKind
from finances.ingest import cash_cli
from finances.web.services.transactions_query import TransactionCard, _project_card
from finances.web.services.transactions_write import category_fits

#: What feeds an account nobody may write to by hand. Keyed by kind because
#: that is what the ledger actually records; the sentence is shown in the
#: disabled option AND quoted back in the 422, so the refusal reads like the
#: dialog rather than like a stack trace.
_FED_BY: dict[AccountKind, str] = {
    AccountKind.BANK: "fed by its statement",
    AccountKind.CRYPTO_SPOT: "fed by the API",
    AccountKind.CRYPTO_FUNDING: "fed by the API",
    AccountKind.CRYPTO_EARN: "fed by the API",
    AccountKind.OTHER: "fed by its importer",
}

_MANUAL_KIND = AccountKind.CASH


class EntryAccount(BaseModel):
    """One row of the dialog's account select."""

    model_config = ConfigDict(extra="forbid")

    id: int
    name: str
    kind: str
    currency: str
    writable: bool
    hint: str | None = None
    """Why this account is closed, in the owner's words. ``None`` when open."""


class NewTransactionRequest(BaseModel):
    """The Add-transaction dialog's payload (rule-009).

    ``amount`` is always positive — the sign belongs to ``kind`` and is
    applied by :mod:`finances.ingest.cash_cli`, not by the caller. Sending a
    negative amount is a mistake worth refusing rather than guessing at.
    """

    model_config = ConfigDict(extra="forbid")

    account_id: int
    occurred_at: date
    kind: Literal["expense", "income"]
    amount: Decimal = Field(gt=0)
    description: str = Field(min_length=1)
    category_id: int | None = None
    notes: str | None = None

    @field_validator("description")
    @classmethod
    def _description_is_not_blank(cls, value: str) -> str:
        stripped = value.strip()
        if not stripped:
            raise ValueError("description must not be blank")
        return stripped

    @field_validator("notes")
    @classmethod
    def _blank_note_is_no_note(cls, value: str | None) -> str | None:
        if value is None:
            return None
        return value.strip() or None


def _writable(account: Account) -> bool:
    """The one predicate. Both the option and the guard read it."""
    return account.kind is _MANUAL_KIND


def _hint_for(account: Account) -> str | None:
    if _writable(account):
        return None
    return _FED_BY.get(account.kind, "fed by its importer")


def entry_accounts(conn: sqlite3.Connection) -> list[EntryAccount]:
    """Every ACTIVE account, cash first, each saying whether it can be written.

    Calls :func:`cash_cli.ensure_cash_usd_account` so the one writable option
    exists before the owner's first cash row rather than after it — the CLI
    has always created it on first use, and a dialog whose only enabled
    option is missing until you have already used the CLI is a dead dialog.
    Idempotent, and it is the account rule-008 fixes for v1, not a new one.
    """
    cash = cash_cli.ensure_cash_usd_account(conn)

    rows: list[EntryAccount] = []
    for account in accounts_repo.list_all(conn):
        assert account.id is not None
        rows.append(
            EntryAccount(
                id=account.id,
                name=account.name,
                kind=account.kind.value,
                currency=account.currency,
                writable=_writable(account),
                hint=_hint_for(account),
            )
        )

    # The one you can act on, first; then the rest by name, so the closed
    # ones read as a list of things that already have a source.
    rows.sort(key=lambda a: (a.id != cash.id, a.name.lower()))
    return rows


def refusal_for(account: Account) -> str:
    """The sentence a closed account is refused with. Plain words, no codes."""
    return (
        f"{account.name} is {_hint_for(account)}, not written by hand. "
        f"{cash_cli.CASH_USD_ACCOUNT_NAME} is the only account you can add a "
        "transaction to here."
    )


def add_transaction(
    conn: sqlite3.Connection, req: NewTransactionRequest
) -> TransactionCard:
    """Write one hand-entered transaction and return its card.

    Raises ``LookupError`` for an account or category that does not exist,
    and ``ValueError`` for one the ledger will not accept — a closed
    account, or a category whose kind contradicts the row's (the same guard
    :func:`transactions_write.apply_edit` applies, because the picker is
    scoped and a crafted POST is not).
    """
    account = accounts_repo.get_by_id(conn, req.account_id)
    if account is None:
        raise LookupError(f"account id={req.account_id} not found")
    if not _writable(account):
        raise ValueError(refusal_for(account))

    kind = TransactionKind(req.kind)

    category_name: str | None = None
    if req.category_id is not None:
        category = categories_repo.get_by_id(conn, req.category_id)
        if category is None:
            raise LookupError(f"category id={req.category_id} not found")
        if not category_fits(kind, category.kind):
            raise ValueError(
                f"category {category.name!r} is a {category.kind.value} category; "
                f"this row is {kind.value}."
            )
        category_name = category.name

    # Midnight Caracas, exactly as ``finances cash add`` dates its rows — the
    # ledger's day boundary is the owner's, not UTC's.
    occurred_at = datetime.combine(req.occurred_at, time(0, 0), tzinfo=CARACAS_TZ)

    writer = (
        cash_cli.add_cash_income
        if kind is TransactionKind.INCOME
        else cash_cli.add_cash_expense
    )
    txn = writer(
        conn,
        amount=req.amount,
        description=req.description,
        occurred_at=occurred_at,
        category_id=req.category_id,
        notes=req.notes,
    )

    return _project_card(
        conn, txn, account_name=account.name, category_name=category_name
    )


__all__ = [
    "EntryAccount",
    "NewTransactionRequest",
    "add_transaction",
    "entry_accounts",
    "refusal_for",
]
