"""Cash USD CLI ingest module (EPIC-011, ADR-008, rule-008, rule-010).

Single cash account in v1 is ``Cash USD`` (kind=cash, currency=USD). This
module exposes the business primitives the Typer ``cash`` subcommand calls
into; the subcommand itself lives in :mod:`finances.cli.main`.

Invariants:

* Every inserted row has ``source='cash_cli'`` and a UUIDv4 ``source_ref``
  (rule-010 — cash entries have no stable external ID).
* Expenses are stored with a **negative** ``amount`` so they decrease the
  ``Cash USD`` balance in ``v_account_balances``.
* Inserts flow through :func:`transactions_repo.upsert_by_source_ref` (rule-010
  forbids raw ``INSERT`` into ``transactions``).
"""

from __future__ import annotations

import sqlite3
import uuid
from datetime import datetime
from decimal import Decimal

from finances.db.repos import accounts as accounts_repo
from finances.db.repos import transactions as transactions_repo
from finances.domain.models import (
    Account,
    AccountKind,
    Category,
    Transaction,
    TransactionKind,
)

CASH_USD_ACCOUNT_NAME = "Cash USD"
CASH_CLI_SOURCE = "cash_cli"


def ensure_cash_usd_account(conn: sqlite3.Connection) -> Account:
    """Return the ``Cash USD`` account, creating it on first use.

    Per rule-008, v1 has exactly one cash account. If a row named ``Cash USD``
    already exists but does not match ``kind=cash`` + ``currency=USD``, the
    database is in a state that violates the rule; raise so callers stop
    before writing more cash rows against the wrong account.
    """
    existing = accounts_repo.get_by_name(conn, CASH_USD_ACCOUNT_NAME)
    if existing is not None:
        if existing.kind != AccountKind.CASH or existing.currency != "USD":
            raise ValueError(
                f"Account {CASH_USD_ACCOUNT_NAME!r} exists but is not a USD cash "
                f"account (kind={existing.kind.value}, currency={existing.currency}); "
                "rule-008 forbids reusing the name for a non-USD-cash account."
            )
        return existing
    return accounts_repo.insert(
        conn,
        Account(name=CASH_USD_ACCOUNT_NAME, kind=AccountKind.CASH, currency="USD"),
    )


def suggest_recent_categories(
    conn: sqlite3.Connection, account_id: int, *, limit: int = 5
) -> list[Category]:
    """Return the most recently used expense categories on ``account_id``.

    Ordered by the latest ``occurred_at`` seen per category. Empty list when
    the account has no categorized expense history yet.
    """
    rows = conn.execute(
        """
        SELECT c.id AS id, c.kind AS kind, c.name AS name, c.active AS active
        FROM transactions t
        JOIN categories c ON c.id = t.category_id
        WHERE t.account_id = ? AND c.kind = 'expense' AND c.active = 1
        GROUP BY c.id
        ORDER BY MAX(t.occurred_at) DESC, c.name ASC
        LIMIT ?
        """,
        (account_id, limit),
    ).fetchall()
    return [
        Category(
            id=row["id"],
            kind=TransactionKind(row["kind"]),
            name=row["name"],
            active=bool(row["active"]),
        )
        for row in rows
    ]


def add_cash_expense(
    conn: sqlite3.Connection,
    *,
    amount: Decimal | str | int,
    description: str,
    occurred_at: datetime,
    category_id: int | None = None,
    source_ref: str | None = None,
) -> Transaction:
    """Record a USD cash expense on the ``Cash USD`` account.

    ``amount`` is the positive USD value the user paid; the negative sign is
    applied internally so the balance decreases by the expected amount.
    ``source_ref`` defaults to a fresh UUIDv4 per rule-010.
    """
    amt = amount if isinstance(amount, Decimal) else Decimal(str(amount))
    if amt <= 0:
        raise ValueError(
            "amount must be a positive number of USD; expense sign is applied internally"
        )
    account = ensure_cash_usd_account(conn)
    assert account.id is not None  # ensure_cash_usd_account always sets id
    ref = source_ref if source_ref is not None else str(uuid.uuid4())
    txn = Transaction(
        account_id=account.id,
        occurred_at=occurred_at,
        kind=TransactionKind.EXPENSE,
        amount=-amt,
        currency="USD",
        description=description,
        category_id=category_id,
        source=CASH_CLI_SOURCE,
        source_ref=ref,
        needs_review=False,
    )
    result = transactions_repo.upsert_by_source_ref(conn, txn)
    persisted = transactions_repo.get_by_id(conn, result["id"])
    assert persisted is not None  # just-inserted row must be fetchable
    return persisted


__all__ = [
    "CASH_CLI_SOURCE",
    "CASH_USD_ACCOUNT_NAME",
    "add_cash_expense",
    "ensure_cash_usd_account",
    "suggest_recent_categories",
]
