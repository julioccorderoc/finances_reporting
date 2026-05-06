from __future__ import annotations

import sqlite3
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, Final

from finances.domain.models import Transaction, TransactionKind


class _Unset:
    """Sentinel for ``update`` arguments that callers want left untouched.

    A bare ``None`` is a *meaningful* value at the SQL boundary
    (e.g. clear ``user_rate``); we need a third state for "do not touch".
    """

    _instance: "_Unset | None" = None

    def __new__(cls) -> "_Unset":
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __repr__(self) -> str:  # pragma: no cover - debug aid
        return "_UNSET"

    def __bool__(self) -> bool:
        return False


_UNSET: Final[_Unset] = _Unset()


def _to_text(value: Decimal | None) -> str | None:
    if value is None:
        return None
    return format(value, "f")


def _iso(value: Any) -> str | None:
    if value is None:
        return None
    return value.isoformat() if hasattr(value, "isoformat") else str(value)


def _row_to_transaction(row: sqlite3.Row) -> Transaction:
    return Transaction(
        id=row["id"],
        account_id=row["account_id"],
        occurred_at=row["occurred_at"],
        kind=TransactionKind(row["kind"]),
        amount=row["amount"] if isinstance(row["amount"], Decimal) else Decimal(str(row["amount"])),
        currency=row["currency"],
        description=row["description"],
        category_id=row["category_id"],
        transfer_id=row["transfer_id"],
        user_rate=(
            None
            if row["user_rate"] is None
            else (
                row["user_rate"]
                if isinstance(row["user_rate"], Decimal)
                else Decimal(str(row["user_rate"]))
            )
        ),
        source=row["source"],
        source_ref=row["source_ref"],
        needs_review=bool(row["needs_review"]),
    )


def insert(conn: sqlite3.Connection, txn: Transaction) -> Transaction:
    cur = conn.execute(
        """
        INSERT INTO transactions (
            account_id, occurred_at, kind, amount, currency, description,
            category_id, transfer_id, user_rate, source, source_ref, needs_review
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            txn.account_id,
            _iso(txn.occurred_at),
            txn.kind.value,
            _to_text(txn.amount),
            txn.currency,
            txn.description,
            txn.category_id,
            txn.transfer_id,
            _to_text(txn.user_rate),
            txn.source,
            txn.source_ref,
            1 if txn.needs_review else 0,
        ),
    )
    return txn.model_copy(update={"id": cur.lastrowid})


def get_by_id(conn: sqlite3.Connection, transaction_id: int) -> Transaction | None:
    row = conn.execute(
        """
        SELECT id, account_id, occurred_at, kind, amount, currency, description,
               category_id, transfer_id, user_rate, source, source_ref, needs_review
        FROM transactions WHERE id = ?
        """,
        (transaction_id,),
    ).fetchone()
    return _row_to_transaction(row) if row else None


def get_by_source_ref(
    conn: sqlite3.Connection, source: str, source_ref: str
) -> Transaction | None:
    row = conn.execute(
        """
        SELECT id, account_id, occurred_at, kind, amount, currency, description,
               category_id, transfer_id, user_rate, source, source_ref, needs_review
        FROM transactions WHERE source = ? AND source_ref = ?
        """,
        (source, source_ref),
    ).fetchone()
    return _row_to_transaction(row) if row else None


def upsert_by_source_ref(conn: sqlite3.Connection, txn: Transaction) -> dict[str, Any]:
    """Insert-or-update on (source, source_ref) per ADR-010.

    Returns {"rows_inserted": 0|1, "rows_updated": 0|1, "id": int}. A second
    identical call returns rows_inserted=0.
    """
    if txn.source_ref is None:
        raise ValueError("upsert_by_source_ref requires a non-null source_ref")

    existing = get_by_source_ref(conn, txn.source, txn.source_ref)
    params = (
        txn.account_id,
        _iso(txn.occurred_at),
        txn.kind.value,
        _to_text(txn.amount),
        txn.currency,
        txn.description,
        txn.category_id,
        txn.transfer_id,
        _to_text(txn.user_rate),
        txn.source,
        txn.source_ref,
        1 if txn.needs_review else 0,
    )
    conn.execute(
        """
        INSERT INTO transactions (
            account_id, occurred_at, kind, amount, currency, description,
            category_id, transfer_id, user_rate, source, source_ref, needs_review
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(source, source_ref) DO UPDATE SET
            account_id   = excluded.account_id,
            occurred_at  = excluded.occurred_at,
            kind         = excluded.kind,
            amount       = excluded.amount,
            currency     = excluded.currency,
            description  = excluded.description,
            category_id  = excluded.category_id,
            transfer_id  = excluded.transfer_id,
            user_rate    = excluded.user_rate,
            needs_review = excluded.needs_review,
            updated_at   = CURRENT_TIMESTAMP
        """,
        params,
    )

    row = conn.execute(
        "SELECT id FROM transactions WHERE source = ? AND source_ref = ?",
        (txn.source, txn.source_ref),
    ).fetchone()
    row_id = int(row["id"])

    return {
        "rows_inserted": 0 if existing else 1,
        "rows_updated": 1 if existing else 0,
        "id": row_id,
    }


def list_by_account(
    conn: sqlite3.Connection, account_id: int, *, limit: int | None = None
) -> list[Transaction]:
    sql = """
        SELECT id, account_id, occurred_at, kind, amount, currency, description,
               category_id, transfer_id, user_rate, source, source_ref, needs_review
        FROM transactions WHERE account_id = ?
        ORDER BY occurred_at DESC, id DESC
    """
    params: tuple[Any, ...] = (account_id,)
    if limit is not None:
        sql += " LIMIT ?"
        params = (account_id, limit)
    rows = conn.execute(sql, params).fetchall()
    return [_row_to_transaction(r) for r in rows]


def count(conn: sqlite3.Connection) -> int:
    row = conn.execute("SELECT COUNT(*) AS c FROM transactions").fetchone()
    return int(row["c"])


def update(
    conn: sqlite3.Connection,
    *,
    id: int,
    category_id: int | None | _Unset = _UNSET,
    user_rate: Decimal | None | _Unset = _UNSET,
    needs_review: bool | _Unset = _UNSET,
) -> Transaction:
    """Patch a single transaction's mutable fields.

    Only fields explicitly passed (not the ``_UNSET`` sentinel) are
    touched; ``None`` is a real value meaning "clear this column". The
    function always sets ``updated_at`` to the current UTC instant
    regardless of which fields were provided.

    Per ADR-009 / rule-009, the return value is a Pydantic ``Transaction``
    re-fetched from the row's authoritative state.

    Per rule-012, this is the only allowed entry point for the web
    viewer's transaction-edit modal — the viewer must not run its own
    UPDATE statements.
    """
    sets: list[str] = []
    params: list[Any] = []

    if not isinstance(category_id, _Unset):
        sets.append("category_id = ?")
        params.append(category_id)

    if not isinstance(user_rate, _Unset):
        sets.append("user_rate = ?")
        params.append(_to_text(user_rate))

    if not isinstance(needs_review, _Unset):
        sets.append("needs_review = ?")
        params.append(1 if needs_review else 0)

    sets.append("updated_at = ?")
    params.append(datetime.now(tz=UTC).isoformat())

    params.append(id)
    sql = f"UPDATE transactions SET {', '.join(sets)} WHERE id = ?"
    cur = conn.execute(sql, params)
    if cur.rowcount == 0:
        raise LookupError(f"transaction id={id} not found")

    refreshed = get_by_id(conn, id)
    if refreshed is None:  # pragma: no cover - defensive; UPDATE just succeeded
        raise LookupError(f"transaction id={id} disappeared after update")
    return refreshed
