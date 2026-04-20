from __future__ import annotations

import sqlite3
from decimal import Decimal
from typing import Any

from finances.domain.models import Transaction, TransactionKind


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
