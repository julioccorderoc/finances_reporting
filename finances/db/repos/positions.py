from __future__ import annotations

import sqlite3
from decimal import Decimal

from finances.domain.models import EarnPosition


def _to_text(value: Decimal | None) -> str | None:
    if value is None:
        return None
    return format(value, "f")


def _iso(value) -> str | None:
    if value is None:
        return None
    return value.isoformat() if hasattr(value, "isoformat") else str(value)


def _row_to_position(row: sqlite3.Row) -> EarnPosition:
    return EarnPosition(
        id=row["id"],
        account_id=row["account_id"],
        product_id=row["product_id"],
        asset=row["asset"],
        principal=(
            row["principal"]
            if isinstance(row["principal"], Decimal)
            else Decimal(str(row["principal"]))
        ),
        apy=(
            None
            if row["apy"] is None
            else (row["apy"] if isinstance(row["apy"], Decimal) else Decimal(str(row["apy"])))
        ),
        started_at=row["started_at"],
        ended_at=row["ended_at"],
        snapshot_at=row["snapshot_at"],
    )


def insert(conn: sqlite3.Connection, position: EarnPosition) -> EarnPosition:
    if position.snapshot_at is None:
        cur = conn.execute(
            """
            INSERT INTO earn_positions (
                account_id, product_id, asset, principal, apy,
                started_at, ended_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                position.account_id,
                position.product_id,
                position.asset,
                _to_text(position.principal),
                _to_text(position.apy),
                _iso(position.started_at),
                _iso(position.ended_at),
            ),
        )
    else:
        cur = conn.execute(
            """
            INSERT INTO earn_positions (
                account_id, product_id, asset, principal, apy,
                started_at, ended_at, snapshot_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                position.account_id,
                position.product_id,
                position.asset,
                _to_text(position.principal),
                _to_text(position.apy),
                _iso(position.started_at),
                _iso(position.ended_at),
                _iso(position.snapshot_at),
            ),
        )
    return position.model_copy(update={"id": cur.lastrowid})


def close(conn: sqlite3.Connection, position_id: int, ended_at) -> None:
    conn.execute(
        "UPDATE earn_positions SET ended_at = ? WHERE id = ?",
        (_iso(ended_at), position_id),
    )


def list_open(conn: sqlite3.Connection, account_id: int | None = None) -> list[EarnPosition]:
    if account_id is None:
        rows = conn.execute(
            """
            SELECT id, account_id, product_id, asset, principal, apy,
                   started_at, ended_at, snapshot_at
            FROM earn_positions
            WHERE ended_at IS NULL
            ORDER BY started_at DESC
            """
        ).fetchall()
    else:
        rows = conn.execute(
            """
            SELECT id, account_id, product_id, asset, principal, apy,
                   started_at, ended_at, snapshot_at
            FROM earn_positions
            WHERE ended_at IS NULL AND account_id = ?
            ORDER BY started_at DESC
            """,
            (account_id,),
        ).fetchall()
    return [_row_to_position(r) for r in rows]
