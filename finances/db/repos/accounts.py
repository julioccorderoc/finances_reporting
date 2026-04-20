from __future__ import annotations

import sqlite3

from finances.domain.models import Account, AccountKind


def _row_to_account(row: sqlite3.Row) -> Account:
    return Account(
        id=row["id"],
        name=row["name"],
        kind=AccountKind(row["kind"]),
        currency=row["currency"],
        institution=row["institution"],
        active=bool(row["active"]),
    )


def insert(conn: sqlite3.Connection, account: Account) -> Account:
    cur = conn.execute(
        """
        INSERT INTO accounts (name, kind, currency, institution, active)
        VALUES (?, ?, ?, ?, ?)
        """,
        (
            account.name,
            account.kind.value,
            account.currency,
            account.institution,
            1 if account.active else 0,
        ),
    )
    return account.model_copy(update={"id": cur.lastrowid})


def get_by_id(conn: sqlite3.Connection, account_id: int) -> Account | None:
    row = conn.execute(
        "SELECT id, name, kind, currency, institution, active FROM accounts WHERE id = ?",
        (account_id,),
    ).fetchone()
    return _row_to_account(row) if row else None


def get_by_name(conn: sqlite3.Connection, name: str) -> Account | None:
    row = conn.execute(
        "SELECT id, name, kind, currency, institution, active FROM accounts WHERE name = ?",
        (name,),
    ).fetchone()
    return _row_to_account(row) if row else None


def list_all(conn: sqlite3.Connection, *, include_inactive: bool = False) -> list[Account]:
    if include_inactive:
        rows = conn.execute(
            "SELECT id, name, kind, currency, institution, active FROM accounts ORDER BY name"
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT id, name, kind, currency, institution, active FROM accounts WHERE active = 1 ORDER BY name"
        ).fetchall()
    return [_row_to_account(r) for r in rows]
