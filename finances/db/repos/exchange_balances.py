"""What the exchange says it holds, per position, per moment (ADR-023).

Pydantic in, Pydantic out (rule-009). ``balance`` never leaves this module
as anything but a ``Decimal`` — it is the figure the ledger is measured
against, and a float here would be a rounding defect in the measuring
stick itself.
"""

from __future__ import annotations

import sqlite3
from datetime import datetime
from decimal import Decimal

from finances.domain.models import ExchangeBalance


def _to_text(value: Decimal) -> str:
    return format(value, "f")


def _iso(value: datetime) -> str:
    return value.isoformat()


def _row_to_balance(row: sqlite3.Row) -> ExchangeBalance:
    return ExchangeBalance(
        id=row["id"],
        account_id=row["account_id"],
        currency=row["currency"],
        balance=Decimal(str(row["balance"])),
        captured_at=row["captured_at"],
        source=row["source"],
    )


def insert(conn: sqlite3.Connection, balance: ExchangeBalance) -> ExchangeBalance:
    """Record one capture, replacing any capture of the same moment.

    Two syncs inside one second would otherwise leave the check a choice
    of two answers for a single instant. The (account, currency,
    captured_at) UNIQUE plus ON CONFLICT makes the later read win, which
    is the one the exchange answered most recently.
    """
    cur = conn.execute(
        """
        INSERT INTO exchange_balances
            (account_id, currency, balance, captured_at, source)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT (account_id, currency, captured_at)
        DO UPDATE SET balance = excluded.balance, source = excluded.source
        """,
        (
            balance.account_id,
            balance.currency,
            _to_text(balance.balance),
            _iso(balance.captured_at),
            balance.source,
        ),
    )
    row_id = cur.lastrowid
    if not row_id:
        found = conn.execute(
            """
            SELECT id FROM exchange_balances
             WHERE account_id = ? AND currency = ? AND captured_at = ?
            """,
            (balance.account_id, balance.currency, _iso(balance.captured_at)),
        ).fetchone()
        row_id = int(found["id"])
    return balance.model_copy(update={"id": row_id})


def latest_per_position(
    conn: sqlite3.Connection,
) -> dict[tuple[int, str], ExchangeBalance]:
    """The newest capture for every ``(account_id, currency)``.

    Keyed by position rather than by account: Spot holds USDT and USDC,
    and the defect this exists for shows in one and not the other.
    """
    rows = conn.execute(
        """
        SELECT b.id, b.account_id, b.currency, b.balance, b.captured_at, b.source
          FROM exchange_balances AS b
          JOIN (
                SELECT account_id, currency, MAX(captured_at) AS captured_at
                  FROM exchange_balances
                 GROUP BY account_id, currency
          ) AS newest
            ON newest.account_id = b.account_id
           AND newest.currency = b.currency
           AND newest.captured_at = b.captured_at
        """
    ).fetchall()
    return {(r["account_id"], r["currency"]): _row_to_balance(r) for r in rows}


__all__ = ["insert", "latest_per_position"]
