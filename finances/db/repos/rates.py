from __future__ import annotations

import sqlite3
from datetime import date
from decimal import Decimal

from finances.domain.models import Rate


def _to_text(value: Decimal) -> str:
    return format(value, "f")


def _row_to_rate(row: sqlite3.Row) -> Rate:
    return Rate(
        id=row["id"],
        as_of_date=row["as_of_date"] if isinstance(row["as_of_date"], date) else date.fromisoformat(row["as_of_date"]),
        base=row["base"],
        quote=row["quote"],
        rate=row["rate"] if isinstance(row["rate"], Decimal) else Decimal(str(row["rate"])),
        source=row["source"],
    )


def insert(conn: sqlite3.Connection, rate: Rate) -> Rate:
    cur = conn.execute(
        """
        INSERT INTO rates (as_of_date, base, quote, rate, source)
        VALUES (?, ?, ?, ?, ?)
        """,
        (
            rate.as_of_date.isoformat(),
            rate.base,
            rate.quote,
            _to_text(rate.rate),
            rate.source,
        ),
    )
    return rate.model_copy(update={"id": cur.lastrowid})


def upsert(conn: sqlite3.Connection, rate: Rate) -> Rate:
    conn.execute(
        """
        INSERT INTO rates (as_of_date, base, quote, rate, source)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(as_of_date, base, quote, source) DO UPDATE SET
            rate = excluded.rate
        """,
        (
            rate.as_of_date.isoformat(),
            rate.base,
            rate.quote,
            _to_text(rate.rate),
            rate.source,
        ),
    )
    row = conn.execute(
        """
        SELECT id, as_of_date, base, quote, rate, source
        FROM rates
        WHERE as_of_date = ? AND base = ? AND quote = ? AND source = ?
        """,
        (rate.as_of_date.isoformat(), rate.base, rate.quote, rate.source),
    ).fetchone()
    return _row_to_rate(row)


def get(
    conn: sqlite3.Connection,
    *,
    as_of_date: date,
    base: str,
    quote: str,
    source: str,
) -> Rate | None:
    row = conn.execute(
        """
        SELECT id, as_of_date, base, quote, rate, source
        FROM rates
        WHERE as_of_date = ? AND base = ? AND quote = ? AND source = ?
        """,
        (as_of_date.isoformat(), base, quote, source),
    ).fetchone()
    return _row_to_rate(row) if row else None


def latest_on_or_before(
    conn: sqlite3.Connection, *, as_of_date: date, base: str, quote: str, source: str
) -> Rate | None:
    row = conn.execute(
        """
        SELECT id, as_of_date, base, quote, rate, source
        FROM rates
        WHERE base = ? AND quote = ? AND source = ? AND as_of_date <= ?
        ORDER BY as_of_date DESC
        LIMIT 1
        """,
        (base, quote, source, as_of_date.isoformat()),
    ).fetchone()
    return _row_to_rate(row) if row else None
