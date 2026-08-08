"""RED — opening positions for history the custodian no longer serves (ADR-020).

A dated plug corrects a balance on its date and freezes there, so every
repair to historical rows invalidates it. An opening position is dated at
the ledger's start and carries a stable ``source_ref``, so restating it
replaces the prior statement instead of layering another correction on top.

The invariant that makes it honest: an opening *balance* is never negative.
A position the ledger overstates does not need one — it needs the outbound
movement nobody recorded.
"""
from __future__ import annotations

import sqlite3
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from finances.domain.opening_positions import (
    OpeningShape,
    ledger_start,
    record_opening,
)

SPOT, FUNDING = 2, 3


def _seed(
    conn: sqlite3.Connection,
    *,
    account_id: int,
    amount: str,
    currency: str = "USDT",
    occurred_at: str = "2025-10-03T00:00:00-04:00",
    kind: str = "income",
    source_ref: str | None = None,
) -> int:
    cur = conn.execute(
        """
        INSERT INTO transactions
            (account_id, occurred_at, kind, amount, currency, description,
             source, source_ref, needs_review)
        VALUES (?, ?, ?, ?, ?, 'seed', 'binance', ?, 0)
        """,
        (
            account_id, occurred_at, kind, amount, currency,
            source_ref or f"seed:{account_id}:{amount}:{currency}",
        ),
    )
    return int(cur.lastrowid)


def _balance(conn: sqlite3.Connection, account_id: int, currency: str) -> Decimal:
    from finances.domain.reconciliation_adjustments import position_balance

    return position_balance(conn, account_id=account_id, currency=currency)


def _opening_rows(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM transactions WHERE source = 'opening_balance' ORDER BY id"
    ).fetchall()


def test_an_understated_position_gets_a_positive_opening_balance(
    seeded_db: sqlite3.Connection,
) -> None:
    _seed(seeded_db, account_id=SPOT, amount="-100")

    result = record_opening(
        seeded_db, account_id=SPOT, currency="USDT", actual=Decimal("25")
    )

    assert result is not None
    assert result.shape is OpeningShape.BALANCE
    assert result.delta == Decimal("125")
    assert _balance(seeded_db, SPOT, "USDT") == Decimal("25")

    rows = _opening_rows(seeded_db)
    assert len(rows) == 1
    assert rows[0]["kind"] == "adjustment"
    assert rows[0]["source_ref"] == f"opening:{SPOT}:USDT"


def test_opening_rows_are_dated_at_the_ledger_start(
    seeded_db: sqlite3.Connection,
) -> None:
    _seed(seeded_db, account_id=SPOT, amount="-100", occurred_at="2025-10-03T00:00:00-04:00")
    _seed(seeded_db, account_id=SPOT, amount="10", occurred_at="2026-05-01T00:00:00-04:00")

    record_opening(seeded_db, account_id=SPOT, currency="USDT", actual=Decimal("0"))

    start = ledger_start(seeded_db)
    assert start.date().isoformat() == "2025-10-03"

    occurred = _opening_rows(seeded_db)[0]["occurred_at"]
    if isinstance(occurred, str):
        occurred = datetime.fromisoformat(occurred)
    assert occurred.date().isoformat() == "2025-10-03"


def test_an_overstated_position_records_the_movement_not_a_negative_balance(
    seeded_db: sqlite3.Connection,
) -> None:
    """Funding overstated, Spot understated — that is a transfer, not a plug."""
    _seed(seeded_db, account_id=FUNDING, amount="1000")

    result = record_opening(
        seeded_db,
        account_id=FUNDING,
        currency="USDT",
        actual=Decimal("400"),
        counterpart_account_id=SPOT,
    )

    assert result is not None
    assert result.shape is OpeningShape.TRANSFER
    assert _balance(seeded_db, FUNDING, "USDT") == Decimal("400")
    assert _balance(seeded_db, SPOT, "USDT") == Decimal("600")

    rows = _opening_rows(seeded_db)
    assert len(rows) == 2
    assert {r["kind"] for r in rows} == {"transfer"}
    assert sum(Decimal(str(r["amount"])) for r in rows) == Decimal("0")
    assert len({r["transfer_id"] for r in rows}) == 1
    assert {r["account_id"] for r in rows} == {FUNDING, SPOT}


def test_an_overstated_position_with_no_counterpart_is_refused(
    seeded_db: sqlite3.Connection,
) -> None:
    """Writing a negative opening balance would assert a falsehood."""
    _seed(seeded_db, account_id=FUNDING, amount="1000")

    with pytest.raises(ValueError, match="negative"):
        record_opening(
            seeded_db, account_id=FUNDING, currency="USDT", actual=Decimal("400")
        )

    assert _opening_rows(seeded_db) == []


def test_a_position_already_matching_writes_nothing(
    seeded_db: sqlite3.Connection,
) -> None:
    _seed(seeded_db, account_id=SPOT, amount="25")

    assert (
        record_opening(
            seeded_db, account_id=SPOT, currency="USDT", actual=Decimal("25")
        )
        is None
    )
    assert _opening_rows(seeded_db) == []


def test_restating_replaces_rather_than_accumulates(
    seeded_db: sqlite3.Connection,
) -> None:
    """The whole point: one opening position per (account, currency), ever."""
    _seed(seeded_db, account_id=SPOT, amount="-100")

    record_opening(seeded_db, account_id=SPOT, currency="USDT", actual=Decimal("25"))
    record_opening(seeded_db, account_id=SPOT, currency="USDT", actual=Decimal("70"))

    rows = _opening_rows(seeded_db)
    assert len(rows) == 1, "a restatement must not layer a second correction"
    assert _balance(seeded_db, SPOT, "USDT") == Decimal("70")


def test_restating_a_transfer_replaces_both_legs(
    seeded_db: sqlite3.Connection,
) -> None:
    _seed(seeded_db, account_id=FUNDING, amount="1000")

    record_opening(
        seeded_db, account_id=FUNDING, currency="USDT",
        actual=Decimal("400"), counterpart_account_id=SPOT,
    )
    record_opening(
        seeded_db, account_id=FUNDING, currency="USDT",
        actual=Decimal("250"), counterpart_account_id=SPOT,
    )

    rows = _opening_rows(seeded_db)
    assert len(rows) == 2
    assert _balance(seeded_db, FUNDING, "USDT") == Decimal("250")
    assert _balance(seeded_db, SPOT, "USDT") == Decimal("750")


def test_restating_one_position_leaves_another_alone(
    seeded_db: sqlite3.Connection,
) -> None:
    _seed(seeded_db, account_id=SPOT, amount="-100", currency="USDT")
    _seed(seeded_db, account_id=SPOT, amount="-5", currency="USDC")

    record_opening(seeded_db, account_id=SPOT, currency="USDT", actual=Decimal("25"))
    record_opening(seeded_db, account_id=SPOT, currency="USDC", actual=Decimal("1"))
    record_opening(seeded_db, account_id=SPOT, currency="USDT", actual=Decimal("30"))

    assert _balance(seeded_db, SPOT, "USDC") == Decimal("1")
    assert _balance(seeded_db, SPOT, "USDT") == Decimal("30")
    assert len(_opening_rows(seeded_db)) == 2


def test_opening_rows_never_read_as_income_or_expense(
    seeded_db: sqlite3.Connection,
) -> None:
    """rule-012: they correct balances, they are not earnings."""
    from finances.domain.money import SQL_NOT_CURRENCY_MOVEMENT

    _seed(seeded_db, account_id=SPOT, amount="-100")
    record_opening(seeded_db, account_id=SPOT, currency="USDT", actual=Decimal("25"))

    leaked = seeded_db.execute(
        "SELECT COUNT(*) FROM transactions"
        f" WHERE source = 'opening_balance' AND ({SQL_NOT_CURRENCY_MOVEMENT})"
    ).fetchone()[0]
    assert leaked == 0
