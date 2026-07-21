"""Tests for finances.domain.realized_rates (ADR-013).

Derives the per-day *realized* VES rate — the rate the owner actually got
when selling USDT on Binance P2P — from the ``user_rate`` already stored on
every P2P fill. Same-day sells are volume-weighted (VWAP).

Selection deliberately keys off ``source_ref LIKE 'p2p:%'`` + a negative
amount rather than ``kind``: bank-anchored pairing (EPIC-006) promotes these
rows to ``kind='transfer'`` after the fact, so ``kind`` is not stable.
"""

from __future__ import annotations

import sqlite3
from datetime import UTC, date, datetime
from decimal import Decimal

from finances.db.repos import accounts as accounts_repo
from finances.db.repos import transactions as txn_repo
from finances.domain import realized_rates
from finances.domain.models import Transaction, TransactionKind

REALIZED_SOURCE = "binance_p2p_realized"


def _binance_account_id(conn: sqlite3.Connection) -> int:
    for account in accounts_repo.list_all(conn):
        if account.name == "Binance Spot":
            assert account.id is not None
            return account.id
    raise AssertionError("seeded_db should provide a 'Binance Spot' account")


def _p2p_fill(
    conn: sqlite3.Connection,
    *,
    account_id: int,
    day: date,
    usdt: str,
    rate: str,
    order: str,
    fiat: str = "VES",
    trade_type: str = "SELL",
    kind: TransactionKind = TransactionKind.EXPENSE,
) -> Transaction:
    """Insert one P2P fill shaped exactly like finances.ingest.binance emits it."""
    signed = -Decimal(usdt) if trade_type == "SELL" else Decimal(usdt)
    return txn_repo.insert(
        conn,
        Transaction(
            account_id=account_id,
            occurred_at=datetime(day.year, day.month, day.day, 12, 0, tzinfo=UTC),
            kind=kind,
            amount=signed,
            currency="USDT",
            description=f"P2P {trade_type} USDT @ {rate} {fiat} (order {order})",
            user_rate=Decimal(rate),
            source="binance",
            source_ref=f"p2p:{order}",
        ),
    )


def _rates_by_day(conn: sqlite3.Connection) -> dict[date, Decimal]:
    rows = conn.execute(
        "SELECT as_of_date, rate FROM rates WHERE source = ? ORDER BY as_of_date",
        (REALIZED_SOURCE,),
    ).fetchall()
    return {
        (
            row["as_of_date"]
            if isinstance(row["as_of_date"], date)
            else date.fromisoformat(row["as_of_date"])
        ): Decimal(str(row["rate"]))
        for row in rows
    }


# The `rate DECIMAL` column carries NUMERIC affinity, so sqlite coerces the
# stored decimal text to a float64 — a pre-existing property of every rate in
# this schema (bcv, binance_p2p_median included), not of this feature. The
# derivation itself stays exact in Decimal; only the round-trip quantizes, by
# ~1e-14 relative. Storage assertions therefore compare within tolerance while
# `compute_realized_rates` is asserted exactly.
_STORAGE_TOLERANCE = Decimal("1e-9")


def _assert_stored(actual: dict[date, Decimal], expected: dict[date, Decimal]) -> None:
    assert actual.keys() == expected.keys()
    for day, want in expected.items():
        assert abs(actual[day] - want) < _STORAGE_TOLERANCE, (
            f"{day}: stored {actual[day]}, expected ~{want}"
        )


# ---------------------------------------------------------------------------
# VWAP derivation
# ---------------------------------------------------------------------------


def test_single_sell_day_uses_its_own_rate(seeded_db: sqlite3.Connection) -> None:
    account_id = _binance_account_id(seeded_db)
    _p2p_fill(
        seeded_db, account_id=account_id, day=date(2025, 7, 1),
        usdt="100", rate="40", order="1",
    )

    results = realized_rates.compute_realized_rates(seeded_db)

    assert len(results) == 1
    assert results[0].as_of_date == date(2025, 7, 1)
    assert results[0].rate == Decimal("40")
    assert results[0].base == "USDT"
    assert results[0].quote == "VES"
    assert results[0].source == REALIZED_SOURCE


def test_same_day_sells_are_volume_weighted(seeded_db: sqlite3.Connection) -> None:
    account_id = _binance_account_id(seeded_db)
    day = date(2025, 7, 1)
    # 100 USDT @ 40 + 50 USDT @ 44 -> 6200 VES for 150 USDT -> 41.333...
    _p2p_fill(seeded_db, account_id=account_id, day=day, usdt="100", rate="40", order="1")
    _p2p_fill(seeded_db, account_id=account_id, day=day, usdt="50", rate="44", order="2")

    results = realized_rates.compute_realized_rates(seeded_db)

    assert len(results) == 1
    assert results[0].rate == Decimal("6200") / Decimal("150")


def test_separate_days_produce_separate_rates(seeded_db: sqlite3.Connection) -> None:
    account_id = _binance_account_id(seeded_db)
    _p2p_fill(
        seeded_db, account_id=account_id, day=date(2025, 7, 1),
        usdt="100", rate="40", order="1",
    )
    _p2p_fill(
        seeded_db, account_id=account_id, day=date(2025, 7, 8),
        usdt="100", rate="50", order="2",
    )

    results = realized_rates.compute_realized_rates(seeded_db)

    assert [(r.as_of_date, r.rate) for r in results] == [
        (date(2025, 7, 1), Decimal("40")),
        (date(2025, 7, 8), Decimal("50")),
    ]


# ---------------------------------------------------------------------------
# Selection rules
# ---------------------------------------------------------------------------


def test_p2p_buys_are_excluded(seeded_db: sqlite3.Connection) -> None:
    """A BUY spends VES to get USDT — a disposal, not an acquisition."""
    account_id = _binance_account_id(seeded_db)
    _p2p_fill(
        seeded_db, account_id=account_id, day=date(2025, 7, 1),
        usdt="100", rate="99", order="1",
        trade_type="BUY", kind=TransactionKind.INCOME,
    )

    assert realized_rates.compute_realized_rates(seeded_db) == []


def test_sell_promoted_to_transfer_is_still_included(
    seeded_db: sqlite3.Connection,
) -> None:
    """Bank-anchored pairing rewrites kind to 'transfer'; selection must survive it."""
    account_id = _binance_account_id(seeded_db)
    _p2p_fill(
        seeded_db, account_id=account_id, day=date(2025, 7, 1),
        usdt="100", rate="40", order="1", kind=TransactionKind.TRANSFER,
    )

    results = realized_rates.compute_realized_rates(seeded_db)

    assert len(results) == 1
    assert results[0].rate == Decimal("40")


def test_non_p2p_rows_are_excluded(seeded_db: sqlite3.Connection) -> None:
    """A plain expense carrying a manual user_rate is not an acquisition."""
    account_id = _binance_account_id(seeded_db)
    txn_repo.insert(
        seeded_db,
        Transaction(
            account_id=account_id,
            occurred_at=datetime(2025, 7, 1, 12, 0, tzinfo=UTC),
            kind=TransactionKind.EXPENSE,
            amount=Decimal("-500"),
            currency="VES",
            description="groceries",
            user_rate=Decimal("77"),
            source="provincial",
            source_ref="row:1",
        ),
    )

    assert realized_rates.compute_realized_rates(seeded_db) == []


def test_sell_without_user_rate_is_excluded(seeded_db: sqlite3.Connection) -> None:
    account_id = _binance_account_id(seeded_db)
    txn_repo.insert(
        seeded_db,
        Transaction(
            account_id=account_id,
            occurred_at=datetime(2025, 7, 1, 12, 0, tzinfo=UTC),
            kind=TransactionKind.EXPENSE,
            amount=Decimal("-100"),
            currency="USDT",
            description="P2P SELL USDT @ ? VES (order 9)",
            user_rate=None,
            source="binance",
            source_ref="p2p:9",
        ),
    )

    assert realized_rates.compute_realized_rates(seeded_db) == []


def test_non_ves_fiat_sell_is_skipped(seeded_db: sqlite3.Connection) -> None:
    """A COP-denominated sell must never be folded into the VES VWAP."""
    account_id = _binance_account_id(seeded_db)
    day = date(2025, 7, 1)
    _p2p_fill(
        seeded_db, account_id=account_id, day=day,
        usdt="100", rate="40", order="1", fiat="VES",
    )
    _p2p_fill(
        seeded_db, account_id=account_id, day=day,
        usdt="100", rate="4000", order="2", fiat="COP",
    )

    results = realized_rates.compute_realized_rates(seeded_db)

    assert len(results) == 1
    assert results[0].rate == Decimal("40")


# ---------------------------------------------------------------------------
# rebuild()
# ---------------------------------------------------------------------------


def test_rebuild_writes_rates_rows(seeded_db: sqlite3.Connection) -> None:
    account_id = _binance_account_id(seeded_db)
    _p2p_fill(
        seeded_db, account_id=account_id, day=date(2025, 7, 1),
        usdt="100", rate="40", order="1",
    )

    written = realized_rates.rebuild(seeded_db)

    assert written == 1
    _assert_stored(_rates_by_day(seeded_db), {date(2025, 7, 1): Decimal("40")})


def test_rebuild_is_idempotent(seeded_db: sqlite3.Connection) -> None:
    account_id = _binance_account_id(seeded_db)
    _p2p_fill(
        seeded_db, account_id=account_id, day=date(2025, 7, 1),
        usdt="100", rate="40", order="1",
    )

    realized_rates.rebuild(seeded_db)
    realized_rates.rebuild(seeded_db)

    _assert_stored(_rates_by_day(seeded_db), {date(2025, 7, 1): Decimal("40")})


def test_rebuild_reflects_new_fills_on_rerun(seeded_db: sqlite3.Connection) -> None:
    account_id = _binance_account_id(seeded_db)
    day = date(2025, 7, 1)
    _p2p_fill(seeded_db, account_id=account_id, day=day, usdt="100", rate="40", order="1")
    realized_rates.rebuild(seeded_db)

    _p2p_fill(seeded_db, account_id=account_id, day=day, usdt="50", rate="44", order="2")
    realized_rates.rebuild(seeded_db)

    _assert_stored(
        _rates_by_day(seeded_db),
        {date(2025, 7, 1): Decimal("6200") / Decimal("150")},
    )


def test_rebuild_with_no_p2p_history_writes_nothing(
    seeded_db: sqlite3.Connection,
) -> None:
    assert realized_rates.rebuild(seeded_db) == 0
    assert _rates_by_day(seeded_db) == {}
