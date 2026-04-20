"""Tests for finances/domain/earn.py — Earn position snapshot reconciliation.

EPIC-007 / ADR-003: every Binance sync pulls `simple_earn_flexible_position` and
reconciles it against the `earn_positions` table. The domain module handles the
snapshot diff; the ingest module just calls it with parsed rows.
"""
from __future__ import annotations

import sqlite3
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from finances.db.repos import accounts as accounts_repo
from finances.db.repos import positions as positions_repo
from finances.domain.earn import EarnSnapshotRow, refresh_earn_positions
from finances.domain.models import Account, AccountKind, EarnPosition


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _binance_earn_account(conn: sqlite3.Connection) -> Account:
    existing = accounts_repo.get_by_name(conn, "Binance Earn")
    if existing is not None:
        return existing
    return accounts_repo.insert(
        conn,
        Account(
            name="Binance Earn",
            kind=AccountKind.CRYPTO_EARN,
            currency="USDT",
            institution="Binance",
        ),
    )


def _snapshot_row(
    product_id: str = "USDT001",
    asset: str = "USDT",
    principal: str = "100.50",
    apy: str = "0.05",
) -> EarnSnapshotRow:
    return EarnSnapshotRow(
        product_id=product_id,
        asset=asset,
        principal=Decimal(principal),
        apy=Decimal(apy),
    )


# ---------------------------------------------------------------------------
# EarnSnapshotRow — Pydantic validation (ADR-009)
# ---------------------------------------------------------------------------

def test_earn_snapshot_row_rejects_float_principal() -> None:
    with pytest.raises(Exception):  # pydantic ValidationError wraps ValueError
        EarnSnapshotRow(product_id="X", asset="USDT", principal=1.5)  # type: ignore[arg-type]


def test_earn_snapshot_row_uppercases_asset() -> None:
    row = EarnSnapshotRow(product_id="X", asset="usdt", principal=Decimal("1"))
    assert row.asset == "USDT"


def test_earn_snapshot_row_accepts_decimal_strings() -> None:
    row = EarnSnapshotRow(
        product_id="X", asset="USDT", principal="100.50", apy="0.0625"
    )
    assert row.principal == Decimal("100.50")
    assert row.apy == Decimal("0.0625")


# ---------------------------------------------------------------------------
# refresh_earn_positions — happy path (insert new)
# ---------------------------------------------------------------------------

def test_refresh_inserts_new_positions(in_memory_db: sqlite3.Connection) -> None:
    earn = _binance_earn_account(in_memory_db)
    snapshot_at = datetime(2026, 4, 19, 12, 0, tzinfo=UTC)

    result = refresh_earn_positions(
        in_memory_db,
        snapshot=[
            _snapshot_row("PROD-A", "USDT", "100"),
            _snapshot_row("PROD-B", "BTC", "0.5", "0.03"),
        ],
        earn_account_id=earn.id,  # type: ignore[arg-type]
        snapshot_at=snapshot_at,
    )

    assert result == {"inserted": 2, "closed": 0, "unchanged": 0}
    open_positions = positions_repo.list_open(in_memory_db, account_id=earn.id)
    assert len(open_positions) == 2
    by_product = {p.product_id: p for p in open_positions}
    assert by_product["PROD-A"].principal == Decimal("100")
    assert by_product["PROD-B"].principal == Decimal("0.5")
    assert by_product["PROD-B"].apy == Decimal("0.03")


def test_refresh_is_idempotent_when_snapshot_unchanged(
    in_memory_db: sqlite3.Connection,
) -> None:
    earn = _binance_earn_account(in_memory_db)
    snapshot_at = datetime(2026, 4, 19, 12, 0, tzinfo=UTC)
    rows = [_snapshot_row("PROD-A", "USDT", "100")]

    first = refresh_earn_positions(
        in_memory_db, snapshot=rows, earn_account_id=earn.id, snapshot_at=snapshot_at  # type: ignore[arg-type]
    )
    assert first == {"inserted": 1, "closed": 0, "unchanged": 0}

    second = refresh_earn_positions(
        in_memory_db,
        snapshot=rows,
        earn_account_id=earn.id,  # type: ignore[arg-type]
        snapshot_at=datetime(2026, 4, 19, 13, 0, tzinfo=UTC),
    )
    assert second == {"inserted": 0, "closed": 0, "unchanged": 1}
    assert len(positions_repo.list_open(in_memory_db, account_id=earn.id)) == 1


# ---------------------------------------------------------------------------
# refresh_earn_positions — principal change closes old, opens new
# ---------------------------------------------------------------------------

def test_refresh_closes_and_reopens_on_principal_change(
    in_memory_db: sqlite3.Connection,
) -> None:
    earn = _binance_earn_account(in_memory_db)
    snapshot_at_1 = datetime(2026, 4, 19, 12, 0, tzinfo=UTC)
    snapshot_at_2 = datetime(2026, 4, 19, 13, 0, tzinfo=UTC)

    refresh_earn_positions(
        in_memory_db,
        snapshot=[_snapshot_row("PROD-A", "USDT", "100")],
        earn_account_id=earn.id,  # type: ignore[arg-type]
        snapshot_at=snapshot_at_1,
    )
    result = refresh_earn_positions(
        in_memory_db,
        snapshot=[_snapshot_row("PROD-A", "USDT", "150")],
        earn_account_id=earn.id,  # type: ignore[arg-type]
        snapshot_at=snapshot_at_2,
    )

    assert result == {"inserted": 1, "closed": 1, "unchanged": 0}
    open_positions = positions_repo.list_open(in_memory_db, account_id=earn.id)
    assert len(open_positions) == 1
    assert open_positions[0].principal == Decimal("150")
    assert open_positions[0].started_at.replace(tzinfo=UTC) == snapshot_at_2 or \
        open_positions[0].started_at == snapshot_at_2


# ---------------------------------------------------------------------------
# refresh_earn_positions — disappeared products get closed
# ---------------------------------------------------------------------------

def test_refresh_closes_positions_missing_from_snapshot(
    in_memory_db: sqlite3.Connection,
) -> None:
    earn = _binance_earn_account(in_memory_db)
    snapshot_at_1 = datetime(2026, 4, 19, 12, 0, tzinfo=UTC)
    snapshot_at_2 = datetime(2026, 4, 19, 13, 0, tzinfo=UTC)

    refresh_earn_positions(
        in_memory_db,
        snapshot=[
            _snapshot_row("PROD-A", "USDT", "100"),
            _snapshot_row("PROD-B", "BTC", "0.5"),
        ],
        earn_account_id=earn.id,  # type: ignore[arg-type]
        snapshot_at=snapshot_at_1,
    )
    result = refresh_earn_positions(
        in_memory_db,
        snapshot=[_snapshot_row("PROD-A", "USDT", "100")],
        earn_account_id=earn.id,  # type: ignore[arg-type]
        snapshot_at=snapshot_at_2,
    )

    assert result == {"inserted": 0, "closed": 1, "unchanged": 1}
    open_positions = positions_repo.list_open(in_memory_db, account_id=earn.id)
    assert len(open_positions) == 1
    assert open_positions[0].product_id == "PROD-A"


# ---------------------------------------------------------------------------
# refresh_earn_positions — failure modes
# ---------------------------------------------------------------------------

def test_refresh_rejects_missing_account(in_memory_db: sqlite3.Connection) -> None:
    with pytest.raises(ValueError, match="account"):
        refresh_earn_positions(
            in_memory_db,
            snapshot=[_snapshot_row()],
            earn_account_id=9999,
            snapshot_at=datetime(2026, 4, 19, tzinfo=UTC),
        )


def test_refresh_rejects_naive_snapshot_at(in_memory_db: sqlite3.Connection) -> None:
    earn = _binance_earn_account(in_memory_db)
    with pytest.raises(ValueError, match="timezone-aware"):
        refresh_earn_positions(
            in_memory_db,
            snapshot=[_snapshot_row()],
            earn_account_id=earn.id,  # type: ignore[arg-type]
            snapshot_at=datetime(2026, 4, 19, 12, 0),
        )


def test_refresh_rejects_duplicate_product_ids(in_memory_db: sqlite3.Connection) -> None:
    earn = _binance_earn_account(in_memory_db)
    with pytest.raises(ValueError, match="duplicate"):
        refresh_earn_positions(
            in_memory_db,
            snapshot=[
                _snapshot_row("PROD-A", "USDT", "100"),
                _snapshot_row("PROD-A", "USDT", "200"),
            ],
            earn_account_id=earn.id,  # type: ignore[arg-type]
            snapshot_at=datetime(2026, 4, 19, 12, 0, tzinfo=UTC),
        )


def test_refresh_empty_snapshot_closes_all_open(
    in_memory_db: sqlite3.Connection,
) -> None:
    earn = _binance_earn_account(in_memory_db)
    refresh_earn_positions(
        in_memory_db,
        snapshot=[_snapshot_row("PROD-A", "USDT", "100")],
        earn_account_id=earn.id,  # type: ignore[arg-type]
        snapshot_at=datetime(2026, 4, 19, 12, 0, tzinfo=UTC),
    )
    result = refresh_earn_positions(
        in_memory_db,
        snapshot=[],
        earn_account_id=earn.id,  # type: ignore[arg-type]
        snapshot_at=datetime(2026, 4, 19, 13, 0, tzinfo=UTC),
    )
    assert result == {"inserted": 0, "closed": 1, "unchanged": 0}
    assert positions_repo.list_open(in_memory_db, account_id=earn.id) == []
