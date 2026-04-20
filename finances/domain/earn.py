"""Earn position snapshot reconciliation (EPIC-007 / ADR-003).

Binance reports current Flexible Earn positions via
``simple_earn_flexible_position``. The ingest adapter parses each row into an
:class:`EarnSnapshotRow` and hands the list to :func:`refresh_earn_positions`,
which diffs the snapshot against the ``earn_positions`` table:

- a product in the snapshot that is not open in DB is inserted;
- a product open in DB whose principal differs from the snapshot is closed and
  re-opened with the new principal (keeping historical rows);
- a product open in DB but absent from the snapshot is closed (redeemed);
- a product whose principal matches the snapshot is unchanged.

Per rule-003, ``earn_positions`` may only be written by
``finances/ingest/binance.py`` — this helper is the single sink for that
endpoint's data.
"""
from __future__ import annotations

import sqlite3
from datetime import datetime
from decimal import Decimal
from typing import Any, Sequence

from pydantic import BaseModel, ConfigDict, field_validator

from finances.db.repos import accounts as accounts_repo
from finances.db.repos import positions as positions_repo
from finances.domain.models import EarnPosition


def _coerce_decimal(v: Any) -> Decimal:
    if isinstance(v, Decimal):
        return v
    if isinstance(v, bool):
        raise ValueError("bool is not a valid monetary value")
    if isinstance(v, float):
        raise ValueError("float monetary inputs are forbidden; use Decimal or str")
    if isinstance(v, (int, str)):
        return Decimal(str(v))
    raise ValueError(f"cannot coerce {type(v).__name__} to Decimal")


class EarnSnapshotRow(BaseModel):
    """One row from ``simple_earn_flexible_position`` (ADR-009 trust boundary)."""

    model_config = ConfigDict(strict=False, extra="forbid")

    product_id: str
    asset: str
    principal: Decimal
    apy: Decimal | None = None

    @field_validator("principal", "apy", mode="before")
    @classmethod
    def _decimal_fields(cls, v: Any) -> Any:
        if v is None:
            return v
        return _coerce_decimal(v)

    @field_validator("asset")
    @classmethod
    def _upper_asset(cls, v: str) -> str:
        return v.upper()


def refresh_earn_positions(
    conn: sqlite3.Connection,
    *,
    snapshot: Sequence[EarnSnapshotRow],
    earn_account_id: int,
    snapshot_at: datetime,
) -> dict[str, int]:
    """Diff ``snapshot`` against the open ``earn_positions`` for the account.

    Returns a counts dict: ``{"inserted": n, "closed": n, "unchanged": n}``.
    """
    if snapshot_at.tzinfo is None or snapshot_at.tzinfo.utcoffset(snapshot_at) is None:
        raise ValueError("snapshot_at must be timezone-aware")
    if accounts_repo.get_by_id(conn, earn_account_id) is None:
        raise ValueError(f"earn account {earn_account_id} does not exist")

    seen_products: set[str] = set()
    for row in snapshot:
        if row.product_id in seen_products:
            raise ValueError(f"duplicate product_id in snapshot: {row.product_id}")
        seen_products.add(row.product_id)

    open_rows = positions_repo.list_open(conn, account_id=earn_account_id)
    open_by_product: dict[str, EarnPosition] = {p.product_id: p for p in open_rows}

    inserted = 0
    closed = 0
    unchanged = 0

    for row in snapshot:
        existing = open_by_product.get(row.product_id)
        if existing is None:
            positions_repo.insert(
                conn,
                EarnPosition(
                    account_id=earn_account_id,
                    product_id=row.product_id,
                    asset=row.asset,
                    principal=row.principal,
                    apy=row.apy,
                    started_at=snapshot_at,
                    snapshot_at=snapshot_at,
                ),
            )
            inserted += 1
        elif existing.principal == row.principal:
            unchanged += 1
        else:
            assert existing.id is not None
            positions_repo.close(conn, existing.id, snapshot_at)
            positions_repo.insert(
                conn,
                EarnPosition(
                    account_id=earn_account_id,
                    product_id=row.product_id,
                    asset=row.asset,
                    principal=row.principal,
                    apy=row.apy,
                    started_at=snapshot_at,
                    snapshot_at=snapshot_at,
                ),
            )
            inserted += 1
            closed += 1

    for product_id, existing in open_by_product.items():
        if product_id not in seen_products:
            assert existing.id is not None
            positions_repo.close(conn, existing.id, snapshot_at)
            closed += 1

    return {"inserted": inserted, "closed": closed, "unchanged": unchanged}


__all__ = ["EarnSnapshotRow", "refresh_earn_positions"]
