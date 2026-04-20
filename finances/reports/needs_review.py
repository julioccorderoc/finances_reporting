"""Needs-review report (EPIC-013).

Surfaces every transaction with ``needs_review = 1`` so the user can triage
unresolved rows. Matches the shape of ``balances.py`` for consistency
(``get_*`` returns a Pydantic list; three ``render_*`` helpers return strings).

The CLI layer wires this into ``finances report needs-review`` — this module
builds the strings; stdout printing happens elsewhere.
"""

from __future__ import annotations

import csv
import io
import json
import sqlite3
from datetime import datetime
from decimal import Decimal
from typing import Any

from pydantic import BaseModel, ConfigDict, field_validator


# ---------------------------------------------------------------------------
# Pydantic boundary
# ---------------------------------------------------------------------------


def _coerce_decimal(v: Any) -> Decimal:
    """Accept Decimal / int / str; reject float + bool (per ADR-009)."""
    if isinstance(v, Decimal):
        return v
    if isinstance(v, bool):
        raise ValueError("bool is not a valid monetary value")
    if isinstance(v, float):
        raise ValueError("float monetary inputs are forbidden; use Decimal or str")
    if isinstance(v, (int, str)):
        return Decimal(str(v))
    raise ValueError(f"cannot coerce {type(v).__name__} to Decimal")


def _require_aware(dt: datetime) -> datetime:
    if dt.tzinfo is None or dt.tzinfo.utcoffset(dt) is None:
        raise ValueError("datetime must be timezone-aware")
    return dt


class NeedsReviewRow(BaseModel):
    """One row from the needs-review query."""

    model_config = ConfigDict(strict=False, extra="forbid")

    transaction_id: int
    occurred_at: datetime
    account_id: int
    kind: str
    amount: Decimal
    currency: str
    description: str | None = None
    source: str

    @field_validator("amount", mode="before")
    @classmethod
    def _decimal_amount(cls, v: Any) -> Decimal:
        return _coerce_decimal(v)

    @field_validator("occurred_at")
    @classmethod
    def _aware_occurred_at(cls, v: datetime) -> datetime:
        return _require_aware(v)

    @field_validator("currency")
    @classmethod
    def _upper_currency(cls, v: str) -> str:
        return v.upper()


# ---------------------------------------------------------------------------
# Query
# ---------------------------------------------------------------------------


def get_needs_review(conn: sqlite3.Connection) -> list[NeedsReviewRow]:
    """Return all ``needs_review = 1`` rows, newest first with id DESC tiebreak."""
    rows = conn.execute(
        """
        SELECT
            id            AS transaction_id,
            occurred_at   AS occurred_at,
            account_id    AS account_id,
            kind          AS kind,
            amount        AS amount,
            currency      AS currency,
            description   AS description,
            source        AS source
        FROM transactions
        WHERE needs_review = 1
        ORDER BY occurred_at DESC, id DESC
        """
    ).fetchall()

    out: list[NeedsReviewRow] = []
    for row in rows:
        amount_raw = row["amount"]
        amount = (
            amount_raw if isinstance(amount_raw, Decimal) else Decimal(str(amount_raw))
        )
        out.append(
            NeedsReviewRow(
                transaction_id=int(row["transaction_id"]),
                occurred_at=row["occurred_at"],
                account_id=int(row["account_id"]),
                kind=row["kind"],
                amount=amount,
                currency=row["currency"],
                description=row["description"],
                source=row["source"],
            )
        )
    return out


# ---------------------------------------------------------------------------
# Renderers
# ---------------------------------------------------------------------------

_CSV_HEADER: tuple[str, ...] = (
    "transaction_id",
    "occurred_at",
    "account_id",
    "kind",
    "amount",
    "currency",
    "description",
    "source",
)


def render_json(rows: list[NeedsReviewRow]) -> str:
    """Serialize as a JSON array; Decimals as strings, datetimes as ISO."""
    payload = [
        {
            "transaction_id": r.transaction_id,
            "occurred_at": r.occurred_at.isoformat(),
            "account_id": r.account_id,
            "kind": r.kind,
            "amount": str(r.amount),
            "currency": r.currency,
            "description": r.description,
            "source": r.source,
        }
        for r in rows
    ]
    return json.dumps(payload)


def render_csv(rows: list[NeedsReviewRow]) -> str:
    """Serialize as CSV. Always emits the header row, even on empty input."""
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(_CSV_HEADER)
    for r in rows:
        writer.writerow(
            [
                r.transaction_id,
                r.occurred_at.isoformat(),
                r.account_id,
                r.kind,
                str(r.amount),
                r.currency,
                r.description if r.description is not None else "",
                r.source,
            ]
        )
    return buf.getvalue()


def render_table(rows: list[NeedsReviewRow]) -> str:
    """Pretty-printed monospace table suitable for stdout.

    Columns: ID, Occurred At, Account, Kind, Amount (right-aligned), Currency,
    Description, Source.
    """
    headers = (
        "ID",
        "Occurred At",
        "Account",
        "Kind",
        "Amount",
        "Currency",
        "Description",
        "Source",
    )
    str_rows: list[tuple[str, ...]] = [
        (
            str(r.transaction_id),
            r.occurred_at.isoformat(),
            str(r.account_id),
            r.kind,
            str(r.amount),
            r.currency,
            r.description if r.description is not None else "",
            r.source,
        )
        for r in rows
    ]

    # max() doesn't accept `default=` when positional args are given, so
    # assemble each column's candidate widths as a list first.
    widths = [
        max([len(headers[i]), *(len(sr[i]) for sr in str_rows)])
        for i in range(len(headers))
    ]

    # Right-align the Amount column (index 4); left-align the rest.
    def _format_row(values: tuple[str, ...]) -> str:
        parts: list[str] = []
        for i, v in enumerate(values):
            if i == 4:
                parts.append(f"{v:>{widths[i]}}")
            else:
                parts.append(f"{v:<{widths[i]}}")
        return "  ".join(parts)

    lines: list[str] = []
    lines.append(_format_row(headers))
    lines.append("  ".join("-" * w for w in widths))
    for sr in str_rows:
        lines.append(_format_row(sr))
    return "\n".join(lines) + "\n"


__all__ = [
    "NeedsReviewRow",
    "get_needs_review",
    "render_csv",
    "render_json",
    "render_table",
]
