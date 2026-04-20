"""Account-balance report (EPIC-013).

Reads the ``v_account_balances`` view from migration ``001_initial.sql`` and
exposes it through a Pydantic boundary plus pretty/JSON/CSV renderers. The
CLI layer (``finances/cli/main.py``) is responsible for printing — this module
only builds strings so unit tests can inspect them without capturing stdout.

Design notes
------------

* The view returns ``balance_native`` as ``REAL`` (SQLite casts at view
  definition time). We convert to ``Decimal`` via ``str(value)`` at the
  boundary so the rest of the system stays Decimal-clean (rule-009 / ADR-009).
* ``AccountBalance`` is a Pydantic v2 ``BaseModel`` with ``strict=True`` and
  ``extra='forbid'`` — any drift in the view's column set surfaces as a
  ``ValidationError`` at ``get_balances`` time rather than silently.
* ``render_json`` emits ``Decimal`` values as strings to preserve precision
  across ``json.loads``/``json.dumps`` round-trips.
"""

from __future__ import annotations

import csv
import io
import json
import sqlite3
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


class AccountBalance(BaseModel):
    """One row of ``v_account_balances`` as a Pydantic model."""

    model_config = ConfigDict(strict=True, extra="forbid")

    account_id: int
    account_name: str
    currency: str
    balance_native: Decimal

    @field_validator("balance_native", mode="before")
    @classmethod
    def _decimal_balance(cls, v: Any) -> Decimal:
        return _coerce_decimal(v)

    @field_validator("currency")
    @classmethod
    def _upper_currency(cls, v: str) -> str:
        return v.upper()


# ---------------------------------------------------------------------------
# Query
# ---------------------------------------------------------------------------


def get_balances(conn: sqlite3.Connection) -> list[AccountBalance]:
    """Return one ``AccountBalance`` per account, ordered by ``account_name``.

    Uses ``v_account_balances`` so we always match the schema contract.
    ``LEFT JOIN`` inside the view ensures accounts with zero transactions
    still show with ``balance_native == 0``.
    """
    rows = conn.execute(
        """
        SELECT account_id, account_name, currency, balance_native
        FROM v_account_balances
        ORDER BY account_name
        """
    ).fetchall()

    balances: list[AccountBalance] = []
    for row in rows:
        raw = row["balance_native"]
        # The view casts to REAL — convert via str() to land in Decimal cleanly.
        decimal_balance = Decimal(str(raw)) if raw is not None else Decimal("0")
        balances.append(
            AccountBalance(
                account_id=int(row["account_id"]),
                account_name=row["account_name"],
                currency=row["currency"],
                balance_native=decimal_balance,
            )
        )
    return balances


# ---------------------------------------------------------------------------
# Renderers
# ---------------------------------------------------------------------------

_CSV_HEADER: tuple[str, ...] = (
    "account_id",
    "account_name",
    "currency",
    "balance_native",
)


def render_json(balances: list[AccountBalance]) -> str:
    """Serialize ``balances`` as a JSON array with Decimals as strings."""
    payload = [
        {
            "account_id": b.account_id,
            "account_name": b.account_name,
            "currency": b.currency,
            "balance_native": str(b.balance_native),
        }
        for b in balances
    ]
    return json.dumps(payload)


def render_csv(balances: list[AccountBalance]) -> str:
    """Serialize ``balances`` as CSV. Always emits the header row."""
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(_CSV_HEADER)
    for b in balances:
        writer.writerow(
            [
                b.account_id,
                b.account_name,
                b.currency,
                str(b.balance_native),
            ]
        )
    return buf.getvalue()


def render_table(balances: list[AccountBalance]) -> str:
    """Pretty-printed monospace table suitable for stdout.

    Columns: ``Account``, ``Currency``, ``Balance`` (right-aligned).
    Column widths adapt to the data so the output is compact on short
    account names but still readable on long ones.
    """
    headers = ("Account", "Currency", "Balance")
    rows_str = [(b.account_name, b.currency, str(b.balance_native)) for b in balances]

    # max() doesn't accept `default=` when positional args are given, so
    # stage the candidates per column instead.
    w_account = max([len(headers[0]), *(len(r[0]) for r in rows_str)])
    w_currency = max([len(headers[1]), *(len(r[1]) for r in rows_str)])
    w_balance = max([len(headers[2]), *(len(r[2]) for r in rows_str)])

    lines: list[str] = []
    lines.append(
        f"{headers[0]:<{w_account}}  {headers[1]:<{w_currency}}  {headers[2]:>{w_balance}}"
    )
    lines.append(f"{'-' * w_account}  {'-' * w_currency}  {'-' * w_balance}")
    for name, currency, balance in rows_str:
        lines.append(
            f"{name:<{w_account}}  {currency:<{w_currency}}  {balance:>{w_balance}}"
        )
    return "\n".join(lines) + "\n"


__all__ = [
    "AccountBalance",
    "get_balances",
    "render_csv",
    "render_json",
    "render_table",
]
