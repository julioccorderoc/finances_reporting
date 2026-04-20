"""Consolidated USD transaction report (EPIC-013).

Produces a read-only view of every non-transfer transaction with its USD
equivalent resolved through ``finances.domain.rates.resolve`` — the single
auditable rate resolver mandated by ADR-005 / rule-005.

The report enforces the ADR-005 **2026-04-19 amendment** ("USDT for
headline, BCV for reference only"):

* Rows whose USD value was sourced from BCV (``bcv`` or ``bcv_carry``) are
  marked ``is_bcv_fallback = True``.
* They are **excluded** from the headline aggregate ``total_usd`` and
  instead rolled up into ``fallback_total_usd`` / ``fallback_row_count``.
* Their ``transaction_id`` is collected into ``strict_violations`` so the
  CLI layer can exit non-zero when invoked with ``--strict``.
* Rows whose resolver returned ``needs_review`` are surfaced with
  ``amount_usd = None`` and *do not* count as BCV-fallback violations —
  they simply need a rate, which is a separate problem.

This module never writes to the database. The resolver may mutate
``Transaction.needs_review`` on its in-memory copy (documented side
effect on ``needs_review`` rows), but we never persist that flip — reports
are read-only by contract.
"""

from __future__ import annotations

import csv
import io
import json
import sqlite3
from datetime import datetime
from decimal import Decimal
from typing import TYPE_CHECKING

from pydantic import BaseModel, ConfigDict

from finances.db.repos.transactions import _row_to_transaction
from finances.domain import rates as rates_engine
from finances.domain.models import Transaction

if TYPE_CHECKING:  # pragma: no cover - import-time only
    from collections.abc import Iterable


# ---------------------------------------------------------------------------
# Pydantic models — strict, extra='forbid' per rule-009.
# ---------------------------------------------------------------------------


_NATIVE_USD_CURRENCIES = frozenset({"USD", "USDT", "USDC"})
_NATIVE_USD_SOURCE = "native_usd"
_BCV_SOURCE_PREFIX = "bcv"
_QUANTIZE = Decimal("0.01")


class ConsolidatedRow(BaseModel):
    """One transaction row inside the consolidated USD report.

    ``rate_source`` is one of:

    * ``user_rate``                 — resolver used ``Transaction.user_rate``
    * ``binance_p2p_median``        — exact-day Binance P2P median
    * ``binance_p2p_median_carry``  — carry-forward Binance P2P median
    * ``bcv``                       — exact-day BCV fallback
    * ``bcv_carry``                 — carry-forward BCV fallback
    * ``native_usd``                — currency is USD/USDT/USDC, no rate used
    * ``needs_review``              — resolver could not find any rate
    """

    model_config = ConfigDict(strict=True, extra="forbid")

    transaction_id: int
    occurred_at: datetime
    account_id: int
    kind: str
    currency: str
    amount_native: Decimal
    amount_usd: Decimal | None
    rate_source: str
    description: str | None
    is_bcv_fallback: bool


class ConsolidatedReport(BaseModel):
    """Full consolidated USD report + headline-rule bookkeeping."""

    model_config = ConfigDict(strict=True, extra="forbid")

    rows: list[ConsolidatedRow]
    total_usd: Decimal
    fallback_total_usd: Decimal
    fallback_row_count: int
    strict_violations: list[int]


# ---------------------------------------------------------------------------
# Core report builder.
# ---------------------------------------------------------------------------


def _fetch_non_transfer_transactions(
    conn: sqlite3.Connection,
) -> list[Transaction]:
    """Return every non-transfer transaction, ordered deterministically."""
    rows = conn.execute(
        """
        SELECT id, account_id, occurred_at, kind, amount, currency, description,
               category_id, transfer_id, user_rate, source, source_ref, needs_review
        FROM transactions
        WHERE kind <> 'transfer'
        ORDER BY occurred_at ASC, id ASC
        """
    ).fetchall()
    return [_row_to_transaction(r) for r in rows]


def _compute_row(conn: sqlite3.Connection, txn: Transaction) -> ConsolidatedRow:
    """Resolve one transaction into a :class:`ConsolidatedRow`.

    Follows the same USD arithmetic as the ``v_transactions_usd`` SQL view so
    report totals stay consistent with any other consumer that might read the
    view directly. All math runs in ``Decimal``; floats never enter the path.
    """
    assert txn.id is not None, "transactions persisted via the repo always have an id"

    # Native-USD-ish currencies bypass the resolver entirely.
    if txn.currency in _NATIVE_USD_CURRENCIES:
        return ConsolidatedRow(
            transaction_id=txn.id,
            occurred_at=txn.occurred_at,
            account_id=txn.account_id,
            kind=txn.kind.value,
            currency=txn.currency,
            amount_native=txn.amount,
            amount_usd=txn.amount,
            rate_source=_NATIVE_USD_SOURCE,
            description=txn.description,
            is_bcv_fallback=False,
        )

    rate, source = rates_engine.resolve(conn, txn)

    if rate is None:
        # needs_review: unresolved, not a BCV fallback.
        return ConsolidatedRow(
            transaction_id=txn.id,
            occurred_at=txn.occurred_at,
            account_id=txn.account_id,
            kind=txn.kind.value,
            currency=txn.currency,
            amount_native=txn.amount,
            amount_usd=None,
            rate_source=source,
            description=txn.description,
            is_bcv_fallback=False,
        )

    amount_usd = txn.amount / rate
    is_bcv_fallback = source.startswith(_BCV_SOURCE_PREFIX)
    return ConsolidatedRow(
        transaction_id=txn.id,
        occurred_at=txn.occurred_at,
        account_id=txn.account_id,
        kind=txn.kind.value,
        currency=txn.currency,
        amount_native=txn.amount,
        amount_usd=amount_usd,
        rate_source=source,
        description=txn.description,
        is_bcv_fallback=is_bcv_fallback,
    )


def build_report(
    conn: sqlite3.Connection, *, strict: bool = False
) -> ConsolidatedReport:
    """Build the consolidated USD report.

    ``strict`` does **not** change the shape of the returned report — it is
    kept as an explicit parameter so callers can document their intent and
    so future behaviour (e.g. logging) can hook in without breaking the API.
    The CLI caller decides to exit non-zero based on
    :attr:`ConsolidatedReport.strict_violations`.
    """
    _ = strict  # accepted for symmetry with the CLI layer; see docstring.

    txns = _fetch_non_transfer_transactions(conn)
    rows: list[ConsolidatedRow] = [_compute_row(conn, txn) for txn in txns]

    total_usd = Decimal("0")
    fallback_total_usd = Decimal("0")
    fallback_row_count = 0
    strict_violations: list[int] = []

    for row in rows:
        if row.amount_usd is None:
            continue
        if row.is_bcv_fallback:
            fallback_total_usd += row.amount_usd
            fallback_row_count += 1
            strict_violations.append(row.transaction_id)
        elif row.rate_source != rates_engine.NEEDS_REVIEW_SOURCE:
            total_usd += row.amount_usd

    return ConsolidatedReport(
        rows=rows,
        total_usd=total_usd,
        fallback_total_usd=fallback_total_usd,
        fallback_row_count=fallback_row_count,
        strict_violations=strict_violations,
    )


# ---------------------------------------------------------------------------
# Rendering helpers.
# ---------------------------------------------------------------------------


def _fmt_decimal(value: Decimal | None) -> str:
    """Quantize Decimals to 2dp for display; None -> empty string."""
    if value is None:
        return ""
    return str(value.quantize(_QUANTIZE))


def _column_widths(rows: list[list[str]]) -> list[int]:
    if not rows:
        return []
    widths = [0] * len(rows[0])
    for row in rows:
        for i, cell in enumerate(row):
            widths[i] = max(widths[i], len(cell))
    return widths


def _format_row(cells: list[str], widths: list[int]) -> str:
    return "  ".join(cell.ljust(widths[i]) for i, cell in enumerate(cells))


def render_table(report: ConsolidatedReport) -> str:
    """Render the report as a pretty plain-text table.

    Columns: Date | Account | Kind | Native | USD | Rate Source | Flag.
    Fallback rows carry a ``[BCV fallback]`` annotation in the Flag column.
    The footer surfaces ``total_usd`` and ``fallback_total_usd`` so the
    user can see the headline / fallback split at a glance.
    """
    header = ["Date", "Account", "Kind", "Native", "USD", "Rate Source", "Flag"]
    body: list[list[str]] = []
    for row in report.rows:
        body.append(
            [
                row.occurred_at.date().isoformat(),
                str(row.account_id),
                row.kind,
                f"{_fmt_decimal(row.amount_native)} {row.currency}",
                _fmt_decimal(row.amount_usd),
                row.rate_source,
                "[BCV fallback]" if row.is_bcv_fallback else "",
            ]
        )

    all_rows = [header, *body]
    widths = _column_widths(all_rows)
    lines = [_format_row(header, widths)]
    if widths:
        lines.append("  ".join("-" * w for w in widths))
    for cells in body:
        lines.append(_format_row(cells, widths))

    footer = (
        f"total_usd (headline): {_fmt_decimal(report.total_usd)}  |  "
        f"fallback_total_usd: {_fmt_decimal(report.fallback_total_usd)} "
        f"({report.fallback_row_count} row"
        f"{'' if report.fallback_row_count == 1 else 's'})"
    )
    lines.append("")
    lines.append(footer)
    return "\n".join(lines)


def _row_to_payload(row: ConsolidatedRow) -> dict[str, object]:
    """Serialize a row for JSON output; Decimals as strings, dt as ISO8601."""
    return {
        "transaction_id": row.transaction_id,
        "occurred_at": row.occurred_at.isoformat(),
        "account_id": row.account_id,
        "kind": row.kind,
        "currency": row.currency,
        "amount_native": str(row.amount_native),
        "amount_usd": None if row.amount_usd is None else str(row.amount_usd),
        "rate_source": row.rate_source,
        "description": row.description,
        "is_bcv_fallback": row.is_bcv_fallback,
    }


def render_json(report: ConsolidatedReport) -> str:
    """Render the report as a JSON document.

    Decimals are serialized as strings to preserve full precision; datetimes
    use ISO-8601. The shape matches :class:`ConsolidatedReport` with the
    rows expanded via :func:`_row_to_payload`.
    """
    payload = {
        "rows": [_row_to_payload(r) for r in report.rows],
        "total_usd": str(report.total_usd),
        "fallback_total_usd": str(report.fallback_total_usd),
        "fallback_row_count": report.fallback_row_count,
        "strict_violations": list(report.strict_violations),
    }
    return json.dumps(payload, indent=2, sort_keys=False)


_CSV_HEADER = (
    "transaction_id",
    "occurred_at",
    "account_id",
    "kind",
    "currency",
    "amount_native",
    "amount_usd",
    "rate_source",
    "is_bcv_fallback",
    "description",
)


def _csv_row(row: ConsolidatedRow) -> Iterable[object]:
    return (
        row.transaction_id,
        row.occurred_at.isoformat(),
        row.account_id,
        row.kind,
        row.currency,
        str(row.amount_native),
        "" if row.amount_usd is None else str(row.amount_usd),
        row.rate_source,
        "true" if row.is_bcv_fallback else "false",
        row.description if row.description is not None else "",
    )


def render_csv(report: ConsolidatedReport) -> str:
    """Render the report as CSV (header + one row per transaction)."""
    buf = io.StringIO()
    writer = csv.writer(buf, lineterminator="\n")
    writer.writerow(_CSV_HEADER)
    for row in report.rows:
        writer.writerow(_csv_row(row))
    return buf.getvalue()


__all__ = [
    "ConsolidatedReport",
    "ConsolidatedRow",
    "build_report",
    "render_csv",
    "render_json",
    "render_table",
]
