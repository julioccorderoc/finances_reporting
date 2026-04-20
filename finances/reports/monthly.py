"""Monthly aggregate report (EPIC-013).

Groups every non-transfer transaction by ``(month, account, category, kind)``
and reports the native total, the *headline* USD total, and a separate
*fallback* USD column for BCV-sourced rows.

ADR-005 (amended 2026-04-19) and rule-005 mandate that BCV-sourced USD values
are never the headline number; they must be surfaced as a fallback. This
report is consumed by the ``Monthly`` Sheets tab — which is a headline
surface — so we isolate BCV-sourced contributions into ``fallback_usd`` and
keep ``total_usd`` limited to ``user_rate`` / ``binance_p2p_median`` /
``native_usd`` contributions.

All rate resolution goes through :func:`finances.domain.rates.resolve` — this
module must never reimplement the priority chain. The native-USD pass-through
(USD / USDT / USDC) mirrors the ``v_transactions_usd`` SQL view so the
sum-invariant in the epic's Definition of Done stays tight.

The module is read-only: it never mutates the DB. The resolver may toggle
``Transaction.needs_review`` on an in-memory copy for rows that fail to
resolve; that side effect is intentional and documented in ADR-005 but never
persisted here.
"""

from __future__ import annotations

import csv
import io
import json
import re
import sqlite3
from collections.abc import Iterable
from decimal import Decimal

from pydantic import BaseModel, ConfigDict

from finances.db.repos import accounts as accounts_repo
from finances.db.repos import categories as categories_repo
from finances.db.repos.transactions import _row_to_transaction
from finances.domain import rates as rates_engine
from finances.domain.models import Transaction


# ---------------------------------------------------------------------------
# Constants.
# ---------------------------------------------------------------------------


_NATIVE_USD_CURRENCIES = frozenset({"USD", "USDT", "USDC"})
_BCV_SOURCE_PREFIX = "bcv"
_MONTH_RE = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")


# ---------------------------------------------------------------------------
# Pydantic boundary.
# ---------------------------------------------------------------------------


class MonthlyRow(BaseModel):
    """One aggregated row of the monthly report.

    Aggregation key is ``(month, account_id, category_id, kind)``. ``month``
    is a ``YYYY-MM`` string. ``total_usd`` sums only headline-eligible USD
    contributions (``user_rate`` / ``binance_p2p_median`` / ``native_usd``);
    ``fallback_usd`` sums BCV-sourced contributions; ``needs_review_count``
    counts rows whose rate could not be resolved and therefore contribute to
    neither total.
    """

    model_config = ConfigDict(strict=True, extra="forbid")

    month: str
    account_id: int
    account_name: str
    category_id: int | None
    category_name: str | None
    kind: str
    tx_count: int
    total_native: Decimal
    currency: str
    total_usd: Decimal
    fallback_usd: Decimal
    needs_review_count: int


class MonthlyReport(BaseModel):
    """Full monthly report + grand totals."""

    model_config = ConfigDict(strict=True, extra="forbid")

    rows: list[MonthlyRow]
    month_range: tuple[str, str] | None
    grand_total_usd: Decimal
    fallback_total_usd: Decimal


# ---------------------------------------------------------------------------
# Input validation.
# ---------------------------------------------------------------------------


def _validate_month(value: str | None, *, field: str) -> str | None:
    if value is None:
        return None
    if not _MONTH_RE.match(value):
        raise ValueError(
            f"invalid {field} value {value!r}: expected YYYY-MM (e.g. '2026-02')"
        )
    return value


# ---------------------------------------------------------------------------
# Aggregation accumulator.
# ---------------------------------------------------------------------------


class _RowAccumulator:
    """Mutable bucket for one ``(month, account, category, kind)`` combo.

    Kept as a plain class so we can mutate Decimal totals without re-building
    a Pydantic model per transaction. The accumulator is serialized to a
    :class:`MonthlyRow` via :meth:`freeze` at the end of ``build_report``.
    """

    __slots__ = (
        "account_id",
        "account_name",
        "category_id",
        "category_name",
        "currency",
        "fallback_usd",
        "kind",
        "month",
        "needs_review_count",
        "total_native",
        "total_usd",
        "tx_count",
    )

    def __init__(
        self,
        *,
        month: str,
        account_id: int,
        account_name: str,
        category_id: int | None,
        category_name: str | None,
        kind: str,
        currency: str,
    ) -> None:
        self.month = month
        self.account_id = account_id
        self.account_name = account_name
        self.category_id = category_id
        self.category_name = category_name
        self.kind = kind
        self.currency = currency
        self.tx_count = 0
        self.total_native = Decimal("0")
        self.total_usd = Decimal("0")
        self.fallback_usd = Decimal("0")
        self.needs_review_count = 0

    def freeze(self) -> MonthlyRow:
        return MonthlyRow(
            month=self.month,
            account_id=self.account_id,
            account_name=self.account_name,
            category_id=self.category_id,
            category_name=self.category_name,
            kind=self.kind,
            tx_count=self.tx_count,
            total_native=self.total_native,
            currency=self.currency,
            total_usd=self.total_usd,
            fallback_usd=self.fallback_usd,
            needs_review_count=self.needs_review_count,
        )


# ---------------------------------------------------------------------------
# Per-transaction USD contribution (mirrors v_transactions_usd math).
# ---------------------------------------------------------------------------


def _resolve_contribution(
    conn: sqlite3.Connection, txn: Transaction
) -> tuple[Decimal, str]:
    """Return ``(amount_usd_or_zero, bucket)`` for ``txn``.

    ``bucket`` is one of ``"headline"``, ``"fallback"``, or ``"needs_review"``
    and routes the amount to the appropriate accumulator field.

    Native-USD currencies (USD/USDT/USDC) bypass the resolver — same as the
    ``v_transactions_usd`` view — so a transaction in one of those currencies
    is always headline-eligible.
    """
    if txn.currency in _NATIVE_USD_CURRENCIES:
        return txn.amount, "headline"

    rate, source = rates_engine.resolve(conn, txn)
    if rate is None:
        return Decimal("0"), "needs_review"

    amount_usd = txn.amount / rate
    if source.startswith(_BCV_SOURCE_PREFIX):
        return amount_usd, "fallback"
    return amount_usd, "headline"


# ---------------------------------------------------------------------------
# Core query.
# ---------------------------------------------------------------------------


def _fetch_transactions_in_range(
    conn: sqlite3.Connection, *, since: str | None, until: str | None
) -> list[Transaction]:
    """Fetch non-transfer transactions whose month is within ``[since, until]``.

    Month filtering happens in SQL via ``strftime('%Y-%m', occurred_at)`` so
    we mirror the ``v_monthly_summary`` / ``v_transactions_usd`` behaviour
    exactly. That keeps the epic's sum-invariant ("report rows sum to the
    same total as ``v_transactions_usd`` for the month") tight.
    """
    sql = [
        """
        SELECT id, account_id, occurred_at, kind, amount, currency, description,
               category_id, transfer_id, user_rate, source, source_ref, needs_review
        FROM transactions
        WHERE kind <> 'transfer'
        """
    ]
    params: list[str] = []
    if since is not None:
        sql.append("AND strftime('%Y-%m', occurred_at) >= ?")
        params.append(since)
    if until is not None:
        sql.append("AND strftime('%Y-%m', occurred_at) <= ?")
        params.append(until)
    sql.append("ORDER BY occurred_at ASC, id ASC")

    rows = conn.execute(" ".join(sql), tuple(params)).fetchall()
    return [_row_to_transaction(r) for r in rows]


# ---------------------------------------------------------------------------
# Public API.
# ---------------------------------------------------------------------------


def build_report(
    conn: sqlite3.Connection,
    *,
    month: str | None = None,
    since: str | None = None,
    until: str | None = None,
) -> MonthlyReport:
    """Build the monthly report.

    Parameters
    ----------
    conn:
        Read-only connection. The function never writes to the DB.
    month:
        Convenience shortcut for ``since=month, until=month``. If provided,
        ``since`` / ``until`` must be ``None`` — the caller may not mix them.
    since, until:
        Inclusive ``YYYY-MM`` bounds. ``None`` means "no bound on that
        side". When both are ``None``, all history is covered.

    Raises
    ------
    ValueError
        If any supplied string is not a ``YYYY-MM`` value, or if ``month`` is
        mixed with ``since``/``until``.
    """
    if month is not None and (since is not None or until is not None):
        raise ValueError(
            "pass either 'month' OR 'since'/'until', not both"
        )

    if month is not None:
        validated = _validate_month(month, field="month")
        since = until = validated
    else:
        since = _validate_month(since, field="since")
        until = _validate_month(until, field="until")
        if since is not None and until is not None and since > until:
            raise ValueError(
                f"invalid range: since={since!r} is after until={until!r}"
            )

    txns = _fetch_transactions_in_range(conn, since=since, until=until)

    # Caches keep us from hammering the repos when a single account or
    # category repeats across many transactions.
    account_name_cache: dict[int, str] = {}
    category_name_cache: dict[int, str | None] = {}

    def _account_name(account_id: int) -> str:
        if account_id not in account_name_cache:
            account = accounts_repo.get_by_id(conn, account_id)
            account_name_cache[account_id] = (
                account.name if account is not None else f"Account #{account_id}"
            )
        return account_name_cache[account_id]

    def _category_name(category_id: int | None) -> str | None:
        if category_id is None:
            return None
        if category_id not in category_name_cache:
            category = categories_repo.get_by_id(conn, category_id)
            category_name_cache[category_id] = (
                category.name if category is not None else None
            )
        return category_name_cache[category_id]

    buckets: dict[tuple[str, int, int | None, str], _RowAccumulator] = {}

    for txn in txns:
        month_key = txn.occurred_at.strftime("%Y-%m")
        key = (month_key, txn.account_id, txn.category_id, txn.kind.value)
        bucket = buckets.get(key)
        if bucket is None:
            bucket = _RowAccumulator(
                month=month_key,
                account_id=txn.account_id,
                account_name=_account_name(txn.account_id),
                category_id=txn.category_id,
                category_name=_category_name(txn.category_id),
                kind=txn.kind.value,
                currency=txn.currency,
            )
            buckets[key] = bucket

        bucket.tx_count += 1
        bucket.total_native += txn.amount

        amount_usd, where = _resolve_contribution(conn, txn)
        if where == "headline":
            bucket.total_usd += amount_usd
        elif where == "fallback":
            bucket.fallback_usd += amount_usd
        else:  # needs_review
            bucket.needs_review_count += 1

    # Deterministic ordering: month, account name, category name, kind.
    rows = sorted(
        (b.freeze() for b in buckets.values()),
        key=lambda r: (
            r.month,
            r.account_name,
            r.category_name or "",
            r.kind,
        ),
    )

    grand_total_usd = sum((r.total_usd for r in rows), Decimal("0"))
    fallback_total_usd = sum((r.fallback_usd for r in rows), Decimal("0"))

    month_range: tuple[str, str] | None
    if since is not None and until is not None:
        month_range = (since, until)
    elif since is not None:
        month_range = (since, since)
    elif until is not None:
        month_range = (until, until)
    else:
        month_range = None

    return MonthlyReport(
        rows=rows,
        month_range=month_range,
        grand_total_usd=grand_total_usd,
        fallback_total_usd=fallback_total_usd,
    )


# ---------------------------------------------------------------------------
# Rendering helpers.
# ---------------------------------------------------------------------------


_CSV_HEADER: tuple[str, ...] = (
    "month",
    "account_id",
    "account_name",
    "category_id",
    "category_name",
    "kind",
    "tx_count",
    "total_native",
    "currency",
    "total_usd",
    "fallback_usd",
    "needs_review_count",
)


def _csv_row(row: MonthlyRow) -> Iterable[object]:
    return (
        row.month,
        row.account_id,
        row.account_name,
        "" if row.category_id is None else row.category_id,
        "" if row.category_name is None else row.category_name,
        row.kind,
        row.tx_count,
        str(row.total_native),
        row.currency,
        str(row.total_usd),
        str(row.fallback_usd),
        row.needs_review_count,
    )


def render_csv(report: MonthlyReport) -> str:
    """Serialize ``report`` as CSV (header + one row per bucket)."""
    buf = io.StringIO()
    writer = csv.writer(buf, lineterminator="\n")
    writer.writerow(_CSV_HEADER)
    for row in report.rows:
        writer.writerow(_csv_row(row))
    return buf.getvalue()


def render_json(report: MonthlyReport) -> str:
    """Serialize ``report`` as JSON with Decimals emitted as strings."""
    payload = {
        "rows": [
            {
                "month": r.month,
                "account_id": r.account_id,
                "account_name": r.account_name,
                "category_id": r.category_id,
                "category_name": r.category_name,
                "kind": r.kind,
                "tx_count": r.tx_count,
                "total_native": str(r.total_native),
                "currency": r.currency,
                "total_usd": str(r.total_usd),
                "fallback_usd": str(r.fallback_usd),
                "needs_review_count": r.needs_review_count,
            }
            for r in report.rows
        ],
        "month_range": (
            list(report.month_range) if report.month_range is not None else None
        ),
        "grand_total_usd": str(report.grand_total_usd),
        "fallback_total_usd": str(report.fallback_total_usd),
    }
    return json.dumps(payload, indent=2, sort_keys=False)


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


def render_table(report: MonthlyReport) -> str:
    """Render ``report`` as a plain-text monospace table.

    Columns: ``Month``, ``Account``, ``Category``, ``Kind``, ``Count``,
    ``Native``, ``USD``, ``Fallback``. Footer surfaces
    ``grand_total_usd`` (headline) and ``fallback_total_usd``.
    """
    header = [
        "Month",
        "Account",
        "Category",
        "Kind",
        "Count",
        "Native",
        "USD",
        "Fallback",
    ]
    body: list[list[str]] = []
    for row in report.rows:
        body.append(
            [
                row.month,
                row.account_name,
                row.category_name if row.category_name is not None else "-",
                row.kind,
                str(row.tx_count),
                f"{row.total_native} {row.currency}",
                str(row.total_usd),
                str(row.fallback_usd),
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
        f"grand_total_usd (headline): {report.grand_total_usd}  |  "
        f"fallback_total_usd: {report.fallback_total_usd}"
    )
    lines.append("")
    lines.append(footer)
    return "\n".join(lines)


__all__ = [
    "MonthlyReport",
    "MonthlyRow",
    "build_report",
    "render_csv",
    "render_json",
    "render_table",
]
