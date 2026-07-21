"""Filtered + paginated transactions query for the web viewer (EPIC-023).

This is the only place the viewer touches the ``transactions`` table for
read-side projections. Per rule-012 we don't reimplement business logic:

* rate resolution → :func:`finances.domain.rates.resolve` (rule-005),
* the SQL is a thin filter against the canonical schema; no JOINs that
  duplicate logic the repos already encapsulate (we only LEFT JOIN
  accounts + categories for cheap label lookup),
* every filter value is bound via parameter substitution. No string
  interpolation. Ever.

The output ``TransactionCard`` is *projection-only*: it carries the
fields a card needs to render. It is the same shape used by Phase 2a's
dashboard "recent activity" panel and Phase 4's triage cards — keep it
self-contained.
"""

from __future__ import annotations

import sqlite3
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

from finances.domain import rates as rates_engine
from finances.domain.models import Transaction, TransactionKind

_NATIVE_USD_CURRENCIES = frozenset({"USD", "USDT", "USDC"})
_NATIVE_USD_SOURCE = "native_usd"
_BCV_SOURCE_PREFIX = "bcv"
_DEFAULT_WINDOW_DAYS = 30

_KindLiteral = Literal["income", "expense", "transfer", "adjustment"]
_NeedsReviewLiteral = Literal["any", "yes", "no"]
_PairedLiteral = Literal["any", "yes", "no"]
_SortLiteral = Literal["occurred_at", "amount_native", "amount_usd"]
_DirectionLiteral = Literal["asc", "desc"]
_PageSizeLiteral = Literal[25, 50, 100]

_SORT_COLUMN_MAP: dict[str, str] = {
    "occurred_at": "t.occurred_at",
    "amount_native": "CAST(t.amount AS REAL)",
    # amount_usd cannot be sorted by SQL (resolver-derived); fall back to
    # native amount as a stable tiebreaker. Documented surprise; the JSON
    # surface will still echo "amount_usd" when requested.
    "amount_usd": "CAST(t.amount AS REAL)",
}

# Shared SELECT prefix for every row that will pass through
# _row_to_transaction. Keep the column list and that function in sync —
# they are the pair that breaks when a column is added to transactions.
TXN_QUERY_BASE = """
    SELECT
        t.id, t.account_id, t.occurred_at, t.kind, t.amount, t.currency,
        t.description, t.category_id, t.transfer_id, t.user_rate,
        t.source, t.source_ref, t.needs_review, t.notes,
        a.name AS account_name,
        c.name AS category_name
    FROM transactions t
    LEFT JOIN accounts a ON a.id = t.account_id
    LEFT JOIN categories c ON c.id = t.category_id
"""


# ---------------------------------------------------------------------------
# Pydantic models.
# ---------------------------------------------------------------------------


class TransactionsFilter(BaseModel):
    """URL-persistent filter state for /transactions.

    All list-valued fields default to empty (``[]``) which the SQL builder
    interprets as "no constraint". ``date_from`` / ``date_to`` default to
    a 30-day window ending today (computed at request time, not import
    time).
    """

    model_config = ConfigDict(extra="forbid")

    date_from: date | None = None
    date_to: date | None = None
    accounts: list[str] = Field(default_factory=list)
    categories: list[str] = Field(default_factory=list)
    kinds: list[_KindLiteral] = Field(default_factory=list)
    sources: list[str] = Field(default_factory=list)
    currencies: list[str] = Field(default_factory=list)
    needs_review: _NeedsReviewLiteral = "any"
    paired: _PairedLiteral = "any"
    q: str = ""
    sort: _SortLiteral = "occurred_at"
    direction: _DirectionLiteral = "desc"
    page: int = 1
    page_size: _PageSizeLiteral = 50


class TransactionCard(BaseModel):
    """One row, projected for card rendering.

    Fields chosen to match the Q6 lock + Phase 2a's dashboard panel.
    No transfer-id badge field; transfer rows are recognized via
    ``kind == 'transfer'``.
    """

    model_config = ConfigDict(extra="forbid")

    id: int
    occurred_at: datetime
    account_name: str
    description: str
    amount_native: Decimal
    currency: str
    amount_usd: Decimal | None
    rate_source: str
    is_bcv_fallback: bool
    kind: str
    category_name: str | None
    needs_review: bool
    notes: str | None = None


class TransactionsPage(BaseModel):
    """Paginated result for /api/transactions and the HTMX fragment."""

    model_config = ConfigDict(extra="forbid")

    rows: list[TransactionCard]
    total: int
    page: int
    page_size: int
    total_pages: int
    filter: TransactionsFilter


# ---------------------------------------------------------------------------
# Filter defaults + resolution.
# ---------------------------------------------------------------------------


def resolve_defaults(f: TransactionsFilter) -> TransactionsFilter:
    """Fill in date defaults at request time (last-30-days window)."""
    if f.date_from is not None and f.date_to is not None:
        return f
    today = datetime.now(tz=timezone.utc).date()
    df = f.date_from if f.date_from is not None else today - timedelta(days=_DEFAULT_WINDOW_DAYS)
    dt = f.date_to if f.date_to is not None else today
    return f.model_copy(update={"date_from": df, "date_to": dt})


# ---------------------------------------------------------------------------
# SQL builder.
# ---------------------------------------------------------------------------


def _build_where(f: TransactionsFilter) -> tuple[str, list[Any]]:
    where: list[str] = []
    params: list[Any] = []

    if f.date_from is not None:
        where.append("date(t.occurred_at) >= ?")
        params.append(f.date_from.isoformat())
    if f.date_to is not None:
        where.append("date(t.occurred_at) <= ?")
        params.append(f.date_to.isoformat())

    if f.accounts:
        placeholders = ",".join(["?"] * len(f.accounts))
        where.append(f"a.name IN ({placeholders})")
        params.extend(f.accounts)

    if f.categories:
        placeholders = ",".join(["?"] * len(f.categories))
        where.append(f"c.name IN ({placeholders})")
        params.extend(f.categories)

    if f.kinds:
        placeholders = ",".join(["?"] * len(f.kinds))
        where.append(f"t.kind IN ({placeholders})")
        params.extend(f.kinds)

    if f.sources:
        placeholders = ",".join(["?"] * len(f.sources))
        where.append(f"t.source IN ({placeholders})")
        params.extend(f.sources)

    if f.currencies:
        placeholders = ",".join(["?"] * len(f.currencies))
        where.append(f"t.currency IN ({placeholders})")
        params.extend(f.currencies)

    if f.needs_review == "yes":
        where.append("t.needs_review = 1")
    elif f.needs_review == "no":
        where.append("t.needs_review = 0")

    # transfer_id IS NULL is what makes a row "unpaired" — kind never does,
    # since backfilled rows can carry kind='transfer' with no counterpart.
    if f.paired == "yes":
        where.append("t.transfer_id IS NOT NULL")
    elif f.paired == "no":
        where.append("t.transfer_id IS NULL")

    if f.q:
        # Free-text search covers the statement description AND the manual
        # note (WP3). NULL notes are fine: NULL LIKE x is NULL, OR handles it.
        where.append("(t.description LIKE ? OR t.notes LIKE ?)")
        # Bind the wildcard pattern as a parameter, never interpolated.
        pattern = f"%{f.q}%"
        params.extend([pattern, pattern])

    sql = " AND ".join(where) if where else "1 = 1"
    return sql, params


def _row_to_transaction(row: sqlite3.Row) -> Transaction:
    return Transaction(
        id=row["id"],
        account_id=row["account_id"],
        occurred_at=row["occurred_at"],
        kind=TransactionKind(row["kind"]),
        amount=row["amount"] if isinstance(row["amount"], Decimal) else Decimal(str(row["amount"])),
        currency=row["currency"],
        description=row["description"],
        category_id=row["category_id"],
        transfer_id=row["transfer_id"],
        user_rate=(
            None
            if row["user_rate"] is None
            else (
                row["user_rate"]
                if isinstance(row["user_rate"], Decimal)
                else Decimal(str(row["user_rate"]))
            )
        ),
        source=row["source"],
        source_ref=row["source_ref"],
        needs_review=bool(row["needs_review"]),
        notes=row["notes"],
    )


def _project_card(
    conn: sqlite3.Connection,
    txn: Transaction,
    *,
    account_name: str,
    category_name: str | None,
) -> TransactionCard:
    """Project a (txn, account_name, category_name) tuple to a card.

    Rate resolution rules:

    * USD/USDT/USDC → ``native_usd``, amount_usd = amount_native, no rate
      lookup (matches ConsolidatedRow native-currency branch).
    * Otherwise call :func:`rates.resolve` and divide native by rate.
    * resolver returning ``(None, "needs_review")`` keeps amount_usd as
      ``None``.
    """
    assert txn.id is not None

    if txn.currency in _NATIVE_USD_CURRENCIES:
        return TransactionCard(
            id=txn.id,
            occurred_at=txn.occurred_at,
            account_name=account_name,
            description=txn.description or "",
            amount_native=txn.amount,
            currency=txn.currency,
            amount_usd=txn.amount,
            rate_source=_NATIVE_USD_SOURCE,
            is_bcv_fallback=False,
            kind=txn.kind.value,
            category_name=category_name,
            needs_review=txn.needs_review,
            notes=txn.notes,
        )

    rate, source = rates_engine.resolve(conn, txn)
    if rate is None:
        return TransactionCard(
            id=txn.id,
            occurred_at=txn.occurred_at,
            account_name=account_name,
            description=txn.description or "",
            amount_native=txn.amount,
            currency=txn.currency,
            amount_usd=None,
            rate_source=source,
            is_bcv_fallback=False,
            kind=txn.kind.value,
            category_name=category_name,
            needs_review=True,
            notes=txn.notes,
        )

    amount_usd = txn.amount / rate
    is_bcv = source.startswith(_BCV_SOURCE_PREFIX)
    return TransactionCard(
        id=txn.id,
        occurred_at=txn.occurred_at,
        account_name=account_name,
        description=txn.description or "",
        amount_native=txn.amount,
        currency=txn.currency,
        amount_usd=amount_usd,
        rate_source=source,
        is_bcv_fallback=is_bcv,
        kind=txn.kind.value,
        category_name=category_name,
        needs_review=txn.needs_review,
        notes=txn.notes,
    )


def query_transactions(
    conn: sqlite3.Connection, f: TransactionsFilter
) -> TransactionsPage:
    """Run the filtered, sorted, paginated query and return a page."""
    f = resolve_defaults(f)

    where_sql, where_params = _build_where(f)

    sort_column = _SORT_COLUMN_MAP.get(f.sort, "t.occurred_at")
    direction = "DESC" if f.direction == "desc" else "ASC"

    # Total count for pagination — same WHERE, no limit.
    count_sql = f"""
        SELECT COUNT(*) AS c
        FROM transactions t
        LEFT JOIN accounts a ON a.id = t.account_id
        LEFT JOIN categories c ON c.id = t.category_id
        WHERE {where_sql}
    """
    total = int(conn.execute(count_sql, where_params).fetchone()["c"])

    offset = (f.page - 1) * f.page_size
    list_sql = f"""
        SELECT
            t.id, t.account_id, t.occurred_at, t.kind, t.amount, t.currency,
            t.description, t.category_id, t.transfer_id, t.user_rate,
            t.source, t.source_ref, t.needs_review, t.notes,
            a.name AS account_name,
            c.name AS category_name
        FROM transactions t
        LEFT JOIN accounts a ON a.id = t.account_id
        LEFT JOIN categories c ON c.id = t.category_id
        WHERE {where_sql}
        ORDER BY {sort_column} {direction}, t.id {direction}
        LIMIT ? OFFSET ?
    """
    rows = conn.execute(list_sql, (*where_params, f.page_size, offset)).fetchall()

    cards: list[TransactionCard] = []
    for row in rows:
        txn = _row_to_transaction(row)
        cards.append(
            _project_card(
                conn,
                txn,
                account_name=row["account_name"] or "",
                category_name=row["category_name"],
            )
        )

    total_pages = max(1, (total + f.page_size - 1) // f.page_size)

    return TransactionsPage(
        rows=cards,
        total=total,
        page=f.page,
        page_size=f.page_size,
        total_pages=total_pages,
        filter=f,
    )


__all__ = [
    "TransactionCard",
    "TransactionsFilter",
    "TransactionsPage",
    "query_transactions",
    "resolve_defaults",
]
