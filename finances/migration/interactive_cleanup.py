"""EPIC-012 interactive cleanup walker.

Iterates ``WHERE needs_review=1`` rows in chronological order. For each
row the caller-supplied ``prompt`` returns ``(category_name, user_rate)``.
Accepting a category clears ``needs_review`` and updates ``category_id``;
an optional ``user_rate`` is persisted alongside. Returning ``(None, _)``
leaves the row flagged so the user can come back to it later.

The prompt is dependency-injected so tests can drive deterministic inputs
without monkey-patching ``input``.
"""
from __future__ import annotations

import csv
import sqlite3
from collections.abc import Callable
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from pathlib import Path

from finances.db.repos import categories as categories_repo
from finances.domain.categorization import CategorizationRequest, suggest
from finances.domain.models import TransactionKind


PromptFn = Callable[[sqlite3.Row], tuple[str | None, str | None]]


@dataclass
class CleanupReport:
    rows_seen: int = 0
    rows_resolved: int = 0
    rows_skipped: int = 0
    errors: list[str] = field(default_factory=list)


def _row_kind(raw: str) -> TransactionKind:
    return TransactionKind(raw)


def _resolve_category(
    conn: sqlite3.Connection, kind: TransactionKind, name: str
) -> int:
    found = categories_repo.get_by_name(conn, kind, name)
    if found is None or found.id is None:
        raise ValueError(
            f"unknown category for kind={kind.value!r}: {name!r}"
        )
    return found.id


def _parse_rate(raw: str | None) -> Decimal | None:
    if raw is None:
        return None
    text = raw.strip()
    if not text:
        return None
    try:
        value = Decimal(text)
    except InvalidOperation as exc:
        raise ValueError(f"invalid user_rate: {raw!r}") from exc
    if value <= 0:
        raise ValueError(f"user_rate must be positive: {raw!r}")
    return value


def _apply(
    conn: sqlite3.Connection,
    *,
    transaction_id: int,
    category_id: int,
    user_rate: Decimal | None,
) -> None:
    if user_rate is None:
        conn.execute(
            "UPDATE transactions "
            "SET category_id = ?, needs_review = 0, updated_at = CURRENT_TIMESTAMP "
            "WHERE id = ?",
            (category_id, transaction_id),
        )
    else:
        conn.execute(
            "UPDATE transactions "
            "SET category_id = ?, user_rate = ?, needs_review = 0, "
            "    updated_at = CURRENT_TIMESTAMP "
            "WHERE id = ?",
            (category_id, format(user_rate, "f"), transaction_id),
        )


def run_cleanup(
    conn: sqlite3.Connection,
    *,
    prompt: PromptFn,
) -> CleanupReport:
    """Walk every ``needs_review=1`` row, applying the prompt's answer.

    ``prompt`` receives one ``sqlite3.Row`` per review item and returns
    ``(category_name, user_rate)`` — both optional. Returning a ``None``
    category leaves the row flagged for a later pass.
    """
    report = CleanupReport()
    rows = conn.execute(
        """
        SELECT id, account_id, occurred_at, kind, amount, currency,
               description, category_id, user_rate, source
        FROM transactions
        WHERE needs_review = 1
        ORDER BY occurred_at ASC, id ASC
        """
    ).fetchall()

    for row in rows:
        report.rows_seen += 1
        answer = prompt(row)
        category_name, rate_raw = answer
        if category_name is None or not category_name.strip():
            report.rows_skipped += 1
            continue
        kind = _row_kind(row["kind"])
        category_id = _resolve_category(conn, kind, category_name.strip())
        user_rate = _parse_rate(rate_raw)
        _apply(
            conn,
            transaction_id=int(row["id"]),
            category_id=category_id,
            user_rate=user_rate,
        )
        report.rows_resolved += 1

    return report


_EXPORT_FIELDS = (
    "id",
    "occurred_at",
    "source",
    "kind",
    "amount",
    "currency",
    "description",
    "suggested_category",
    "category",
    "user_rate",
)


def _suggested_category_name(
    conn: sqlite3.Connection, row: sqlite3.Row
) -> str:
    """Return the rules-engine hit for this row, if any, else empty string."""
    match = suggest(
        conn,
        CategorizationRequest(
            description=row["description"],
            source=row["source"],
            account_id=row["account_id"],
        ),
    )
    if match is None:
        return ""
    cat = categories_repo.get_by_id(conn, match.category_id)
    return "" if cat is None else cat.name


def export_needs_review(conn: sqlite3.Connection, csv_path: Path) -> int:
    """Dump every ``needs_review=1`` row to ``csv_path`` for batch review.

    Columns: ``id``, ``occurred_at``, ``source``, ``kind``, ``amount``,
    ``currency``, ``description``, ``suggested_category`` (what the
    rules engine would pick; empty if no hit), ``category`` (blank —
    user fills in Sheets), ``user_rate`` (blank — user fills optionally).

    Returns the number of rows written.
    """
    rows = conn.execute(
        """
        SELECT id, occurred_at, source, kind, amount, currency,
               description, account_id, user_rate
        FROM transactions
        WHERE needs_review = 1
        ORDER BY occurred_at ASC, id ASC
        """
    ).fetchall()
    with csv_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(_EXPORT_FIELDS))
        writer.writeheader()
        for row in rows:
            occurred_at = row["occurred_at"]
            iso = (
                occurred_at.isoformat()
                if hasattr(occurred_at, "isoformat")
                else str(occurred_at)
            )
            existing_rate = (
                format(Decimal(str(row["user_rate"])), "f")
                if row["user_rate"] is not None
                else ""
            )
            writer.writerow({
                "id": row["id"],
                "occurred_at": iso,
                "source": row["source"],
                "kind": row["kind"],
                "amount": format(Decimal(str(row["amount"])), "f"),
                "currency": row["currency"],
                "description": row["description"] or "",
                "suggested_category": _suggested_category_name(conn, row),
                "category": "",
                "user_rate": existing_rate,
            })
    return len(rows)


def import_cleanup_csv(
    conn: sqlite3.Connection, csv_path: Path
) -> CleanupReport:
    """Read a user-edited cleanup CSV and apply the ``category`` column.

    Contract:

    * The ``id`` column pins each row to an existing transaction.
    * ``category`` blank → row is skipped (``rows_skipped`` counter).
    * ``category`` present → resolved against the row's kind; unknown
      names raise ``ValueError`` before any write (no partial apply).
    * ``user_rate`` present → parsed as Decimal and persisted; blank →
      left untouched.
    """
    report = CleanupReport()
    with csv_path.open("r", encoding="utf-8") as handle:
        entries = list(csv.DictReader(handle))

    # Pre-validate every category before we write anything, so a typo on
    # row 500 doesn't leave rows 1–499 half-applied.
    parsed: list[tuple[int, int | None, Decimal | None]] = []
    for entry in entries:
        report.rows_seen += 1
        tid_raw = (entry.get("id") or "").strip()
        if not tid_raw:
            continue
        transaction_id = int(tid_raw)
        category_name = (entry.get("category") or "").strip()
        if not category_name:
            report.rows_skipped += 1
            parsed.append((transaction_id, None, None))
            continue
        row = conn.execute(
            "SELECT kind FROM transactions WHERE id = ?",
            (transaction_id,),
        ).fetchone()
        if row is None:
            raise ValueError(f"transaction id {transaction_id} not found")
        kind = _row_kind(row["kind"])
        category_id = _resolve_category(conn, kind, category_name)
        user_rate = _parse_rate(entry.get("user_rate"))
        parsed.append((transaction_id, category_id, user_rate))

    for transaction_id, category_id, user_rate in parsed:
        if category_id is None:
            continue
        _apply(
            conn,
            transaction_id=transaction_id,
            category_id=category_id,
            user_rate=user_rate,
        )
        report.rows_resolved += 1

    return report


__all__ = [
    "CleanupReport",
    "PromptFn",
    "export_needs_review",
    "import_cleanup_csv",
    "run_cleanup",
]
