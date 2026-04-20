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

import sqlite3
from collections.abc import Callable
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation

from finances.db.repos import categories as categories_repo
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


__all__ = [
    "CleanupReport",
    "PromptFn",
    "run_cleanup",
]
