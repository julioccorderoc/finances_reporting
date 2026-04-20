"""Categorization rules engine (EPIC-004, ADR-006, rule-006).

Given a transaction description, source, and optionally an account, return a
`category_id` (or `None` to signal `needs_review=1`). Rules live in the
`category_rules` table and are regex patterns with optional `source` and
`account_id` scoping plus an integer `priority` (lower wins).

Per ADR-006 amendment, `suggest()` is one tier in an open-ended categorization
priority chain. Future tiers (receipt-supplied category, per-transaction user
override) extend the resolver here — ingesters never set `category_id` directly
(rule-006).

Per ADR-009, inputs are Pydantic models, not loose dicts or kwargs.
"""

from __future__ import annotations

import re
import sqlite3
from functools import lru_cache

from pydantic import BaseModel, ConfigDict


class CategorizationRequest(BaseModel):
    """Input payload for `suggest()`.

    `description` is the free-text bank/exchange memo. `source` is the ingest
    identifier (`provincial`, `binance`, `cash_cli`, ...). `account_id` is
    optional — supply it when account-scoped rules may apply.
    """

    model_config = ConfigDict(strict=True, extra="forbid")

    description: str | None
    source: str
    account_id: int | None = None


class CategoryRule(BaseModel):
    """Row from `category_rules` as a Pydantic model."""

    model_config = ConfigDict(strict=True, extra="forbid")

    id: int | None = None
    pattern: str
    category_id: int
    source: str | None = None
    account_id: int | None = None
    priority: int = 100
    active: bool = True


class RuleMatch(BaseModel):
    """Return payload from `suggest()`."""

    model_config = ConfigDict(strict=True, extra="forbid")

    category_id: int
    rule_id: int
    pattern: str


# Compiled regexes are deterministic functions of their patterns; cache them
# across calls so a typical batch (thousands of rows) doesn't pay re-compile
# cost on every invocation.
@lru_cache(maxsize=512)
def _compile(pattern: str) -> re.Pattern[str]:
    return re.compile(pattern, re.IGNORECASE)


def _row_to_rule(row: sqlite3.Row) -> CategoryRule:
    return CategoryRule(
        id=row["id"],
        pattern=row["pattern"],
        category_id=row["category_id"],
        source=row["source"],
        account_id=row["account_id"],
        priority=row["priority"],
        active=bool(row["active"]),
    )


def load_rules(
    conn: sqlite3.Connection, *, include_inactive: bool = False
) -> list[CategoryRule]:
    """Return every rule in priority order (lowest `priority` first)."""
    query = (
        "SELECT id, pattern, category_id, source, account_id, priority, active "
        "FROM category_rules "
    )
    if not include_inactive:
        query += "WHERE active = 1 "
    query += "ORDER BY priority ASC, id ASC"
    return [_row_to_rule(r) for r in conn.execute(query).fetchall()]


def suggest(
    conn: sqlite3.Connection, request: CategorizationRequest
) -> RuleMatch | None:
    """Return the highest-priority rule that matches the request, or None.

    Scoping semantics:
      * A rule with `source` set matches only that source; `source IS NULL`
        matches any source.
      * A rule with `account_id` set matches only that account; `account_id IS
        NULL` matches any account.

    Priority: lower number wins. Ties are broken in favor of the more specific
    rule (source-scoped before global; account-scoped before global), so the
    user can add a broad fallback at `priority=100` and a narrower override at
    the same priority without re-shuffling everyone.
    """
    if not isinstance(request, CategorizationRequest):
        raise TypeError(
            "suggest() requires a CategorizationRequest; got "
            f"{type(request).__name__}"
        )

    description = request.description
    if not description:
        return None

    # Pull only rules whose scope is compatible with the request. SQLite does
    # the scope filtering; we do the regex matching in Python.
    rows = conn.execute(
        """
        SELECT id, pattern, category_id, source, account_id, priority, active
        FROM category_rules
        WHERE active = 1
          AND (source IS NULL OR source = ?)
          AND (account_id IS NULL OR account_id = ?)
        ORDER BY
            priority ASC,
            (source IS NULL) ASC,
            (account_id IS NULL) ASC,
            id ASC
        """,
        (request.source, request.account_id),
    ).fetchall()

    for row in rows:
        rule = _row_to_rule(row)
        if _compile(rule.pattern).search(description):
            assert rule.id is not None
            return RuleMatch(
                category_id=rule.category_id,
                rule_id=rule.id,
                pattern=rule.pattern,
            )
    return None


__all__ = [
    "CategorizationRequest",
    "CategoryRule",
    "RuleMatch",
    "load_rules",
    "suggest",
]
