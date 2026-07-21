"""WP4 — category usage stats service (tests precede impl per rule-011).

``top_categories`` ranks active categories by usage count over a trailing
window of calendar months, and pads with seed (id) order when history is
thin. Uses the tmp-DB web fixtures — never the real finances.db.

Seed helper note: real expense amounts are NEGATIVE (project sign
convention). Do not copy the positive-amount habit of the seeded_web_db
fixture.
"""

from __future__ import annotations

import sqlite3
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import uuid4

from finances.db.repos import accounts as accounts_repo
from finances.db.repos import categories as categories_repo
from finances.db.repos import transactions as transactions_repo
from finances.domain.models import (
    Account,
    AccountKind,
    Category,
    Transaction,
    TransactionKind,
)
from finances.web.services.category_stats import top_categories


def _seed_account(conn: sqlite3.Connection) -> int:
    acct = accounts_repo.insert(
        conn,
        Account(
            name="Provincial",
            kind=AccountKind.BANK,
            currency="VES",
            institution="Provincial",
        ),
    )
    assert acct.id is not None
    return acct.id


def _cat(conn: sqlite3.Connection, kind: TransactionKind, name: str) -> Category:
    cat = categories_repo.get_by_name(conn, kind, name)
    assert cat is not None, f"seed category {name} missing"
    return cat


def _use(
    conn: sqlite3.Connection,
    account_id: int,
    category_id: int | None,
    *,
    when: datetime,
    times: int = 1,
    kind: TransactionKind = TransactionKind.EXPENSE,
) -> None:
    """Insert ``times`` transactions in ``category_id`` dated ``when``."""
    amount = Decimal("-25.00") if kind == TransactionKind.EXPENSE else Decimal("25.00")
    for _ in range(times):
        transactions_repo.insert(
            conn,
            Transaction(
                account_id=account_id,
                occurred_at=when,
                kind=kind,
                amount=amount,
                currency="VES",
                description="category-stats seed",
                category_id=category_id,
                source="test",
                source_ref=f"cs-{uuid4()}",
            ),
        )


def test_orders_by_usage_count_desc(web_db: sqlite3.Connection) -> None:
    acct = _seed_account(web_db)
    yesterday = datetime.now(tz=UTC) - timedelta(days=1)
    groceries = _cat(web_db, TransactionKind.EXPENSE, "Groceries")
    transport = _cat(web_db, TransactionKind.EXPENSE, "Transport")
    health = _cat(web_db, TransactionKind.EXPENSE, "Health")
    _use(web_db, acct, groceries.id, when=yesterday, times=3)
    _use(web_db, acct, transport.id, when=yesterday, times=2)
    _use(web_db, acct, health.id, when=yesterday, times=1)

    result = top_categories(web_db, kind=TransactionKind.EXPENSE, limit=3)

    assert [c.name for c in result] == ["Groceries", "Transport", "Health"]


def test_usage_outside_window_does_not_rank(web_db: sqlite3.Connection) -> None:
    acct = _seed_account(web_db)
    two_years_ago = datetime.now(tz=UTC) - timedelta(days=730)
    yesterday = datetime.now(tz=UTC) - timedelta(days=1)
    dating = _cat(web_db, TransactionKind.EXPENSE, "Dating")
    groceries = _cat(web_db, TransactionKind.EXPENSE, "Groceries")
    _use(web_db, acct, dating.id, when=two_years_ago, times=10)
    _use(web_db, acct, groceries.id, when=yesterday, times=1)

    result = top_categories(web_db, kind=TransactionKind.EXPENSE, months=12, limit=2)

    names = [c.name for c in result]
    # Groceries (1 in-window use) ranks first; Dating's 10 uses are outside the
    # window, so it earns no ranking power and can only reappear through the
    # seed-order pad, behind every lower-id category (Transport is id 6, Dating
    # id 15). `limit` is pinned here so the assertion tests that ordering and
    # not how many categories happen to be active (migration 013 deactivated
    # two, which shortened the pad).
    assert names[0] == "Groceries"
    assert "Dating" not in names


def test_kind_filter_limits_to_that_kind(web_db: sqlite3.Connection) -> None:
    acct = _seed_account(web_db)
    yesterday = datetime.now(tz=UTC) - timedelta(days=1)
    salary = _cat(web_db, TransactionKind.INCOME, "Salary")
    groceries = _cat(web_db, TransactionKind.EXPENSE, "Groceries")
    _use(web_db, acct, salary.id, when=yesterday, times=5, kind=TransactionKind.INCOME)
    _use(web_db, acct, groceries.id, when=yesterday, times=1)

    result = top_categories(web_db, kind=TransactionKind.EXPENSE)

    assert result
    assert all(c.kind == TransactionKind.EXPENSE for c in result)
    assert "Salary" not in [c.name for c in result]


def test_kind_accepts_plain_string(web_db: sqlite3.Connection) -> None:
    result = top_categories(web_db, kind="income", limit=4)

    assert len(result) == 4
    assert all(c.kind == TransactionKind.INCOME for c in result)


def test_default_limit_is_8(web_db: sqlite3.Connection) -> None:
    # 18 active expense categories exist from the seed migrations (002-005).
    result = top_categories(web_db, kind=TransactionKind.EXPENSE)

    assert len(result) == 8


def test_thin_history_falls_back_to_seed_order(web_db: sqlite3.Connection) -> None:
    # No transactions at all → first 8 active categories in id (seed) order.
    result = top_categories(web_db, limit=8)

    assert len(result) == 8
    expected = [
        int(r["id"])
        for r in web_db.execute(
            "SELECT id FROM categories WHERE active = 1 ORDER BY id ASC LIMIT 8"
        ).fetchall()
    ]
    assert [c.id for c in result] == expected


def test_inactive_categories_never_returned(web_db: sqlite3.Connection) -> None:
    acct = _seed_account(web_db)
    zombie = categories_repo.insert(
        web_db,
        Category(kind=TransactionKind.EXPENSE, name="Zombie", active=False),
    )
    _use(
        web_db,
        acct,
        zombie.id,
        when=datetime.now(tz=UTC) - timedelta(days=1),
        times=5,
    )

    result = top_categories(web_db, kind=TransactionKind.EXPENSE)

    assert "Zombie" not in [c.name for c in result]


def test_returns_pydantic_category_models(web_db: sqlite3.Connection) -> None:
    result = top_categories(web_db, limit=3)

    assert result
    assert all(isinstance(c, Category) for c in result)
