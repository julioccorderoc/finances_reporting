"""Shared fixtures for the web test suite (EPIC-023, Phase 2).

Phase 2b ships this conftest so the rest of Phase 2 (Agents 2a, 2c, 2d)
can reuse the ``web_client_factory``, ``seeded_web_db`` and the
``TransactionCard`` stub helpers without duplicating boilerplate.

Conventions:

* Tests build a fresh app per test via ``web_client_factory(conn)``; the
  factory points the app's ``WebSettings.db_path`` at a temp file backed by
  ``conn`` (we copy schema + rows over so tests stay hermetic).
* All seed writes go through the existing repos (rule-009 / rule-012).
* No global mutation: every test gets a clean DB.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Callable, Iterator
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from finances.db.connection import _register_decimal_adapters
from finances.db.migrate import apply_migrations
from finances.db.repos import accounts as accounts_repo
from finances.db.repos import categories as categories_repo
from finances.db.repos import rates as rates_repo
from finances.db.repos import transactions as transactions_repo
from finances.domain.models import (
    Account,
    AccountKind,
    Rate,
    Transaction,
    TransactionKind,
)
from jinja2 import StrictUndefined

from finances.web.app import create_app
from finances.web.settings import WebSettings


# ---------------------------------------------------------------------------
# DB factory — file-backed, per-test, points the web app at a real path.
# ---------------------------------------------------------------------------


def _open_file_conn(path: Path) -> sqlite3.Connection:
    _register_decimal_adapters()
    conn = sqlite3.connect(
        str(path),
        detect_types=sqlite3.PARSE_DECLTYPES,
        isolation_level=None,
    )
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


@pytest.fixture
def web_db_path(tmp_path: Path) -> Path:
    return tmp_path / "web.db"


@pytest.fixture
def web_db(web_db_path: Path) -> Iterator[sqlite3.Connection]:
    """File-backed sqlite connection with migrations applied."""
    conn = _open_file_conn(web_db_path)
    apply_migrations(conn)
    try:
        yield conn
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# App / TestClient factory.
# ---------------------------------------------------------------------------


@pytest.fixture
def web_client_factory(
    web_db_path: Path,
) -> Callable[[], TestClient]:
    """Factory that builds a TestClient pointed at ``web_db_path``."""

    def _make() -> TestClient:
        app = create_app(WebSettings(host="127.0.0.1", db_path=web_db_path))
        # Strict in tests, lenient in production (jinja's default Undefined
        # renders empty for {{ x }} and only raises on operations). One line
        # turns every web test in this suite into a missing-context check —
        # the runtime deliberately stays lenient, because an em dash where a
        # number belongs is worse in a ledger than a loud failure in CI.
        app.state.templates.env.undefined = StrictUndefined
        return TestClient(app)

    return _make


# ---------------------------------------------------------------------------
# Seeded fixtures for Phase 2b transactions tests.
# ---------------------------------------------------------------------------


def _aware(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=UTC)
    return dt


@pytest.fixture
def seeded_web_db(web_db: sqlite3.Connection) -> sqlite3.Connection:
    """Seed a deterministic dataset that exercises every rate path.

    Rows added (so tests can pin behaviour without re-seeding):

    * Provincial Bolivares (VES) — 4 expenses, 1 income, mix of dates.
    * Cash USD (USD) — 1 expense (native_usd path).
    * Binance Spot (USDT) — 1 income (native_usd path).
    * One Provincial expense with ``user_rate`` set explicitly.
    * One Provincial expense with NO matching rate (forces needs_review).
    * One BCV rate row + one binance_p2p_median rate row, dated ~today.

    **Expense amounts are negative, as production stores them.** They were
    positive here for a long time, and the fixture consequently held no
    negative amount at all. That hid a live defect: the dashboard's top-5
    spend chart ranked signed totals with ``reverse=True``, which is correct
    for positive inputs and returns the five *smallest* categories for real
    ones — the front-page chart was built out of Fees at $3.26 while
    Purchases at $1,103 sat in "Other". It also meant no web test ever
    exercised ``amount < 0``, the predicate behind the realized-basis
    rebuild hook and the pairing sign check.
    """
    today = datetime.now(tz=UTC)
    one_week = today - timedelta(days=7)
    two_weeks = today - timedelta(days=14)

    # --- accounts ----------------------------------------------------------
    provincial = accounts_repo.insert(
        web_db,
        Account(
            name="Provincial",
            kind=AccountKind.BANK,
            currency="VES",
            institution="Provincial",
        ),
    )
    cash = accounts_repo.insert(
        web_db,
        Account(name="Cash USD", kind=AccountKind.CASH, currency="USD"),
    )
    binance = accounts_repo.insert(
        web_db,
        Account(
            name="Binance Spot",
            kind=AccountKind.CRYPTO_SPOT,
            currency="USDT",
            institution="Binance",
        ),
    )

    # --- categories (already seeded by migration 002) ----------------------
    food = categories_repo.get_by_name(web_db, TransactionKind.EXPENSE, "Groceries")
    salary = categories_repo.get_by_name(web_db, TransactionKind.INCOME, "Salary")
    assert food is not None and salary is not None

    # --- rates: P2P median for ~today, BCV for ~today -----------------------
    rates_repo.upsert(
        web_db,
        Rate(
            as_of_date=date.today(),
            base="USDT",
            quote="VES",
            rate=Decimal("36.50"),
            source="binance_p2p_median",
        ),
    )
    rates_repo.upsert(
        web_db,
        Rate(
            as_of_date=date.today(),
            base="USD",
            quote="VES",
            rate=Decimal("36.10"),
            source="bcv",
        ),
    )

    # --- transactions ------------------------------------------------------
    rows = [
        # Provincial expense — should resolve via P2P median (today).
        Transaction(
            account_id=provincial.id,
            occurred_at=_aware(today),
            kind=TransactionKind.EXPENSE,
            amount=Decimal("-365.00"),
            currency="VES",
            description="COM.PAGO bodega",
            category_id=food.id,
            source="provincial",
            source_ref="prov-1",
        ),
        # Provincial expense — recent.
        Transaction(
            account_id=provincial.id,
            occurred_at=_aware(one_week),
            kind=TransactionKind.EXPENSE,
            amount=Decimal("-100.00"),
            currency="VES",
            description="COM.PAGO farmacia",
            category_id=food.id,
            source="provincial",
            source_ref="prov-2",
        ),
        # Provincial expense — within range, with explicit user_rate.
        Transaction(
            account_id=provincial.id,
            occurred_at=_aware(two_weeks),
            kind=TransactionKind.EXPENSE,
            amount=Decimal("-3650.00"),
            currency="VES",
            description="COM.PAGO grocery",
            category_id=food.id,
            user_rate=Decimal("36.0"),
            source="provincial",
            source_ref="prov-3",
        ),
        # Provincial income — within range.
        Transaction(
            account_id=provincial.id,
            occurred_at=_aware(one_week),
            kind=TransactionKind.INCOME,
            amount=Decimal("36500.00"),
            currency="VES",
            description="ABONO nomina",
            category_id=salary.id,
            source="provincial",
            source_ref="prov-4",
        ),
        # Provincial expense, dated FAR in the past so no rate applies and
        # the resolver returns needs_review.
        Transaction(
            account_id=provincial.id,
            occurred_at=_aware(datetime(2010, 1, 1, tzinfo=UTC)),
            kind=TransactionKind.EXPENSE,
            amount=Decimal("-999.00"),
            currency="VES",
            description="LEGACY needs review",
            source="provincial",
            source_ref="prov-needs-review",
            needs_review=True,
        ),
        # Cash USD expense — native_usd.
        Transaction(
            account_id=cash.id,
            occurred_at=_aware(today),
            kind=TransactionKind.EXPENSE,
            amount=Decimal("-12.50"),
            currency="USD",
            description="lunch",
            source="cash_cli",
            source_ref="cash-1",
        ),
        # Binance Spot income — native_usd.
        Transaction(
            account_id=binance.id,
            occurred_at=_aware(one_week),
            kind=TransactionKind.INCOME,
            amount=Decimal("100.00"),
            currency="USDT",
            description="Earn payout",
            source="binance",
            source_ref="bin-1",
        ),
    ]
    for txn in rows:
        transactions_repo.insert(web_db, txn)

    return web_db


# Re-export the seeded date helpers so tests can build precise filter ranges.


@pytest.fixture
def today_date() -> date:
    return datetime.now(tz=UTC).date()
