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


# ---------------------------------------------------------------------------
# Triage redesign (Wave 2) — a ledger that carries every state the surface
# has to render. The queue screen and the modal run are asserted against
# this rather than against live data: the live ledger's deepest rate carry
# is six days, so *Priced roughly* is empty there and the automatic pair
# matcher only proposes inside ±1 day / ±2%, so no live proposal is ever
# refusable (design_handoff_triage/NOTES.md, Wave 1.1).
# ---------------------------------------------------------------------------


@pytest.fixture
def triage_web_db(web_db: sqlite3.Connection) -> sqlite3.Connection:
    """Every triage state at once, on fixed dates.

    * ``cat-only`` — uncategorised, priced in-window (bucket 0).
    * ``both`` — uncategorised AND priced from a nearest rate, so it is
      one item with two badges in bucket 0 (criterion A2).
    * ``rough`` — categorised, priced approximately (bucket 2).
    * ``parked`` — uncategorised and out of the queue.
    * a rule-backed guess (``traki`` → Purchases) and a learned one (the
      same bank string filed three times).
    * a same-day, exact-amount deposit + sell, which the automatic
      matcher proposes as a pair (bucket 1).
    * a second deposit + sell seven days apart, which the matcher will
      not propose (±1 day) and the manual pair path refuses outright,
      so the modal's danger banner has something real to render (H3).
    """
    bank = accounts_repo.insert(
        web_db,
        Account(
            name="Provincial",
            kind=AccountKind.BANK,
            currency="VES",
            institution="Provincial",
        ),
    )
    funding = accounts_repo.insert(
        web_db,
        Account(
            name="Binance Funding",
            kind=AccountKind.CRYPTO_FUNDING,
            currency="USDT",
            institution="Binance",
        ),
    )
    groceries = categories_repo.get_by_name(
        web_db, TransactionKind.EXPENSE, "Groceries"
    )
    assert groceries is not None

    # One rate island, far from every transaction below except the first
    # two, so the nearest-rate branch (ADR-021) is what prices the rest.
    for as_of, source, base, rate in (
        (date(2026, 6, 27), "binance_p2p_median", "USDT", Decimal("155.00")),
        (date(2026, 7, 1), "binance_p2p_median", "USDT", Decimal("160.00")),
        (date(2026, 7, 1), "bcv", "USD", Decimal("144.60")),
    ):
        rates_repo.upsert(
            web_db,
            Rate(
                as_of_date=as_of,
                base=base,
                quote="VES",
                rate=rate,
                source=source,
            ),
        )

    def _bank_row(**kwargs) -> Transaction:
        base = {
            "account_id": bank.id,
            "kind": TransactionKind.EXPENSE,
            "currency": "VES",
            "source": "provincial",
        }
        return Transaction(**{**base, **kwargs})

    transactions_repo.insert(
        web_db,
        _bank_row(
            occurred_at=datetime(2026, 7, 3, tzinfo=UTC),
            amount=Decimal("-16000.00"),
            description="LUNCHERIA MILY GOURMET",
            source_ref="triage-cat-only",
        ),
    )
    transactions_repo.insert(
        web_db,
        _bank_row(
            occurred_at=datetime(2026, 3, 2, tzinfo=UTC),
            amount=Decimal("-24000.00"),
            description="COMPRA POS 3311 TRAKI",
            source_ref="triage-both",
        ),
    )
    transactions_repo.insert(
        web_db,
        _bank_row(
            occurred_at=datetime(2026, 3, 4, tzinfo=UTC),
            amount=Decimal("-8000.00"),
            description="CAR.DRV0013196230",
            category_id=groceries.id,
            source_ref="triage-rough",
        ),
    )
    # History behind the learned guess: the same bank string, filed the
    # same way three times, is what makes the queue offer it (G7).
    for n in range(3):
        transactions_repo.insert(
            web_db,
            _bank_row(
                occurred_at=datetime(2026, 6, 28 + n, tzinfo=UTC),
                amount=Decimal("-12000.00"),
                description="LUNCHERIA MILY GOURMET",
                category_id=groceries.id,
                source_ref=f"triage-history-{n}",
            ),
        )

    # A rule the engine will match, so the other guess cites a regex.
    purchases = categories_repo.get_by_name(
        web_db, TransactionKind.EXPENSE, "Purchases"
    )
    assert purchases is not None
    web_db.execute(
        "INSERT INTO category_rules (pattern, category_id, source, priority) "
        "VALUES (?, ?, ?, ?)",
        ("traki", purchases.id, "provincial", 10),
    )

    transactions_repo.insert(
        web_db,
        _bank_row(
            occurred_at=datetime(2024, 11, 3, tzinfo=UTC),
            amount=Decimal("-6400.00"),
            description="PAGO MOVIL 04141234567",
            source_ref="triage-parked",
            parked=True,
        ),
    )

    # The proposable pair: same day, and the sell's own rate values it at
    # exactly the deposit, so BankAnchoredP2pPairing offers it.
    transactions_repo.insert(
        web_db,
        _bank_row(
            occurred_at=datetime(2026, 7, 2, tzinfo=UTC),
            kind=TransactionKind.INCOME,
            amount=Decimal("32000.00"),
            description="ABONO P2P",
            source_ref="triage-pair-deposit",
        ),
    )
    transactions_repo.insert(
        web_db,
        Transaction(
            account_id=funding.id,
            occurred_at=datetime(2026, 7, 2, tzinfo=UTC),
            kind=TransactionKind.EXPENSE,
            amount=Decimal("-200.00"),
            currency="USDT",
            description="P2P sell",
            user_rate=Decimal("160.00"),
            source="binance",
            source_ref="triage-pair-sell",
        ),
    )

    # The refusable pair: seven days apart, so the matcher never proposes
    # it (its window is one day) and confirm_pair raises on it. Reachable
    # only through the pair modal's own URL — which is exactly where the
    # design's disabled button and danger banner live.
    transactions_repo.insert(
        web_db,
        _bank_row(
            occurred_at=datetime(2026, 1, 10, tzinfo=UTC),
            kind=TransactionKind.INCOME,
            amount=Decimal("50000.00"),
            description="ABONO P2P LEGACY",
            source_ref="triage-refused-deposit",
        ),
    )
    transactions_repo.insert(
        web_db,
        Transaction(
            account_id=funding.id,
            occurred_at=datetime(2026, 1, 17, tzinfo=UTC),
            kind=TransactionKind.EXPENSE,
            amount=Decimal("-200.00"),
            currency="USDT",
            description="P2P sell legacy",
            user_rate=Decimal("230.00"),
            source="binance",
            source_ref="triage-refused-sell",
        ),
    )

    return web_db


# Re-export the seeded date helpers so tests can build precise filter ranges.


@pytest.fixture
def today_date() -> date:
    return datetime.now(tz=UTC).date()
