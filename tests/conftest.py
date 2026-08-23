"""Shared pytest fixtures + factories for the finances test suite.

This module implements the EPIC-002b testing-infrastructure contract.

Conventions (see ADR-011 / rule-011):

* **Mock at the SDK/HTTP boundary, not at internal helpers.** When a test needs
  to control Binance behaviour, patch the SDK methods exposed on the
  ``mocked_binance_sdk`` fixture — never reach inside our ingest adapters. When
  a test needs to control an outbound HTTP call, use the ``mocked_http``
  fixture (``responses``) to register the URL/response pair rather than
  monkey-patching ``httpx``.
* **Use polyfactory factories for valid Pydantic models.** ``AccountFactory``,
  ``CategoryFactory``, ``TransactionFactory``, ``RateFactory`` and
  ``EarnPositionFactory`` each produce instances that pass the strict
  validators on the domain models. Tests that need a specific field value
  should override via ``Factory.build(field=...)`` instead of building a
  model literal by hand.
* **Fixture scopes.** All DB fixtures here are function-scoped (the default).
  The SQLite connections they expose are cheap to rebuild and keep each test
  hermetic. Do not promote them to module/session scope without a Wave-2
  discussion — cross-test mutation debugging is expensive.

Fixtures exposed (inventory):

* ``db_path`` — temp path for a file-backed sqlite DB (pre-existing).
* ``db_conn`` — file-backed sqlite3.Connection with migrations applied
  (pre-existing).
* ``in_memory_db`` — :memory: sqlite3.Connection with migrations applied.
* ``seeded_db`` — ``in_memory_db`` + a minimal v1 taxonomy + accounts seed.
* ``mocked_http`` — activated ``responses.RequestsMock`` for HTTP boundary
  mocking.
* ``mocked_binance_sdk`` — ``MagicMock`` shaped like ``binance.Client`` for
  SDK boundary mocking (Wave-2 ingest tests).
* ``AccountFactory`` / ``CategoryFactory`` / ``TransactionFactory`` /
  ``RateFactory`` / ``EarnPositionFactory`` — polyfactory factories for the
  domain models.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest
import responses
from polyfactory.factories.pydantic_factory import ModelFactory

from finances import config as _config
from finances.db.connection import get_connection
from finances.db.migrate import MIGRATIONS_DIR, apply_migrations
from finances.domain.models import (
    Account,
    AccountKind,
    Category,
    EarnPosition,
    Rate,
    Transaction,
    TransactionKind,
)


# ---------------------------------------------------------------------------
# Pre-existing DB fixtures (kept intact per EPIC-002b).
# ---------------------------------------------------------------------------


@pytest.fixture
def db_path(tmp_path: Path) -> Path:
    return tmp_path / "test.db"


@pytest.fixture
def db_conn(db_path: Path) -> Iterator[sqlite3.Connection]:
    conn = get_connection(db_path)
    apply_migrations(conn)
    try:
        yield conn
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# In-memory DB fixture (EPIC-002b).
# ---------------------------------------------------------------------------


def _open_in_memory_connection() -> sqlite3.Connection:
    """Open an in-memory sqlite3 connection shaped like ``get_connection``.

    We intentionally mirror ``finances.db.connection.get_connection`` instead of
    reusing it: ``get_connection`` insists on a filesystem path (it calls
    ``path.parent.mkdir``), and applying migrations against ``:memory:``
    requires keeping the *same* connection alive — a fresh file-path handle
    would see a different database.
    """
    # Import lazily so the Decimal/datetime adapters are registered on the
    # shared sqlite3 module; we need that side-effect before opening the
    # in-memory connection.
    from finances.db.connection import _register_decimal_adapters

    _register_decimal_adapters()
    conn = sqlite3.connect(
        ":memory:",
        detect_types=sqlite3.PARSE_DECLTYPES,
        isolation_level=None,
    )
    conn.row_factory = sqlite3.Row
    # WAL is a no-op for :memory: (it silently falls back to "memory"), but we
    # issue the same pragmas so behaviour matches the file-backed fixture as
    # closely as sqlite allows.
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA synchronous = NORMAL")
    return conn


@pytest.fixture
def in_memory_db() -> Iterator[sqlite3.Connection]:
    """Yield an in-memory sqlite3 connection with all migrations applied.

    Uses the same ``apply_migrations`` runner as the file-backed fixture so any
    future migration (``002_*.sql``, etc.) is automatically picked up. The
    connection is closed on teardown.
    """
    conn = _open_in_memory_connection()
    apply_migrations(conn, migrations_dir=MIGRATIONS_DIR)
    try:
        yield conn
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Seeded DB fixture (EPIC-002b — placeholder seed until EPIC-004 lands).
# ---------------------------------------------------------------------------


_V1_ACCOUNTS: tuple[tuple[str, AccountKind, str, str | None], ...] = (
    ("Provincial Bolivares", AccountKind.BANK, "VES", "Provincial"),
    ("Binance Spot", AccountKind.CRYPTO_SPOT, "USDT", "Binance"),
    ("Binance Funding", AccountKind.CRYPTO_FUNDING, "USDT", "Binance"),
    ("Binance Earn", AccountKind.CRYPTO_EARN, "USDT", "Binance"),
    ("Cash USD", AccountKind.CASH, "USD", None),
)


@pytest.fixture
def seeded_db(in_memory_db: sqlite3.Connection) -> Iterator[sqlite3.Connection]:
    """``in_memory_db`` + minimal v1 accounts.

    The v1 category taxonomy is seeded by migration ``002_seed_categories.sql``
    (EPIC-004) and is therefore already present in ``in_memory_db``; this
    fixture only adds the accounts test fixtures rely on.
    """
    from finances.db.repos import accounts as accounts_repo

    for name, kind, currency, institution in _V1_ACCOUNTS:
        accounts_repo.insert(
            in_memory_db,
            Account(name=name, kind=kind, currency=currency, institution=institution),
        )

    yield in_memory_db


# ---------------------------------------------------------------------------
# HTTP + SDK boundary mocks (EPIC-002b).
# ---------------------------------------------------------------------------


@pytest.fixture
def mocked_http() -> Iterator[responses.RequestsMock]:
    """Activated ``responses.RequestsMock`` for BCV + P2P ingest tests.

    Register URLs on the yielded object with ``rsps.add(...)``. ``assert_all_requests_are_fired``
    stays at its default (``True``) — if a registered URL is never hit the
    test fails, which forces tests to declare only the requests they actually
    exercise.
    """
    with responses.RequestsMock() as rsps:
        yield rsps


@pytest.fixture
def mocked_binance_sdk() -> MagicMock:
    """``MagicMock`` spec'd against the real ``binance.spot.Spot`` client.

    ``spec=Spot`` means configuring or calling a method that does not exist
    on the installed SDK raises ``AttributeError`` immediately — this is what
    catches SDK method renames that a bare ``MagicMock`` silently absorbs
    (regression: ``simple_earn_flexible_rewards_history`` never existed on
    binance-connector 3.12; the real name is ``get_flexible_rewards_history``).
    Tests patch return values via
    ``mocked_binance_sdk.my_trades.return_value = [...]``. Returning empty
    lists by default keeps "no-op" tests honest — the SDK was consulted,
    found nothing, and the ingester is free to short-circuit.

    Per rule-011: **mock at the SDK boundary, not at our ingest helpers.**
    Tests should inject this mock at the adapter seam (e.g. by passing it to
    the ingest function or by patching ``binance.Client`` at module level),
    not at internal parsing utilities.
    """
    from binance.spot import Spot

    client = MagicMock(name="binance.Spot", spec=Spot)
    # Spot / funding snapshots.
    client.account.return_value = {"balances": []}
    client.funding_wallet.return_value = []
    # Trade history endpoints.
    client.my_trades.return_value = []
    # C2C / P2P fills.
    client.c2c_trade_history.return_value = {"data": [], "total": 0}
    # Simple Earn (flexible + locked).
    client.get_flexible_product_position.return_value = {"rows": [], "total": 0}
    client.get_locked_product_position.return_value = {"rows": [], "total": 0}
    client.get_flexible_rewards_history.return_value = {"rows": [], "total": 0}
    client.get_flexible_subscription_record.return_value = {"rows": [], "total": 0}
    client.get_flexible_redemption_record.return_value = {"rows": [], "total": 0}
    # Deposits / withdrawals.
    client.deposit_history.return_value = []
    client.withdraw_history.return_value = []
    return client


# ---------------------------------------------------------------------------
# Polyfactory factories (EPIC-002b).
# ---------------------------------------------------------------------------
#
# Each factory produces a valid instance of its model — i.e. one that survives
# the strict validators in `finances/domain/models.py`. Notably:
#
# * `currency`, `base`, `quote`, `asset` fields are upper-cased by the
#   model validators; we supply already-upper strings to avoid drift from
#   polyfactory's Faker seed.
# * `amount`, `principal`, `apy`, `rate` must be `Decimal` (not `float`).
# * `occurred_at`, `started_at`, etc. must be timezone-aware.


def _aware_now() -> datetime:
    return datetime.now(tz=UTC)


class AccountFactory(ModelFactory[Account]):
    __model__ = Account
    __allow_none_optionals__ = 0.0

    id = None
    created_at = None
    updated_at = None
    kind = AccountKind.BANK
    currency = "USD"
    active = True

    @classmethod
    def name(cls) -> str:
        return cls.__faker__.unique.company()[:64]


class CategoryFactory(ModelFactory[Category]):
    __model__ = Category
    __allow_none_optionals__ = 0.0

    id = None
    created_at = None
    kind = TransactionKind.EXPENSE
    active = True

    @classmethod
    def name(cls) -> str:
        return cls.__faker__.unique.word().title()[:64]


class TransactionFactory(ModelFactory[Transaction]):
    __model__ = Transaction
    __allow_none_optionals__ = 0.0

    id = None
    created_at = None
    updated_at = None
    account_id = 1
    category_id = None
    transfer_id = None
    user_rate = None
    description = None
    needs_review = False
    parked = False
    notes = None
    kind = TransactionKind.EXPENSE
    currency = "USD"
    source = "test"

    @classmethod
    def amount(cls) -> Decimal:
        # Two decimal places keeps the JSON round-trip stable.
        return Decimal(str(round(cls.__faker__.pyfloat(left_digits=4, right_digits=2), 2)))

    @classmethod
    def occurred_at(cls) -> datetime:
        return _aware_now()

    @classmethod
    def source_ref(cls) -> str:
        return cls.__faker__.unique.uuid4()


class RateFactory(ModelFactory[Rate]):
    __model__ = Rate
    __allow_none_optionals__ = 0.0

    id = None
    created_at = None
    base = "USD"
    quote = "VES"
    source = "bcv"

    @classmethod
    def rate(cls) -> Decimal:
        return Decimal(str(round(cls.__faker__.pyfloat(left_digits=3, right_digits=4, positive=True), 4)))

    @classmethod
    def as_of_date(cls) -> date:
        return cls.__faker__.date_object()


class EarnPositionFactory(ModelFactory[EarnPosition]):
    __model__ = EarnPosition
    __allow_none_optionals__ = 0.0

    id = None
    ended_at = None
    snapshot_at = None
    account_id = 1
    asset = "USDT"

    @classmethod
    def product_id(cls) -> str:
        return cls.__faker__.unique.bothify(text="PROD-####")

    @classmethod
    def principal(cls) -> Decimal:
        return Decimal(str(round(cls.__faker__.pyfloat(left_digits=4, right_digits=2, positive=True), 2)))

    @classmethod
    def apy(cls) -> Decimal:
        return Decimal(str(round(cls.__faker__.pyfloat(left_digits=1, right_digits=4, positive=True), 4)))

    @classmethod
    def started_at(cls) -> datetime:
        return _aware_now()


# Expose the factories as fixtures too, so tests that prefer DI over direct
# imports can receive them via the standard pytest injection path.


@pytest.fixture
def account_factory() -> type[AccountFactory]:
    return AccountFactory


@pytest.fixture
def category_factory() -> type[CategoryFactory]:
    return CategoryFactory


@pytest.fixture
def transaction_factory() -> type[TransactionFactory]:
    return TransactionFactory


@pytest.fixture
def rate_factory() -> type[RateFactory]:
    return RateFactory


@pytest.fixture
def earn_position_factory() -> type[EarnPositionFactory]:
    return EarnPositionFactory


__all__ = [
    "AccountFactory",
    "CategoryFactory",
    "EarnPositionFactory",
    "RateFactory",
    "TransactionFactory",
    "account_factory",
    "category_factory",
    "db_conn",
    "db_path",
    "earn_position_factory",
    "in_memory_db",
    "mocked_binance_sdk",
    "mocked_http",
    "rate_factory",
    "seeded_db",
    "transaction_factory",
]


# Silence unused-import warnings for symbols consumed only via __all__.
_ = (Any,)


# ---------------------------------------------------------------------------
# Live-DB guard.
#
# The suite must never open the real ``finances.config.DB_PATH``. A bare
# ``sqlite3.connect`` on a missing path CREATES it (a 4096-byte stub in a fresh
# worktree) and the WAL pragmas rewrite the header of an existing one, so a
# single stray connect silently mutates Julio's ledger file. We wrap
# ``sqlite3.connect`` for the whole session and fail loudly instead.
#
# Installed from ``pytest_configure`` so it also covers module-import side
# effects during collection.
# ---------------------------------------------------------------------------

_REAL_SQLITE_CONNECT = sqlite3.connect

# Resolved once, at conftest import, from the package layout — NOT read back
# from ``config.DB_PATH`` at call time. Plenty of CLI tests legitimately
# monkeypatch ``config.DB_PATH`` to a tmp file; the thing that must stay
# untouched is the real ledger next to the ``finances`` package.
_LIVE_DB_PATH = (_config.PROJECT_ROOT / "finances.db").resolve()


def _connect_target_path(database: object, uri: bool) -> Path | None:
    """Best-effort filesystem path for a ``sqlite3.connect`` first argument."""
    if isinstance(database, Path):
        return database
    if not isinstance(database, str):
        return None  # bytes/int handles: not something we can compare
    text = database
    if text == ":memory:" or not text:
        return None
    if uri or text.startswith("file:"):
        from urllib.parse import unquote, urlsplit

        split = urlsplit(text)
        text = unquote(split.path)
        if not text:
            return None
    return Path(text)


def _guarded_sqlite_connect(database: Any = ":memory:", *args: Any, **kwargs: Any):
    target = _connect_target_path(database, bool(kwargs.get("uri", False)))
    if target is not None:
        try:
            same = target.expanduser().resolve() == _LIVE_DB_PATH
        except OSError:  # pragma: no cover - unresolvable path is not the live DB
            same = False
        if same:
            raise AssertionError(
                "test opened the live database at "
                f"{_LIVE_DB_PATH} (via sqlite3.connect({database!r})). "
                "Tests must use tmp_path / the db_path fixture / "
                "FINANCES_WEB_DB_PATH — never the real finances.db."
            )
    return _REAL_SQLITE_CONNECT(database, *args, **kwargs)


def pytest_configure(config: pytest.Config) -> None:  # noqa: ARG001
    sqlite3.connect = _guarded_sqlite_connect


def pytest_unconfigure(config: pytest.Config) -> None:  # noqa: ARG001
    sqlite3.connect = _REAL_SQLITE_CONNECT


def _live_db_fingerprint() -> tuple[bool, int, int] | None:
    """(exists, size, mtime_ns) for the real ledger — cheap and total."""
    try:
        stat = _LIVE_DB_PATH.stat()
    except FileNotFoundError:
        return (False, 0, 0)
    return (True, stat.st_size, stat.st_mtime_ns)


@pytest.fixture(scope="session", autouse=True)
def _live_db_is_never_touched() -> Iterator[None]:
    """Second half of the live-DB guard, for what the wrapper cannot see.

    ``_guarded_sqlite_connect`` only covers *this* interpreter. Tests that
    shell out — ``finances serve`` under its reload supervisor is the one
    that did — open the ledger in a child process where nothing is patched.
    So we also fingerprint the file around the whole session: a WAL header
    rewrite bumps mtime even when no row changes, and on a fresh worktree
    the connect materialises a 4096-byte stub from nothing.
    """
    before = _live_db_fingerprint()
    yield
    after = _live_db_fingerprint()
    assert after == before, (
        f"the live database at {_LIVE_DB_PATH} changed during the test "
        f"session ({before} -> {after}). Something opened it — most likely a "
        "test that shells out to a `finances` CLI command without pointing "
        "it at a tmp DB. (If you were running `finances serve` in another "
        "window against this same checkout, that is the other explanation.)"
    )
