"""The Binance ingest must stop re-creating the ``pay:`` twins (2026-09-03).

Binance reports one send twice: the *withdraw* history gave the backfill
``withdraw:hash:…`` and the *Pay* history gives the live sync
``pay:<orderId>``. Different reference, same money — so
``UNIQUE(source, source_ref)`` cannot see it, and the ledger counted
2,260.72 USDT twice across ten rows (see
``docs/plans/2026-09-03-ledger-actions-decisions.md`` §2).

Two independent defences, both tested here:

* **the tombstone** (ADR-022) — a deleted row's ``(source, source_ref)``
  is retired, so a deep re-sync cannot bring *those ten* back;
* **the twin guard** — a ``pay:`` event that matches an existing
  ``withdraw:`` row in amount, currency and Caracas calendar day is
  skipped, so the *next* one is never written in the first place.

Two things the live ledger settles about the shape of the guard:

* **Caracas days, not UTC days.** The legacy rows are dated at Caracas
  midnight (``04:00Z``) and the Pay rows carry real timestamps; five of
  the ten twins fall on the *next* UTC day (5775 at ``01:44Z`` against
  859's ``2025-11-05``). Comparing UTC dates would catch half of them.
* **Any Binance account, not only the same one.** Pay history always
  reports Spot; the backfill put five of the ten withdraw rows on
  Funding (1031, 1035, 943, 1076, 963). An account-equality guard would
  again catch half. The scope is the accounts the Binance sync owns.
"""

from __future__ import annotations

import sqlite3
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any
from unittest.mock import MagicMock

import pytest

from finances.config import CARACAS_TZ
from finances.db.repos import accounts as accounts_repo
from finances.db.repos import transactions as transactions_repo
from finances.domain.models import (
    Account,
    AccountKind,
    Transaction,
    TransactionKind,
)
from finances.ingest.binance import sync_binance

# A recent reference instant, chosen so the Pay timestamp lands on the day
# AFTER the withdraw row in UTC while staying the SAME day in Caracas —
# the 5775/859 shape.
_PAY_UTC = (datetime.now(tz=UTC) - timedelta(days=5)).replace(
    hour=1, minute=44, second=37, microsecond=0
)
_CARACAS_DAY = _PAY_UTC.astimezone(CARACAS_TZ).date()
# Caracas midnight, the way the backfill dates a legacy row.
_WITHDRAW_UTC = datetime(
    _CARACAS_DAY.year, _CARACAS_DAY.month, _CARACAS_DAY.day, tzinfo=CARACAS_TZ
).astimezone(UTC)


def _seed_accounts(conn: sqlite3.Connection) -> dict[str, int]:
    ids: dict[str, int] = {}
    for name, kind in (
        ("Binance Spot", AccountKind.CRYPTO_SPOT),
        ("Binance Funding", AccountKind.CRYPTO_FUNDING),
        ("Binance Earn", AccountKind.CRYPTO_EARN),
    ):
        account = accounts_repo.get_by_name(conn, name) or accounts_repo.insert(
            conn,
            Account(
                name=name, kind=kind, currency="USDT", institution="Binance"
            ),
        )
        assert account.id is not None
        ids[name] = account.id
    return ids


def _seed_withdraw(
    conn: sqlite3.Connection,
    *,
    account_id: int,
    amount: str = "-700",
    currency: str = "USDT",
    occurred_at: datetime | None = None,
    source_ref: str = "withdraw:hash:16c9d776f92b04bb",
) -> int:
    """The legacy leg — what the backfill wrote for the same send."""
    stored = transactions_repo.insert(
        conn,
        Transaction(
            account_id=account_id,
            occurred_at=occurred_at or _WITHDRAW_UTC,
            kind=TransactionKind.EXPENSE,
            amount=Decimal(amount),
            currency=currency,
            description="Binance send USDT — Cambio $700 efectivo Jorge",
            source="binance",
            source_ref=source_ref,
        ),
    )
    assert stored.id is not None
    return stored.id


def _sdk_with_pay(
    mocked_binance_sdk: MagicMock,
    *,
    amount: str = "-700",
    currency: str = "USDT",
    when: datetime | None = None,
    order_id: str = "396332446325137408",
) -> MagicMock:
    """A quiet SDK whose only content is one Pay event."""
    mocked_binance_sdk.time.return_value = {"serverTime": 1_700_000_000_000}
    mocked_binance_sdk.pay_history.return_value = {
        "data": [
            {
                "orderId": order_id,
                "orderType": "C2C",
                "amount": amount,
                "currency": currency,
                "transactionTime": int(
                    (when or _PAY_UTC).timestamp() * 1000
                ),
            }
        ]
    }
    return mocked_binance_sdk


def _sync(conn: sqlite3.Connection, client: MagicMock) -> dict[str, Any]:
    return sync_binance(
        conn,
        client=client,
        since=_WITHDRAW_UTC - timedelta(days=2),
    )


def _pay_rows(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    return conn.execute(
        "SELECT id, source_ref FROM transactions WHERE source_ref LIKE 'pay:%'"
    ).fetchall()


# ---------------------------------------------------------------------------
# The guard
# ---------------------------------------------------------------------------


def test_pay_twin_of_a_withdraw_row_is_skipped(
    in_memory_db: sqlite3.Connection, mocked_binance_sdk: MagicMock
) -> None:
    """The 5775/859 shape: same money, two references, one row kept."""
    ids = _seed_accounts(in_memory_db)
    _seed_withdraw(in_memory_db, account_id=ids["Binance Spot"])

    stats = _sync(in_memory_db, _sdk_with_pay(mocked_binance_sdk))

    assert _pay_rows(in_memory_db) == []
    assert stats["rows_skipped_pay_twin"] == 1
    assert stats["errors"] == []


def test_the_twin_is_found_on_another_binance_account(
    in_memory_db: sqlite3.Connection, mocked_binance_sdk: MagicMock
) -> None:
    """The 6135/1036 shape — Pay reports Spot, the legacy row is Funding."""
    ids = _seed_accounts(in_memory_db)
    _seed_withdraw(in_memory_db, account_id=ids["Binance Funding"])

    stats = _sync(in_memory_db, _sdk_with_pay(mocked_binance_sdk))

    assert _pay_rows(in_memory_db) == []
    assert stats["rows_skipped_pay_twin"] == 1


def test_a_pay_event_with_no_twin_is_ingested(
    in_memory_db: sqlite3.Connection, mocked_binance_sdk: MagicMock
) -> None:
    """The guard must not eat ordinary Binance Pay spending."""
    _seed_accounts(in_memory_db)

    stats = _sync(in_memory_db, _sdk_with_pay(mocked_binance_sdk))

    assert len(_pay_rows(in_memory_db)) == 1
    assert stats["rows_skipped_pay_twin"] == 0


def test_a_different_amount_is_not_a_twin(
    in_memory_db: sqlite3.Connection, mocked_binance_sdk: MagicMock
) -> None:
    ids = _seed_accounts(in_memory_db)
    _seed_withdraw(in_memory_db, account_id=ids["Binance Spot"], amount="-500")

    stats = _sync(in_memory_db, _sdk_with_pay(mocked_binance_sdk))

    assert len(_pay_rows(in_memory_db)) == 1
    assert stats["rows_skipped_pay_twin"] == 0


def test_a_different_currency_is_not_a_twin(
    in_memory_db: sqlite3.Connection, mocked_binance_sdk: MagicMock
) -> None:
    ids = _seed_accounts(in_memory_db)
    _seed_withdraw(
        in_memory_db, account_id=ids["Binance Spot"], currency="USDC"
    )

    stats = _sync(in_memory_db, _sdk_with_pay(mocked_binance_sdk))

    assert len(_pay_rows(in_memory_db)) == 1
    assert stats["rows_skipped_pay_twin"] == 0


def test_another_caracas_day_is_not_a_twin(
    in_memory_db: sqlite3.Connection, mocked_binance_sdk: MagicMock
) -> None:
    ids = _seed_accounts(in_memory_db)
    _seed_withdraw(
        in_memory_db,
        account_id=ids["Binance Spot"],
        occurred_at=_WITHDRAW_UTC - timedelta(days=1),
    )

    stats = _sync(in_memory_db, _sdk_with_pay(mocked_binance_sdk))

    assert len(_pay_rows(in_memory_db)) == 1
    assert stats["rows_skipped_pay_twin"] == 0


def test_the_utc_day_boundary_does_not_hide_the_twin(
    in_memory_db: sqlite3.Connection, mocked_binance_sdk: MagicMock
) -> None:
    """Explicit: the two rows are on different UTC days and still twins."""
    ids = _seed_accounts(in_memory_db)
    _seed_withdraw(in_memory_db, account_id=ids["Binance Spot"])

    assert _PAY_UTC.date() != _WITHDRAW_UTC.date()
    stats = _sync(in_memory_db, _sdk_with_pay(mocked_binance_sdk))

    assert stats["rows_skipped_pay_twin"] == 1


def test_a_pay_row_only_matches_a_withdraw_row(
    in_memory_db: sqlite3.Connection, mocked_binance_sdk: MagicMock
) -> None:
    """A P2P sell of the same size on the same day is a different event."""
    ids = _seed_accounts(in_memory_db)
    _seed_withdraw(
        in_memory_db,
        account_id=ids["Binance Spot"],
        source_ref="p2p:12345",
    )

    stats = _sync(in_memory_db, _sdk_with_pay(mocked_binance_sdk))

    assert len(_pay_rows(in_memory_db)) == 1
    assert stats["rows_skipped_pay_twin"] == 0


# ---------------------------------------------------------------------------
# The tombstone, through the live sync (ADR-022 §2.2)
# ---------------------------------------------------------------------------


def test_a_deleted_pay_row_is_not_resurrected_by_a_resync(
    in_memory_db: sqlite3.Connection, mocked_binance_sdk: MagicMock
) -> None:
    _seed_accounts(in_memory_db)
    client = _sdk_with_pay(mocked_binance_sdk)
    _sync(in_memory_db, client)
    row = _pay_rows(in_memory_db)[0]
    transactions_repo.delete(in_memory_db, row["id"], reason="twin of 859")

    stats = _sync(in_memory_db, client)

    assert _pay_rows(in_memory_db) == []
    assert stats["rows_inserted"] == 0
    assert stats["rows_skipped_deleted"] == 1


@pytest.mark.parametrize("key", ["rows_skipped_deleted", "rows_skipped_pay_twin"])
def test_the_counters_are_always_reported(
    in_memory_db: sqlite3.Connection, mocked_binance_sdk: MagicMock, key: str
) -> None:
    """A clean run reports zero rather than omitting the key."""
    _seed_accounts(in_memory_db)

    stats = _sync(in_memory_db, _sdk_with_pay(mocked_binance_sdk))

    assert stats[key] == 0
