"""Moving money into Binance Earn is a transfer, and must be recorded.

ADR-003 decided this in April: "Subscriptions and redemptions are double-entry
transfers per ADR-002", and it explicitly rejected the alternative — "just log
rewards as income, ignore principal" — because that "loses the
investment-tracking goal".

The rejected option is what shipped. ``sync_binance`` calls
``get_flexible_product_position`` (a snapshot of principal) and
``get_flexible_rewards_history`` (interest), but never
``get_flexible_subscription_record`` or ``get_flexible_redemption_record``.
So the Earn account holds nothing but interest rows, and the principal that
earned it never appears to have left Spot.

On the live ledger that is 10,526 USDT and 6,378 USDC of subscriptions, and
9,793 USDT of redemptions, that no report has ever seen — and it is why
Binance Spot shows an impossible negative USDT balance.

Both record shapes name their counterpart account (``sourceAccount`` on a
subscription, ``destAccount`` on a redemption), so each leg lands where the
money actually went rather than on an assumed default.
"""

from __future__ import annotations

import sqlite3
from unittest.mock import MagicMock

import pytest

from finances.db.repos import accounts as accounts_repo
from finances.domain.models import Account, AccountKind
from finances.ingest.binance import sync_binance

# 2026-03-19T12:00:00Z and 2026-03-20T12:00:00Z, inside the default lookback
# window the other ingest tests use.
SUBSCRIBE_MS = 1_774_008_000_000
REDEEM_MS = 1_774_094_400_000


@pytest.fixture
def binance_accounts(in_memory_db: sqlite3.Connection) -> dict[str, int]:
    ids: dict[str, int] = {}
    for name, kind in (
        ("Binance Spot", AccountKind.CRYPTO_SPOT),
        ("Binance Funding", AccountKind.CRYPTO_FUNDING),
        ("Binance Earn", AccountKind.CRYPTO_EARN),
    ):
        account = accounts_repo.insert(
            in_memory_db,
            Account(name=name, kind=kind, currency="USDT", institution="Binance"),
        )
        assert account.id is not None
        ids[name] = account.id
    return ids


def _legs(conn: sqlite3.Connection, prefix: str) -> list[sqlite3.Row]:
    return conn.execute(
        "SELECT id, account_id, kind, amount, currency, transfer_id, source_ref "
        "FROM transactions WHERE source_ref LIKE ? ORDER BY id",
        (f"{prefix}%",),
    ).fetchall()


def _sync(conn: sqlite3.Connection, client: MagicMock) -> dict:
    # A wide lookback so the fixed timestamps above always fall inside the
    # window regardless of when the suite runs.
    return sync_binance(conn, client=client, lookback_days=4000)


def test_a_subscription_moves_money_from_spot_into_earn(
    in_memory_db: sqlite3.Connection,
    mocked_binance_sdk: MagicMock,
    binance_accounts: dict[str, int],
) -> None:
    mocked_binance_sdk.get_flexible_subscription_record.return_value = {
        "rows": [
            {
                "amount": "750",
                "asset": "USDT",
                "time": SUBSCRIBE_MS,
                "status": "SUCCESS",
                "purchaseId": 12383413269,
                "type": "NORMAL",
                "sourceAccount": "SPOT",
                "productId": "USDT001",
            }
        ],
        "total": 1,
    }

    _sync(in_memory_db, mocked_binance_sdk)

    legs = _legs(in_memory_db, "earn-subscribe:")
    assert len(legs) == 2
    assert {row["kind"] for row in legs} == {"transfer"}
    assert legs[0]["transfer_id"] == legs[1]["transfer_id"]

    by_account = {row["account_id"]: row for row in legs}
    out = by_account[binance_accounts["Binance Spot"]]
    into = by_account[binance_accounts["Binance Earn"]]
    assert float(out["amount"]) == -750.0
    assert float(into["amount"]) == 750.0
    assert into["currency"] == "USDT"


def test_a_redemption_lands_in_the_account_binance_names(
    in_memory_db: sqlite3.Connection,
    mocked_binance_sdk: MagicMock,
    binance_accounts: dict[str, int],
) -> None:
    """destAccount is FUNDING on most real redemptions, not SPOT."""
    mocked_binance_sdk.get_flexible_redemption_record.return_value = {
        "rows": [
            {
                "amount": "201.70039079",
                "asset": "USDT",
                "time": REDEEM_MS,
                "status": "PAID",
                "redeemId": 969774807,
                "destAccount": "FUNDING",
                "productId": "USDT001",
            }
        ],
        "total": 1,
    }

    _sync(in_memory_db, mocked_binance_sdk)

    legs = _legs(in_memory_db, "earn-redeem:")
    assert len(legs) == 2
    by_account = {row["account_id"]: row for row in legs}
    out = by_account[binance_accounts["Binance Earn"]]
    into = by_account[binance_accounts["Binance Funding"]]
    assert float(out["amount"]) == -201.70039079
    assert float(into["amount"]) == 201.70039079


def test_a_subscription_funded_from_funding_debits_funding(
    in_memory_db: sqlite3.Connection,
    mocked_binance_sdk: MagicMock,
    binance_accounts: dict[str, int],
) -> None:
    """sourceAccount is read, not assumed."""
    mocked_binance_sdk.get_flexible_subscription_record.return_value = {
        "rows": [
            {
                "amount": "100",
                "asset": "USDC",
                "time": SUBSCRIBE_MS,
                "status": "SUCCESS",
                "purchaseId": 555,
                "sourceAccount": "FUNDING",
                "productId": "USDC001",
            }
        ],
        "total": 1,
    }

    _sync(in_memory_db, mocked_binance_sdk)

    legs = _legs(in_memory_db, "earn-subscribe:")
    accounts = {row["account_id"] for row in legs}
    assert accounts == {
        binance_accounts["Binance Funding"],
        binance_accounts["Binance Earn"],
    }
    assert {row["currency"] for row in legs} == {"USDC"}


def test_earn_principal_ingest_is_idempotent(
    in_memory_db: sqlite3.Connection,
    mocked_binance_sdk: MagicMock,
    binance_accounts: dict[str, int],
) -> None:
    mocked_binance_sdk.get_flexible_subscription_record.return_value = {
        "rows": [
            {
                "amount": "750",
                "asset": "USDT",
                "time": SUBSCRIBE_MS,
                "status": "SUCCESS",
                "purchaseId": 12383413269,
                "sourceAccount": "SPOT",
                "productId": "USDT001",
            }
        ],
        "total": 1,
    }

    first = _sync(in_memory_db, mocked_binance_sdk)
    second = _sync(in_memory_db, mocked_binance_sdk)

    assert first["rows_inserted"] == 2
    assert second["rows_inserted"] == 0
    assert len(_legs(in_memory_db, "earn-subscribe:")) == 2


def test_an_unsuccessful_subscription_is_not_recorded(
    in_memory_db: sqlite3.Connection,
    mocked_binance_sdk: MagicMock,
    binance_accounts: dict[str, int],
) -> None:
    """A pending or failed order did not move money."""
    mocked_binance_sdk.get_flexible_subscription_record.return_value = {
        "rows": [
            {
                "amount": "750",
                "asset": "USDT",
                "time": SUBSCRIBE_MS,
                "status": "PURCHASING",
                "purchaseId": 99,
                "sourceAccount": "SPOT",
                "productId": "USDT001",
            }
        ],
        "total": 1,
    }

    _sync(in_memory_db, mocked_binance_sdk)

    assert _legs(in_memory_db, "earn-subscribe:") == []


def test_an_endpoint_failure_is_reported_not_swallowed(
    in_memory_db: sqlite3.Connection,
    mocked_binance_sdk: MagicMock,
    binance_accounts: dict[str, int],
) -> None:
    mocked_binance_sdk.get_flexible_subscription_record.side_effect = RuntimeError(
        "boom"
    )

    result = _sync(in_memory_db, mocked_binance_sdk)

    assert any("earn-subscribe" in err for err in result["errors"])
