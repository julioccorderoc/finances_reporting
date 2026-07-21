"""Tests for finances/ingest/binance.py — Binance incremental sync.

EPIC-007 / ADR-003 / ADR-009 / ADR-010.

Coverage:
- Server-time offset computation
- Per-endpoint ``Raw*Row`` → ``Transaction`` mapping (ADR-009)
- Stable SDK-ID-based ``source_ref`` (ADR-010)
- Idempotency (second run inserts 0)
- Funding↔Spot internal transfers paired via ``create_transfer``
- Earn rewards as Interest income + ``earn_positions`` refresh (ADR-003)
- ``--since`` / ``--lookback-days`` flag plumbing

Per rule-011: SDK mocked via the ``mocked_binance_sdk`` fixture; no live calls.
"""
from __future__ import annotations

import sqlite3
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any
from unittest.mock import MagicMock

import pytest
from typer.testing import CliRunner

from finances.cli.main import app
from finances.db.repos import accounts as accounts_repo
from finances.db.repos import categories as categories_repo
from finances.db.repos import import_state as import_state_repo
from finances.db.repos import positions as positions_repo
from finances.db.repos import transactions as transactions_repo
from finances.domain.models import Account, AccountKind, Category, TransactionKind
from finances.ingest.binance import (
    DEFAULT_LOOKBACK_DAYS,
    RawBinanceConvertRow,
    RawBinanceDepositRow,
    RawBinanceEarnRewardRow,
    RawBinanceP2pRow,
    RawBinancePayRow,
    RawBinanceTransferRow,
    RawBinanceWithdrawRow,
    compute_server_offset_ms,
    sync_binance,
)


# ---------------------------------------------------------------------------
# Seed helpers — minimal Binance accounts + Interest category
# ---------------------------------------------------------------------------

def _seed_binance_accounts(conn: sqlite3.Connection) -> dict[str, int]:
    seeds = (
        ("Binance Spot", AccountKind.CRYPTO_SPOT, "USDT"),
        ("Binance Funding", AccountKind.CRYPTO_FUNDING, "USDT"),
        ("Binance Earn", AccountKind.CRYPTO_EARN, "USDT"),
    )
    ids: dict[str, int] = {}
    for name, kind, currency in seeds:
        existing = accounts_repo.get_by_name(conn, name)
        if existing is None:
            existing = accounts_repo.insert(
                conn,
                Account(name=name, kind=kind, currency=currency, institution="Binance"),
            )
        assert existing.id is not None
        ids[name] = existing.id
    # Interest income category (rule-003).
    if categories_repo.get_by_name(conn, TransactionKind.INCOME, "Interest") is None:
        categories_repo.insert(
            conn, Category(kind=TransactionKind.INCOME, name="Interest")
        )
    return ids


# ---------------------------------------------------------------------------
# Server-time sync
# ---------------------------------------------------------------------------

def test_compute_server_offset_ms_happy_path() -> None:
    client = MagicMock()
    client.time.return_value = {"serverTime": 1_700_000_005_000}
    offset = compute_server_offset_ms(client, local_time_ms=1_700_000_000_000)
    assert offset == 5_000


def test_compute_server_offset_ms_uses_current_time_when_unspecified() -> None:
    client = MagicMock()
    client.time.return_value = {"serverTime": 1_700_000_000_000}
    offset = compute_server_offset_ms(client)
    assert isinstance(offset, int)


def test_compute_server_offset_ms_raises_on_malformed_response() -> None:
    client = MagicMock()
    client.time.return_value = {}
    with pytest.raises(KeyError):
        compute_server_offset_ms(client, local_time_ms=1_700_000_000_000)


# ---------------------------------------------------------------------------
# RawBinanceDepositRow
# ---------------------------------------------------------------------------

def test_deposit_row_to_transaction_income_on_spot() -> None:
    row = RawBinanceDepositRow(
        txId="abc123",
        coin="USDT",
        amount="50.00",
        insertTime=1_700_000_000_000,
    )
    txn = row.to_transaction(spot_account_id=1)
    assert txn.account_id == 1
    assert txn.kind == TransactionKind.INCOME
    assert txn.amount == Decimal("50.00")
    assert txn.currency == "USDT"
    assert txn.source == "binance"
    assert txn.source_ref == "deposit:abc123"


def test_deposit_row_rejects_missing_txid() -> None:
    with pytest.raises(Exception):
        RawBinanceDepositRow(coin="USDT", amount="1", insertTime=1_700_000_000_000)  # type: ignore[call-arg]


# ---------------------------------------------------------------------------
# RawBinanceWithdrawRow
# ---------------------------------------------------------------------------

def test_withdraw_row_to_transaction_expense_on_spot() -> None:
    row = RawBinanceWithdrawRow(
        id="wdr-42",
        coin="USDT",
        amount="20.00",
        applyTime="2026-04-19 12:00:00",
    )
    txn = row.to_transaction(spot_account_id=1)
    assert txn.kind == TransactionKind.EXPENSE
    assert txn.amount == Decimal("-20.00")
    assert txn.source_ref == "withdraw:wdr-42"


# ---------------------------------------------------------------------------
# RawBinanceP2pRow — sell/buy, remark includes unitPrice+fiat
# ---------------------------------------------------------------------------

def test_p2p_sell_row_emits_expense_with_user_rate_and_remark() -> None:
    row = RawBinanceP2pRow(
        orderNumber="O-1",
        tradeType="SELL",
        asset="USDT",
        amount="10.00",
        unitPrice="150.00",
        fiat="VES",
        createTime=1_700_000_000_000,
    )
    txn = row.to_transaction(spot_account_id=1)
    assert txn.kind == TransactionKind.EXPENSE
    assert txn.amount == Decimal("-10.00")
    assert txn.currency == "USDT"
    assert txn.user_rate == Decimal("150.00")
    assert txn.source_ref == "p2p:O-1"
    assert "150.00" in (txn.description or "")
    assert "VES" in (txn.description or "")


def test_p2p_buy_row_emits_income() -> None:
    row = RawBinanceP2pRow(
        orderNumber="O-2",
        tradeType="BUY",
        asset="USDT",
        amount="10.00",
        unitPrice="150.00",
        fiat="VES",
        createTime=1_700_000_000_000,
    )
    txn = row.to_transaction(spot_account_id=1)
    assert txn.kind == TransactionKind.INCOME
    assert txn.amount == Decimal("10.00")


# ---------------------------------------------------------------------------
# RawBinanceConvertRow — produces two legs
# ---------------------------------------------------------------------------

def test_convert_row_emits_both_legs() -> None:
    row = RawBinanceConvertRow(
        orderId="T-1",
        fromAsset="USDT",
        fromAmount="100.00",
        toAsset="BTC",
        toAmount="0.0015",
        createTime=1_700_000_000_000,
    )
    legs = row.to_transactions(spot_account_id=1)
    assert len(legs) == 2
    from_leg, to_leg = legs
    assert from_leg.kind == TransactionKind.EXPENSE
    assert from_leg.amount == Decimal("-100.00")
    assert from_leg.currency == "USDT"
    assert from_leg.source_ref == "convert:T-1:from"
    assert to_leg.kind == TransactionKind.INCOME
    assert to_leg.amount == Decimal("0.0015")
    assert to_leg.currency == "BTC"
    assert to_leg.source_ref == "convert:T-1:to"


def test_convert_row_parses_real_trade_flow_shape() -> None:
    """Real /sapi/v1/convert/tradeFlow rows: orderId (int) + quoteId, no tranId.

    Regression: model originally required a ``tranId`` field that the live
    API never sends.
    """
    row = RawBinanceConvertRow.model_validate(
        {
            "quoteId": "f63ade9e22bd4d33",
            "orderId": 940708407462087195,
            "orderStatus": "SUCCESS",
            "fromAsset": "USDT",
            "fromAmount": "20",
            "toAsset": "BNB",
            "toAmount": "0.06154036",
            "ratio": "0.00307702",
            "inverseRatio": "324.99",
            "createTime": 1_699_100_000_000,
            "walletType": "SPOT",
        }
    )
    legs = row.to_transactions(spot_account_id=1)
    assert legs[0].source_ref == "convert:940708407462087195:from"
    assert legs[1].source_ref == "convert:940708407462087195:to"


# ---------------------------------------------------------------------------
# RawBinanceTransferRow — Funding↔Spot
# ---------------------------------------------------------------------------

def test_transfer_row_main_to_funding_direction() -> None:
    row = RawBinanceTransferRow(
        tranId=555,
        type="MAIN_FUNDING",
        asset="USDT",
        amount="10.00",
        timestamp=1_700_000_000_000,
    )
    assert row.from_kind() == "spot"
    assert row.to_kind() == "funding"


def test_transfer_row_funding_to_main_direction() -> None:
    row = RawBinanceTransferRow(
        tranId=556,
        type="FUNDING_MAIN",
        asset="USDT",
        amount="10.00",
        timestamp=1_700_000_000_000,
    )
    assert row.from_kind() == "funding"
    assert row.to_kind() == "spot"


def test_transfer_row_rejects_unknown_type() -> None:
    with pytest.raises(ValueError):
        RawBinanceTransferRow(
            tranId=1,
            type="UNKNOWN_DIRECTION",
            asset="USDT",
            amount="10",
            timestamp=1_700_000_000_000,
        )


# ---------------------------------------------------------------------------
# RawBinanceEarnRewardRow
# ---------------------------------------------------------------------------

def test_earn_reward_row_to_transaction_is_interest_income_on_earn() -> None:
    row = RawBinanceEarnRewardRow(
        asset="USDT",
        rewards="0.12345678",
        time=1_700_000_000_000,
        type="BONUS",
        projectId="PROJECT-X",
    )
    txn = row.to_transaction(earn_account_id=3, interest_category_id=7)
    assert txn.account_id == 3
    assert txn.kind == TransactionKind.INCOME
    assert txn.amount == Decimal("0.12345678")
    assert txn.category_id == 7
    assert txn.source_ref.startswith("earn-reward:")


# ---------------------------------------------------------------------------
# RawBinancePayRow
# ---------------------------------------------------------------------------

def test_pay_row_positive_amount_is_income() -> None:
    # Binance Pay API returns a *signed* amount: positive = money in.
    row = RawBinancePayRow(
        orderId="PAY-1",
        orderType="C2C",
        amount="5.00",
        currency="USDT",
        transactionTime=1_700_000_000_000,
    )
    txn = row.to_transaction(spot_account_id=1)
    assert txn.kind == TransactionKind.INCOME
    assert txn.amount == Decimal("5.00")
    assert txn.source_ref == "pay:PAY-1"
    assert "(incoming)" in txn.description


def test_pay_row_negative_amount_is_expense() -> None:
    # Regression (2026-07-11): a C2C *send* carries orderType="C2C" (never
    # starts with "PAY") but a negative amount — money leaving the account.
    # The old orderType heuristic mislabeled every send as income. The sign of
    # ``amount`` is the source of truth: negative => expense.
    row = RawBinancePayRow(
        orderId="PAY-2",
        orderType="C2C",
        amount="-5.00",
        currency="USDT",
        transactionTime=1_700_000_000_000,
    )
    txn = row.to_transaction(spot_account_id=1)
    assert txn.kind == TransactionKind.EXPENSE
    assert txn.amount == Decimal("-5.00")
    assert "(outgoing)" in txn.description


# ---------------------------------------------------------------------------
# sync_binance — end-to-end happy path
# ---------------------------------------------------------------------------

def _configure_sdk_with_sample_data(
    mocked_binance_sdk: MagicMock,
) -> None:
    """Populate every endpoint the ingester touches with 1-2 rows."""
    mocked_binance_sdk.time.return_value = {"serverTime": 1_700_000_000_000}

    mocked_binance_sdk.deposit_history.return_value = [
        {"txId": "DEP-1", "coin": "USDT", "amount": "100.00", "insertTime": 1_699_000_000_000},
    ]
    mocked_binance_sdk.withdraw_history.return_value = [
        {"id": "WDR-1", "coin": "USDT", "amount": "10.00", "applyTime": "2026-04-10 00:00:00"},
    ]
    mocked_binance_sdk.c2c_trade_history.return_value = {
        "data": [
            {
                "orderNumber": "P2P-1",
                "tradeType": "SELL",
                "asset": "USDT",
                "amount": "50.00",
                "unitPrice": "150.00",
                "fiat": "VES",
                "createTime": 1_699_500_000_000,
            },
        ],
        "total": 1,
    }
    mocked_binance_sdk.get_convert_trade_history.return_value = {
        "list": [
            {
                "orderId": 940700000000000001,
                "quoteId": "q-conv-1",
                "orderStatus": "SUCCESS",
                "fromAsset": "USDT",
                "fromAmount": "30.00",
                "toAsset": "BTC",
                "toAmount": "0.0005",
                "createTime": 1_699_100_000_000,
            },
        ]
    }
    mocked_binance_sdk.user_universal_transfer_history.return_value = {
        "rows": [
            {
                "tranId": 777,
                "type": "MAIN_FUNDING",
                "asset": "USDT",
                "amount": "20.00",
                "timestamp": 1_699_200_000_000,
            },
        ],
        "total": 1,
    }
    def _rewards_history(*_args: Any, type: str, **_kwargs: Any) -> dict[str, Any]:
        if type != "BONUS":
            return {"rows": [], "total": 0}
        return {
            "rows": [
                {
                    "asset": "USDT",
                    "rewards": "0.0123",
                    "time": 1_699_300_000_000,
                    "type": "BONUS",
                    "projectId": "PROJ-A",
                },
            ],
            "total": 1,
        }

    mocked_binance_sdk.get_flexible_rewards_history.side_effect = _rewards_history
    mocked_binance_sdk.pay_history.return_value = {
        "data": [
            {
                "orderId": "PAY-1",
                "orderType": "C2C",
                "amount": "5.00",
                "currency": "USDT",
                "transactionTime": 1_699_400_000_000,
            },
        ]
    }
    mocked_binance_sdk.get_flexible_product_position.return_value = {
        "rows": [
            {
                "productId": "USDT001",
                "asset": "USDT",
                "totalAmount": "500.00",
                "latestAnnualPercentageRate": "0.05",
            },
        ],
        "total": 1,
    }


def test_sync_binance_happy_path_inserts_and_is_idempotent(
    in_memory_db: sqlite3.Connection,
    mocked_binance_sdk: MagicMock,
) -> None:
    _seed_binance_accounts(in_memory_db)
    _configure_sdk_with_sample_data(mocked_binance_sdk)

    first = sync_binance(in_memory_db, client=mocked_binance_sdk, lookback_days=35)
    assert first["rows_inserted"] >= 1
    assert first["errors"] == []

    initial_count = transactions_repo.count(in_memory_db)
    assert initial_count > 0

    second = sync_binance(in_memory_db, client=mocked_binance_sdk, lookback_days=35)
    assert second["rows_inserted"] == 0
    assert transactions_repo.count(in_memory_db) == initial_count


def test_sync_binance_creates_paired_transfer_with_shared_transfer_id(
    in_memory_db: sqlite3.Connection,
    mocked_binance_sdk: MagicMock,
) -> None:
    acct_ids = _seed_binance_accounts(in_memory_db)
    mocked_binance_sdk.time.return_value = {"serverTime": 1_700_000_000_000}
    mocked_binance_sdk.user_universal_transfer_history.return_value = {
        "rows": [
            {
                "tranId": 9001,
                "type": "MAIN_FUNDING",
                "asset": "USDT",
                "amount": "25.00",
                "timestamp": 1_699_200_000_000,
            },
        ],
        "total": 1,
    }
    sync_binance(in_memory_db, client=mocked_binance_sdk, lookback_days=35)

    spot_txns = transactions_repo.list_by_account(in_memory_db, acct_ids["Binance Spot"])
    funding_txns = transactions_repo.list_by_account(
        in_memory_db, acct_ids["Binance Funding"]
    )
    transfer_legs = [
        t
        for t in spot_txns + funding_txns
        if t.transfer_id is not None and t.source_ref and "9001" in t.source_ref
    ]
    assert len(transfer_legs) == 2
    assert transfer_legs[0].transfer_id == transfer_legs[1].transfer_id
    assert transfer_legs[0].account_id != transfer_legs[1].account_id
    assert transfer_legs[0].kind == TransactionKind.TRANSFER
    assert transfer_legs[1].kind == TransactionKind.TRANSFER


def test_sync_binance_earn_rewards_become_interest_income_on_earn_account(
    in_memory_db: sqlite3.Connection,
    mocked_binance_sdk: MagicMock,
) -> None:
    acct_ids = _seed_binance_accounts(in_memory_db)
    interest = categories_repo.get_by_name(
        in_memory_db, TransactionKind.INCOME, "Interest"
    )
    assert interest is not None
    mocked_binance_sdk.time.return_value = {"serverTime": 1_700_000_000_000}
    def _rewards_history(*_args: Any, type: str, **_kwargs: Any) -> dict[str, Any]:
        if type != "BONUS":
            return {"rows": [], "total": 0}
        return {
            "rows": [
                {
                    "asset": "USDT",
                    "rewards": "0.50",
                    "time": 1_699_300_000_000,
                    "type": "BONUS",
                    "projectId": "PROJ-A",
                },
            ],
            "total": 1,
        }

    mocked_binance_sdk.get_flexible_rewards_history.side_effect = _rewards_history

    sync_binance(in_memory_db, client=mocked_binance_sdk, lookback_days=35)

    earn_txns = transactions_repo.list_by_account(in_memory_db, acct_ids["Binance Earn"])
    reward = next((t for t in earn_txns if t.source_ref.startswith("earn-reward:")), None)
    assert reward is not None
    assert reward.kind == TransactionKind.INCOME
    assert reward.amount == Decimal("0.50")
    assert reward.category_id == interest.id


def test_sync_binance_refreshes_earn_positions_from_snapshot(
    in_memory_db: sqlite3.Connection,
    mocked_binance_sdk: MagicMock,
) -> None:
    acct_ids = _seed_binance_accounts(in_memory_db)
    mocked_binance_sdk.time.return_value = {"serverTime": 1_700_000_000_000}
    mocked_binance_sdk.get_flexible_product_position.return_value = {
        "rows": [
            {
                "productId": "USDT001",
                "asset": "USDT",
                "totalAmount": "500.00",
                "latestAnnualPercentageRate": "0.05",
            },
            {
                "productId": "BTC001",
                "asset": "BTC",
                "totalAmount": "0.1",
                "latestAnnualPercentageRate": "0.03",
            },
        ],
        "total": 2,
    }
    sync_binance(in_memory_db, client=mocked_binance_sdk, lookback_days=35)
    open_positions = positions_repo.list_open(
        in_memory_db, account_id=acct_ids["Binance Earn"]
    )
    assert {p.product_id for p in open_positions} == {"USDT001", "BTC001"}
    # Regression: real payloads carry latestAnnualPercentageRate, not 'apr' —
    # apy must round-trip, not silently store NULL.
    assert {p.product_id: p.apy for p in open_positions} == {
        "USDT001": Decimal("0.05"),
        "BTC001": Decimal("0.03"),
    }


def test_sync_binance_earn_position_fetch_failure_keeps_positions(
    in_memory_db: sqlite3.Connection,
    mocked_binance_sdk: MagicMock,
) -> None:
    """API failure on the position fetch must NOT touch open positions —
    refreshing from an empty snapshot would close every one of them."""
    acct_ids = _seed_binance_accounts(in_memory_db)
    mocked_binance_sdk.time.return_value = {"serverTime": 1_700_000_000_000}
    mocked_binance_sdk.get_flexible_product_position.return_value = {
        "rows": [
            {
                "productId": "USDT001",
                "asset": "USDT",
                "totalAmount": "500.00",
                "latestAnnualPercentageRate": "0.05",
            },
        ],
        "total": 1,
    }
    sync_binance(in_memory_db, client=mocked_binance_sdk, lookback_days=35)

    mocked_binance_sdk.get_flexible_product_position.side_effect = RuntimeError(
        "boom 451"
    )
    result = sync_binance(in_memory_db, client=mocked_binance_sdk, lookback_days=35)

    open_positions = positions_repo.list_open(
        in_memory_db, account_id=acct_ids["Binance Earn"]
    )
    assert {p.product_id for p in open_positions} == {"USDT001"}
    assert result["earn_positions"] == {"inserted": 0, "closed": 0, "unchanged": 0}
    assert any("earn-position" in e for e in result["errors"])


def test_sync_binance_endpoint_error_does_not_advance_watermark(
    in_memory_db: sqlite3.Connection,
    mocked_binance_sdk: MagicMock,
) -> None:
    """A swallowed endpoint failure must not move last_synced_at — otherwise
    the failed window is silently skipped forever on the next run."""
    _seed_binance_accounts(in_memory_db)
    mocked_binance_sdk.time.return_value = {"serverTime": 1_700_000_000_000}
    mocked_binance_sdk.pay_history.side_effect = RuntimeError("transient 429")

    result = sync_binance(in_memory_db, client=mocked_binance_sdk, lookback_days=35)

    assert result["errors"], "expected the pay failure to be reported"
    assert import_state_repo.get_state(in_memory_db, "binance") is None


def test_sync_binance_records_import_state_last_synced_at(
    in_memory_db: sqlite3.Connection,
    mocked_binance_sdk: MagicMock,
) -> None:
    _seed_binance_accounts(in_memory_db)
    mocked_binance_sdk.time.return_value = {"serverTime": 1_700_000_000_000}

    before = datetime.now(tz=UTC)
    sync_binance(in_memory_db, client=mocked_binance_sdk, lookback_days=35)
    after = datetime.now(tz=UTC)

    state = import_state_repo.get_state(in_memory_db, "binance")
    assert state is not None
    synced_at = state["last_synced_at"]
    if isinstance(synced_at, str):
        synced_at = datetime.fromisoformat(synced_at)
    if synced_at.tzinfo is None:
        synced_at = synced_at.replace(tzinfo=UTC)
    assert before - timedelta(seconds=5) <= synced_at <= after + timedelta(seconds=5)


# ---------------------------------------------------------------------------
# Lookback + since flags
# ---------------------------------------------------------------------------

def test_default_lookback_days_is_35() -> None:
    assert DEFAULT_LOOKBACK_DAYS == 35


def test_sync_binance_uses_since_when_provided(
    in_memory_db: sqlite3.Connection,
    mocked_binance_sdk: MagicMock,
) -> None:
    _seed_binance_accounts(in_memory_db)
    mocked_binance_sdk.time.return_value = {"serverTime": 1_700_000_000_000}
    since = datetime(2026, 1, 1, tzinfo=UTC)

    sync_binance(in_memory_db, client=mocked_binance_sdk, since=since, lookback_days=35)

    # At least one endpoint was called with startTime matching our `since`.
    expected_start_ms = int(since.timestamp() * 1000)
    call_kwargs = _collect_kwargs(mocked_binance_sdk.deposit_history.call_args_list)
    assert any(kw.get("startTime") == expected_start_ms for kw in call_kwargs)


def test_sync_binance_uses_lookback_when_no_since_and_no_state(
    in_memory_db: sqlite3.Connection,
    mocked_binance_sdk: MagicMock,
) -> None:
    _seed_binance_accounts(in_memory_db)
    mocked_binance_sdk.time.return_value = {"serverTime": 1_700_000_000_000}

    sync_binance(in_memory_db, client=mocked_binance_sdk, lookback_days=7)

    call_kwargs = _collect_kwargs(mocked_binance_sdk.deposit_history.call_args_list)
    assert call_kwargs, "deposit_history should be called"
    start_times = [kw.get("startTime") for kw in call_kwargs if "startTime" in kw]
    assert start_times
    now_ms = int(datetime.now(tz=UTC).timestamp() * 1000)
    expected_lower = now_ms - (7 * 24 * 60 * 60 * 1000) - 5_000  # 5s slack
    expected_upper = now_ms - (7 * 24 * 60 * 60 * 1000) + 5_000
    assert any(expected_lower <= s <= expected_upper for s in start_times)


def _collect_kwargs(call_list) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for c in call_list:
        out.append(dict(c.kwargs))
    return out


# ---------------------------------------------------------------------------
# CLI wiring — `finances ingest binance`
# ---------------------------------------------------------------------------

def test_cli_ingest_binance_invokes_sync(
    in_memory_db: sqlite3.Connection,
    mocked_binance_sdk: MagicMock,
    monkeypatch,
    tmp_path,
) -> None:
    from finances.db.connection import get_connection

    _seed_binance_accounts(in_memory_db)
    mocked_binance_sdk.time.return_value = {"serverTime": 1_700_000_000_000}

    # Point the CLI at a temp DB and inject our in-memory migrations-applied
    # conn. Simplest: monkeypatch the factory used by the CLI.
    db_file = tmp_path / "cli.db"
    file_conn = get_connection(db_file)
    from finances.db.migrate import apply_migrations

    apply_migrations(file_conn)
    _seed_binance_accounts(file_conn)
    file_conn.close()

    monkeypatch.setattr("finances.cli.main.DB_PATH", db_file)
    monkeypatch.setattr(
        "finances.cli.main._make_binance_client",
        lambda: mocked_binance_sdk,
    )

    runner = CliRunner()
    result = runner.invoke(app, ["ingest", "binance", "--lookback-days", "7"])
    assert result.exit_code == 0, result.stdout


def test_window_chunks_split_at_29_days() -> None:
    """Binance sapi endpoints reject ranges >30 days (-6021). An 81-day
    window must become 3 chunks of <=29 days covering the full range."""
    from finances.ingest.binance import _MAX_WINDOW_MS, _window_chunks

    day_ms = 24 * 60 * 60 * 1000
    start = 1_700_000_000_000
    end = start + 81 * day_ms

    chunks = _window_chunks(start, end)

    assert len(chunks) == 3
    assert chunks[0][0] == start
    assert chunks[-1][1] == end
    for s, e in chunks:
        assert e - s <= _MAX_WINDOW_MS
    # Contiguous: no gap between consecutive chunks.
    for (_, prev_end), (next_start, _) in zip(chunks, chunks[1:]):
        assert next_start == prev_end

    # Degenerate/small windows stay a single chunk.
    assert _window_chunks(start, start) == [(start, start)]
    assert _window_chunks(start, start + day_ms) == [(start, start + day_ms)]


def test_sync_binance_chunks_large_windows_per_endpoint(
    in_memory_db: sqlite3.Connection,
    mocked_binance_sdk: MagicMock,
) -> None:
    """A --since 81 days back must produce one call per <=29-day chunk on
    every windowed endpoint, each window no larger than 30 days."""
    _seed_binance_accounts(in_memory_db)
    mocked_binance_sdk.time.return_value = {"serverTime": 1_700_000_000_000}

    since = datetime.now(tz=UTC) - timedelta(days=81)
    sync_binance(in_memory_db, client=mocked_binance_sdk, since=since)

    day_ms = 24 * 60 * 60 * 1000
    endpoints = {
        "deposit_history": ("startTime", "endTime"),
        "c2c_trade_history": ("startTimestamp", "endTimestamp"),
        "pay_history": ("startTime", "endTime"),
    }
    for endpoint, (start_key, end_key) in endpoints.items():
        calls = getattr(mocked_binance_sdk, endpoint).call_args_list
        assert len(calls) >= 3, f"{endpoint} not chunked: {len(calls)} calls"
        for call in calls:
            span = call.kwargs[end_key] - call.kwargs[start_key]
            assert span <= 30 * day_ms, f"{endpoint} window too large: {span}"

    # All three disjoint reward streams must be queried — REALTIME is the
    # dominant daily-interest stream and must never be dropped silently.
    reward_types = {
        c.kwargs["type"]
        for c in mocked_binance_sdk.get_flexible_rewards_history.call_args_list
    }
    assert reward_types == {"REALTIME", "BONUS", "REWARDS"}


def test_fetch_rewards_pages_paginates_past_first_page() -> None:
    """Rewards history defaults to 10 rows/page (max 100) with the real count
    in `total` — a 105-row window must yield 2 calls and all 105 rows."""
    from binance.spot import Spot

    from finances.ingest.binance import _fetch_rewards_pages

    client = MagicMock(name="binance.Spot", spec=Spot)

    def _paged(*_args: Any, current: int, size: int, **_kwargs: Any) -> dict[str, Any]:
        assert size == 100
        rows = [
            {"asset": "USDT", "rewards": "0.01", "time": i, "type": "REALTIME"}
            for i in range((current - 1) * 100, min(current * 100, 105))
        ]
        return {"rows": rows, "total": 105}

    client.get_flexible_rewards_history.side_effect = _paged

    rows = _fetch_rewards_pages(
        client, reward_type="REALTIME", start_ms=0, end_ms=1_000_000
    )

    assert len(rows) == 105
    assert client.get_flexible_rewards_history.call_count == 2
    currents = [
        c.kwargs["current"]
        for c in client.get_flexible_rewards_history.call_args_list
    ]
    assert currents == [1, 2]


def test_cli_ingest_binance_naive_since_interpreted_as_caracas(
    monkeypatch,
    tmp_path,
) -> None:
    """`--since 2026-04-19` arrives tz-naive from Typer; the CLI must localize
    it to America/Caracas instead of letting sync_binance reject it
    (regression: ValueError '--since must be timezone-aware')."""
    from finances.config import CARACAS_TZ

    monkeypatch.setattr("finances.cli.main.DB_PATH", tmp_path / "cli.db")
    monkeypatch.setattr(
        "finances.cli.main._make_binance_client", lambda: MagicMock()
    )

    captured: dict[str, Any] = {}

    def fake_sync(conn, *, client, since, lookback_days, dry_run):
        captured["since"] = since
        return {
            "rows_inserted": 0,
            "rows_updated": 0,
            "earn_positions": 0,
            "errors": [],
        }

    monkeypatch.setattr("finances.ingest.binance.sync_binance", fake_sync)

    runner = CliRunner()
    result = runner.invoke(
        app, ["ingest", "binance", "--since", "2026-04-19", "--dry-run"]
    )
    assert result.exit_code == 0, result.stdout
    assert captured["since"] == datetime(2026, 4, 19, tzinfo=CARACAS_TZ)


# ---------------------------------------------------------------------------
# dry_run flag — no DB writes, no provenance, return shape preserved
# ---------------------------------------------------------------------------

def test_sync_binance_dry_run_persists_nothing(
    in_memory_db: sqlite3.Connection,
    mocked_binance_sdk: MagicMock,
) -> None:
    """dry_run=True executes the full pipeline but rolls back every write.

    Returned shape is the same as the real run; ``rows_inserted`` counts what
    *would* be inserted. ``transactions``, ``import_runs``, ``import_state``,
    and ``earn_positions`` all remain untouched.
    """
    _seed_binance_accounts(in_memory_db)
    _configure_sdk_with_sample_data(mocked_binance_sdk)

    result = sync_binance(
        in_memory_db, client=mocked_binance_sdk, lookback_days=35, dry_run=True
    )

    assert result["rows_inserted"] > 0, "dry-run should report would-be inserts"

    txn_count = in_memory_db.execute(
        "SELECT COUNT(*) FROM transactions WHERE source='binance'"
    ).fetchone()[0]
    assert txn_count == 0, "dry-run must not persist transactions"

    runs_count = in_memory_db.execute(
        "SELECT COUNT(*) FROM import_runs WHERE source='binance'"
    ).fetchone()[0]
    assert runs_count == 0, "dry-run must not persist import_runs rows"

    state_count = in_memory_db.execute(
        "SELECT COUNT(*) FROM import_state WHERE source='binance'"
    ).fetchone()[0]
    assert state_count == 0, "dry-run must not persist import_state rows"

    earn_count = in_memory_db.execute(
        "SELECT COUNT(*) FROM earn_positions"
    ).fetchone()[0]
    assert earn_count == 0, "dry-run must not persist earn_positions rows"


def test_sync_binance_dry_run_after_real_run_does_not_double_count(
    in_memory_db: sqlite3.Connection,
    mocked_binance_sdk: MagicMock,
) -> None:
    """A real run followed by dry_run reports zero would-be inserts (idempotent)
    and leaves the existing real-run rows intact."""
    _seed_binance_accounts(in_memory_db)
    _configure_sdk_with_sample_data(mocked_binance_sdk)

    real = sync_binance(in_memory_db, client=mocked_binance_sdk, lookback_days=35)
    assert real["rows_inserted"] > 0
    real_txn_count = in_memory_db.execute(
        "SELECT COUNT(*) FROM transactions WHERE source='binance'"
    ).fetchone()[0]
    assert real_txn_count > 0
    real_runs_count = in_memory_db.execute(
        "SELECT COUNT(*) FROM import_runs WHERE source='binance'"
    ).fetchone()[0]
    assert real_runs_count == 1

    dry = sync_binance(
        in_memory_db, client=mocked_binance_sdk, lookback_days=35, dry_run=True
    )
    assert dry["rows_inserted"] == 0, (
        "second pass against same SDK data is fully duplicate; dry-run "
        "should report 0 would-be inserts"
    )

    txn_count_after = in_memory_db.execute(
        "SELECT COUNT(*) FROM transactions WHERE source='binance'"
    ).fetchone()[0]
    assert txn_count_after == real_txn_count, (
        "dry-run must not corrupt or remove real-run data"
    )

    runs_count_after = in_memory_db.execute(
        "SELECT COUNT(*) FROM import_runs WHERE source='binance'"
    ).fetchone()[0]
    assert runs_count_after == 1, "only the real run should leave provenance"


# ---------------------------------------------------------------------------
# Realized cost-basis rates are materialized as part of the run (ADR-013).
# ---------------------------------------------------------------------------


def _realized_rows(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    return conn.execute(
        "SELECT as_of_date, base, quote, rate FROM rates "
        "WHERE source = 'binance_p2p_realized' ORDER BY as_of_date"
    ).fetchall()


def test_sync_binance_materializes_realized_rates(
    in_memory_db: sqlite3.Connection,
    mocked_binance_sdk: MagicMock,
) -> None:
    """A P2P sell must leave behind the realized rate the resolver reads."""
    _seed_binance_accounts(in_memory_db)
    _configure_sdk_with_sample_data(mocked_binance_sdk)

    sync_binance(in_memory_db, client=mocked_binance_sdk, lookback_days=35)

    rows = _realized_rows(in_memory_db)
    assert len(rows) == 1
    assert rows[0]["base"] == "USDT"
    assert rows[0]["quote"] == "VES"
    assert Decimal(str(rows[0]["rate"])) == Decimal("150.00")


def test_sync_binance_dry_run_writes_no_realized_rates(
    in_memory_db: sqlite3.Connection,
    mocked_binance_sdk: MagicMock,
) -> None:
    """dry_run rolls back the whole pipeline, realized rates included."""
    _seed_binance_accounts(in_memory_db)
    _configure_sdk_with_sample_data(mocked_binance_sdk)

    sync_binance(
        in_memory_db, client=mocked_binance_sdk, lookback_days=35, dry_run=True
    )

    assert _realized_rows(in_memory_db) == []


def test_sync_binance_realized_rates_are_idempotent(
    in_memory_db: sqlite3.Connection,
    mocked_binance_sdk: MagicMock,
) -> None:
    _seed_binance_accounts(in_memory_db)
    _configure_sdk_with_sample_data(mocked_binance_sdk)

    sync_binance(in_memory_db, client=mocked_binance_sdk, lookback_days=35)
    sync_binance(in_memory_db, client=mocked_binance_sdk, lookback_days=35)

    assert len(_realized_rows(in_memory_db)) == 1
