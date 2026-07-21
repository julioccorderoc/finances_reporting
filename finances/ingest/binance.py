"""Binance incremental ingest (EPIC-007 / ADR-003 / ADR-009 / ADR-010).

Pulls Binance SDK endpoints, parses each row into a ``RawBinance*Row`` Pydantic
model, and writes canonical :class:`Transaction` rows via
``repos.transactions.upsert_by_source_ref`` using stable SDK-provided IDs as
``source_ref`` (ADR-010). Funding↔Spot internal transfers emit a paired row via
``domain.transfers.create_transfer``. Earn rewards become ``Interest`` income on
the Binance Earn account and the ``earn_positions`` table is refreshed from
``get_flexible_product_position``.

Per ADR-002 amendment: P2P sells do **not** create a bank-side leg here — the
bank is the pairing anchor and that job lives in ``finances/ingest/provincial.py``.

SDK mocked at the boundary per rule-011.
"""
from __future__ import annotations

import sqlite3
import time
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any, Iterable, Literal, Sequence

from pydantic import BaseModel, ConfigDict, field_validator

from finances.config import BINANCE_DEFAULT_LOOKBACK_DAYS
from finances.db.repos import accounts as accounts_repo
from finances.db.repos import categories as categories_repo
from finances.db.repos import import_state as import_state_repo
from finances.db.repos import transactions as transactions_repo
from finances.domain import realized_rates
from finances.domain.earn import EarnSnapshotRow, refresh_earn_positions
from finances.domain.models import Transaction, TransactionKind
from finances.domain.transfers import create_transfer

SOURCE = "binance"
DEFAULT_LOOKBACK_DAYS: int = BINANCE_DEFAULT_LOOKBACK_DAYS

_SPOT_ACCOUNT_NAME = "Binance Spot"
_FUNDING_ACCOUNT_NAME = "Binance Funding"
_EARN_ACCOUNT_NAME = "Binance Earn"
_INTEREST_CATEGORY = ("income", "Interest")


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def _coerce_decimal(v: Any) -> Decimal:
    if isinstance(v, Decimal):
        return v
    if isinstance(v, bool):
        raise ValueError("bool is not a valid monetary value")
    if isinstance(v, float):
        raise ValueError("float monetary inputs are forbidden")
    if isinstance(v, (int, str)):
        return Decimal(str(v))
    raise ValueError(f"cannot coerce {type(v).__name__} to Decimal")


def _from_ms(value: int | str) -> datetime:
    return datetime.fromtimestamp(int(value) / 1000, tz=UTC)


def _parse_occurred_at(value: Any) -> datetime:
    """Accept Binance's two timestamp shapes: ms-epoch int or ISO-ish string."""
    if isinstance(value, (int, float)):
        return _from_ms(int(value))
    if isinstance(value, str):
        try:
            return _from_ms(int(value))
        except ValueError:
            pass
        dt = datetime.fromisoformat(value.replace(" ", "T"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)
        return dt
    raise ValueError(f"cannot parse timestamp: {value!r}")


def compute_server_offset_ms(
    client: Any, *, local_time_ms: int | None = None
) -> int:
    """Return ``serverTime - localTime`` in milliseconds (legacy script logic)."""
    server_time = int(client.time()["serverTime"])
    local = int(local_time_ms if local_time_ms is not None else time.time() * 1000)
    return server_time - local


# ---------------------------------------------------------------------------
# Raw SDK row models (ADR-009)
# ---------------------------------------------------------------------------

class _RawBase(BaseModel):
    model_config = ConfigDict(strict=False, extra="ignore")


class RawBinanceDepositRow(_RawBase):
    txId: str
    coin: str
    amount: Decimal
    insertTime: int

    @field_validator("amount", mode="before")
    @classmethod
    def _dec(cls, v: Any) -> Decimal:
        return _coerce_decimal(v)

    def to_transaction(self, *, spot_account_id: int) -> Transaction:
        return Transaction(
            account_id=spot_account_id,
            occurred_at=_from_ms(self.insertTime),
            kind=TransactionKind.INCOME,
            amount=self.amount,
            currency=self.coin.upper(),
            description=f"Binance deposit {self.coin.upper()}",
            source=SOURCE,
            source_ref=f"deposit:{self.txId}",
        )


class RawBinanceWithdrawRow(_RawBase):
    id: str
    coin: str
    amount: Decimal
    applyTime: Any

    @field_validator("amount", mode="before")
    @classmethod
    def _dec(cls, v: Any) -> Decimal:
        return _coerce_decimal(v)

    def to_transaction(self, *, spot_account_id: int) -> Transaction:
        return Transaction(
            account_id=spot_account_id,
            occurred_at=_parse_occurred_at(self.applyTime),
            kind=TransactionKind.EXPENSE,
            amount=-self.amount,
            currency=self.coin.upper(),
            description=f"Binance withdraw {self.coin.upper()}",
            source=SOURCE,
            source_ref=f"withdraw:{self.id}",
        )


class RawBinanceP2pRow(_RawBase):
    orderNumber: str
    tradeType: Literal["BUY", "SELL"]
    asset: str
    amount: Decimal
    unitPrice: Decimal
    fiat: str
    createTime: int

    @field_validator("amount", "unitPrice", mode="before")
    @classmethod
    def _dec(cls, v: Any) -> Decimal:
        return _coerce_decimal(v)

    def to_transaction(self, *, spot_account_id: int) -> Transaction:
        if self.tradeType == "SELL":
            kind = TransactionKind.EXPENSE
            amount = -self.amount
        else:
            kind = TransactionKind.INCOME
            amount = self.amount
        description = (
            f"P2P {self.tradeType} {self.asset.upper()} @ "
            f"{format(self.unitPrice, 'f')} {self.fiat.upper()} "
            f"(order {self.orderNumber})"
        )
        return Transaction(
            account_id=spot_account_id,
            occurred_at=_from_ms(self.createTime),
            kind=kind,
            amount=amount,
            currency=self.asset.upper(),
            description=description,
            user_rate=self.unitPrice,
            source=SOURCE,
            source_ref=f"p2p:{self.orderNumber}",
        )


class RawBinanceConvertRow(_RawBase):
    # Real /sapi/v1/convert/tradeFlow rows carry orderId/quoteId, not tranId.
    orderId: str
    fromAsset: str
    fromAmount: Decimal
    toAsset: str
    toAmount: Decimal
    createTime: int

    @field_validator("fromAmount", "toAmount", mode="before")
    @classmethod
    def _dec(cls, v: Any) -> Decimal:
        return _coerce_decimal(v)

    @field_validator("orderId", mode="before")
    @classmethod
    def _str_id(cls, v: Any) -> str:
        return str(v)

    def to_transactions(self, *, spot_account_id: int) -> list[Transaction]:
        occurred_at = _from_ms(self.createTime)
        description = (
            f"Convert {format(self.fromAmount, 'f')} {self.fromAsset.upper()} → "
            f"{format(self.toAmount, 'f')} {self.toAsset.upper()} (order {self.orderId})"
        )
        from_leg = Transaction(
            account_id=spot_account_id,
            occurred_at=occurred_at,
            kind=TransactionKind.EXPENSE,
            amount=-self.fromAmount,
            currency=self.fromAsset.upper(),
            description=description,
            source=SOURCE,
            source_ref=f"convert:{self.orderId}:from",
        )
        to_leg = Transaction(
            account_id=spot_account_id,
            occurred_at=occurred_at,
            kind=TransactionKind.INCOME,
            amount=self.toAmount,
            currency=self.toAsset.upper(),
            description=description,
            source=SOURCE,
            source_ref=f"convert:{self.orderId}:to",
        )
        return [from_leg, to_leg]


_TRANSFER_DIRECTIONS = {
    "MAIN_FUNDING": ("spot", "funding"),
    "FUNDING_MAIN": ("funding", "spot"),
}


class RawBinanceTransferRow(_RawBase):
    tranId: int | str
    type: str
    asset: str
    amount: Decimal
    timestamp: int

    @field_validator("amount", mode="before")
    @classmethod
    def _dec(cls, v: Any) -> Decimal:
        return _coerce_decimal(v)

    @field_validator("type")
    @classmethod
    def _check_type(cls, v: str) -> str:
        if v not in _TRANSFER_DIRECTIONS:
            raise ValueError(f"unknown transfer direction: {v}")
        return v

    def from_kind(self) -> str:
        return _TRANSFER_DIRECTIONS[self.type][0]

    def to_kind(self) -> str:
        return _TRANSFER_DIRECTIONS[self.type][1]


class RawBinanceEarnRewardRow(_RawBase):
    asset: str
    rewards: Decimal
    time: int
    type: str
    projectId: str | None = None

    @field_validator("rewards", mode="before")
    @classmethod
    def _dec(cls, v: Any) -> Decimal:
        return _coerce_decimal(v)

    def source_ref(self) -> str:
        return (
            f"earn-reward:{self.projectId or 'noproj'}:{self.asset.upper()}:{self.time}"
        )

    def to_transaction(
        self, *, earn_account_id: int, interest_category_id: int | None
    ) -> Transaction:
        return Transaction(
            account_id=earn_account_id,
            occurred_at=_from_ms(self.time),
            kind=TransactionKind.INCOME,
            amount=self.rewards,
            currency=self.asset.upper(),
            description=f"Earn reward {self.type} {self.asset.upper()}",
            category_id=interest_category_id,
            source=SOURCE,
            source_ref=self.source_ref(),
        )


class RawBinancePayRow(_RawBase):
    orderId: str
    orderType: str
    amount: Decimal
    currency: str
    transactionTime: int

    @field_validator("amount", mode="before")
    @classmethod
    def _dec(cls, v: Any) -> Decimal:
        return _coerce_decimal(v)

    def to_transaction(self, *, spot_account_id: int) -> Transaction:
        # Binance Pay's transaction history returns a *signed* ``amount``:
        # positive = money in (income), negative = money out (expense). Trust
        # the sign — not ``orderType``. A C2C *send* carries a negative amount
        # but an orderType of "C2C" (never "PAY..."), so the previous
        # orderType heuristic mislabeled every send as income.
        if self.amount < 0:
            kind = TransactionKind.EXPENSE
            direction = "outgoing"
        else:
            kind = TransactionKind.INCOME
            direction = "incoming"
        return Transaction(
            account_id=spot_account_id,
            occurred_at=_from_ms(self.transactionTime),
            kind=kind,
            amount=self.amount,
            currency=self.currency.upper(),
            description=f"Binance Pay {self.orderType} ({direction})",
            source=SOURCE,
            source_ref=f"pay:{self.orderId}",
        )


# ---------------------------------------------------------------------------
# Main sync entry point
# ---------------------------------------------------------------------------

def _resolve_accounts(conn: sqlite3.Connection) -> dict[str, int]:
    ids: dict[str, int] = {}
    for name in (_SPOT_ACCOUNT_NAME, _FUNDING_ACCOUNT_NAME, _EARN_ACCOUNT_NAME):
        acct = accounts_repo.get_by_name(conn, name)
        if acct is None or acct.id is None:
            raise RuntimeError(
                f"account '{name}' must exist before running Binance ingest"
            )
        ids[name] = acct.id
    return ids


def _interest_category_id(conn: sqlite3.Connection) -> int | None:
    cat = categories_repo.get_by_name(conn, TransactionKind.INCOME, "Interest")
    return cat.id if cat is not None else None


_MAX_WINDOW_DAYS = 29
_MAX_WINDOW_MS = _MAX_WINDOW_DAYS * 24 * 60 * 60 * 1000


def _window_chunks(start_ms: int, end_ms: int) -> list[tuple[int, int]]:
    """Split ``[start_ms, end_ms]`` into windows of at most 29 days.

    Binance sapi history endpoints cap query ranges at 30 days and either
    reject longer ones (-6021 "Query time range too large") or silently
    truncate. Overlap at chunk boundaries is harmless — dedup via
    ``UNIQUE(source, source_ref)`` absorbs it.
    """
    chunks: list[tuple[int, int]] = []
    cursor = start_ms
    while cursor < end_ms:
        chunks.append((cursor, min(cursor + _MAX_WINDOW_MS, end_ms)))
        cursor += _MAX_WINDOW_MS
    return chunks or [(start_ms, end_ms)]


def _resolve_time_window(
    conn: sqlite3.Connection,
    *,
    since: datetime | None,
    lookback_days: int,
) -> tuple[int, int]:
    now_ms = int(datetime.now(tz=UTC).timestamp() * 1000)
    if since is not None:
        if since.tzinfo is None:
            raise ValueError("--since must be timezone-aware")
        start_ms = int(since.timestamp() * 1000)
    else:
        state = import_state_repo.get_state(conn, SOURCE)
        last = state["last_synced_at"] if state else None
        if last is not None:
            if isinstance(last, str):
                last_dt = datetime.fromisoformat(last)
            else:
                last_dt = last
            if last_dt.tzinfo is None:
                last_dt = last_dt.replace(tzinfo=UTC)
            start_ms = int(last_dt.timestamp() * 1000)
        else:
            start_ms = now_ms - lookback_days * 24 * 60 * 60 * 1000
    return start_ms, now_ms


def _unpack_rows(response: Any) -> list[dict[str, Any]]:
    if response is None:
        return []
    if isinstance(response, list):
        return [r for r in response if isinstance(r, dict)]
    if isinstance(response, dict):
        for key in ("data", "rows", "list"):
            if key in response and isinstance(response[key], list):
                return response[key]
    return []


def _ingest_deposits(
    conn: sqlite3.Connection,
    client: Any,
    *,
    start_ms: int,
    end_ms: int,
    spot_id: int,
    stats: dict[str, int],
    errors: list[str],
) -> None:
    raw = _unpack_rows(client.deposit_history(startTime=start_ms, endTime=end_ms))
    for item in raw:
        try:
            row = RawBinanceDepositRow.model_validate(item)
        except Exception as exc:  # noqa: BLE001
            errors.append(f"deposit: {exc}")
            continue
        result = transactions_repo.upsert_by_source_ref(
            conn, row.to_transaction(spot_account_id=spot_id)
        )
        stats["rows_inserted"] += result["rows_inserted"]
        stats["rows_updated"] += result["rows_updated"]


def _ingest_withdrawals(
    conn: sqlite3.Connection,
    client: Any,
    *,
    start_ms: int,
    end_ms: int,
    spot_id: int,
    stats: dict[str, int],
    errors: list[str],
) -> None:
    raw = _unpack_rows(client.withdraw_history(startTime=start_ms, endTime=end_ms))
    for item in raw:
        try:
            row = RawBinanceWithdrawRow.model_validate(item)
        except Exception as exc:  # noqa: BLE001
            errors.append(f"withdraw: {exc}")
            continue
        result = transactions_repo.upsert_by_source_ref(
            conn, row.to_transaction(spot_account_id=spot_id)
        )
        stats["rows_inserted"] += result["rows_inserted"]
        stats["rows_updated"] += result["rows_updated"]


def _ingest_p2p(
    conn: sqlite3.Connection,
    client: Any,
    *,
    start_ms: int,
    end_ms: int,
    spot_id: int,
    stats: dict[str, int],
    errors: list[str],
) -> None:
    for trade_type in ("BUY", "SELL"):
        try:
            response = client.c2c_trade_history(
                tradeType=trade_type, startTimestamp=start_ms, endTimestamp=end_ms
            )
        except Exception as exc:  # noqa: BLE001
            errors.append(f"p2p {trade_type}: {exc}")
            continue
        for item in _unpack_rows(response):
            item = {**item, "tradeType": item.get("tradeType", trade_type)}
            try:
                row = RawBinanceP2pRow.model_validate(item)
            except Exception as exc:  # noqa: BLE001
                errors.append(f"p2p {trade_type}: {exc}")
                continue
            result = transactions_repo.upsert_by_source_ref(
                conn, row.to_transaction(spot_account_id=spot_id)
            )
            stats["rows_inserted"] += result["rows_inserted"]
            stats["rows_updated"] += result["rows_updated"]


def _ingest_converts(
    conn: sqlite3.Connection,
    client: Any,
    *,
    start_ms: int,
    end_ms: int,
    spot_id: int,
    stats: dict[str, int],
    errors: list[str],
) -> None:
    raw = _unpack_rows(
        client.get_convert_trade_history(startTime=start_ms, endTime=end_ms)
    )
    for item in raw:
        status = item.get("orderStatus") if isinstance(item, dict) else None
        if status is not None and status != "SUCCESS":
            continue
        try:
            row = RawBinanceConvertRow.model_validate(item)
        except Exception as exc:  # noqa: BLE001
            errors.append(f"convert: {exc}")
            continue
        for leg in row.to_transactions(spot_account_id=spot_id):
            result = transactions_repo.upsert_by_source_ref(conn, leg)
            stats["rows_inserted"] += result["rows_inserted"]
            stats["rows_updated"] += result["rows_updated"]


def _ingest_internal_transfers(
    conn: sqlite3.Connection,
    client: Any,
    *,
    start_ms: int,
    end_ms: int,
    account_ids: dict[str, int],
    stats: dict[str, int],
    errors: list[str],
) -> None:
    kind_to_id = {
        "spot": account_ids[_SPOT_ACCOUNT_NAME],
        "funding": account_ids[_FUNDING_ACCOUNT_NAME],
    }
    for transfer_type in ("MAIN_FUNDING", "FUNDING_MAIN"):
        try:
            response = client.user_universal_transfer_history(
                type=transfer_type, startTime=start_ms, endTime=end_ms
            )
        except Exception as exc:  # noqa: BLE001
            errors.append(f"transfer {transfer_type}: {exc}")
            continue
        for item in _unpack_rows(response):
            item = {**item, "type": item.get("type", transfer_type)}
            try:
                row = RawBinanceTransferRow.model_validate(item)
            except Exception as exc:  # noqa: BLE001
                errors.append(f"transfer {transfer_type}: {exc}")
                continue
            source_ref_from = f"transfer:{row.tranId}:from"
            source_ref_to = f"transfer:{row.tranId}:to"
            existing = transactions_repo.get_by_source_ref(
                conn, SOURCE, source_ref_from
            )
            if existing is not None:
                continue  # idempotent: pair already materialized
            create_transfer(
                conn,
                from_account_id=kind_to_id[row.from_kind()],
                to_account_id=kind_to_id[row.to_kind()],
                amount=row.amount,
                currency=row.asset.upper(),
                occurred_at=_from_ms(row.timestamp),
                description=f"Binance internal transfer {transfer_type}",
                source=SOURCE,
                source_ref_from=source_ref_from,
                source_ref_to=source_ref_to,
            )
            stats["rows_inserted"] += 2


_REWARDS_PAGE_SIZE = 100


def _fetch_rewards_pages(
    client: Any, *, reward_type: str, start_ms: int, end_ms: int
) -> list[dict[str, Any]]:
    """Fetch ALL pages of flexible-rewards history for one reward type.

    The endpoint defaults to 10 rows/page (max 100) and reports the full
    count in ``total``; without paging, daily REALTIME rewards silently
    truncate at the first page.
    """
    rows: list[dict[str, Any]] = []
    current = 1
    while True:
        response = client.get_flexible_rewards_history(
            type=reward_type,
            startTime=start_ms,
            endTime=end_ms,
            current=current,
            size=_REWARDS_PAGE_SIZE,
        )
        page = _unpack_rows(response)
        rows.extend(page)
        total = response.get("total") if isinstance(response, dict) else None
        if not page or len(page) < _REWARDS_PAGE_SIZE:
            return rows
        if isinstance(total, int) and len(rows) >= total:
            return rows
        current += 1


def _ingest_earn_rewards(
    conn: sqlite3.Connection,
    client: Any,
    *,
    start_ms: int,
    end_ms: int,
    earn_id: int,
    interest_id: int | None,
    stats: dict[str, int],
    errors: list[str],
) -> None:
    raw: list[Any] = []
    # The rewards endpoint requires `type`; the three streams are disjoint.
    for reward_type in ("REALTIME", "BONUS", "REWARDS"):
        try:
            raw.extend(
                _fetch_rewards_pages(
                    client,
                    reward_type=reward_type,
                    start_ms=start_ms,
                    end_ms=end_ms,
                )
            )
        except Exception as exc:  # noqa: BLE001
            errors.append(f"earn-reward[{reward_type}]: {exc}")
    for item in raw:
        try:
            row = RawBinanceEarnRewardRow.model_validate(item)
        except Exception as exc:  # noqa: BLE001
            errors.append(f"earn-reward: {exc}")
            continue
        result = transactions_repo.upsert_by_source_ref(
            conn,
            row.to_transaction(
                earn_account_id=earn_id, interest_category_id=interest_id
            ),
        )
        stats["rows_inserted"] += result["rows_inserted"]
        stats["rows_updated"] += result["rows_updated"]


def _ingest_pay(
    conn: sqlite3.Connection,
    client: Any,
    *,
    start_ms: int,
    end_ms: int,
    spot_id: int,
    stats: dict[str, int],
    errors: list[str],
) -> None:
    try:
        response = client.pay_history(startTime=start_ms, endTime=end_ms)
    except Exception as exc:  # noqa: BLE001
        errors.append(f"pay: {exc}")
        return
    for item in _unpack_rows(response):
        try:
            row = RawBinancePayRow.model_validate(item)
        except Exception as exc:  # noqa: BLE001
            errors.append(f"pay: {exc}")
            continue
        result = transactions_repo.upsert_by_source_ref(
            conn, row.to_transaction(spot_account_id=spot_id)
        )
        stats["rows_inserted"] += result["rows_inserted"]
        stats["rows_updated"] += result["rows_updated"]


def _ingest_earn_positions(
    conn: sqlite3.Connection,
    client: Any,
    *,
    earn_id: int,
    snapshot_at: datetime,
    errors: list[str],
) -> dict[str, int]:
    try:
        raw = _unpack_rows(client.get_flexible_product_position(size=100))
    except Exception as exc:  # noqa: BLE001
        # Do NOT refresh on a failed fetch: an empty snapshot would wrongly
        # close every open position.
        errors.append(f"earn-position: {exc}")
        return {"inserted": 0, "closed": 0, "unchanged": 0}
    snapshot: list[EarnSnapshotRow] = []
    for item in raw:
        try:
            # Real positions carry latestAnnualPercentageRate; 'apr' kept as
            # fallback for older payload shapes.
            apr_raw = item.get("latestAnnualPercentageRate", item.get("apr"))
            snapshot.append(
                EarnSnapshotRow(
                    product_id=str(item["productId"]),
                    asset=str(item["asset"]),
                    principal=_coerce_decimal(item.get("totalAmount", item.get("amount", "0"))),
                    apy=_coerce_decimal(apr_raw) if apr_raw is not None else None,
                )
            )
        except Exception as exc:  # noqa: BLE001
            errors.append(f"earn-position: {exc}")
    return refresh_earn_positions(
        conn,
        snapshot=snapshot,
        earn_account_id=earn_id,
        snapshot_at=snapshot_at,
    )


def sync_binance(
    conn: sqlite3.Connection,
    *,
    client: Any,
    since: datetime | None = None,
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Pull every configured Binance endpoint, upsert into the ledger.

    Idempotent by construction: stable SDK IDs in ``source_ref`` mean a second
    run on identical mock data inserts 0 rows (ADR-010).

    Per rule-007 / ADR-007, every run records one row in ``import_runs``:
    a ``status='success'`` row on the happy path (with ``rows_inserted`` /
    ``rows_updated`` counts), or a ``status='error'`` row carrying a brief
    error summary and re-raising the exception on failure.

    When ``dry_run=True``, the function performs the same fetch/parse/upsert
    pipeline (so ``rows_inserted`` accurately reflects what *would* be written),
    but the surrounding transaction is rolled back and no rows are written to
    ``transactions``, ``earn_positions``, ``import_runs``, or ``import_state``.
    """
    run_id = None if dry_run else import_state_repo.start_run(conn, SOURCE)
    try:
        account_ids = _resolve_accounts(conn)
        interest_id = _interest_category_id(conn)
        compute_server_offset_ms(client)
        start_ms, end_ms = _resolve_time_window(
            conn, since=since, lookback_days=lookback_days
        )

        stats = {"rows_inserted": 0, "rows_updated": 0}
        errors: list[str] = []

        spot_id = account_ids[_SPOT_ACCOUNT_NAME]
        earn_id = account_ids[_EARN_ACCOUNT_NAME]

        conn.execute("BEGIN")
        try:
            for chunk_start, chunk_end in _window_chunks(start_ms, end_ms):
                _ingest_deposits(
                    conn, client, start_ms=chunk_start, end_ms=chunk_end,
                    spot_id=spot_id, stats=stats, errors=errors,
                )
                _ingest_withdrawals(
                    conn, client, start_ms=chunk_start, end_ms=chunk_end,
                    spot_id=spot_id, stats=stats, errors=errors,
                )
                _ingest_p2p(
                    conn, client, start_ms=chunk_start, end_ms=chunk_end,
                    spot_id=spot_id, stats=stats, errors=errors,
                )
                _ingest_converts(
                    conn, client, start_ms=chunk_start, end_ms=chunk_end,
                    spot_id=spot_id, stats=stats, errors=errors,
                )
                _ingest_internal_transfers(
                    conn, client, start_ms=chunk_start, end_ms=chunk_end,
                    account_ids=account_ids, stats=stats, errors=errors,
                )
                _ingest_earn_rewards(
                    conn, client, start_ms=chunk_start, end_ms=chunk_end,
                    earn_id=earn_id, interest_id=interest_id, stats=stats,
                    errors=errors,
                )
                _ingest_pay(
                    conn, client, start_ms=chunk_start, end_ms=chunk_end,
                    spot_id=spot_id, stats=stats, errors=errors,
                )

            snapshot_at = datetime.now(tz=UTC)
            earn_stats = _ingest_earn_positions(
                conn, client, earn_id=earn_id, snapshot_at=snapshot_at, errors=errors,
            )

            # Any P2P sell just ingested changes the realized VES cost basis
            # (ADR-013). Rebuilding inside the transaction keeps the derived
            # rates consistent with the fills they come from, and lets
            # ``dry_run`` roll both back together. It runs after the chunk
            # loop so every fill in the window is accounted for at once.
            realized_rates.rebuild(conn)

            if dry_run:
                conn.execute("ROLLBACK")
            else:
                conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise

        if not dry_run:
            if not errors:
                # Only advance the sync watermark on a fully clean run. A
                # swallowed endpoint failure would otherwise move
                # last_synced_at past a window that was never ingested —
                # re-running must re-cover it (dedup makes that free).
                import_state_repo.upsert_state(
                    conn, source=SOURCE, last_synced_at=snapshot_at
                )

            import_state_repo.finish_run(
                conn,
                run_id,
                status="success" if not errors else "error",
                rows_inserted=stats["rows_inserted"],
                rows_updated=stats["rows_updated"],
                error="; ".join(errors)[:4000] if errors else None,
            )

        return {
            **stats,
            "earn_positions": earn_stats,
            "errors": errors,
            "start_ms": start_ms,
            "end_ms": end_ms,
        }
    except Exception as exc:
        if not dry_run:
            error_msg = f"{type(exc).__name__}: {exc}"[:4000]
            import_state_repo.finish_run(
                conn, run_id, status="error", error=error_msg
            )
        raise


__all__ = [
    "DEFAULT_LOOKBACK_DAYS",
    "RawBinanceConvertRow",
    "RawBinanceDepositRow",
    "RawBinanceEarnRewardRow",
    "RawBinanceP2pRow",
    "RawBinancePayRow",
    "RawBinanceTransferRow",
    "RawBinanceWithdrawRow",
    "compute_server_offset_ms",
    "sync_binance",
]
