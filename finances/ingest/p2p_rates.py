"""EPIC-010 — Binance public P2P rate fetcher.

Fetches the top-N BUY and SELL adverts for a `(asset, fiat)` pair from
Binance's public P2P search endpoint, computes the median price on each side,
and upserts three rows into the `rates` table per run:

* ``source='binance_p2p_median_buy'``  — median of top-N BUY adverts.
* ``source='binance_p2p_median_sell'`` — median of top-N SELL adverts.
* ``source='binance_p2p_median'``      — midpoint of BUY/SELL medians. This is
  the headline USDT/VES rate consumed by the rate resolver (ADR-005) and by
  ``v_consolidated_usd`` (schema 001_initial.sql).

Idempotency is provided by ``rates.UNIQUE(as_of_date, base, quote, source)`` —
re-running the same day updates the existing rows rather than duplicating.
"""

from __future__ import annotations

import sqlite3
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any

import httpx
from pydantic import BaseModel, ConfigDict, field_validator

from finances.db.repos import rates as rates_repo
from finances.domain.models import Rate

BINANCE_P2P_SEARCH_URL = "https://p2p.binance.com/bapi/c2c/v2/friendly/c2c/adv/search"
DEFAULT_TOP_N = 10
REQUEST_TIMEOUT_SECONDS = 15.0


class RawP2pAdvert(BaseModel):
    """Trust-boundary Pydantic model for one Binance P2P advert (ADR-009).

    The public P2P response nests the interesting fields under ``data[].adv``;
    callers are expected to flatten them before construction.
    """

    model_config = ConfigDict(strict=False, extra="forbid")

    price: Decimal
    asset: str
    fiat_unit: str
    trade_type: str

    @field_validator("price", mode="before")
    @classmethod
    def _decimal_price(cls, v: Any) -> Decimal:
        if isinstance(v, Decimal):
            return v
        try:
            return Decimal(str(v))
        except InvalidOperation as exc:
            raise ValueError(f"price {v!r} is not a valid decimal") from exc

    @field_validator("asset", "fiat_unit", mode="before")
    @classmethod
    def _upper_code(cls, v: Any) -> Any:
        return v.upper() if isinstance(v, str) else v

    @field_validator("trade_type", mode="before")
    @classmethod
    def _validate_trade_type(cls, v: Any) -> Any:
        if not isinstance(v, str):
            return v
        v = v.upper()
        if v not in ("BUY", "SELL"):
            raise ValueError(f"trade_type must be BUY or SELL, got {v!r}")
        return v


def fetch_p2p_adverts(
    asset: str,
    fiat: str,
    trade_type: str,
    *,
    rows: int = DEFAULT_TOP_N,
    client: httpx.Client | None = None,
) -> list[RawP2pAdvert]:
    """POST to Binance's public P2P search endpoint and return validated adverts.

    Args:
        asset: e.g. ``"USDT"``.
        fiat: e.g. ``"VES"``.
        trade_type: ``"BUY"`` or ``"SELL"`` (advert side).
        rows: page size to request (default :data:`DEFAULT_TOP_N`).
        client: optional ``httpx.Client`` for dependency injection. When omitted,
            a short-lived client with :data:`REQUEST_TIMEOUT_SECONDS` is used.

    Returns:
        A list of :class:`RawP2pAdvert`. Empty when the endpoint returns no
        ``data`` key or an empty list.

    Raises:
        httpx.HTTPStatusError: non-2xx response.
        pydantic.ValidationError: malformed advert fields.
    """
    payload: dict[str, Any] = {
        "asset": asset.upper(),
        "fiat": fiat.upper(),
        "tradeType": trade_type.upper(),
        "page": 1,
        "rows": rows,
        "payTypes": [],
        "publisherType": None,
    }

    if client is not None:
        response = client.post(BINANCE_P2P_SEARCH_URL, json=payload)
        response.raise_for_status()
        body = response.json()
    else:
        with httpx.Client(timeout=REQUEST_TIMEOUT_SECONDS) as http:
            response = http.post(BINANCE_P2P_SEARCH_URL, json=payload)
            response.raise_for_status()
            body = response.json()

    raw_data = body.get("data") if isinstance(body, dict) else None
    if not raw_data:
        return []

    adverts: list[RawP2pAdvert] = []
    for item in raw_data:
        adv = item.get("adv", {}) if isinstance(item, dict) else {}
        adverts.append(
            RawP2pAdvert(
                price=adv.get("price"),
                asset=adv.get("asset"),
                fiat_unit=adv.get("fiatUnit"),
                trade_type=trade_type,
            )
        )
    return adverts


def compute_median_price(adverts: list[RawP2pAdvert]) -> Decimal:
    """Return the median advert price as a :class:`Decimal`.

    Uses the even-count average of the two middle values (standard median),
    kept in ``Decimal`` arithmetic so downstream Rate rows retain precision.

    Raises:
        ValueError: ``adverts`` is empty.
    """
    if not adverts:
        raise ValueError("cannot compute median of zero adverts")

    prices = sorted(a.price for a in adverts)
    n = len(prices)
    mid = n // 2
    if n % 2 == 1:
        return prices[mid]
    return (prices[mid - 1] + prices[mid]) / Decimal(2)


def ingest_p2p_rates(
    conn: sqlite3.Connection,
    *,
    as_of_date: date | None = None,
    asset: str = "USDT",
    fiat: str = "VES",
    rows: int = DEFAULT_TOP_N,
    client: httpx.Client | None = None,
) -> dict[str, Any]:
    """Fetch BUY+SELL P2P medians and upsert buy/sell/midpoint rows.

    Writes three rows to ``rates`` for ``(as_of_date, asset, fiat)``:

    * ``source='binance_p2p_median_buy'``
    * ``source='binance_p2p_median_sell'``
    * ``source='binance_p2p_median'`` — midpoint (headline)

    Args:
        conn: Open sqlite3 connection with the schema migrations applied.
        as_of_date: Date to stamp on the rate rows. Defaults to today (UTC).
        asset: Base asset code (default ``"USDT"``).
        fiat: Quote currency code (default ``"VES"``).
        rows: Top-N adverts to fetch per side.
        client: Optional ``httpx.Client`` for DI; otherwise one is created.

    Returns:
        A dict with keys ``as_of_date``, ``buy_median``, ``sell_median``,
        ``midpoint``, ``buy_adverts_used``, ``sell_adverts_used``,
        ``rows_written``.

    Raises:
        RuntimeError: either BUY or SELL side returned zero adverts.
    """
    if as_of_date is None:
        as_of_date = datetime.now(tz=timezone.utc).date()

    buy_adverts = fetch_p2p_adverts(asset, fiat, "BUY", rows=rows, client=client)
    sell_adverts = fetch_p2p_adverts(asset, fiat, "SELL", rows=rows, client=client)

    if not buy_adverts or not sell_adverts:
        raise RuntimeError(
            f"insufficient P2P adverts: buy={len(buy_adverts)}, sell={len(sell_adverts)}"
        )

    buy_median = compute_median_price(buy_adverts)
    sell_median = compute_median_price(sell_adverts)
    midpoint = (buy_median + sell_median) / Decimal(2)

    base_code = asset.upper()
    quote_code = fiat.upper()

    rows_written: list[Rate] = []
    for src, value in (
        ("binance_p2p_median_buy", buy_median),
        ("binance_p2p_median_sell", sell_median),
        ("binance_p2p_median", midpoint),
    ):
        rate = Rate(
            as_of_date=as_of_date,
            base=base_code,
            quote=quote_code,
            rate=value,
            source=src,
        )
        rows_written.append(rates_repo.upsert(conn, rate))

    return {
        "as_of_date": as_of_date,
        "buy_median": buy_median,
        "sell_median": sell_median,
        "midpoint": midpoint,
        "buy_adverts_used": len(buy_adverts),
        "sell_adverts_used": len(sell_adverts),
        "rows_written": rows_written,
    }


__all__ = [
    "BINANCE_P2P_SEARCH_URL",
    "DEFAULT_TOP_N",
    "RawP2pAdvert",
    "compute_median_price",
    "fetch_p2p_adverts",
    "ingest_p2p_rates",
]
