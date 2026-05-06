"""EPIC-010 — Binance P2P Rate Fetcher (TDD, red phase).

These tests are written before any implementation, per rule-011. They exercise
the full public API of `finances.ingest.p2p_rates`:

* `RawP2pAdvert` — Pydantic trust-boundary model (ADR-009).
* `fetch_p2p_adverts` — POSTs to Binance's public P2P search endpoint and
  returns a list of validated adverts.
* `compute_median_price` — median of a list of adverts.
* `ingest_p2p_rates` — writes three rows per run (BUY median, SELL median,
  midpoint) to the `rates` table using `source='binance_p2p_median[_buy|_sell]'`
  per ADR-005.

HTTP mocking note: rule-011 names `responses` as the HTTP-mocking tool, but
`responses` only patches the `requests` library. This project uses `httpx`
(see `pyproject.toml` and `finances/ingest/bcv.py`). Following the EPIC-009
precedent, HTTP is mocked by patching `httpx.Client` on the ingest module via
`pytest-mock`.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import Any
from unittest.mock import MagicMock

import httpx
import pytest
from pydantic import ValidationError

from finances.db.repos import rates as rates_repo


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _advert_payload(price: str, asset: str = "USDT", fiat: str = "VES") -> dict[str, Any]:
    """Minimal shape of one item in Binance's public P2P search response."""
    return {
        "adv": {
            "advNo": f"adv-{price}",
            "price": price,
            "asset": asset,
            "fiatUnit": fiat,
        }
    }


def _p2p_response(prices: list[str], asset: str = "USDT", fiat: str = "VES") -> dict[str, Any]:
    return {
        "code": "000000",
        "success": True,
        "data": [_advert_payload(p, asset=asset, fiat=fiat) for p in prices],
        "total": len(prices),
    }


class _FakeResponse:
    """Minimal httpx.Response-shaped stand-in returned by the mocked Client."""

    def __init__(self, payload: dict[str, Any] | list[Any], status_code: int = 200) -> None:
        self._payload = payload
        self.status_code = status_code

    def json(self) -> Any:
        return self._payload

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise httpx.HTTPStatusError(
                f"HTTP {self.status_code}",
                request=httpx.Request("POST", "https://example.invalid"),
                response=httpx.Response(self.status_code),
            )


def _install_mock_client(mocker: Any, responses_in_order: list[_FakeResponse]) -> MagicMock:
    """Patch the module's `httpx.Client` so `Client(...).post(...)` returns in
    order from `responses_in_order`. Returns the mocked Client class so callers
    can assert on it."""
    from finances.ingest import p2p_rates as mod

    instance = MagicMock(name="httpx.Client-instance")
    instance.post.side_effect = responses_in_order
    client_cm = MagicMock(name="httpx.Client-cm")
    client_cm.__enter__ = MagicMock(return_value=instance)
    client_cm.__exit__ = MagicMock(return_value=False)
    # Support both `httpx.Client(...)` direct use and `with httpx.Client(...) as c`.
    client_cm.post = instance.post
    client_cm.close = MagicMock()
    client_cls = mocker.patch.object(mod.httpx, "Client", return_value=client_cm)
    return client_cls


# ---------------------------------------------------------------------------
# RawP2pAdvert (trust-boundary Pydantic model per ADR-009)
# ---------------------------------------------------------------------------


class TestRawP2pAdvert:
    def test_accepts_valid_advert_and_coerces_types(self) -> None:
        from finances.ingest.p2p_rates import RawP2pAdvert

        adv = RawP2pAdvert(
            price="38.25",
            asset="usdt",
            fiat_unit="ves",
            trade_type="buy",
        )

        assert adv.price == Decimal("38.25")
        assert adv.asset == "USDT"
        assert adv.fiat_unit == "VES"
        assert adv.trade_type == "BUY"

    def test_rejects_non_numeric_price(self) -> None:
        from finances.ingest.p2p_rates import RawP2pAdvert

        with pytest.raises(ValidationError):
            RawP2pAdvert(
                price="not-a-number",
                asset="USDT",
                fiat_unit="VES",
                trade_type="BUY",
            )

    def test_rejects_bad_trade_type(self) -> None:
        from finances.ingest.p2p_rates import RawP2pAdvert

        with pytest.raises(ValidationError):
            RawP2pAdvert(
                price="38.25",
                asset="USDT",
                fiat_unit="VES",
                trade_type="HOLD",
            )


# ---------------------------------------------------------------------------
# fetch_p2p_adverts (HTTP boundary)
# ---------------------------------------------------------------------------


class TestFetchP2pAdverts:
    def test_posts_to_public_search_endpoint_with_expected_body(self, mocker: Any) -> None:
        from finances.ingest import p2p_rates as mod

        client_cls = _install_mock_client(
            mocker,
            [_FakeResponse(_p2p_response(["38.10", "38.20", "38.30"]))],
        )

        adverts = mod.fetch_p2p_adverts("USDT", "VES", "BUY", rows=3)

        assert len(adverts) == 3
        assert {a.price for a in adverts} == {
            Decimal("38.10"),
            Decimal("38.20"),
            Decimal("38.30"),
        }
        assert all(a.trade_type == "BUY" for a in adverts)
        assert all(a.asset == "USDT" and a.fiat_unit == "VES" for a in adverts)

        # Assert on the outbound request shape.
        instance = client_cls.return_value
        instance.post.assert_called_once()
        call_args = instance.post.call_args
        url = call_args.args[0] if call_args.args else call_args.kwargs["url"]
        body = call_args.kwargs.get("json")
        assert "p2p.binance.com" in url
        assert "adv/search" in url
        assert body is not None
        assert body["asset"] == "USDT"
        assert body["fiat"] == "VES"
        assert body["tradeType"] == "BUY"
        assert body["rows"] == 3

    def test_sell_side_uses_tradeType_sell(self, mocker: Any) -> None:
        from finances.ingest import p2p_rates as mod

        client_cls = _install_mock_client(
            mocker, [_FakeResponse(_p2p_response(["37.90", "38.05"]))]
        )

        adverts = mod.fetch_p2p_adverts("USDT", "VES", "SELL", rows=2)

        assert len(adverts) == 2
        assert all(a.trade_type == "SELL" for a in adverts)
        body = client_cls.return_value.post.call_args.kwargs["json"]
        assert body["tradeType"] == "SELL"

    def test_raises_on_http_error(self, mocker: Any) -> None:
        from finances.ingest import p2p_rates as mod

        _install_mock_client(mocker, [_FakeResponse({"error": "nope"}, status_code=500)])

        with pytest.raises(httpx.HTTPStatusError):
            mod.fetch_p2p_adverts("USDT", "VES", "BUY")

    def test_returns_empty_when_data_key_missing(self, mocker: Any) -> None:
        from finances.ingest import p2p_rates as mod

        _install_mock_client(mocker, [_FakeResponse({"code": "000000", "success": True})])

        adverts = mod.fetch_p2p_adverts("USDT", "VES", "BUY")
        assert adverts == []


# ---------------------------------------------------------------------------
# compute_median_price
# ---------------------------------------------------------------------------


def _make_adverts(prices: list[str], trade_type: str = "BUY") -> list[Any]:
    from finances.ingest.p2p_rates import RawP2pAdvert

    return [
        RawP2pAdvert(price=p, asset="USDT", fiat_unit="VES", trade_type=trade_type)
        for p in prices
    ]


class TestComputeMedianPrice:
    def test_odd_count_picks_middle(self) -> None:
        from finances.ingest.p2p_rates import compute_median_price

        adverts = _make_adverts(["38.10", "38.30", "38.20"])  # unsorted on purpose

        assert compute_median_price(adverts) == Decimal("38.20")

    def test_even_count_averages_middle_two(self) -> None:
        from finances.ingest.p2p_rates import compute_median_price

        adverts = _make_adverts(["38.10", "38.40", "38.20", "38.30"])

        assert compute_median_price(adverts) == Decimal("38.25")

    def test_empty_raises_value_error(self) -> None:
        from finances.ingest.p2p_rates import compute_median_price

        with pytest.raises(ValueError):
            compute_median_price([])


# ---------------------------------------------------------------------------
# ingest_p2p_rates (integration with in-memory DB)
# ---------------------------------------------------------------------------


class TestIngestP2pRates:
    def test_writes_buy_sell_and_midpoint_rows(self, in_memory_db: Any, mocker: Any) -> None:
        from finances.ingest import p2p_rates as mod

        # BUY side: median of [38.00, 38.20, 38.40] = 38.20
        # SELL side: median of [37.80, 37.90, 38.00] = 37.90
        # midpoint = (38.20 + 37.90) / 2 = 38.05
        _install_mock_client(
            mocker,
            [
                _FakeResponse(_p2p_response(["38.00", "38.20", "38.40"])),
                _FakeResponse(_p2p_response(["37.80", "37.90", "38.00"])),
            ],
        )

        result = mod.ingest_p2p_rates(
            in_memory_db,
            as_of_date=date(2026, 4, 19),
            asset="USDT",
            fiat="VES",
            rows=3,
        )

        assert result["buy_median"] == Decimal("38.20")
        assert result["sell_median"] == Decimal("37.90")
        assert result["midpoint"] == Decimal("38.05")

        buy = rates_repo.get(
            in_memory_db,
            as_of_date=date(2026, 4, 19),
            base="USDT",
            quote="VES",
            source="binance_p2p_median_buy",
        )
        sell = rates_repo.get(
            in_memory_db,
            as_of_date=date(2026, 4, 19),
            base="USDT",
            quote="VES",
            source="binance_p2p_median_sell",
        )
        mid = rates_repo.get(
            in_memory_db,
            as_of_date=date(2026, 4, 19),
            base="USDT",
            quote="VES",
            source="binance_p2p_median",
        )
        assert buy is not None and buy.rate == Decimal("38.20")
        assert sell is not None and sell.rate == Decimal("37.90")
        assert mid is not None and mid.rate == Decimal("38.05")

    def test_default_pair_is_usdt_ves(self, in_memory_db: Any, mocker: Any) -> None:
        from finances.ingest import p2p_rates as mod

        _install_mock_client(
            mocker,
            [
                _FakeResponse(_p2p_response(["38.00"])),
                _FakeResponse(_p2p_response(["37.80"])),
            ],
        )

        result = mod.ingest_p2p_rates(in_memory_db, as_of_date=date(2026, 4, 19))

        # Default asset/fiat should be USDT/VES (ADR-005 headline pair).
        mid = rates_repo.get(
            in_memory_db,
            as_of_date=date(2026, 4, 19),
            base="USDT",
            quote="VES",
            source="binance_p2p_median",
        )
        assert mid is not None
        assert result["midpoint"] == (Decimal("38.00") + Decimal("37.80")) / Decimal(2)

    def test_raises_when_either_side_has_no_adverts(
        self, in_memory_db: Any, mocker: Any
    ) -> None:
        from finances.ingest import p2p_rates as mod

        # BUY returns adverts, SELL returns empty -> should error, not write.
        _install_mock_client(
            mocker,
            [
                _FakeResponse(_p2p_response(["38.00", "38.20"])),
                _FakeResponse(_p2p_response([])),
            ],
        )

        with pytest.raises(RuntimeError, match="insufficient P2P adverts"):
            mod.ingest_p2p_rates(
                in_memory_db, as_of_date=date(2026, 4, 19), asset="USDT", fiat="VES"
            )

        # Nothing persisted.
        rows = in_memory_db.execute(
            "SELECT COUNT(*) AS c FROM rates WHERE source LIKE 'binance_p2p%'"
        ).fetchone()
        assert int(rows["c"]) == 0

    def test_idempotent_same_day_rerun_upserts_in_place(
        self, in_memory_db: Any, mocker: Any
    ) -> None:
        from finances.ingest import p2p_rates as mod

        # First run.
        _install_mock_client(
            mocker,
            [
                _FakeResponse(_p2p_response(["38.00", "38.20", "38.40"])),
                _FakeResponse(_p2p_response(["37.80", "37.90", "38.00"])),
            ],
        )
        mod.ingest_p2p_rates(
            in_memory_db, as_of_date=date(2026, 4, 19), asset="USDT", fiat="VES", rows=3
        )

        # Second run same day with different prices — should update in place,
        # not duplicate.
        _install_mock_client(
            mocker,
            [
                _FakeResponse(_p2p_response(["39.00", "39.10", "39.20"])),
                _FakeResponse(_p2p_response(["38.80", "38.90", "39.00"])),
            ],
        )
        mod.ingest_p2p_rates(
            in_memory_db, as_of_date=date(2026, 4, 19), asset="USDT", fiat="VES", rows=3
        )

        rows = in_memory_db.execute(
            """
            SELECT COUNT(*) AS c FROM rates
            WHERE as_of_date = '2026-04-19'
              AND base = 'USDT' AND quote = 'VES'
              AND source LIKE 'binance_p2p_median%'
            """
        ).fetchone()
        assert int(rows["c"]) == 3  # buy, sell, midpoint — no duplicates

        mid = rates_repo.get(
            in_memory_db,
            as_of_date=date(2026, 4, 19),
            base="USDT",
            quote="VES",
            source="binance_p2p_median",
        )
        assert mid is not None
        assert mid.rate == (Decimal("39.10") + Decimal("38.90")) / Decimal(2)


# ---------------------------------------------------------------------------
# dry_run — mirrors the BCV reference pattern (see test_ingest_bcv.py)
# ---------------------------------------------------------------------------


class TestIngestP2pRatesDryRun:
    def test_ingest_p2p_rates_dry_run_persists_nothing(
        self, in_memory_db: Any, mocker: Any
    ) -> None:
        """dry_run=True validates and reports counts without writing to the DB.

        Returned ``rows_written`` reflects the rate rows that *would* be
        inserted; ``rates``, ``import_runs``, and ``import_state`` remain
        untouched.
        """
        from finances.ingest import p2p_rates as mod

        _install_mock_client(
            mocker,
            [
                _FakeResponse(_p2p_response(["38.00", "38.20", "38.40"])),
                _FakeResponse(_p2p_response(["37.80", "37.90", "38.00"])),
            ],
        )

        result = mod.ingest_p2p_rates(
            in_memory_db,
            as_of_date=date(2026, 4, 19),
            asset="USDT",
            fiat="VES",
            rows=3,
            dry_run=True,
        )

        # Would-be count > 0 — empty DB, so all three rates would be net-new.
        assert len(result["rows_written"]) > 0
        assert len(result["rows_written"]) == 3

        rates_count = in_memory_db.execute(
            "SELECT COUNT(*) FROM rates WHERE source LIKE 'binance_p2p%'"
        ).fetchone()[0]
        assert rates_count == 0, "dry-run must not persist rate rows"

        runs_count = in_memory_db.execute(
            "SELECT COUNT(*) FROM import_runs WHERE source='binance_p2p_median'"
        ).fetchone()[0]
        assert runs_count == 0, "dry-run must not persist import_runs rows"

        state_count = in_memory_db.execute(
            "SELECT COUNT(*) FROM import_state WHERE source='binance_p2p_median'"
        ).fetchone()[0]
        assert state_count == 0, "dry-run must not persist import_state rows"

    def test_ingest_p2p_rates_dry_run_after_real_run_does_not_double_count(
        self, in_memory_db: Any, mocker: Any
    ) -> None:
        """A real run followed by dry_run leaves the existing real-run rows
        intact and reports zero would-be NEW inserts (same-day upsert is a
        no-op in terms of net-new rows)."""
        from finances.ingest import p2p_rates as mod

        # First: real run.
        _install_mock_client(
            mocker,
            [
                _FakeResponse(_p2p_response(["38.00", "38.20", "38.40"])),
                _FakeResponse(_p2p_response(["37.80", "37.90", "38.00"])),
            ],
        )
        real_result = mod.ingest_p2p_rates(
            in_memory_db,
            as_of_date=date(2026, 4, 19),
            asset="USDT",
            fiat="VES",
            rows=3,
        )
        assert len(real_result["rows_written"]) == 3

        # Capture the real-run rates so we can verify they survive the dry-run.
        real_mid = rates_repo.get(
            in_memory_db,
            as_of_date=date(2026, 4, 19),
            base="USDT",
            quote="VES",
            source="binance_p2p_median",
        )
        assert real_mid is not None

        # Then: dry-run, same day, different prices. No net-new rates exist
        # to insert (all three sources already have a row at this date), so
        # ``rows_written`` reports zero would-be inserts.
        _install_mock_client(
            mocker,
            [
                _FakeResponse(_p2p_response(["50.00", "50.10", "50.20"])),
                _FakeResponse(_p2p_response(["49.80", "49.90", "50.00"])),
            ],
        )
        dry_result = mod.ingest_p2p_rates(
            in_memory_db,
            as_of_date=date(2026, 4, 19),
            asset="USDT",
            fiat="VES",
            rows=3,
            dry_run=True,
        )
        assert len(dry_result["rows_written"]) == 0, (
            "second pass against the same date is fully duplicate; dry-run "
            "should report 0 would-be inserts"
        )

        # Real-run data is intact and unchanged.
        rates_count = in_memory_db.execute(
            "SELECT COUNT(*) FROM rates "
            "WHERE source LIKE 'binance_p2p%' AND as_of_date='2026-04-19'"
        ).fetchone()[0]
        assert rates_count == 3, "dry-run must not corrupt real-run data"

        post_mid = rates_repo.get(
            in_memory_db,
            as_of_date=date(2026, 4, 19),
            base="USDT",
            quote="VES",
            source="binance_p2p_median",
        )
        assert post_mid is not None
        assert post_mid.rate == real_mid.rate, "dry-run must not overwrite rates"

        # Only one provenance row from the real run.
        runs_count = in_memory_db.execute(
            "SELECT COUNT(*) FROM import_runs WHERE source='binance_p2p_median'"
        ).fetchone()[0]
        assert runs_count == 1, "only the real run should leave provenance"
