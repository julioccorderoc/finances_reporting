"""RED — a conversion ignores the wallet Binance names (ADR-025).

After ADR-024 both USDT positions agree with the exchange to the cent.
One difference survived, and ADR-023's check named both halves of it:
Spot USDC reads −400.38 against 0.23, Funding USDC reads 400.00 against
nothing at all.

The rows say what happened, one minute apart:

    19:32  earn-redeem:989421227        +400 USDC  -> Funding (destAccount=FUNDING)
    19:33  convert:2342399485914713482  -400.19 USDC -> Spot

The redemption is right — Binance's own record says FUNDING. The convert
consumed the USDC that had just landed there, and `to_transactions` books
every leg against Spot.

Binance has been naming the wallet on the record the whole time. Across
the ledger's history: seven conversions ``SPOT``, one ``SPOT_FUNDING`` —
2026-08-22, this one.
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from finances.ingest.binance import RawBinanceConvertRow

SPOT_ID = 2
FUNDING_ID = 3


def _row(wallet_type: str | None = None) -> RawBinanceConvertRow:
    payload = {
        "orderId": "2342399485914713482",
        "fromAsset": "USDC",
        "fromAmount": "400.191772",
        "toAsset": "USDT",
        "toAmount": "400.05103465",
        "createTime": 1_787_600_000_000,
    }
    if wallet_type is not None:
        payload["walletType"] = wallet_type
    return RawBinanceConvertRow.model_validate(payload)


def _legs(wallet_type: str | None = None):
    return _row(wallet_type).to_transactions(
        spot_account_id=SPOT_ID, funding_account_id=FUNDING_ID
    )


class TestConvertReadsItsWallet:
    def test_a_spot_conversion_keeps_both_legs_on_spot(self):
        """Seven of the eight conversions in the ledger, unchanged."""
        out_leg, in_leg = _legs("SPOT")

        assert out_leg.account_id == SPOT_ID
        assert in_leg.account_id == SPOT_ID

    def test_a_record_with_no_wallet_type_is_treated_as_spot(self):
        """The endpoint carried no such field once. Absence is the old
        behaviour, not an unknown wallet."""
        out_leg, in_leg = _legs(None)

        assert out_leg.account_id == SPOT_ID
        assert in_leg.account_id == SPOT_ID

    def test_a_combined_wallet_conversion_draws_from_funding(self):
        """The live 2026-08-22 case: 400.19 USDC left, and Funding was
        where 400.00 of it sat."""
        out_leg, _ = _legs("SPOT_FUNDING")

        assert out_leg.account_id == FUNDING_ID
        assert out_leg.amount == Decimal("-400.191772")
        assert out_leg.currency == "USDC"

    def test_the_proceeds_of_a_combined_conversion_land_in_spot(self):
        """Booking both legs against Funding would put 400.05 USDT there
        that Binance says is not there — and the USDT positions currently
        agree to the cent. The measurement decides this, not symmetry."""
        _, in_leg = _legs("SPOT_FUNDING")

        assert in_leg.account_id == SPOT_ID
        assert in_leg.amount == Decimal("400.05103465")
        assert in_leg.currency == "USDT"

    def test_it_stays_one_transfer_pair_across_two_accounts(self):
        """rule-002 defines a transfer as movement between two (account,
        currency) positions; ADR-017 widened that to two positions on one
        account. A cross-account conversion is the ordinary reading, not a
        new shape — and both legs must still share the id."""
        out_leg, in_leg = _legs("SPOT_FUNDING")

        assert out_leg.transfer_id == in_leg.transfer_id
        assert out_leg.transfer_id is not None
        assert out_leg.source_ref != in_leg.source_ref

    def test_an_unrecognised_wallet_is_refused_rather_than_assumed(self):
        """Silently filing an unknown wallet as Spot is the exact failure
        this ADR corrects. The next wallet Binance invents must surface as
        an ingest error, not as a slow drift."""
        with pytest.raises(ValueError):
            _legs("MARGIN_ISOLATED")
