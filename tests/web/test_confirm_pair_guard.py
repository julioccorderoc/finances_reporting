"""The manual pair-confirm must refuse a pairing that cannot be real.

Three guards existed and all three missed it. ``create_transfer`` drift-checks
only same-currency pairs; ``doctor`` exempted cross-currency ones; and
``transfers.validate`` is called from no write path. Run against a copy of
the live ledger, ``confirm_pair`` accepted a 2 261 Bs deposit dated
2025-11-06 paired with a 200.44 USDT sell dated 2026-07-30 — eight months
and a factor of 75 apart. Both rows then left income and expense for good,
silently.

The bounds are deliberately looser than the automatic matcher's ±1 day / 2%:
the manual path exists for the cases the matcher will not take. They catch a
mis-click, not a judgement call.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from finances.db.repos import accounts as accounts_repo
from finances.db.repos import transactions as txn_repo
from finances.domain.models import Account, AccountKind, Transaction, TransactionKind
from finances.web.services.triage import confirm_pair


@pytest.fixture
def pairables(in_memory_db):
    conn = in_memory_db
    spot = accounts_repo.insert(
        conn,
        Account(name="Binance Spot", kind=AccountKind.CRYPTO_SPOT, currency="USDT"),
    )
    bank = accounts_repo.insert(
        conn, Account(name="Provincial", kind=AccountKind.BANK, currency="VES")
    )
    return conn, spot, bank


def _sell(conn, spot, *, when, amount="-100", rate="800", ref="sell"):
    return txn_repo.insert(
        conn,
        Transaction(
            account_id=spot.id,
            occurred_at=when,
            kind=TransactionKind.EXPENSE,
            amount=Decimal(amount),
            currency="USDT",
            description=f"P2P SELL USDT @ {rate} VES (order 1)",
            user_rate=Decimal(rate) if rate else None,
            source="binance",
            source_ref=ref,
        ),
    )


def _deposit(conn, bank, *, when, amount="80000", ref="dep"):
    return txn_repo.insert(
        conn,
        Transaction(
            account_id=bank.id,
            occurred_at=when,
            kind=TransactionKind.INCOME,
            amount=Decimal(amount),
            currency="VES",
            description="ABONO",
            source="provincial",
            source_ref=ref,
        ),
    )


DAY = datetime(2026, 5, 12, tzinfo=UTC)


def test_a_matching_pair_is_accepted(pairables):
    conn, spot, bank = pairables
    sell = _sell(conn, spot, when=DAY)
    dep = _deposit(conn, bank, when=DAY)

    result = confirm_pair(conn, deposit_id=dep.id, sell_id=sell.id)

    assert result["transfer_id"]
    assert txn_repo.get_by_id(conn, sell.id).transfer_id == result["transfer_id"]


def test_a_close_but_imperfect_pair_is_still_accepted(pairables):
    """The manual path exists for what the ±1 day / 2% matcher will not take."""
    conn, spot, bank = pairables
    sell = _sell(conn, spot, when=DAY)
    dep = _deposit(conn, bank, when=datetime(2026, 5, 15, tzinfo=UTC), amount="76000")

    assert confirm_pair(conn, deposit_id=dep.id, sell_id=sell.id)["transfer_id"]


def test_the_live_ledger_mis_click_is_refused(pairables):
    """2,261 Bs vs 200.44 USDT, eight months apart — the proven case."""
    conn, spot, bank = pairables
    sell = _sell(
        conn,
        spot,
        when=datetime(2026, 7, 30, tzinfo=UTC),
        amount="-200.44",
        rate="848.10",
    )
    dep = _deposit(
        conn, bank, when=datetime(2025, 11, 6, tzinfo=UTC), amount="2261"
    )

    with pytest.raises(ValueError, match="refusing to pair"):
        confirm_pair(conn, deposit_id=dep.id, sell_id=sell.id)


def test_a_wrong_amount_on_the_same_day_is_refused(pairables):
    conn, spot, bank = pairables
    sell = _sell(conn, spot, when=DAY)  # worth 80,000 Bs
    dep = _deposit(conn, bank, when=DAY, amount="20000")

    with pytest.raises(ValueError, match="apart"):
        confirm_pair(conn, deposit_id=dep.id, sell_id=sell.id)


def test_a_refused_pair_leaves_both_rows_untouched(pairables):
    conn, spot, bank = pairables
    sell = _sell(conn, spot, when=DAY)
    dep = _deposit(conn, bank, when=DAY, amount="20000")

    with pytest.raises(ValueError):
        confirm_pair(conn, deposit_id=dep.id, sell_id=sell.id)

    after_sell = txn_repo.get_by_id(conn, sell.id)
    after_dep = txn_repo.get_by_id(conn, dep.id)
    assert after_sell.transfer_id is None
    assert after_dep.transfer_id is None
    assert after_sell.kind is TransactionKind.EXPENSE
    assert after_dep.kind is TransactionKind.INCOME


def test_a_sell_without_a_rate_is_judged_on_dates_alone(pairables):
    """Legacy rows carry no user_rate; the manual path is how they get cleared."""
    conn, spot, bank = pairables
    sell = _sell(conn, spot, when=DAY, rate="")
    dep = _deposit(conn, bank, when=DAY, amount="12345")

    assert confirm_pair(conn, deposit_id=dep.id, sell_id=sell.id)["transfer_id"]


def test_a_rateless_sell_far_from_the_deposit_is_still_refused(pairables):
    conn, spot, bank = pairables
    sell = _sell(conn, spot, when=datetime(2026, 7, 30, tzinfo=UTC), rate="")
    dep = _deposit(conn, bank, when=datetime(2025, 11, 6, tzinfo=UTC))

    with pytest.raises(ValueError, match="days apart"):
        confirm_pair(conn, deposit_id=dep.id, sell_id=sell.id)
