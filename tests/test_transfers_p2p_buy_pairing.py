"""The other half of a P2P trade: bolívars out, USDT in.

``BankAnchoredP2pPairing`` only ever paired a bank *deposit* with a
Binance *sell* — the direction the owner uses most. Buying USDT is the
same movement of money read backwards: the bank is debited, the exchange
credited, and the two rows are one transfer (rule-002), not an expense
plus income out of nowhere.

The live ledger has two such rows, both unpaired and both filed income
with no category: ``P2P BUY USDT @ 887 VES`` (2026-08-13) and
``P2P BUY USDT @ 305 VES`` (2025-10-21). Left alone, each one inflates a
month's income by the dollars bought.

Per rule-011 these land before the implementation.
"""

from __future__ import annotations

import sqlite3
from datetime import timedelta
from decimal import Decimal

from finances.db.repos import transactions as txn_repo
from finances.domain.models import TransactionKind
from finances.domain.transfers import BankAnchoredP2pPairing, validate

from tests.test_transfers import (  # noqa: F401  (fixture + helper reuse)
    FIXED_AT,
    _account_id,
    _insert_expense_row,
    _insert_income_row,
)


def _buy(
    conn: sqlite3.Connection,
    *,
    account_id: int,
    amount: str,
    rate: str,
    fiat: str = "VES",
    when=FIXED_AT,
):
    """A Binance P2P buy: USDT in, priced in ``fiat`` per USDT."""
    return _insert_income_row(
        conn,
        account_id=account_id,
        amount=Decimal(amount),
        currency="USDT",
        source="binance",
        occurred_at=when,
        user_rate=Decimal(rate),
        description=f"P2P BUY USDT @ {rate} {fiat} (order 22921295)",
    )


def _debit(
    conn: sqlite3.Connection,
    *,
    account_id: int,
    amount: str,
    when=FIXED_AT,
):
    """The bolívars leaving the bank to pay for that buy."""
    return _insert_expense_row(
        conn,
        account_id=account_id,
        amount=Decimal(amount),
        currency="VES",
        source="provincial",
        occurred_at=when,
    )


def test_a_bank_debit_is_paired_with_the_binance_buy(
    seeded_db: sqlite3.Connection,
) -> None:
    provincial = _account_id(seeded_db, "Provincial Bolivares")
    spot = _account_id(seeded_db, "Binance Spot")

    bank = _debit(seeded_db, account_id=provincial, amount="-39995")
    buy = _buy(seeded_db, account_id=spot, amount="45.09", rate="887")

    proposals = BankAnchoredP2pPairing(seeded_db).match()

    assert len(proposals) == 1
    assert proposals[0].details["bank_transaction_id"] == bank.id
    assert proposals[0].details["binance_transaction_id"] == buy.id


def test_applying_a_buy_proposal_makes_one_valid_transfer(
    seeded_db: sqlite3.Connection,
) -> None:
    provincial = _account_id(seeded_db, "Provincial Bolivares")
    spot = _account_id(seeded_db, "Binance Spot")

    bank = _debit(seeded_db, account_id=provincial, amount="-39995")
    buy = _buy(seeded_db, account_id=spot, amount="45.09", rate="887")

    strat = BankAnchoredP2pPairing(seeded_db)
    strat.apply(strat.match()[0])

    bank_row = txn_repo.get_by_id(seeded_db, bank.id)
    buy_row = txn_repo.get_by_id(seeded_db, buy.id)
    assert bank_row is not None and buy_row is not None
    assert bank_row.kind is TransactionKind.TRANSFER
    assert buy_row.kind is TransactionKind.TRANSFER
    assert bank_row.transfer_id is not None
    assert bank_row.transfer_id == buy_row.transfer_id
    assert validate(seeded_db, bank_row.transfer_id) is True


def test_a_proposal_says_which_way_the_money_went(
    seeded_db: sqlite3.Connection,
) -> None:
    """A reconciliation report that cannot tell a buy from a sell reads as
    if the pairer did the same thing twice."""
    provincial = _account_id(seeded_db, "Provincial Bolivares")
    spot = _account_id(seeded_db, "Binance Spot")

    _debit(seeded_db, account_id=provincial, amount="-39995")
    _buy(seeded_db, account_id=spot, amount="45.09", rate="887")

    assert BankAnchoredP2pPairing(seeded_db).match()[0].details["direction"] == "buy"


def test_a_deposit_still_pairs_with_a_sell_and_says_so(
    seeded_db: sqlite3.Connection,
) -> None:
    provincial = _account_id(seeded_db, "Provincial Bolivares")
    spot = _account_id(seeded_db, "Binance Spot")

    _insert_income_row(
        seeded_db,
        account_id=provincial,
        amount=Decimal("12000"),
        currency="VES",
        source="provincial",
    )
    _insert_expense_row(
        seeded_db,
        account_id=spot,
        amount=Decimal("-10"),
        currency="USDT",
        source="binance",
        user_rate=Decimal("1200"),
        description="P2P SELL USDT @ 1200 VES (order 22886333)",
    )

    proposals = BankAnchoredP2pPairing(seeded_db).match()

    assert len(proposals) == 1
    assert proposals[0].details["direction"] == "sell"


def test_a_debit_is_never_paired_with_a_sell(seeded_db: sqlite3.Connection) -> None:
    """Both legs leaving is not a transfer, whatever the amounts say."""
    provincial = _account_id(seeded_db, "Provincial Bolivares")
    spot = _account_id(seeded_db, "Binance Spot")

    _debit(seeded_db, account_id=provincial, amount="-12000")
    _insert_expense_row(
        seeded_db,
        account_id=spot,
        amount=Decimal("-10"),
        currency="USDT",
        source="binance",
        user_rate=Decimal("1200"),
        description="P2P SELL USDT @ 1200 VES (order 22886333)",
    )

    assert BankAnchoredP2pPairing(seeded_db).match() == []


def test_a_deposit_is_never_paired_with_a_buy(seeded_db: sqlite3.Connection) -> None:
    provincial = _account_id(seeded_db, "Provincial Bolivares")
    spot = _account_id(seeded_db, "Binance Spot")

    _insert_income_row(
        seeded_db,
        account_id=provincial,
        amount=Decimal("12000"),
        currency="VES",
        source="provincial",
    )
    _buy(seeded_db, account_id=spot, amount="10", rate="1200")

    assert BankAnchoredP2pPairing(seeded_db).match() == []


def test_the_buy_side_respects_the_amount_tolerance(
    seeded_db: sqlite3.Connection,
) -> None:
    provincial = _account_id(seeded_db, "Provincial Bolivares")
    spot = _account_id(seeded_db, "Binance Spot")

    _debit(seeded_db, account_id=provincial, amount="-25000")
    _buy(seeded_db, account_id=spot, amount="45.09", rate="887")

    assert BankAnchoredP2pPairing(seeded_db).match() == []


def test_the_buy_side_respects_the_window(seeded_db: sqlite3.Connection) -> None:
    provincial = _account_id(seeded_db, "Provincial Bolivares")
    spot = _account_id(seeded_db, "Binance Spot")

    _debit(seeded_db, account_id=provincial, amount="-39995")
    _buy(
        seeded_db,
        account_id=spot,
        amount="45.09",
        rate="887",
        when=FIXED_AT + timedelta(days=3),
    )

    assert BankAnchoredP2pPairing(seeded_db, window_days=2).match() == []


def test_a_dollar_priced_buy_never_anchors_against_bolivars(
    seeded_db: sqlite3.Connection,
) -> None:
    """The fiat lives only in the description; ``user_rate`` is a bare
    number, so without reading it back a USD-priced buy converts to a
    plausible-looking bolívar figure (the guard the sell side already has).
    """
    provincial = _account_id(seeded_db, "Provincial Bolivares")
    spot = _account_id(seeded_db, "Binance Spot")

    _debit(seeded_db, account_id=provincial, amount="-39995")
    _buy(seeded_db, account_id=spot, amount="45.09", rate="887", fiat="USD")

    assert BankAnchoredP2pPairing(seeded_db).match() == []


def test_both_directions_claim_each_row_at_most_once(
    seeded_db: sqlite3.Connection,
) -> None:
    """A day with a sell and a buy on it produces two disjoint pairs."""
    provincial = _account_id(seeded_db, "Provincial Bolivares")
    spot = _account_id(seeded_db, "Binance Spot")

    deposit = _insert_income_row(
        seeded_db,
        account_id=provincial,
        amount=Decimal("12000"),
        currency="VES",
        source="provincial",
    )
    sell = _insert_expense_row(
        seeded_db,
        account_id=spot,
        amount=Decimal("-10"),
        currency="USDT",
        source="binance",
        user_rate=Decimal("1200"),
        description="P2P SELL USDT @ 1200 VES (order 22886333)",
    )
    debit = _debit(seeded_db, account_id=provincial, amount="-39995")
    buy = _buy(seeded_db, account_id=spot, amount="45.09", rate="887")

    proposals = BankAnchoredP2pPairing(seeded_db).match()

    assert len(proposals) == 2
    banks = {p.details["bank_transaction_id"] for p in proposals}
    exchange = {p.details["binance_transaction_id"] for p in proposals}
    assert banks == {deposit.id, debit.id}
    assert exchange == {sell.id, buy.id}
