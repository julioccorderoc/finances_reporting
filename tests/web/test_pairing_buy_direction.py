"""Pairing a P2P *buy* by hand: bolívars out of the bank, USDT in.

The manual picker only ever opened on outgoing Binance rows — the modal
gated it on ``txn.amount < 0`` — so the two ``P2P BUY USDT @`` rows in the
live ledger had no way to be paired at all: not by the automatic strategy,
not by hand. Each one reads as income the owner never earned.

The scoring half of the picker was already sign-agnostic; what follows
pins the surface: which rows the picker opens on, what it offers, and that
confirming writes one transfer.

Per rule-011 these land before the implementation.
"""

from __future__ import annotations

import sqlite3
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from finances.db.repos import accounts as accounts_repo
from finances.db.repos import transactions as transactions_repo
from finances.domain.models import Account, AccountKind, Transaction, TransactionKind
from finances.web.services.pairing import find_pair_candidates

BUY_AT = datetime(2026, 8, 13, 15, 0, tzinfo=UTC)


@pytest.fixture
def buy_db(web_db: sqlite3.Connection) -> sqlite3.Connection:
    """A P2P buy, the debit that paid for it, and three near misses."""
    provincial = accounts_repo.insert(
        web_db,
        Account(
            name="Provincial",
            kind=AccountKind.BANK,
            currency="VES",
            institution="Provincial",
        ),
    )
    binance = accounts_repo.insert(
        web_db,
        Account(
            name="Binance Spot",
            kind=AccountKind.CRYPTO_SPOT,
            currency="USDT",
            institution="Binance",
        ),
    )

    # 45.09 USDT at 887 VES/USDT → 39 994.83 VES expected out of the bank.
    transactions_repo.insert(
        web_db,
        Transaction(
            account_id=binance.id,
            occurred_at=BUY_AT,
            kind=TransactionKind.INCOME,
            amount=Decimal("45.09"),
            currency="USDT",
            description="P2P BUY USDT @ 887 VES (order 22921295)",
            user_rate=Decimal("887"),
            source="binance",
            source_ref="p2p-buy-1",
        ),
    )
    # A Binance credit with no rate: an ordinary deposit, not a P2P trade.
    transactions_repo.insert(
        web_db,
        Transaction(
            account_id=binance.id,
            occurred_at=BUY_AT,
            kind=TransactionKind.INCOME,
            amount=Decimal("90"),
            currency="USDC",
            description="Binance deposit USDC",
            source="binance",
            source_ref="usdc-deposit-1",
        ),
    )

    rows = [
        ("debit-exact", BUY_AT, TransactionKind.EXPENSE, Decimal("-39995.00")),
        ("debit-far", BUY_AT, TransactionKind.EXPENSE, Decimal("-1802.00")),
        ("dep-wrong-side", BUY_AT, TransactionKind.INCOME, Decimal("39995.00")),
        (
            "debit-too-old",
            BUY_AT - timedelta(days=5),
            TransactionKind.EXPENSE,
            Decimal("-39995.00"),
        ),
    ]
    for ref, when, kind, amount in rows:
        transactions_repo.insert(
            web_db,
            Transaction(
                account_id=provincial.id,
                occurred_at=when,
                kind=kind,
                amount=amount,
                currency="VES",
                description=ref,
                source="provincial",
                source_ref=ref,
            ),
        )
    return web_db


def _txn_id(conn: sqlite3.Connection, source_ref: str) -> int:
    row = conn.execute(
        "SELECT id FROM transactions WHERE source_ref = ?", (source_ref,)
    ).fetchone()
    assert row is not None, f"missing fixture row {source_ref!r}"
    return int(row["id"])


# ---------------------------------------------------------------------------
# Scoring
# ---------------------------------------------------------------------------


def test_the_debit_that_paid_for_the_buy_is_offered_first(
    buy_db: sqlite3.Connection,
) -> None:
    result = find_pair_candidates(buy_db, sell_id=_txn_id(buy_db, "p2p-buy-1"))

    assert result.expected_ves == Decimal("45.09") * Decimal("887")
    assert [c.card.description for c in result.candidates][0] == "debit-exact"


def test_a_deposit_cannot_be_the_other_leg_of_a_buy(
    buy_db: sqlite3.Connection,
) -> None:
    """Both legs arriving is not a transfer — the same guard the sell
    direction applies to expenses, read the other way round."""
    result = find_pair_candidates(buy_db, sell_id=_txn_id(buy_db, "p2p-buy-1"))

    by_ref = {c.card.description: c for c in result.candidates}
    assert by_ref["debit-exact"].pairable is True
    assert by_ref["dep-wrong-side"].pairable is False


# ---------------------------------------------------------------------------
# The surface
# ---------------------------------------------------------------------------


def test_the_modal_opens_the_picker_on_a_buy(
    buy_db: sqlite3.Connection, web_client_factory
) -> None:
    buy_id = _txn_id(buy_db, "p2p-buy-1")
    client = web_client_factory()

    resp = client.get(f"/_partial/transactions/{buy_id}/modal")

    assert resp.status_code == 200, resp.text
    assert f"/_partial/transactions/{buy_id}/pair-candidates" in resp.text


def test_the_modal_names_the_bank_side_it_is_looking_for(
    buy_db: sqlite3.Connection, web_client_factory
) -> None:
    """"Pair with deposit" over a list of debits is a lie the owner has to
    decode mid-decision."""
    buy_id = _txn_id(buy_db, "p2p-buy-1")
    sell_like = _txn_id(buy_db, "debit-exact")
    client = web_client_factory()

    buy_modal = client.get(f"/_partial/transactions/{buy_id}/modal").text

    assert "Pair with debit" in buy_modal
    assert "Pair with deposit" not in buy_modal
    assert sell_like  # the bank row itself never offers the picker
    assert (
        "pair-candidates"
        not in client.get(f"/_partial/transactions/{sell_like}/modal").text
    )


def test_an_ordinary_binance_credit_is_not_offered_the_picker(
    buy_db: sqlite3.Connection, web_client_factory
) -> None:
    """A USDC deposit is money arriving, not a trade against the bank.
    Without a rate there is nothing to score and nothing to pair."""
    deposit_id = _txn_id(buy_db, "usdc-deposit-1")
    client = web_client_factory()

    resp = client.get(f"/_partial/transactions/{deposit_id}/modal")

    assert resp.status_code == 200, resp.text
    assert "pair-candidates" not in resp.text


def test_the_candidates_partial_lists_debits_for_a_buy(
    buy_db: sqlite3.Connection, web_client_factory
) -> None:
    buy_id = _txn_id(buy_db, "p2p-buy-1")
    client = web_client_factory()

    resp = client.get(f"/_partial/transactions/{buy_id}/pair-candidates")

    assert resp.status_code == 200, resp.text
    assert "debit-exact" in resp.text
    assert "debit-too-old" not in resp.text


def test_confirming_a_buy_pair_writes_one_transfer(
    buy_db: sqlite3.Connection, web_client_factory
) -> None:
    buy_id = _txn_id(buy_db, "p2p-buy-1")
    debit_id = _txn_id(buy_db, "debit-exact")
    client = web_client_factory()

    resp = client.post(f"/_partial/transactions/{buy_id}/pair/{debit_id}")

    assert resp.status_code == 200, resp.text
    rows = buy_db.execute(
        "SELECT transfer_id, kind FROM transactions WHERE id IN (?, ?)",
        (buy_id, debit_id),
    ).fetchall()
    transfer_ids = {row["transfer_id"] for row in rows}
    assert len(transfer_ids) == 1 and None not in transfer_ids
    assert {row["kind"] for row in rows} == {"transfer"}
