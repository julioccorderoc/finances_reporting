"""Changing money into dollar bills — the ledger shape of "this became cash".

Four of these exist in two years. Rows 859/5740 and 863/5741 are the 2025
pair; this module is how the next one gets recorded without a script.

The shape is fixed by what is already in the ledger: a double-entry transfer
(rule-002), the outgoing row promoted to a leg, a ``Cash USD`` leg carrying
the dollars actually received, and — when the two legs are in different
currencies — a ``user_rate`` on the outgoing row holding the price the
exchange was struck at (ADR-015), which is what makes ``transfers.validate``
see the pair sum to zero.

Why not ``create_transfer``'s anchor-only mode: it copies ONE amount to both
legs, which is right for a same-currency move and silently wrong for a
conversion, where 36,000 bolívares become 40 dollars.
"""

from __future__ import annotations

import sqlite3
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from finances.db.repos import transactions as transactions_repo
from finances.domain import cash_conversion, transfers
from finances.domain.models import Transaction, TransactionKind


def _cash_account_id(conn: sqlite3.Connection) -> int:
    """Looked up, never hardcoded.

    Migration 020 seeds two bank accounts before the ``seeded_db`` fixture
    adds the v1 five, so ``Cash USD`` is id 7 here and id 5 in the live
    ledger. A test that pins the number tests the seed order.
    """
    return cash_conversion.cash_account_id(conn)


def _outgoing(
    conn: sqlite3.Connection,
    *,
    account_id: int = 2,
    amount: str = "-580",
    currency: str = "USDT",
    source: str = "binance",
    source_ref: str = "pay:448771282321014784",
    description: str = "Binance Pay C2C (outgoing)",
    kind: TransactionKind = TransactionKind.EXPENSE,
) -> int:
    stored = transactions_repo.insert(
        conn,
        Transaction(
            account_id=account_id,
            occurred_at=datetime(2026, 8, 15, 16, 43, tzinfo=UTC),
            kind=kind,
            amount=Decimal(amount),
            currency=currency,
            description=description,
            source=source,
            source_ref=source_ref,
            needs_review=True,
        ),
    )
    assert stored.id is not None
    return stored.id


def _legs(conn: sqlite3.Connection, transfer_id: str) -> list[Transaction]:
    ids = [
        int(r["id"])
        for r in conn.execute(
            "SELECT id FROM transactions WHERE transfer_id = ? ORDER BY id",
            (transfer_id,),
        )
    ]
    out = [transactions_repo.get_by_id(conn, i) for i in ids]
    return [t for t in out if t is not None]


# ---------------------------------------------------------------------------
# The same-currency case: 580 USDT out, $580 in hand
# ---------------------------------------------------------------------------


def test_it_pairs_the_outgoing_row_with_a_cash_leg(
    seeded_db: sqlite3.Connection,
) -> None:
    anchor_id = _outgoing(seeded_db)

    result = cash_conversion.convert_to_cash(
        seeded_db, transaction_id=anchor_id, usd_received=Decimal("580")
    )

    legs = _legs(seeded_db, result.transfer_id)
    assert [leg.id for leg in legs] == sorted([anchor_id, result.cash_transaction_id])
    assert {leg.kind for leg in legs} == {TransactionKind.TRANSFER}


def test_the_cash_leg_lands_in_cash_usd_as_dollars(
    seeded_db: sqlite3.Connection,
) -> None:
    anchor_id = _outgoing(seeded_db)

    result = cash_conversion.convert_to_cash(
        seeded_db, transaction_id=anchor_id, usd_received=Decimal("580")
    )

    cash = transactions_repo.get_by_id(seeded_db, result.cash_transaction_id)
    assert cash is not None
    assert cash.account_id == _cash_account_id(seeded_db)
    assert cash.amount == Decimal("580")
    assert cash.currency == "USD"


def test_the_cash_leg_carries_a_deterministic_ref(
    seeded_db: sqlite3.Connection,
) -> None:
    """rule-010: a stable external id where one exists, derived from the anchor."""
    anchor_id = _outgoing(seeded_db)

    result = cash_conversion.convert_to_cash(
        seeded_db, transaction_id=anchor_id, usd_received=Decimal("580")
    )

    cash = transactions_repo.get_by_id(seeded_db, result.cash_transaction_id)
    assert cash is not None
    assert cash.source == "internal"
    assert cash.source_ref == f"cash:binance:{anchor_id}"


def test_no_rate_is_invented_when_the_legs_already_agree(
    seeded_db: sqlite3.Connection,
) -> None:
    """580 USDT for $580 is 1:1 (ADR-015). Rows 859/863 carry no rate either."""
    anchor_id = _outgoing(seeded_db)

    cash_conversion.convert_to_cash(
        seeded_db, transaction_id=anchor_id, usd_received=Decimal("580")
    )

    anchor = transactions_repo.get_by_id(seeded_db, anchor_id)
    assert anchor is not None
    assert anchor.user_rate is None


# ---------------------------------------------------------------------------
# The cross-currency case: 36,000 Bs out, $40 in hand
# ---------------------------------------------------------------------------


def test_it_sets_the_struck_rate_on_a_foreign_currency_row(
    seeded_db: sqlite3.Connection,
) -> None:
    anchor_id = _outgoing(
        seeded_db,
        account_id=1,
        amount="-36000",
        currency="VES",
        source="provincial",
        source_ref="hash:52809099a320229b",
        description="DR OB V07372929 191NAC.C",
    )

    cash_conversion.convert_to_cash(
        seeded_db, transaction_id=anchor_id, usd_received=Decimal("40")
    )

    anchor = transactions_repo.get_by_id(seeded_db, anchor_id)
    assert anchor is not None
    assert anchor.user_rate == Decimal("900.0000")


def test_the_cross_currency_pair_validates(seeded_db: sqlite3.Connection) -> None:
    """rule-002: the legs must sum to zero once the struck rate prices them."""
    anchor_id = _outgoing(
        seeded_db,
        account_id=1,
        amount="-36000",
        currency="VES",
        source="provincial",
        source_ref="hash:52809099a320229b",
        description="DR OB V07372929 191NAC.C",
    )

    result = cash_conversion.convert_to_cash(
        seeded_db, transaction_id=anchor_id, usd_received=Decimal("40")
    )

    assert transfers.validate(seeded_db, result.transfer_id) is True


def test_a_fee_on_a_dollar_move_still_gets_a_rate(
    seeded_db: sqlite3.Connection,
) -> None:
    """580 USDT out but only $575 in hand is not 1:1, whatever the currency."""
    anchor_id = _outgoing(seeded_db)

    cash_conversion.convert_to_cash(
        seeded_db, transaction_id=anchor_id, usd_received=Decimal("575")
    )

    anchor = transactions_repo.get_by_id(seeded_db, anchor_id)
    assert anchor is not None
    assert anchor.user_rate == Decimal("1.0087")


# ---------------------------------------------------------------------------
# What it refuses
# ---------------------------------------------------------------------------


def test_it_refuses_a_row_that_is_already_paired(
    seeded_db: sqlite3.Connection,
) -> None:
    anchor_id = _outgoing(seeded_db)
    cash_conversion.convert_to_cash(
        seeded_db, transaction_id=anchor_id, usd_received=Decimal("580")
    )

    with pytest.raises(ValueError, match="already half of a transfer"):
        cash_conversion.convert_to_cash(
            seeded_db, transaction_id=anchor_id, usd_received=Decimal("580")
        )


def test_it_refuses_money_coming_in(seeded_db: sqlite3.Connection) -> None:
    """A conversion spends the outgoing side. An income row is the other half."""
    anchor_id = _outgoing(
        seeded_db, amount="580", kind=TransactionKind.INCOME, source_ref="pay:in"
    )

    with pytest.raises(ValueError, match="money leaving"):
        cash_conversion.convert_to_cash(
            seeded_db, transaction_id=anchor_id, usd_received=Decimal("580")
        )


def test_it_refuses_a_row_already_in_the_cash_account(
    seeded_db: sqlite3.Connection,
) -> None:
    """Cash into cash moves nothing, and would pair the account with itself."""
    anchor_id = _outgoing(
        seeded_db,
        account_id=_cash_account_id(seeded_db),
        currency="USD",
        source="cash_cli",
        source_ref="cash-1",
        description="lunch",
    )

    with pytest.raises(ValueError, match="already cash"):
        cash_conversion.convert_to_cash(
            seeded_db, transaction_id=anchor_id, usd_received=Decimal("580")
        )


def test_it_refuses_a_reconciliation_row(seeded_db: sqlite3.Connection) -> None:
    """ADR-018 plugs are the ledger's own correction (ADR-022 §2.3)."""
    anchor_id = _outgoing(
        seeded_db,
        source="reconciliation",
        source_ref="reconcile:2:USDT:abc",
        description="adjustment",
    )

    with pytest.raises(ValueError, match="the ledger's own correction"):
        cash_conversion.convert_to_cash(
            seeded_db, transaction_id=anchor_id, usd_received=Decimal("580")
        )


@pytest.mark.parametrize("bad", ["0", "-40"])
def test_it_refuses_a_non_positive_amount(
    seeded_db: sqlite3.Connection, bad: str
) -> None:
    anchor_id = _outgoing(seeded_db)

    with pytest.raises(ValueError, match="dollars received must be positive"):
        cash_conversion.convert_to_cash(
            seeded_db, transaction_id=anchor_id, usd_received=Decimal(bad)
        )


def test_it_refuses_a_row_that_does_not_exist(seeded_db: sqlite3.Connection) -> None:
    with pytest.raises(LookupError):
        cash_conversion.convert_to_cash(
            seeded_db, transaction_id=99999, usd_received=Decimal("40")
        )


# ---------------------------------------------------------------------------
# It composes with unpair — the undo the whole feature depends on
# ---------------------------------------------------------------------------


def test_a_conversion_can_be_unpaired_and_the_cash_leg_deleted(
    seeded_db: sqlite3.Connection,
) -> None:
    """Typing 4000 instead of 40 has to be recoverable through the viewer."""
    anchor_id = _outgoing(
        seeded_db,
        account_id=1,
        amount="-36000",
        currency="VES",
        source="provincial",
        source_ref="hash:52809099a320229b",
    )
    result = cash_conversion.convert_to_cash(
        seeded_db, transaction_id=anchor_id, usd_received=Decimal("4000")
    )

    transfers.unpair(seeded_db, transfer_id=result.transfer_id)
    transactions_repo.delete(seeded_db, result.cash_transaction_id)

    anchor = transactions_repo.get_by_id(seeded_db, anchor_id)
    assert anchor is not None
    assert anchor.kind is TransactionKind.EXPENSE
    assert anchor.transfer_id is None
    assert transactions_repo.get_by_id(seeded_db, result.cash_transaction_id) is None
