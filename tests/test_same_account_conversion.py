"""A currency conversion inside one account is a transfer, not two events.

Converting USDC to USDT inside Binance Spot moves value between two
positions the owner holds. ``create_transfer`` refused it because both legs
sit on one ``account_id``, so the ingest wrote an expense row *and* an
income row and every report counted both: about $11,846 of phantom expense
against $11,845 of phantom income, netting the ~$1.81 the conversion
actually cost.

The old rule — "both legs of a transfer on one account is an error" — was
right about what it was protecting against and wrong about how it said it.
Nothing moves when USDT leaves and re-enters the same account. Something
very much moves when USDC leaves and USDT arrives.

The invariant is therefore restated in terms of *positions* rather than
accounts: **a transfer moves value between two distinct (account, currency)
pairs.** Same account with the same currency is still refused; same account
with different currencies is a conversion, which is what this ledger has
been unable to express.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from finances.db.repos import accounts as accounts_repo
from finances.db.repos import transactions as txn_repo
from finances.domain import integrity
from finances.domain.models import Account, AccountKind, Transaction, TransactionKind
from finances.domain.transfers import create_transfer
from finances.reports import consolidated_usd

WHEN = datetime(2026, 5, 12, tzinfo=UTC)


@pytest.fixture
def spot(in_memory_db):
    conn = in_memory_db
    account = accounts_repo.insert(
        conn,
        Account(name="Binance Spot", kind=AccountKind.CRYPTO_SPOT, currency="USDT"),
    )
    return conn, account


def _leg(account, *, amount, currency, kind, ref):
    return Transaction(
        account_id=account.id,
        occurred_at=WHEN,
        kind=kind,
        amount=Decimal(amount),
        currency=currency,
        description="Convert 500 USDC → 500.4 USDT (order 1)",
        source="binance",
        source_ref=ref,
    )


def _convert(conn, account):
    # Fund the USDC position first: converting 500 USDC you never received
    # leaves a negative asset balance, which doctor now (correctly) calls an
    # error, and these tests are about the conversion rule.
    txn_repo.insert(
        conn,
        _leg(
            account,
            amount="500",
            currency="USDC",
            kind=TransactionKind.INCOME,
            ref="deposit:1",
        ).model_copy(update={"description": "salary"}),
    )
    out = txn_repo.insert(
        conn,
        _leg(
            account,
            amount="-500",
            currency="USDC",
            kind=TransactionKind.EXPENSE,
            ref="convert:1:from",
        ),
    )
    inn = txn_repo.insert(
        conn,
        _leg(
            account,
            amount="500.40",
            currency="USDT",
            kind=TransactionKind.INCOME,
            ref="convert:1:to",
        ),
    )
    return out, inn


def test_a_cross_currency_pair_on_one_account_is_allowed(spot):
    conn, account = spot
    out, inn = _convert(conn, account)

    pair = create_transfer(
        conn, anchor_transaction_id=out.id, counterpart_transaction_id=inn.id
    )

    assert pair.from_transaction_id == out.id
    assert pair.to_transaction_id == inn.id
    for txn_id in (out.id, inn.id):
        row = txn_repo.get_by_id(conn, txn_id)
        assert row.kind is TransactionKind.TRANSFER
        assert row.transfer_id == pair.transfer_id


def test_a_same_currency_pair_on_one_account_is_still_refused(spot):
    """Nothing moves when USDT leaves and re-enters the same account."""
    conn, account = spot
    out = txn_repo.insert(
        conn,
        _leg(
            account,
            amount="-500",
            currency="USDT",
            kind=TransactionKind.EXPENSE,
            ref="a",
        ),
    )
    inn = txn_repo.insert(
        conn,
        _leg(
            account,
            amount="500",
            currency="USDT",
            kind=TransactionKind.INCOME,
            ref="b",
        ),
    )

    with pytest.raises(ValueError, match="same account"):
        create_transfer(
            conn, anchor_transaction_id=out.id, counterpart_transaction_id=inn.id
        )


def test_a_paired_conversion_leaves_income_and_expense(spot):
    """The whole point: it stops being counted as spending and earning."""
    conn, account = spot
    out, inn = _convert(conn, account)
    before = consolidated_usd.build_report(conn)
    assert len(before.rows) == 3  # the funding deposit + both convert legs

    create_transfer(
        conn, anchor_transaction_id=out.id, counterpart_transaction_id=inn.id
    )

    after = consolidated_usd.build_report(conn)
    assert [r.transaction_id for r in after.rows] == [out.id - 1]
    assert after.total_usd == Decimal("500")


def test_doctor_no_longer_calls_a_conversion_an_error(spot):
    conn, account = spot
    out, inn = _convert(conn, account)
    create_transfer(
        conn, anchor_transaction_id=out.id, counterpart_transaction_id=inn.id
    )

    report = integrity.run_checks(conn)
    names = {f.check for f in report.findings}

    assert "transfer_legs_same_account" not in names
    assert report.ok


def test_doctor_still_catches_a_same_currency_self_transfer(spot):
    """The invariant the old check protected has not been given up."""
    conn, account = spot
    for i, amount in enumerate(("-500", "500")):
        txn_repo.insert(
            conn,
            _leg(
                account,
                amount=amount,
                currency="USDT",
                kind=TransactionKind.TRANSFER,
                ref=f"self-{i}",
            ).model_copy(update={"transfer_id": "self"}),
        )

    report = integrity.run_checks(conn)

    assert "transfer_legs_same_account" in {f.check for f in report.findings}


def test_the_conversion_is_priced_and_nets_to_about_zero(spot):
    """USDC and USDT are both dollars, so the pair must not leak value."""
    conn, account = spot
    out, inn = _convert(conn, account)
    create_transfer(
        conn, anchor_transaction_id=out.id, counterpart_transaction_id=inn.id
    )

    report = integrity.run_checks(conn)

    assert "transfer_usd_imbalance" not in {f.check for f in report.findings}
