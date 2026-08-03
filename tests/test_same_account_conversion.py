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
from finances.domain.reconciliation import run_reconciliation_pass
from finances.domain.transfers import SameAccountConvertPairing, create_transfer
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


# ---------------------------------------------------------------------------
# Legacy halves: the two conversions whose legs never shared an order id
# ---------------------------------------------------------------------------
#
# The legacy backfill hashed each leg of a conversion separately, so 891/892
# and 910/911 on the live ledger carry four different order keys for two real
# events. The order-id join can never match them, and it never will: rewriting
# source_ref would break the dedup key (rule-010).
#
# They are still recoverable. A same-day, same-account pair of opposite sign
# in two different currencies, agreeing to within 2%, is one conversion —
# there is no second reading of it. Where several candidates do compete the
# pass claims the tightest fit first and consumes each leg once, the same
# discipline ADR-002's 2026-07-26 amendment adopted for P2P: it asserts that N
# outgoing legs consumed N incoming ones, not that any individual pair is the
# true counterparty.


def _legacy_halves(
    conn,
    account,
    *,
    out_amount: str,
    out_currency: str,
    in_amount: str,
    in_currency: str,
    tag: str,
    when: datetime = WHEN,
):
    """Two convert legs with *different* hashed order keys, as backfill wrote."""
    txn_repo.insert(
        conn,
        _leg(
            account,
            amount=str(abs(Decimal(out_amount))),
            currency=out_currency,
            kind=TransactionKind.INCOME,
            ref=f"deposit:{tag}",
        ).model_copy(update={"description": "salary", "occurred_at": when}),
    )
    out = txn_repo.insert(
        conn,
        _leg(
            account,
            amount=out_amount,
            currency=out_currency,
            kind=TransactionKind.EXPENSE,
            ref=f"convert:hash:{tag}aaaa:from",
        ).model_copy(update={"occurred_at": when}),
    )
    inn = txn_repo.insert(
        conn,
        _leg(
            account,
            amount=in_amount,
            currency=in_currency,
            kind=TransactionKind.INCOME,
            ref=f"convert:hash:{tag}bbbb:to",
        ).model_copy(update={"occurred_at": when}),
    )
    return out, inn


def test_legacy_halves_are_paired_despite_mismatched_order_keys(spot):
    conn, account = spot
    out, inn = _legacy_halves(
        conn,
        account,
        out_amount="-1240.00",
        out_currency="USDC",
        in_amount="1239.18",
        in_currency="USDT",
        tag="nov23",
    )

    report = run_reconciliation_pass(SameAccountConvertPairing(conn))

    assert report.proposals_applied == 1
    assert report.errors == []
    paired_out = txn_repo.get_by_id(conn, out.id)
    paired_in = txn_repo.get_by_id(conn, inn.id)
    assert paired_out.kind is TransactionKind.TRANSFER
    assert paired_out.transfer_id is not None
    assert paired_out.transfer_id == paired_in.transfer_id


def test_legacy_pairing_refuses_a_different_day(spot):
    conn, account = spot
    _legacy_halves(
        conn,
        account,
        out_amount="-1240.00",
        out_currency="USDC",
        in_amount="1239.18",
        in_currency="USDT",
        tag="split",
    )
    conn.execute(
        "UPDATE transactions SET occurred_at = ? "
        "WHERE source_ref = 'convert:hash:splitbbbb:to'",
        (datetime(2026, 5, 20, tzinfo=UTC).isoformat(),),
    )

    report = run_reconciliation_pass(SameAccountConvertPairing(conn))

    assert report.proposals_found == 0


def test_legacy_pairing_refuses_amounts_that_disagree(spot):
    """A 50% gap is not a conversion; it is two unrelated events."""
    conn, account = spot
    _legacy_halves(
        conn,
        account,
        out_amount="-1000.00",
        out_currency="USDC",
        in_amount="500.00",
        in_currency="USDT",
        tag="drift",
    )

    report = run_reconciliation_pass(SameAccountConvertPairing(conn))

    assert report.proposals_found == 0


def test_legacy_pairing_refuses_the_same_currency(spot):
    """Same account, same currency is the shape that moves nothing."""
    conn, account = spot
    _legacy_halves(
        conn,
        account,
        out_amount="-500.00",
        out_currency="USDT",
        in_amount="500.00",
        in_currency="USDT",
        tag="samecur",
    )

    report = run_reconciliation_pass(SameAccountConvertPairing(conn))

    assert report.proposals_found == 0


def test_legacy_pairing_leaves_a_lone_leg_alone(spot):
    conn, account = spot
    lone = txn_repo.insert(
        conn,
        _leg(
            account,
            amount="-321.00",
            currency="USDC",
            kind=TransactionKind.EXPENSE,
            ref="convert:hash:orphan:from",
        ),
    )

    report = run_reconciliation_pass(SameAccountConvertPairing(conn))

    assert report.proposals_found == 0
    assert txn_repo.get_by_id(conn, lone.id).transfer_id is None


def test_pairing_stays_idempotent_with_legacy_halves_present(spot):
    conn, account = spot
    _legacy_halves(
        conn,
        account,
        out_amount="-1240.00",
        out_currency="USDC",
        in_amount="1239.18",
        in_currency="USDT",
        tag="idem",
    )

    first = run_reconciliation_pass(SameAccountConvertPairing(conn))
    second = run_reconciliation_pass(SameAccountConvertPairing(conn))

    assert first.proposals_applied == 1
    assert second.proposals_found == 0


def test_doctor_stops_naming_legacy_halves_once_they_are_paired(spot):
    """Pairing answers the question convert_leg_without_counterpart asks.

    The check recovers an order id from source_ref, which pairing does not
    rewrite and must not (rule-010). So a repaired legacy pair keeps its
    mismatched keys forever. What the check actually asks is whether a lone
    leg reads as money spent — and a leg carrying a transfer_id does not,
    because reports exclude it by kind.
    """
    conn, account = spot
    _legacy_halves(
        conn,
        account,
        out_amount="-1240.00",
        out_currency="USDC",
        in_amount="1239.18",
        in_currency="USDT",
        tag="doc",
    )

    before = {f.check for f in integrity.run_checks(conn).findings}
    assert "convert_leg_without_counterpart" in before

    run_reconciliation_pass(SameAccountConvertPairing(conn))

    after = {f.check for f in integrity.run_checks(conn).findings}
    assert "convert_leg_without_counterpart" not in after
