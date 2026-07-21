"""Candidate finder for manual P2P pairing (sell → bank deposit).

Covers: which rows qualify, how drift is scored, ordering, and the
same-sign guard that create_transfer would otherwise reject.

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

SELL_AT = datetime(2026, 5, 10, 15, 0, tzinfo=UTC)


@pytest.fixture
def pairing_db(web_db: sqlite3.Connection) -> sqlite3.Connection:
    """A sell plus five Provincial rows spanning every candidate branch."""
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

    # The sell: 30.83 USDT at 648.65 VES/USDT → 19 997.88 VES expected.
    transactions_repo.insert(
        web_db,
        Transaction(
            account_id=binance.id,
            occurred_at=SELL_AT,
            kind=TransactionKind.EXPENSE,
            amount=Decimal("-30.83"),
            currency="USDT",
            description="P2P SELL USDT @ 648.65 VES",
            user_rate=Decimal("648.65"),
            source="binance",
            source_ref="p2p-sell-1",
        ),
    )

    rows = [
        # Exact deposit, same day.
        ("dep-exact", SELL_AT, TransactionKind.INCOME, Decimal("20000.00")),
        # Far-off deposit, same day — still listed, high drift.
        ("dep-far", SELL_AT, TransactionKind.INCOME, Decimal("1250.00")),
        # Deposit one day later, also close.
        (
            "dep-next-day",
            SELL_AT + timedelta(days=1),
            TransactionKind.INCOME,
            Decimal("19900.00"),
        ),
        # Same-sign row: an expense cannot be the other leg.
        ("exp-same-sign", SELL_AT, TransactionKind.EXPENSE, Decimal("-20000.00")),
        # Outside the ±2 day window.
        (
            "dep-too-old",
            SELL_AT - timedelta(days=5),
            TransactionKind.INCOME,
            Decimal("20000.00"),
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


def _sell_id(conn: sqlite3.Connection) -> int:
    row = conn.execute(
        "SELECT id FROM transactions WHERE source_ref = ?", ("p2p-sell-1",)
    ).fetchone()
    assert row is not None
    return int(row["id"])


def _refs(result) -> list[str]:
    return [c.card.description for c in result.candidates]


def test_expected_ves_is_amount_times_user_rate(pairing_db: sqlite3.Connection) -> None:
    result = find_pair_candidates(pairing_db, sell_id=_sell_id(pairing_db))
    assert result.expected_ves == Decimal("30.83") * Decimal("648.65")


def test_income_and_expense_candidates_are_both_listed(
    pairing_db: sqlite3.Connection,
) -> None:
    """A deposit filed under the wrong kind must stay visible."""
    refs = _refs(find_pair_candidates(pairing_db, sell_id=_sell_id(pairing_db)))
    assert "dep-exact" in refs
    assert "exp-same-sign" in refs


def test_candidates_outside_the_window_are_excluded(
    pairing_db: sqlite3.Connection,
) -> None:
    refs = _refs(find_pair_candidates(pairing_db, sell_id=_sell_id(pairing_db)))
    assert "dep-too-old" not in refs


def test_widening_the_window_pulls_in_the_older_deposit(
    pairing_db: sqlite3.Connection,
) -> None:
    refs = _refs(
        find_pair_candidates(pairing_db, sell_id=_sell_id(pairing_db), window_days=7)
    )
    assert "dep-too-old" in refs


def test_already_paired_rows_are_excluded(pairing_db: sqlite3.Connection) -> None:
    pairing_db.execute(
        "UPDATE transactions SET transfer_id = 'tid-x' WHERE source_ref = ?",
        ("dep-exact",),
    )
    refs = _refs(find_pair_candidates(pairing_db, sell_id=_sell_id(pairing_db)))
    assert "dep-exact" not in refs


def test_candidates_are_sorted_closest_match_first(
    pairing_db: sqlite3.Connection,
) -> None:
    refs = _refs(find_pair_candidates(pairing_db, sell_id=_sell_id(pairing_db)))
    assert refs[0] == "dep-exact"
    assert refs.index("dep-next-day") < refs.index("dep-far")


def test_same_sign_candidate_is_not_pairable(pairing_db: sqlite3.Connection) -> None:
    result = find_pair_candidates(pairing_db, sell_id=_sell_id(pairing_db))
    same_sign = next(
        c for c in result.candidates if c.card.description == "exp-same-sign"
    )
    assert same_sign.pairable is False
    assert same_sign.blocked_reason is not None
    deposit = next(c for c in result.candidates if c.card.description == "dep-exact")
    assert deposit.pairable is True
    assert deposit.blocked_reason is None


def test_sell_without_user_rate_yields_no_expected_and_no_drift(
    pairing_db: sqlite3.Connection,
) -> None:
    pairing_db.execute(
        "UPDATE transactions SET user_rate = NULL WHERE source_ref = ?", ("p2p-sell-1",)
    )
    result = find_pair_candidates(pairing_db, sell_id=_sell_id(pairing_db))
    assert result.expected_ves is None
    assert result.candidates  # still listed, just unscored
    assert all(c.drift_ratio is None for c in result.candidates)


def test_unknown_sell_id_raises_lookup_error(pairing_db: sqlite3.Connection) -> None:
    with pytest.raises(LookupError):
        find_pair_candidates(pairing_db, sell_id=999999)
