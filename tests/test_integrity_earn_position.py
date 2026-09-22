"""The Earn position against the exchange's principal (ADR-028 §4).

Every other Binance check reasons about the ledger's own rows. Spot and
Funding have ``exchange_balances`` to be measured against (ADR-023); Earn has
had ``earn_positions`` since ADR-003 — fetched from Binance's own position
endpoint — and **nothing ever compared the two**. So when 410 BONUS rewards
were booked to Earn while Binance paid them into Spot, the Earn account read
exactly the misplacement high, every per-account figure agreed with itself,
and no check said a word. The gap it left behind was closed with plugs
(ADR-018), which is the failure ADR-020 was written about.

This check closes the blind spot: for each asset the exchange reports an
active Earn principal for, the ledger's Earn balance must agree within
``EXCHANGE_BALANCE_TOLERANCE``. Compared as of the position's own snapshot,
so a stale claim is not a defect, and skipped where the ledger has no row at
all — it has no id to name, and `finances report balances` is where an
absent position shows.
"""

from __future__ import annotations

import sqlite3
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from finances.db.repos import accounts as accounts_repo
from finances.db.repos import positions as positions_repo
from finances.db.repos import transactions as txn_repo
from finances.domain import integrity
from finances.domain.models import EarnPosition, Transaction, TransactionKind

CHECK = "earn_position_disagrees_with_principal"
SNAPSHOT = datetime(2026, 9, 22, 14, 17, 58, tzinfo=UTC)


def _names(report):
    return {f.check for f in report.findings}


def _finding(report, name):
    return next(f for f in report.findings if f.check == name)


@pytest.fixture
def earn(seeded_db: sqlite3.Connection):
    conn = seeded_db
    account = accounts_repo.get_by_name(conn, "Binance Earn")
    assert account is not None and account.id is not None
    return conn, account


def _position(
    conn: sqlite3.Connection,
    account_id: int,
    *,
    asset: str = "USDT",
    principal: str = "1700.97",
    ended_at: datetime | None = None,
    snapshot_at: datetime | None = SNAPSHOT,
    product_id: str = "USDT001",
) -> EarnPosition:
    return positions_repo.insert(
        conn,
        EarnPosition(
            account_id=account_id,
            product_id=product_id,
            asset=asset,
            principal=Decimal(principal),
            apy=None,
            started_at=ended_at or SNAPSHOT,
            ended_at=ended_at,
            snapshot_at=snapshot_at,
        ),
    )


def _reward(
    conn: sqlite3.Connection,
    account_id: int,
    *,
    amount: str,
    ref: str,
    currency: str = "USDT",
    when: datetime | None = None,
):
    return txn_repo.insert(
        conn,
        Transaction(
            account_id=account_id,
            occurred_at=when or SNAPSHOT - timedelta(hours=1),
            kind=TransactionKind.INCOME,
            amount=Decimal(amount),
            currency=currency,
            description="Earn reward",
            source="binance",
            source_ref=ref,
        ),
    )


def test_check_is_registered() -> None:
    assert CHECK in {c.name for c in integrity.CHECKS}


def test_an_earning_ledger_that_outruns_the_principal_is_an_error(earn) -> None:
    """The BONUS misplacement in miniature: ledger 4.98 above the exchange."""
    conn, account = earn
    _position(conn, account.id, principal="1700.97")
    first = _reward(conn, account.id, amount="1705.95", ref="earn:1")

    report = integrity.run_checks(conn)

    assert CHECK in _names(report)
    finding = _finding(report, CHECK)
    assert finding.severity is integrity.Severity.ERROR
    assert finding.count == 1
    assert finding.sample_ids == [first.id]


def test_a_ledger_short_of_the_principal_is_an_error_too(earn) -> None:
    """The direction does not matter; the disagreement does."""
    conn, account = earn
    _position(conn, account.id, principal="1700.97")
    _reward(conn, account.id, amount="1690.00", ref="earn:2")

    assert CHECK in _names(integrity.run_checks(conn))


def test_agreement_within_tolerance_is_silent(earn) -> None:
    """Reward accrual moves by fractions — the gate is the ADR-023 tolerance."""
    conn, account = earn
    _position(conn, account.id, principal="1700.97")
    _reward(conn, account.id, amount="1701.20", ref="earn:3")

    assert CHECK not in _names(integrity.run_checks(conn))


def test_an_asset_the_ledger_has_no_row_for_is_skipped(earn) -> None:
    """The existing convention: nothing to point at, so nothing to report."""
    conn, account = earn
    _position(conn, account.id, asset="USDC", principal="5002.90", product_id="USDC001")

    assert CHECK not in _names(integrity.run_checks(conn))


def test_a_closed_position_is_not_the_claim(earn) -> None:
    """Only the open product is what the exchange says it holds now."""
    conn, account = earn
    _position(
        conn,
        account.id,
        principal="9999.99",
        ended_at=SNAPSHOT + timedelta(days=1),
    )
    _reward(conn, account.id, amount="1700.97", ref="earn:4")

    assert CHECK not in _names(integrity.run_checks(conn))


def test_rows_after_the_snapshot_are_not_counted(earn) -> None:
    """A snapshot is a claim about a moment, compared to that moment."""
    conn, account = earn
    _position(conn, account.id, principal="100.00")
    _reward(
        conn,
        account.id,
        amount="100.00",
        ref="earn:5",
        when=SNAPSHOT - timedelta(hours=1),
    )
    _reward(
        conn,
        account.id,
        amount="40.00",
        ref="earn:6",
        when=SNAPSHOT + timedelta(hours=1),
    )

    assert CHECK not in _names(integrity.run_checks(conn))


def test_each_asset_is_checked_separately(earn) -> None:
    """A healthy USDT balance must not mask a broken USDC one."""
    conn, account = earn
    _position(conn, account.id, asset="USDT", principal="1700.97")
    _position(conn, account.id, asset="USDC", principal="5002.90", product_id="USDC001")
    _reward(conn, account.id, amount="1700.97", ref="earn:7")
    _reward(conn, account.id, amount="5008.00", currency="USDC", ref="earn:8")

    report = integrity.run_checks(conn)

    assert CHECK in _names(report)
    assert _finding(report, CHECK).count == 1


def test_the_misplaced_bonus_rows_are_the_defect_it_catches(earn) -> None:
    """The live incident, in miniature.

    Binance paid 4.97640115 USDT of BONUS rewards into Spot; the ledger had
    booked them to Earn, so Earn read exactly that much above the principal
    and every check stayed quiet. Moving the rows to the wallet Binance
    named — the ingest's own repair (ADR-027 §2.3) — is what silences it.
    """
    conn, account = earn
    _position(conn, account.id, principal="1700.97")
    bonuses = [
        _reward(conn, account.id, amount="2.40000000", ref="earn-reward:noproj:USDT:1"),
        _reward(conn, account.id, amount="2.57640115", ref="earn-reward:noproj:USDT:2"),
    ]
    _reward(conn, account.id, amount="1700.97", ref="earn:realtime")
    assert _finding(integrity.run_checks(conn), CHECK).count == 1

    for row in bonuses:
        assert row.id is not None
        moved = row.model_copy(update={"account_id": 2})
        txn_repo.upsert_by_source_ref(conn, moved)

    assert CHECK not in _names(integrity.run_checks(conn))


def test_a_clean_ledger_says_nothing(earn) -> None:
    conn, account = earn
    _position(conn, account.id, principal="1700.97")
    _reward(conn, account.id, amount="1700.97", ref="earn:clean")

    assert CHECK not in _names(integrity.run_checks(conn))
