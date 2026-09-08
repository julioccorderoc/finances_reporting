"""RED — the ledger has never asked Binance what it holds (ADR-023).

`finances doctor` reported Binance Spot at −2,017.45 USDT for months and
could not say why, because nothing in the system had ever compared the
ledger to the exchange. `earn_positions` (ADR-003) does exactly that for
Earn, and it is what proved Earn correct. Spot and Funding had no
equivalent: grepping `finances/` for `funding_wallet` or `.account()`
returns nothing, though the SDK exposes both and `conftest` mocks both.

Today's reading, which only became possible once both numbers sat in one
place:

    Spot USDT    exchange 0.55      ledger -2,017.45
    Funding USDT exchange 272.56    ledger  2,335.56

The combined free balance is nearly right while each wallet is out by two
thousand — the signature of a booking sent to the wrong wallet. No check
the ledger owns can see that, because every one of them reasons about the
ledger's own rows and those are internally consistent. Only an outside
figure can.

These tests cover the three pieces: the table that records the exchange's
claim, the ingest step that captures it, and the check that compares. The
comparison is *as of the capture*, which is what keeps a stale snapshot
from reading as a defect.
"""

from __future__ import annotations

import sqlite3
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from finances.db.repos import exchange_balances as balances_repo
from finances.db.repos import transactions as txn_repo
from finances.domain.integrity import run_checks
from finances.domain.models import ExchangeBalance, Transaction, TransactionKind

CAPTURED_AT = datetime(2026, 9, 7, 18, 0, tzinfo=UTC)


def _account_id(conn: sqlite3.Connection, name: str) -> int:
    row = conn.execute("SELECT id FROM accounts WHERE name = ?", (name,)).fetchone()
    assert row is not None, f"no account named {name!r}"
    return int(row[0])


def _finding(conn: sqlite3.Connection, name: str):
    return next((f for f in run_checks(conn).findings if f.check == name), None)


def _snapshot(
    conn: sqlite3.Connection,
    *,
    account: str,
    currency: str = "USDT",
    balance: str,
    captured_at: datetime = CAPTURED_AT,
) -> ExchangeBalance:
    return balances_repo.insert(
        conn,
        ExchangeBalance(
            account_id=_account_id(conn, account),
            currency=currency,
            balance=Decimal(balance),
            captured_at=captured_at,
            source="binance",
        ),
    )


def _txn(
    conn: sqlite3.Connection,
    *,
    account: str,
    amount: str,
    currency: str = "USDT",
    occurred_at: datetime,
    source_ref: str,
) -> int:
    txn = txn_repo.insert(
        conn,
        Transaction(
            account_id=_account_id(conn, account),
            occurred_at=occurred_at,
            kind=TransactionKind.INCOME,
            amount=Decimal(amount),
            currency=currency,
            description="row",
            source="binance",
            source_ref=source_ref,
        ),
    )
    assert txn.id is not None
    return txn.id


# ---------------------------------------------------------------------------
# The table: a dated external claim, never a derived figure
# ---------------------------------------------------------------------------


class TestExchangeBalancesRepo:
    def test_a_snapshot_round_trips_with_its_decimal_intact(
        self, seeded_db: sqlite3.Connection
    ):
        """Balances are money. A float here would be a rounding defect in
        the one number the ledger is being measured against."""
        saved = _snapshot(seeded_db, account="Binance Spot", balance="272.55998864")

        assert saved.id is not None
        latest = balances_repo.latest_per_position(seeded_db)
        assert latest[(saved.account_id, "USDT")].balance == Decimal("272.55998864")

    def test_the_latest_capture_per_position_wins(
        self, seeded_db: sqlite3.Connection
    ):
        _snapshot(
            seeded_db,
            account="Binance Spot",
            balance="100",
            captured_at=datetime(2026, 9, 1, tzinfo=UTC),
        )
        _snapshot(seeded_db, account="Binance Spot", balance="0.55")

        latest = balances_repo.latest_per_position(seeded_db)
        spot = _account_id(seeded_db, "Binance Spot")
        assert latest[(spot, "USDT")].balance == Decimal("0.55")

    def test_positions_are_tracked_per_currency_not_per_account(
        self, seeded_db: sqlite3.Connection
    ):
        """Spot holds USDT and USDC and they are different claims — the
        defect this ADR exists for shows in one and not the other."""
        _snapshot(seeded_db, account="Binance Spot", currency="USDT", balance="0.55")
        _snapshot(seeded_db, account="Binance Spot", currency="USDC", balance="0.23")

        latest = balances_repo.latest_per_position(seeded_db)
        spot = _account_id(seeded_db, "Binance Spot")
        assert latest[(spot, "USDT")].balance == Decimal("0.55")
        assert latest[(spot, "USDC")].balance == Decimal("0.23")

    def test_recapturing_the_same_moment_replaces_rather_than_duplicates(
        self, seeded_db: sqlite3.Connection
    ):
        """Two syncs in one second must not leave the check a choice of
        two answers for one moment."""
        _snapshot(seeded_db, account="Binance Spot", balance="0.55")
        _snapshot(seeded_db, account="Binance Spot", balance="0.60")

        count = seeded_db.execute(
            "SELECT COUNT(*) FROM exchange_balances WHERE captured_at = ?",
            (CAPTURED_AT.isoformat(),),
        ).fetchone()[0]
        assert count == 1


# ---------------------------------------------------------------------------
# The check: the ledger, as of the capture, against the exchange
# ---------------------------------------------------------------------------


class TestPositionDisagreesWithExchange:
    def test_a_position_matching_the_exchange_is_not_reported(
        self, seeded_db: sqlite3.Connection
    ):
        _txn(
            seeded_db,
            account="Binance Spot",
            amount="100",
            occurred_at=datetime(2026, 8, 1, tzinfo=UTC),
            source_ref="agrees",
        )
        _snapshot(seeded_db, account="Binance Spot", balance="100")

        assert _finding(seeded_db, "position_disagrees_with_exchange") is None

    def test_the_live_defect_is_reported_as_an_error(
        self, seeded_db: sqlite3.Connection
    ):
        """Spot: exchange 0.55, ledger −2,017.45. Two thousand dollars of
        difference that no other check in the system can see."""
        _txn(
            seeded_db,
            account="Binance Spot",
            amount="-2017.45",
            occurred_at=datetime(2026, 8, 1, tzinfo=UTC),
            source_ref="misfiled",
        )
        _snapshot(seeded_db, account="Binance Spot", balance="0.55")

        found = _finding(seeded_db, "position_disagrees_with_exchange")
        assert found is not None
        assert found.severity.value == "error"

    def test_dust_below_the_tolerance_is_not_a_disagreement(
        self, seeded_db: sqlite3.Connection
    ):
        """Earn accrues reward continuously and every wallet carries dust;
        a check that fires on cents is one nobody reads."""
        _txn(
            seeded_db,
            account="Binance Spot",
            amount="100",
            occurred_at=datetime(2026, 8, 1, tzinfo=UTC),
            source_ref="dusty",
        )
        _snapshot(seeded_db, account="Binance Spot", balance="100.4")

        assert _finding(seeded_db, "position_disagrees_with_exchange") is None

    def test_rows_landing_after_the_capture_are_not_counted_against_it(
        self, seeded_db: sqlite3.Connection
    ):
        """A snapshot is a claim about a moment. Compared to today's
        ledger it would read as a defect the day after any sync — which is
        every day."""
        _txn(
            seeded_db,
            account="Binance Spot",
            amount="100",
            occurred_at=datetime(2026, 8, 1, tzinfo=UTC),
            source_ref="before",
        )
        _snapshot(seeded_db, account="Binance Spot", balance="100")
        _txn(
            seeded_db,
            account="Binance Spot",
            amount="500",
            occurred_at=datetime(2026, 9, 8, tzinfo=UTC),
            source_ref="after",
        )

        assert _finding(seeded_db, "position_disagrees_with_exchange") is None

    def test_offset_aware_timestamps_compare_by_instant_not_by_string(
        self, seeded_db: sqlite3.Connection
    ):
        """Provincial writes -04:00 and Binance writes +00:00. Comparing
        those as text puts a Caracas afternoon before a UTC morning."""
        _txn(
            seeded_db,
            account="Binance Spot",
            amount="100",
            occurred_at=datetime(2026, 8, 1, tzinfo=UTC),
            source_ref="counted",
        )
        # 2026-09-07T20:00-04:00 is 00:00 on the 8th UTC — after the
        # capture, so it must not count. As text it sorts before it.
        seeded_db.execute(
            """
            INSERT INTO transactions
                (account_id, occurred_at, kind, amount, currency,
                 description, source, source_ref, needs_review)
            VALUES (?, '2026-09-07T20:00:00-04:00', 'income', '500', 'USDT',
                    'caracas', 'binance', 'tz-trap', 0)
            """,
            (_account_id(seeded_db, "Binance Spot"),),
        )
        _snapshot(seeded_db, account="Binance Spot", balance="100")

        assert _finding(seeded_db, "position_disagrees_with_exchange") is None

    def test_a_position_with_no_snapshot_is_not_reported(
        self, seeded_db: sqlite3.Connection
    ):
        """Cash and the bank have no exchange to ask. Silence is not a
        disagreement."""
        _txn(
            seeded_db,
            account="Cash USD",
            amount="500",
            currency="USD",
            occurred_at=datetime(2026, 8, 1, tzinfo=UTC),
            source_ref="cash",
        )

        assert _finding(seeded_db, "position_disagrees_with_exchange") is None

    def test_each_wallet_is_judged_alone(self, seeded_db: sqlite3.Connection):
        """The live defect's combined total is nearly right — Spot and
        Funding are each out by two thousand in opposite directions. A
        check that nets them sees nothing, which is how this survived a
        year."""
        _txn(
            seeded_db,
            account="Binance Spot",
            amount="-2000",
            occurred_at=datetime(2026, 8, 1, tzinfo=UTC),
            source_ref="spot-short",
        )
        _txn(
            seeded_db,
            account="Binance Funding",
            amount="2000",
            occurred_at=datetime(2026, 8, 1, tzinfo=UTC),
            source_ref="funding-long",
        )
        _snapshot(seeded_db, account="Binance Spot", balance="0")
        _snapshot(seeded_db, account="Binance Funding", balance="0")

        found = _finding(seeded_db, "position_disagrees_with_exchange")
        assert found is not None
        assert found.count == 2


# ---------------------------------------------------------------------------
# The capture: two read-only calls at the end of a sync
# ---------------------------------------------------------------------------


class TestCaptureFromTheSdk:
    @pytest.fixture
    def client(self, mocked_binance_sdk):
        mocked_binance_sdk.account.return_value = {
            "balances": [
                {"asset": "USDT", "free": "0.54790000", "locked": "0"},
                {"asset": "USDC", "free": "0.23286600", "locked": "0"},
                {"asset": "BNB", "free": "0", "locked": "0"},
            ]
        }
        mocked_binance_sdk.funding_wallet.return_value = [
            {"asset": "USDT", "free": "272.55998864", "locked": "0", "freeze": "0"},
        ]
        return mocked_binance_sdk

    def test_it_records_both_wallets_from_the_sdk(
        self, seeded_db: sqlite3.Connection, client
    ):
        from finances.ingest.binance import capture_exchange_balances

        capture_exchange_balances(seeded_db, client, captured_at=CAPTURED_AT)

        latest = balances_repo.latest_per_position(seeded_db)
        spot = _account_id(seeded_db, "Binance Spot")
        funding = _account_id(seeded_db, "Binance Funding")
        assert latest[(spot, "USDT")].balance == Decimal("0.54790000")
        assert latest[(spot, "USDC")].balance == Decimal("0.23286600")
        assert latest[(funding, "USDT")].balance == Decimal("272.55998864")

    def test_locked_and_frozen_amounts_count_as_held(
        self, seeded_db: sqlite3.Connection, client
    ):
        """A locked balance is money the ledger's rows still account for.
        Dropping it would manufacture a disagreement."""
        client.account.return_value = {
            "balances": [{"asset": "USDT", "free": "10", "locked": "5"}]
        }
        client.funding_wallet.return_value = [
            {"asset": "USDT", "free": "1", "locked": "2", "freeze": "3"}
        ]
        from finances.ingest.binance import capture_exchange_balances

        capture_exchange_balances(seeded_db, client, captured_at=CAPTURED_AT)

        latest = balances_repo.latest_per_position(seeded_db)
        assert latest[(_account_id(seeded_db, "Binance Spot"), "USDT")].balance == (
            Decimal("15")
        )
        assert latest[(_account_id(seeded_db, "Binance Funding"), "USDT")].balance == (
            Decimal("6")
        )

    def test_an_asset_the_ledger_never_holds_is_not_recorded(
        self, seeded_db: sqlite3.Connection, client
    ):
        """A zero BNB dust line is not a position; recording it would put
        a permanent phantom in front of the check."""
        from finances.ingest.binance import capture_exchange_balances

        capture_exchange_balances(seeded_db, client, captured_at=CAPTURED_AT)

        assets = {
            row[0]
            for row in seeded_db.execute("SELECT DISTINCT currency FROM exchange_balances")
        }
        assert "BNB" not in assets

    def test_earn_shadow_balances_are_not_recorded_as_spot_positions(
        self, seeded_db: sqlite3.Connection, client
    ):
        """Binance reports Simple Earn principal inside the Spot wallet as
        an ``LD``-prefixed asset — LDUSDT 1,763.26 and LDUSDC 4,523.42 on
        the live account. That is the Earn account's money, already
        modelled by ADR-003 and already checked against the position
        endpoint. Recording it here would assert Spot holds an asset it
        does not, and double-count Earn for anything summing this table."""
        client.account.return_value = {
            "balances": [
                {"asset": "USDT", "free": "0.55", "locked": "0"},
                {"asset": "LDUSDT", "free": "1763.25644955", "locked": "0"},
                {"asset": "LDUSDC", "free": "4523.41746174", "locked": "0"},
            ]
        }
        from finances.ingest.binance import capture_exchange_balances

        capture_exchange_balances(seeded_db, client, captured_at=CAPTURED_AT)

        assets = {
            row[0]
            for row in seeded_db.execute(
                "SELECT DISTINCT currency FROM exchange_balances"
            )
        }
        assert assets == {"USDT"}

    def test_a_position_the_exchange_omits_is_recorded_as_zero(
        self, seeded_db: sqlite3.Connection, client
    ):
        """The exchange lists nothing for an asset it holds none of. That
        is not silence — against a ledger that thinks it holds 400 USDC it
        is a flat contradiction, and the most important one there is. On
        the live account this is exactly Funding USDC: ledger 400.00,
        exchange nothing at all."""
        _txn(
            seeded_db,
            account="Binance Funding",
            amount="400",
            currency="USDC",
            occurred_at=datetime(2026, 8, 22, tzinfo=UTC),
            source_ref="earn-redeem:989421227:to",
        )
        client.funding_wallet.return_value = [
            {"asset": "USDT", "free": "272.55998864", "locked": "0", "freeze": "0"}
        ]
        from finances.ingest.binance import capture_exchange_balances

        capture_exchange_balances(seeded_db, client, captured_at=CAPTURED_AT)

        latest = balances_repo.latest_per_position(seeded_db)
        funding = _account_id(seeded_db, "Binance Funding")
        assert latest[(funding, "USDC")].balance == Decimal("0")
        assert _finding(seeded_db, "position_disagrees_with_exchange") is not None

    def test_an_sdk_failure_is_reported_not_raised(
        self, seeded_db: sqlite3.Connection, client
    ):
        """A sync that already wrote rows must not be undone because the
        last read failed — the same contract every other ingest step has."""
        client.funding_wallet.side_effect = RuntimeError("451 from Binance")
        from finances.ingest.binance import capture_exchange_balances

        errors = capture_exchange_balances(seeded_db, client, captured_at=CAPTURED_AT)

        assert any("451" in e for e in errors)
        latest = balances_repo.latest_per_position(seeded_db)
        # The Spot half still landed.
        assert (_account_id(seeded_db, "Binance Spot"), "USDT") in latest
