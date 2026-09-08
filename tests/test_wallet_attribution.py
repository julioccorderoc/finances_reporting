"""RED — P2P and Pay are booked against the wrong Binance wallet (ADR-024).

`RawBinanceP2pRow.to_transaction` and `RawBinancePayRow.to_transaction`
both take `spot_account_id`, so every P2P order and every Binance Pay
event is filed against Spot. Binance settles P2P in **Funding** and spends
Pay from it. 158 + 15 rows, −13,748.63 USDT, for about a year.

The ledger says so without any outside help: 9,842.05 USDT was
transferred into Funding and 225.50 has ever left it. Nothing there
consumes anything, because the consumption is recorded on Spot. ADR-023's
exchange snapshot then makes it exact — Spot 0.55 against the ledger's
−2,017.45, Funding 272.56 against 2,335.56.

Two pieces here: the ingest stops making new ones, and a repair command
moves the history. `source_ref` is never touched — it is the dedup key
(ADR-010), and rewriting it would re-import every repaired row as new.
"""

from __future__ import annotations

import sqlite3
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from finances.db.repos import transactions as txn_repo
from finances.domain.models import Transaction, TransactionKind
from finances.domain.wallet_attribution import repair_wallet_attribution
from finances.ingest.binance import RawBinanceP2pRow, RawBinancePayRow

SPOT = "Binance Spot"
FUNDING = "Binance Funding"


def _account_id(conn: sqlite3.Connection, name: str) -> int:
    row = conn.execute("SELECT id FROM accounts WHERE name = ?", (name,)).fetchone()
    assert row is not None, f"no account named {name!r}"
    return int(row[0])


def _row(
    conn: sqlite3.Connection,
    *,
    account: str,
    source_ref: str,
    amount: str = "-50",
    currency: str = "USDT",
    kind: TransactionKind = TransactionKind.EXPENSE,
    category_id: int | None = None,
    user_rate: Decimal | None = None,
) -> int:
    txn = txn_repo.insert(
        conn,
        Transaction(
            account_id=_account_id(conn, account),
            occurred_at=datetime(2026, 5, 12, tzinfo=UTC),
            kind=kind,
            amount=Decimal(amount),
            currency=currency,
            description="row",
            category_id=category_id,
            user_rate=user_rate,
            source="binance",
            source_ref=source_ref,
        ),
    )
    assert txn.id is not None
    return txn.id


def _account_of(conn: sqlite3.Connection, txn_id: int) -> int:
    return int(
        conn.execute(
            "SELECT account_id FROM transactions WHERE id = ?", (txn_id,)
        ).fetchone()[0]
    )


# ---------------------------------------------------------------------------
# The ingest stops making new ones
# ---------------------------------------------------------------------------


class TestIngestBooksAgainstFunding:
    def test_a_p2p_sell_lands_on_funding(self):
        txn = RawBinanceP2pRow(
            orderNumber="991",
            tradeType="SELL",
            asset="USDT",
            amount=Decimal("50"),
            unitPrice=Decimal("300"),
            fiat="VES",
            createTime=1_777_000_000_000,
        ).to_transaction(funding_account_id=3)

        assert txn.account_id == 3

    def test_a_p2p_buy_lands_on_funding_too(self):
        """Both directions settle in the same wallet — a buy credits it."""
        txn = RawBinanceP2pRow(
            orderNumber="992",
            tradeType="BUY",
            asset="USDT",
            amount=Decimal("50"),
            unitPrice=Decimal("300"),
            fiat="VES",
            createTime=1_777_000_000_000,
        ).to_transaction(funding_account_id=3)

        assert txn.account_id == 3
        assert txn.amount == Decimal("50")

    def test_a_pay_send_lands_on_funding(self):
        txn = RawBinancePayRow(
            orderId="p-1",
            orderType="C2C",
            amount=Decimal("-100"),
            currency="USDT",
            transactionTime=1_777_000_000_000,
        ).to_transaction(funding_account_id=3)

        assert txn.account_id == 3

    def test_the_parameter_is_named_for_the_wallet_it_means(self):
        """Renamed, not just repointed: a `spot_account_id` that receives a
        funding id is the bug this ADR is about, one refactor later."""
        with pytest.raises(TypeError):
            RawBinanceP2pRow(
                orderNumber="993",
                tradeType="SELL",
                asset="USDT",
                amount=Decimal("50"),
                unitPrice=Decimal("300"),
                fiat="VES",
                createTime=1_777_000_000_000,
            ).to_transaction(spot_account_id=2)


# ---------------------------------------------------------------------------
# The repair moves the history
# ---------------------------------------------------------------------------


class TestRepairWalletAttribution:
    def test_it_moves_p2p_and_pay_off_spot(self, seeded_db: sqlite3.Connection):
        p2p = _row(seeded_db, account=SPOT, source_ref="p2p:991")
        pay = _row(seeded_db, account=SPOT, source_ref="pay:p-1")

        report = repair_wallet_attribution(seeded_db)

        funding = _account_id(seeded_db, FUNDING)
        assert _account_of(seeded_db, p2p) == funding
        assert _account_of(seeded_db, pay) == funding
        assert report.moved == 2

    def test_it_leaves_every_other_binance_row_where_it_is(
        self, seeded_db: sqlite3.Connection
    ):
        """Converts, deposits, withdrawals and Earn all settle in Spot and
        already read their wallet from the record."""
        kept = [
            _row(seeded_db, account=SPOT, source_ref=ref)
            for ref in (
                "convert:1:from",
                "deposit:abc",
                "withdraw:9",
                "earn-subscribe:5",
                "earn-redeem:6:to",
                "transfer:7:from",
            )
        ]

        repair_wallet_attribution(seeded_db)

        spot = _account_id(seeded_db, SPOT)
        assert [_account_of(seeded_db, i) for i in kept] == [spot] * len(kept)

    def test_running_it_twice_changes_nothing_the_second_time(
        self, seeded_db: sqlite3.Connection
    ):
        _row(seeded_db, account=SPOT, source_ref="p2p:991")

        repair_wallet_attribution(seeded_db)
        again = repair_wallet_attribution(seeded_db)

        assert again.moved == 0

    def test_a_dry_run_reports_without_writing(self, seeded_db: sqlite3.Connection):
        p2p = _row(seeded_db, account=SPOT, source_ref="p2p:991")

        report = repair_wallet_attribution(seeded_db, dry_run=True)

        assert report.moved == 1
        assert _account_of(seeded_db, p2p) == _account_id(seeded_db, SPOT)

    def test_the_row_keeps_everything_but_its_account(
        self, seeded_db: sqlite3.Connection
    ):
        """source_ref above all: it is the dedup key (ADR-010), and a
        rewritten one would re-import this row as a new event."""
        category = int(
            seeded_db.execute(
                "SELECT id FROM categories WHERE name = 'Internal Transfer'"
            ).fetchone()[0]
        )
        p2p = _row(
            seeded_db,
            account=SPOT,
            source_ref="p2p:991",
            amount="-33.61",
            category_id=category,
            user_rate=Decimal("300"),
        )
        before = dict(
            seeded_db.execute(
                "SELECT * FROM transactions WHERE id = ?", (p2p,)
            ).fetchone()
        )

        repair_wallet_attribution(seeded_db)

        after = dict(
            seeded_db.execute(
                "SELECT * FROM transactions WHERE id = ?", (p2p,)
            ).fetchone()
        )
        changed = {k for k in before if before[k] != after[k]}
        assert changed == {"account_id"}

    def test_a_paired_p2p_sell_stays_paired(self, seeded_db: sqlite3.Connection):
        """The bank anchors P2P pairing (ADR-002 amendment). Moving the
        Binance leg to another account leaves it a cross-account pair —
        but a repair that silently broke one would be worse than the bug."""
        sell = _row(seeded_db, account=SPOT, source_ref="p2p:991")
        deposit = _row(
            seeded_db,
            account="Provincial Bolivares",
            source_ref="hash:bank",
            amount="15000",
            currency="VES",
            kind=TransactionKind.INCOME,
        )
        seeded_db.execute(
            "UPDATE transactions SET kind = 'transfer', transfer_id = 'pair-1'"
            " WHERE id IN (?, ?)",
            (sell, deposit),
        )

        repair_wallet_attribution(seeded_db)

        legs = seeded_db.execute(
            "SELECT account_id FROM transactions WHERE transfer_id = 'pair-1'"
        ).fetchall()
        assert len(legs) == 2
        assert _account_of(seeded_db, sell) == _account_id(seeded_db, FUNDING)

    def test_a_p2p_row_already_on_funding_is_not_touched(
        self, seeded_db: sqlite3.Connection
    ):
        """The backfill put some rows on Funding already. The repair is
        defined by where a row *is*, not by what it is."""
        _row(seeded_db, account=FUNDING, source_ref="p2p:already")

        assert repair_wallet_attribution(seeded_db).moved == 0

    def test_the_report_names_the_totals_moved_per_currency(
        self, seeded_db: sqlite3.Connection
    ):
        """A repair of 173 rows is read before it is trusted; a count
        alone does not let the owner check it against the exchange."""
        _row(seeded_db, account=SPOT, source_ref="p2p:991", amount="-33.61")
        _row(seeded_db, account=SPOT, source_ref="pay:p-1", amount="-100")
        _row(
            seeded_db,
            account=SPOT,
            source_ref="p2p:992",
            amount="-25",
            currency="USDC",
        )

        report = repair_wallet_attribution(seeded_db, dry_run=True)

        assert report.moved == 3
        assert report.totals_by_currency["USDT"] == Decimal("-133.61")
        assert report.totals_by_currency["USDC"] == Decimal("-25")
