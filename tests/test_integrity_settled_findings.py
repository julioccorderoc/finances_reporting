"""RED — three checks report rows the ledger has already settled.

``finances doctor`` on the live ledger reports 32 rows under
``transfer_legs_same_account``, and every one of the sixteen groups is an
**ADR-019 reversal pair**: a failed bank charge and its same-day
``REVERSO CARGO``, on one account, in one currency, netting exactly zero.
``domain/reversals.py::pair_reversal`` *requires* that shape; the check
calls it an error. The check was written 2026-08-03, ADR-019 landed after
it, and nobody went back. Two more checks report the same pairs and the
same finished work from a different angle:

* ``category_kind_mismatch`` — the charge leg keeps its expense category
  after being promoted to ``kind='transfer'``. ``pair_reversal`` does
  that deliberately, and reports drop the row by kind before the category
  is ever consulted (``money.SQL_NOT_CURRENCY_MOVEMENT``).
* ``unpaired_p2p_sells`` — four sells the owner has already marked
  *Internal Transfer*, a transfer-kind category, which is exactly how
  rule-005's movement rule says "this moved, it was not spent". The
  check's own description ("still counted as an expense") is untrue of
  them. ``convert_leg_without_counterpart`` solved the identical problem
  by excluding rows that had since been resolved; this one never did.

A permanent false ERROR is worse than noise: ``doctor --strict`` exits
non-zero forever, so the check that would catch a *real* pairing defect
can no longer be used as a gate. Each test below states what the check
must keep catching alongside what it must stop reporting — an exemption
that swallows the real defect is not a fix.
"""

from __future__ import annotations

import sqlite3
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from finances.db.repos import categories as categories_repo
from finances.db.repos import transactions as txn_repo
from finances.domain.integrity import run_checks
from finances.domain.models import Transaction, TransactionKind
from finances.domain.reversals import REVERSAL_MARKERS, pair_reversal

FIXED_AT = datetime(2026, 7, 13, 12, 0, tzinfo=UTC)


def _finding(conn: sqlite3.Connection, name: str):
    return next((f for f in run_checks(conn).findings if f.check == name), None)


def _account_id(conn: sqlite3.Connection, name: str) -> int:
    row = conn.execute("SELECT id FROM accounts WHERE name = ?", (name,)).fetchone()
    assert row is not None, f"no account named {name!r}"
    return int(row[0])


def _category_id(conn: sqlite3.Connection, kind: TransactionKind, name: str) -> int:
    category = categories_repo.get_by_name(conn, kind, name)
    assert category is not None and category.id is not None
    return category.id


def _row(
    conn: sqlite3.Connection,
    *,
    account_id: int,
    amount: str,
    description: str,
    source_ref: str,
    currency: str = "VES",
    kind: TransactionKind = TransactionKind.EXPENSE,
    category_id: int | None = None,
    occurred_at: datetime = FIXED_AT,
    user_rate: Decimal | None = None,
) -> int:
    txn = txn_repo.insert(
        conn,
        Transaction(
            account_id=account_id,
            occurred_at=occurred_at,
            kind=kind,
            amount=Decimal(amount),
            currency=currency,
            description=description,
            category_id=category_id,
            user_rate=user_rate,
            source="provincial",
            source_ref=source_ref,
        ),
    )
    assert txn.id is not None
    return txn.id


def _force_pair(conn: sqlite3.Connection, *ids: int, transfer_id: str) -> None:
    """Pair rows the way a defect would, not the way the code allows.

    ``pair_reversal`` and ``create_transfer`` both refuse the shapes these
    tests need to keep catching, which is the whole reason the check
    exists.
    """
    conn.executemany(
        "UPDATE transactions SET kind = 'transfer', transfer_id = ?,"
        " needs_review = 0 WHERE id = ?",
        [(transfer_id, i) for i in ids],
    )


# ---------------------------------------------------------------------------
# transfer_legs_same_account — ADR-019 reversal pairs are not a defect
# ---------------------------------------------------------------------------


class TestReversalPairsAreNotSamePositionDefects:
    def test_a_reversal_pair_written_by_pair_reversal_is_not_reported(
        self, seeded_db: sqlite3.Connection
    ):
        """The writer and the check must not state opposite rules."""
        bank = _account_id(seeded_db, "Provincial Bolivares")
        charge = _row(
            seeded_db,
            account_id=bank,
            amount="-1250",
            description="DR OB V27209763 102BANCO",
            source_ref="hash:charge",
        )
        reversal = _row(
            seeded_db,
            account_id=bank,
            amount="1250",
            description="REVERSO CARGO",
            source_ref="hash:reversal",
            kind=TransactionKind.INCOME,
        )
        pair_reversal(
            seeded_db,
            charge_transaction_id=charge,
            reversal_transaction_id=reversal,
        )

        assert _finding(seeded_db, "transfer_legs_same_account") is None

    def test_a_same_position_pair_with_no_reversal_leg_is_still_an_error(
        self, seeded_db: sqlite3.Connection
    ):
        """The defect the check exists for: nothing moved, and a real
        movement is hidden behind the pairing."""
        bank = _account_id(seeded_db, "Provincial Bolivares")
        a = _row(
            seeded_db,
            account_id=bank,
            amount="-1250",
            description="DR OB V27209763 102BANCO",
            source_ref="hash:a",
        )
        b = _row(
            seeded_db,
            account_id=bank,
            amount="1250",
            description="TRAV0014270401000011818",
            source_ref="hash:b",
            kind=TransactionKind.INCOME,
        )
        _force_pair(seeded_db, a, b, transfer_id="not-a-reversal")

        found = _finding(seeded_db, "transfer_legs_same_account")
        assert found is not None
        assert set(found.sample_ids) == {a, b}

    def test_a_reversal_wording_that_does_not_net_to_zero_is_still_an_error(
        self, seeded_db: sqlite3.Connection
    ):
        """A reversal repays the charge exactly. Anything else paired to a
        ``REVERSO CARGO`` row is a mis-pair wearing the right words."""
        bank = _account_id(seeded_db, "Provincial Bolivares")
        charge = _row(
            seeded_db,
            account_id=bank,
            amount="-1250",
            description="DR OB V27209763 102BANCO",
            source_ref="hash:charge-uneven",
        )
        reversal = _row(
            seeded_db,
            account_id=bank,
            amount="900",
            description="REVERSO CARGO",
            source_ref="hash:reversal-uneven",
            kind=TransactionKind.INCOME,
        )
        _force_pair(seeded_db, charge, reversal, transfer_id="uneven-reversal")

        found = _finding(seeded_db, "transfer_legs_same_account")
        assert found is not None
        assert set(found.sample_ids) == {charge, reversal}

    @pytest.mark.parametrize("marker", REVERSAL_MARKERS)
    def test_every_reversal_marker_the_pairer_matches_is_exempt(
        self, seeded_db: sqlite3.Connection, marker: str
    ):
        """``REVERSAL_MARKERS`` is documented as extendable. Extending it
        without teaching the check would resurrect the false ERROR for the
        new wording, silently."""
        bank = _account_id(seeded_db, "Provincial Bolivares")
        charge = _row(
            seeded_db,
            account_id=bank,
            amount="-60.38",
            description="COM. PAGO MOVIL",
            source_ref=f"hash:charge-{marker}",
        )
        reversal = _row(
            seeded_db,
            account_id=bank,
            amount="60.38",
            description=marker,
            source_ref=f"hash:reversal-{marker}",
            kind=TransactionKind.INCOME,
        )
        pair_reversal(
            seeded_db,
            charge_transaction_id=charge,
            reversal_transaction_id=reversal,
        )

        assert _finding(seeded_db, "transfer_legs_same_account") is None


# ---------------------------------------------------------------------------
# category_kind_mismatch — a transfer row's category is not consulted
# ---------------------------------------------------------------------------


class TestCategoryMismatchIgnoresTransfers:
    def test_a_paired_charge_keeping_its_expense_category_is_not_reported(
        self, seeded_db: sqlite3.Connection
    ):
        """ADR-019 keeps the charge leg's category on purpose — it may be
        hand triage — and every aggregate drops the row by kind first."""
        bank = _account_id(seeded_db, "Provincial Bolivares")
        charge = _row(
            seeded_db,
            account_id=bank,
            amount="-60.38",
            description="COM. PAGO MOVIL",
            source_ref="hash:fees-charge",
            category_id=_category_id(seeded_db, TransactionKind.EXPENSE, "Fees"),
        )
        reversal = _row(
            seeded_db,
            account_id=bank,
            amount="60.38",
            description="REVERSO CARGO",
            source_ref="hash:fees-reversal",
            kind=TransactionKind.INCOME,
        )
        pair_reversal(
            seeded_db,
            charge_transaction_id=charge,
            reversal_transaction_id=reversal,
        )

        assert _finding(seeded_db, "category_kind_mismatch") is None

    def test_income_filed_under_an_expense_category_is_still_reported(
        self, seeded_db: sqlite3.Connection
    ):
        """The ten reimbursement-shaped rows on the live ledger. Money
        came in; the category says it went out. Still a question."""
        bank = _account_id(seeded_db, "Provincial Bolivares")
        reimbursement = _row(
            seeded_db,
            account_id=bank,
            amount="2261",
            description="DR OB 04149578152 102BAN",
            source_ref="hash:reimbursement",
            kind=TransactionKind.INCOME,
            category_id=_category_id(
                seeded_db, TransactionKind.EXPENSE, "Other Expense"
            ),
        )

        found = _finding(seeded_db, "category_kind_mismatch")
        assert found is not None
        assert found.sample_ids == [reimbursement]


# ---------------------------------------------------------------------------
# unpaired_p2p_sells — a movement category answers the question
# ---------------------------------------------------------------------------


class TestUnpairedP2pSellsRespectMovementCategories:
    def _bank_statement_start(self, conn: sqlite3.Connection) -> None:
        """Sells before the first bank statement are excluded by the check
        already; give it one so these tests exercise the new clause."""
        _row(
            conn,
            account_id=_account_id(conn, "Provincial Bolivares"),
            amount="10000",
            description="TRAV0001",
            source_ref="hash:statement-start",
            kind=TransactionKind.INCOME,
            occurred_at=datetime(2026, 1, 1, tzinfo=UTC),
        )

    def _sell(
        self,
        conn: sqlite3.Connection,
        *,
        source_ref: str,
        category_id: int | None = None,
    ) -> int:
        return _row(
            conn,
            account_id=_account_id(conn, "Binance Spot"),
            amount="-33.61",
            currency="USDT",
            description="P2P SELL USDT @ 300 VES (order 991)",
            source_ref=source_ref,
            category_id=category_id,
            user_rate=Decimal("300"),
        )

    def test_a_sell_the_owner_called_internal_transfer_is_not_backlog(
        self, seeded_db: sqlite3.Connection
    ):
        self._bank_statement_start(seeded_db)
        self._sell(
            seeded_db,
            source_ref="p2p:settled",
            category_id=_category_id(
                seeded_db, TransactionKind.TRANSFER, "Internal Transfer"
            ),
        )

        assert _finding(seeded_db, "unpaired_p2p_sells") is None

    def test_a_sell_with_no_category_is_still_backlog(
        self, seeded_db: sqlite3.Connection
    ):
        self._bank_statement_start(seeded_db)
        sell = self._sell(seeded_db, source_ref="p2p:open")

        found = _finding(seeded_db, "unpaired_p2p_sells")
        assert found is not None
        assert found.sample_ids == [sell]

    def test_a_sell_under_a_spending_category_is_still_backlog(
        self, seeded_db: sqlite3.Connection
    ):
        """Only a *transfer-kind* category asserts "this moved". Filing a
        sell under Leisure does not answer the pairing question."""
        self._bank_statement_start(seeded_db)
        sell = self._sell(
            seeded_db,
            source_ref="p2p:miscategorised",
            category_id=_category_id(seeded_db, TransactionKind.EXPENSE, "Leisure"),
        )

        found = _finding(seeded_db, "unpaired_p2p_sells")
        assert found is not None
        assert found.sample_ids == [sell]
