"""EPIC-006 — Reconciliation Engine & Double-Entry Transfers (red-phase TDD).

This file is written BEFORE implementation: every import below refers to a
module that EPIC-006 will add in parallel (``finances.domain.reconciliation``
and ``finances.domain.transfers``). Running this suite today will fail with
``ModuleNotFoundError`` — that is intentional. Tests pin the public API
contract called out in the EPIC-006 plan so the implementation can land as
green commits against this file.

Scope covered here:

* ``run_reconciliation_pass`` driver + ``ReconciliationStrategy`` protocol
  plumbing (generic pass runner, pluggable strategy seam for EPIC-017+).
* ``MatchProposal`` / ``ReconciliationReport`` value objects.
* ``create_transfer`` — three-mode entry point per ADR-002 (fresh legs,
  anchor-only, both-anchors promotion).
* ``validate`` — double-entry invariants (same-currency net-to-zero,
  cross-currency user_rate conversion, tolerance knob).
* ``find_unreconciled`` — orphan detection via ``v_unreconciled_transfers``.
* ``BankAnchoredP2pPairing`` — the Provincial-anchor strategy called out in
  ADR-002 amendments.

Per rule-002 any direct SQL that bypasses ``create_transfer`` is marked with
the sentinel comment ``deliberate malformed fixture`` so future readers can
see at a glance that the row is crafted for negative-path testing.
"""

from __future__ import annotations

import sqlite3
import uuid
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

import pytest

from finances.db.repos import accounts as acc_repo
from finances.db.repos import transactions as txn_repo
from finances.domain.models import Transaction, TransactionKind
from finances.domain.reconciliation import (
    MatchProposal,
    ReconciliationReport,
    run_reconciliation_pass,
)
from finances.domain.transfers import (
    BankAnchoredP2pPairing,
    TransferPair,
    create_transfer,
    find_unreconciled,
    validate,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

FIXED_AT = datetime(2026, 4, 15, 12, 0, tzinfo=UTC)


def _unique_ref(tag: str = "txn") -> str:
    """Return a unique source_ref so UNIQUE(source, source_ref) never collides."""
    return f"{tag}-{uuid.uuid4()}"


def _insert_income_row(
    conn: sqlite3.Connection,
    *,
    account_id: int,
    amount: Decimal,
    currency: str,
    occurred_at: datetime = FIXED_AT,
    source: str = "test",
    source_ref: str | None = None,
    user_rate: Decimal | None = None,
    description: str | None = None,
) -> Transaction:
    """Insert a plain INCOME row via the repo; returns the hydrated Transaction."""
    txn = Transaction(
        account_id=account_id,
        occurred_at=occurred_at,
        kind=TransactionKind.INCOME,
        amount=amount,
        currency=currency,
        description=description,
        source=source,
        source_ref=source_ref or _unique_ref("income"),
        user_rate=user_rate,
    )
    return txn_repo.insert(conn, txn)


def _insert_expense_row(
    conn: sqlite3.Connection,
    *,
    account_id: int,
    amount: Decimal,
    currency: str,
    occurred_at: datetime = FIXED_AT,
    source: str = "test",
    source_ref: str | None = None,
    user_rate: Decimal | None = None,
    description: str | None = None,
) -> Transaction:
    """Insert a plain EXPENSE row via the repo (amount expected negative)."""
    txn = Transaction(
        account_id=account_id,
        occurred_at=occurred_at,
        kind=TransactionKind.EXPENSE,
        amount=amount,
        currency=currency,
        description=description,
        source=source,
        source_ref=source_ref or _unique_ref("expense"),
        user_rate=user_rate,
    )
    return txn_repo.insert(conn, txn)


def _account_id(conn: sqlite3.Connection, name: str) -> int:
    acct = acc_repo.get_by_name(conn, name)
    assert acct is not None and acct.id is not None, f"seed missing {name!r}"
    return acct.id


# ---------------------------------------------------------------------------
# Fake strategies (for Protocol plumbing tests)
# ---------------------------------------------------------------------------


class _RecordingStrategy:
    """Minimal strategy that records every apply() call for assertion."""

    name = "recording"

    def __init__(self, proposals: list[MatchProposal]) -> None:
        self._proposals = proposals
        self.applied: list[MatchProposal] = []

    def match(self) -> list[MatchProposal]:
        return list(self._proposals)

    def apply(self, proposal: MatchProposal) -> None:
        self.applied.append(proposal)


class _FlakyStrategy:
    """Strategy whose apply() raises for a specific proposal index."""

    name = "flaky"

    def __init__(
        self, proposals: list[MatchProposal], *, fail_at: int, error: Exception
    ) -> None:
        self._proposals = proposals
        self._fail_at = fail_at
        self._error = error
        self.apply_call_count = 0

    def match(self) -> list[MatchProposal]:
        return list(self._proposals)

    def apply(self, proposal: MatchProposal) -> None:
        idx = self.apply_call_count
        self.apply_call_count += 1
        if idx == self._fail_at:
            raise self._error


# ---------------------------------------------------------------------------
# TestReconciliation — driver + protocol plumbing
# ---------------------------------------------------------------------------


class TestReconciliation:
    """Covers finances.domain.reconciliation (strategy-agnostic driver)."""

    def test_run_reconciliation_pass_invokes_match_and_apply_on_each_proposal(self):
        proposals = [
            MatchProposal(strategy="recording", details={"i": 0}),
            MatchProposal(strategy="recording", details={"i": 1}),
            MatchProposal(strategy="recording", details={"i": 2}),
        ]
        strategy = _RecordingStrategy(proposals)

        run_reconciliation_pass(strategy)

        assert strategy.applied == proposals

    def test_run_reconciliation_pass_reports_counts_and_strategy_name(self):
        proposals = [
            MatchProposal(strategy="recording", details={"i": n}) for n in range(3)
        ]
        strategy = _RecordingStrategy(proposals)

        report = run_reconciliation_pass(strategy)

        assert isinstance(report, ReconciliationReport)
        assert report.strategy == strategy.name
        assert report.proposals_found == 3
        assert report.proposals_applied == 3
        assert report.errors == []

    def test_run_reconciliation_pass_empty_match_returns_zero_counts(self):
        strategy = _RecordingStrategy([])

        report = run_reconciliation_pass(strategy)

        assert report.proposals_found == 0
        assert report.proposals_applied == 0
        assert report.errors == []
        assert strategy.applied == []

    def test_run_reconciliation_pass_captures_apply_errors_without_aborting(self):
        proposals = [
            MatchProposal(strategy="flaky", details={"i": 0}),
            MatchProposal(strategy="flaky", details={"i": 1}),
            MatchProposal(strategy="flaky", details={"i": 2}),
        ]
        strategy = _FlakyStrategy(
            proposals, fail_at=1, error=ValueError("boom for idx 1")
        )

        report = run_reconciliation_pass(strategy)

        # Pass does not abort: apply() is still called for idx 2.
        assert strategy.apply_call_count == 3
        assert report.proposals_found == 3
        assert report.proposals_applied == 2
        assert len(report.errors) == 1
        # Error is captured as a string (not a raw exception instance) so it
        # survives JSON/report serialization.
        assert isinstance(report.errors[0], str)
        assert "boom for idx 1" in report.errors[0]

    def test_reconciliation_strategy_protocol_accepts_any_duck_typed_class(self):
        """The Protocol must be structural: ANY object exposing name/match/apply
        works, even when it does NOT inherit from ReconciliationStrategy.

        This is the "seam is pluggable" test — EPIC-017 (new strategies) must
        be able to register classes without coupling to this module.
        """

        # Deliberately NOT a subclass of ReconciliationStrategy: proves
        # structural typing rather than nominal inheritance.
        class NoopStrategy:
            name = "noop_duck"

            def __init__(self) -> None:
                self.match_calls = 0
                self.apply_calls = 0

            def match(self) -> list[MatchProposal]:
                self.match_calls += 1
                return [MatchProposal(strategy=self.name, details={})]

            def apply(self, proposal: MatchProposal) -> None:
                self.apply_calls += 1

        # runtime_checkable Protocols with non-method members (e.g. `name: str`)
        # refuse `issubclass` but still accept structurally-compatible classes
        # at call sites. The contract we care about is the call below succeeding
        # with NO explicit registration:
        strat = NoopStrategy()
        report = run_reconciliation_pass(strat)

        assert strat.match_calls == 1
        assert strat.apply_calls == 1
        assert report.strategy == "noop_duck"
        assert report.proposals_applied == 1

    def test_match_proposal_is_frozen_pydantic_model(self):
        # MatchProposal is a frozen Pydantic model (rule-009): downstream
        # code inspects fields via model attrs and ships it via model_dump().
        from pydantic import BaseModel, ValidationError

        assert issubclass(MatchProposal, BaseModel)

        mp = MatchProposal(strategy="x", details={"k": "v"}, confidence=0.9)
        assert mp.strategy == "x"
        assert mp.details == {"k": "v"}
        assert mp.confidence == 0.9
        assert mp.model_dump() == {"strategy": "x", "details": {"k": "v"}, "confidence": 0.9}

        # Frozen: cannot rebind fields after construction.
        with pytest.raises(ValidationError):
            mp.strategy = "mutated"  # type: ignore[misc]


# ---------------------------------------------------------------------------
# TestTransfers — create_transfer, validate, find_unreconciled, strategy
# ---------------------------------------------------------------------------


class TestTransfers:
    """Covers finances.domain.transfers (double-entry engine)."""

    # ---------------- create_transfer: fresh mode ----------------

    def test_create_transfer_fresh_writes_two_rows_with_shared_transfer_id(
        self, seeded_db: sqlite3.Connection
    ):
        funding = _account_id(seeded_db, "Binance Funding")
        spot = _account_id(seeded_db, "Binance Spot")

        pair = create_transfer(
            seeded_db,
            from_account_id=funding,
            to_account_id=spot,
            amount=Decimal("100"),
            currency="USDT",
            occurred_at=FIXED_AT,
            description="internal move",
        )

        assert isinstance(pair, TransferPair)
        from_row = txn_repo.get_by_id(seeded_db, pair.from_transaction_id)
        to_row = txn_repo.get_by_id(seeded_db, pair.to_transaction_id)
        assert from_row is not None and to_row is not None

        assert from_row.kind == TransactionKind.TRANSFER
        assert to_row.kind == TransactionKind.TRANSFER
        assert from_row.transfer_id == to_row.transfer_id == pair.transfer_id
        assert from_row.amount == Decimal("-100")
        assert to_row.amount == Decimal("100")
        assert from_row.currency == "USDT"
        assert to_row.currency == "USDT"
        assert from_row.account_id == funding
        assert to_row.account_id == spot

    def test_create_transfer_fresh_generates_uuid_when_transfer_id_not_provided(
        self, seeded_db: sqlite3.Connection
    ):
        funding = _account_id(seeded_db, "Binance Funding")
        spot = _account_id(seeded_db, "Binance Spot")

        pair = create_transfer(
            seeded_db,
            from_account_id=funding,
            to_account_id=spot,
            amount=Decimal("50"),
            currency="USDT",
            occurred_at=FIXED_AT,
        )

        # UUID4 parses without raising; version field == 4.
        parsed = uuid.UUID(pair.transfer_id)
        assert parsed.version == 4
        assert str(parsed) == pair.transfer_id

    def test_create_transfer_fresh_accepts_explicit_transfer_id(
        self, seeded_db: sqlite3.Connection
    ):
        funding = _account_id(seeded_db, "Binance Funding")
        spot = _account_id(seeded_db, "Binance Spot")

        pair = create_transfer(
            seeded_db,
            from_account_id=funding,
            to_account_id=spot,
            amount=Decimal("25"),
            currency="USDT",
            occurred_at=FIXED_AT,
            transfer_id="explicit-abc",
        )

        assert pair.transfer_id == "explicit-abc"
        from_row = txn_repo.get_by_id(seeded_db, pair.from_transaction_id)
        to_row = txn_repo.get_by_id(seeded_db, pair.to_transaction_id)
        assert from_row is not None and to_row is not None
        assert from_row.transfer_id == "explicit-abc"
        assert to_row.transfer_id == "explicit-abc"

    def test_create_transfer_fresh_rejects_same_account_both_sides(
        self, seeded_db: sqlite3.Connection
    ):
        funding = _account_id(seeded_db, "Binance Funding")

        with pytest.raises(ValueError):
            create_transfer(
                seeded_db,
                from_account_id=funding,
                to_account_id=funding,
                amount=Decimal("10"),
                currency="USDT",
                occurred_at=FIXED_AT,
            )

    @pytest.mark.parametrize(
        "omit",
        ["from_account_id", "to_account_id", "amount", "currency", "occurred_at"],
    )
    def test_create_transfer_fresh_rejects_missing_required_fields(
        self, seeded_db: sqlite3.Connection, omit: str
    ):
        funding = _account_id(seeded_db, "Binance Funding")
        spot = _account_id(seeded_db, "Binance Spot")

        kwargs: dict[str, Any] = {
            "from_account_id": funding,
            "to_account_id": spot,
            "amount": Decimal("10"),
            "currency": "USDT",
            "occurred_at": FIXED_AT,
        }
        kwargs[omit] = None

        with pytest.raises(ValueError):
            create_transfer(seeded_db, **kwargs)

    @pytest.mark.parametrize("bad_amount", [Decimal("0"), Decimal("-5")])
    def test_create_transfer_fresh_rejects_non_positive_amount(
        self, seeded_db: sqlite3.Connection, bad_amount: Decimal
    ):
        funding = _account_id(seeded_db, "Binance Funding")
        spot = _account_id(seeded_db, "Binance Spot")

        with pytest.raises(ValueError):
            create_transfer(
                seeded_db,
                from_account_id=funding,
                to_account_id=spot,
                amount=bad_amount,
                currency="USDT",
                occurred_at=FIXED_AT,
            )

    # ---------------- create_transfer: anchor-only mode ----------------

    def test_create_transfer_with_anchor_updates_anchor_and_inserts_counterpart(
        self, seeded_db: sqlite3.Connection
    ):
        funding = _account_id(seeded_db, "Binance Funding")
        spot = _account_id(seeded_db, "Binance Spot")

        anchor = _insert_income_row(
            seeded_db,
            account_id=funding,
            amount=Decimal("-100"),  # from-leg side: outgoing
            currency="USDT",
        )
        assert anchor.id is not None

        pair = create_transfer(
            seeded_db,
            anchor_transaction_id=anchor.id,
            from_account_id=funding,
            to_account_id=spot,
            amount=Decimal("100"),
            currency="USDT",
            occurred_at=FIXED_AT,
        )

        # Anchor row promoted in place.
        anchor_reloaded = txn_repo.get_by_id(seeded_db, anchor.id)
        assert anchor_reloaded is not None
        assert anchor_reloaded.kind == TransactionKind.TRANSFER
        assert anchor_reloaded.transfer_id == pair.transfer_id

        # Counterpart is a NEW row on the other account.
        counterpart_id = (
            pair.to_transaction_id
            if pair.from_transaction_id == anchor.id
            else pair.from_transaction_id
        )
        counterpart = txn_repo.get_by_id(seeded_db, counterpart_id)
        assert counterpart is not None
        assert counterpart.id != anchor.id
        assert counterpart.account_id == spot
        assert counterpart.kind == TransactionKind.TRANSFER
        assert counterpart.transfer_id == pair.transfer_id
        # Same-currency pair must net to zero.
        assert anchor_reloaded.amount + counterpart.amount == Decimal("0")

    def test_create_transfer_with_anchor_on_to_account_inverts_role(
        self, seeded_db: sqlite3.Connection
    ):
        funding = _account_id(seeded_db, "Binance Funding")
        spot = _account_id(seeded_db, "Binance Spot")

        # Anchor sits on the DESTINATION account: should become the to-leg.
        anchor = _insert_income_row(
            seeded_db,
            account_id=spot,
            amount=Decimal("100"),
            currency="USDT",
        )
        assert anchor.id is not None

        pair = create_transfer(
            seeded_db,
            anchor_transaction_id=anchor.id,
            from_account_id=funding,
            to_account_id=spot,
            amount=Decimal("100"),
            currency="USDT",
            occurred_at=FIXED_AT,
        )

        assert pair.to_transaction_id == anchor.id
        assert pair.from_transaction_id != anchor.id

        from_row = txn_repo.get_by_id(seeded_db, pair.from_transaction_id)
        to_row = txn_repo.get_by_id(seeded_db, pair.to_transaction_id)
        assert from_row is not None and to_row is not None
        assert from_row.account_id == funding
        assert to_row.account_id == spot
        assert from_row.amount == Decimal("-100")
        assert to_row.amount == Decimal("100")

    def test_create_transfer_with_anchor_defaults_amount_currency_from_anchor(
        self, seeded_db: sqlite3.Connection
    ):
        funding = _account_id(seeded_db, "Binance Funding")
        spot = _account_id(seeded_db, "Binance Spot")

        anchor = _insert_income_row(
            seeded_db,
            account_id=funding,
            amount=Decimal("-73.42"),
            currency="USDT",
            occurred_at=FIXED_AT,
        )
        assert anchor.id is not None

        pair = create_transfer(
            seeded_db,
            anchor_transaction_id=anchor.id,
            to_account_id=spot,
            # Intentionally omit amount/currency/from_account_id/occurred_at:
            # they all default off the anchor.
        )

        counterpart_id = (
            pair.to_transaction_id
            if pair.from_transaction_id == anchor.id
            else pair.from_transaction_id
        )
        counterpart = txn_repo.get_by_id(seeded_db, counterpart_id)
        anchor_reloaded = txn_repo.get_by_id(seeded_db, anchor.id)
        assert counterpart is not None and anchor_reloaded is not None

        assert counterpart.currency == anchor_reloaded.currency == "USDT"
        # Opposite signs → sum to zero.
        assert counterpart.amount + anchor_reloaded.amount == Decimal("0")

    def test_create_transfer_anchor_nonexistent_raises(
        self, seeded_db: sqlite3.Connection
    ):
        funding = _account_id(seeded_db, "Binance Funding")
        spot = _account_id(seeded_db, "Binance Spot")

        with pytest.raises(ValueError):
            create_transfer(
                seeded_db,
                anchor_transaction_id=99999,
                from_account_id=funding,
                to_account_id=spot,
                amount=Decimal("10"),
                currency="USDT",
                occurred_at=FIXED_AT,
            )

    def test_create_transfer_anchor_account_unrelated_raises(
        self, seeded_db: sqlite3.Connection
    ):
        funding = _account_id(seeded_db, "Binance Funding")
        spot = _account_id(seeded_db, "Binance Spot")
        cash = _account_id(seeded_db, "Cash USD")

        # Anchor lives on Cash USD but the transfer is between Funding↔Spot.
        anchor = _insert_income_row(
            seeded_db,
            account_id=cash,
            amount=Decimal("100"),
            currency="USD",
        )
        assert anchor.id is not None

        with pytest.raises(ValueError):
            create_transfer(
                seeded_db,
                anchor_transaction_id=anchor.id,
                from_account_id=funding,
                to_account_id=spot,
                amount=Decimal("100"),
                currency="USDT",
                occurred_at=FIXED_AT,
            )

    # ---------------- create_transfer: both-anchors mode ----------------

    def test_create_transfer_both_anchors_promotes_existing_rows_to_transfer(
        self, seeded_db: sqlite3.Connection
    ):
        provincial = _account_id(seeded_db, "Provincial Bolivares")
        spot = _account_id(seeded_db, "Binance Spot")

        # (a) Provincial bolivar deposit (income side).
        a = _insert_income_row(
            seeded_db,
            account_id=provincial,
            amount=Decimal("12000"),
            currency="VES",
            source="provincial",
        )
        # (b) Binance USDT P2P sell (expense side, negative amount, user_rate set).
        b = _insert_expense_row(
            seeded_db,
            account_id=spot,
            amount=Decimal("-10"),
            currency="USDT",
            source="binance",
            user_rate=Decimal("1200"),
        )
        assert a.id is not None and b.id is not None

        pair = create_transfer(
            seeded_db,
            anchor_transaction_id=a.id,
            counterpart_transaction_id=b.id,
        )

        a_reloaded = txn_repo.get_by_id(seeded_db, a.id)
        b_reloaded = txn_repo.get_by_id(seeded_db, b.id)
        assert a_reloaded is not None and b_reloaded is not None

        assert a_reloaded.kind == TransactionKind.TRANSFER
        assert b_reloaded.kind == TransactionKind.TRANSFER
        assert a_reloaded.transfer_id == b_reloaded.transfer_id == pair.transfer_id
        # Amounts + accounts untouched.
        assert a_reloaded.amount == Decimal("12000")
        assert b_reloaded.amount == Decimal("-10")
        assert a_reloaded.account_id == provincial
        assert b_reloaded.account_id == spot

    def test_create_transfer_both_anchors_same_account_raises(
        self, seeded_db: sqlite3.Connection
    ):
        spot = _account_id(seeded_db, "Binance Spot")
        a = _insert_income_row(
            seeded_db, account_id=spot, amount=Decimal("100"), currency="USDT"
        )
        b = _insert_expense_row(
            seeded_db, account_id=spot, amount=Decimal("-100"), currency="USDT"
        )
        assert a.id is not None and b.id is not None

        with pytest.raises(ValueError):
            create_transfer(
                seeded_db,
                anchor_transaction_id=a.id,
                counterpart_transaction_id=b.id,
            )

    def test_create_transfer_both_anchors_mismatched_same_currency_amounts_raises(
        self, seeded_db: sqlite3.Connection
    ):
        funding = _account_id(seeded_db, "Binance Funding")
        spot = _account_id(seeded_db, "Binance Spot")

        a = _insert_income_row(
            seeded_db, account_id=funding, amount=Decimal("100"), currency="USDT"
        )
        # Same sign — DOES NOT net to zero; tolerance check must reject it.
        b = _insert_income_row(
            seeded_db, account_id=spot, amount=Decimal("100"), currency="USDT"
        )
        assert a.id is not None and b.id is not None

        with pytest.raises(ValueError):
            create_transfer(
                seeded_db,
                anchor_transaction_id=a.id,
                counterpart_transaction_id=b.id,
            )

    def test_create_transfer_both_anchors_honors_existing_transfer_id_if_given(
        self, seeded_db: sqlite3.Connection
    ):
        funding = _account_id(seeded_db, "Binance Funding")
        spot = _account_id(seeded_db, "Binance Spot")

        a = _insert_income_row(
            seeded_db, account_id=funding, amount=Decimal("50"), currency="USDT"
        )
        b = _insert_expense_row(
            seeded_db, account_id=spot, amount=Decimal("-50"), currency="USDT"
        )
        assert a.id is not None and b.id is not None

        pair = create_transfer(
            seeded_db,
            anchor_transaction_id=a.id,
            counterpart_transaction_id=b.id,
            transfer_id="fixed-xyz",
        )

        assert pair.transfer_id == "fixed-xyz"
        a_reloaded = txn_repo.get_by_id(seeded_db, a.id)
        b_reloaded = txn_repo.get_by_id(seeded_db, b.id)
        assert a_reloaded is not None and b_reloaded is not None
        assert a_reloaded.transfer_id == "fixed-xyz"
        assert b_reloaded.transfer_id == "fixed-xyz"

    def test_create_transfer_both_anchors_rejects_rows_already_in_other_transfer(
        self, seeded_db: sqlite3.Connection
    ):
        funding = _account_id(seeded_db, "Binance Funding")
        spot = _account_id(seeded_db, "Binance Spot")

        a = _insert_income_row(
            seeded_db, account_id=funding, amount=Decimal("40"), currency="USDT"
        )
        b = _insert_expense_row(
            seeded_db, account_id=spot, amount=Decimal("-40"), currency="USDT"
        )
        assert a.id is not None and b.id is not None

        # deliberate malformed fixture — bypasses create_transfer so row (a)
        # carries a conflicting transfer_id. The new create_transfer call must
        # refuse to silently overwrite it.
        seeded_db.execute(
            "UPDATE transactions SET transfer_id = ? WHERE id = ?", ("other", a.id)
        )

        with pytest.raises(ValueError):
            create_transfer(
                seeded_db,
                anchor_transaction_id=a.id,
                counterpart_transaction_id=b.id,
                transfer_id="new",
            )

    # ---------------- create_transfer: general rules ----------------

    def test_create_transfer_result_contains_correct_from_and_to_ids(
        self, seeded_db: sqlite3.Connection
    ):
        funding = _account_id(seeded_db, "Binance Funding")
        spot = _account_id(seeded_db, "Binance Spot")

        pair = create_transfer(
            seeded_db,
            from_account_id=funding,
            to_account_id=spot,
            amount=Decimal("10"),
            currency="USDT",
            occurred_at=FIXED_AT,
        )

        from_row = txn_repo.get_by_id(seeded_db, pair.from_transaction_id)
        to_row = txn_repo.get_by_id(seeded_db, pair.to_transaction_id)
        assert from_row is not None and to_row is not None
        assert from_row.account_id == funding
        assert to_row.account_id == spot

    # ---------------- validate ----------------

    def test_validate_passes_for_balanced_same_currency_pair(
        self, seeded_db: sqlite3.Connection
    ):
        funding = _account_id(seeded_db, "Binance Funding")
        spot = _account_id(seeded_db, "Binance Spot")

        pair = create_transfer(
            seeded_db,
            from_account_id=funding,
            to_account_id=spot,
            amount=Decimal("123.45"),
            currency="USDT",
            occurred_at=FIXED_AT,
        )

        assert validate(seeded_db, pair.transfer_id) is True

    def test_validate_fails_when_only_one_leg_exists(
        self, seeded_db: sqlite3.Connection
    ):
        funding = _account_id(seeded_db, "Binance Funding")
        spot = _account_id(seeded_db, "Binance Spot")

        pair = create_transfer(
            seeded_db,
            from_account_id=funding,
            to_account_id=spot,
            amount=Decimal("10"),
            currency="USDT",
            occurred_at=FIXED_AT,
        )
        # Delete one leg via raw SQL to strand the transfer_id.
        seeded_db.execute(
            "DELETE FROM transactions WHERE id = ?", (pair.from_transaction_id,)
        )

        assert validate(seeded_db, pair.transfer_id) is False

    def test_validate_fails_when_amounts_do_not_net_to_zero(
        self, seeded_db: sqlite3.Connection
    ):
        funding = _account_id(seeded_db, "Binance Funding")
        spot = _account_id(seeded_db, "Binance Spot")

        # deliberate malformed fixture — bypasses create_transfer so the legs
        # can be saved with kind='transfer' + a shared transfer_id but amounts
        # that DO NOT net to zero. This is exactly the case validate() must
        # catch on a subsequent audit pass.
        bad_id = "bad-transfer-id"
        a = Transaction(
            account_id=funding,
            occurred_at=FIXED_AT,
            kind=TransactionKind.TRANSFER,
            amount=Decimal("100"),
            currency="USDT",
            transfer_id=bad_id,
            source="test",
            source_ref=_unique_ref("bad-a"),
        )
        b = Transaction(
            account_id=spot,
            occurred_at=FIXED_AT,
            kind=TransactionKind.TRANSFER,
            amount=Decimal("99"),
            currency="USDT",
            transfer_id=bad_id,
            source="test",
            source_ref=_unique_ref("bad-b"),
        )
        txn_repo.insert(seeded_db, a)
        txn_repo.insert(seeded_db, b)

        assert validate(seeded_db, bad_id) is False

    def test_validate_fails_for_unknown_transfer_id(
        self, seeded_db: sqlite3.Connection
    ):
        assert validate(seeded_db, "never-existed") is False

    def test_validate_passes_for_cross_currency_with_user_rates(
        self, seeded_db: sqlite3.Connection
    ):
        provincial = _account_id(seeded_db, "Provincial Bolivares")
        spot = _account_id(seeded_db, "Binance Spot")

        # deliberate malformed fixture — we need explicit user_rate values on
        # each leg; create_transfer would default those, so we craft the pair
        # directly. Amounts are chosen so |−10 USDT × 1 USD/USDT + 1200 VES ×
        # 0.00833333... USD/VES| ≈ 0 within Decimal("0.01") tolerance.
        shared = "xcur-transfer"
        usdt_leg = Transaction(
            account_id=spot,
            occurred_at=FIXED_AT,
            kind=TransactionKind.TRANSFER,
            amount=Decimal("-10"),
            currency="USDT",
            user_rate=Decimal("1"),  # USD per USDT
            transfer_id=shared,
            source="test",
            source_ref=_unique_ref("xcur-usdt"),
        )
        ves_leg = Transaction(
            account_id=provincial,
            occurred_at=FIXED_AT,
            kind=TransactionKind.TRANSFER,
            amount=Decimal("1200"),
            currency="VES",
            user_rate=Decimal("0.00833333333333"),  # USD per VES → 1200 × rate ≈ 10
            transfer_id=shared,
            source="test",
            source_ref=_unique_ref("xcur-ves"),
        )
        txn_repo.insert(seeded_db, usdt_leg)
        txn_repo.insert(seeded_db, ves_leg)

        assert validate(seeded_db, shared) is True

    def test_validate_fails_for_cross_currency_without_user_rate(
        self, seeded_db: sqlite3.Connection
    ):
        provincial = _account_id(seeded_db, "Provincial Bolivares")
        spot = _account_id(seeded_db, "Binance Spot")

        # deliberate malformed fixture — different currencies and at least one
        # leg lacks user_rate; validate() has no way to compute equivalence.
        shared = "xcur-missing-rate"
        a = Transaction(
            account_id=spot,
            occurred_at=FIXED_AT,
            kind=TransactionKind.TRANSFER,
            amount=Decimal("-10"),
            currency="USDT",
            user_rate=Decimal("1"),
            transfer_id=shared,
            source="test",
            source_ref=_unique_ref("mr-a"),
        )
        b = Transaction(
            account_id=provincial,
            occurred_at=FIXED_AT,
            kind=TransactionKind.TRANSFER,
            amount=Decimal("1200"),
            currency="VES",
            user_rate=None,  # <-- missing
            transfer_id=shared,
            source="test",
            source_ref=_unique_ref("mr-b"),
        )
        txn_repo.insert(seeded_db, a)
        txn_repo.insert(seeded_db, b)

        assert validate(seeded_db, shared) is False

    def test_validate_custom_tolerance(self, seeded_db: sqlite3.Connection):
        funding = _account_id(seeded_db, "Binance Funding")
        spot = _account_id(seeded_db, "Binance Spot")

        # deliberate malformed fixture — pair sums to 0.05 so it fails the
        # default tolerance (0.01) but passes the custom one (0.10).
        shared = "tol-check"
        a = Transaction(
            account_id=funding,
            occurred_at=FIXED_AT,
            kind=TransactionKind.TRANSFER,
            amount=Decimal("-10.00"),
            currency="USDT",
            transfer_id=shared,
            source="test",
            source_ref=_unique_ref("tol-a"),
        )
        b = Transaction(
            account_id=spot,
            occurred_at=FIXED_AT,
            kind=TransactionKind.TRANSFER,
            amount=Decimal("10.05"),
            currency="USDT",
            transfer_id=shared,
            source="test",
            source_ref=_unique_ref("tol-b"),
        )
        txn_repo.insert(seeded_db, a)
        txn_repo.insert(seeded_db, b)

        assert validate(seeded_db, shared) is False
        assert validate(seeded_db, shared, tolerance=Decimal("0.10")) is True

    # ---------------- find_unreconciled ----------------

    def test_find_unreconciled_returns_empty_when_all_pairs_valid(
        self, seeded_db: sqlite3.Connection
    ):
        funding = _account_id(seeded_db, "Binance Funding")
        spot = _account_id(seeded_db, "Binance Spot")

        create_transfer(
            seeded_db,
            from_account_id=funding,
            to_account_id=spot,
            amount=Decimal("20"),
            currency="USDT",
            occurred_at=FIXED_AT,
        )

        assert find_unreconciled(seeded_db) == []

    def test_find_unreconciled_returns_orphan_when_one_leg_missing(
        self, seeded_db: sqlite3.Connection
    ):
        funding = _account_id(seeded_db, "Binance Funding")
        spot = _account_id(seeded_db, "Binance Spot")

        pair = create_transfer(
            seeded_db,
            from_account_id=funding,
            to_account_id=spot,
            amount=Decimal("20"),
            currency="USDT",
            occurred_at=FIXED_AT,
        )
        seeded_db.execute(
            "DELETE FROM transactions WHERE id = ?", (pair.from_transaction_id,)
        )

        rows = find_unreconciled(seeded_db)
        assert len(rows) == 1
        row = rows[0]
        assert set(row.keys()) >= {
            "transfer_id",
            "leg_count",
            "transaction_ids",
            "account_ids",
        }
        assert row["transfer_id"] == pair.transfer_id

    def test_find_unreconciled_catches_null_transfer_id_rows(
        self, seeded_db: sqlite3.Connection
    ):
        funding = _account_id(seeded_db, "Binance Funding")

        # deliberate malformed fixture — a kind='transfer' row with NULL
        # transfer_id should never happen in production, but if the DB ever
        # gets into that state v_unreconciled_transfers must surface it so we
        # can clean it up.
        orphan = Transaction(
            account_id=funding,
            occurred_at=FIXED_AT,
            kind=TransactionKind.TRANSFER,
            amount=Decimal("5"),
            currency="USDT",
            transfer_id=None,
            source="test",
            source_ref=_unique_ref("null-tid"),
        )
        txn_repo.insert(seeded_db, orphan)

        rows = find_unreconciled(seeded_db)
        assert any(r["transfer_id"] is None for r in rows)

    # ---------------- BankAnchoredP2pPairing ----------------

    def test_bank_anchored_pairing_matches_same_day_deposit_and_sell(
        self, seeded_db: sqlite3.Connection
    ):
        provincial = _account_id(seeded_db, "Provincial Bolivares")
        spot = _account_id(seeded_db, "Binance Spot")

        bank = _insert_income_row(
            seeded_db,
            account_id=provincial,
            amount=Decimal("12000"),
            currency="VES",
            source="provincial",
            occurred_at=FIXED_AT,
        )
        binance = _insert_expense_row(
            seeded_db,
            account_id=spot,
            amount=Decimal("-10"),
            currency="USDT",
            source="binance",
            occurred_at=FIXED_AT,
            user_rate=Decimal("1200"),
        )
        assert bank.id is not None and binance.id is not None

        strat = BankAnchoredP2pPairing(seeded_db)
        proposals = strat.match()

        assert len(proposals) == 1
        p = proposals[0]
        assert p.strategy == "bank_anchored_p2p_pairing"
        assert p.details["bank_transaction_id"] == bank.id
        assert p.details["binance_transaction_id"] == binance.id

        strat.apply(p)

        bank_r = txn_repo.get_by_id(seeded_db, bank.id)
        binance_r = txn_repo.get_by_id(seeded_db, binance.id)
        assert bank_r is not None and binance_r is not None
        assert bank_r.kind == TransactionKind.TRANSFER
        assert binance_r.kind == TransactionKind.TRANSFER
        assert bank_r.transfer_id is not None
        assert bank_r.transfer_id == binance_r.transfer_id

    @pytest.mark.parametrize(
        "delta_days, should_match",
        [(2, True), (3, False)],
    )
    def test_bank_anchored_pairing_respects_window_days_boundary(
        self,
        seeded_db: sqlite3.Connection,
        delta_days: int,
        should_match: bool,
    ):
        provincial = _account_id(seeded_db, "Provincial Bolivares")
        spot = _account_id(seeded_db, "Binance Spot")

        _insert_income_row(
            seeded_db,
            account_id=provincial,
            amount=Decimal("12000"),
            currency="VES",
            source="provincial",
            occurred_at=FIXED_AT,
        )
        _insert_expense_row(
            seeded_db,
            account_id=spot,
            amount=Decimal("-10"),
            currency="USDT",
            source="binance",
            occurred_at=FIXED_AT + timedelta(days=delta_days),
            user_rate=Decimal("1200"),
        )

        strat = BankAnchoredP2pPairing(seeded_db, window_days=2)
        proposals = strat.match()

        if should_match:
            assert len(proposals) == 1
        else:
            assert proposals == []

    def test_bank_anchored_pairing_ignores_already_paired_rows(
        self, seeded_db: sqlite3.Connection
    ):
        provincial = _account_id(seeded_db, "Provincial Bolivares")
        spot = _account_id(seeded_db, "Binance Spot")

        bank = _insert_income_row(
            seeded_db,
            account_id=provincial,
            amount=Decimal("12000"),
            currency="VES",
            source="provincial",
            occurred_at=FIXED_AT,
        )
        _insert_expense_row(
            seeded_db,
            account_id=spot,
            amount=Decimal("-10"),
            currency="USDT",
            source="binance",
            occurred_at=FIXED_AT,
            user_rate=Decimal("1200"),
        )

        # deliberate malformed fixture — stamp the bank row with a transfer_id
        # WITHOUT going through create_transfer so we can verify the strategy
        # skips already-paired rows regardless of whether the pairing is valid.
        seeded_db.execute(
            "UPDATE transactions SET transfer_id = ? WHERE id = ?",
            ("pre-existing", bank.id),
        )

        strat = BankAnchoredP2pPairing(seeded_db)
        assert strat.match() == []

    def test_bank_anchored_pairing_requires_binance_user_rate(
        self, seeded_db: sqlite3.Connection
    ):
        provincial = _account_id(seeded_db, "Provincial Bolivares")
        spot = _account_id(seeded_db, "Binance Spot")

        _insert_income_row(
            seeded_db,
            account_id=provincial,
            amount=Decimal("12000"),
            currency="VES",
            source="provincial",
            occurred_at=FIXED_AT,
        )
        _insert_expense_row(
            seeded_db,
            account_id=spot,
            amount=Decimal("-10"),
            currency="USDT",
            source="binance",
            occurred_at=FIXED_AT,
            user_rate=None,  # <-- missing — cannot compute equivalence
        )

        strat = BankAnchoredP2pPairing(seeded_db)
        assert strat.match() == []

    def test_bank_anchored_pairing_tolerance_excludes_large_diff(
        self, seeded_db: sqlite3.Connection
    ):
        provincial = _account_id(seeded_db, "Provincial Bolivares")
        spot = _account_id(seeded_db, "Binance Spot")

        _insert_income_row(
            seeded_db,
            account_id=provincial,
            amount=Decimal("12000"),
            currency="VES",
            source="provincial",
            occurred_at=FIXED_AT,
        )
        # 9 USDT × 1200 VES/USDT = 10 800 VES → ~10% diff against 12 000 VES,
        # well above the default 2% tolerance.
        _insert_expense_row(
            seeded_db,
            account_id=spot,
            amount=Decimal("-9"),
            currency="USDT",
            source="binance",
            occurred_at=FIXED_AT,
            user_rate=Decimal("1200"),
        )

        strat = BankAnchoredP2pPairing(seeded_db)
        assert strat.match() == []

    def test_bank_anchored_pairing_via_run_reconciliation_pass_end_to_end(
        self, seeded_db: sqlite3.Connection
    ):
        provincial = _account_id(seeded_db, "Provincial Bolivares")
        spot = _account_id(seeded_db, "Binance Spot")

        bank = _insert_income_row(
            seeded_db,
            account_id=provincial,
            amount=Decimal("12000"),
            currency="VES",
            source="provincial",
            occurred_at=FIXED_AT,
        )
        binance = _insert_expense_row(
            seeded_db,
            account_id=spot,
            amount=Decimal("-10"),
            currency="USDT",
            source="binance",
            occurred_at=FIXED_AT,
            user_rate=Decimal("1200"),
        )
        assert bank.id is not None and binance.id is not None

        report = run_reconciliation_pass(BankAnchoredP2pPairing(seeded_db))

        assert report.strategy == "bank_anchored_p2p_pairing"
        assert report.proposals_found == 1
        assert report.proposals_applied == 1
        assert report.errors == []

        bank_r = txn_repo.get_by_id(seeded_db, bank.id)
        binance_r = txn_repo.get_by_id(seeded_db, binance.id)
        assert bank_r is not None and binance_r is not None
        assert bank_r.kind == TransactionKind.TRANSFER
        assert binance_r.kind == TransactionKind.TRANSFER
        assert bank_r.transfer_id == binance_r.transfer_id
        assert bank_r.transfer_id is not None

    def test_bank_anchored_pairing_skips_ambiguous_matches(
        self, seeded_db: sqlite3.Connection
    ):
        provincial = _account_id(seeded_db, "Provincial Bolivares")
        spot = _account_id(seeded_db, "Binance Spot")

        _insert_income_row(
            seeded_db,
            account_id=provincial,
            amount=Decimal("12000"),
            currency="VES",
            source="provincial",
            occurred_at=FIXED_AT,
        )
        # TWO candidate Binance sells, both inside the tolerance & window.
        _insert_expense_row(
            seeded_db,
            account_id=spot,
            amount=Decimal("-10"),
            currency="USDT",
            source="binance",
            occurred_at=FIXED_AT,
            user_rate=Decimal("1200"),
        )
        _insert_expense_row(
            seeded_db,
            account_id=spot,
            amount=Decimal("-10"),
            currency="USDT",
            source="binance",
            occurred_at=FIXED_AT + timedelta(hours=1),
            user_rate=Decimal("1200"),
        )

        strat = BankAnchoredP2pPairing(seeded_db)
        # Ambiguous → exactly-one rule means NO proposal is emitted.
        assert strat.match() == []


# ---------------------------------------------------------------------------
# TestEdgeCases — branch coverage for defensive paths
# ---------------------------------------------------------------------------


class TestEdgeCases:
    """Cover remaining defensive branches to satisfy the 85% coverage gate."""

    def test_create_transfer_counterpart_without_anchor_raises(
        self, seeded_db: sqlite3.Connection
    ):
        with pytest.raises(ValueError):
            create_transfer(seeded_db, counterpart_transaction_id=1)

    def test_create_transfer_both_anchors_nonexistent_counterpart_raises(
        self, seeded_db: sqlite3.Connection
    ):
        funding = _account_id(seeded_db, "Binance Funding")
        anchor = _insert_income_row(
            seeded_db, account_id=funding, amount=Decimal("100"), currency="USDT"
        )
        assert anchor.id is not None
        with pytest.raises(ValueError):
            create_transfer(
                seeded_db,
                anchor_transaction_id=anchor.id,
                counterpart_transaction_id=99999,
            )

    def test_create_transfer_both_anchors_explicit_from_matches_second_row(
        self, seeded_db: sqlite3.Connection
    ):
        funding = _account_id(seeded_db, "Binance Funding")
        spot = _account_id(seeded_db, "Binance Spot")
        row_a = _insert_income_row(
            seeded_db, account_id=funding, amount=Decimal("-50"), currency="USDT"
        )
        row_b = _insert_income_row(
            seeded_db, account_id=spot, amount=Decimal("50"), currency="USDT"
        )
        assert row_a.id is not None and row_b.id is not None

        # Pass from_account_id = spot → row_b becomes the from-leg.
        pair = create_transfer(
            seeded_db,
            anchor_transaction_id=row_a.id,
            counterpart_transaction_id=row_b.id,
            from_account_id=spot,
        )
        assert pair.from_transaction_id == row_b.id
        assert pair.to_transaction_id == row_a.id

    def test_create_transfer_both_anchors_same_sign_without_from_id_raises(
        self, seeded_db: sqlite3.Connection
    ):
        funding = _account_id(seeded_db, "Binance Funding")
        spot = _account_id(seeded_db, "Binance Spot")
        row_a = _insert_income_row(
            seeded_db, account_id=funding, amount=Decimal("100"), currency="USDT"
        )
        row_b = _insert_income_row(
            seeded_db, account_id=spot, amount=Decimal("-100"), currency="USDT"
        )
        # Both positive would collide same-currency drift; use mixed signs that net.
        # Now deliberately rewrite row_b to positive to trigger same-sign branch.
        # deliberate malformed fixture: direct SQL to force same-sign both-anchors path.
        seeded_db.execute(
            "UPDATE transactions SET amount = '100' WHERE id = ?", (row_b.id,)
        )
        assert row_a.id is not None and row_b.id is not None
        with pytest.raises(ValueError):
            create_transfer(
                seeded_db,
                anchor_transaction_id=row_a.id,
                counterpart_transaction_id=row_b.id,
            )

    def test_create_transfer_both_anchors_from_id_matches_neither_row_raises(
        self, seeded_db: sqlite3.Connection
    ):
        funding = _account_id(seeded_db, "Binance Funding")
        spot = _account_id(seeded_db, "Binance Spot")
        cash = _account_id(seeded_db, "Cash USD")
        row_a = _insert_income_row(
            seeded_db, account_id=funding, amount=Decimal("-50"), currency="USDT"
        )
        row_b = _insert_income_row(
            seeded_db, account_id=spot, amount=Decimal("50"), currency="USDT"
        )
        assert row_a.id is not None and row_b.id is not None
        with pytest.raises(ValueError):
            create_transfer(
                seeded_db,
                anchor_transaction_id=row_a.id,
                counterpart_transaction_id=row_b.id,
                from_account_id=cash,
            )

    def test_create_transfer_anchor_only_flips_sign_when_anchor_wrong_sign(
        self, seeded_db: sqlite3.Connection
    ):
        """Anchor is on from_account but stored with positive amount — impl flips
        sign on promotion so the from-leg becomes negative."""
        funding = _account_id(seeded_db, "Binance Funding")
        spot = _account_id(seeded_db, "Binance Spot")
        # Anchor stored as +50 but will be the from-leg → should end up -50.
        anchor = _insert_income_row(
            seeded_db, account_id=funding, amount=Decimal("50"), currency="USDT"
        )
        assert anchor.id is not None

        pair = create_transfer(
            seeded_db,
            anchor_transaction_id=anchor.id,
            from_account_id=funding,
            to_account_id=spot,
            amount=Decimal("50"),
            currency="USDT",
            occurred_at=FIXED_AT,
        )
        anchor_after = txn_repo.get_by_id(seeded_db, anchor.id)
        assert anchor_after is not None
        assert anchor_after.amount == Decimal("-50")
        assert pair.from_transaction_id == anchor.id

    def test_create_transfer_anchor_only_rejects_zero_amount(
        self, seeded_db: sqlite3.Connection
    ):
        funding = _account_id(seeded_db, "Binance Funding")
        spot = _account_id(seeded_db, "Binance Spot")
        anchor = _insert_income_row(
            seeded_db, account_id=funding, amount=Decimal("100"), currency="USDT"
        )
        assert anchor.id is not None
        with pytest.raises(ValueError):
            create_transfer(
                seeded_db,
                anchor_transaction_id=anchor.id,
                from_account_id=funding,
                to_account_id=spot,
                amount=Decimal("0"),
                currency="USDT",
                occurred_at=FIXED_AT,
            )

    def test_validate_false_when_legs_share_transfer_id_but_kind_not_transfer(
        self, seeded_db: sqlite3.Connection
    ):
        funding = _account_id(seeded_db, "Binance Funding")
        spot = _account_id(seeded_db, "Binance Spot")
        # deliberate malformed fixture: two INCOME legs sharing a transfer_id
        a = Transaction(
            account_id=funding, occurred_at=FIXED_AT, kind=TransactionKind.INCOME,
            amount=Decimal("-100"), currency="USDT", transfer_id="badkind",
            source="test", source_ref=_unique_ref("a"),
        )
        b = Transaction(
            account_id=spot, occurred_at=FIXED_AT, kind=TransactionKind.INCOME,
            amount=Decimal("100"), currency="USDT", transfer_id="badkind",
            source="test", source_ref=_unique_ref("b"),
        )
        txn_repo.insert(seeded_db, a)
        txn_repo.insert(seeded_db, b)

        assert validate(seeded_db, "badkind") is False

    def test_validate_false_when_both_legs_on_same_account(
        self, seeded_db: sqlite3.Connection
    ):
        funding = _account_id(seeded_db, "Binance Funding")
        # deliberate malformed fixture: two TRANSFER legs on the same account
        a = Transaction(
            account_id=funding, occurred_at=FIXED_AT, kind=TransactionKind.TRANSFER,
            amount=Decimal("-50"), currency="USDT", transfer_id="sameacct",
            source="test", source_ref=_unique_ref("a"),
        )
        b = Transaction(
            account_id=funding, occurred_at=FIXED_AT, kind=TransactionKind.TRANSFER,
            amount=Decimal("50"), currency="USDT", transfer_id="sameacct",
            source="test", source_ref=_unique_ref("b"),
        )
        txn_repo.insert(seeded_db, a)
        txn_repo.insert(seeded_db, b)

        assert validate(seeded_db, "sameacct") is False

    def test_bank_anchored_pairing_skips_zero_amount_bank_row(
        self, seeded_db: sqlite3.Connection
    ):
        bank = _account_id(seeded_db, "Provincial Bolivares")
        # deliberate malformed fixture: a zero-amount bank deposit
        _insert_income_row(
            seeded_db, account_id=bank, amount=Decimal("0"), currency="VES",
            source="provincial", occurred_at=FIXED_AT,
        )
        strat = BankAnchoredP2pPairing(seeded_db)
        assert strat.match() == []
