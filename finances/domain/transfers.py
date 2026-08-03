"""Transfer pairing primitives and strategies (EPIC-006 / ADR-002).

This module implements the three-mode ``create_transfer`` helper, a
``validate`` function that verifies a pairing is self-consistent, a
``find_unreconciled`` query over ``v_unreconciled_transfers``, and the
bank-anchored P2P pairing strategy.

All amounts flow through :class:`decimal.Decimal`; floats are never
used for monetary arithmetic (per ADR-009).
"""
from __future__ import annotations

import re
import sqlite3
import uuid
from datetime import date, datetime
from decimal import Decimal
from typing import Any

from pydantic import BaseModel, ConfigDict

from finances.db.repos import transactions as txn_repo
from finances.domain.models import Transaction, TransactionKind
from finances.domain.reconciliation import MatchProposal


# ---------------------------------------------------------------------------
# SQL constants
# ---------------------------------------------------------------------------

# Bank-anchored P2P pairing: fetch candidate bank deposits (income, no
# transfer, positive amount).
SQL_BANK_DEPOSITS = """
    SELECT id, account_id, occurred_at, amount, currency
    FROM transactions
    WHERE source = :bank_source
      AND kind = 'income'
      AND transfer_id IS NULL
      AND CAST(amount AS REAL) > 0
    ORDER BY occurred_at ASC, id ASC
"""

# Bank-anchored P2P pairing: fetch every unreconciled Binance sell. The
# ±window_days band is applied in Python rather than here because the
# assignment is global — every (deposit, sell) pair is scored before any
# is claimed, so the whole candidate set has to be in hand at once.
#
# We include expense, income and transfer kinds because historical data is
# inconsistent on sign-kind pairing; backfilled sells in particular landed
# as kind='transfer' with a NULL transfer_id (orphan half-transfers) and
# would otherwise be invisible here. The ``transfer_id IS NULL`` guard is
# what actually establishes "unreconciled"; kind alone never does. We
# filter by amount < 0 and require a user_rate.
SQL_BINANCE_CANDIDATES = """
    SELECT id, account_id, occurred_at, amount, currency, user_rate, description
    FROM transactions
    WHERE source = :binance_source
      AND kind IN ('expense', 'income', 'transfer')
      AND transfer_id IS NULL
      AND CAST(amount AS REAL) < 0
      AND user_rate IS NOT NULL
    ORDER BY occurred_at ASC, id ASC
"""

# The fiat a P2P order was priced in survives only in the description that
# ``finances.ingest.binance`` writes:
#
#     P2P SELL USDT @ 630.5170239596469104665825977 VES (order 2289...)
#
# ``user_rate`` itself is a bare number, so without reading this back a
# USD-priced sell converts to a plausible-looking bolivar figure.
_FIAT_IN_DESCRIPTION_RE = re.compile(r"@\s+\S+\s+([A-Za-z]{3,})\s+\(order\b")

# All legs sharing a transfer_id. Used by ``validate``.
SQL_TRANSFER_LEGS = """
    SELECT id, account_id, occurred_at, kind, amount, currency, description,
           category_id, transfer_id, user_rate, source, source_ref, needs_review
    FROM transactions
    WHERE transfer_id = ?
"""

# Unreconciled transfers view.
SQL_UNRECONCILED_VIEW = """
    SELECT transfer_id, leg_count, transaction_ids, account_ids
    FROM v_unreconciled_transfers
"""


# ---------------------------------------------------------------------------
# Public types
# ---------------------------------------------------------------------------

class TransferPair(BaseModel):
    """Identifiers for a paired transfer's two legs."""

    model_config = ConfigDict(frozen=True)

    transfer_id: str
    from_transaction_id: int
    to_transaction_id: int


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _to_decimal(value: Any) -> Decimal:
    """Coerce a DB value to Decimal without passing through float."""
    if value is None:
        raise ValueError("cannot coerce None to Decimal")
    if isinstance(value, Decimal):
        return value
    if isinstance(value, (int, str)):
        return Decimal(str(value))
    # Defensive: sqlite should not hand back floats for TEXT columns, but
    # guard anyway. We stringify rather than cast directly to keep
    # precision predictable.
    return Decimal(str(value))


def _to_decimal_or_none(value: Any) -> Decimal | None:
    if value is None:
        return None
    return _to_decimal(value)


def _decimal_text(value: Decimal) -> str:
    """Serialize a Decimal to the canonical 'plain' text form used in DB."""
    return format(value, "f")


def _iso(value: datetime) -> str:
    return value.isoformat()


# Treated as 1:1 with the dollar (ADR-014). Stablecoins depeg by
# fractions of a percent; the ledger's tolerances are wider than that,
# and a price feed here would not change any decision.
USD_EQUIVALENT_CURRENCIES: frozenset[str] = frozenset({"USD", "USDT", "USDC"})

# How far a cross-currency pair may miss, as a fraction of the larger
# leg. Shared by the pairing strategy (which admits a pair at this
# threshold) and by validate (which afterwards checks it holds).
#
# They must be one constant. When they were two, the strategy paired at
# 2% relative while validate demanded 0.01 absolute, so 21 of the
# ledger's 97 cross-currency transfers were admitted by one rule and
# rejected by the other. Absolute cents are meaningless here anyway: the
# legs are in different units and the rate is itself an approximation of
# a counterparty's quote.
DEFAULT_AMOUNT_TOLERANCE_RATIO: Decimal = Decimal("0.02")


def _to_usd(
    amount: Decimal,
    currency: str,
    rate: Decimal | None,
) -> Decimal | None:
    """Express ``amount`` in USD, or ``None`` if it cannot be priced.

    Per ADR-014 ``user_rate`` is bolívares per dollar, so a non-USD
    amount *divides* by it. Multiplying — which is what this function
    replaced — produces VES²/USD, a quantity with no meaning.
    """
    if currency.upper() in USD_EQUIVALENT_CURRENCIES:
        return amount
    if rate is None or rate <= 0:
        return None
    return amount / rate


# ---------------------------------------------------------------------------
# create_transfer — three modes
# ---------------------------------------------------------------------------

def _promote_to_transfer(
    conn: sqlite3.Connection,
    *,
    transaction_id: int,
    transfer_id: str,
) -> None:
    """Promote a row into a transfer leg and retire its review flag.

    ``needs_review`` asks "could the importer categorise this?" — for a
    paired transfer the question no longer applies, because transfers
    are not categorised at all (rule-006) and triage already drops
    ``kind='transfer'`` from the review surface. Leaving the flag up
    stranded the row: triage called it resolved while every surface
    reading the column directly kept queueing it.

    Only the flag is cleared. ``category_id`` is left as-is so a
    category the owner set by hand survives the promotion.
    """
    conn.execute(
        "UPDATE transactions SET kind = 'transfer', transfer_id = ?, "
        "needs_review = 0, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
        (transfer_id, transaction_id),
    )


def _set_amount(
    conn: sqlite3.Connection,
    *,
    transaction_id: int,
    amount: Decimal,
) -> None:
    conn.execute(
        "UPDATE transactions SET amount = ?, updated_at = CURRENT_TIMESTAMP "
        "WHERE id = ?",
        (_decimal_text(amount), transaction_id),
    )


def _insert_leg(
    conn: sqlite3.Connection,
    *,
    account_id: int,
    occurred_at: datetime,
    amount: Decimal,
    currency: str,
    description: str | None,
    source: str,
    source_ref: str | None,
    transfer_id: str,
    user_rate: Decimal | None,
) -> int:
    txn = Transaction(
        account_id=account_id,
        occurred_at=occurred_at,
        kind=TransactionKind.TRANSFER,
        amount=amount,
        currency=currency,
        description=description,
        transfer_id=transfer_id,
        user_rate=user_rate,
        source=source,
        source_ref=source_ref,
    )
    inserted = txn_repo.insert(conn, txn)
    assert inserted.id is not None  # insert() always populates id
    return inserted.id


def create_transfer(
    conn: sqlite3.Connection,
    *,
    from_account_id: int | None = None,
    to_account_id: int | None = None,
    amount: Decimal | None = None,
    currency: str | None = None,
    occurred_at: datetime | None = None,
    description: str | None = None,
    source: str = "internal",
    source_ref_from: str | None = None,
    source_ref_to: str | None = None,
    user_rate_from: Decimal | None = None,
    user_rate_to: Decimal | None = None,
    anchor_transaction_id: int | None = None,
    counterpart_transaction_id: int | None = None,
    transfer_id: str | None = None,
) -> TransferPair:
    """Create or finalize a double-entry transfer pair.

    Three modes, distinguished by which anchor ids are supplied:

    - **Fresh** (no anchors): insert both legs.
    - **Anchor-only** (anchor only): promote the anchor row and insert
      the counterpart leg on the other account.
    - **Both-anchors** (both ids): promote both existing rows and share
      a ``transfer_id``.
    """
    tid = transfer_id or str(uuid.uuid4())

    # -- Mode 3: both anchors --------------------------------------------------
    if anchor_transaction_id is not None and counterpart_transaction_id is not None:
        row_a = txn_repo.get_by_id(conn, anchor_transaction_id)
        row_b = txn_repo.get_by_id(conn, counterpart_transaction_id)
        if row_a is None or row_b is None:
            raise ValueError("both anchor transactions must exist")
        if row_a.account_id == row_b.account_id:
            raise ValueError("both-anchors legs must be on different accounts")

        # Reject conflicting pre-existing transfer_ids.
        for existing in (row_a.transfer_id, row_b.transfer_id):
            if existing is not None and existing != tid:
                raise ValueError(
                    "existing transfer_id on anchor row conflicts with supplied id"
                )

        # Determine from/to leg.
        if from_account_id is not None:
            if from_account_id == row_a.account_id:
                from_row, to_row = row_a, row_b
            elif from_account_id == row_b.account_id:
                from_row, to_row = row_b, row_a
            else:
                raise ValueError(
                    "from_account_id does not match either anchor's account"
                )
        else:
            a_neg = row_a.amount < 0
            b_neg = row_b.amount < 0
            if a_neg and not b_neg:
                from_row, to_row = row_a, row_b
            elif b_neg and not a_neg:
                from_row, to_row = row_b, row_a
            else:
                # Both same sign: caller must disambiguate.
                raise ValueError(
                    "both anchors share sign; supply from_account_id to "
                    "disambiguate from/to leg"
                )

        # Same-currency drift check. Cross-currency sums are deferred to
        # validate() since they require rate lookup.
        if from_row.currency == to_row.currency:
            drift = abs(from_row.amount + to_row.amount)
            if drift > Decimal("0.01"):
                raise ValueError(
                    f"same-currency both-anchors drift {drift} exceeds tolerance"
                )

        assert from_row.id is not None and to_row.id is not None
        _promote_to_transfer(conn, transaction_id=from_row.id, transfer_id=tid)
        _promote_to_transfer(conn, transaction_id=to_row.id, transfer_id=tid)

        return TransferPair(
            transfer_id=tid,
            from_transaction_id=from_row.id,
            to_transaction_id=to_row.id,
        )

    # -- Mode 2: anchor-only ---------------------------------------------------
    if anchor_transaction_id is not None and counterpart_transaction_id is None:
        anchor = txn_repo.get_by_id(conn, anchor_transaction_id)
        if anchor is None:
            raise ValueError(f"anchor transaction {anchor_transaction_id} not found")

        # Infer the missing side from the anchor's account when possible.
        if from_account_id is None and to_account_id is not None:
            from_account_id = anchor.account_id
        elif to_account_id is None and from_account_id is not None:
            to_account_id = anchor.account_id
        elif from_account_id is None and to_account_id is None:
            raise ValueError(
                "anchor-only mode requires at least one of from_account_id / to_account_id"
            )
        if from_account_id == to_account_id:
            raise ValueError("from_account_id and to_account_id must differ")

        if anchor.account_id == from_account_id:
            anchor_is_from = True
        elif anchor.account_id == to_account_id:
            anchor_is_from = False
        else:
            raise ValueError(
                "anchor.account_id must equal from_account_id or to_account_id"
            )

        # Resolve defaults from anchor row when not supplied.
        resolved_amount = abs(anchor.amount) if amount is None else abs(amount)
        if resolved_amount == 0:
            raise ValueError("transfer amount must be non-zero")
        resolved_currency = currency if currency is not None else anchor.currency
        resolved_when = occurred_at if occurred_at is not None else anchor.occurred_at

        # Anchor-side target sign.
        anchor_signed = -resolved_amount if anchor_is_from else resolved_amount

        # Promote anchor; flip sign if the current row sign doesn't match
        # its resolved role.
        assert anchor.id is not None
        _promote_to_transfer(conn, transaction_id=anchor.id, transfer_id=tid)
        if anchor.amount != anchor_signed:
            _set_amount(conn, transaction_id=anchor.id, amount=anchor_signed)

        # Insert counterpart leg on the other account, opposite sign.
        counterpart_account = (
            to_account_id if anchor_is_from else from_account_id
        )
        counterpart_amount = (
            resolved_amount if anchor_is_from else -resolved_amount
        )
        # Per-leg source_ref/user_rate mapping.
        if anchor_is_from:
            counterpart_source_ref = source_ref_to
            counterpart_user_rate = user_rate_to
        else:
            counterpart_source_ref = source_ref_from
            counterpart_user_rate = user_rate_from

        counterpart_id = _insert_leg(
            conn,
            account_id=counterpart_account,
            occurred_at=resolved_when,
            amount=counterpart_amount,
            currency=resolved_currency,
            description=description,
            source=source,
            source_ref=counterpart_source_ref,
            transfer_id=tid,
            user_rate=counterpart_user_rate,
        )

        if anchor_is_from:
            return TransferPair(
                transfer_id=tid,
                from_transaction_id=anchor.id,
                to_transaction_id=counterpart_id,
            )
        return TransferPair(
            transfer_id=tid,
            from_transaction_id=counterpart_id,
            to_transaction_id=anchor.id,
        )

    # -- Mode 1: fresh (no anchors) -------------------------------------------
    if anchor_transaction_id is None and counterpart_transaction_id is None:
        if (
            from_account_id is None
            or to_account_id is None
            or amount is None
            or currency is None
            or occurred_at is None
        ):
            raise ValueError(
                "fresh mode requires from_account_id, to_account_id, amount, "
                "currency, and occurred_at"
            )
        if from_account_id == to_account_id:
            raise ValueError("from_account_id and to_account_id must differ")
        if amount <= 0:
            raise ValueError("amount must be positive non-zero for fresh transfers")

        from_id = _insert_leg(
            conn,
            account_id=from_account_id,
            occurred_at=occurred_at,
            amount=-amount,
            currency=currency,
            description=description,
            source=source,
            source_ref=source_ref_from,
            transfer_id=tid,
            user_rate=user_rate_from,
        )
        to_id = _insert_leg(
            conn,
            account_id=to_account_id,
            occurred_at=occurred_at,
            amount=amount,
            currency=currency,
            description=description,
            source=source,
            source_ref=source_ref_to,
            transfer_id=tid,
            user_rate=user_rate_to,
        )
        return TransferPair(
            transfer_id=tid, from_transaction_id=from_id, to_transaction_id=to_id
        )

    # counterpart without anchor isn't a defined mode.
    raise ValueError(
        "invalid combination of anchor_transaction_id and counterpart_transaction_id"
    )


# ---------------------------------------------------------------------------
# validate
# ---------------------------------------------------------------------------

def validate(
    conn: sqlite3.Connection,
    transfer_id: str,
    *,
    tolerance: Decimal = Decimal("0.01"),
    cross_currency_tolerance_ratio: Decimal = DEFAULT_AMOUNT_TOLERANCE_RATIO,
) -> bool:
    """Return True iff the transfer is well-formed.

    A well-formed transfer has exactly two legs, both of kind
    ``transfer``, on different accounts, whose amounts cancel.

    Same-currency legs must cancel to within ``tolerance``, an absolute
    figure in that currency — nothing is approximated, so cents are a
    fair demand.

    Cross-currency legs are converted to USD per ADR-014 and must cancel
    to within ``cross_currency_tolerance_ratio`` of the larger leg. The
    threshold is relative because the conversion is: the rate is one
    counterparty's quote, and the pair was admitted at this same ratio
    by :class:`BankAnchoredP2pPairing`.
    """
    rows = conn.execute(SQL_TRANSFER_LEGS, (transfer_id,)).fetchall()
    if len(rows) != 2:
        return False

    # Model-parse so we inherit Decimal conversion / sign semantics.
    # SQLite's DECIMAL affinity may have coerced amount/user_rate into
    # floats on the way back out; stringify first so the Pydantic
    # validator (which rejects floats) accepts the value.
    legs: list[Transaction] = []
    for row in rows:
        try:
            raw_amount = row["amount"]
            raw_rate = row["user_rate"]
            legs.append(
                Transaction(
                    id=row["id"],
                    account_id=row["account_id"],
                    occurred_at=row["occurred_at"],
                    kind=TransactionKind(row["kind"]),
                    amount=raw_amount if isinstance(raw_amount, Decimal) else str(raw_amount),
                    currency=row["currency"],
                    description=row["description"],
                    category_id=row["category_id"],
                    transfer_id=row["transfer_id"],
                    user_rate=(
                        None
                        if raw_rate is None
                        else (raw_rate if isinstance(raw_rate, Decimal) else str(raw_rate))
                    ),
                    source=row["source"],
                    source_ref=row["source_ref"],
                    needs_review=bool(row["needs_review"]),
                )
            )
        except Exception:
            return False

    a, b = legs
    if a.kind is not TransactionKind.TRANSFER or b.kind is not TransactionKind.TRANSFER:
        return False
    if a.account_id == b.account_id:
        return False

    if a.currency == b.currency:
        return abs(a.amount + b.amount) <= tolerance

    # Cross-currency: express both legs in USD, then check they cancel.
    #
    # Per ADR-014 user_rate is bolívares per dollar wherever it sits, so
    # the conversion depends on the *leg's* currency, not on which leg
    # happens to store the rate. A leg without one borrows its
    # counterpart's: a pairing is one economic event at one rate, and
    # Provincial rows never carry a rate at all.
    rate = a.user_rate if a.user_rate is not None else b.user_rate
    a_usd = _to_usd(a.amount, a.currency, rate)
    b_usd = _to_usd(b.amount, b.currency, rate)
    if a_usd is None or b_usd is None:
        return False

    # Relative, not absolute: this pair was admitted at a relative
    # threshold, so checking it against a fixed number of cents would
    # contradict the rule that created it.
    largest = max(abs(a_usd), abs(b_usd))
    if largest == 0:
        return abs(a_usd + b_usd) <= tolerance
    return abs(a_usd + b_usd) / largest <= cross_currency_tolerance_ratio


# ---------------------------------------------------------------------------
# find_unreconciled
# ---------------------------------------------------------------------------

def find_unreconciled(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    """Return rows of ``v_unreconciled_transfers`` as plain dicts.

    Preserves ``transfer_id`` as ``None`` when the view emits SQL NULL.
    """
    rows = conn.execute(SQL_UNRECONCILED_VIEW).fetchall()
    return [
        {
            "transfer_id": row["transfer_id"],
            "leg_count": row["leg_count"],
            "transaction_ids": row["transaction_ids"],
            "account_ids": row["account_ids"],
        }
        for row in rows
    ]


# ---------------------------------------------------------------------------
# BankAnchoredP2pPairing strategy
# ---------------------------------------------------------------------------

class BankAnchoredP2pPairing:
    """Strategy: pair a bank P2P deposit with the Binance-side sell.

    A bank row (income, positive amount) and a Binance row (negative
    amount, non-null ``user_rate``) whose dates are within
    ±``window_days`` are eligible when the bolivar-equivalents agree
    within ``amount_tolerance_ratio``.

    Every eligible (deposit, sell) pair is scored, then claimed greedily
    in order of *closest date, then tightest drift, then id* — a global
    assignment rather than a per-deposit decision. Each deposit and each
    sell is consumed at most once; whatever is left over stays unpaired
    and visible for triage.

    **Why not refuse when several sells fit?** Provincial statements
    carry a date and no transaction id, so which deposit belongs to
    which sell is unknowable in principle. Three 20 000 Bs deposits
    against three ~20 000 Bs sells on one day admit no correct answer,
    only correct *totals*. The earlier exactly-one rule treated that as
    a reason to pair nothing, which stranded both halves and left the
    sells counted as expenses they are not. Consuming N sells against N
    deposits produces the same balances under any assignment, so the
    ledger is right either way. See ADR-002 (2026-07-26 amendment).

    Dates are compared by calendar day, not by instant: bank rows carry
    no time component, so hour-level precision would be false rigour.
    """

    name: str = "bank_anchored_p2p_pairing"

    def __init__(
        self,
        conn: sqlite3.Connection,
        *,
        window_days: int = 1,
        bank_source: str = "provincial",
        binance_source: str = "binance",
        amount_tolerance_ratio: Decimal = DEFAULT_AMOUNT_TOLERANCE_RATIO,
    ) -> None:
        # Pure configuration; no I/O here so construction is cheap and
        # failure modes stay in match()/apply().
        self._conn = conn
        self._window_days = window_days
        self._bank_source = bank_source
        self._binance_source = binance_source
        self._amount_tolerance_ratio = amount_tolerance_ratio

    def match(self) -> list[MatchProposal]:
        bank_rows = self._conn.execute(
            SQL_BANK_DEPOSITS,
            {"bank_source": self._bank_source},
        ).fetchall()
        sell_rows = self._conn.execute(
            SQL_BINANCE_CANDIDATES,
            {"binance_source": self._binance_source},
        ).fetchall()

        scored = self._score_candidates(bank_rows, sell_rows)

        # Claim greedily down the sorted list. Because the sort key ends in
        # (sell id, bank id) the assignment is total-ordered, so identical
        # inputs always produce an identical result.
        used_banks: set[int] = set()
        used_sells: set[int] = set()
        proposals: list[MatchProposal] = []
        for _gap, _drift, sell_id, bank_id in scored:
            if bank_id in used_banks or sell_id in used_sells:
                continue
            used_banks.add(bank_id)
            used_sells.add(sell_id)
            proposals.append(
                MatchProposal(
                    strategy=self.name,
                    details={
                        "bank_transaction_id": bank_id,
                        "binance_transaction_id": sell_id,
                    },
                )
            )

        return proposals

    def _score_candidates(
        self,
        bank_rows: list[sqlite3.Row],
        sell_rows: list[sqlite3.Row],
    ) -> list[tuple[int, Decimal, int, int]]:
        """Every eligible (deposit, sell) pairing, best fit first.

        Sorted by day gap, then drift ratio, then sell id, then bank id.
        """
        scored: list[tuple[int, Decimal, int, int]] = []

        for bank in bank_rows:
            bank_amount = _to_decimal(bank["amount"])
            if bank_amount == 0:
                continue
            bank_day = self._parse_datetime(bank["occurred_at"]).date()
            bank_currency = bank["currency"]

            for sell in sell_rows:
                rate = _to_decimal_or_none(sell["user_rate"])
                if rate is None or rate <= 0:
                    continue
                if not self._fiat_is_compatible(sell["description"], bank_currency):
                    continue

                sell_day = self._parse_datetime(sell["occurred_at"]).date()
                day_gap = self._day_gap(bank_day, sell_day)
                if day_gap > self._window_days:
                    continue

                expected = abs(_to_decimal(sell["amount"])) * rate
                drift_ratio = abs(bank_amount - expected) / bank_amount
                if drift_ratio > self._amount_tolerance_ratio:
                    continue

                scored.append((day_gap, drift_ratio, int(sell["id"]), int(bank["id"])))

        scored.sort()
        return scored

    def apply(self, proposal: MatchProposal) -> None:
        bank_id = proposal.details["bank_transaction_id"]
        binance_id = proposal.details["binance_transaction_id"]
        create_transfer(
            self._conn,
            anchor_transaction_id=bank_id,
            counterpart_transaction_id=binance_id,
        )

    # -- helpers -----------------------------------------------------------
    @staticmethod
    def _day_gap(bank_day: date, sell_day: date) -> int:
        """Calendar days between a deposit and a sell.

        Provincial rows have a date but no time, and the two sources sit
        in different timezones (bank in Caracas, Binance in UTC), so a
        sell late at night can land on the neighbouring calendar day.
        That is precisely what the ±1 day default absorbs.
        """
        return abs((sell_day - bank_day).days)

    @staticmethod
    def _fiat_is_compatible(description: Any, bank_currency: Any) -> bool:
        """Whether a sell may anchor against a deposit in ``bank_currency``.

        Rejects only a *known* mismatch. A description that does not carry
        the ingest-written ``@ <rate> <FIAT> (order ...)`` shape — legacy
        or backfilled rows, or none at all — is allowed through, so this
        guard closes the USD-priced hole without silently dropping rows
        whose denomination was never recorded.
        """
        if description is None:
            return True
        found = _FIAT_IN_DESCRIPTION_RE.search(str(description))
        if found is None:
            return True
        return found.group(1).upper() == str(bank_currency).upper()

    @staticmethod
    def _parse_datetime(value: Any) -> datetime:
        """Parse an ISO-8601 timestamp from sqlite back into datetime.

        sqlite stores ``occurred_at`` as text; callers always write it
        via :func:`datetime.isoformat`, so ``fromisoformat`` round-trips
        cleanly (Python ≥ 3.11 handles offsets with colon or ``Z``).
        """
        if isinstance(value, datetime):
            return value
        text = str(value)
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        return datetime.fromisoformat(text)


__all__ = [
    "BankAnchoredP2pPairing",
    "TransferPair",
    "create_transfer",
    "find_unreconciled",
    "validate",
]
