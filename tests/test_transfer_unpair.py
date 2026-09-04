"""Breaking a pair — the surface ADR-022 said did not exist yet.

``transactions_repo.delete`` refuses a paired row: *"This row is one half of
a transfer — the pair has to be broken first."* Nothing could break one,
because ``_promote_to_transfer`` overwrote ``kind`` and ``needs_review``
without recording them. Migration 024 records them; :func:`transfers.unpair`
replays them.

Two decisions these tests pin:

* **Unpair never deletes.** It breaks the pair and leaves both rows standing.
  The orphan is then an ordinary unpaired row that ADR-022's delete accepts,
  so deletion keeps happening in exactly one place with one set of tombstone
  rules, instead of pairing quietly growing a second delete path.
* **No pre-image, no unpair.** The 270 pairs that predate migration 024 are
  refused rather than guessed at — an importer-authored leg was born
  ``kind='transfer'`` and never was an expense, so deriving ``expense`` from
  a negative sign would corrupt it.
"""

from __future__ import annotations

import sqlite3
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from finances.db.repos import transactions as transactions_repo
from finances.domain import transfers
from finances.domain.models import Transaction, TransactionKind


def _row(
    conn: sqlite3.Connection,
    *,
    account_id: int,
    amount: str,
    currency: str,
    kind: TransactionKind = TransactionKind.EXPENSE,
    source: str = "binance",
    source_ref: str = "pay:1",
    needs_review: bool = True,
    description: str = "Binance Pay C2C (outgoing)",
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
            needs_review=needs_review,
        ),
    )
    assert stored.id is not None
    return stored.id


def _same_currency_pair(conn: sqlite3.Connection) -> tuple[str, int, int]:
    """A promoted pair of pre-existing rows, plus its two leg ids."""
    out_id = _row(conn, account_id=2, amount="-580", currency="USDT")
    in_id = _row(
        conn,
        account_id=3,
        amount="580",
        currency="USDT",
        kind=TransactionKind.INCOME,
        source_ref="pay:2",
        needs_review=False,
        description="Binance Pay C2C (incoming)",
    )
    pair = transfers.create_transfer(
        conn, anchor_transaction_id=out_id, counterpart_transaction_id=in_id
    )
    return pair.transfer_id, out_id, in_id


def _provenance(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM transfer_pairings ORDER BY transaction_id"
    ).fetchall()


# ---------------------------------------------------------------------------
# The pre-image is recorded when the pair is made
# ---------------------------------------------------------------------------


def test_pairing_records_a_pre_image_for_each_leg(seeded_db: sqlite3.Connection) -> None:
    transfer_id, out_id, in_id = _same_currency_pair(seeded_db)

    rows = _provenance(seeded_db)
    assert [r["transaction_id"] for r in rows] == sorted([out_id, in_id])
    assert {r["transfer_id"] for r in rows} == {transfer_id}


def test_pre_image_holds_what_the_row_actually_was(
    seeded_db: sqlite3.Connection,
) -> None:
    """Not what the sign implies — what the column held a moment earlier."""
    _, out_id, in_id = _same_currency_pair(seeded_db)

    by_id = {r["transaction_id"]: r for r in _provenance(seeded_db)}
    assert (by_id[out_id]["prior_kind"], by_id[out_id]["prior_needs_review"]) == (
        "expense",
        1,
    )
    assert (by_id[in_id]["prior_kind"], by_id[in_id]["prior_needs_review"]) == (
        "income",
        0,
    )


# ---------------------------------------------------------------------------
# unpair replays it
# ---------------------------------------------------------------------------


def test_unpair_clears_the_transfer_id_on_both_legs(
    seeded_db: sqlite3.Connection,
) -> None:
    transfer_id, out_id, in_id = _same_currency_pair(seeded_db)

    transfers.unpair(seeded_db, transfer_id=transfer_id)

    for txn_id in (out_id, in_id):
        txn = transactions_repo.get_by_id(seeded_db, txn_id)
        assert txn is not None
        assert txn.transfer_id is None


def test_unpair_restores_kind_and_review_flag_exactly(
    seeded_db: sqlite3.Connection,
) -> None:
    transfer_id, out_id, in_id = _same_currency_pair(seeded_db)

    transfers.unpair(seeded_db, transfer_id=transfer_id)

    out = transactions_repo.get_by_id(seeded_db, out_id)
    incoming = transactions_repo.get_by_id(seeded_db, in_id)
    assert out is not None and incoming is not None
    assert (out.kind, out.needs_review) == (TransactionKind.EXPENSE, True)
    assert (incoming.kind, incoming.needs_review) == (TransactionKind.INCOME, False)


def test_unpair_leaves_both_rows_standing(seeded_db: sqlite3.Connection) -> None:
    """It breaks a pair. Deleting is a separate, deliberate act (ADR-022)."""
    transfer_id, out_id, in_id = _same_currency_pair(seeded_db)

    transfers.unpair(seeded_db, transfer_id=transfer_id)

    assert transactions_repo.get_by_id(seeded_db, out_id) is not None
    assert transactions_repo.get_by_id(seeded_db, in_id) is not None


def test_unpair_consumes_the_pre_image(seeded_db: sqlite3.Connection) -> None:
    """Replayed once. A second unpair has nothing to put back and refuses."""
    transfer_id, _, _ = _same_currency_pair(seeded_db)

    transfers.unpair(seeded_db, transfer_id=transfer_id)

    assert _provenance(seeded_db) == []
    with pytest.raises(ValueError, match="no record of what these rows were"):
        transfers.unpair(seeded_db, transfer_id=transfer_id)


def test_unpair_returns_the_legs_it_broke(seeded_db: sqlite3.Connection) -> None:
    """The caller names what changed in the toast, so it needs the ids back."""
    transfer_id, out_id, in_id = _same_currency_pair(seeded_db)

    broken = transfers.unpair(seeded_db, transfer_id=transfer_id)

    assert sorted(t.id for t in broken) == sorted([out_id, in_id])


# ---------------------------------------------------------------------------
# What it refuses
# ---------------------------------------------------------------------------


def test_unpair_refuses_a_pair_with_no_pre_image(
    seeded_db: sqlite3.Connection,
) -> None:
    """Every pair made before migration 024 — including importer-authored ones.

    An ``earn-redeem`` or ``convert`` leg was born ``kind='transfer'``. Backing
    it out to ``expense`` because the amount is negative would invent history.
    """
    out_id = _row(seeded_db, account_id=2, amount="-600", currency="USDC")
    in_id = _row(
        seeded_db,
        account_id=4,
        amount="600",
        currency="USDC",
        kind=TransactionKind.INCOME,
        source_ref="earn-redeem:1:to",
    )
    seeded_db.execute(
        "UPDATE transactions SET kind='transfer', transfer_id='legacy-pair' "
        "WHERE id IN (?, ?)",
        (out_id, in_id),
    )

    with pytest.raises(ValueError, match="no record of what these rows were"):
        transfers.unpair(seeded_db, transfer_id="legacy-pair")


def test_unpair_refuses_an_unknown_transfer_id(seeded_db: sqlite3.Connection) -> None:
    with pytest.raises(ValueError, match="no record of what these rows were"):
        transfers.unpair(seeded_db, transfer_id="nope")


# ---------------------------------------------------------------------------
# The point of the whole thing: delete works again afterwards
# ---------------------------------------------------------------------------


def test_delete_accepts_a_leg_once_the_pair_is_broken(
    seeded_db: sqlite3.Connection,
) -> None:
    transfer_id, out_id, _ = _same_currency_pair(seeded_db)
    with pytest.raises(ValueError, match="one half of a transfer"):
        transactions_repo.delete(seeded_db, out_id)

    transfers.unpair(seeded_db, transfer_id=transfer_id)

    transactions_repo.delete(seeded_db, out_id)
    assert transactions_repo.get_by_id(seeded_db, out_id) is None


def test_unpair_refuses_the_ledgers_own_corrections(
    seeded_db: sqlite3.Connection,
) -> None:
    """An ADR-020 opening pair is not a mistake the owner made.

    ``record_opening`` goes through ``create_transfer``, so from migration 024
    onward its legs DO carry a pre-image — which would otherwise make the
    ledger's own restatement breakable from a footer button. Same refusal
    ``transactions_repo.delete`` already gives for these sources (ADR-022 §2.3).
    """
    out_id = _row(
        seeded_db,
        account_id=2,
        amount="-100",
        currency="USDT",
        source="opening_balance",
        source_ref="opening-transfer:2:5:USDT:from",
    )
    in_id = _row(
        seeded_db,
        account_id=5,
        amount="100",
        currency="USD",
        kind=TransactionKind.INCOME,
        source="opening_balance",
        source_ref="opening-transfer:2:5:USDT:to",
    )
    pair = transfers.create_transfer(
        seeded_db, anchor_transaction_id=out_id, counterpart_transaction_id=in_id
    )

    with pytest.raises(ValueError, match="the ledger's own correction"):
        transfers.unpair(seeded_db, transfer_id=pair.transfer_id)


def test_unpair_clears_a_leg_the_pairing_itself_created(
    seeded_db: sqlite3.Connection,
) -> None:
    """A leg born paired carries no pre-image, but must still come loose.

    ``_insert_leg`` writes ``transfer_id`` at insert time and never goes
    through ``_promote_to_transfer``. If unpair only touched recorded legs,
    that one would keep a ``transfer_id`` pointing at a transfer that no
    longer exists — an orphan worse than the one it replaced.
    """
    anchor_id = _row(seeded_db, account_id=2, amount="-580", currency="USDT")
    pair = transfers.create_transfer(
        seeded_db,
        anchor_transaction_id=anchor_id,
        to_account_id=3,
        amount=Decimal("580"),
        currency="USDT",
        description="counterpart",
        source="internal",
        source_ref_to="internal:1",
    )
    created_id = (
        pair.to_transaction_id
        if pair.to_transaction_id != anchor_id
        else pair.from_transaction_id
    )

    transfers.unpair(seeded_db, transfer_id=pair.transfer_id)

    created = transactions_repo.get_by_id(seeded_db, created_id)
    assert created is not None
    assert created.transfer_id is None


def test_unpair_puts_back_the_rate_the_pairing_wrote(
    seeded_db: sqlite3.Connection,
) -> None:
    """A conversion sets a struck rate. Cancelling it must take that back.

    Found in the browser, not here: unpair restored ``kind`` and
    ``needs_review`` and left ``user_rate`` behind, so a cancelled conversion
    left the row priced at exactly the figure the owner had just rejected —
    forever, and invisibly, because nothing on the row says where it came
    from.

    The pre-image is "what the row was when it was paired". An edit made
    *after* pairing is reverted too; that is the honest reading, and
    ``transaction_edits`` keeps the trail either way.
    """
    from finances.domain import cash_conversion

    anchor_id = _row(
        seeded_db,
        account_id=1,
        amount="-36000",
        currency="VES",
        source="provincial",
        source_ref="hash:52809099a320229b",
    )
    result = cash_conversion.convert_to_cash(
        seeded_db, transaction_id=anchor_id, usd_received=Decimal("40")
    )
    priced = transactions_repo.get_by_id(seeded_db, anchor_id)
    assert priced is not None and priced.user_rate == Decimal("900.0000")

    transfers.unpair(seeded_db, transfer_id=result.transfer_id)

    anchor = transactions_repo.get_by_id(seeded_db, anchor_id)
    assert anchor is not None
    assert anchor.user_rate is None


def test_unpair_keeps_a_rate_that_predates_the_pairing(
    seeded_db: sqlite3.Connection,
) -> None:
    """Only what the pairing changed comes back — not the owner's own rate."""
    anchor_id = _row(
        seeded_db,
        account_id=1,
        amount="-36000",
        currency="VES",
        source="provincial",
        source_ref="hash:52809099a320229b",
    )
    transactions_repo.update(seeded_db, id=anchor_id, user_rate=Decimal("880"))
    counterpart = _row(
        seeded_db,
        account_id=3,
        amount="40.91",
        currency="USDT",
        kind=TransactionKind.INCOME,
        source_ref="pay:counterpart",
    )
    pair = transfers.create_transfer(
        seeded_db,
        anchor_transaction_id=anchor_id,
        counterpart_transaction_id=counterpart,
    )

    transfers.unpair(seeded_db, transfer_id=pair.transfer_id)

    anchor = transactions_repo.get_by_id(seeded_db, anchor_id)
    assert anchor is not None
    assert anchor.user_rate == Decimal("880")
