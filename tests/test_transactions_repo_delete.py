"""``transactions_repo.delete`` — a real DELETE plus a tombstone (ADR-022).

What the tests below pin, in the ADR's order:

* §2.1 the row goes, a ``(source, source_ref)`` tombstone stays, and the
  two happen together — a failure leaves neither;
* §2.2 ``cash_cli`` rows are exempt from the tombstone;
* §2.3 a paired row and the reconciliation engine's own rows are refused.

The snapshot is the record: it carries the deleted row in a JSON-ready
shape so a future undo is a re-insert, and so the viewer can name what it
removed in the toast.
"""

from __future__ import annotations

import json
import sqlite3
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from finances.db.repos import transactions as transactions_repo
from finances.domain.models import Transaction, TransactionKind

CASH_SOURCE = "cash_cli"


def _txn(
    account_id: int = 1,
    *,
    source: str = "binance",
    source_ref: str = "pay:1",
    amount: str = "-700",
    currency: str = "USDT",
    description: str | None = "Binance Pay C2C (outgoing)",
    transfer_id: str | None = None,
) -> Transaction:
    return Transaction(
        account_id=account_id,
        occurred_at=datetime(2025, 11, 6, 12, 0, tzinfo=UTC),
        kind=TransactionKind.EXPENSE,
        amount=Decimal(amount),
        currency=currency,
        description=description,
        transfer_id=transfer_id,
        source=source,
        source_ref=source_ref,
    )


def _insert(conn: sqlite3.Connection, txn: Transaction) -> int:
    stored = transactions_repo.insert(conn, txn)
    assert stored.id is not None
    return stored.id


def _tombstones(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM deleted_transactions ORDER BY source, source_ref"
    ).fetchall()


# ---------------------------------------------------------------------------
# §2.1 — the row goes and the tombstone stays
# ---------------------------------------------------------------------------


def test_delete_removes_the_row(seeded_db: sqlite3.Connection) -> None:
    txn_id = _insert(seeded_db, _txn())

    transactions_repo.delete(seeded_db, txn_id, reason="twin of 859")

    assert transactions_repo.get_by_id(seeded_db, txn_id) is None


def test_delete_writes_the_tombstone(seeded_db: sqlite3.Connection) -> None:
    txn_id = _insert(seeded_db, _txn())

    transactions_repo.delete(seeded_db, txn_id, reason="twin of 859")

    rows = _tombstones(seeded_db)
    assert len(rows) == 1
    assert rows[0]["source"] == "binance"
    assert rows[0]["source_ref"] == "pay:1"
    assert rows[0]["reason"] == "twin of 859"
    # UTC, ISO-8601 — parseable, not just truthy.
    assert datetime.fromisoformat(rows[0]["deleted_at"]).tzinfo is not None


def test_delete_returns_the_snapshot(seeded_db: sqlite3.Connection) -> None:
    txn_id = _insert(seeded_db, _txn())

    tomb = transactions_repo.delete(seeded_db, txn_id, reason="twin of 859")

    assert tomb.source == "binance"
    assert tomb.source_ref == "pay:1"
    assert tomb.reason == "twin of 859"
    # Enough of the row to name it in a toast and to re-insert it later.
    assert tomb.snapshot["id"] == txn_id
    assert tomb.snapshot["description"] == "Binance Pay C2C (outgoing)"
    assert Decimal(str(tomb.snapshot["amount"])) == Decimal("-700")
    assert tomb.snapshot["currency"] == "USDT"


def test_snapshot_column_is_the_row_as_json(seeded_db: sqlite3.Connection) -> None:
    txn_id = _insert(seeded_db, _txn())

    transactions_repo.delete(seeded_db, txn_id, reason=None)

    stored = json.loads(_tombstones(seeded_db)[0]["snapshot"])
    assert stored["source_ref"] == "pay:1"
    assert stored["account_id"] == 1
    # Every value survived json.dumps — Decimal and datetime included.
    assert isinstance(stored["amount"], str)
    assert stored["occurred_at"].startswith("2025-11-06")


def test_reason_is_optional(seeded_db: sqlite3.Connection) -> None:
    txn_id = _insert(seeded_db, _txn())

    tomb = transactions_repo.delete(seeded_db, txn_id, reason=None)

    assert tomb.reason is None
    assert _tombstones(seeded_db)[0]["reason"] is None


def test_deleting_a_second_row_with_a_ref_already_tombstoned_replaces_it(
    seeded_db: sqlite3.Connection,
) -> None:
    """A re-imported-then-deleted-again row must not raise on the PK.

    Only reachable if something bypassed the ingest skip, but a delete
    that crashes would leave the owner with a row they cannot remove.
    """
    first = _insert(seeded_db, _txn())
    transactions_repo.delete(seeded_db, first, reason="first")
    second = _insert(seeded_db, _txn())

    transactions_repo.delete(seeded_db, second, reason="again")

    rows = _tombstones(seeded_db)
    assert len(rows) == 1
    assert rows[0]["reason"] == "again"


def test_delete_takes_the_edit_history_with_it(
    seeded_db: sqlite3.Connection,
) -> None:
    """Migration 009's FK is ON DELETE CASCADE — prove it fires."""
    txn_id = _insert(seeded_db, _txn())
    seeded_db.execute(
        "INSERT INTO transaction_edits (transaction_id, field, old_value, new_value)"
        " VALUES (?, 'notes', NULL, 'a note')",
        (txn_id,),
    )

    transactions_repo.delete(seeded_db, txn_id, reason=None)

    left = seeded_db.execute(
        "SELECT COUNT(*) AS c FROM transaction_edits WHERE transaction_id = ?",
        (txn_id,),
    ).fetchone()["c"]
    assert left == 0


def test_unknown_id_raises_lookup_error(seeded_db: sqlite3.Connection) -> None:
    with pytest.raises(LookupError):
        transactions_repo.delete(seeded_db, 9999, reason=None)


def test_a_refused_delete_writes_no_tombstone(
    seeded_db: sqlite3.Connection,
) -> None:
    """The refusal happens before anything is written."""
    txn_id = _insert(seeded_db, _txn(transfer_id="t-1"))

    with pytest.raises(ValueError):
        transactions_repo.delete(seeded_db, txn_id, reason=None)

    assert _tombstones(seeded_db) == []
    assert transactions_repo.get_by_id(seeded_db, txn_id) is not None


# ---------------------------------------------------------------------------
# §2.2 — cash rows carry no tombstone
# ---------------------------------------------------------------------------


def test_cash_cli_row_is_deleted_without_a_tombstone(
    seeded_db: sqlite3.Connection,
) -> None:
    """Nothing re-ingests cash, and two identical cash entries hash alike.

    A tombstone here would block the owner re-entering a legitimate
    second "$12 lunch" on the same day (ADR-022 §2.2).
    """
    txn_id = _insert(
        seeded_db,
        _txn(
            account_id=5,
            source=CASH_SOURCE,
            source_ref="8f1c2d3e-0000-4000-8000-000000000001",
            currency="USD",
            description="lunch",
        ),
    )

    tomb = transactions_repo.delete(seeded_db, txn_id, reason="mistyped")

    assert transactions_repo.get_by_id(seeded_db, txn_id) is None
    assert _tombstones(seeded_db) == []
    # The caller still gets the record of what went.
    assert tomb.source == CASH_SOURCE
    assert tomb.snapshot["description"] == "lunch"


# ---------------------------------------------------------------------------
# §2.3 — what may not be deleted
# ---------------------------------------------------------------------------


def test_paired_row_is_refused(seeded_db: sqlite3.Connection) -> None:
    txn_id = _insert(seeded_db, _txn(transfer_id="transfer-1"))

    with pytest.raises(ValueError, match="half of a transfer"):
        transactions_repo.delete(seeded_db, txn_id, reason=None)

    assert transactions_repo.get_by_id(seeded_db, txn_id) is not None


@pytest.mark.parametrize("source", ["reconciliation", "opening_balance"])
def test_engine_written_rows_are_refused(
    seeded_db: sqlite3.Connection, source: str
) -> None:
    """Removing one by hand re-opens what it closed (ADR-018 / ADR-020)."""
    txn_id = _insert(
        seeded_db,
        _txn(account_id=2, source=source, source_ref=f"{source}:2:USDT"),
    )

    with pytest.raises(ValueError, match=source):
        transactions_repo.delete(seeded_db, txn_id, reason=None)

    assert transactions_repo.get_by_id(seeded_db, txn_id) is not None


# ---------------------------------------------------------------------------
# §2.2 — the ingest honours the tombstone
# ---------------------------------------------------------------------------


def test_upsert_skips_a_tombstoned_ref(seeded_db: sqlite3.Connection) -> None:
    """The whole point: re-importing the statement must not resurrect it."""
    txn_id = _insert(seeded_db, _txn())
    transactions_repo.delete(seeded_db, txn_id, reason="twin of 859")

    result = transactions_repo.upsert_by_source_ref(seeded_db, _txn())

    assert result["rows_inserted"] == 0
    assert result["rows_updated"] == 0
    assert result["rows_skipped_deleted"] == 1
    assert result["id"] is None
    assert transactions_repo.get_by_source_ref(seeded_db, "binance", "pay:1") is None


def test_upsert_reports_zero_skips_on_a_live_ref(
    seeded_db: sqlite3.Connection,
) -> None:
    """The counter is present on every result, so callers can just add it."""
    result = transactions_repo.upsert_by_source_ref(seeded_db, _txn())

    assert result["rows_inserted"] == 1
    assert result["rows_skipped_deleted"] == 0
    assert result["id"] is not None


def test_a_tombstone_retires_one_pair_only(seeded_db: sqlite3.Connection) -> None:
    """Deleting ``binance/pay:1`` must not block ``binance/pay:2``."""
    txn_id = _insert(seeded_db, _txn())
    transactions_repo.delete(seeded_db, txn_id, reason=None)

    other = transactions_repo.upsert_by_source_ref(
        seeded_db, _txn(source_ref="pay:2")
    )

    assert other["rows_inserted"] == 1
    assert other["rows_skipped_deleted"] == 0


def test_a_deleted_cash_row_can_be_entered_again(
    seeded_db: sqlite3.Connection,
) -> None:
    """No tombstone means no block — the ADR §2.2 reason, end to end."""
    cash = _txn(
        account_id=5,
        source=CASH_SOURCE,
        source_ref="8f1c2d3e-0000-4000-8000-000000000001",
        currency="USD",
        description="lunch",
    )
    txn_id = _insert(seeded_db, cash)
    transactions_repo.delete(seeded_db, txn_id, reason="mistyped")

    again = transactions_repo.upsert_by_source_ref(seeded_db, cash)

    assert again["rows_inserted"] == 1
    assert again["rows_skipped_deleted"] == 0
