"""Tests for EPIC-013 — ``finances/reports/needs_review.py``.

Per rule-011: every public function gets ≥1 happy-path AND ≥1 failure-mode test,
and these tests are committed **before** the implementation.
"""

from __future__ import annotations

import csv
import io
import json
import sqlite3
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from finances.db.repos import accounts as accounts_repo
from finances.db.repos import transactions as transactions_repo
from finances.domain.models import (
    Account,
    AccountKind,
    Transaction,
    TransactionKind,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _mk_account(conn: sqlite3.Connection, name: str = "Bank") -> Account:
    return accounts_repo.insert(
        conn,
        Account(name=name, kind=AccountKind.BANK, currency="USD", institution=None),
    )


def _mk_txn(
    conn: sqlite3.Connection,
    *,
    account_id: int,
    amount: Decimal,
    source_ref: str,
    occurred_at: datetime,
    needs_review: bool,
    description: str | None = "x",
    kind: TransactionKind = TransactionKind.EXPENSE,
    currency: str = "USD",
) -> Transaction:
    txn = Transaction(
        account_id=account_id,
        occurred_at=occurred_at,
        kind=kind,
        amount=amount,
        currency=currency,
        description=description,
        source="test",
        source_ref=source_ref,
        needs_review=needs_review,
    )
    return transactions_repo.insert(conn, txn)


# ---------------------------------------------------------------------------
# get_needs_review
# ---------------------------------------------------------------------------


def test_get_needs_review_happy_path_returns_only_flagged_rows_newest_first(
    in_memory_db: sqlite3.Connection,
) -> None:
    from finances.reports.needs_review import NeedsReviewRow, get_needs_review

    acc = _mk_account(in_memory_db, "Alpha")
    assert acc.id is not None

    # Two flagged, one not.
    older = _mk_txn(
        in_memory_db,
        account_id=acc.id,
        amount=Decimal("10.00"),
        source_ref="nr-1",
        occurred_at=datetime(2026, 1, 1, tzinfo=UTC),
        needs_review=True,
        description="older review",
    )
    newer = _mk_txn(
        in_memory_db,
        account_id=acc.id,
        amount=Decimal("20.00"),
        source_ref="nr-2",
        occurred_at=datetime(2026, 2, 1, tzinfo=UTC),
        needs_review=True,
        description="newer review",
    )
    _mk_txn(
        in_memory_db,
        account_id=acc.id,
        amount=Decimal("30.00"),
        source_ref="nr-3",
        occurred_at=datetime(2026, 3, 1, tzinfo=UTC),
        needs_review=False,
        description="resolved",
    )

    rows = get_needs_review(in_memory_db)
    assert all(isinstance(r, NeedsReviewRow) for r in rows)
    assert len(rows) == 2
    # Newest first.
    assert rows[0].transaction_id == newer.id
    assert rows[1].transaction_id == older.id
    assert rows[0].amount == Decimal("20.00")
    assert rows[0].description == "newer review"
    assert rows[0].source == "test"


def test_get_needs_review_empty_returns_empty_list(
    in_memory_db: sqlite3.Connection,
) -> None:
    from finances.reports.needs_review import get_needs_review

    assert get_needs_review(in_memory_db) == []


def test_get_needs_review_nothing_flagged_returns_empty_list(
    in_memory_db: sqlite3.Connection,
) -> None:
    from finances.reports.needs_review import get_needs_review

    acc = _mk_account(in_memory_db, "Clean")
    assert acc.id is not None
    _mk_txn(
        in_memory_db,
        account_id=acc.id,
        amount=Decimal("1.00"),
        source_ref="clean-1",
        occurred_at=datetime(2026, 1, 1, tzinfo=UTC),
        needs_review=False,
    )

    assert get_needs_review(in_memory_db) == []


def test_get_needs_review_orders_by_occurred_at_desc_id_desc_tie_break(
    in_memory_db: sqlite3.Connection,
) -> None:
    from finances.reports.needs_review import get_needs_review

    acc = _mk_account(in_memory_db, "Tie")
    assert acc.id is not None

    same_ts = datetime(2026, 4, 1, 12, 0, tzinfo=UTC)
    first = _mk_txn(
        in_memory_db,
        account_id=acc.id,
        amount=Decimal("1.00"),
        source_ref="tie-1",
        occurred_at=same_ts,
        needs_review=True,
    )
    second = _mk_txn(
        in_memory_db,
        account_id=acc.id,
        amount=Decimal("2.00"),
        source_ref="tie-2",
        occurred_at=same_ts,
        needs_review=True,
    )

    rows = get_needs_review(in_memory_db)
    assert [r.transaction_id for r in rows] == [second.id, first.id]


# ---------------------------------------------------------------------------
# NeedsReviewRow model validation (failure-mode)
# ---------------------------------------------------------------------------


def test_needs_review_row_rejects_float_amount() -> None:
    from pydantic import ValidationError

    from finances.reports.needs_review import NeedsReviewRow

    with pytest.raises(ValidationError):
        NeedsReviewRow(
            transaction_id=1,
            occurred_at=datetime(2026, 1, 1, tzinfo=UTC),
            account_id=1,
            kind="expense",
            amount=1.23,  # type: ignore[arg-type]
            currency="USD",
            description=None,
            source="test",
        )


def test_needs_review_row_extra_field_forbidden() -> None:
    from pydantic import ValidationError

    from finances.reports.needs_review import NeedsReviewRow

    with pytest.raises(ValidationError):
        NeedsReviewRow(
            transaction_id=1,
            occurred_at=datetime(2026, 1, 1, tzinfo=UTC),
            account_id=1,
            kind="expense",
            amount=Decimal("1.00"),
            currency="USD",
            description=None,
            source="test",
            bogus="nope",  # type: ignore[call-arg]
        )


# ---------------------------------------------------------------------------
# render_json
# ---------------------------------------------------------------------------


def test_render_json_happy_path_decimal_as_string_and_iso_datetime(
    in_memory_db: sqlite3.Connection,
) -> None:
    from finances.reports.needs_review import get_needs_review, render_json

    acc = _mk_account(in_memory_db, "JSONR")
    assert acc.id is not None
    _mk_txn(
        in_memory_db,
        account_id=acc.id,
        amount=Decimal("7.50"),
        source_ref="j-1",
        occurred_at=datetime(2026, 5, 10, 9, 30, tzinfo=UTC),
        needs_review=True,
        description="coffee",
    )

    rows = get_needs_review(in_memory_db)
    payload = render_json(rows)
    parsed = json.loads(payload)
    assert isinstance(parsed, list)
    assert len(parsed) == 1
    row = parsed[0]
    assert isinstance(row["amount"], str)
    assert row["amount"] == "7.50"
    assert row["description"] == "coffee"
    # ISO-format datetime with timezone.
    assert "2026-05-10" in row["occurred_at"]
    assert row["occurred_at"].endswith("+00:00") or row["occurred_at"].endswith("Z")


def test_render_json_empty_list_returns_empty_json_array() -> None:
    from finances.reports.needs_review import render_json

    assert json.loads(render_json([])) == []


# ---------------------------------------------------------------------------
# render_csv
# ---------------------------------------------------------------------------


def test_render_csv_happy_path_header_and_row(
    in_memory_db: sqlite3.Connection,
) -> None:
    from finances.reports.needs_review import get_needs_review, render_csv

    acc = _mk_account(in_memory_db, "CSVR")
    assert acc.id is not None
    _mk_txn(
        in_memory_db,
        account_id=acc.id,
        amount=Decimal("3.14"),
        source_ref="c-1",
        occurred_at=datetime(2026, 6, 1, tzinfo=UTC),
        needs_review=True,
        description="pi",
    )

    rows = get_needs_review(in_memory_db)
    out = render_csv(rows)
    reader = csv.reader(io.StringIO(out))
    out_rows = list(reader)
    assert out_rows[0] == [
        "transaction_id",
        "occurred_at",
        "account_id",
        "kind",
        "amount",
        "currency",
        "description",
        "source",
    ]
    assert len(out_rows) == 2
    assert out_rows[1][4] == "3.14"
    assert out_rows[1][5] == "USD"
    assert out_rows[1][6] == "pi"


def test_render_csv_empty_returns_header_only_no_crash() -> None:
    from finances.reports.needs_review import render_csv

    out = render_csv([])
    reader = csv.reader(io.StringIO(out))
    rows = list(reader)
    assert rows == [
        [
            "transaction_id",
            "occurred_at",
            "account_id",
            "kind",
            "amount",
            "currency",
            "description",
            "source",
        ]
    ]


# ---------------------------------------------------------------------------
# render_table
# ---------------------------------------------------------------------------


def test_render_table_contains_row_data_and_headers(
    in_memory_db: sqlite3.Connection,
) -> None:
    from finances.reports.needs_review import get_needs_review, render_table

    acc = _mk_account(in_memory_db, "TBL")
    assert acc.id is not None
    _mk_txn(
        in_memory_db,
        account_id=acc.id,
        amount=Decimal("99.99"),
        source_ref="t-1",
        occurred_at=datetime(2026, 7, 1, 8, 0, tzinfo=UTC),
        needs_review=True,
        description="needs it",
    )

    rows = get_needs_review(in_memory_db)
    out = render_table(rows)

    # Column headers.
    for header in (
        "ID",
        "Occurred At",
        "Account",
        "Kind",
        "Amount",
        "Currency",
        "Description",
        "Source",
    ):
        assert header in out
    assert "99.99" in out
    assert "USD" in out
    assert "needs it" in out


def test_render_table_empty_list_still_prints_headers() -> None:
    from finances.reports.needs_review import render_table

    out = render_table([])
    for header in (
        "ID",
        "Occurred At",
        "Account",
        "Kind",
        "Amount",
        "Currency",
        "Description",
        "Source",
    ):
        assert header in out
