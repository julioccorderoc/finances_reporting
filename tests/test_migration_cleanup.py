"""EPIC-012 interactive cleanup — red-phase tests.

The cleanup walker iterates ``WHERE needs_review=1`` rows, asks the user
for a category (name) + optional user_rate, and persists the answer.
Input/output are injected so tests drive deterministic prompts.
"""
from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import pytest

from finances.db.repos import accounts as accounts_repo
from finances.db.repos import categories as categories_repo
from finances.db.repos import transactions as txn_repo
from finances.domain.models import (
    Account,
    AccountKind,
    Category,
    Transaction,
    TransactionKind,
)


@pytest.fixture
def db_with_review_rows(
    in_memory_db: sqlite3.Connection,
) -> Iterator[sqlite3.Connection]:
    """Seed two needs_review rows + one resolved row, and one expense category."""
    acct = accounts_repo.insert(
        in_memory_db,
        Account(
            name="Provincial Bolivares",
            kind=AccountKind.BANK,
            currency="VES",
            institution="Provincial",
        ),
    )
    assert acct.id is not None
    cat = categories_repo.get_by_name(
        in_memory_db, TransactionKind.EXPENSE, "Food"
    )
    if cat is None:
        cat = categories_repo.insert(
            in_memory_db,
            Category(kind=TransactionKind.EXPENSE, name="Food"),
        )
    assert cat.id is not None

    occurred = datetime(2025, 11, 5, tzinfo=UTC)
    txn_repo.insert(
        in_memory_db,
        Transaction(
            account_id=acct.id,
            occurred_at=occurred,
            kind=TransactionKind.EXPENSE,
            amount=Decimal("-100"),
            currency="VES",
            description="UNMATCHED DEBIT 1",
            source="provincial",
            source_ref="hash:aaaa111100000001",
            needs_review=True,
        ),
    )
    txn_repo.insert(
        in_memory_db,
        Transaction(
            account_id=acct.id,
            occurred_at=occurred,
            kind=TransactionKind.EXPENSE,
            amount=Decimal("-200"),
            currency="VES",
            description="UNMATCHED DEBIT 2",
            source="provincial",
            source_ref="hash:aaaa111100000002",
            needs_review=True,
        ),
    )
    txn_repo.insert(
        in_memory_db,
        Transaction(
            account_id=acct.id,
            occurred_at=occurred,
            kind=TransactionKind.EXPENSE,
            amount=Decimal("-300"),
            currency="VES",
            description="RESOLVED DEBIT",
            source="provincial",
            source_ref="hash:aaaa111100000003",
            needs_review=False,
        ),
    )
    yield in_memory_db


def test_cleanup_applies_category_and_clears_flag(
    db_with_review_rows: sqlite3.Connection,
) -> None:
    from finances.migration.interactive_cleanup import run_cleanup

    # Two scripted answers, one per needs_review row.
    answers = iter([
        ("Food", None),      # first row: category only
        ("Food", "225"),     # second row: category + user_rate
    ])

    def prompt(_row: sqlite3.Row) -> tuple[str | None, str | None]:
        return next(answers)

    report = run_cleanup(db_with_review_rows, prompt=prompt)

    remaining = db_with_review_rows.execute(
        "SELECT COUNT(*) FROM transactions WHERE needs_review=1"
    ).fetchone()[0]
    assert remaining == 0
    assert report.rows_resolved == 2

    # user_rate persisted on the second row only.
    rate_rows = db_with_review_rows.execute(
        "SELECT source_ref, user_rate FROM transactions WHERE source_ref IN "
        "(?, ?) ORDER BY source_ref",
        ("hash:aaaa111100000001", "hash:aaaa111100000002"),
    ).fetchall()
    assert rate_rows[0]["user_rate"] is None
    assert Decimal(str(rate_rows[1]["user_rate"])) == Decimal("225")


def test_cleanup_skip_leaves_row_flagged(
    db_with_review_rows: sqlite3.Connection,
) -> None:
    from finances.migration.interactive_cleanup import run_cleanup

    answers = iter([
        (None, None),        # first row: skipped (no category supplied)
        ("Food", None),      # second row: answered
    ])

    def prompt(_row: sqlite3.Row) -> tuple[str | None, str | None]:
        return next(answers)

    report = run_cleanup(db_with_review_rows, prompt=prompt)

    remaining = db_with_review_rows.execute(
        "SELECT COUNT(*) FROM transactions WHERE needs_review=1"
    ).fetchone()[0]
    assert remaining == 1
    assert report.rows_resolved == 1
    assert report.rows_skipped == 1


def test_cleanup_unknown_category_raises(
    db_with_review_rows: sqlite3.Connection,
) -> None:
    from finances.migration.interactive_cleanup import run_cleanup

    def prompt(_row: sqlite3.Row) -> tuple[str | None, str | None]:
        return ("This Category Does Not Exist", None)

    with pytest.raises(ValueError, match="category"):
        run_cleanup(db_with_review_rows, prompt=prompt)


# ---------------------------------------------------------------------------
# CSV round-trip (batch review workflow).
# ---------------------------------------------------------------------------


def test_export_needs_review_writes_csv(
    db_with_review_rows: sqlite3.Connection, tmp_path: "Path",  # type: ignore[name-defined]
) -> None:
    import csv

    from finances.migration.interactive_cleanup import export_needs_review

    dest = tmp_path / "review.csv"
    count = export_needs_review(db_with_review_rows, dest)
    assert count == 2

    with dest.open("r", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    assert len(rows) == 2
    # Required columns so the user can edit in Sheets and hand back.
    expected_cols = {
        "id", "occurred_at", "source", "kind", "amount", "currency",
        "description", "suggested_category", "category", "user_rate",
    }
    assert expected_cols <= set(rows[0].keys())
    # Review-only: resolved rows stay out.
    ids = {int(r["id"]) for r in rows}
    resolved_id = db_with_review_rows.execute(
        "SELECT id FROM transactions WHERE source_ref='hash:aaaa111100000003'"
    ).fetchone()["id"]
    assert resolved_id not in ids


def test_import_cleanup_csv_applies_categories(
    db_with_review_rows: sqlite3.Connection, tmp_path: "Path",  # type: ignore[name-defined]
) -> None:
    import csv
    from decimal import Decimal

    from finances.migration.interactive_cleanup import (
        export_needs_review,
        import_cleanup_csv,
    )

    dest = tmp_path / "review.csv"
    export_needs_review(db_with_review_rows, dest)

    # Fill the category column, leave the second row's user_rate filled.
    with dest.open("r", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
        fieldnames = rows[0].keys()
    rows[0]["category"] = "Food"
    rows[1]["category"] = "Food"
    rows[1]["user_rate"] = "225"
    with dest.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    report = import_cleanup_csv(db_with_review_rows, dest)
    assert report.rows_resolved == 2

    remaining = db_with_review_rows.execute(
        "SELECT COUNT(*) FROM transactions WHERE needs_review=1"
    ).fetchone()[0]
    assert remaining == 0

    applied = db_with_review_rows.execute(
        "SELECT user_rate FROM transactions WHERE source_ref=?",
        ("hash:aaaa111100000002",),
    ).fetchone()
    assert Decimal(str(applied["user_rate"])) == Decimal("225")


def test_import_cleanup_csv_skips_blank_category(
    db_with_review_rows: sqlite3.Connection, tmp_path: "Path",  # type: ignore[name-defined]
) -> None:
    import csv

    from finances.migration.interactive_cleanup import (
        export_needs_review,
        import_cleanup_csv,
    )

    dest = tmp_path / "review.csv"
    export_needs_review(db_with_review_rows, dest)
    with dest.open("r", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
        fieldnames = rows[0].keys()
    rows[0]["category"] = ""         # skipped
    rows[1]["category"] = "Food"     # applied
    with dest.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    report = import_cleanup_csv(db_with_review_rows, dest)
    assert report.rows_resolved == 1
    assert report.rows_skipped == 1
    remaining = db_with_review_rows.execute(
        "SELECT COUNT(*) FROM transactions WHERE needs_review=1"
    ).fetchone()[0]
    assert remaining == 1


def test_export_includes_legacy_annotations(
    in_memory_db: sqlite3.Connection, tmp_path: Path
) -> None:
    """When ``legacy_dir`` is given, each row's legacy Sub-Category and
    Category columns from the source sheet appear alongside it so the user
    can translate their prior classification into the v1 taxonomy."""
    import csv as _csv
    import shutil

    from finances.migration.backfill import run_backfill
    from finances.migration.interactive_cleanup import export_needs_review

    # Copy synthetic 3-row slices into a tmp data dir the backfill can read.
    legacy_dir = tmp_path / "data"
    legacy_dir.mkdir()
    fixtures = Path(__file__).parent / "fixtures" / "backfill"
    for name in (
        "Finanzas - Binance.csv",
        "Finanzas - Provincial.csv",
        "Finanzas - BCV.csv",
    ):
        shutil.copy(fixtures / name, legacy_dir / name)

    run_backfill(in_memory_db, legacy_dir)

    dest = tmp_path / "review.csv"
    export_needs_review(in_memory_db, dest, legacy_dir=legacy_dir)

    with dest.open("r", encoding="utf-8") as handle:
        rows = list(_csv.DictReader(handle))
    assert rows, "export produced zero rows"

    assert {"legacy_sub_category", "legacy_category"} <= set(rows[0].keys())

    # The fixture's USDC deposit on 01-Nov-2025 is tagged Salary/Inflow.
    deposit = next(
        r for r in rows if r["source"] == "binance" and r["currency"] == "USDC"
    )
    assert deposit["legacy_sub_category"] == "Salary"
    assert deposit["legacy_category"] == "Inflow"


def test_import_cleanup_csv_unknown_category_raises(
    db_with_review_rows: sqlite3.Connection, tmp_path: "Path",  # type: ignore[name-defined]
) -> None:
    import csv

    from finances.migration.interactive_cleanup import (
        export_needs_review,
        import_cleanup_csv,
    )

    dest = tmp_path / "review.csv"
    export_needs_review(db_with_review_rows, dest)
    with dest.open("r", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
        fieldnames = rows[0].keys()
    rows[0]["category"] = "Definitely Not A Category"
    with dest.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    with pytest.raises(ValueError, match="category"):
        import_cleanup_csv(db_with_review_rows, dest)
