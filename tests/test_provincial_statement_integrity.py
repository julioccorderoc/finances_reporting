"""A bank export must not be able to lose rows silently.

The bank's web export caps at 99 rows and says nothing. Asking for a month
with more movements than that returns the *last* 99. Three holes reached the
live ledger this way — 2026-01-07..01-11, 2026-05-01..05-14 and
2026-06-02..06-06, together about 24 days and an estimated $803 of bolívar
spending that is simply absent.

Every statement carries a running ``Saldo``. The ingest already parses it and
then discarded it. Two facts make it a complete check:

* ``closing - opening == sum(montos)`` over the whole file. Order-independent,
  so same-day row shuffling cannot produce a false positive. Verified exact
  (delta 0.00) on 13 of the 14 archived statements — and it catches the 14th,
  ``provincial-may.xls``, which is internally missing Bs 1 400.
* a row count sitting exactly on the page limit is the truncation signature.

Warnings never block the load (the owner asked for loud, not fatal).
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import pytest

from finances.db.repos import accounts as accounts_repo
from finances.db.repos import transactions as txn_repo
from finances.domain.models import Account, AccountKind, Transaction, TransactionKind
from finances.ingest import provincial

HEADER = "Fecha;Descripcion;Monto;Saldo"


def _statement(tmp_path: Path, rows: list[tuple[str, str, str, str]], name="s.csv") -> Path:
    """Write a semicolon statement, newest row first as the bank exports it."""
    path = tmp_path / name
    body = "\n".join(HEADER.split()[0] and ";".join(r) for r in rows)
    path.write_text(f"{HEADER}\n{body}\n", encoding="utf-8")
    return path


@pytest.fixture
def bank(in_memory_db):
    conn = in_memory_db
    accounts_repo.insert(
        conn,
        Account(
            name=provincial.DEFAULT_ACCOUNT_NAME,
            kind=AccountKind.BANK,
            currency="VES",
        ),
    )
    return conn


def test_clean_statement_produces_no_warnings(bank, tmp_path):
    path = _statement(
        tmp_path,
        [
            ("03/05/2026", "C", "-100,00", "800,00"),
            ("02/05/2026", "B", "-50,00", "900,00"),
            ("01/05/2026", "A", "-50,00", "950,00"),
        ],
    )
    report = provincial.ingest_csv(bank, path, run_pairing=False)
    assert report.warnings == []


def test_a_missing_row_inside_the_range_is_detected(bank, tmp_path):
    """The Saldo jumps by 400 more than the montos explain."""
    path = _statement(
        tmp_path,
        [
            ("03/05/2026", "C", "-100,00", "400,00"),
            ("02/05/2026", "B", "-50,00", "900,00"),
            ("01/05/2026", "A", "-50,00", "950,00"),
        ],
    )
    report = provincial.ingest_csv(bank, path, run_pairing=False)

    assert any("400" in w for w in report.warnings)
    assert any("saldo" in w.lower() or "balance" in w.lower() for w in report.warnings)


def test_a_broken_statement_still_loads(bank, tmp_path):
    """Loud, not fatal — the owner decides what to re-export."""
    path = _statement(
        tmp_path,
        [
            ("03/05/2026", "C", "-100,00", "400,00"),
            ("02/05/2026", "B", "-50,00", "900,00"),
        ],
    )
    report = provincial.ingest_csv(bank, path, run_pairing=False)

    assert report.warnings
    assert report.rows_inserted == 2


def test_row_count_on_the_page_limit_is_flagged(bank, tmp_path):
    """99 rows is the bank's page cap, and the signature of truncation."""
    rows = []
    saldo = Decimal("10000")
    for i in range(provincial.EXPORT_PAGE_SIZE):
        rows.append((f"{(i % 28) + 1:02d}/05/2026", f"row{i}", "-1,00", f"{saldo:.2f}".replace(".", ",")))
        saldo += Decimal("1")
    path = _statement(tmp_path, rows)

    report = provincial.ingest_csv(bank, path, run_pairing=False)

    assert any(str(provincial.EXPORT_PAGE_SIZE) in w for w in report.warnings)
    assert any("truncat" in w.lower() for w in report.warnings)


def test_a_gap_against_existing_history_is_flagged(bank, tmp_path):
    """April 30 on file, statement opens May 15 — fourteen days unaccounted."""
    txn_repo.insert(
        bank,
        Transaction(
            account_id=1,
            occurred_at=datetime(2026, 4, 30, tzinfo=UTC),
            kind=TransactionKind.EXPENSE,
            amount=Decimal("-10"),
            currency="VES",
            description="prior",
            source="provincial",
            source_ref="prior",
        ),
    )
    path = _statement(
        tmp_path,
        [
            ("16/05/2026", "B", "-50,00", "900,00"),
            ("15/05/2026", "A", "-50,00", "950,00"),
        ],
    )

    report = provincial.ingest_csv(bank, path, run_pairing=False)

    assert any("2026-05-01" in w and "2026-05-14" in w for w in report.warnings)


def test_contiguous_statement_is_not_flagged_as_a_gap(bank, tmp_path):
    txn_repo.insert(
        bank,
        Transaction(
            account_id=1,
            occurred_at=datetime(2026, 5, 14, tzinfo=UTC),
            kind=TransactionKind.EXPENSE,
            amount=Decimal("-10"),
            currency="VES",
            description="prior",
            source="provincial",
            source_ref="prior",
        ),
    )
    path = _statement(
        tmp_path,
        [
            ("16/05/2026", "B", "-50,00", "900,00"),
            ("15/05/2026", "A", "-50,00", "950,00"),
        ],
    )

    report = provincial.ingest_csv(bank, path, run_pairing=False)

    assert not any("gap" in w.lower() for w in report.warnings)


def test_statement_without_a_saldo_column_is_not_falsely_flagged(bank, tmp_path):
    path = tmp_path / "no-saldo.csv"
    path.write_text(
        "Fecha;Descripcion;Monto\n"
        "02/05/2026;B;-50,00\n"
        "01/05/2026;A;-50,00\n",
        encoding="utf-8",
    )
    report = provincial.ingest_csv(bank, path, run_pairing=False)
    assert not any("saldo" in w.lower() for w in report.warnings)


def test_warnings_are_recorded_on_the_import_run(bank, tmp_path):
    path = _statement(
        tmp_path,
        [
            ("03/05/2026", "C", "-100,00", "400,00"),
            ("02/05/2026", "B", "-50,00", "900,00"),
        ],
    )
    provincial.ingest_csv(bank, path, run_pairing=False)

    row = bank.execute(
        "SELECT status, error FROM import_runs ORDER BY id DESC LIMIT 1"
    ).fetchone()
    assert row["status"] == "success"
    assert row["error"] and "warning" in row["error"].lower()


def test_dry_run_reports_warnings_without_writing(bank, tmp_path):
    path = _statement(
        tmp_path,
        [
            ("03/05/2026", "C", "-100,00", "400,00"),
            ("02/05/2026", "B", "-50,00", "900,00"),
        ],
    )
    report = provincial.ingest_csv(bank, path, run_pairing=False, dry_run=True)

    assert report.warnings
    assert bank.execute("SELECT COUNT(*) c FROM transactions").fetchone()["c"] == 0
    assert bank.execute("SELECT COUNT(*) c FROM import_runs").fetchone()["c"] == 0
