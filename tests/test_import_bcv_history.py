"""Tests for the one-time BCV rate-gap import (docs/plans/revival/02-rate-gap.md).

The parser is the risky part (Spanish dates, Venezuelan decimals, accented
weekdays, ``▲/▼`` change markers) so it gets the bulk of the coverage. The
import path is exercised for idempotency and never-overwrite guarantees against
an in-memory migrated DB.

``scripts/`` is not a Python package (only ``finances`` is packaged), so the
one-time script is loaded by file path via importlib.
"""
from __future__ import annotations

import importlib.util
import sqlite3
from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest

from finances.db.repos import rates as rates_repo
from finances.domain.models import Rate
from finances.ingest.bcv import BcvParseError, RawBcvRow

FIXTURES = Path(__file__).parent / "fixtures"


def _load_script():
    path = Path(__file__).resolve().parent.parent / "scripts" / "import_bcv_history.py"
    spec = importlib.util.spec_from_file_location("import_bcv_history", path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


mod = _load_script()


def _excerpt() -> str:
    return (FIXTURES / "bcv_history_excerpt.html").read_text(encoding="utf-8")


# ---------------------------------------------------------------------------
# Parser
# ---------------------------------------------------------------------------


def test_parse_history_extracts_every_day_block() -> None:
    rows = mod.parse_bcv_history(_excerpt())
    assert len(rows) == 5
    assert all(isinstance(r, RawBcvRow) for r in rows)


def test_parse_history_reads_usd_and_eur_exactly() -> None:
    rows = {r.as_of_date: r for r in mod.parse_bcv_history(_excerpt())}
    assert rows[date(2026, 7, 10)].usd == Decimal("709.69")
    assert rows[date(2026, 7, 10)].eur == Decimal("811.44")
    assert rows[date(2026, 7, 9)].usd == Decimal("700.22")
    assert rows[date(2026, 7, 9)].eur == Decimal("798.29")


def test_parse_history_handles_accented_weekday() -> None:
    """'Miércoles, 8 de julio de 2026' must parse (accent in weekday)."""
    rows = {r.as_of_date: r for r in mod.parse_bcv_history(_excerpt())}
    assert date(2026, 7, 8) in rows
    assert rows[date(2026, 7, 8)].usd == Decimal("685.94")


def test_parse_history_handles_down_marker_day() -> None:
    """A '▼ 0.00%' change marker must not leak into the parsed value."""
    rows = {r.as_of_date: r for r in mod.parse_bcv_history(_excerpt())}
    assert rows[date(2026, 7, 3)].usd == Decimal("652.97")
    assert rows[date(2026, 7, 3)].eur == Decimal("747.32")


def test_parse_history_skips_header_row() -> None:
    """The <thead> row has no parseable date and must be dropped, not raise."""
    rows = mod.parse_bcv_history(_excerpt())
    # 5 body rows, header excluded — proves the header td did not become a row.
    assert len(rows) == 5
    assert all(isinstance(r.as_of_date, date) for r in rows)


@pytest.mark.parametrize(
    "html",
    [
        "",
        "<html><body><p>no table here</p></body></html>",
        "<table><tbody><tr><td>garbage</td></tr></tbody></table>",
    ],
)
def test_parse_history_raises_when_nothing_parses(html: str) -> None:
    with pytest.raises(BcvParseError):
        mod.parse_bcv_history(html)


# ---------------------------------------------------------------------------
# Import (against in-memory migrated DB)
# ---------------------------------------------------------------------------


def _bcv_usd_rows(conn: sqlite3.Connection) -> list[tuple[str, str]]:
    return [
        (r["as_of_date"], r["rate"])
        for r in conn.execute(
            "SELECT as_of_date, rate FROM rates "
            "WHERE source='bcv' AND base='USD' ORDER BY as_of_date"
        ).fetchall()
    ]


def test_import_inserts_missing_rows(in_memory_db: sqlite3.Connection) -> None:
    rows = mod.parse_bcv_history(_excerpt())
    result = mod.import_history(in_memory_db, rows, apply=True)
    # 5 days x (USD + EUR).
    assert result.inserted == 10
    total = in_memory_db.execute(
        "SELECT COUNT(*) FROM rates WHERE source='bcv'"
    ).fetchone()[0]
    assert total == 10


def test_imported_rows_are_indistinguishable_from_live(
    in_memory_db: sqlite3.Connection,
) -> None:
    """Imported rows must look exactly like live-scraper rows to the resolver."""
    rows = mod.parse_bcv_history(_excerpt())
    mod.import_history(in_memory_db, rows, apply=True)
    got = rates_repo.get(
        in_memory_db,
        as_of_date=date(2026, 7, 9),
        base="USD",
        quote="VES",
        source="bcv",
    )
    assert got is not None
    assert got.base == "USD"
    assert got.quote == "VES"
    assert got.source == "bcv"
    assert got.rate == Decimal("700.22")


def test_import_is_idempotent(in_memory_db: sqlite3.Connection) -> None:
    rows = mod.parse_bcv_history(_excerpt())
    first = mod.import_history(in_memory_db, rows, apply=True)
    second = mod.import_history(in_memory_db, rows, apply=True)
    assert first.inserted == 10
    assert second.inserted == 0
    total = in_memory_db.execute(
        "SELECT COUNT(*) FROM rates WHERE source='bcv'"
    ).fetchone()[0]
    assert total == 10


def test_import_skips_existing_and_never_overwrites(
    in_memory_db: sqlite3.Connection,
) -> None:
    """A pre-existing row must be preserved byte-for-byte, not overwritten."""
    sentinel = Rate(
        as_of_date=date(2026, 7, 9),
        base="USD",
        quote="VES",
        rate=Decimal("999.99"),  # deliberately wrong; import must NOT touch it
        source="bcv",
    )
    rates_repo.insert(in_memory_db, sentinel)

    rows = mod.parse_bcv_history(_excerpt())
    result = mod.import_history(in_memory_db, rows, apply=True)

    # 5 days x 2 currencies = 10, minus the 1 already-present USD row = 9.
    assert result.inserted == 9
    preserved = rates_repo.get(
        in_memory_db,
        as_of_date=date(2026, 7, 9),
        base="USD",
        quote="VES",
        source="bcv",
    )
    assert preserved is not None
    assert preserved.rate == Decimal("999.99")


def test_dry_run_plans_but_writes_nothing(in_memory_db: sqlite3.Connection) -> None:
    rows = mod.parse_bcv_history(_excerpt())
    result = mod.import_history(in_memory_db, rows, apply=False)
    assert len(result.planned) == 10
    assert result.inserted == 0
    total = in_memory_db.execute(
        "SELECT COUNT(*) FROM rates WHERE source='bcv'"
    ).fetchone()[0]
    assert total == 0


def test_dry_run_then_apply_inserts_full_set(
    in_memory_db: sqlite3.Connection,
) -> None:
    rows = mod.parse_bcv_history(_excerpt())
    mod.import_history(in_memory_db, rows, apply=False)
    result = mod.import_history(in_memory_db, rows, apply=True)
    assert result.inserted == 10


def test_default_paths_use_clean_root_layout() -> None:
    """Root cleanup 2026-07-11: source HTML lives in data/, backups in backups/."""
    assert mod.DEFAULT_HTML == "data/tasas-bcv-july-9.html"
    assert mod.BACKUP_HINT.startswith(
        'sqlite3 finances.db ".backup backups/finances-backup-'
    )
