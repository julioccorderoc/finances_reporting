"""EPIC-012 one-time backfill — end-to-end test on a 3-row synthetic slice.

Per rule-011, this file lands in the red phase before ``finances/migration/backfill.py``.
Coverage is waived on ``finances/migration/**`` (roadmap EPIC-012), so the
asserts here focus on externally-visible behaviour: the resulting ledger
shape + the P2P pairing that ``run_reconciliation_pass`` applies at the
end of the backfill.
"""
from __future__ import annotations

import sqlite3
from decimal import Decimal
from pathlib import Path

import pytest

FIXTURES = Path(__file__).parent / "fixtures" / "backfill"


@pytest.fixture
def backfill_data_dir(tmp_path: Path) -> Path:
    """Copy the synthetic 3-row slices into an isolated tmp dir."""
    import shutil

    dest = tmp_path / "data"
    dest.mkdir()
    for name in (
        "Finanzas - Binance.csv",
        "Finanzas - Provincial.csv",
        "Finanzas - BCV.csv",
    ):
        shutil.copy(FIXTURES / name, dest / name)
    return dest


def _count(conn: sqlite3.Connection, sql: str, *params: object) -> int:
    row = conn.execute(sql, params).fetchone()
    return int(row[0])


def test_run_backfill_materializes_expected_rows(
    in_memory_db: sqlite3.Connection, backfill_data_dir: Path
) -> None:
    from finances.migration.backfill import run_backfill

    report = run_backfill(in_memory_db, backfill_data_dir)

    # 3 Binance legacy rows expand to 4 transactions (Internal Transfer is
    # a double-entry pair; Deposit + P2P-Sell contribute one each).
    binance_rows = _count(
        in_memory_db, "SELECT COUNT(*) FROM transactions WHERE source='binance'"
    )
    assert binance_rows == 4

    provincial_rows = _count(
        in_memory_db, "SELECT COUNT(*) FROM transactions WHERE source='provincial'"
    )
    assert provincial_rows == 3

    # 3 BCV rows × 2 currencies (USD + EUR) = 6 rate rows.
    bcv_rates = _count(
        in_memory_db, "SELECT COUNT(*) FROM rates WHERE source='bcv'"
    )
    assert bcv_rates == 6

    # Report tallies match the ledger.
    assert report.binance_rows_inserted == 4
    assert report.provincial_rows_inserted == 3
    assert report.bcv_rates_inserted == 6


def test_run_backfill_pairs_bank_anchored_p2p(
    in_memory_db: sqlite3.Connection, backfill_data_dir: Path
) -> None:
    from finances.migration.backfill import run_backfill

    report = run_backfill(in_memory_db, backfill_data_dir)

    # One bank-anchored proposal was found and applied.
    assert report.reconciliation is not None
    assert report.reconciliation.proposals_found == 1
    assert report.reconciliation.proposals_applied == 1

    # The P2P pair (one provincial leg + one binance leg) shares a
    # transfer_id. The separate internal-transfer pair (both binance) has
    # its own id; filter to the row sets that cross the source boundary.
    cross_source_pairs = in_memory_db.execute(
        """
        SELECT transfer_id
        FROM transactions
        WHERE transfer_id IS NOT NULL
        GROUP BY transfer_id
        HAVING COUNT(DISTINCT source) = 2
        """
    ).fetchall()
    assert len(cross_source_pairs) == 1
    pair_tid = cross_source_pairs[0]["transfer_id"]
    legs = in_memory_db.execute(
        "SELECT source FROM transactions WHERE transfer_id = ? ORDER BY source",
        (pair_tid,),
    ).fetchall()
    assert [row["source"] for row in legs] == ["binance", "provincial"]


def test_run_backfill_is_idempotent(
    in_memory_db: sqlite3.Connection, backfill_data_dir: Path
) -> None:
    from finances.migration.backfill import run_backfill

    run_backfill(in_memory_db, backfill_data_dir)
    total_first = _count(in_memory_db, "SELECT COUNT(*) FROM transactions")

    # Re-running on the same DB with the same CSVs must insert zero rows.
    # `force=True` bypasses the non-empty-DB guard that otherwise protects
    # cleanup state from being wiped by an accidental second backfill.
    second = run_backfill(in_memory_db, backfill_data_dir, force=True)

    total_second = _count(in_memory_db, "SELECT COUNT(*) FROM transactions")
    assert total_second == total_first
    assert second.binance_rows_inserted == 0
    assert second.provincial_rows_inserted == 0


def test_run_backfill_creates_missing_accounts(
    in_memory_db: sqlite3.Connection, backfill_data_dir: Path
) -> None:
    from finances.migration.backfill import run_backfill

    # Fresh DB has no accounts seeded; backfill must create them.
    pre = _count(in_memory_db, "SELECT COUNT(*) FROM accounts")
    assert pre == 0

    run_backfill(in_memory_db, backfill_data_dir)

    post_names = {
        r["name"] for r in in_memory_db.execute("SELECT name FROM accounts")
    }
    assert {
        "Provincial Bolivares",
        "Binance Spot",
        "Binance Funding",
        "Binance Earn",
        "Cash USD",
    } <= post_names


def test_run_backfill_missing_file_raises(
    in_memory_db: sqlite3.Connection, tmp_path: Path
) -> None:
    from finances.migration.backfill import run_backfill

    empty = tmp_path / "empty"
    empty.mkdir()
    with pytest.raises(FileNotFoundError):
        run_backfill(in_memory_db, empty)


def test_run_backfill_refuses_on_non_empty_db(
    in_memory_db: sqlite3.Connection, backfill_data_dir: Path
) -> None:
    """Re-running backfill resets needs_review/category_id via upsert; refuse
    by default so a stray re-run can't wipe cleanup work."""
    from finances.migration.backfill import run_backfill

    run_backfill(in_memory_db, backfill_data_dir)
    with pytest.raises(RuntimeError, match="already contains"):
        run_backfill(in_memory_db, backfill_data_dir)


def test_run_backfill_force_bypasses_guard(
    in_memory_db: sqlite3.Connection, backfill_data_dir: Path
) -> None:
    """`force=True` allows the explicit re-run (migration author's escape hatch)."""
    from finances.migration.backfill import run_backfill

    run_backfill(in_memory_db, backfill_data_dir)
    # Must not raise. Returns a fresh report.
    second = run_backfill(in_memory_db, backfill_data_dir, force=True)
    assert second.binance_rows_inserted == 0


def test_provincial_rows_carry_user_rate_from_tasa_usdt(
    in_memory_db: sqlite3.Connection, backfill_data_dir: Path
) -> None:
    """Tasa USDT from the legacy Provincial CSV becomes the Provincial row
    ``user_rate`` so ``v_transactions_usd`` can render USD correctly."""
    from finances.migration.backfill import run_backfill

    run_backfill(in_memory_db, backfill_data_dir)

    rates = [
        Decimal(str(r["user_rate"]))
        for r in in_memory_db.execute(
            "SELECT user_rate FROM transactions WHERE source='provincial' "
            "AND user_rate IS NOT NULL"
        )
    ]
    # All 3 provincial fixtures carry a Tasa USDT value (320, 314.76, 315).
    assert Decimal("320") in rates
    assert Decimal("314.76") in rates
    assert Decimal("315") in rates
