"""EPIC-012 one-time backfill — end-to-end test on a 3-row synthetic slice.

Per rule-011, this file lands in the red phase before ``finances/migration/backfill.py``.
Coverage is waived on ``finances/migration/**`` (roadmap EPIC-012), so the
asserts here focus on externally-visible behaviour: the resulting ledger
shape + the P2P pairing that ``run_reconciliation_pass`` applies at the
end of the backfill.
"""
from __future__ import annotations

import csv as _csv
import sqlite3
from decimal import Decimal
from pathlib import Path

import pytest

FIXTURES = Path(__file__).parent / "fixtures" / "backfill"

_PROVINCIAL_HEADERS = [
    "Fecha", "Month", "Month-week", "Week", "Referencia", "Descripción",
    "Sub-Category", "Monto", "Tipo", "Tasa del día", "Monto (BCV)",
    "Tasa USDT", "Monto (USDT)", "Comentarios", "Category",
]
_BINANCE_HEADERS = [
    "Fecha", "Cuenta", "Operación", "Coin", "Amount", "Remark",
    "Month", "Week", "Sub-Category", "Category", "Type",
]


def _write_csv(path: Path, headers: list[str], rows: list[dict[str, str]]) -> None:
    """Emit a legacy-shape CSV (3 empty prelude rows, then header, then data)."""
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = _csv.writer(fh)
        for _ in range(3):
            writer.writerow([""] * len(headers))
        writer.writerow(headers)
        for row in rows:
            writer.writerow([row.get(h, "") for h in headers])


def _write_provincial_csv(path: Path, rows: list[dict[str, str]]) -> None:
    _write_csv(path, _PROVINCIAL_HEADERS, rows)


def _write_binance_csv(path: Path, rows: list[dict[str, str]]) -> None:
    _write_csv(path, _BINANCE_HEADERS, rows)


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


def test_derive_p2p_rates_greedy_empty_when_rate_lookup_covers_all(
    tmp_path: Path,
) -> None:
    """When the Provincial ``Tasa USDT`` column is populated, the date-keyed
    lookup in ``build_rate_index_from_provincial`` already stamps
    ``user_rate`` on the Binance side — there is nothing for the greedy
    fallback to derive, so the returned map is empty."""
    from finances.migration.backfill import (
        build_rate_index_from_provincial,
        derive_p2p_rates_greedy,
    )

    prov = tmp_path / "prov.csv"
    bin_ = tmp_path / "bin.csv"
    _write_provincial_csv(prov, [
        {"Fecha": "05-Nov-2025", "Sub-Category": "Exchange",
         "Monto": "Bs 32,000.00", "Tasa USDT": "Bs 320.00"},
    ])
    _write_binance_csv(bin_, [
        {"Fecha": "05-Nov-2025", "Cuenta": "Funding", "Operación": "P2P-Sell",
         "Coin": "USDT", "Amount": "-$100.00", "Remark": "P2P - 99900001"},
    ])

    rate_lookup = build_rate_index_from_provincial(prov)
    derived = derive_p2p_rates_greedy(bin_, prov, rate_lookup=rate_lookup)

    assert derived == {}


def test_derive_p2p_rates_greedy_single_pair(tmp_path: Path) -> None:
    """Binance orphan (no rate on that date) gets a rate derived from the
    same-day Provincial ``Exchange`` row's Bs amount divided by the USDT
    amount on the Binance leg."""
    from finances.migration.backfill import derive_p2p_rates_greedy

    prov = tmp_path / "prov.csv"
    bin_ = tmp_path / "bin.csv"
    _write_provincial_csv(prov, [
        {"Fecha": "04-Apr-2026", "Sub-Category": "Exchange",
         "Monto": "Bs 20,000.00", "Tasa USDT": ""},
    ])
    _write_binance_csv(bin_, [
        {"Fecha": "04-Apr-2026", "Cuenta": "Funding", "Operación": "P2P-Sell",
         "Coin": "USDT", "Amount": "-$31.85", "Remark": "P2P - hash:abc123"},
    ])

    derived = derive_p2p_rates_greedy(bin_, prov, rate_lookup={})

    assert "p2p:hash:abc123" in derived
    expected = Decimal("20000") / Decimal("31.85")
    # Allow sub-cent difference from Decimal division rounding.
    assert abs(derived["p2p:hash:abc123"] - expected) < Decimal("0.01")


def test_derive_p2p_rates_greedy_pairs_by_descending_magnitude(
    tmp_path: Path,
) -> None:
    """Greedy policy: sort both Provincial Bs amounts and Binance USDT
    amounts by magnitude descending, then pair position-wise. Deterministic
    regardless of CSV row order."""
    from finances.migration.backfill import derive_p2p_rates_greedy

    prov = tmp_path / "prov.csv"
    bin_ = tmp_path / "bin.csv"
    # Small row appears before large in CSV — greedy still pairs large↔large.
    _write_provincial_csv(prov, [
        {"Fecha": "04-Apr-2026", "Sub-Category": "Exchange",
         "Monto": "Bs 10,000.00", "Tasa USDT": ""},
        {"Fecha": "04-Apr-2026", "Sub-Category": "Exchange",
         "Monto": "Bs 50,000.00", "Tasa USDT": ""},
    ])
    _write_binance_csv(bin_, [
        {"Fecha": "04-Apr-2026", "Cuenta": "Funding", "Operación": "P2P-Sell",
         "Coin": "USDT", "Amount": "-$20.00", "Remark": "P2P - SMALL"},
        {"Fecha": "04-Apr-2026", "Cuenta": "Funding", "Operación": "P2P-Sell",
         "Coin": "USDT", "Amount": "-$100.00", "Remark": "P2P - BIG"},
    ])

    derived = derive_p2p_rates_greedy(bin_, prov, rate_lookup={})

    # 50000/100 = 500 (largest pair), 10000/20 = 500 (smaller pair).
    assert derived["p2p:BIG"] == Decimal("500")
    assert derived["p2p:SMALL"] == Decimal("500")


def test_derive_p2p_rates_greedy_skips_date_without_provincial_exchange(
    tmp_path: Path,
) -> None:
    """October 2025 orphans: Binance P2P-Sell with no Provincial Exchange
    row on the same date stays orphan. The greedy pass must not invent a
    rate from unrelated Provincial rows."""
    from finances.migration.backfill import derive_p2p_rates_greedy

    prov = tmp_path / "prov.csv"
    bin_ = tmp_path / "bin.csv"
    _write_provincial_csv(prov, [
        # A non-Exchange provincial row on the same date must not be used.
        {"Fecha": "30-Oct-2025", "Sub-Category": "Food",
         "Monto": "-Bs 280.00", "Tasa USDT": "Bs 229.00"},
    ])
    _write_binance_csv(bin_, [
        {"Fecha": "30-Oct-2025", "Cuenta": "Funding", "Operación": "P2P-Sell",
         "Coin": "USDT", "Amount": "-$66.00",
         "Remark": "P2P - 22817265469083672576"},
    ])

    derived = derive_p2p_rates_greedy(bin_, prov, rate_lookup={})
    assert derived == {}


def test_derive_p2p_rates_greedy_only_fills_orphans(tmp_path: Path) -> None:
    """When the date-keyed rate_lookup already has a rate for a date,
    greedy does not emit an entry for that date's orphans — ``user_rate``
    is already stamped upstream. This avoids double-writing rates."""
    from finances.migration.backfill import derive_p2p_rates_greedy

    prov = tmp_path / "prov.csv"
    bin_ = tmp_path / "bin.csv"
    _write_provincial_csv(prov, [
        {"Fecha": "05-Nov-2025", "Sub-Category": "Exchange",
         "Monto": "Bs 32,000.00", "Tasa USDT": "Bs 320.00"},
    ])
    _write_binance_csv(bin_, [
        {"Fecha": "05-Nov-2025", "Cuenta": "Funding", "Operación": "P2P-Sell",
         "Coin": "USDT", "Amount": "-$100.00", "Remark": "P2P - 99900001"},
    ])

    # date-keyed lookup hit — greedy stays out.
    derived = derive_p2p_rates_greedy(
        bin_, prov, rate_lookup={__import__("datetime").date(2025, 11, 5): Decimal("320")}
    )
    assert derived == {}


def test_run_backfill_stamps_derived_rate_on_orphan_p2p_sell(
    in_memory_db: sqlite3.Connection, tmp_path: Path
) -> None:
    """End-to-end: a Binance P2P-Sell with no Tasa USDT rate, paired with
    a same-day Provincial Exchange row, ends up with a derived ``user_rate``
    in the database and its transfer_id populated by the reconciliation
    pass (pairing then works as if the rate had been in the CSV)."""
    from finances.migration.backfill import run_backfill

    data_dir = tmp_path / "data"
    data_dir.mkdir()
    _write_provincial_csv(data_dir / "Finanzas - Provincial.csv", [
        {"Fecha": "04-Apr-2026", "Sub-Category": "Exchange",
         "Descripción": "Venta USDT",
         "Monto": "Bs 20,000.00", "Tipo": "Transfer",
         "Tasa USDT": ""},
    ])
    _write_binance_csv(data_dir / "Finanzas - Binance.csv", [
        {"Fecha": "04-Apr-2026", "Cuenta": "Funding", "Operación": "P2P-Sell",
         "Coin": "USDT", "Amount": "-$31.85",
         "Remark": "P2P - hash:abc123"},
    ])
    # BCV CSV is optional-ish but run_backfill checks for its existence path.
    _write_csv(data_dir / "Finanzas - BCV.csv", ["Dia", "USD", "EURO"], [])

    run_backfill(in_memory_db, data_dir)

    bin_rate = in_memory_db.execute(
        "SELECT user_rate FROM transactions "
        "WHERE source='binance' AND description LIKE 'P2P SELL%'"
    ).fetchone()
    assert bin_rate is not None
    rate = Decimal(str(bin_rate["user_rate"]))
    # 20,000 / 31.85 ≈ 627.94
    assert Decimal("627") < rate < Decimal("629")


def test_run_backfill_applies_legacy_sub_category_mapping(
    in_memory_db: sqlite3.Connection, backfill_data_dir: Path
) -> None:
    """The legacy Provincial/Binance CSVs carry a `Sub-Category` column
    that is Julio's own hand-categorization. Per the closed legacy→v1
    mapping (2026-04-20), backfill stamps `category_id` on every row
    where the legacy annotation resolves to a v1 category — this is
    the authoritative source, not the rules engine (the rules engine
    runs afterwards for rows without a legacy annotation)."""
    from finances.migration.backfill import run_backfill

    report = run_backfill(in_memory_db, backfill_data_dir)

    # "COM. PAGO MOVIL" with legacy Sub-Category `Commissions` → `Fees`.
    # The regex rule would also catch it, but the legacy path wins by
    # running first (lower-latency, more specific to this user's data).
    commission = in_memory_db.execute(
        "SELECT c.name FROM transactions t "
        "JOIN categories c ON c.id = t.category_id "
        "WHERE t.description LIKE 'COM. PAGO MOVIL%'"
    ).fetchone()
    assert commission is not None
    assert commission["name"] == "Fees"

    # "PANADERIA LUISANA 2004" legacy Sub-Category `Food` → `Groceries`
    # after 2026-04-20 rename.
    bakery = in_memory_db.execute(
        "SELECT c.name FROM transactions t "
        "JOIN categories c ON c.id = t.category_id "
        "WHERE t.description LIKE '%PANADERIA%'"
    ).fetchone()
    assert bakery is not None
    assert bakery["name"] == "Groceries"

    # Report exposes how many rows the legacy pass stamped.
    assert report.rows_legacy_mapped >= 2


def test_run_backfill_auto_categorizes_matching_rows(
    in_memory_db: sqlite3.Connection, backfill_data_dir: Path
) -> None:
    """Per ADR-006 / rule-006, the categorization engine runs as part of
    the backfill. Rows whose description hits a seeded rule — or whose
    legacy Sub-Category maps via the closed table — must come out with
    ``category_id`` set and ``needs_review=0``."""
    from finances.migration.backfill import run_backfill

    run_backfill(in_memory_db, backfill_data_dir)

    matched = in_memory_db.execute(
        "SELECT category_id, needs_review FROM transactions "
        "WHERE description LIKE '%PANADERIA%'"
    ).fetchone()
    assert matched is not None
    assert matched["category_id"] is not None
    assert matched["needs_review"] == 0

    commission = in_memory_db.execute(
        "SELECT category_id, needs_review FROM transactions "
        "WHERE description LIKE 'COM. PAGO MOVIL%'"
    ).fetchone()
    assert commission is not None
    assert commission["category_id"] is not None
    assert commission["needs_review"] == 0


def test_run_backfill_leaves_unmatched_rows_needing_review(
    in_memory_db: sqlite3.Connection, tmp_path: Path
) -> None:
    """Rows with no legacy Sub-Category mapping AND no rules-engine match
    stay ``needs_review=1`` with ``category_id`` NULL — Phase F residual."""
    from finances.migration.backfill import run_backfill

    data_dir = tmp_path / "data"
    data_dir.mkdir()
    # Sub-Category 'Mystery' is not in the closed mapping and matches no
    # seeded rule; the row must stay needs_review after backfill.
    _write_provincial_csv(data_dir / "Finanzas - Provincial.csv", [
        {"Fecha": "15-Feb-2026", "Sub-Category": "Mystery",
         "Referencia": "UNFAMILIAR MERCHANT XYZ",
         "Descripción": "Gasto raro", "Monto": "-Bs 100.00",
         "Tipo": "Expense", "Tasa USDT": "Bs 400.00"},
    ])
    _write_binance_csv(data_dir / "Finanzas - Binance.csv", [])
    _write_csv(data_dir / "Finanzas - BCV.csv", ["Dia", "USD", "EURO"], [])

    run_backfill(in_memory_db, data_dir)

    row = in_memory_db.execute(
        "SELECT category_id, needs_review FROM transactions "
        "WHERE description LIKE 'UNFAMILIAR%'"
    ).fetchone()
    assert row is not None
    assert row["category_id"] is None
    assert row["needs_review"] == 1


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
