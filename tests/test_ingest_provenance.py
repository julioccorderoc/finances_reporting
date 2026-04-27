"""Per-source guarantee that every ingest writes one ``import_runs`` row.

Rule-007 / ADR-007: every ingest run MUST start an ``import_runs`` row at
entry and finish it (status='success' on success, status='error' with an
``error`` message on failure) at exit. ``finances/ingest/bcv.py`` is the
reference implementation. These tests pin the same behaviour on every
other ingest entry point.

Use the existing ``db_conn`` fixture from ``tests/conftest.py`` (a
file-backed, migrated sqlite3 connection that closes on teardown).

Each test below is owned by a single ingest module; tests in this file
are intentionally short and isolated so the contract is obvious.
"""
from __future__ import annotations


# === BEGIN binance ============================================================
def test_binance_sync_writes_import_runs_row(
    mocked_binance_sdk, seeded_db
) -> None:
    """``binance.sync_binance`` records start + finish in ``import_runs``."""
    from finances.ingest import binance

    binance.sync_binance(seeded_db, client=mocked_binance_sdk)

    rows = seeded_db.execute(
        "SELECT source, status, error, rows_inserted, finished_at "
        "FROM import_runs WHERE source = 'binance'"
    ).fetchall()
    assert len(rows) == 1, f"expected exactly 1 binance run row, got {len(rows)}"
    row = rows[0]
    assert row["status"] == "success"
    assert row["error"] is None
    assert row["finished_at"] is not None
    assert row["rows_inserted"] is not None
# === END binance ==============================================================


# === BEGIN provincial =========================================================
def test_provincial_ingest_writes_import_runs_row(
    tmp_path, seeded_db
) -> None:
    """``provincial.ingest_csv`` records start + finish in ``import_runs``."""
    from finances.ingest import provincial

    csv_path = tmp_path / "provincial.csv"
    csv_path.write_text(
        "Fecha;Descripción;Monto;Saldo\n"
        "15/03/2026;TEST CHARGE;-100,00;500,00\n",
        encoding="utf-8",
    )

    provincial.ingest_csv(seeded_db, csv_path, run_pairing=False)

    rows = seeded_db.execute(
        "SELECT source, status, error, rows_inserted, finished_at "
        "FROM import_runs WHERE source = 'provincial'"
    ).fetchall()
    assert len(rows) == 1, f"expected exactly 1 provincial run row, got {len(rows)}"
    row = rows[0]
    assert row["status"] == "success"
    assert row["error"] is None
    assert row["finished_at"] is not None
# === END provincial ===========================================================


# === BEGIN p2p_rates ==========================================================
def test_p2p_rates_ingest_writes_import_runs_row(
    monkeypatch, db_conn
) -> None:
    """``p2p_rates.ingest_p2p_rates`` records start + finish in ``import_runs``."""
    from decimal import Decimal

    from finances.ingest import p2p_rates
    from finances.ingest.p2p_rates import RawP2pAdvert

    def _fake_adverts(asset, fiat, side, *, rows, client=None):
        price = Decimal("50.00") if side == "BUY" else Decimal("51.00")
        return [
            RawP2pAdvert(
                price=price,
                asset=asset,
                fiat_unit=fiat,
                trade_type=side,
            )
        ]

    monkeypatch.setattr(p2p_rates, "fetch_p2p_adverts", _fake_adverts)

    p2p_rates.ingest_p2p_rates(db_conn)

    rows = db_conn.execute(
        "SELECT source, status, error, rows_inserted, finished_at "
        "FROM import_runs WHERE source = 'binance_p2p_median'"
    ).fetchall()
    assert len(rows) == 1, (
        f"expected exactly 1 binance_p2p_median run row, got {len(rows)}"
    )
    row = rows[0]
    assert row["status"] == "success"
    assert row["error"] is None
    assert row["finished_at"] is not None
# === END p2p_rates ============================================================


# === BEGIN backfill ===========================================================
def test_backfill_writes_import_runs_row(tmp_path, db_conn) -> None:
    """``backfill.run_backfill`` records an umbrella row in ``import_runs``.

    Per the plan's intent: even a no-op backfill (header-only CSVs)
    must still leave an audit trail under source='backfill'. Inner
    per-source rows (bcv/provincial/binance) are orthogonal — this
    test only asserts the umbrella row.
    """
    from finances.migration import backfill

    data_dir = tmp_path / "legacy"
    data_dir.mkdir()
    # Header-only CSVs satisfy the FileNotFoundError gate AND yield zero
    # data rows from _iter_legacy_csv_rows (which keys off the "Fecha"
    # token in _HEADER_TOKENS).
    (data_dir / backfill.BINANCE_CSV_NAME).write_text(
        "Fecha\n", encoding="utf-8"
    )
    (data_dir / backfill.PROVINCIAL_CSV_NAME).write_text(
        "Fecha\n", encoding="utf-8"
    )

    backfill.run_backfill(db_conn, data_dir)

    rows = db_conn.execute(
        "SELECT source, status, error, finished_at "
        "FROM import_runs WHERE source = 'backfill'"
    ).fetchall()
    assert len(rows) == 1, f"expected exactly 1 backfill run row, got {len(rows)}"
    row = rows[0]
    assert row["status"] == "success"
    assert row["error"] is None
    assert row["finished_at"] is not None
# === END backfill =============================================================
