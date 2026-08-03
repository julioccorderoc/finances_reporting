"""Legacy Binance Convert rows must record both sides of the swap.

A conversion moves money between denominations: USDC out, USDT in. The
legacy sheet records it two ways.

Some rows carry each leg separately, one row per side::

    23-Nov-2025, USDC, -$1,240.00, "Cambio de sueldo a USDT"
    23-Nov-2025, USDT,  $1,239.18, "Cambio a USDT de sueldo"

Others carry only the outgoing leg, and put the destination in the
remark::

    21-Dec-2025, USDC, -$1,350.00, "Converted to 1349.10883625 USDT"

``_handle_binance_convert`` treated every row as the first shape, so for
the second shape the incoming USDT was never written. The outgoing leg
stood alone and, because reports count every row that is not a transfer,
read as money spent. Five such rows in the production ledger: 5 353.94
USDC of expense on money that only changed denomination.

The destination was in the source data the whole time — nobody parsed it.
"""

from __future__ import annotations

import csv as _csv
import sqlite3
from decimal import Decimal
from pathlib import Path

import pytest

_BINANCE_HEADERS = [
    "Fecha", "Cuenta", "Operación", "Coin", "Amount", "Remark",
    "Month", "Week", "Sub-Category", "Category", "Type",
]


def _write_binance_csv(path: Path, rows: list[dict[str, str]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = _csv.writer(fh)
        for _ in range(3):
            writer.writerow([""] * len(_BINANCE_HEADERS))
        writer.writerow(_BINANCE_HEADERS)
        for row in rows:
            writer.writerow([row.get(h, "") for h in _BINANCE_HEADERS])


def _convert_row(*, coin: str, amount: str, remark: str) -> dict[str, str]:
    return {
        "Fecha": "21-Dec-2025",
        "Cuenta": "Spot",
        "Operación": "Binance Convert",
        "Coin": coin,
        "Amount": amount,
        "Remark": remark,
        "Month": "2025-Dec",
        "Week": "25-W51",
        "Sub-Category": "Exchange",
        "Category": "Others",
        "Type": "Transfer",
    }


@pytest.fixture
def convert_only_dir(tmp_path: Path) -> Path:
    """A data dir holding one Binance sheet and nothing else."""
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    _write_binance_csv(
        data_dir / "Finanzas - Binance.csv",
        [
            _convert_row(
                coin="USDC",
                amount="-$1,350.00",
                remark="Converted to 1349.10883625 USDT",
            )
        ],
    )
    return data_dir


def _rows(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    return conn.execute(
        "SELECT id, kind, amount, currency, source_ref, description "
        "FROM transactions WHERE source_ref LIKE 'convert:%' "
        "ORDER BY source_ref"
    ).fetchall()


class TestDestinationInRemark:
    def test_both_legs_are_written(
        self, seeded_db: sqlite3.Connection, convert_only_dir: Path
    ):
        from finances.migration.backfill import run_backfill

        run_backfill(seeded_db, convert_only_dir)

        rows = _rows(seeded_db)
        assert len(rows) == 2

        outgoing = next(r for r in rows if r["source_ref"].endswith(":from"))
        incoming = next(r for r in rows if r["source_ref"].endswith(":to"))

        assert outgoing["kind"] == "expense"
        assert Decimal(str(outgoing["amount"])) == Decimal("-1350.00")
        assert outgoing["currency"] == "USDC"

        assert incoming["kind"] == "income"
        assert Decimal(str(incoming["amount"])) == Decimal("1349.10883625")
        assert incoming["currency"] == "USDT"

    def test_both_legs_share_one_order_id(
        self, seeded_db: sqlite3.Connection, convert_only_dir: Path
    ):
        """Sharing the id is what lets doctor recognise them as one swap."""
        from finances.migration.backfill import run_backfill

        run_backfill(seeded_db, convert_only_dir)

        refs = [r["source_ref"] for r in _rows(seeded_db)]
        keys = {ref.rsplit(":", 1)[0] for ref in refs}
        assert len(keys) == 1

    def test_description_names_both_sides(
        self, seeded_db: sqlite3.Connection, convert_only_dir: Path
    ):
        from finances.migration.backfill import run_backfill

        run_backfill(seeded_db, convert_only_dir)

        for row in _rows(seeded_db):
            assert "UNKNOWN" not in row["description"]
            assert "USDC" in row["description"]
            assert "USDT" in row["description"]

    def test_the_swap_nets_to_roughly_zero(
        self, seeded_db: sqlite3.Connection, convert_only_dir: Path
    ):
        """The whole point: converting money is not spending it."""
        from finances.migration.backfill import run_backfill

        run_backfill(seeded_db, convert_only_dir)

        total = sum(Decimal(str(r["amount"])) for r in _rows(seeded_db))
        assert abs(total) < Decimal("1")

    def test_doctor_reports_no_orphan_leg(
        self, seeded_db: sqlite3.Connection, convert_only_dir: Path
    ):
        from finances.domain.integrity import run_checks
        from finances.migration.backfill import run_backfill

        run_backfill(seeded_db, convert_only_dir)

        checks = [f.check for f in run_checks(seeded_db).findings]
        assert "convert_leg_without_counterpart" not in checks

    def test_rerunning_the_backfill_adds_nothing(
        self, seeded_db: sqlite3.Connection, convert_only_dir: Path
    ):
        from finances.migration.backfill import run_backfill

        run_backfill(seeded_db, convert_only_dir)
        first = [dict(r) for r in _rows(seeded_db)]
        run_backfill(seeded_db, convert_only_dir)
        second = [dict(r) for r in _rows(seeded_db)]

        assert first == second


class TestLegPerRowShapeIsUntouched:
    """Rows that already carry one leg each must stay single-legged.

    Writing a second leg for these would double-count the conversion —
    the sheet already has the other side as its own row.
    """

    def test_a_remark_without_a_destination_yields_one_row(
        self, seeded_db: sqlite3.Connection, tmp_path: Path
    ):
        from finances.migration.backfill import run_backfill

        data_dir = tmp_path / "data"
        data_dir.mkdir()
        _write_binance_csv(
            data_dir / "Finanzas - Binance.csv",
            [
                _convert_row(
                    coin="USDC",
                    amount="-$1,240.00",
                    remark="Cambio de sueldo a USDT",
                ),
                _convert_row(
                    coin="USDT",
                    amount="$1,239.18",
                    remark="Cambio a USDT de sueldo",
                ),
            ],
        )

        run_backfill(seeded_db, data_dir)

        rows = _rows(seeded_db)
        assert len(rows) == 2
        kinds = sorted(r["kind"] for r in rows)
        assert kinds == ["expense", "income"]


class TestRemarkParsing:
    @pytest.mark.parametrize(
        "remark, expected",
        [
            ("Converted to 1349.10883625 USDT", (Decimal("1349.10883625"), "USDT")),
            ("Converted to 540.02900761 USDT", (Decimal("540.02900761"), "USDT")),
            ("converted to 1,666.44 USDT", (Decimal("1666.44"), "USDT")),
            ("Converted to 12 BTC", (Decimal("12"), "BTC")),
        ],
    )
    def test_recognises_a_destination(self, remark: str, expected):
        from finances.migration.backfill import parse_convert_destination

        assert parse_convert_destination(remark) == expected

    @pytest.mark.parametrize(
        "remark",
        [
            "Cambio de sueldo a USDT",
            "Cambio a USDT de sueldo",
            "",
            "Converted to USDT",
            "some unrelated note",
        ],
    )
    def test_returns_none_when_there_is_no_destination(self, remark: str):
        from finances.migration.backfill import parse_convert_destination

        assert parse_convert_destination(remark) is None
