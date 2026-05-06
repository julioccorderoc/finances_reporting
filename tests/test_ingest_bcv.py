from __future__ import annotations

from datetime import date
from decimal import Decimal
from pathlib import Path
import sqlite3

import httpx
import pytest
from pydantic import ValidationError

from finances.ingest import bcv as bcv_module
from finances.ingest.bcv import (
    BCV_URL,
    BcvParseError,
    RawBcvRow,
    SOURCE_NAME,
    clean_currency,
    fetch_bcv_html,
    ingest_bcv,
    parse_bcv_html,
    parse_spanish_date,
)


FIXTURES = Path(__file__).parent / "fixtures"


def _snapshot() -> str:
    return (FIXTURES / "bcv_snapshot.html").read_text(encoding="utf-8")


def _mangled() -> str:
    return (FIXTURES / "bcv_mangled.html").read_text(encoding="utf-8")


def test_module_constants_are_correct() -> None:
    """BCV_URL and SOURCE_NAME pin to documented values."""
    assert SOURCE_NAME == "bcv"
    assert isinstance(BCV_URL, str) and BCV_URL.startswith("http")


def test_parse_spanish_date_happy() -> None:
    assert parse_spanish_date("Viernes, 17 de abril de 2026") == date(2026, 4, 17)
    assert parse_spanish_date("Miércoles, 15 de enero de 2025") == date(2025, 1, 15)


@pytest.mark.parametrize(
    "bad",
    [
        "",
        "no commas here",
        "Jueves, notadate",
        "Jueves, 40 de febrero de 2026",
        "Jueves, 12 de notamonth de 2026",
    ],
)
def test_parse_spanish_date_handles_malformed_inputs(bad: str) -> None:
    assert parse_spanish_date(bad) is None


def test_clean_currency_happy() -> None:
    assert clean_currency("Bs.S\xa0480,25") == Decimal("480.25")
    assert clean_currency(" Bs.S 12,00 ") == Decimal("12.00")


@pytest.mark.parametrize("bad", ["", "Bs.S", "abc", "Bs.S —"])
def test_clean_currency_raises_on_unparseable(bad: str) -> None:
    with pytest.raises(ValueError):
        clean_currency(bad)


def test_raw_bcv_row_rejects_float() -> None:
    """Rule-009: Decimal-only at trust boundary. float is lossy, must reject."""
    with pytest.raises(ValidationError):
        RawBcvRow(as_of_date=date(2026, 4, 17), usd=1.5, eur=Decimal("2"))
    row = RawBcvRow(as_of_date=date(2026, 4, 17), usd="480.25", eur="565.41")
    assert isinstance(row.usd, Decimal)
    assert isinstance(row.eur, Decimal)
    assert row.usd == Decimal("480.25")
    assert row.eur == Decimal("565.41")


def test_raw_bcv_row_forbids_extra_fields() -> None:
    with pytest.raises(ValidationError):
        RawBcvRow(
            as_of_date=date(2026, 4, 17),
            usd=Decimal("480.25"),
            eur=Decimal("565.41"),
            bogus="x",
        )


@pytest.mark.snapshot
def test_parse_bcv_html_parses_full_snapshot() -> None:
    rows = parse_bcv_html(_snapshot())
    assert len(rows) == 67
    assert rows[0].as_of_date == date(2026, 4, 17)
    assert rows[0].usd == Decimal("480.25")
    assert rows[0].eur == Decimal("565.41")
    assert rows[1].as_of_date == date(2026, 4, 16)
    assert rows[1].usd == Decimal("479.77")
    assert rows[1].eur == Decimal("565.98")
    for r in rows:
        assert isinstance(r.usd, Decimal) and r.usd > 0
        assert isinstance(r.eur, Decimal) and r.eur > 0


def test_parse_bcv_html_raises_on_missing_tbody() -> None:
    with pytest.raises(BcvParseError):
        parse_bcv_html(_mangled())


def test_parse_bcv_html_raises_on_empty_tbody() -> None:
    html = "<html><body><table><tbody></tbody></table></body></html>"
    with pytest.raises(BcvParseError):
        parse_bcv_html(html)


def test_parse_bcv_html_raises_when_no_valid_rows() -> None:
    html = (
        "<html><body><table><tbody>"
        "<tr><td>only-one</td></tr>"
        "<tr><td>two</td><td>cells</td></tr>"
        "</tbody></table></body></html>"
    )
    with pytest.raises(BcvParseError):
        parse_bcv_html(html)


def test_fetch_bcv_html_returns_text_on_200(mocker) -> None:
    response = mocker.MagicMock()
    response.text = "<html>ok</html>"
    response.raise_for_status = mocker.MagicMock()
    client_instance = mocker.MagicMock()
    client_instance.get.return_value = response
    client_cm = mocker.MagicMock()
    client_cm.__enter__.return_value = client_instance
    client_cm.__exit__.return_value = False
    client_cls = mocker.patch.object(bcv_module.httpx, "Client", return_value=client_cm)

    out = fetch_bcv_html("https://example.test")

    assert out == "<html>ok</html>"
    _, kwargs = client_cls.call_args
    assert kwargs.get("timeout") == 10.0
    client_instance.get.assert_called_once_with("https://example.test")
    response.raise_for_status.assert_called_once()


def test_fetch_bcv_html_raises_on_http_error(mocker) -> None:
    response = mocker.MagicMock()
    response.raise_for_status.side_effect = httpx.HTTPStatusError(
        "500",
        request=mocker.MagicMock(),
        response=mocker.MagicMock(),
    )
    client_instance = mocker.MagicMock()
    client_instance.get.return_value = response
    client_cm = mocker.MagicMock()
    client_cm.__enter__.return_value = client_instance
    client_cm.__exit__.return_value = False
    mocker.patch.object(bcv_module.httpx, "Client", return_value=client_cm)

    with pytest.raises(httpx.HTTPStatusError):
        fetch_bcv_html("https://example.test")


def test_ingest_bcv_happy_path(seeded_db: sqlite3.Connection) -> None:
    inserted = ingest_bcv(seeded_db, html=_snapshot())
    assert inserted == 134

    total = seeded_db.execute(
        "SELECT COUNT(*) FROM rates WHERE source='bcv'"
    ).fetchone()[0]
    assert total == 134

    usd_count = seeded_db.execute(
        "SELECT COUNT(*) FROM rates WHERE source='bcv' AND base='USD'"
    ).fetchone()[0]
    assert usd_count == 67

    eur_count = seeded_db.execute(
        "SELECT COUNT(*) FROM rates WHERE source='bcv' AND base='EUR'"
    ).fetchone()[0]
    assert eur_count == 67

    run = seeded_db.execute(
        "SELECT source, status, rows_inserted, error FROM import_runs "
        "WHERE source='bcv' ORDER BY id DESC LIMIT 1"
    ).fetchone()
    assert run is not None
    assert run["source"] == "bcv"
    assert run["status"] == "success"
    assert run["rows_inserted"] == 134
    assert run["error"] is None

    state = seeded_db.execute(
        "SELECT source FROM import_state WHERE source='bcv'"
    ).fetchone()
    assert state is not None


def test_ingest_bcv_idempotent_same_day(seeded_db: sqlite3.Connection) -> None:
    first = ingest_bcv(seeded_db, html=_snapshot())
    second = ingest_bcv(seeded_db, html=_snapshot())
    assert first == 134
    assert second == 0

    total = seeded_db.execute(
        "SELECT COUNT(*) FROM rates WHERE source='bcv'"
    ).fetchone()[0]
    assert total == 134

    runs = seeded_db.execute(
        "SELECT status FROM import_runs WHERE source='bcv' ORDER BY id ASC"
    ).fetchall()
    assert len(runs) == 2
    assert all(r["status"] == "success" for r in runs)


def test_ingest_bcv_parse_failure_writes_error_run(
    seeded_db: sqlite3.Connection,
) -> None:
    with pytest.raises(BcvParseError):
        ingest_bcv(seeded_db, html=_mangled())

    rates_count = seeded_db.execute(
        "SELECT COUNT(*) FROM rates WHERE source='bcv'"
    ).fetchone()[0]
    assert rates_count == 0

    run = seeded_db.execute(
        "SELECT source, status, error FROM import_runs "
        "WHERE source='bcv' ORDER BY id DESC LIMIT 1"
    ).fetchone()
    assert run is not None
    assert run["source"] == "bcv"
    assert run["status"] == "error"
    assert run["error"] is not None
    assert "BcvParseError" in run["error"]


def test_ingest_bcv_fetch_failure_writes_error_run(
    seeded_db: sqlite3.Connection,
    mocker,
) -> None:
    mocker.patch.object(
        bcv_module,
        "fetch_bcv_html",
        side_effect=httpx.ConnectError("boom"),
    )

    with pytest.raises(httpx.ConnectError):
        ingest_bcv(seeded_db)

    rates_count = seeded_db.execute(
        "SELECT COUNT(*) FROM rates WHERE source='bcv'"
    ).fetchone()[0]
    assert rates_count == 0

    run = seeded_db.execute(
        "SELECT status, error FROM import_runs "
        "WHERE source='bcv' ORDER BY id DESC LIMIT 1"
    ).fetchone()
    assert run is not None
    assert run["status"] == "error"
    assert run["error"] is not None
    assert "boom" in run["error"] or "ConnectError" in run["error"]


def test_ingest_bcv_preserves_existing_rates_on_failure(
    seeded_db: sqlite3.Connection,
) -> None:
    """Rule-007(c): parse failure leaves existing rates rows untouched."""
    from finances.db.repos import rates as rates_repo
    from finances.domain.models import Rate

    rates_repo.insert(
        seeded_db,
        Rate(
            as_of_date=date(2026, 4, 17),
            base="USD",
            quote="VES",
            rate=Decimal("999"),
            source="bcv",
        ),
    )
    seeded_db.commit()

    with pytest.raises(BcvParseError):
        ingest_bcv(seeded_db, html=_mangled())

    row = seeded_db.execute(
        "SELECT rate FROM rates "
        "WHERE source='bcv' AND base='USD' AND quote='VES' AND as_of_date=?",
        (date(2026, 4, 17).isoformat(),),
    ).fetchone()
    assert row is not None
    assert Decimal(str(row["rate"])) == Decimal("999")
