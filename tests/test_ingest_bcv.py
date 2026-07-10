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
def test_parse_bcv_html_parses_homepage_snapshot() -> None:
    """Homepage publishes one snapshot per day → exactly 1 row, USD+EUR only."""
    rows = parse_bcv_html(_snapshot())
    assert len(rows) == 1
    assert rows[0].as_of_date == date(2026, 4, 28)
    assert rows[0].usd == Decimal("485.22510000")
    assert rows[0].eur == Decimal("569.29520082")
    assert isinstance(rows[0].usd, Decimal) and rows[0].usd > 0
    assert isinstance(rows[0].eur, Decimal) and rows[0].eur > 0


def test_parse_bcv_html_ignores_decoy_date_spans() -> None:
    """The fixture carries decoy date-display-single spans (news/archive
    blocks) BEFORE the rates widget, mirroring the live homepage. The parser
    must anchor to the widget's own 'Fecha Valor' span, not document order."""
    html = _snapshot()
    assert html.count('class="date-display-single"') >= 3, (
        "fixture must contain decoy date spans for this test to mean anything"
    )
    rows = parse_bcv_html(html)
    assert rows[0].as_of_date == date(2026, 4, 28)  # not 2026-05-01 (decoy)


def test_assert_plausible_as_of_date_accepts_recent() -> None:
    from finances.ingest.bcv import assert_plausible_as_of_date

    assert_plausible_as_of_date(date(2026, 7, 8), today=date(2026, 7, 10))
    assert_plausible_as_of_date(date(2026, 7, 11), today=date(2026, 7, 10))


def test_assert_plausible_as_of_date_rejects_stale() -> None:
    """A live scrape yielding a date weeks away means the parser latched onto
    the wrong DOM element — must raise, never store (rule-007)."""
    from finances.ingest.bcv import assert_plausible_as_of_date

    with pytest.raises(BcvParseError):
        assert_plausible_as_of_date(date(2026, 5, 1), today=date(2026, 7, 10))


def test_ingest_bcv_live_fetch_rejects_implausible_date(
    seeded_db: sqlite3.Connection, mocker
) -> None:
    """Live-fetch path (html=None): a mis-anchored/stale value date must
    error the run and write nothing, not silently store a wrong-dated rate."""
    stale = _snapshot()  # fixture value date 2026-04-28, far from today
    mocker.patch("finances.ingest.bcv.fetch_bcv_html", return_value=stale)

    with pytest.raises(BcvParseError):
        ingest_bcv(seeded_db)

    count = seeded_db.execute(
        "SELECT count(*) FROM rates WHERE source='bcv'"
    ).fetchone()[0]
    assert count == 0
    run = seeded_db.execute(
        "SELECT status FROM import_runs WHERE source='bcv' ORDER BY id DESC LIMIT 1"
    ).fetchone()
    assert run is not None and run[0] == "error"


def test_parse_bcv_html_raises_when_rate_blocks_missing() -> None:
    """Mangled fixture has no #dolar/#euro blocks → BcvParseError."""
    with pytest.raises(BcvParseError):
        parse_bcv_html(_mangled())


def test_parse_bcv_html_raises_on_missing_date_span() -> None:
    html = "<html><body><div id='dolar'><strong>1,00</strong></div></body></html>"
    with pytest.raises(BcvParseError):
        parse_bcv_html(html)


def test_parse_bcv_html_raises_on_missing_dolar_block() -> None:
    """USD is mandatory — missing #dolar trips the error path."""
    html = (
        "<html><body>"
        "<span class='date-display-single' content='2026-04-28T00:00:00-04:00'>x</span>"
        "<div id='euro'><strong>569,29520082</strong></div>"
        "</body></html>"
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
    """Homepage snapshot is one day → 2 rate rows (USD + EUR)."""
    inserted = ingest_bcv(seeded_db, html=_snapshot())
    assert inserted == 2

    total = seeded_db.execute(
        "SELECT COUNT(*) FROM rates WHERE source='bcv'"
    ).fetchone()[0]
    assert total == 2

    usd_count = seeded_db.execute(
        "SELECT COUNT(*) FROM rates WHERE source='bcv' AND base='USD'"
    ).fetchone()[0]
    assert usd_count == 1

    eur_count = seeded_db.execute(
        "SELECT COUNT(*) FROM rates WHERE source='bcv' AND base='EUR'"
    ).fetchone()[0]
    assert eur_count == 1

    run = seeded_db.execute(
        "SELECT source, status, rows_inserted, error FROM import_runs "
        "WHERE source='bcv' ORDER BY id DESC LIMIT 1"
    ).fetchone()
    assert run is not None
    assert run["source"] == "bcv"
    assert run["status"] == "success"
    assert run["rows_inserted"] == 2
    assert run["error"] is None

    state = seeded_db.execute(
        "SELECT source FROM import_state WHERE source='bcv'"
    ).fetchone()
    assert state is not None


def test_ingest_bcv_idempotent_same_day(seeded_db: sqlite3.Connection) -> None:
    first = ingest_bcv(seeded_db, html=_snapshot())
    second = ingest_bcv(seeded_db, html=_snapshot())
    assert first == 2
    assert second == 0

    total = seeded_db.execute(
        "SELECT COUNT(*) FROM rates WHERE source='bcv'"
    ).fetchone()[0]
    assert total == 2

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


def test_ingest_bcv_dry_run_persists_nothing(seeded_db: sqlite3.Connection) -> None:
    """dry_run=True validates and reports counts without writing to the DB.

    Returned int is the number of rate rows that *would* be inserted; the
    rates, import_runs, and import_state tables remain untouched.
    """
    would_insert = ingest_bcv(seeded_db, html=_snapshot(), dry_run=True)
    assert would_insert == 2

    rates_count = seeded_db.execute(
        "SELECT COUNT(*) FROM rates WHERE source='bcv'"
    ).fetchone()[0]
    assert rates_count == 0, "dry-run must not persist rate rows"

    runs_count = seeded_db.execute(
        "SELECT COUNT(*) FROM import_runs WHERE source='bcv'"
    ).fetchone()[0]
    assert runs_count == 0, "dry-run must not persist import_runs rows"

    state_count = seeded_db.execute(
        "SELECT COUNT(*) FROM import_state WHERE source='bcv'"
    ).fetchone()[0]
    assert state_count == 0, "dry-run must not persist import_state rows"


def test_ingest_bcv_dry_run_after_real_run_does_not_double_count(
    seeded_db: sqlite3.Connection,
) -> None:
    """A real run followed by dry_run shows accurate would-be inserts (zero
    on idempotent re-run) and leaves the existing real-run rows intact."""
    real_inserted = ingest_bcv(seeded_db, html=_snapshot())
    assert real_inserted == 2

    would_insert_again = ingest_bcv(seeded_db, html=_snapshot(), dry_run=True)
    assert would_insert_again == 0, (
        "second pass against same snapshot is fully duplicate; dry-run "
        "should report 0 would-be inserts"
    )

    total = seeded_db.execute(
        "SELECT COUNT(*) FROM rates WHERE source='bcv'"
    ).fetchone()[0]
    assert total == 2, "dry-run must not corrupt real-run data"

    runs = seeded_db.execute(
        "SELECT COUNT(*) FROM import_runs WHERE source='bcv'"
    ).fetchone()[0]
    assert runs == 1, "only the real run should leave provenance"
