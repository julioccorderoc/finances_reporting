"""CLI + auto-regen tests for the static report (Thing 4).

Covers:

* ``finances html [--output PATH]`` renders a standalone file (exit 0, file
  exists, no external URLs),
* the shared ``_regenerate_default_report`` helper writes the default
  ``report.html`` and *swallows* failures (warn-only, never raises),
* a successful non-dry-run ingest command triggers a regen; a ``--dry-run``
  invocation does not.
"""

from __future__ import annotations

import re
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from unittest.mock import Mock

import pytest
from typer.testing import CliRunner

from finances import config
from finances.db.connection import get_connection
from finances.db.migrate import apply_migrations
from finances.db.repos import accounts as accounts_repo
from finances.db.repos import transactions as transactions_repo
from finances.domain.models import Account, AccountKind, Transaction, TransactionKind

_URL_RE = re.compile(r"https?://")


def _seed_db(path: Path) -> None:
    conn = get_connection(path)
    apply_migrations(conn)
    try:
        acct = accounts_repo.insert(
            conn, Account(name="Cash USD", kind=AccountKind.CASH, currency="USD")
        )
        transactions_repo.insert(
            conn,
            Transaction(
                account_id=acct.id,
                occurred_at=datetime.now(tz=UTC),
                kind=TransactionKind.EXPENSE,
                amount=Decimal("9.99"),
                currency="USD",
                description="cli seed",
                source="cash_cli",
                source_ref="cli-seed-1",
            ),
        )
    finally:
        conn.close()


@pytest.fixture
def seeded_cli_db(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Path:
    db_file = tmp_path / "cli-finances.db"
    _seed_db(db_file)
    monkeypatch.setattr(config, "DB_PATH", db_file)
    return db_file


def test_html_command_writes_standalone_file(
    seeded_cli_db: Path, tmp_path: Path
) -> None:
    from finances.cli.main import app

    out = tmp_path / "out.html"
    runner = CliRunner()
    result = runner.invoke(app, ["html", "--output", str(out)])

    assert result.exit_code == 0, result.output
    assert out.exists() and out.stat().st_size > 0
    content = out.read_text(encoding="utf-8")
    assert "<!doctype html" in content.lower()
    assert _URL_RE.search(content) is None


def test_html_command_defaults_to_config_report_path(
    seeded_cli_db: Path, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    from finances.cli.main import app

    default_out = tmp_path / "default-report.html"
    monkeypatch.setattr(config, "REPORT_HTML_PATH", default_out)

    runner = CliRunner()
    result = runner.invoke(app, ["html"])

    assert result.exit_code == 0, result.output
    assert default_out.exists()


def test_regenerate_default_report_writes_file(
    seeded_cli_db: Path, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    from finances.cli import main as cli_main

    out = tmp_path / "regen-report.html"
    monkeypatch.setattr(config, "REPORT_HTML_PATH", out)

    cli_main._regenerate_default_report()

    assert out.exists() and out.stat().st_size > 0


def test_regenerate_default_report_swallows_failure(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    from finances.cli import main as cli_main

    # A schemaless DB path → export raises "no such table"; helper must warn,
    # not raise, and must not leave a partial file behind.
    empty_db = tmp_path / "empty.db"
    out = tmp_path / "should-not-exist.html"
    monkeypatch.setattr(config, "DB_PATH", empty_db)
    monkeypatch.setattr(config, "REPORT_HTML_PATH", out)

    cli_main._regenerate_default_report()  # must not raise

    assert not out.exists()


def test_ingest_success_triggers_regen_but_dry_run_does_not(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    from finances.cli import main as cli_main

    db_file = tmp_path / "ingest.db"
    conn = get_connection(db_file)
    apply_migrations(conn)
    conn.close()
    # The bcv command binds DB_PATH from main's module namespace.
    monkeypatch.setattr(cli_main, "DB_PATH", db_file)

    # Patch the ingest at its module boundary (command lazily imports it).
    import finances.ingest.bcv as bcv_ingest

    monkeypatch.setattr(bcv_ingest, "ingest_bcv", lambda conn, dry_run=False: 3)

    spy = Mock()
    monkeypatch.setattr(cli_main, "_regenerate_default_report", spy)

    runner = CliRunner()

    ok = runner.invoke(cli_main.app, ["ingest", "bcv"])
    assert ok.exit_code == 0, ok.output
    assert spy.call_count == 1

    spy.reset_mock()
    dry = runner.invoke(cli_main.app, ["ingest", "bcv", "--dry-run"])
    assert dry.exit_code == 0, dry.output
    assert spy.call_count == 0
