"""Tests for ``finances backup`` and the stray-root-backup doctor warning.

Backups must always land in ``backups/`` (never the repo root), be taken
through the sqlite3 backup API (WAL-safe, single file — no ``-shm``/``-wal``
siblings), and ``finances doctor`` must call out any backup-looking files
that still sit next to the live ``finances.db``.
"""

from __future__ import annotations

import sqlite3
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import pytest
from typer.testing import CliRunner

from finances import config
from finances.db.connection import get_connection
from finances.db.migrate import apply_migrations
from finances.db.repos import accounts as accounts_repo
from finances.db.repos import transactions as transactions_repo
from finances.domain.models import Account, AccountKind, Transaction, TransactionKind


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
                amount=Decimal("-9.99"),
                currency="USD",
                description="backup seed",
                source="cash_cli",
                source_ref="backup-seed-1",
            ),
        )
    finally:
        conn.close()


@pytest.fixture
def backup_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Path:
    """Seeded DB in a fake project root with its own backups/ dir."""
    root = tmp_path / "root"
    root.mkdir()
    db_file = root / "finances.db"
    _seed_db(db_file)
    monkeypatch.setattr(config, "DB_PATH", db_file)
    monkeypatch.setattr(config, "PROJECT_ROOT", root)
    monkeypatch.setattr(config, "BACKUPS_DIR", root / "backups")
    return root


def _run(args: list[str]):
    from finances.cli.main import app

    return CliRunner().invoke(app, args)


def test_backup_writes_single_file_into_backups_dir(backup_env: Path) -> None:
    result = _run(["backup"])

    assert result.exit_code == 0, result.output
    files = list((backup_env / "backups").iterdir())
    assert len(files) == 1
    made = files[0]
    assert made.name.startswith("finances-") and made.suffix == ".db"
    # WAL-safe API backup: no -shm/-wal siblings, and data is intact.
    conn = sqlite3.connect(f"file:{made}?immutable=1", uri=True)
    try:
        n = conn.execute("SELECT COUNT(*) FROM transactions").fetchone()[0]
    finally:
        conn.close()
    assert n == 1
    # Nothing new lands in the root.
    assert [p.name for p in backup_env.iterdir() if p.is_file()] == ["finances.db"]


def test_backup_label_lands_in_filename(backup_env: Path) -> None:
    result = _run(["backup", "--label", "adr019"])

    assert result.exit_code == 0, result.output
    (made,) = (backup_env / "backups").iterdir()
    assert "adr019" in made.name


def test_backup_rejects_label_with_path_separators(backup_env: Path) -> None:
    result = _run(["backup", "--label", "../evil"])

    assert result.exit_code != 0
    assert not (backup_env / "backups").exists() or not list(
        (backup_env / "backups").iterdir()
    )


def test_doctor_warns_on_stray_root_backups(backup_env: Path) -> None:
    (backup_env / "finances.db.bak-adr017").touch()
    (backup_env / "finances.db-bak-20260726-204310").touch()
    (backup_env / "finances-backup-2026-07-09.db").touch()

    result = _run(["doctor"])

    assert result.exit_code == 0, result.output
    assert "backups/" in result.output
    for name in (
        "finances.db.bak-adr017",
        "finances.db-bak-20260726-204310",
        "finances-backup-2026-07-09.db",
    ):
        assert name in result.output


def test_doctor_quiet_when_root_is_clean(backup_env: Path) -> None:
    result = _run(["doctor"])

    assert result.exit_code == 0, result.output
    assert "stray" not in result.output.lower()
