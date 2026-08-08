"""Migration 020 — Bancamiga and Banco de Venezuela join the ledger.

Owner holds three bolivar bank accounts, not one. Provincial has been the
only one on the books since 001; the other two were carried in his head at
zero. Reconciliation needs every custodian position to have somewhere to
land, so both get seeded empty, alongside Provincial and shaped like it.
"""

from __future__ import annotations

import sqlite3

import pytest

from finances.db.migrate import apply_migrations
from finances.db.repos import accounts as accounts_repo
from finances.domain.models import AccountKind


@pytest.fixture()
def migrated_db(tmp_path) -> sqlite3.Connection:
    conn = sqlite3.connect(tmp_path / "test.db")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    apply_migrations(conn)
    yield conn
    conn.close()


@pytest.mark.parametrize(
    ("name", "institution"),
    [
        ("Bancamiga Bolivares", "Bancamiga"),
        ("Venezuela Bolivares", "Banco de Venezuela"),
    ],
)
def test_new_bank_account_seeded(
    migrated_db: sqlite3.Connection, name: str, institution: str
) -> None:
    account = accounts_repo.get_by_name(migrated_db, name)
    assert account is not None
    assert account.kind is AccountKind.BANK
    assert account.currency == "VES"
    assert account.institution == institution
    assert account.active is True


def test_new_accounts_start_empty(migrated_db: sqlite3.Connection) -> None:
    for name in ("Bancamiga Bolivares", "Venezuela Bolivares"):
        account = accounts_repo.get_by_name(migrated_db, name)
        assert account is not None
        (count,) = migrated_db.execute(
            "SELECT COUNT(*) FROM transactions WHERE account_id = ?", (account.id,)
        ).fetchone()
        assert count == 0


def test_existing_accounts_untouched(migrated_db: sqlite3.Connection) -> None:
    """The seed adds rows; it never rewrites one that is already there.

    Accounts predate this migration — they were created by the backfill, not
    by 001 — so a live DB reaches 020 with Provincial already present. The
    seed must leave such a row exactly as it found it.
    """
    migrated_db.execute(
        "UPDATE accounts SET institution = 'Somewhere else', active = 0 "
        "WHERE name = 'Bancamiga Bolivares'"
    )
    migrated_db.execute("DELETE FROM _migrations WHERE filename LIKE '020!_%' ESCAPE '!'")
    apply_migrations(migrated_db)

    account = accounts_repo.get_by_name(migrated_db, "Bancamiga Bolivares")
    assert account is not None
    assert account.institution == "Somewhere else"
    assert account.active is False


def test_seed_is_idempotent(migrated_db: sqlite3.Connection) -> None:
    apply_migrations(migrated_db)
    rows = migrated_db.execute(
        "SELECT COUNT(*) FROM accounts WHERE name IN ('Bancamiga Bolivares', 'Venezuela Bolivares')"
    ).fetchone()
    assert rows[0] == 2
