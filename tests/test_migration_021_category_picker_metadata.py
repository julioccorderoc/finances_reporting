"""Migration 021 — picker metadata on ``categories`` (tests precede impl, rule-011).

The triage redesign needs three facts the table never carried: whether a
category may be picked by hand (``auto_only``), whether it may occupy one
of the eight numbered chips (``chip_eligible``), and which Lucide glyph
draws it (``icon``).

Owner decisions 2026-08-21, taken against the live DB rather than the
design's stale read:

* **Fees stays pickable** — migration 018 reactivated it deliberately for
  hand-triage and the ADR-019 reversal cleanup. But it is the single
  most-used category (371 rows in 12 months) purely because rules assign
  it, so it is ``chip_eligible = 0``: in the list, never on key ``1``.
* **Clothing retires** — its ten rows fold into ``Purchases`` and the
  category is deactivated, never deleted (criterion E9).

``active`` keeps meaning "not retired". Pickable is
``active AND NOT auto_only``.
"""

from __future__ import annotations

import shutil
import sqlite3
from pathlib import Path

import pytest

from finances.db.migrate import MIGRATIONS_DIR, apply_migrations
from finances.db.repos import categories as categories_repo

AUTO_ONLY_NAMES = {
    "Internal Transfer",
    "External Transfer",
    "FX Diff",
    "Reconciliation",
    "Interest",
}


@pytest.fixture()
def migrated_db(tmp_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(tmp_path / "test.db")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    apply_migrations(conn)
    yield conn
    conn.close()


def _columns(conn: sqlite3.Connection) -> dict[str, sqlite3.Row]:
    return {r["name"]: r for r in conn.execute("PRAGMA table_info(categories)")}


def test_adds_auto_only_chip_eligible_and_icon(migrated_db: sqlite3.Connection) -> None:
    cols = _columns(migrated_db)
    assert cols["auto_only"]["notnull"] == 1
    assert cols["auto_only"]["dflt_value"] == "0"
    assert cols["chip_eligible"]["notnull"] == 1
    assert cols["chip_eligible"]["dflt_value"] == "1"
    assert cols["icon"]["notnull"] == 0


def test_auto_only_marks_the_never_hand_picked_set(migrated_db: sqlite3.Connection) -> None:
    rows = migrated_db.execute(
        "SELECT name, auto_only FROM categories WHERE auto_only = 1"
    ).fetchall()
    assert {r["name"] for r in rows} == AUTO_ONLY_NAMES


def test_every_transfer_and_adjustment_category_is_auto_only(
    migrated_db: sqlite3.Connection,
) -> None:
    """Kind-driven, so a future adjustment category inherits the rule."""
    rows = migrated_db.execute(
        "SELECT name FROM categories WHERE kind IN ('transfer', 'adjustment') AND auto_only = 0"
    ).fetchall()
    assert rows == []


def test_fees_is_pickable_but_never_a_chip(migrated_db: sqlite3.Connection) -> None:
    row = migrated_db.execute(
        "SELECT active, auto_only, chip_eligible FROM categories"
        " WHERE kind = 'expense' AND name = 'Fees'"
    ).fetchone()
    assert row["active"] == 1
    assert row["auto_only"] == 0
    assert row["chip_eligible"] == 0


def test_fees_is_the_only_chip_exclusion(migrated_db: sqlite3.Connection) -> None:
    rows = migrated_db.execute(
        "SELECT name FROM categories WHERE chip_eligible = 0"
    ).fetchall()
    assert [r["name"] for r in rows] == ["Fees"]


def test_clothing_is_deactivated_not_deleted(migrated_db: sqlite3.Connection) -> None:
    clothing = categories_repo.get_by_name(migrated_db, "expense", "Clothing")
    assert clothing is not None, "E9: retired categories are deactivated, never deleted"
    assert clothing.active is False


def test_every_active_category_has_an_icon(migrated_db: sqlite3.Connection) -> None:
    missing = migrated_db.execute(
        "SELECT name FROM categories WHERE active = 1 AND (icon IS NULL OR icon = '')"
    ).fetchall()
    assert [r["name"] for r in missing] == []


def test_icons_match_the_design_kit(migrated_db: sqlite3.Connection) -> None:
    icons = {
        r["name"]: r["icon"]
        for r in migrated_db.execute("SELECT name, icon FROM categories")
    }
    assert icons["Groceries"] == "shopping-basket"
    assert icons["Going Out"] == "utensils"
    assert icons["Transport"] == "car"
    assert icons["Personal Care"] == "scissors"
    assert icons["Loan Repayment"] == "rotate-ccw"
    # Not in the design fixture — chosen here.
    assert icons["Internal Transfer"] == "arrow-left-right"
    assert icons["External Transfer"] == "arrow-up-right"
    assert icons["FX Diff"] == "coins"


def test_clothing_rows_fold_into_purchases(tmp_path: Path) -> None:
    """Applied to a DB that already holds Clothing rows, 021 moves them."""
    staged = tmp_path / "migrations"
    staged.mkdir()
    for path in sorted(MIGRATIONS_DIR.glob("*.sql")):
        if path.name.startswith("021_"):
            continue
        shutil.copy(path, staged / path.name)

    conn = sqlite3.connect(tmp_path / "staged.db")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    apply_migrations(conn, staged)

    clothing_id = conn.execute(
        "SELECT id FROM categories WHERE kind = 'expense' AND name = 'Clothing'"
    ).fetchone()["id"]
    account_id = conn.execute(
        "INSERT INTO accounts (name, kind, currency) VALUES ('T', 'bank', 'VES')"
    ).lastrowid
    conn.execute(
        "INSERT INTO transactions"
        " (account_id, occurred_at, kind, amount, currency, description,"
        "  category_id, source, source_ref)"
        " VALUES (?, '2026-05-01T00:00:00+00:00', 'expense', -40, 'VES', 'ZARA',"
        "         ?, 'test', 'clothing-1')",
        (account_id, clothing_id),
    )
    conn.commit()

    for path in sorted(MIGRATIONS_DIR.glob("021_*.sql")):
        shutil.copy(path, staged / path.name)
    applied = apply_migrations(conn, staged)
    assert any(name.startswith("021_") for name in applied)

    row = conn.execute(
        "SELECT c.name AS category FROM transactions t"
        " JOIN categories c ON c.id = t.category_id"
        " WHERE t.source_ref = 'clothing-1'"
    ).fetchone()
    assert row["category"] == "Purchases"
    orphans = conn.execute(
        "SELECT COUNT(*) AS n FROM transactions WHERE category_id = ?", (clothing_id,)
    ).fetchone()["n"]
    assert orphans == 0
    conn.close()
