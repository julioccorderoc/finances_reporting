"""Migration 027 — categories roll up into groups (ADR-026).

The /monthly chart draws the top five categories and folds the rest into
"Other". On the owner's ledger Other was the largest block in four of six
months — 45.8% of July 2026 — because the fixed household costs are split
across four categories none of which reaches the top five on its own.

The fix is a second, coarser axis over categories: ``categories.group_name``.
NULL means "this category stands for itself". The group is a label on the
category, not a table of its own, and a transaction still carries exactly
one ``category_id`` — nothing about ingest, categorization or triage learns
that groups exist.

The mapping is the owner's (ADR-026 §2.2), seeded once here and owned by
the rows afterwards (§2.7): ``Home`` and ``Social`` are data, and the
owner may rename them with an UPDATE.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from finances.db.migrate import apply_migrations
from finances.domain.integrity import GROUPED_CATEGORIES

HOME = ("Rent", "Utilities", "Transport", "Personal Care")
SOCIAL = ("Dating", "Going Out", "Leisure", "Family", "Gifts")
STANDALONE = ("Purchases", "Groceries", "Health")
# Lending left this list on 2026-09-08: migration 028 stores it as Other.
LONG_TAIL = ("Fees", "Other Expense", "Education", "Subscriptions")


@pytest.fixture()
def migrated_db(tmp_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(tmp_path / "test.db")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    apply_migrations(conn)
    yield conn
    conn.close()


def _group_of(conn: sqlite3.Connection, name: str, *, kind: str = "expense") -> str | None:
    row = conn.execute(
        "SELECT group_name FROM categories WHERE kind = ? AND name = ?",
        (kind, name),
    ).fetchone()
    assert row is not None, f"no {kind} category named {name!r}"
    return row["group_name"]


def _grouped(conn: sqlite3.Connection) -> dict[tuple[str, str], str]:
    rows = conn.execute(
        "SELECT kind, name, group_name FROM categories WHERE group_name IS NOT NULL"
    ).fetchall()
    return {(r["kind"], r["name"]): r["group_name"] for r in rows}


@pytest.mark.parametrize("name", HOME)
def test_home_groups_the_fixed_household_costs(
    migrated_db: sqlite3.Connection, name: str
) -> None:
    assert _group_of(migrated_db, name) == "Home"


@pytest.mark.parametrize("name", SOCIAL)
def test_social_groups_the_sociable_categories(
    migrated_db: sqlite3.Connection, name: str
) -> None:
    assert _group_of(migrated_db, name) == "Social"


@pytest.mark.parametrize("name", STANDALONE)
def test_purchases_groceries_and_health_stand_alone(
    migrated_db: sqlite3.Connection, name: str
) -> None:
    """Deliberate, not an oversight: each is its own series on the chart."""
    assert _group_of(migrated_db, name) is None


@pytest.mark.parametrize("name", LONG_TAIL)
def test_the_long_tail_stays_ungrouped(
    migrated_db: sqlite3.Connection, name: str
) -> None:
    """The tail falls into the computed Other on its own; nothing is
    written down for it (ADR-026 §2.6)."""
    assert _group_of(migrated_db, name) is None


def test_other_is_not_a_group_that_competes(migrated_db: sqlite3.Connection) -> None:
    """A stored ``Other`` (migration 028) is an instruction to fold, not a
    fifth group: the seed here stores none, and 028 adds exactly one."""
    assert "Other" not in {
        _group_of(migrated_db, name) for name in HOME + SOCIAL + STANDALONE + LONG_TAIL
    }


def test_only_expense_categories_carry_a_group(
    migrated_db: sqlite3.Connection,
) -> None:
    kinds = {kind for kind, _ in _grouped(migrated_db)}
    assert kinds == {"expense"}


def test_the_seed_and_the_doctor_agree_on_what_is_grouped(
    migrated_db: sqlite3.Connection,
) -> None:
    """The doctor's guard against a rename (ADR-026 §3) is only as good as
    its list of what the seed grouped — so that list is pinned to the seed
    here, and every member of it must be a category that exists."""
    assert set(_grouped(migrated_db)) == set(GROUPED_CATEGORIES)


def test_the_migration_is_recorded_once(migrated_db: sqlite3.Connection) -> None:
    row = migrated_db.execute(
        "SELECT COUNT(*) AS c FROM _migrations WHERE filename LIKE '027_%'"
    ).fetchone()
    assert row["c"] == 1


def test_re_running_changes_no_rows(migrated_db: sqlite3.Connection) -> None:
    before = migrated_db.execute(
        "SELECT id, kind, name, active, group_name FROM categories ORDER BY id"
    ).fetchall()

    assert apply_migrations(migrated_db) == []

    after = migrated_db.execute(
        "SELECT id, kind, name, active, group_name FROM categories ORDER BY id"
    ).fetchall()
    assert [tuple(r) for r in after] == [tuple(r) for r in before]


def test_the_seed_statements_are_idempotent_on_their_own(
    tmp_path: Path,
) -> None:
    """The runner applies a file once, but the UPDATEs must also be safe to
    replay by hand: the owner edits this column with plain SQL (§2.7), and
    the seed is the statement he will copy from. Replayed on an already
    seeded table it changes nothing."""
    from finances.db.migrate import MIGRATIONS_DIR

    conn = sqlite3.connect(tmp_path / "replay.db")
    conn.row_factory = sqlite3.Row
    apply_migrations(conn)

    sql = (MIGRATIONS_DIR / "027_category_groups.sql").read_text(encoding="utf-8")
    seed_only = "\n".join(
        line for line in sql.splitlines() if not line.lstrip().upper().startswith("ALTER TABLE")
    )
    before = _grouped(conn)
    conn.executescript(seed_only)
    assert _grouped(conn) == before
    conn.close()
