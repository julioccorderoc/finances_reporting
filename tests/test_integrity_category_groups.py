"""``finances doctor`` guards the category groups against a rename.

ADR-023 stores a category's group as a plain string on the category row.
That is the cheap choice, and its cost is named in the ADR (§3): a
category can be renamed while ``group_name`` still points at the old
label, and nothing structural will notice. The guard is a doctor check
that every non-NULL ``group_name`` sits on a category the seed (migration
026) knows.

A warning, not an error: the ledger is not wrong when this fires — the
taxonomy has drifted from what the seed described, and the owner should
look. An ERROR here would fail ``doctor --strict`` forever after a
deliberate edit, which is the permanent-false-finding trap three other
checks had to be pulled out of.
"""

from __future__ import annotations

import sqlite3

from finances.domain.integrity import CHECKS, GROUPED_CATEGORIES, Severity, run_checks

CHECK = "category_group_unknown"


def _finding(conn: sqlite3.Connection):
    return next((f for f in run_checks(conn).findings if f.check == CHECK), None)


def _category_id(conn: sqlite3.Connection, name: str) -> int:
    row = conn.execute(
        "SELECT id FROM categories WHERE kind = 'expense' AND name = ?", (name,)
    ).fetchone()
    assert row is not None, name
    return int(row[0])


def test_the_check_exists_as_a_warning() -> None:
    check = next((c for c in CHECKS if c.name == CHECK), None)
    assert check is not None, f"no {CHECK} check"
    assert check.severity is Severity.WARNING


def test_the_seeded_mapping_is_clean(seeded_db: sqlite3.Connection) -> None:
    """Fresh from migration 026, every grouped category is one the seed knows."""
    assert _finding(seeded_db) is None


def test_a_group_on_a_category_the_seed_never_grouped_is_reported(
    seeded_db: sqlite3.Connection,
) -> None:
    groceries = _category_id(seeded_db, "Groceries")
    seeded_db.execute(
        "UPDATE categories SET group_name = 'Home' WHERE id = ?", (groceries,)
    )

    finding = _finding(seeded_db)
    assert finding is not None
    assert finding.severity is Severity.WARNING
    assert finding.count == 1
    assert finding.sample_ids == [groceries]


def test_a_renamed_category_is_reported(seeded_db: sqlite3.Connection) -> None:
    """The rename ADR-023 §3 worries about: Rent becomes Housing, and the
    row still says Home. The seed never knew a Housing."""
    rent = _category_id(seeded_db, "Rent")
    seeded_db.execute("UPDATE categories SET name = 'Housing' WHERE id = ?", (rent,))

    finding = _finding(seeded_db)
    assert finding is not None
    assert finding.sample_ids == [rent]


def test_renaming_the_group_itself_is_not_reported(
    seeded_db: sqlite3.Connection,
) -> None:
    """The group labels are data (§2.7). The owner renaming Home to House
    with an UPDATE is the edit the design exists to allow."""
    seeded_db.execute("UPDATE categories SET group_name = 'House' WHERE group_name = 'Home'")

    assert _finding(seeded_db) is None


def test_a_group_on_a_non_expense_category_is_reported(
    seeded_db: sqlite3.Connection,
) -> None:
    """The seed grouped expense categories only; a Salary in Home is drift."""
    row = seeded_db.execute(
        "SELECT id FROM categories WHERE kind = 'income' AND name = 'Salary'"
    ).fetchone()
    assert row is not None
    seeded_db.execute("UPDATE categories SET group_name = 'Home' WHERE id = ?", (row[0],))

    finding = _finding(seeded_db)
    assert finding is not None
    assert finding.sample_ids == [int(row[0])]


def test_ungrouping_a_seeded_category_is_not_reported(
    seeded_db: sqlite3.Connection,
) -> None:
    """NULL is never drift: it is the majority state (§2.1)."""
    seeded_db.execute("UPDATE categories SET group_name = NULL WHERE name = 'Gifts'")

    assert _finding(seeded_db) is None


def test_the_check_reads_the_same_list_the_migration_test_pins() -> None:
    """One list, two guards: the migration test proves the seed matches it,
    this check proves the ledger still does."""
    check = next(c for c in CHECKS if c.name == CHECK)
    assert check.sql is not None
    for _, name in GROUPED_CATEGORIES:
        assert name in check.sql
