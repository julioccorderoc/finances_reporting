"""Disambiguating tests come from the doc, not from the UI (criterion K7).

``docs/architecture/category-definitions.md`` is authoritative for what
each category *means*; the triage picker shows that sentence at the
moment of choosing (E4/E5). Retyping the sentences into a template would
create a second source of truth that silently drifts, so they are parsed
out of the doc and cached.

The coverage test here is the loud failure: if a pickable category has no
row in the doc, the suite names it and the fix is to write the sentence,
not to hardcode one.
"""

from __future__ import annotations

import sqlite3

import pytest

from finances.db.migrate import apply_migrations
from finances.db.repos import categories as categories_repo
from finances.domain.category_definitions import (
    DEFINITIONS_PATH,
    category_tests,
    missing_tests,
    definition_for,
)


@pytest.fixture()
def migrated_db(tmp_path) -> sqlite3.Connection:
    conn = sqlite3.connect(tmp_path / "test.db")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    apply_migrations(conn)
    yield conn
    conn.close()


def test_the_source_of_truth_exists() -> None:
    assert DEFINITIONS_PATH.is_file(), f"{DEFINITIONS_PATH} is the source of truth for K7"


def test_parses_expense_and_income_tables() -> None:
    tests = category_tests()
    assert tests["Groceries"].startswith("Food consumed at home")
    assert "romantic" in tests["Dating"]
    assert tests["Salary"] == "Employment pay."


def test_parses_the_transfer_table_too() -> None:
    """Migration 022 made the two transfer categories pickable, so their
    sentences are on screen and must come from the doc like the rest."""
    tests = category_tests()
    assert "External Transfer" in tests
    assert "Internal Transfer" in tests
    # The adjustment pair is system-written and is not a picker sentence.
    assert "FX Diff" not in tests
    assert "Reconciliation" not in tests


def test_borrowed_has_a_sentence_that_separates_it_from_the_neighbours() -> None:
    """Migration 025's category is only useful if the picker can tell the
    owner which of the three movement categories he is looking at, and
    which way the money is owed."""
    borrowed = category_tests()["Borrowed"]

    assert "lent" in borrowed.lower()
    # It must name the mirror image it is NOT: money he lends out.
    assert "Lending" in borrowed


def test_strips_markdown_emphasis_from_the_sentence() -> None:
    leisure = category_tests()["Leisure"]
    assert "**" not in leisure
    assert leisure.startswith("Non-food recreation")


def test_is_cached_so_the_doc_is_read_once() -> None:
    assert category_tests() is category_tests()


def test_result_is_read_only() -> None:
    with pytest.raises(TypeError):
        category_tests()["Groceries"] = "nope"  # type: ignore[index]


def test_definition_for_returns_none_for_an_unknown_category() -> None:
    assert definition_for("Nonexistent Category") is None


def test_every_pickable_category_has_a_test(migrated_db: sqlite3.Connection) -> None:
    names = [c.name for c in categories_repo.list_pickable(migrated_db)]
    missing = missing_tests(names)
    assert missing == [], (
        "these pickable categories have no disambiguating test in "
        f"{DEFINITIONS_PATH.name}: {', '.join(missing)}"
    )


def test_retired_categories_are_not_required_to_have_one() -> None:
    """Clothing moved to the retired table in 021; that must not fail coverage."""
    assert missing_tests(["Clothing"]) == ["Clothing"]
    assert missing_tests([]) == []
