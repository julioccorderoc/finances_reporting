"""Migration 012 — `Leisure` is redefined as non-food recreation only.

Owner decision 2026-07-21: `Leisure` was the legacy backfill bucket for
going-out food (see migration 004's comment). Migration 007 later added
`Going Out` for exactly that, leaving two categories with one meaning.

The new lines are:

    Leisure    non-food recreation / experiences (tours, events, cinema)
    Going Out  food + drink consumed out
    Groceries  food consumed at home

Only the *mechanically unambiguous* legacy rows move: named food
merchants -> `Going Out`, supermarkets/butchers -> `Groceries`. Opaque
rows (pago movil `CAR.DRV*`, bank transfers `DR OB *`, bare person names,
`EVENTOS RZEMIEN CA`, `DOGGO53 C A`) stay in `Leisure` for the owner to
triage by hand — the description carries no intent signal.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

MIGRATION = (
    Path(__file__).resolve().parents[1]
    / "finances"
    / "db"
    / "migrations"
    / "012_leisure_food_split.sql"
)

# Named food merchants — moved to `Going Out`.
FOOD_MERCHANTS = (
    "LUNCHERIA MILY GOURMET",
    "EL GRAN HORNO CA",
    "GUILLEN PASTELES Y EMPAN",
    "PICA FOOD CA",
    "PICA FOOD C A",
    "PAN PAST PIZZE Y CHAR",
    "CUATRO TENEDORES GASTR",
    "CAFFE NOVENTA",
    "NEGRONI GROUP 25",
    "LA PANADERIA 2025 CA",
    "AL CARBON C A",
    "LOS CHINOS LAW C A",
    "PIZZERIA PERTUTTI CA",
    "HELADERIA EFE",
    "PIZZA DE VERDAD",
    "VAMOS PA QUE MENCHO",
)

# Supermarkets / butchers — home food, so `Groceries`.
GROCERY_MERCHANTS = (
    "HIPERMERCADO KARI C,A",
    "HIPERMERCADO KARI C.A",
    "MI SUPER, C.A",
    "FIGUEROA MINI ABASTO Y V",
    "CARNICER Y CHARCUTER T",
)

# Descriptions with no intent signal — must stay put.
OPAQUE = (
    "CAR.DRV0027142544",
    "DR OB V16601078 102BANCO",
    "EVENTOS RZEMIEN CA",
    "DOGGO53 C A",
    "ANA CECILIA TORREALBA",
    "TRAV0014270401000011818",
    "ABO.DRV0027142544",
)


def _category_id(conn: sqlite3.Connection, kind: str, name: str) -> int:
    row = conn.execute(
        "SELECT id FROM categories WHERE kind = ? AND name = ?", (kind, name)
    ).fetchone()
    assert row is not None, f"category {kind}/{name} missing"
    return int(row["id"])


def _insert(
    conn: sqlite3.Connection, description: str, category: str, ref: str
) -> None:
    conn.execute(
        """
        INSERT INTO transactions
            (account_id, occurred_at, kind, amount, currency, description,
             category_id, source, source_ref)
        VALUES (1, '2026-01-15T12:00:00-04:00', 'expense', -100.0, 'VES', ?,
                ?, 'provincial', ?)
        """,
        (description, _category_id(conn, "expense", category), ref),
    )


def _category_of(conn: sqlite3.Connection, ref: str) -> str:
    row = conn.execute(
        """
        SELECT c.name AS name
        FROM transactions t JOIN categories c ON c.id = t.category_id
        WHERE t.source_ref = ?
        """,
        (ref,),
    ).fetchone()
    assert row is not None, f"transaction {ref} missing"
    return str(row["name"])


def _run_migration(conn: sqlite3.Connection) -> None:
    conn.executescript(MIGRATION.read_text())


@pytest.fixture()
def leisure_db(seeded_db: sqlite3.Connection) -> sqlite3.Connection:
    """`seeded_db` + one Leisure row per description under test."""
    for i, description in enumerate(FOOD_MERCHANTS + GROCERY_MERCHANTS + OPAQUE):
        _insert(seeded_db, description, "Leisure", f"ref-{i}")
    return seeded_db


def test_food_merchants_move_to_going_out(leisure_db: sqlite3.Connection) -> None:
    _run_migration(leisure_db)
    for i, description in enumerate(FOOD_MERCHANTS):
        assert _category_of(leisure_db, f"ref-{i}") == "Going Out", description


def test_supermarkets_move_to_groceries(leisure_db: sqlite3.Connection) -> None:
    _run_migration(leisure_db)
    offset = len(FOOD_MERCHANTS)
    for i, description in enumerate(GROCERY_MERCHANTS):
        assert _category_of(leisure_db, f"ref-{offset + i}") == "Groceries", description


def test_opaque_rows_stay_in_leisure(leisure_db: sqlite3.Connection) -> None:
    _run_migration(leisure_db)
    offset = len(FOOD_MERCHANTS) + len(GROCERY_MERCHANTS)
    for i, description in enumerate(OPAQUE):
        assert _category_of(leisure_db, f"ref-{offset + i}") == "Leisure", description


def test_same_merchant_outside_leisure_is_untouched(
    seeded_db: sqlite3.Connection,
) -> None:
    # The owner already hand-tagged some of these merchants elsewhere
    # (a pizzeria visit can be a date). Only Leisure rows may move.
    _insert(seeded_db, "PIZZA DE VERDAD", "Dating", "dating-1")
    _insert(seeded_db, "HIPERMERCADO KARI C.A", "Family", "family-1")
    _run_migration(seeded_db)
    assert _category_of(seeded_db, "dating-1") == "Dating"
    assert _category_of(seeded_db, "family-1") == "Family"


def test_migration_is_idempotent(leisure_db: sqlite3.Connection) -> None:
    _run_migration(leisure_db)
    _run_migration(leisure_db)
    assert _category_of(leisure_db, "ref-0") == "Going Out"
    assert _category_of(leisure_db, f"ref-{len(FOOD_MERCHANTS)}") == "Groceries"
    offset = len(FOOD_MERCHANTS) + len(GROCERY_MERCHANTS)
    assert _category_of(leisure_db, f"ref-{offset}") == "Leisure"


def test_needs_review_is_not_raised_by_the_move(leisure_db: sqlite3.Connection) -> None:
    # These rows are being *corrected*, not un-categorized: they must not
    # come back through the triage queue.
    _run_migration(leisure_db)
    row = leisure_db.execute(
        "SELECT COUNT(*) AS n FROM transactions WHERE needs_review = 1"
    ).fetchone()
    assert row["n"] == 0
