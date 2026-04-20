"""Tests for `finances.domain.categorization` (EPIC-004).

Covers the regex-rules engine, priority ordering, source/account scoping, and
the seeded v1 taxonomy per ADR-006 and rule-006. Test commits precede
implementation commits (rule-011).
"""

from __future__ import annotations

import sqlite3
from typing import cast

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from finances.db.repos import accounts as accounts_repo
from finances.db.repos import categories as categories_repo
from finances.domain.categorization import (
    CategorizationRequest,
    CategoryRule,
    RuleMatch,
    load_rules,
    suggest,
)
from finances.domain.models import Account, AccountKind, Category, TransactionKind


# ---------------------------------------------------------------------------
# Helpers.
# ---------------------------------------------------------------------------


def _insert_category(conn: sqlite3.Connection, kind: TransactionKind, name: str) -> int:
    """Return category id, inserting it if missing.

    The migration-seeded v1 taxonomy already contains most names we want, so
    this helper is idempotent against the seed.
    """
    existing = categories_repo.get_by_name(conn, kind, name)
    if existing is not None and existing.id is not None:
        return existing.id
    cat = categories_repo.insert(conn, Category(kind=kind, name=name))
    assert cat.id is not None
    return cat.id


def _insert_account(
    conn: sqlite3.Connection,
    *,
    name: str = "Test Account",
    currency: str = "USD",
    kind: AccountKind = AccountKind.BANK,
) -> int:
    acc = accounts_repo.insert(
        conn, Account(name=name, kind=kind, currency=currency)
    )
    assert acc.id is not None
    return acc.id


def _insert_rule(
    conn: sqlite3.Connection,
    *,
    pattern: str,
    category_id: int,
    source: str | None = None,
    account_id: int | None = None,
    priority: int = 100,
    active: bool = True,
) -> int:
    cur = conn.execute(
        """
        INSERT INTO category_rules (pattern, category_id, source, account_id, priority, active)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (pattern, category_id, source, account_id, priority, 1 if active else 0),
    )
    return cast(int, cur.lastrowid)


# ---------------------------------------------------------------------------
# Seed migration verification (EPIC-004 DoD: v1 taxonomy present).
# ---------------------------------------------------------------------------


V1_TAXONOMY: tuple[tuple[TransactionKind, str], ...] = (
    (TransactionKind.INCOME, "Salary"),
    (TransactionKind.INCOME, "Gigs"),
    (TransactionKind.INCOME, "Interest"),
    (TransactionKind.INCOME, "Other Income"),
    (TransactionKind.EXPENSE, "Food"),
    (TransactionKind.EXPENSE, "Transport"),
    (TransactionKind.EXPENSE, "Health"),
    (TransactionKind.EXPENSE, "Family"),
    (TransactionKind.EXPENSE, "Lifestyle"),
    (TransactionKind.EXPENSE, "Subscriptions"),
    (TransactionKind.EXPENSE, "Purchases"),
    (TransactionKind.EXPENSE, "Fees"),
    (TransactionKind.EXPENSE, "Tools"),
    (TransactionKind.EXPENSE, "Other Expense"),
    (TransactionKind.TRANSFER, "Internal Transfer"),
    (TransactionKind.TRANSFER, "External Transfer"),
    (TransactionKind.ADJUSTMENT, "Reconciliation"),
    (TransactionKind.ADJUSTMENT, "FX Diff"),
)


def test_v1_taxonomy_is_seeded(in_memory_db: sqlite3.Connection) -> None:
    """ADR-006 v1 taxonomy must be present after migrations run."""
    for kind, name in V1_TAXONOMY:
        row = in_memory_db.execute(
            "SELECT id FROM categories WHERE kind = ? AND name = ?",
            (kind.value, name),
        ).fetchone()
        assert row is not None, f"missing v1 category: {kind.value}/{name}"


def test_ant_category_is_not_seeded(in_memory_db: sqlite3.Connection) -> None:
    """Per EPIC-004 technical boundary: drop 'Ant'."""
    row = in_memory_db.execute(
        "SELECT id FROM categories WHERE name = 'Ant'"
    ).fetchone()
    assert row is None


def test_no_id_is_not_seeded_as_destination(in_memory_db: sqlite3.Connection) -> None:
    """Per EPIC-004 technical boundary: drop 'No ID' — use needs_review=1."""
    row = in_memory_db.execute(
        "SELECT id FROM categories WHERE name = 'No ID'"
    ).fetchone()
    assert row is None


def test_seed_includes_at_least_one_rule_per_common_pattern(
    in_memory_db: sqlite3.Connection,
) -> None:
    """DoD: category_rules seeded with at least one rule per common description
    pattern observed in Provincial + Binance CSVs."""
    (count,) = in_memory_db.execute(
        "SELECT COUNT(*) FROM category_rules WHERE active = 1"
    ).fetchone()
    assert count >= 10, f"expected ≥10 seeded rules, got {count}"


# ---------------------------------------------------------------------------
# `suggest()` — happy paths.
# ---------------------------------------------------------------------------


def test_suggest_matches_active_rule(in_memory_db: sqlite3.Connection) -> None:
    food_id = _insert_category(in_memory_db, TransactionKind.EXPENSE, "Food")
    _insert_rule(in_memory_db, pattern=r"PANADERIA", category_id=food_id)

    match = suggest(
        in_memory_db,
        CategorizationRequest(description="PANADERIA LUISANA", source="provincial"),
    )

    assert match is not None
    assert match.category_id == food_id


def test_suggest_is_case_insensitive(in_memory_db: sqlite3.Connection) -> None:
    food_id = _insert_category(in_memory_db, TransactionKind.EXPENSE, "Food")
    _insert_rule(in_memory_db, pattern=r"panaderia", category_id=food_id)

    match = suggest(
        in_memory_db,
        CategorizationRequest(description="PANADERIA LUISANA", source="provincial"),
    )

    assert match is not None
    assert match.category_id == food_id


def test_suggest_source_scoped_rule_matches_same_source(
    in_memory_db: sqlite3.Connection,
) -> None:
    fees_id = _insert_category(in_memory_db, TransactionKind.EXPENSE, "Fees")
    _insert_rule(
        in_memory_db,
        pattern=r"COM\. PAGO MOVIL",
        category_id=fees_id,
        source="provincial",
    )

    match = suggest(
        in_memory_db,
        CategorizationRequest(description="COM. PAGO MOVIL", source="provincial"),
    )

    assert match is not None
    assert match.category_id == fees_id


def test_suggest_global_rule_matches_any_source(
    in_memory_db: sqlite3.Connection,
) -> None:
    """A rule with source=NULL matches every source."""
    fees_id = _insert_category(in_memory_db, TransactionKind.EXPENSE, "Fees")
    _insert_rule(in_memory_db, pattern=r"fee", category_id=fees_id, source=None)

    for source in ("provincial", "binance", "cash_cli"):
        match = suggest(
            in_memory_db,
            CategorizationRequest(description="monthly fee", source=source),
        )
        assert match is not None, f"rule did not match source={source}"
        assert match.category_id == fees_id


def test_suggest_account_scoped_rule_matches_same_account(
    in_memory_db: sqlite3.Connection,
) -> None:
    interest_id = _insert_category(in_memory_db, TransactionKind.INCOME, "Interest")
    account_id = _insert_account(in_memory_db, name="Binance Earn (test)")
    _insert_rule(
        in_memory_db,
        pattern=r"ZZ_ACCT_TOKEN",
        category_id=interest_id,
        account_id=account_id,
    )

    match = suggest(
        in_memory_db,
        CategorizationRequest(
            description="ZZ_ACCT_TOKEN", source="test_src", account_id=account_id
        ),
    )

    assert match is not None
    assert match.category_id == interest_id


# ---------------------------------------------------------------------------
# `suggest()` — failure modes.
# ---------------------------------------------------------------------------


def test_suggest_returns_none_on_no_match(in_memory_db: sqlite3.Connection) -> None:
    food_id = _insert_category(in_memory_db, TransactionKind.EXPENSE, "Food")
    _insert_rule(in_memory_db, pattern=r"PANADERIA", category_id=food_id)

    match = suggest(
        in_memory_db,
        CategorizationRequest(description="ATM withdrawal", source="provincial"),
    )

    assert match is None


def test_suggest_returns_none_on_empty_description(
    in_memory_db: sqlite3.Connection,
) -> None:
    food_id = _insert_category(in_memory_db, TransactionKind.EXPENSE, "Food")
    _insert_rule(in_memory_db, pattern=r".+", category_id=food_id)

    assert (
        suggest(in_memory_db, CategorizationRequest(description="", source="provincial"))
        is None
    )
    assert (
        suggest(in_memory_db, CategorizationRequest(description=None, source="provincial"))
        is None
    )


def test_suggest_skips_inactive_rules(in_memory_db: sqlite3.Connection) -> None:
    food_id = _insert_category(in_memory_db, TransactionKind.EXPENSE, "Food")
    # Deliberately unique token so the migration-seeded rules don't shadow this.
    _insert_rule(
        in_memory_db,
        pattern=r"ZZ_INACTIVE_TOKEN",
        category_id=food_id,
        active=False,
    )

    match = suggest(
        in_memory_db,
        CategorizationRequest(description="ZZ_INACTIVE_TOKEN", source="provincial"),
    )

    assert match is None


def test_suggest_source_scoped_rule_does_not_match_other_source(
    in_memory_db: sqlite3.Connection,
) -> None:
    fees_id = _insert_category(in_memory_db, TransactionKind.EXPENSE, "Fees")
    _insert_rule(
        in_memory_db,
        pattern=r"fee",
        category_id=fees_id,
        source="binance",
    )

    match = suggest(
        in_memory_db,
        CategorizationRequest(description="monthly fee", source="provincial"),
    )

    assert match is None


def test_suggest_account_scoped_rule_does_not_match_other_account(
    in_memory_db: sqlite3.Connection,
) -> None:
    interest_id = _insert_category(in_memory_db, TransactionKind.INCOME, "Interest")
    scoped_account = _insert_account(in_memory_db, name="Scoped Earn")
    other_account = _insert_account(in_memory_db, name="Other Earn")
    # Unique token so no seeded rule shadows this test.
    _insert_rule(
        in_memory_db,
        pattern=r"ZZ_SCOPED_TOKEN",
        category_id=interest_id,
        account_id=scoped_account,
    )

    match = suggest(
        in_memory_db,
        CategorizationRequest(
            description="ZZ_SCOPED_TOKEN", source="test_src", account_id=other_account
        ),
    )

    assert match is None


def test_suggest_rejects_non_pydantic_input(in_memory_db: sqlite3.Connection) -> None:
    """ADR-009: engine accepts a Pydantic model, not a raw dict."""
    with pytest.raises((TypeError, AttributeError)):
        suggest(in_memory_db, {"description": "x", "source": "provincial"})  # type: ignore[arg-type]


def test_categorization_request_rejects_extra_fields() -> None:
    """Strict model forbids drift (ADR-009)."""
    with pytest.raises(Exception):
        CategorizationRequest(  # type: ignore[call-arg]
            description="x",
            source="provincial",
            unknown_field="boom",
        )


# ---------------------------------------------------------------------------
# Priority ordering.
# ---------------------------------------------------------------------------


def test_suggest_returns_highest_priority_match(in_memory_db: sqlite3.Connection) -> None:
    """Lower `priority` wins; first-match-by-priority semantics."""
    food_id = _insert_category(in_memory_db, TransactionKind.EXPENSE, "Food")
    lifestyle_id = _insert_category(in_memory_db, TransactionKind.EXPENSE, "Lifestyle")

    # Lifestyle rule has priority=10 (wins over Food at priority=100).
    _insert_rule(in_memory_db, pattern=r"RESTAURANT", category_id=food_id, priority=100)
    _insert_rule(
        in_memory_db, pattern=r"RESTAURANT", category_id=lifestyle_id, priority=10
    )

    match = suggest(
        in_memory_db,
        CategorizationRequest(description="RESTAURANT ACME", source="provincial"),
    )

    assert match is not None
    assert match.category_id == lifestyle_id


def test_suggest_scoped_rule_beats_global_at_same_priority(
    in_memory_db: sqlite3.Connection,
) -> None:
    """When priorities tie, source-scoped rule wins over source=NULL."""
    food_id = _insert_category(in_memory_db, TransactionKind.EXPENSE, "Food")
    fees_id = _insert_category(in_memory_db, TransactionKind.EXPENSE, "Fees")

    _insert_rule(
        in_memory_db, pattern=r"TRAV", category_id=food_id, source=None, priority=50
    )
    _insert_rule(
        in_memory_db,
        pattern=r"TRAV",
        category_id=fees_id,
        source="provincial",
        priority=50,
    )

    match = suggest(
        in_memory_db,
        CategorizationRequest(description="TRAV123456", source="provincial"),
    )

    assert match is not None
    assert match.category_id == fees_id


# ---------------------------------------------------------------------------
# Property-based tests (rule-011 mandatory).
# ---------------------------------------------------------------------------


@given(
    priorities=st.lists(
        st.integers(min_value=1, max_value=999),
        min_size=2,
        max_size=10,
        unique=True,
    ),
)
@settings(max_examples=50, deadline=None)
def test_priority_ordering_is_total(
    tmp_path_factory: pytest.TempPathFactory, priorities: list[int]
) -> None:
    """For any set of rules with the same pattern, suggest() returns the one
    with the lowest priority value."""
    from finances.db.connection import get_connection
    from finances.db.migrate import apply_migrations
    from pathlib import Path

    migrations_dir = Path(__file__).resolve().parents[1] / "finances" / "db" / "migrations"
    db_path = tmp_path_factory.mktemp("prop_priority") / "t.db"
    conn = get_connection(db_path)
    try:
        apply_migrations(conn, migrations_dir=migrations_dir)
        # Fresh scratch category — migration seeds exist but we use a unique name.
        scratch_id = _insert_category(
            conn, TransactionKind.EXPENSE, f"ScratchProp-{id(priorities)}"
        )
        rules_by_priority: dict[int, int] = {}
        for p in priorities:
            cid = _insert_category(
                conn, TransactionKind.EXPENSE, f"ScratchCat-{id(priorities)}-{p}"
            )
            _insert_rule(conn, pattern=r"WIDGET", category_id=cid, priority=p)
            rules_by_priority[p] = cid

        # Inject noise rule with higher-priority-number (should lose).
        _insert_rule(conn, pattern=r"WIDGET", category_id=scratch_id, priority=9999)

        match = suggest(
            conn,
            CategorizationRequest(description="WIDGET 123", source="provincial"),
        )
        expected_cid = rules_by_priority[min(priorities)]
        assert match is not None
        assert match.category_id == expected_cid
    finally:
        conn.close()


@given(
    scope_source=st.sampled_from(["provincial", "binance", "cash_cli"]),
    request_source=st.sampled_from(["provincial", "binance", "cash_cli"]),
)
@settings(max_examples=30, deadline=None)
def test_source_scoping_property(
    tmp_path_factory: pytest.TempPathFactory,
    scope_source: str,
    request_source: str,
) -> None:
    """A source-scoped rule matches iff request.source == rule.source."""
    from finances.db.connection import get_connection
    from finances.db.migrate import apply_migrations
    from pathlib import Path

    migrations_dir = Path(__file__).resolve().parents[1] / "finances" / "db" / "migrations"
    db_path = tmp_path_factory.mktemp("prop_src") / "t.db"
    conn = get_connection(db_path)
    try:
        apply_migrations(conn, migrations_dir=migrations_dir)
        cid = _insert_category(
            conn,
            TransactionKind.EXPENSE,
            f"ScopeCat-{id((scope_source, request_source))}",
        )
        _insert_rule(
            conn, pattern=r"UNIQUEPROBE", category_id=cid, source=scope_source
        )

        match = suggest(
            conn,
            CategorizationRequest(description="UNIQUEPROBE 1", source=request_source),
        )

        if scope_source == request_source:
            assert match is not None
            assert match.category_id == cid
        else:
            assert match is None
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# `load_rules()`.
# ---------------------------------------------------------------------------


def test_load_rules_returns_active_only_by_default(
    in_memory_db: sqlite3.Connection,
) -> None:
    food_id = _insert_category(in_memory_db, TransactionKind.EXPENSE, "Food")
    _insert_rule(in_memory_db, pattern=r"A", category_id=food_id, active=True)
    _insert_rule(in_memory_db, pattern=r"B", category_id=food_id, active=False)

    rules = load_rules(in_memory_db)

    patterns = {r.pattern for r in rules}
    assert "A" in patterns
    assert "B" not in patterns
    # Seed rules (from migration) also active — we only assert presence of "A",
    # not list length.
    for r in rules:
        assert isinstance(r, CategoryRule)
        assert r.active is True


def test_load_rules_include_inactive(in_memory_db: sqlite3.Connection) -> None:
    food_id = _insert_category(in_memory_db, TransactionKind.EXPENSE, "Food")
    _insert_rule(in_memory_db, pattern=r"A-active", category_id=food_id, active=True)
    _insert_rule(in_memory_db, pattern=r"B-inactive", category_id=food_id, active=False)

    rules = load_rules(in_memory_db, include_inactive=True)

    patterns = {r.pattern for r in rules}
    assert "A-active" in patterns
    assert "B-inactive" in patterns


# ---------------------------------------------------------------------------
# Seed rules actually fire against real CSV shapes.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "description, source, expected_kind",
    [
        ("COM. PAGO MOVIL", "provincial", TransactionKind.EXPENSE),
        ("PANADERIA LUISANA 2004", "provincial", TransactionKind.EXPENSE),
        ("LUNCHERIA MILY GOURMET", "provincial", TransactionKind.EXPENSE),
        ("DIGITEL", "provincial", TransactionKind.EXPENSE),
        ("Netflix Subscription", "binance", TransactionKind.EXPENSE),
        ("Earn reward flexible", "binance", TransactionKind.INCOME),
    ],
)
def test_seeded_rules_fire_on_real_shapes(
    in_memory_db: sqlite3.Connection,
    description: str,
    source: str,
    expected_kind: TransactionKind,
) -> None:
    """Smoke test: the migration-seeded rules actually match representative
    descriptions from the CSVs."""
    match = suggest(
        in_memory_db,
        CategorizationRequest(description=description, source=source),
    )
    assert match is not None, f"no seed rule matched: {description!r} / {source}"
    row = in_memory_db.execute(
        "SELECT kind FROM categories WHERE id = ?", (match.category_id,)
    ).fetchone()
    assert TransactionKind(row["kind"]) == expected_kind


# ---------------------------------------------------------------------------
# `RuleMatch` is a Pydantic model (ADR-009 compliance).
# ---------------------------------------------------------------------------


def test_rule_match_is_pydantic() -> None:
    from pydantic import BaseModel

    assert issubclass(CategoryRule, BaseModel)
    assert issubclass(CategorizationRequest, BaseModel)
    assert issubclass(RuleMatch, BaseModel)


# ---------------------------------------------------------------------------
# CLI dry-run (Typer).
# ---------------------------------------------------------------------------


def test_categorize_dry_run_reports_percentage(tmp_path) -> None:
    """`finances categorize --dry-run --source provincial` reports auto-classify %."""
    from typer.testing import CliRunner

    from finances.cli.main import app
    from finances.db.connection import get_connection
    from finances.db.migrate import apply_migrations
    from pathlib import Path

    migrations_dir = Path(__file__).resolve().parents[1] / "finances" / "db" / "migrations"
    db_path = tmp_path / "finances.db"
    conn = get_connection(db_path)
    try:
        apply_migrations(conn, migrations_dir=migrations_dir)
        # Seed 1 account + 2 provincial transactions — 1 that should match a
        # seeded rule, 1 that should not.
        conn.execute(
            "INSERT INTO accounts (name, kind, currency, institution) "
            "VALUES (?, ?, ?, ?)",
            ("Provincial Bolivares", "bank", "VES", "Provincial"),
        )
        account_id = conn.execute(
            "SELECT id FROM accounts WHERE name = 'Provincial Bolivares'"
        ).fetchone()["id"]
        conn.executemany(
            """
            INSERT INTO transactions (
                account_id, occurred_at, kind, amount, currency,
                description, source, source_ref, needs_review
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1)
            """,
            [
                (
                    account_id,
                    "2025-11-01T00:00:00+00:00",
                    "expense",
                    "-9.41",
                    "VES",
                    "COM. PAGO MOVIL",
                    "provincial",
                    "ref-1",
                ),
                (
                    account_id,
                    "2025-11-02T00:00:00+00:00",
                    "expense",
                    "-100.00",
                    "VES",
                    "ZZZZZ UNMATCHABLE TOKEN",
                    "provincial",
                    "ref-2",
                ),
            ],
        )
        conn.commit()
    finally:
        conn.close()

    runner = CliRunner()
    result = runner.invoke(
        app,
        [
            "categorize",
            "--dry-run",
            "--source",
            "provincial",
            "--db-path",
            str(db_path),
        ],
    )
    assert result.exit_code == 0, result.output
    # Should mention "50" (1 of 2 matched) and "provincial".
    assert "provincial" in result.output.lower()
    assert "50" in result.output or "1/2" in result.output


def test_categorize_dry_run_on_empty_db_exits_clean(tmp_path) -> None:
    from typer.testing import CliRunner

    from finances.cli.main import app
    from finances.db.connection import get_connection
    from finances.db.migrate import apply_migrations
    from pathlib import Path

    migrations_dir = Path(__file__).resolve().parents[1] / "finances" / "db" / "migrations"
    db_path = tmp_path / "finances.db"
    conn = get_connection(db_path)
    try:
        apply_migrations(conn, migrations_dir=migrations_dir)
    finally:
        conn.close()

    runner = CliRunner()
    result = runner.invoke(
        app,
        [
            "categorize",
            "--dry-run",
            "--source",
            "provincial",
            "--db-path",
            str(db_path),
        ],
    )
    assert result.exit_code == 0, result.output
