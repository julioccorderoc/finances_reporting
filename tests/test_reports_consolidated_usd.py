"""Tests for finances.reports.consolidated_usd (EPIC-013, ADR-005 amendment).

Covers:

* The priority-chain semantics as they surface through the consolidated
  report (``user_rate`` vs. ``binance_p2p_median`` vs. ``bcv`` fallback
  vs. ``needs_review``).
* The ADR-005 2026-04-19 headline rule: BCV-sourced USD values are
  flagged, excluded from the headline aggregate, and surfaced as
  ``strict_violations`` so the CLI layer can exit non-zero on
  ``--strict``.
* The three render helpers (``render_table``/``render_json``/
  ``render_csv``).

These tests deliberately reuse the shared ``in_memory_db`` /
``seeded_db`` fixtures from ``tests/conftest.py`` and the
``TransactionFactory`` / ``RateFactory`` polyfactories.
"""

from __future__ import annotations

import csv
import io
import json
import sqlite3
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any

import pytest

from finances.db.repos import rates as rates_repo
from finances.db.repos import transactions as transactions_repo
from finances.domain.models import Rate, Transaction, TransactionKind
from finances.reports import consolidated_usd
from finances.reports.consolidated_usd import (
    ConsolidatedReport,
    ConsolidatedRow,
    build_report,
    render_csv,
    render_json,
    render_table,
)
from tests.conftest import RateFactory, TransactionFactory


# ---------------------------------------------------------------------------
# Small helpers so individual tests stay short and readable.
# ---------------------------------------------------------------------------


def _insert_txn(conn: sqlite3.Connection, **overrides: Any) -> Transaction:
    txn = TransactionFactory.build(**overrides)
    return transactions_repo.insert(conn, txn)


def _insert_rate(conn: sqlite3.Connection, **overrides: Any) -> Rate:
    return rates_repo.insert(conn, RateFactory.build(**overrides))


def _txn_on(day: date, **overrides: Any) -> dict[str, Any]:
    return {
        "occurred_at": datetime(day.year, day.month, day.day, 12, 0, tzinfo=UTC),
        **overrides,
    }


# ---------------------------------------------------------------------------
# Happy paths for each branch of the rate resolver, observed through the
# report's eyes.
# ---------------------------------------------------------------------------


def test_user_rate_wins_and_is_not_flagged(seeded_db: sqlite3.Connection) -> None:
    """user_rate=50, amount=1000 VES -> 20 USD, rate_source='user_rate'."""
    _insert_txn(
        seeded_db,
        account_id=1,
        amount=Decimal("1000"),
        currency="VES",
        user_rate=Decimal("50"),
        kind=TransactionKind.EXPENSE,
        **_txn_on(date(2025, 6, 15)),
    )

    report = build_report(seeded_db)

    assert len(report.rows) == 1
    row = report.rows[0]
    assert row.rate_source == "user_rate"
    assert row.amount_usd == Decimal("20")
    assert row.is_bcv_fallback is False
    assert report.total_usd == Decimal("20")
    assert report.fallback_total_usd == Decimal("0")
    assert report.fallback_row_count == 0
    assert report.strict_violations == []


def test_binance_p2p_median_resolves_and_is_not_flagged(
    seeded_db: sqlite3.Connection,
) -> None:
    day = date(2025, 6, 15)
    _insert_rate(
        seeded_db,
        as_of_date=day,
        base="USDT",
        quote="VES",
        source="binance_p2p_median",
        rate=Decimal("80"),
    )
    _insert_txn(
        seeded_db,
        account_id=1,
        amount=Decimal("800"),
        currency="VES",
        user_rate=None,
        kind=TransactionKind.EXPENSE,
        **_txn_on(day),
    )

    report = build_report(seeded_db)

    assert len(report.rows) == 1
    row = report.rows[0]
    assert row.rate_source == "binance_p2p_median"
    assert row.amount_usd == Decimal("10")
    assert row.is_bcv_fallback is False
    assert report.total_usd == Decimal("10")
    assert report.fallback_total_usd == Decimal("0")
    assert report.strict_violations == []


def test_bcv_fallback_is_flagged_and_excluded_from_headline(
    seeded_db: sqlite3.Connection,
) -> None:
    """BCV-sourced rows keep an amount_usd but must NOT count toward total."""
    day = date(2025, 6, 15)
    _insert_rate(
        seeded_db,
        as_of_date=day,
        base="USD",
        quote="VES",
        source="bcv",
        rate=Decimal("40"),
    )
    inserted = _insert_txn(
        seeded_db,
        account_id=1,
        amount=Decimal("400"),
        currency="VES",
        user_rate=None,
        kind=TransactionKind.EXPENSE,
        **_txn_on(day),
    )

    report = build_report(seeded_db)

    assert len(report.rows) == 1
    row = report.rows[0]
    assert row.rate_source == "bcv"
    assert row.is_bcv_fallback is True
    assert row.amount_usd == Decimal("10")
    # Excluded from the headline, counted in the fallback bucket.
    assert report.total_usd == Decimal("0")
    assert report.fallback_total_usd == Decimal("10")
    assert report.fallback_row_count == 1
    assert inserted.id is not None
    assert report.strict_violations == [inserted.id]


def test_bcv_carry_fallback_is_still_flagged(seeded_db: sqlite3.Connection) -> None:
    """carry-forward BCV rows must still trip the headline rule."""
    _insert_rate(
        seeded_db,
        as_of_date=date(2025, 6, 10),
        base="USD",
        quote="VES",
        source="bcv",
        rate=Decimal("40"),
    )
    inserted = _insert_txn(
        seeded_db,
        account_id=1,
        amount=Decimal("400"),
        currency="VES",
        user_rate=None,
        kind=TransactionKind.EXPENSE,
        **_txn_on(date(2025, 6, 15)),
    )

    report = build_report(seeded_db)

    row = report.rows[0]
    assert row.rate_source == "bcv_carry"
    assert row.is_bcv_fallback is True
    assert inserted.id is not None
    assert report.strict_violations == [inserted.id]


def test_strict_flag_populates_violations_only_for_bcv_rows(
    seeded_db: sqlite3.Connection,
) -> None:
    """Mix a user_rate row + a BCV-fallback row; only the BCV row is a violation."""
    day = date(2025, 6, 15)
    _insert_rate(
        seeded_db,
        as_of_date=day,
        base="USD",
        quote="VES",
        source="bcv",
        rate=Decimal("40"),
    )
    ok_txn = _insert_txn(
        seeded_db,
        account_id=1,
        amount=Decimal("1000"),
        currency="VES",
        user_rate=Decimal("50"),
        kind=TransactionKind.EXPENSE,
        **_txn_on(day),
    )
    bad_txn = _insert_txn(
        seeded_db,
        account_id=1,
        amount=Decimal("400"),
        currency="VES",
        user_rate=None,
        kind=TransactionKind.EXPENSE,
        **_txn_on(day),
    )

    report = build_report(seeded_db, strict=True)

    # strict=True still returns the full report — CLI decides the exit code.
    assert len(report.rows) == 2
    assert ok_txn.id is not None and bad_txn.id is not None
    assert bad_txn.id in report.strict_violations
    assert ok_txn.id not in report.strict_violations


def test_needs_review_row_has_no_usd_and_is_not_a_strict_violation(
    seeded_db: sqlite3.Connection,
) -> None:
    """Unresolved rows are not BCV fallbacks; they need a rate, not a flag."""
    inserted = _insert_txn(
        seeded_db,
        account_id=1,
        amount=Decimal("1000"),
        currency="VES",
        user_rate=None,
        kind=TransactionKind.EXPENSE,
        **_txn_on(date(2025, 6, 15)),
    )

    report = build_report(seeded_db)

    row = report.rows[0]
    assert row.rate_source == "needs_review"
    assert row.amount_usd is None
    assert row.is_bcv_fallback is False
    assert inserted.id is not None
    assert inserted.id not in report.strict_violations
    # Unresolved rows do not add to either total.
    assert report.total_usd == Decimal("0")
    assert report.fallback_total_usd == Decimal("0")


def test_native_usd_pass_through(seeded_db: sqlite3.Connection) -> None:
    """USDT / USD / USDC rows must keep their native amount and not be BCV fallbacks."""
    _insert_txn(
        seeded_db,
        account_id=2,  # Binance Spot (USDT)
        amount=Decimal("12.34"),
        currency="USDT",
        user_rate=None,
        kind=TransactionKind.INCOME,
        **_txn_on(date(2025, 6, 15)),
    )

    report = build_report(seeded_db)

    row = report.rows[0]
    assert row.rate_source == "native_usd"
    assert row.amount_usd == Decimal("12.34")
    assert row.is_bcv_fallback is False
    assert report.total_usd == Decimal("12.34")


# ---------------------------------------------------------------------------
# Structural / correctness tests.
# ---------------------------------------------------------------------------


def test_transfers_are_excluded_from_report(seeded_db: sqlite3.Connection) -> None:
    day = date(2025, 6, 15)
    _insert_txn(
        seeded_db,
        account_id=2,
        amount=Decimal("-10"),
        currency="USDT",
        user_rate=None,
        kind=TransactionKind.TRANSFER,
        transfer_id="xfer-1",
        source_ref="xfer-1-out",
        **_txn_on(day),
    )
    _insert_txn(
        seeded_db,
        account_id=3,
        amount=Decimal("10"),
        currency="USDT",
        user_rate=None,
        kind=TransactionKind.TRANSFER,
        transfer_id="xfer-1",
        source_ref="xfer-1-in",
        **_txn_on(day),
    )
    # One non-transfer row to prove the filter keeps the rest.
    _insert_txn(
        seeded_db,
        account_id=2,
        amount=Decimal("5"),
        currency="USDT",
        user_rate=None,
        kind=TransactionKind.INCOME,
        **_txn_on(day),
    )

    report = build_report(seeded_db)

    assert len(report.rows) == 1
    assert report.rows[0].kind == "income"
    assert all(r.kind != "transfer" for r in report.rows)


def test_totals_add_up_across_mixed_rows(seeded_db: sqlite3.Connection) -> None:
    """3 non-fallback + 2 fallback rows -> totals reflect the split correctly."""
    day = date(2025, 6, 15)
    _insert_rate(
        seeded_db,
        as_of_date=day,
        base="USD",
        quote="VES",
        source="bcv",
        rate=Decimal("40"),
    )

    # Three headline-eligible rows: 10 + 20 + 5 = 35 USD.
    _insert_txn(
        seeded_db,
        account_id=1,
        amount=Decimal("500"),
        currency="VES",
        user_rate=Decimal("50"),
        kind=TransactionKind.EXPENSE,
        **_txn_on(day),
    )  # 10 USD
    _insert_txn(
        seeded_db,
        account_id=1,
        amount=Decimal("1000"),
        currency="VES",
        user_rate=Decimal("50"),
        kind=TransactionKind.EXPENSE,
        **_txn_on(day),
    )  # 20 USD
    _insert_txn(
        seeded_db,
        account_id=2,
        amount=Decimal("5"),
        currency="USDT",
        user_rate=None,
        kind=TransactionKind.INCOME,
        **_txn_on(day),
    )  # 5 USD native

    # Two BCV-fallback rows: 10 + 2 = 12 USD in the fallback bucket.
    _insert_txn(
        seeded_db,
        account_id=1,
        amount=Decimal("400"),
        currency="VES",
        user_rate=None,
        kind=TransactionKind.EXPENSE,
        **_txn_on(day),
    )  # 10 USD (BCV)
    _insert_txn(
        seeded_db,
        account_id=1,
        amount=Decimal("80"),
        currency="VES",
        user_rate=None,
        kind=TransactionKind.EXPENSE,
        **_txn_on(day),
    )  # 2 USD (BCV)

    report = build_report(seeded_db)

    assert len(report.rows) == 5
    assert report.total_usd == Decimal("35")
    assert report.fallback_total_usd == Decimal("12")
    assert report.fallback_row_count == 2
    assert len(report.strict_violations) == 2


def test_rows_sorted_by_occurred_at_then_id(seeded_db: sqlite3.Connection) -> None:
    later = _insert_txn(
        seeded_db,
        account_id=2,
        amount=Decimal("1"),
        currency="USDT",
        kind=TransactionKind.INCOME,
        **_txn_on(date(2025, 6, 15)),
    )
    earlier = _insert_txn(
        seeded_db,
        account_id=2,
        amount=Decimal("2"),
        currency="USDT",
        kind=TransactionKind.INCOME,
        **_txn_on(date(2025, 6, 10)),
    )

    report = build_report(seeded_db)
    ids = [r.transaction_id for r in report.rows]

    assert earlier.id is not None and later.id is not None
    assert ids == [earlier.id, later.id]


# ---------------------------------------------------------------------------
# Rendering helpers.
# ---------------------------------------------------------------------------


def _make_sample_report(seeded_db: sqlite3.Connection) -> ConsolidatedReport:
    """Produce a report with one native, one user_rate, one BCV row."""
    day = date(2025, 6, 15)
    _insert_rate(
        seeded_db,
        as_of_date=day,
        base="USD",
        quote="VES",
        source="bcv",
        rate=Decimal("40"),
    )
    _insert_txn(
        seeded_db,
        account_id=2,
        amount=Decimal("5"),
        currency="USDT",
        kind=TransactionKind.INCOME,
        description="coffee",
        **_txn_on(day),
    )
    _insert_txn(
        seeded_db,
        account_id=1,
        amount=Decimal("1000"),
        currency="VES",
        user_rate=Decimal("50"),
        kind=TransactionKind.EXPENSE,
        description="lunch",
        **_txn_on(day),
    )
    _insert_txn(
        seeded_db,
        account_id=1,
        amount=Decimal("400"),
        currency="VES",
        user_rate=None,
        kind=TransactionKind.EXPENSE,
        description="groceries",
        **_txn_on(day),
    )
    return build_report(seeded_db)


def test_render_json_round_trips_as_structured_payload(
    seeded_db: sqlite3.Connection,
) -> None:
    report = _make_sample_report(seeded_db)

    out = render_json(report)
    parsed = json.loads(out)

    assert set(parsed.keys()) == {
        "rows",
        "total_usd",
        "fallback_total_usd",
        "fallback_row_count",
        "strict_violations",
    }
    # Decimals serialized as strings (no 0.30000000000000004 artifacts).
    assert isinstance(parsed["total_usd"], str)
    assert isinstance(parsed["fallback_total_usd"], str)
    assert parsed["total_usd"] == "25"  # 5 USDT native + 20 user_rate USD
    assert parsed["fallback_total_usd"] == "10"  # 400 / 40
    assert parsed["fallback_row_count"] == 1
    # Each row also carries Decimals as strings and datetimes as ISO8601.
    for row in parsed["rows"]:
        assert isinstance(row["amount_native"], str)
        assert row["amount_usd"] is None or isinstance(row["amount_usd"], str)
        # ISO-8601 string is round-trippable.
        datetime.fromisoformat(row["occurred_at"])


def test_render_csv_has_expected_header_and_row_count(
    seeded_db: sqlite3.Connection,
) -> None:
    report = _make_sample_report(seeded_db)

    out = render_csv(report)
    reader = csv.reader(io.StringIO(out))
    header = next(reader)
    body = list(reader)

    assert header == [
        "transaction_id",
        "occurred_at",
        "account_id",
        "kind",
        "currency",
        "amount_native",
        "amount_usd",
        "rate_source",
        "is_bcv_fallback",
        "description",
    ]
    assert len(body) == len(report.rows) == 3


def test_render_table_annotates_bcv_fallback_rows(
    seeded_db: sqlite3.Connection,
) -> None:
    report = _make_sample_report(seeded_db)

    out = render_table(report)

    assert "[BCV fallback]" in out
    # Non-fallback rows must not be annotated.
    assert out.count("[BCV fallback]") == report.fallback_row_count
    # Totals surfaced in the footer so the user sees the split.
    assert "total_usd" in out.lower() or "headline" in out.lower()
    assert "fallback" in out.lower()


def test_render_table_handles_empty_report(
    in_memory_db: sqlite3.Connection,
) -> None:
    """Empty report must render a header without crashing."""
    report = build_report(in_memory_db)
    assert report.rows == []

    out = render_table(report)

    # Headers still present; no BCV annotation (nothing to flag).
    assert "Date" in out
    assert "USD" in out
    assert "[BCV fallback]" not in out


# ---------------------------------------------------------------------------
# Pydantic boundary / contract.
# ---------------------------------------------------------------------------


def test_consolidated_row_is_strict_pydantic_model() -> None:
    """ConsolidatedRow must forbid extras and reject unknown fields (rule-009)."""
    with pytest.raises(Exception):
        ConsolidatedRow(
            transaction_id=1,
            occurred_at=datetime(2025, 6, 15, 12, 0, tzinfo=UTC),
            account_id=1,
            kind="expense",
            currency="USD",
            amount_native=Decimal("1"),
            amount_usd=Decimal("1"),
            rate_source="native_usd",
            description=None,
            is_bcv_fallback=False,
            extra_field="nope",  # type: ignore[call-arg]
        )


def test_build_report_does_not_write_to_db(seeded_db: sqlite3.Connection) -> None:
    """Reports are read-only; needs_review stays what the DB already has."""
    _insert_txn(
        seeded_db,
        account_id=1,
        amount=Decimal("1000"),
        currency="VES",
        user_rate=None,
        kind=TransactionKind.EXPENSE,
        **_txn_on(date(2025, 6, 15)),
    )

    build_report(seeded_db)

    row = seeded_db.execute(
        "SELECT needs_review FROM transactions WHERE currency = 'VES'"
    ).fetchone()
    # The resolver mutates its in-memory Transaction, but the DB row must stay
    # untouched — reports never persist.
    assert row["needs_review"] == 0


# Silence unused-import warnings for symbols consumed only via __all__.
_ = (consolidated_usd,)
