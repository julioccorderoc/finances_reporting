"""Tests for EPIC-013 — ``finances/reports/monthly.py``.

Per rule-011: every public function gets >=1 happy-path AND >=1 failure-mode
test, and these tests are committed **before** the implementation.

The monthly report is a headline report per the ADR-005 2026-04-19 amendment,
so BCV-sourced USD values must be isolated from the headline total. The test
suite exercises the full priority chain (user_rate, P2P median, BCV fallback,
needs_review) and asserts the v_transactions_usd round-trip invariant that
the epic's Definition of Done hinges on.
"""

from __future__ import annotations

import csv
import io
import json
import sqlite3
from datetime import UTC, date, datetime
from decimal import Decimal

import pytest

from finances.db.repos import accounts as accounts_repo
from finances.db.repos import categories as categories_repo
from finances.db.repos import rates as rates_repo
from finances.db.repos import transactions as transactions_repo
from finances.domain.models import (
    Account,
    AccountKind,
    Category,
    Rate,
    Transaction,
    TransactionKind,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _insert_account(
    conn: sqlite3.Connection,
    name: str,
    *,
    kind: AccountKind = AccountKind.BANK,
    currency: str = "USD",
) -> Account:
    return accounts_repo.insert(
        conn,
        Account(name=name, kind=kind, currency=currency, institution=None),
    )


def _insert_category(
    conn: sqlite3.Connection,
    name: str,
    *,
    kind: TransactionKind = TransactionKind.EXPENSE,
) -> Category:
    return categories_repo.insert(conn, Category(kind=kind, name=name))


def _insert_txn(
    conn: sqlite3.Connection,
    account_id: int,
    amount: Decimal,
    *,
    occurred_at: datetime,
    currency: str = "USD",
    kind: TransactionKind = TransactionKind.EXPENSE,
    source_ref: str,
    category_id: int | None = None,
    user_rate: Decimal | None = None,
    transfer_id: str | None = None,
) -> Transaction:
    txn = Transaction(
        account_id=account_id,
        occurred_at=occurred_at,
        kind=kind,
        amount=amount,
        currency=currency,
        description=None,
        category_id=category_id,
        transfer_id=transfer_id,
        user_rate=user_rate,
        source="test",
        source_ref=source_ref,
    )
    return transactions_repo.insert(conn, txn)


def _insert_rate(
    conn: sqlite3.Connection,
    *,
    as_of: date,
    base: str,
    quote: str,
    rate: Decimal,
    source: str,
) -> Rate:
    return rates_repo.insert(
        conn,
        Rate(as_of_date=as_of, base=base, quote=quote, rate=rate, source=source),
    )


def _dt(y: int, m: int, d: int) -> datetime:
    return datetime(y, m, d, 12, 0, tzinfo=UTC)


# ---------------------------------------------------------------------------
# build_report — happy path
# ---------------------------------------------------------------------------


def test_build_report_happy_path_single_month(
    in_memory_db: sqlite3.Connection,
) -> None:
    from finances.reports.monthly import MonthlyReport, MonthlyRow, build_report

    acc = _insert_account(in_memory_db, "Alpha Bank", currency="USD")
    cat_food = _insert_category(in_memory_db, "Food-MonthlyTest")
    cat_tx = _insert_category(in_memory_db, "Transport-MonthlyTest")
    assert acc.id is not None
    assert cat_food.id is not None
    assert cat_tx.id is not None

    _insert_txn(
        in_memory_db, acc.id, Decimal("-10.00"),
        occurred_at=_dt(2026, 2, 5), source_ref="m-1", category_id=cat_food.id,
    )
    _insert_txn(
        in_memory_db, acc.id, Decimal("-20.00"),
        occurred_at=_dt(2026, 2, 10), source_ref="m-2", category_id=cat_food.id,
    )
    _insert_txn(
        in_memory_db, acc.id, Decimal("-5.00"),
        occurred_at=_dt(2026, 2, 15), source_ref="m-3", category_id=cat_tx.id,
    )

    report = build_report(in_memory_db, month="2026-02")

    assert isinstance(report, MonthlyReport)
    # One row per (month, account, category, kind) combo.
    assert len(report.rows) == 2
    assert all(isinstance(r, MonthlyRow) for r in report.rows)

    by_cat = {r.category_name: r for r in report.rows}
    assert "Food-MonthlyTest" in by_cat
    assert "Transport-MonthlyTest" in by_cat
    food_row = by_cat["Food-MonthlyTest"]
    assert food_row.month == "2026-02"
    assert food_row.account_name == "Alpha Bank"
    assert food_row.tx_count == 2
    assert food_row.total_native == Decimal("-30.00")
    assert food_row.currency == "USD"
    # USD native pass-through -> total_usd matches native, no fallback.
    assert food_row.total_usd == Decimal("-30.00")
    assert food_row.fallback_usd == Decimal("0")
    assert food_row.needs_review_count == 0


# ---------------------------------------------------------------------------
# build_report — multi-month filter
# ---------------------------------------------------------------------------


def test_build_report_month_filter_isolates_target_month(
    in_memory_db: sqlite3.Connection,
) -> None:
    from finances.reports.monthly import build_report

    acc = _insert_account(in_memory_db, "Filter Bank", currency="USD")
    assert acc.id is not None

    _insert_txn(
        in_memory_db, acc.id, Decimal("-1.00"),
        occurred_at=_dt(2026, 1, 15), source_ref="jan-1",
    )
    _insert_txn(
        in_memory_db, acc.id, Decimal("-2.00"),
        occurred_at=_dt(2026, 2, 15), source_ref="feb-1",
    )
    _insert_txn(
        in_memory_db, acc.id, Decimal("-4.00"),
        occurred_at=_dt(2026, 2, 20), source_ref="feb-2",
    )

    report = build_report(in_memory_db, month="2026-02")

    assert len(report.rows) == 1
    assert report.rows[0].month == "2026-02"
    assert report.rows[0].tx_count == 2
    assert report.rows[0].total_native == Decimal("-6.00")


def test_build_report_since_until_range(
    in_memory_db: sqlite3.Connection,
) -> None:
    from finances.reports.monthly import build_report

    acc = _insert_account(in_memory_db, "Range Bank", currency="USD")
    assert acc.id is not None

    _insert_txn(
        in_memory_db, acc.id, Decimal("-1.00"),
        occurred_at=_dt(2025, 12, 15), source_ref="out-before",
    )
    _insert_txn(
        in_memory_db, acc.id, Decimal("-2.00"),
        occurred_at=_dt(2026, 1, 15), source_ref="jan-1",
    )
    _insert_txn(
        in_memory_db, acc.id, Decimal("-3.00"),
        occurred_at=_dt(2026, 2, 15), source_ref="feb-1",
    )
    _insert_txn(
        in_memory_db, acc.id, Decimal("-4.00"),
        occurred_at=_dt(2026, 3, 15), source_ref="mar-1",
    )
    _insert_txn(
        in_memory_db, acc.id, Decimal("-5.00"),
        occurred_at=_dt(2026, 4, 15), source_ref="out-after",
    )

    report = build_report(in_memory_db, since="2026-01", until="2026-03")

    months = sorted(r.month for r in report.rows)
    assert months == ["2026-01", "2026-02", "2026-03"]
    assert report.month_range == ("2026-01", "2026-03")


# ---------------------------------------------------------------------------
# build_report — transfers excluded
# ---------------------------------------------------------------------------


def test_build_report_excludes_transfers(
    in_memory_db: sqlite3.Connection,
) -> None:
    from finances.reports.monthly import build_report

    a1 = _insert_account(in_memory_db, "Src Account", currency="USD")
    a2 = _insert_account(in_memory_db, "Dst Account", currency="USD")
    assert a1.id is not None
    assert a2.id is not None

    _insert_txn(
        in_memory_db, a1.id, Decimal("-100.00"),
        kind=TransactionKind.TRANSFER,
        occurred_at=_dt(2026, 2, 10),
        source_ref="xfr-1a",
        transfer_id="xfer-1",
    )
    _insert_txn(
        in_memory_db, a2.id, Decimal("100.00"),
        kind=TransactionKind.TRANSFER,
        occurred_at=_dt(2026, 2, 10),
        source_ref="xfr-1b",
        transfer_id="xfer-1",
    )
    # Keep one non-transfer to ensure the report is not empty by accident.
    _insert_txn(
        in_memory_db, a1.id, Decimal("-7.00"),
        occurred_at=_dt(2026, 2, 12),
        source_ref="exp-1",
    )

    report = build_report(in_memory_db, month="2026-02")

    # Only the expense row should come back.
    assert len(report.rows) == 1
    assert report.rows[0].kind == "expense"
    assert all(r.kind != "transfer" for r in report.rows)


# ---------------------------------------------------------------------------
# build_report — BCV fallback is isolated from the headline total
# ---------------------------------------------------------------------------


def test_build_report_bcv_fallback_isolated_from_headline(
    in_memory_db: sqlite3.Connection,
) -> None:
    from finances.reports.monthly import build_report

    acc = _insert_account(in_memory_db, "BCV Bank", currency="VES")
    assert acc.id is not None

    # Only BCV rate available -> resolver returns bcv-sourced rate.
    _insert_rate(
        in_memory_db,
        as_of=date(2026, 2, 10),
        base="USD",
        quote="VES",
        rate=Decimal("40"),
        source="bcv",
    )
    _insert_txn(
        in_memory_db, acc.id, Decimal("-400.00"), currency="VES",
        occurred_at=_dt(2026, 2, 15),
        source_ref="bcv-1",
    )

    report = build_report(in_memory_db, month="2026-02")

    assert len(report.rows) == 1
    row = report.rows[0]
    assert row.total_usd == Decimal("0")
    assert row.fallback_usd != Decimal("0")
    assert row.fallback_usd < Decimal("0")  # expense -> negative USD
    # Grand totals segregate headline from fallback.
    assert report.grand_total_usd == Decimal("0")
    assert report.fallback_total_usd == row.fallback_usd


# ---------------------------------------------------------------------------
# build_report — user_rate + P2P median contribute to headline
# ---------------------------------------------------------------------------


def test_build_report_headline_includes_user_rate_and_p2p_median(
    in_memory_db: sqlite3.Connection,
) -> None:
    from finances.reports.monthly import build_report

    acc_ves = _insert_account(in_memory_db, "VES Bank", currency="VES")
    acc_usdt = _insert_account(
        in_memory_db, "Spot", kind=AccountKind.CRYPTO_SPOT, currency="USDT"
    )
    assert acc_ves.id is not None
    assert acc_usdt.id is not None

    _insert_rate(
        in_memory_db,
        as_of=date(2026, 2, 10),
        base="USDT",
        quote="VES",
        rate=Decimal("50"),
        source="binance_p2p_median",
    )

    # Row 1: VES expense, user_rate wins.
    _insert_txn(
        in_memory_db, acc_ves.id, Decimal("-200.00"), currency="VES",
        occurred_at=_dt(2026, 2, 10),
        source_ref="ves-user",
        user_rate=Decimal("40"),
    )
    # Row 2: VES expense, no user_rate -> P2P median.
    _insert_txn(
        in_memory_db, acc_ves.id, Decimal("-250.00"), currency="VES",
        occurred_at=_dt(2026, 2, 12),
        source_ref="ves-p2p",
    )
    # Row 3: USDT native pass-through.
    _insert_txn(
        in_memory_db, acc_usdt.id, Decimal("-15.00"), currency="USDT",
        occurred_at=_dt(2026, 2, 14),
        source_ref="usdt-1",
    )

    report = build_report(in_memory_db, month="2026-02")

    for row in report.rows:
        assert row.fallback_usd == Decimal("0")
    assert report.fallback_total_usd == Decimal("0")
    assert report.grand_total_usd != Decimal("0")
    # Sanity: grand total equals sum of the three headline USD contributions:
    #   -200/40 + -250/50 + -15 = -5 + -5 + -15 = -25
    assert report.grand_total_usd == Decimal("-25")


# ---------------------------------------------------------------------------
# build_report — needs_review rows contribute to neither total
# ---------------------------------------------------------------------------


def test_build_report_needs_review_excluded_from_totals(
    in_memory_db: sqlite3.Connection,
) -> None:
    from finances.reports.monthly import build_report

    acc = _insert_account(in_memory_db, "NoRate Bank", currency="VES")
    assert acc.id is not None

    # No rates at all -> resolver returns needs_review.
    _insert_txn(
        in_memory_db, acc.id, Decimal("-999.00"), currency="VES",
        occurred_at=_dt(2026, 2, 10),
        source_ref="nr-1",
    )

    report = build_report(in_memory_db, month="2026-02")

    assert len(report.rows) == 1
    row = report.rows[0]
    assert row.needs_review_count == 1
    assert row.total_usd == Decimal("0")
    assert row.fallback_usd == Decimal("0")
    assert report.grand_total_usd == Decimal("0")
    assert report.fallback_total_usd == Decimal("0")


# ---------------------------------------------------------------------------
# Invariant: sum of USD contributions == sum from v_transactions_usd view
# ---------------------------------------------------------------------------


def test_build_report_sum_matches_v_transactions_usd(
    in_memory_db: sqlite3.Connection,
) -> None:
    from finances.reports.monthly import build_report

    acc_usd = _insert_account(in_memory_db, "USD Bank", currency="USD")
    acc_usdt = _insert_account(
        in_memory_db, "Spot", kind=AccountKind.CRYPTO_SPOT, currency="USDT"
    )
    acc_ves = _insert_account(in_memory_db, "VES Bank", currency="VES")
    assert acc_usd.id is not None
    assert acc_usdt.id is not None
    assert acc_ves.id is not None

    _insert_rate(
        in_memory_db,
        as_of=date(2026, 2, 1),
        base="USDT",
        quote="VES",
        rate=Decimal("50"),
        source="binance_p2p_median",
    )

    # 5 non-transfer txns, no BCV anywhere.
    _insert_txn(
        in_memory_db, acc_usd.id, Decimal("-12.50"),
        occurred_at=_dt(2026, 2, 2), source_ref="v-1",
    )
    _insert_txn(
        in_memory_db, acc_usd.id, Decimal("-7.25"),
        occurred_at=_dt(2026, 2, 5), source_ref="v-2",
    )
    _insert_txn(
        in_memory_db, acc_usdt.id, Decimal("-3.00"), currency="USDT",
        occurred_at=_dt(2026, 2, 10), source_ref="v-3",
    )
    _insert_txn(
        in_memory_db, acc_ves.id, Decimal("-500.00"), currency="VES",
        occurred_at=_dt(2026, 2, 12), source_ref="v-4",
        user_rate=Decimal("40"),
    )
    _insert_txn(
        in_memory_db, acc_ves.id, Decimal("-750.00"), currency="VES",
        occurred_at=_dt(2026, 2, 20), source_ref="v-5",  # uses P2P
    )

    row = in_memory_db.execute(
        """
        SELECT SUM(CAST(amount_usd AS REAL)) AS s FROM v_transactions_usd
        WHERE kind <> 'transfer' AND strftime('%Y-%m', occurred_at) = ?
        """,
        ("2026-02",),
    ).fetchone()
    view_sum = Decimal(str(row["s"] or "0"))

    report = build_report(in_memory_db, month="2026-02")
    report_sum = sum(
        (r.total_usd + r.fallback_usd for r in report.rows), Decimal("0")
    )

    assert abs(report_sum - view_sum) < Decimal("0.01")


# ---------------------------------------------------------------------------
# Grand totals match the per-row sums
# ---------------------------------------------------------------------------


def test_build_report_grand_totals_match_row_sums(
    in_memory_db: sqlite3.Connection,
) -> None:
    from finances.reports.monthly import build_report

    acc_usd = _insert_account(in_memory_db, "Sum USD", currency="USD")
    acc_ves = _insert_account(in_memory_db, "Sum VES", currency="VES")
    acc_ves_bcv = _insert_account(in_memory_db, "Sum VES BCV-only", currency="VES")
    assert acc_usd.id is not None
    assert acc_ves.id is not None
    assert acc_ves_bcv.id is not None

    # P2P only from 2026-02-10 onward; BCV from 2026-02-01 onward.
    _insert_rate(
        in_memory_db,
        as_of=date(2026, 2, 10),
        base="USDT",
        quote="VES",
        rate=Decimal("50"),
        source="binance_p2p_median",
    )
    _insert_rate(
        in_memory_db,
        as_of=date(2026, 2, 1),
        base="USD",
        quote="VES",
        rate=Decimal("40"),
        source="bcv",
    )

    _insert_txn(
        in_memory_db, acc_usd.id, Decimal("-10.00"),
        occurred_at=_dt(2026, 2, 3), source_ref="g-1",
    )
    _insert_txn(
        in_memory_db, acc_ves.id, Decimal("-100.00"), currency="VES",
        occurred_at=_dt(2026, 2, 15), source_ref="g-2",  # P2P (post 2-10)
    )
    # Pre-P2P VES txn: only BCV available -> BCV fallback path.
    _insert_txn(
        in_memory_db, acc_ves_bcv.id, Decimal("-400.00"), currency="VES",
        occurred_at=_dt(2026, 2, 5), source_ref="g-3",  # BCV fallback
    )

    report = build_report(in_memory_db, month="2026-02")

    assert report.grand_total_usd == sum(
        (r.total_usd for r in report.rows), Decimal("0")
    )
    assert report.fallback_total_usd == sum(
        (r.fallback_usd for r in report.rows), Decimal("0")
    )
    assert report.fallback_total_usd != Decimal("0")


# ---------------------------------------------------------------------------
# Rendering helpers
# ---------------------------------------------------------------------------


def test_render_json_round_trips_with_decimal_strings(
    in_memory_db: sqlite3.Connection,
) -> None:
    from finances.reports.monthly import build_report, render_json

    acc = _insert_account(in_memory_db, "JSON Monthly", currency="USD")
    assert acc.id is not None
    _insert_txn(
        in_memory_db, acc.id, Decimal("-4.50"),
        occurred_at=_dt(2026, 2, 3), source_ref="j-1",
    )

    report = build_report(in_memory_db, month="2026-02")
    payload = render_json(report)
    parsed = json.loads(payload)

    assert isinstance(parsed, dict)
    assert "rows" in parsed
    # Grand totals are Decimal strings. Arithmetic on Decimal strips trailing
    # zeros (-4.50 + 0 -> -4.5), so compare values rather than exact format.
    assert Decimal(parsed["grand_total_usd"]) == Decimal("-4.50")
    assert Decimal(parsed["fallback_total_usd"]) == Decimal("0")
    assert isinstance(parsed["rows"], list)
    # Row-level totals: value equality (Decimal arithmetic trims trailing zeros).
    assert Decimal(parsed["rows"][0]["total_native"]) == Decimal("-4.50")
    assert isinstance(parsed["rows"][0]["total_usd"], str)


def test_render_csv_header_and_row(
    in_memory_db: sqlite3.Connection,
) -> None:
    from finances.reports.monthly import build_report, render_csv

    acc = _insert_account(in_memory_db, "CSV Monthly", currency="USD")
    assert acc.id is not None
    _insert_txn(
        in_memory_db, acc.id, Decimal("-3.00"),
        occurred_at=_dt(2026, 2, 3), source_ref="c-1",
    )

    report = build_report(in_memory_db, month="2026-02")
    out = render_csv(report)

    reader = csv.reader(io.StringIO(out))
    rows = list(reader)
    assert rows[0] == [
        "month",
        "account_id",
        "account_name",
        "category_id",
        "category_name",
        "kind",
        "tx_count",
        "total_native",
        "currency",
        "total_usd",
        "fallback_usd",
        "needs_review_count",
    ]
    assert len(rows) == 2
    assert rows[1][0] == "2026-02"
    assert rows[1][2] == "CSV Monthly"


def test_render_csv_empty_report_returns_header_only(
    in_memory_db: sqlite3.Connection,
) -> None:
    from finances.reports.monthly import build_report, render_csv

    report = build_report(in_memory_db, month="2026-02")
    out = render_csv(report)
    reader = csv.reader(io.StringIO(out))
    rows = list(reader)
    assert len(rows) == 1
    assert rows[0][0] == "month"


def test_render_table_empty_report_does_not_crash(
    in_memory_db: sqlite3.Connection,
) -> None:
    from finances.reports.monthly import build_report, render_table

    report = build_report(in_memory_db, month="2026-02")
    out = render_table(report)
    assert "Month" in out
    assert "Account" in out
    assert "USD" in out


def test_render_table_contains_row_values(
    in_memory_db: sqlite3.Connection,
) -> None:
    from finances.reports.monthly import build_report, render_table

    acc = _insert_account(in_memory_db, "Table Monthly", currency="USD")
    assert acc.id is not None
    _insert_txn(
        in_memory_db, acc.id, Decimal("-12.34"),
        occurred_at=_dt(2026, 2, 3), source_ref="t-1",
    )

    report = build_report(in_memory_db, month="2026-02")
    out = render_table(report)

    assert "2026-02" in out
    assert "Table Monthly" in out
    assert "-12.34" in out


# ---------------------------------------------------------------------------
# Failure-mode — input validation
# ---------------------------------------------------------------------------


def test_build_report_invalid_month_raises_value_error(
    in_memory_db: sqlite3.Connection,
) -> None:
    from finances.reports.monthly import build_report

    with pytest.raises(ValueError):
        build_report(in_memory_db, month="not-a-month")


def test_build_report_invalid_since_raises_value_error(
    in_memory_db: sqlite3.Connection,
) -> None:
    from finances.reports.monthly import build_report

    with pytest.raises(ValueError):
        build_report(in_memory_db, since="2026-13", until="2026-14")


def test_monthly_row_rejects_float_amount() -> None:
    from pydantic import ValidationError

    from finances.reports.monthly import MonthlyRow

    with pytest.raises(ValidationError):
        MonthlyRow(
            month="2026-02",
            account_id=1,
            account_name="A",
            category_id=None,
            category_name=None,
            kind="expense",
            tx_count=1,
            total_native=1.23,  # type: ignore[arg-type]
            currency="USD",
            total_usd=Decimal("0"),
            fallback_usd=Decimal("0"),
            needs_review_count=0,
        )


def test_monthly_row_extra_field_forbidden() -> None:
    from pydantic import ValidationError

    from finances.reports.monthly import MonthlyRow

    with pytest.raises(ValidationError):
        MonthlyRow(
            month="2026-02",
            account_id=1,
            account_name="A",
            category_id=None,
            category_name=None,
            kind="expense",
            tx_count=1,
            total_native=Decimal("0"),
            currency="USD",
            total_usd=Decimal("0"),
            fallback_usd=Decimal("0"),
            needs_review_count=0,
            sneaky="no",  # type: ignore[call-arg]
        )
