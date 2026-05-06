"""Tests for the /monthly pivot + chart + mobile single-month view (EPIC-023, Phase 2c).

Per rule-011 these land before the implementation. They cover:

* the desktop pivot HTML page renders with kind tabs + grid,
* the mobile-UA path swaps to monthly_mobile.html with chevron navigation,
* the ?layout= query parameter overrides the UA-based switch,
* default range = 6 months ending today,
* range presets (3m, ytd) resolve correctly,
* kind tabs filter the pivot rows,
* pivot rows sorted by row total desc; cells carry drill URLs;
  BCV fallback contributions land in fallback_usd not total_usd,
* chart series cap = top 5 + Other,
* chart fallback shadow series picks up BCV-derived rows,
* mobile view lists categories, has prev/next chevrons, pct sums to 1,
* JSON API and HTMX partials.
"""

from __future__ import annotations

import sqlite3
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

import pytest
from fastapi.testclient import TestClient

from finances.db.repos import accounts as accounts_repo
from finances.db.repos import categories as categories_repo
from finances.db.repos import transactions as transactions_repo
from finances.domain.models import (
    Account,
    AccountKind,
    Transaction,
    TransactionKind,
)


# ---------------------------------------------------------------------------
# Helpers — month arithmetic that mirrors the service layer.
# ---------------------------------------------------------------------------


def _month_str(d: date) -> str:
    return f"{d.year:04d}-{d.month:02d}"


def _shift_month(d: date, delta_months: int) -> date:
    """Return the first day of the month ``delta_months`` away from d."""
    total = d.year * 12 + (d.month - 1) + delta_months
    return date(total // 12, (total % 12) + 1, 1)


# ---------------------------------------------------------------------------
# Page render / smoke.
# ---------------------------------------------------------------------------


def test_monthly_page_renders_with_seeded_db_desktop(
    seeded_web_db: sqlite3.Connection,
    web_client_factory,
) -> None:
    client: TestClient = web_client_factory()
    resp = client.get("/monthly", headers={"User-Agent": "Mozilla/5.0 desktop"})
    assert resp.status_code == 200
    body = resp.text
    assert "Monthly" in body
    assert "<body" in body
    # Desktop pivot grid is present.
    assert "monthly-pivot" in body
    # Kind tabs are rendered (Expense / Income / Net).
    assert "data-kind-tab" in body


def test_monthly_page_renders_mobile_with_mobile_ua(
    seeded_web_db: sqlite3.Connection,
    web_client_factory,
) -> None:
    client = web_client_factory()
    resp = client.get(
        "/monthly",
        headers={"User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 16_0) Mobile/15E"},
    )
    assert resp.status_code == 200
    body = resp.text
    # Mobile view markers (chevron navigation + mobile container).
    assert "monthly-mobile" in body
    # No desktop pivot grid on mobile.
    assert "monthly-pivot" not in body


def test_layout_query_param_overrides_ua(
    seeded_web_db: sqlite3.Connection,
    web_client_factory,
) -> None:
    client = web_client_factory()
    # Mobile UA + layout=desktop → desktop.
    resp = client.get(
        "/monthly?layout=desktop",
        headers={"User-Agent": "iPhone Mobile/15E"},
    )
    assert resp.status_code == 200
    assert "monthly-pivot" in resp.text
    assert "monthly-mobile" not in resp.text

    # Desktop UA + layout=mobile → mobile.
    resp2 = client.get(
        "/monthly?layout=mobile",
        headers={"User-Agent": "Mozilla/5.0 desktop"},
    )
    assert resp2.status_code == 200
    assert "monthly-mobile" in resp2.text
    assert "monthly-pivot" not in resp2.text


# ---------------------------------------------------------------------------
# Range preset resolution.
# ---------------------------------------------------------------------------


def test_default_range_is_6_months(
    seeded_web_db: sqlite3.Connection,
    web_client_factory,
) -> None:
    """Default preset = 6m → 6 months ending current calendar month."""
    from finances.web.services.monthly_view import (
        MonthlyFilter,
        resolve_month_range,
    )

    today = date.today()
    f = MonthlyFilter()
    since, until = resolve_month_range(f, today=today)
    assert until == _month_str(today)
    expected_since = _shift_month(today, -5)
    assert since == _month_str(expected_since)


def test_range_preset_3m_resolves_correctly() -> None:
    from finances.web.services.monthly_view import (
        MonthlyFilter,
        MonthlyRangePreset,
        resolve_month_range,
    )

    today = date(2026, 5, 15)
    f = MonthlyFilter(range_preset=MonthlyRangePreset.M3)
    since, until = resolve_month_range(f, today=today)
    assert since == "2026-03"
    assert until == "2026-05"


def test_range_preset_ytd_starts_at_january() -> None:
    from finances.web.services.monthly_view import (
        MonthlyFilter,
        MonthlyRangePreset,
        resolve_month_range,
    )

    today = date(2026, 5, 15)
    f = MonthlyFilter(range_preset=MonthlyRangePreset.YTD)
    since, until = resolve_month_range(f, today=today)
    assert since == "2026-01"
    assert until == "2026-05"


# ---------------------------------------------------------------------------
# Kind filtering.
# ---------------------------------------------------------------------------


def test_kind_expense_excludes_income_rows(
    seeded_web_db: sqlite3.Connection,
) -> None:
    from finances.web.services.monthly_view import (
        MonthlyFilter,
        MonthlyKind,
        build_pivot,
    )

    f = MonthlyFilter(kind=MonthlyKind.EXPENSE)
    pivot = build_pivot(seeded_web_db, f, today=date.today())
    # Salary is income-only in the seeded set; should be absent.
    names = [r.category_name for r in pivot.rows]
    assert "Salary" not in names


def test_kind_income_excludes_expense(
    seeded_web_db: sqlite3.Connection,
) -> None:
    from finances.web.services.monthly_view import (
        MonthlyFilter,
        MonthlyKind,
        build_pivot,
    )

    f = MonthlyFilter(kind=MonthlyKind.INCOME)
    pivot = build_pivot(seeded_web_db, f, today=date.today())
    names = [r.category_name for r in pivot.rows]
    # Groceries is expense-only; should be absent. Salary should be present.
    assert "Groceries" not in names
    assert "Salary" in names


def test_kind_net_combines_income_minus_expense(
    seeded_web_db: sqlite3.Connection,
) -> None:
    from finances.web.services.monthly_view import (
        MonthlyFilter,
        MonthlyKind,
        build_pivot,
    )

    f_exp = MonthlyFilter(kind=MonthlyKind.EXPENSE)
    f_inc = MonthlyFilter(kind=MonthlyKind.INCOME)
    f_net = MonthlyFilter(kind=MonthlyKind.NET)

    today = date.today()
    p_exp = build_pivot(seeded_web_db, f_exp, today=today)
    p_inc = build_pivot(seeded_web_db, f_inc, today=today)
    p_net = build_pivot(seeded_web_db, f_net, today=today)

    # Net grand_total_usd = income - expense.
    assert p_net.totals.grand_total_usd == (
        p_inc.totals.grand_total_usd - p_exp.totals.grand_total_usd
    )


# ---------------------------------------------------------------------------
# Pivot ordering + drill URLs + BCV fallback overlay.
# ---------------------------------------------------------------------------


def test_pivot_categories_sorted_by_row_total_desc(
    seeded_web_db: sqlite3.Connection,
) -> None:
    from finances.web.services.monthly_view import (
        MonthlyFilter,
        MonthlyKind,
        build_pivot,
    )

    f = MonthlyFilter(kind=MonthlyKind.EXPENSE)
    pivot = build_pivot(seeded_web_db, f, today=date.today())
    totals = [abs(r.row_total_usd) for r in pivot.rows]
    for a, b in zip(totals, totals[1:]):
        assert a >= b


def test_pivot_cells_carry_drill_url_with_correct_filters(
    seeded_web_db: sqlite3.Connection,
) -> None:
    from finances.web.services.monthly_view import (
        MonthlyFilter,
        MonthlyKind,
        build_pivot,
    )

    f = MonthlyFilter(kind=MonthlyKind.EXPENSE)
    pivot = build_pivot(seeded_web_db, f, today=date.today())
    # Find any non-empty cell.
    found = False
    for row in pivot.rows:
        for month, cell in row.cells_by_month.items():
            if cell.tx_count > 0:
                url = cell.drill_url
                assert url.startswith("/transactions?")
                assert "date_from=" in url
                assert "date_to=" in url
                assert "kinds=expense" in url
                # Category name must round-trip in URL when present.
                if row.category_name is not None:
                    assert "categories=" in url
                # Month must appear in either date_from or date_to.
                assert month.split("-")[0] in url and month.split("-")[1] in url
                found = True
                break
        if found:
            break
    assert found, "expected at least one non-empty pivot cell"


def test_pivot_excludes_bcv_from_total_usd(
    web_db: sqlite3.Connection,
) -> None:
    """BCV-sourced contributions go to fallback_usd, not total_usd.

    We seed a single VES expense whose only available rate is BCV and
    assert the row's total_usd is 0 while fallback_usd is non-zero.
    """
    from finances.db.repos import rates as rates_repo
    from finances.domain.models import Rate
    from finances.web.services.monthly_view import (
        MonthlyFilter,
        MonthlyKind,
        build_pivot,
    )

    today = datetime.now(tz=UTC)

    # Account, category.
    prov = accounts_repo.insert(
        web_db,
        Account(
            name="Provincial",
            kind=AccountKind.BANK,
            currency="VES",
            institution="Provincial",
        ),
    )
    food = categories_repo.get_by_name(web_db, TransactionKind.EXPENSE, "Groceries")
    assert food is not None

    # Only a BCV rate exists.
    rates_repo.upsert(
        web_db,
        Rate(
            as_of_date=today.date(),
            base="USD",
            quote="VES",
            rate=Decimal("36.10"),
            source="bcv",
        ),
    )

    transactions_repo.insert(
        web_db,
        Transaction(
            account_id=prov.id,
            occurred_at=today,
            kind=TransactionKind.EXPENSE,
            amount=Decimal("3610.00"),
            currency="VES",
            description="bcv-only expense",
            category_id=food.id,
            source="provincial",
            source_ref="bcv-only-1",
        ),
    )

    f = MonthlyFilter(kind=MonthlyKind.EXPENSE)
    pivot = build_pivot(web_db, f, today=today.date())
    matching = [r for r in pivot.rows if r.category_name == "Groceries"]
    assert matching, "expected a Groceries row"
    row = matching[0]
    assert row.row_total_usd == Decimal("0")
    assert row.row_fallback_usd > Decimal("0")
    # The cell for the current month carries the same property.
    cur = _month_str(today.date())
    cell = row.cells_by_month[cur]
    assert cell.total_usd == Decimal("0")
    assert cell.fallback_usd > Decimal("0")


def test_pivot_bcv_fallback_overlay_marker_present(
    web_db: sqlite3.Connection,
    web_client_factory,
) -> None:
    """Cell with BCV-only contribution renders the overlay marker class."""
    from finances.db.repos import rates as rates_repo
    from finances.domain.models import Rate

    today = datetime.now(tz=UTC)
    prov = accounts_repo.insert(
        web_db,
        Account(
            name="Provincial",
            kind=AccountKind.BANK,
            currency="VES",
            institution="Provincial",
        ),
    )
    food = categories_repo.get_by_name(web_db, TransactionKind.EXPENSE, "Groceries")
    assert food is not None
    rates_repo.upsert(
        web_db,
        Rate(
            as_of_date=today.date(),
            base="USD",
            quote="VES",
            rate=Decimal("36.10"),
            source="bcv",
        ),
    )
    transactions_repo.insert(
        web_db,
        Transaction(
            account_id=prov.id,
            occurred_at=today,
            kind=TransactionKind.EXPENSE,
            amount=Decimal("3610.00"),
            currency="VES",
            description="bcv-only expense",
            category_id=food.id,
            source="provincial",
            source_ref="bcv-only-2",
        ),
    )

    client = web_client_factory()
    resp = client.get(
        "/monthly?layout=desktop&range_preset=3m",
        headers={"User-Agent": "desktop"},
    )
    assert resp.status_code == 200
    body = resp.text
    # The cell (or row) is annotated with an overlay marker when fallback > 0.
    assert "data-bcv-fallback" in body


# ---------------------------------------------------------------------------
# Chart series.
# ---------------------------------------------------------------------------


def test_chart_series_top_5_plus_other(
    web_db: sqlite3.Connection,
) -> None:
    """7+ expense categories → chart yields 6 series (top 5 + Other)."""
    from finances.db.repos import rates as rates_repo
    from finances.domain.models import Rate
    from finances.web.services.monthly_view import (
        MonthlyFilter,
        MonthlyKind,
        build_chart,
    )

    today = datetime.now(tz=UTC)

    cash = accounts_repo.insert(
        web_db,
        Account(name="Cash USD", kind=AccountKind.CASH, currency="USD"),
    )

    # Make sure rates are not relevant here — using USD account, native_usd path.
    # 7 distinct expense categories, descending amounts.
    cat_names = [
        "Groceries",
        "Transport",
        "Utilities",
        "Entertainment",
        "Health",
        "Restaurants",
        "Other Expense",
    ]
    found_cats = []
    for name in cat_names:
        c = categories_repo.get_by_name(web_db, TransactionKind.EXPENSE, name)
        if c is not None:
            found_cats.append(c)

    # If fewer than 7 default categories exist for expense, supplement
    # with uncategorized rows (None category_id) — the dimensionality
    # test still holds because each will be its own row.
    amount = Decimal("100.00")
    for i, cat in enumerate(found_cats[:7]):
        transactions_repo.insert(
            web_db,
            Transaction(
                account_id=cash.id,
                occurred_at=today,
                kind=TransactionKind.EXPENSE,
                amount=amount,
                currency="USD",
                description=f"row-{i}",
                category_id=cat.id,
                source="cash_cli",
                source_ref=f"chart-{i}",
            ),
        )
        amount -= Decimal("10")

    # If we don't have 7 distinct categories, also add uncategorized
    # entries to get over the top-5 boundary.
    needed = 7 - len(found_cats)
    for j in range(needed):
        transactions_repo.insert(
            web_db,
            Transaction(
                account_id=cash.id,
                occurred_at=today,
                kind=TransactionKind.EXPENSE,
                amount=Decimal("5.00") + Decimal(j),
                currency="USD",
                description=f"uncategorized-{j}",
                source="cash_cli",
                source_ref=f"chart-uncat-{j}",
            ),
        )

    f = MonthlyFilter(kind=MonthlyKind.EXPENSE)
    chart = build_chart(web_db, f, today=today.date())
    assert len(chart.series) == 6
    assert chart.series[-1].category == "Other"


def test_chart_fallback_per_month_shadow(
    web_db: sqlite3.Connection,
) -> None:
    """A BCV-only seeded txn shows up in fallback_per_month for its month."""
    from finances.db.repos import rates as rates_repo
    from finances.domain.models import Rate
    from finances.web.services.monthly_view import (
        MonthlyFilter,
        MonthlyKind,
        build_chart,
    )

    today = datetime.now(tz=UTC)
    prov = accounts_repo.insert(
        web_db,
        Account(
            name="Provincial",
            kind=AccountKind.BANK,
            currency="VES",
            institution="Provincial",
        ),
    )
    food = categories_repo.get_by_name(web_db, TransactionKind.EXPENSE, "Groceries")
    assert food is not None
    rates_repo.upsert(
        web_db,
        Rate(
            as_of_date=today.date(),
            base="USD",
            quote="VES",
            rate=Decimal("36.10"),
            source="bcv",
        ),
    )
    transactions_repo.insert(
        web_db,
        Transaction(
            account_id=prov.id,
            occurred_at=today,
            kind=TransactionKind.EXPENSE,
            amount=Decimal("3610.00"),
            currency="VES",
            description="bcv shadow",
            category_id=food.id,
            source="provincial",
            source_ref="shadow-1",
        ),
    )

    f = MonthlyFilter(kind=MonthlyKind.EXPENSE)
    chart = build_chart(web_db, f, today=today.date())
    cur = _month_str(today.date())
    assert cur in chart.months
    idx = chart.months.index(cur)
    assert chart.fallback_per_month[idx] > Decimal("0")


# ---------------------------------------------------------------------------
# Mobile view.
# ---------------------------------------------------------------------------


def test_mobile_view_lists_categories_for_current_month(
    seeded_web_db: sqlite3.Connection,
) -> None:
    from finances.web.services.monthly_view import (
        MonthlyFilter,
        build_mobile,
    )

    today = date.today()
    f = MonthlyFilter()
    mobile = build_mobile(seeded_web_db, f, today=today, month=_month_str(today))
    assert mobile.month == _month_str(today)
    # Seeded set has at least one expense category for current month.
    assert len(mobile.categories) >= 1


def test_mobile_view_chevrons_navigate_months(
    seeded_web_db: sqlite3.Connection,
) -> None:
    from finances.web.services.monthly_view import (
        MonthlyFilter,
        build_mobile,
    )

    today = date(2026, 5, 15)
    f = MonthlyFilter()
    mobile = build_mobile(seeded_web_db, f, today=today, month="2026-05")
    assert mobile.prev_month == "2026-04"
    assert mobile.next_month == "2026-06"

    # Year wrap.
    mobile2 = build_mobile(seeded_web_db, f, today=today, month="2026-01")
    assert mobile2.prev_month == "2025-12"
    assert mobile2.next_month == "2026-02"


def test_mobile_view_pct_of_month_sums_to_one(
    seeded_web_db: sqlite3.Connection,
) -> None:
    from finances.web.services.monthly_view import (
        MonthlyFilter,
        build_mobile,
    )

    today = date.today()
    f = MonthlyFilter()
    mobile = build_mobile(seeded_web_db, f, today=today, month=_month_str(today))
    if mobile.categories:
        total_pct = sum(c.pct_of_month for c in mobile.categories)
        assert abs(total_pct - 1.0) < 0.001
    else:
        # No expense rows means no pct entries, nothing to assert.
        assert mobile.month_total_usd == Decimal("0")


# ---------------------------------------------------------------------------
# JSON API + HTMX partials.
# ---------------------------------------------------------------------------


def test_api_monthly_endpoint_returns_pivot_json(
    seeded_web_db: sqlite3.Connection,
    web_client_factory,
) -> None:
    client = web_client_factory()
    resp = client.get("/api/monthly?range_preset=6m")
    assert resp.status_code == 200
    payload = resp.json()
    for key in ("months", "rows", "totals", "filter"):
        assert key in payload, f"missing key: {key}"
    assert isinstance(payload["months"], list)
    assert isinstance(payload["rows"], list)


def test_htmx_partial_pivot_no_full_html(
    seeded_web_db: sqlite3.Connection,
    web_client_factory,
) -> None:
    client = web_client_factory()
    resp = client.get(
        "/_partial/monthly/pivot?range_preset=3m",
        headers={"HX-Request": "true"},
    )
    assert resp.status_code == 200
    body = resp.text
    assert "<html" not in body.lower()
    assert "<body" not in body.lower()
    # The pivot grid marker must still be present.
    assert "monthly-pivot" in body
