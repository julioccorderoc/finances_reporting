"""Tests for the static HTML report export (Thing 4 — ``finances html``).

Per rule-011 these land before the implementation. The export service is a
*projection* over the existing viewer query services; its whole reason to
exist is that "report numbers == viewer numbers by construction". So the
tests assert exactly that equality against:

* :func:`finances.web.services.net_worth.compute_net_worth` (header net worth),
* :func:`finances.web.services.monthly_view.build_mobile` (category breakdown),
* :func:`finances.web.services.rates_view.build_rates_chart` (rate trend),
* :func:`finances.web.services.dashboard.build_recent_activity` (recent list),
* :func:`finances.reports.monthly.build_report` (12-month flows + needs_review).

Plus the standalone-file guarantees from the plan gate:

* the rendered HTML contains **no** external ``http(s)://`` URLs,
* it is a valid, self-contained document (doctype, inlined Chart.js, embedded
  JSON) that uses card-rows, never ``<table>``,
* :func:`export_html` writes one file and never mutates the DB.
"""

from __future__ import annotations

import re
import sqlite3
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

import pytest

from finances.db.repos import accounts as accounts_repo
from finances.db.repos import transactions as transactions_repo
from finances.domain.models import Account, AccountKind, Transaction, TransactionKind
from finances.reports import html_export
from finances.reports import monthly as monthly_report
from finances.web.services.dashboard import build_recent_activity
from finances.web.services.monthly_view import (
    MonthlyFilter,
    MonthlyKind,
    build_mobile,
)
from finances.web.services.net_worth import compute_net_worth
from finances.web.services.rates_view import build_rates_chart

_URL_RE = re.compile(r"https?://")


@pytest.fixture
def now() -> datetime:
    return datetime.now(tz=UTC)


def _uncat(label: str | None) -> str:
    return "Uncategorized" if label is None else label


# ---------------------------------------------------------------------------
# build_report_context — numbers equal the viewer services.
# ---------------------------------------------------------------------------


def test_net_worth_matches_net_worth_service(
    seeded_web_db: sqlite3.Connection, now: datetime
) -> None:
    ctx = html_export.build_report_context(seeded_web_db, now=now)
    expected = compute_net_worth(seeded_web_db, as_of_date=now.date())
    assert ctx.net_worth_usd == expected.total_usdt
    # Per-account list carries the same accounts + USDT contributions.
    by_name = {a.name: a for a in ctx.accounts}
    for c in expected.contributions:
        assert c.account_name in by_name
        assert by_name[c.account_name].balance_usdt == c.contribution_usdt
        assert by_name[c.account_name].balance_native == c.balance_native


def test_needs_review_count_matches_query(
    seeded_web_db: sqlite3.Connection, now: datetime
) -> None:
    ctx = html_export.build_report_context(seeded_web_db, now=now)
    expected = int(
        seeded_web_db.execute(
            "SELECT COUNT(*) AS c FROM transactions WHERE needs_review = 1"
        ).fetchone()["c"]
    )
    assert ctx.needs_review_count == expected
    # The seeded fixture plants exactly one needs_review row.
    assert expected == 1


def test_current_month_categories_match_build_mobile(
    seeded_web_db: sqlite3.Connection, now: datetime
) -> None:
    month = now.strftime("%Y-%m")
    ctx = html_export.build_report_context(seeded_web_db, now=now)
    mobile = build_mobile(
        seeded_web_db,
        MonthlyFilter(kind=MonthlyKind.EXPENSE),
        month=month,
    )
    expected = {(_uncat(c.category_name), c.total_usd) for c in mobile.categories}
    got = {(c.category, c.total_usd) for c in ctx.category_current}
    assert got == expected
    assert ctx.current_month == month


def test_rate_trend_matches_rates_view(
    seeded_web_db: sqlite3.Connection, now: datetime
) -> None:
    ctx = html_export.build_report_context(seeded_web_db, now=now)
    chart = build_rates_chart(seeded_web_db, range_days=90)
    expected = {
        s.label: [(p.as_of_date, p.rate) for p in s.points] for s in chart.series
    }
    got = {s.label: [(p.date, p.rate) for p in s.points] for s in ctx.rate_trend}
    assert got == expected


def test_recent_matches_recent_activity(
    seeded_web_db: sqlite3.Connection, now: datetime
) -> None:
    ctx = html_export.build_report_context(seeded_web_db, now=now)
    expected = build_recent_activity(seeded_web_db, limit=html_export.RECENT_LIMIT)
    assert len(ctx.recent) == len(expected)
    assert [r.description for r in ctx.recent] == [c.description for c in expected]
    assert [r.amount_usd for r in ctx.recent] == [c.amount_usd for c in expected]


def test_monthly_flows_span_12_months_and_net_is_income_plus_signed_expense(
    seeded_web_db: sqlite3.Connection, now: datetime
) -> None:
    ctx = html_export.build_report_context(seeded_web_db, now=now)
    assert len(ctx.monthly_flows) == 12
    assert ctx.monthly_flows[-1].month == now.strftime("%Y-%m")
    # ``total_usd`` is *signed* (expenses negative on real data), so net is the
    # signed sum, never income - expense (that would double-negate expenses).
    for flow in ctx.monthly_flows:
        assert flow.net_usd == flow.income_usd + flow.expense_usd

    # Current-month income/expense equal the canonical monthly report totals.
    month = now.strftime("%Y-%m")
    report = monthly_report.build_report(seeded_web_db, month=month)
    exp_income = sum(
        (r.total_usd for r in report.rows if r.kind == "income"), Decimal("0")
    )
    exp_expense = sum(
        (r.total_usd for r in report.rows if r.kind == "expense"), Decimal("0")
    )
    current = ctx.monthly_flows[-1]
    assert current.income_usd == exp_income
    assert current.expense_usd == exp_expense


def test_monthly_flow_net_reflects_real_negative_expense_sign(
    web_db: sqlite3.Connection, now: datetime
) -> None:
    """Regression: with the real convention (expenses stored negative) the
    current-month net must be a loss, not a spurious gain."""
    acct = accounts_repo.insert(
        web_db, Account(name="Cash USD", kind=AccountKind.CASH, currency="USD")
    )
    transactions_repo.insert(
        web_db,
        Transaction(
            account_id=acct.id,
            occurred_at=now,
            kind=TransactionKind.INCOME,
            amount=Decimal("100.00"),
            currency="USD",
            description="paycheck",
            source="cash_cli",
            source_ref="inc-1",
        ),
    )
    transactions_repo.insert(
        web_db,
        Transaction(
            account_id=acct.id,
            occurred_at=now,
            kind=TransactionKind.EXPENSE,
            amount=Decimal("-30.00"),  # money out is negative (real convention)
            currency="USD",
            description="dinner",
            source="cash_cli",
            source_ref="exp-1",
        ),
    )
    ctx = html_export.build_report_context(web_db, now=now)
    current = ctx.monthly_flows[-1]
    assert current.income_usd == Decimal("100.00")
    assert current.expense_usd == Decimal("-30.00")
    assert current.net_usd == Decimal("70.00")  # 100 + (-30), a $70 surplus


# ---------------------------------------------------------------------------
# Freshness (last transaction date per source; stale > 10 days).
# ---------------------------------------------------------------------------


def test_freshness_flags_stale_source(
    web_db: sqlite3.Connection, now: datetime
) -> None:
    acct = accounts_repo.insert(
        web_db,
        Account(name="Old Bank", kind=AccountKind.BANK, currency="USD"),
    )
    fresh = accounts_repo.insert(
        web_db,
        Account(name="New Cash", kind=AccountKind.CASH, currency="USD"),
    )
    transactions_repo.insert(
        web_db,
        Transaction(
            account_id=acct.id,
            occurred_at=now - timedelta(days=30),
            kind=TransactionKind.EXPENSE,
            amount=Decimal("5.00"),
            currency="USD",
            description="stale one",
            source="stalesrc",
            source_ref="stale-1",
        ),
    )
    transactions_repo.insert(
        web_db,
        Transaction(
            account_id=fresh.id,
            occurred_at=now,
            kind=TransactionKind.EXPENSE,
            amount=Decimal("2.00"),
            currency="USD",
            description="fresh one",
            source="freshsrc",
            source_ref="fresh-1",
        ),
    )

    ctx = html_export.build_report_context(web_db, now=now)
    by_source = {f.source: f for f in ctx.freshness}
    assert by_source["stalesrc"].stale is True
    assert by_source["freshsrc"].stale is False
    assert "stalesrc" in ctx.stale_sources
    assert "freshsrc" not in ctx.stale_sources


# ---------------------------------------------------------------------------
# render_html — standalone, no external URLs, card-rows not tables.
# ---------------------------------------------------------------------------


def test_render_html_has_no_external_urls(
    seeded_web_db: sqlite3.Connection, now: datetime
) -> None:
    ctx = html_export.build_report_context(seeded_web_db, now=now)
    html = html_export.render_html(ctx, chartjs_source="/* stub */\nconsole.log(1);")
    assert not _URL_RE.search(html), "rendered report must contain no http(s):// URLs"


def test_render_html_is_valid_standalone_document(
    seeded_web_db: sqlite3.Connection, now: datetime
) -> None:
    ctx = html_export.build_report_context(seeded_web_db, now=now)
    marker = "CHARTJS_STUB_MARKER_42"
    html = html_export.render_html(ctx, chartjs_source=f"/* {marker} */")
    lower = html.lower()
    assert "<!doctype html" in lower
    assert "<html" in lower and "</html>" in lower
    assert "<script" in lower
    # Chart.js is inlined (the stub body appears), not linked.
    assert marker in html
    assert 'src="/static' not in html
    assert "cdn." not in html
    # Data is embedded as JSON for the charts.
    assert "REPORT_DATA" in html
    # Standing owner rule: card-rows, never <table> for data lists.
    assert "<table" not in lower


def test_render_html_escapes_description_script_breakout(
    web_db: sqlite3.Connection, now: datetime
) -> None:
    """A malicious/edge description must not break out of the data <script>."""
    acct = accounts_repo.insert(
        web_db, Account(name="X", kind=AccountKind.CASH, currency="USD")
    )
    transactions_repo.insert(
        web_db,
        Transaction(
            account_id=acct.id,
            occurred_at=now,
            kind=TransactionKind.EXPENSE,
            amount=Decimal("1.00"),
            currency="USD",
            description="</script><script>alert(1)</script>",
            source="cash_cli",
            source_ref="xss-1",
        ),
    )
    ctx = html_export.build_report_context(web_db, now=now)
    html = html_export.render_html(ctx, chartjs_source="/* stub */")
    # The raw closing tag must never appear verbatim inside embedded JSON.
    assert "</script><script>alert(1)</script>" not in html


# ---------------------------------------------------------------------------
# export_html — writes one file, read-only wrt the DB, real Chart.js inlined.
# ---------------------------------------------------------------------------


def test_export_html_writes_file_readonly_and_url_free(
    seeded_web_db: sqlite3.Connection, tmp_path, now: datetime
) -> None:
    before = int(
        seeded_web_db.execute("SELECT COUNT(*) AS c FROM transactions").fetchone()["c"]
    )
    out = tmp_path / "report.html"
    result = html_export.export_html(seeded_web_db, out, now=now)

    assert result == out
    assert out.exists() and out.stat().st_size > 0
    content = out.read_text(encoding="utf-8")
    assert "<!doctype html" in content.lower()
    # Real vendored Chart.js is inlined but its license-banner URLs stripped.
    assert _URL_RE.search(content) is None
    assert "Chart" in content  # the real library symbol is present

    after = int(
        seeded_web_db.execute("SELECT COUNT(*) AS c FROM transactions").fetchone()["c"]
    )
    assert after == before  # export path never mutates the DB
