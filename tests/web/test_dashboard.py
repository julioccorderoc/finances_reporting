"""Tests for the / dashboard page (EPIC-023, Phase 2a).

Per rule-011 these land before the implementation. They cover:

* the dashboard page renders with seeded data,
* KPI tiles (net worth, this-month spend/income, needs review),
* net-worth math: native USD pass-through, P2P-only resolution per
  ADR-005 amendment, never falls back to BCV, missing-pair warning,
* sync status strip — chips per source, severity rules,
* recent activity uses canonical card_transaction.html partial,
* 6-month spend trend stacked-bar chart data shape,
* HTMX partial for the sync strip returns no full HTML shell.
"""

from __future__ import annotations

import sqlite3
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from fastapi.testclient import TestClient

from finances.db.repos import accounts as accounts_repo
from finances.db.repos import categories as categories_repo
from finances.db.repos import import_state as import_state_repo
from finances.db.repos import rates as rates_repo
from finances.db.repos import transactions as transactions_repo
from finances.domain.models import (
    Account,
    AccountKind,
    Rate,
    Transaction,
    TransactionKind,
)


# ---------------------------------------------------------------------------
# Page render / smoke.
# ---------------------------------------------------------------------------


def test_dashboard_renders_with_seeded_db(
    seeded_web_db: sqlite3.Connection,
    web_client_factory,
) -> None:
    client: TestClient = web_client_factory()
    resp = client.get("/")
    assert resp.status_code == 200
    body = resp.text
    assert "<body" in body
    # Each KPI label appears.
    assert "Net worth" in body
    assert "This month spend" in body
    assert "This month income" in body
    assert "Needs review" in body
    # Recent activity heading.
    assert "Recent activity" in body


def test_kpi_tiles_endpoint_returns_expected_shape(
    seeded_web_db: sqlite3.Connection,
    web_client_factory,
) -> None:
    client = web_client_factory()
    resp = client.get("/api/dashboard/kpis")
    assert resp.status_code == 200
    payload = resp.json()
    for key in ("net_worth", "month_spend", "month_income", "needs_review"):
        assert key in payload, f"missing key: {key}"
        tile = payload[key]
        for field in ("label", "value", "hint", "severity"):
            assert field in tile, f"missing field {field} in {key}"


# ---------------------------------------------------------------------------
# Net worth — math.
# ---------------------------------------------------------------------------


def test_net_worth_aggregates_usd_accounts_one_to_one(
    seeded_web_db: sqlite3.Connection,
    web_client_factory,
) -> None:
    """Cash USD account balance contributes 1:1 (no rate lookup)."""
    from datetime import datetime as _dt

    from finances.web.services.net_worth import compute_net_worth

    nw = compute_net_worth(seeded_web_db, as_of_date=_dt.now(tz=UTC).date())
    # The seeded "Cash USD" has one row with amount=12.50 (sign convention
    # in the fixture stores positive magnitudes). Balance = SUM(amount) = 12.50.
    contribs = {(c.account_name, c.currency): c for c in nw.contributions}
    cash = contribs[("Cash USD", "USD")]
    assert cash.balance_native == Decimal("12.5")
    # USD account: rate is exactly 1, contribution equals balance.
    assert cash.rate_to_usdt == Decimal("1")
    assert cash.contribution_usdt == Decimal("12.5")


def test_net_worth_uses_p2p_for_ves_account(
    seeded_web_db: sqlite3.Connection,
    web_client_factory,
) -> None:
    """VES Provincial balance reflects USD value via P2P rate."""
    from datetime import datetime as _dt

    from finances.web.services.net_worth import compute_net_worth

    today = _dt.now(tz=UTC).date()
    nw = compute_net_worth(seeded_web_db, as_of_date=today)
    contribs = {(c.account_name, c.currency): c for c in nw.contributions}
    prov = contribs[("Provincial", "VES")]
    # P2P median is 36.50 VES per USDT (seeded).
    assert prov.rate_to_usdt == Decimal("36.50")
    expected_usdt = prov.balance_native / Decimal("36.50")
    assert prov.contribution_usdt == expected_usdt
    # No missing pair recorded for VES on this fixture.
    assert "VES→USDT" not in nw.missing_pairs


def test_net_worth_warns_when_p2p_missing(
    web_db: sqlite3.Connection,
    web_client_factory,
) -> None:
    """VES balance with NO P2P rate → contribution is None, missing_pairs has VES→USDT."""
    from datetime import datetime as _dt

    from finances.web.services.net_worth import compute_net_worth

    today = _dt.now(tz=UTC)

    provincial = accounts_repo.insert(
        web_db,
        Account(name="Provincial", kind=AccountKind.BANK, currency="VES"),
    )
    food = categories_repo.get_by_name(web_db, TransactionKind.EXPENSE, "Groceries")
    assert food is not None
    transactions_repo.insert(
        web_db,
        Transaction(
            account_id=provincial.id,
            occurred_at=today,
            kind=TransactionKind.EXPENSE,
            amount=Decimal("-100.00"),
            currency="VES",
            description="probe",
            category_id=food.id,
            source="provincial",
            source_ref="probe",
        ),
    )

    nw = compute_net_worth(web_db, as_of_date=today.date())
    contribs = {c.account_name: c for c in nw.contributions}
    prov = contribs["Provincial"]
    assert prov.rate_to_usdt is None
    assert prov.contribution_usdt is None
    assert "VES→USDT" in nw.missing_pairs

    # API tile reflects warning severity.
    client = web_client_factory()
    resp = client.get("/api/dashboard/kpis")
    payload = resp.json()
    assert payload["net_worth"]["severity"] == "warning"


def test_net_worth_never_uses_bcv(
    web_db: sqlite3.Connection,
    web_client_factory,
) -> None:
    """If only a BCV rate exists for VES, the resolver does NOT use it for net worth."""
    from datetime import datetime as _dt

    from finances.web.services.net_worth import compute_net_worth

    today = _dt.now(tz=UTC)
    provincial = accounts_repo.insert(
        web_db,
        Account(name="Provincial", kind=AccountKind.BANK, currency="VES"),
    )
    food = categories_repo.get_by_name(web_db, TransactionKind.EXPENSE, "Groceries")
    assert food is not None

    rates_repo.upsert(
        web_db,
        Rate(
            as_of_date=today.date(),
            base="USD",
            quote="VES",
            rate=Decimal("100.0"),
            source="bcv",
        ),
    )
    transactions_repo.insert(
        web_db,
        Transaction(
            account_id=provincial.id,
            occurred_at=today,
            kind=TransactionKind.EXPENSE,
            amount=Decimal("-1000.00"),
            currency="VES",
            description="probe",
            category_id=food.id,
            source="provincial",
            source_ref="bcv-only",
        ),
    )

    nw = compute_net_worth(web_db, as_of_date=today.date())
    prov = next(c for c in nw.contributions if c.account_name == "Provincial")
    # Even though BCV rate exists, contribution must be None — never BCV.
    assert prov.rate_to_usdt is None
    assert prov.contribution_usdt is None
    assert "VES→USDT" in nw.missing_pairs


# ---------------------------------------------------------------------------
# Month spend / income.
# ---------------------------------------------------------------------------


def test_month_spend_includes_only_current_calendar_month(
    web_db: sqlite3.Connection,
    web_client_factory,
) -> None:
    """Transactions from a previous month must NOT contribute to the headline tile."""
    today = datetime.now(tz=UTC)
    # First day of current month at noon to avoid edge cases.
    cur_month_dt = today.replace(day=1, hour=12, minute=0, second=0, microsecond=0)
    # Previous month — pick last day of previous month.
    prev_month_dt = cur_month_dt - timedelta(days=1)

    cash = accounts_repo.insert(
        web_db,
        Account(name="Cash USD", kind=AccountKind.CASH, currency="USD"),
    )
    food = categories_repo.get_by_name(web_db, TransactionKind.EXPENSE, "Groceries")
    assert food is not None

    # Current-month expense: $25.
    transactions_repo.insert(
        web_db,
        Transaction(
            account_id=cash.id,
            occurred_at=cur_month_dt,
            kind=TransactionKind.EXPENSE,
            amount=Decimal("-25.00"),
            currency="USD",
            description="this month",
            category_id=food.id,
            source="cash_cli",
            source_ref="this",
        ),
    )
    # Previous-month expense: $999 (must be excluded).
    transactions_repo.insert(
        web_db,
        Transaction(
            account_id=cash.id,
            occurred_at=prev_month_dt,
            kind=TransactionKind.EXPENSE,
            amount=Decimal("-999.00"),
            currency="USD",
            description="last month",
            category_id=food.id,
            source="cash_cli",
            source_ref="prev",
        ),
    )

    client = web_client_factory()
    resp = client.get("/api/dashboard/kpis")
    payload = resp.json()
    # Spend tile value is a formatted string. We assert "25" appears and "999" does not.
    spend_value = payload["month_spend"]["value"]
    assert "25" in spend_value
    assert "999" not in spend_value


def test_month_spend_excludes_bcv_fallback(
    web_db: sqlite3.Connection,
    web_client_factory,
) -> None:
    """A VES expense whose only rate is BCV should NOT count toward the spend headline."""
    today = datetime.now(tz=UTC)
    cur_month_dt = today.replace(day=1, hour=12, minute=0, second=0, microsecond=0)

    provincial = accounts_repo.insert(
        web_db,
        Account(name="Provincial", kind=AccountKind.BANK, currency="VES"),
    )
    food = categories_repo.get_by_name(web_db, TransactionKind.EXPENSE, "Groceries")
    assert food is not None

    # ONLY BCV rate, no P2P.
    rates_repo.upsert(
        web_db,
        Rate(
            as_of_date=cur_month_dt.date(),
            base="USD",
            quote="VES",
            rate=Decimal("100.0"),
            source="bcv",
        ),
    )
    transactions_repo.insert(
        web_db,
        Transaction(
            account_id=provincial.id,
            occurred_at=cur_month_dt,
            kind=TransactionKind.EXPENSE,
            amount=Decimal("-100000.00"),
            currency="VES",
            description="bcv-only expense",
            category_id=food.id,
            source="provincial",
            source_ref="bcv-only",
        ),
    )

    client = web_client_factory()
    resp = client.get("/api/dashboard/kpis")
    payload = resp.json()
    # 100000 / 100 = $1000. Headline must NOT show 1000.
    assert "1000" not in payload["month_spend"]["value"]


def test_needs_review_count(
    web_db: sqlite3.Connection,
    web_client_factory,
) -> None:
    """The needs-review tile reflects the number of needs_review=1 rows."""
    today = datetime.now(tz=UTC)
    cash = accounts_repo.insert(
        web_db,
        Account(name="Cash USD", kind=AccountKind.CASH, currency="USD"),
    )
    food = categories_repo.get_by_name(web_db, TransactionKind.EXPENSE, "Groceries")
    assert food is not None
    n = 3
    for i in range(n):
        transactions_repo.insert(
            web_db,
            Transaction(
                account_id=cash.id,
                occurred_at=today,
                kind=TransactionKind.EXPENSE,
                amount=Decimal("-1.00"),
                currency="USD",
                description=f"nr-{i}",
                category_id=food.id,
                source="cash_cli",
                source_ref=f"nr-{i}",
                needs_review=True,
            ),
        )

    client = web_client_factory()
    resp = client.get("/api/dashboard/kpis")
    payload = resp.json()
    assert str(n) in payload["needs_review"]["value"]
    assert payload["needs_review"]["severity"] == "alert"


# ---------------------------------------------------------------------------
# Sync status strip.
# ---------------------------------------------------------------------------


def test_sync_status_strip_returns_four_chips(
    web_db: sqlite3.Connection,
    web_client_factory,
) -> None:
    client = web_client_factory()
    resp = client.get(
        "/_partial/dashboard/sync-status",
        headers={"HX-Request": "true"},
    )
    assert resp.status_code == 200
    body = resp.text
    for source in ("binance", "provincial", "bcv", "p2p_rates"):
        assert source in body, f"missing source chip: {source}"


def test_sync_status_severity_red_on_recent_error(
    web_db: sqlite3.Connection,
    web_client_factory,
) -> None:
    run_id = import_state_repo.start_run(web_db, "binance")
    import_state_repo.finish_run(
        web_db, run_id, status="error", error="boom"
    )

    client = web_client_factory()
    resp = client.get("/_partial/dashboard/sync-status", headers={"HX-Request": "true"})
    body = resp.text
    # Find the binance chip and confirm red severity rendered.
    # The chip carries data-severity for tests.
    assert 'data-source="binance"' in body
    assert 'data-source="binance" data-severity="red"' in body or (
        'data-severity="red"' in body and "binance" in body
    )


def test_sync_status_severity_yellow_when_stale(
    web_db: sqlite3.Connection,
    web_client_factory,
) -> None:
    """A success that finished 5 days ago is past the binance/p2p_rates 24h window."""
    cur = web_db.execute(
        """
        INSERT INTO import_runs (source, started_at, finished_at, status, rows_inserted)
        VALUES (?, datetime('now', '-6 days'), datetime('now', '-5 days'), 'success', 1)
        """,
        ("binance",),
    )
    assert cur.lastrowid is not None

    client = web_client_factory()
    resp = client.get("/_partial/dashboard/sync-status", headers={"HX-Request": "true"})
    body = resp.text
    assert 'data-source="binance"' in body
    # locate binance chip portion of body and check severity
    idx = body.find('data-source="binance"')
    # Slice a window large enough to contain the data-severity attr.
    window = body[idx : idx + 200]
    assert 'data-severity="yellow"' in window


def test_sync_status_severity_green_when_recent_success(
    web_db: sqlite3.Connection,
    web_client_factory,
) -> None:
    cur = web_db.execute(
        """
        INSERT INTO import_runs (source, started_at, finished_at, status, rows_inserted)
        VALUES (?, datetime('now', '-1 minute'), datetime('now'), 'success', 1)
        """,
        ("binance",),
    )
    assert cur.lastrowid is not None

    client = web_client_factory()
    resp = client.get("/_partial/dashboard/sync-status", headers={"HX-Request": "true"})
    body = resp.text
    idx = body.find('data-source="binance"')
    assert idx >= 0
    window = body[idx : idx + 200]
    assert 'data-severity="green"' in window


# ---------------------------------------------------------------------------
# Recent activity.
# ---------------------------------------------------------------------------


def test_recent_activity_excludes_transfer_and_adjustment(
    web_db: sqlite3.Connection,
    web_client_factory,
) -> None:
    today = datetime.now(tz=UTC)
    cash = accounts_repo.insert(
        web_db,
        Account(name="Cash USD", kind=AccountKind.CASH, currency="USD"),
    )
    food = categories_repo.get_by_name(web_db, TransactionKind.EXPENSE, "Groceries")
    assert food is not None

    # An expense (should appear).
    transactions_repo.insert(
        web_db,
        Transaction(
            account_id=cash.id,
            occurred_at=today,
            kind=TransactionKind.EXPENSE,
            amount=Decimal("-5.00"),
            currency="USD",
            description="VISIBLE-EXPENSE",
            category_id=food.id,
            source="cash_cli",
            source_ref="vis-1",
        ),
    )
    # A transfer (must NOT appear).
    transactions_repo.insert(
        web_db,
        Transaction(
            account_id=cash.id,
            occurred_at=today,
            kind=TransactionKind.TRANSFER,
            amount=Decimal("-1.00"),
            currency="USD",
            description="HIDDEN-TRANSFER",
            transfer_id="abc",
            source="cash_cli",
            source_ref="trn-1",
        ),
    )
    # An adjustment (must NOT appear).
    transactions_repo.insert(
        web_db,
        Transaction(
            account_id=cash.id,
            occurred_at=today,
            kind=TransactionKind.ADJUSTMENT,
            amount=Decimal("-2.00"),
            currency="USD",
            description="HIDDEN-ADJUST",
            source="cash_cli",
            source_ref="adj-1",
        ),
    )

    client = web_client_factory()
    resp = client.get("/")
    body = resp.text
    assert "VISIBLE-EXPENSE" in body
    assert "HIDDEN-TRANSFER" not in body
    assert "HIDDEN-ADJUST" not in body


def test_recent_activity_uses_canonical_card_partial(
    seeded_web_db: sqlite3.Connection,
    web_client_factory,
) -> None:
    client = web_client_factory()
    resp = client.get("/api/dashboard/recent")
    assert resp.status_code == 200

    page = client.get("/")
    body = page.text
    # The canonical card partial leaves these markers on each rendered row.
    assert "data-card-row" in body
    assert "data-tx-id" in body


# ---------------------------------------------------------------------------
# 6-month spend trend.
# ---------------------------------------------------------------------------


def test_spend_trend_returns_expected_months(
    seeded_web_db: sqlite3.Connection,
    web_client_factory,
) -> None:
    client = web_client_factory()
    resp = client.get("/api/dashboard/spend-trend")
    assert resp.status_code == 200
    payload = resp.json()
    # 6 months total.
    assert len(payload["months"]) == 6
    # fallback shadow series matches the months count.
    assert len(payload["fallback_total_per_month"]) == 6
    # each series carries one value per month.
    for s in payload["series"]:
        assert len(s["values"]) == 6


def test_spend_trend_top_5_plus_other(
    web_db: sqlite3.Connection,
    web_client_factory,
) -> None:
    """7 distinct expense categories used in the window → top-5 + 'Other' = 6 series."""
    today = datetime.now(tz=UTC)
    cash = accounts_repo.insert(
        web_db,
        Account(name="Cash USD", kind=AccountKind.CASH, currency="USD"),
    )

    # Use 7 seeded categories with distinct totals (1..7) so top-5 is unambiguous.
    cat_names = [
        "Groceries",
        "Leisure",
        "Transport",
        "Health",
        "Subscriptions",
        "Utilities",
        "Personal Care",
    ]
    for idx, name in enumerate(cat_names, start=1):
        cat = categories_repo.get_by_name(web_db, TransactionKind.EXPENSE, name)
        assert cat is not None, f"seed category missing: {name}"
        transactions_repo.insert(
            web_db,
            Transaction(
                account_id=cash.id,
                occurred_at=today,
                kind=TransactionKind.EXPENSE,
                amount=Decimal(f"-{idx * 10}.00"),
                currency="USD",
                description=f"probe-{name}",
                category_id=cat.id,
                source="cash_cli",
                source_ref=f"probe-{name}",
            ),
        )

    client = web_client_factory()
    resp = client.get("/api/dashboard/spend-trend")
    payload = resp.json()
    series_names = [s["category"] for s in payload["series"]]
    assert len(series_names) == 6
    assert "Other" in series_names


def test_spend_trend_fallback_shadow_series(
    web_db: sqlite3.Connection,
    web_client_factory,
) -> None:
    today = datetime.now(tz=UTC)
    cur_month_dt = today.replace(day=1, hour=12, minute=0, second=0, microsecond=0)
    cur_month = cur_month_dt.strftime("%Y-%m")

    provincial = accounts_repo.insert(
        web_db,
        Account(name="Provincial", kind=AccountKind.BANK, currency="VES"),
    )
    food = categories_repo.get_by_name(web_db, TransactionKind.EXPENSE, "Groceries")
    assert food is not None

    rates_repo.upsert(
        web_db,
        Rate(
            as_of_date=cur_month_dt.date(),
            base="USD",
            quote="VES",
            rate=Decimal("100.0"),
            source="bcv",
        ),
    )
    transactions_repo.insert(
        web_db,
        Transaction(
            account_id=provincial.id,
            occurred_at=cur_month_dt,
            kind=TransactionKind.EXPENSE,
            amount=Decimal("50000.00"),
            currency="VES",
            description="bcv only",
            category_id=food.id,
            source="provincial",
            source_ref="fb-1",
        ),
    )

    client = web_client_factory()
    resp = client.get("/api/dashboard/spend-trend")
    payload = resp.json()
    months = payload["months"]
    fb = payload["fallback_total_per_month"]
    idx = months.index(cur_month)
    # 50000 VES / 100 = 500 USD fallback. Stored as Decimal-compatible str.
    assert Decimal(fb[idx]) != Decimal("0")


# ---------------------------------------------------------------------------
# HTMX shell behaviour.
# ---------------------------------------------------------------------------


def test_dashboard_partial_sync_status_no_full_html(
    web_db: sqlite3.Connection,
    web_client_factory,
) -> None:
    client = web_client_factory()
    resp = client.get(
        "/_partial/dashboard/sync-status",
        headers={"HX-Request": "true"},
    )
    assert resp.status_code == 200
    body = resp.text.lower()
    assert "<html" not in body
    assert "<body" not in body
