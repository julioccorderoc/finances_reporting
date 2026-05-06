"""Read-only tests for /accounts and /rates (EPIC-023, Phase 2d).

Per rule-011 these land before the implementation. They cover:

* `/accounts` HTML render, sort order, drill URL, USDT-equivalent for
  USD-native, VES with P2P, and VES without P2P (em-dash).
* `/api/accounts` JSON shape.
* `/rates` chart + latest-per-pair card list, range toggle through HTMX
  partial, JSON shape.

The seed deliberately includes accounts in a non-alphabetical insertion
order with a mix of active/inactive flags so we can verify the sort
contract (active first, then by name).
"""

from __future__ import annotations

import sqlite3
from datetime import date, timedelta
from decimal import Decimal

import pytest
from fastapi.testclient import TestClient

from finances.db.repos import accounts as accounts_repo
from finances.db.repos import rates as rates_repo
from finances.domain.models import Account, AccountKind, Rate


# ---------------------------------------------------------------------------
# Phase 2d-specific fixture: more accounts (incl. inactive) and several
# rate rows per (base, quote, source) so the latest-per-pair query has
# something to collapse.
# ---------------------------------------------------------------------------


@pytest.fixture
def accounts_rates_db(seeded_web_db: sqlite3.Connection) -> sqlite3.Connection:
    """Augment ``seeded_web_db`` with extras for the /accounts and /rates pages.

    * Add an inactive Banesco VES account (no transactions; balance == 0).
    * Add a USD bank account (Mercantil USD).
    * Backfill 14 days of P2P + BCV rate history so the chart has points.
    """
    today = date.today()

    accounts_repo.insert(
        seeded_web_db,
        Account(
            name="Banesco VES",
            kind=AccountKind.BANK,
            currency="VES",
            institution="Banesco",
            active=False,
        ),
    )
    accounts_repo.insert(
        seeded_web_db,
        Account(
            name="Mercantil USD",
            kind=AccountKind.BANK,
            currency="USD",
            institution="Mercantil",
            active=True,
        ),
    )

    # Backfill 14 daily rate rows for both series.
    for offset in range(1, 15):
        d = today - timedelta(days=offset)
        rates_repo.upsert(
            seeded_web_db,
            Rate(
                as_of_date=d,
                base="USDT",
                quote="VES",
                rate=Decimal("36.00") + Decimal(offset) / Decimal("100"),
                source="binance_p2p_median",
            ),
        )
        rates_repo.upsert(
            seeded_web_db,
            Rate(
                as_of_date=d,
                base="USD",
                quote="VES",
                rate=Decimal("35.50") + Decimal(offset) / Decimal("100"),
                source="bcv",
            ),
        )

    # Also include a non-VES rate stream so we can prove the chart filters
    # to the two canonical series only.
    rates_repo.upsert(
        seeded_web_db,
        Rate(
            as_of_date=today,
            base="USDT",
            quote="USD",
            rate=Decimal("1.0"),
            source="binance_p2p_median",
        ),
    )
    return seeded_web_db


# ---------------------------------------------------------------------------
# /accounts
# ---------------------------------------------------------------------------


def test_accounts_page_renders(
    accounts_rates_db: sqlite3.Connection,
    web_client_factory,
) -> None:
    client: TestClient = web_client_factory()
    resp = client.get("/accounts")
    assert resp.status_code == 200
    body = resp.text
    assert "Accounts" in body
    # at least the active seeded names should appear
    assert "Provincial" in body
    assert "Cash USD" in body
    assert "Binance Spot" in body
    assert "<body" in body  # full page extends base.html


def test_accounts_card_for_usd_account_has_balance_usdt_equal_to_native(
    accounts_rates_db: sqlite3.Connection,
    web_client_factory,
) -> None:
    """USD account → USDT balance == native balance (1:1 per ADR-005)."""
    from finances.web.services.accounts_view import build_account_cards

    today = date.today()
    cards = build_account_cards(accounts_rates_db, today=today)
    cash = next(c for c in cards if c.name == "Cash USD")
    assert cash.currency == "USD"
    assert cash.balance_usdt == cash.balance_native
    # The seed records the cash expense as a positive amount (12.50);
    # the view sums amounts as-is, so the balance equals that figure.
    # The exact value isn't load-bearing for this test, but the 1:1
    # USD->USDT identity is.
    assert cash.balance_native == Decimal("12.5")


def test_accounts_card_for_ves_account_uses_p2p_rate(
    accounts_rates_db: sqlite3.Connection,
    web_client_factory,
) -> None:
    """VES account → balance_usdt = balance_native / latest USDT/VES P2P median."""
    from finances.web.services.accounts_view import build_account_cards

    today = date.today()
    cards = build_account_cards(accounts_rates_db, today=today)
    provincial = next(c for c in cards if c.name == "Provincial")
    assert provincial.currency == "VES"
    assert provincial.balance_usdt is not None
    # Today's USDT/VES P2P median = 36.50 (from seeded_web_db).
    expected = provincial.balance_native / Decimal("36.50")
    assert provincial.balance_usdt == expected


def test_accounts_card_for_ves_account_with_no_p2p_shows_dash(
    web_db: sqlite3.Connection,
    web_client_factory,
) -> None:
    """No P2P pair available → balance_usdt is None and HTML renders an em dash."""
    from finances.web.services.accounts_view import build_account_cards

    # Insert a VES account with no rate rows.
    account = accounts_repo.insert(
        web_db,
        Account(
            name="VES Solo",
            kind=AccountKind.BANK,
            currency="VES",
        ),
    )
    today = date.today()
    cards = build_account_cards(web_db, today=today)
    target = next(c for c in cards if c.name == account.name)
    assert target.balance_usdt is None

    client = web_client_factory()
    resp = client.get("/accounts")
    assert resp.status_code == 200
    # The em dash entity appears for missing USDT-equivalent.
    assert "VES Solo" in resp.text
    assert "&mdash;" in resp.text


def test_accounts_card_drill_url_points_to_filtered_transactions(
    accounts_rates_db: sqlite3.Connection,
    web_client_factory,
) -> None:
    from finances.web.services.accounts_view import build_account_cards

    today = date.today()
    cards = build_account_cards(accounts_rates_db, today=today)
    for c in cards:
        assert c.drill_url.startswith("/transactions?accounts=")
        # Account name must round-trip through the URL.
        assert c.name.replace(" ", "+") in c.drill_url or c.name in c.drill_url


def test_accounts_inactive_accounts_render_after_active(
    accounts_rates_db: sqlite3.Connection,
    web_client_factory,
) -> None:
    """Sort order: active accounts first, then inactive; alpha within each group."""
    from finances.web.services.accounts_view import build_account_cards

    today = date.today()
    cards = build_account_cards(accounts_rates_db, today=today)
    active_names = [c.name for c in cards if c.active]
    inactive_names = [c.name for c in cards if not c.active]

    # Active appear before inactive in the master list.
    last_active_idx = max(i for i, c in enumerate(cards) if c.active)
    first_inactive_idx = min((i for i, c in enumerate(cards) if not c.active), default=10**9)
    assert last_active_idx < first_inactive_idx

    # Within each group the names are alpha-sorted.
    assert active_names == sorted(active_names)
    assert inactive_names == sorted(inactive_names)
    # Banesco VES is the only inactive account in this fixture.
    assert "Banesco VES" in inactive_names


def test_api_accounts_returns_card_list_json(
    accounts_rates_db: sqlite3.Connection,
    web_client_factory,
) -> None:
    client = web_client_factory()
    resp = client.get("/api/accounts")
    assert resp.status_code == 200
    payload = resp.json()
    assert isinstance(payload, list)
    assert len(payload) >= 3
    sample = payload[0]
    for key in (
        "id",
        "name",
        "kind",
        "institution",
        "currency",
        "balance_native",
        "balance_usdt",
        "active",
        "drill_url",
    ):
        assert key in sample, f"missing key in account JSON: {key}"


# ---------------------------------------------------------------------------
# /rates
# ---------------------------------------------------------------------------


def test_rates_page_renders(
    accounts_rates_db: sqlite3.Connection,
    web_client_factory,
) -> None:
    client = web_client_factory()
    resp = client.get("/rates")
    assert resp.status_code == 200
    body = resp.text
    assert "Rates" in body
    # Chart canvas
    assert "<canvas" in body
    # Latest-per-pair card list — at least USDT/VES P2P should show.
    assert "USDT" in body and "VES" in body


def test_rates_chart_default_range_30_days(
    accounts_rates_db: sqlite3.Connection,
) -> None:
    from finances.web.services.rates_view import build_rates_chart

    chart = build_rates_chart(accounts_rates_db)
    assert chart.range_days == 30
    # Each series may have <= 30 daily points.
    for series in chart.series:
        assert len(series.points) <= 30


def test_rates_chart_range_toggle_via_partial(
    accounts_rates_db: sqlite3.Connection,
    web_client_factory,
) -> None:
    client = web_client_factory()
    resp = client.get(
        "/_partial/rates/chart",
        params={"range_days": 7},
        headers={"HX-Request": "true"},
    )
    assert resp.status_code == 200
    body = resp.text
    # Fragment only — no full HTML shell.
    assert "<html" not in body.lower()
    assert "<body" not in body.lower()
    # Must contain a canvas (the chart target) AND chart data inline.
    assert "<canvas" in body

    # Service-level: ≤ 7 points per series.
    from finances.web.services.rates_view import build_rates_chart

    chart = build_rates_chart(accounts_rates_db, range_days=7)
    for series in chart.series:
        assert len(series.points) <= 7


def test_rates_chart_includes_p2p_and_bcv_series(
    accounts_rates_db: sqlite3.Connection,
) -> None:
    from finances.web.services.rates_view import build_rates_chart

    chart = build_rates_chart(accounts_rates_db, range_days=30)
    labels = {s.label for s in chart.series}
    sources = {s.source for s in chart.series}
    assert "binance_p2p_median" in sources
    assert "bcv" in sources
    # And the label text mentions P2P and BCV (case-insensitive).
    joined = " ".join(labels).lower()
    assert "p2p" in joined
    assert "bcv" in joined


def test_rates_latest_per_pair_groups_correctly(
    accounts_rates_db: sqlite3.Connection,
) -> None:
    """Multiple rates per (base, quote, source) → only the most recent in latest list."""
    from finances.web.services.rates_view import build_latest_rates

    latest = build_latest_rates(accounts_rates_db)
    # Each (base, quote, source) tuple appears at most once.
    seen: set[tuple[str, str, str]] = set()
    for card in latest:
        key = (card.base, card.quote, card.source)
        assert key not in seen, f"duplicate group: {key}"
        seen.add(key)

    # USDT/VES binance_p2p_median should resolve to today's row (rate 36.50).
    p2p = next(
        c for c in latest if c.base == "USDT" and c.quote == "VES" and c.source == "binance_p2p_median"
    )
    assert p2p.rate == Decimal("36.50")
    assert p2p.as_of_date == date.today()


def test_api_rates_returns_chart_and_latest_lists(
    accounts_rates_db: sqlite3.Connection,
    web_client_factory,
) -> None:
    client = web_client_factory()
    resp = client.get("/api/rates")
    assert resp.status_code == 200
    payload = resp.json()
    assert "chart" in payload
    assert "latest" in payload
    assert isinstance(payload["latest"], list)
    chart = payload["chart"]
    assert "series" in chart
    assert "range_days" in chart
    assert chart["range_days"] == 30


def test_htmx_partial_rates_chart_no_full_html(
    accounts_rates_db: sqlite3.Connection,
    web_client_factory,
) -> None:
    client = web_client_factory()
    resp = client.get(
        "/_partial/rates/chart",
        headers={"HX-Request": "true"},
    )
    assert resp.status_code == 200
    body = resp.text
    assert "<html" not in body.lower()
    assert "<body" not in body.lower()
    assert "<canvas" in body
