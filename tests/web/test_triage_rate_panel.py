"""Triage modal discloses all three candidate rates (spec §5.1).

The header shows e.g. "20,000.00 VES  $41.38" with no indication of the
divisor. These tests pin that the modal names every candidate rate for the
transaction's day and marks the one that produced the dollar figure.
"""

from __future__ import annotations

import sqlite3
from datetime import UTC, date, datetime
from decimal import Decimal

import pytest
from fastapi.testclient import TestClient

from finances.db.repos import accounts as accounts_repo
from finances.db.repos import rates as rates_repo
from finances.db.repos import transactions as transactions_repo
from finances.domain.models import (
    Account,
    AccountKind,
    Rate,
    Transaction,
    TransactionKind,
)

DAY = date(2026, 4, 23)
DAY_AT = datetime(2026, 4, 23, tzinfo=UTC)


@pytest.fixture
def panel_db(web_db: sqlite3.Connection) -> sqlite3.Connection:
    """One VES expense priced by BCV fallback, plus one native-USD row."""
    provincial = accounts_repo.insert(
        web_db,
        Account(name="Provincial Bolivares", kind=AccountKind.BANK, currency="VES"),
    )
    cash = accounts_repo.insert(
        web_db,
        Account(name="Cash USD", kind=AccountKind.CASH, currency="USD"),
    )

    # Only BCV exists for that day, so the resolver falls through to it.
    rates_repo.upsert(
        web_db,
        Rate(as_of_date=DAY, base="USD", quote="VES",
             rate=Decimal("483.30"), source="bcv"),
    )

    transactions_repo.insert(
        web_db,
        Transaction(
            account_id=provincial.id,
            occurred_at=DAY_AT,
            kind=TransactionKind.TRANSFER,
            amount=Decimal("20000.00"),
            currency="VES",
            description="TRAV0028021997000012403",
            source="provincial",
            source_ref="hash:9a12b3992e998132",
            needs_review=True,
        ),
    )
    transactions_repo.insert(
        web_db,
        Transaction(
            account_id=cash.id,
            occurred_at=DAY_AT,
            kind=TransactionKind.EXPENSE,
            amount=Decimal("12.50"),
            currency="USD",
            description="lunch",
            source="cash_cli",
            source_ref="cash-panel-1",
        ),
    )
    return web_db


@pytest.fixture
def priced_panel_db(panel_db: sqlite3.Connection) -> sqlite3.Connection:
    """All three tiers present, each a different (round) number of dollars."""
    rates_repo.upsert(
        panel_db,
        Rate(as_of_date=DAY, base="USDT", quote="VES",
             rate=Decimal("500.00"), source="binance_p2p_realized"),
    )
    rates_repo.upsert(
        panel_db,
        Rate(as_of_date=DAY, base="USDT", quote="VES",
             rate=Decimal("400.00"), source="binance_p2p_median"),
    )
    rates_repo.upsert(
        panel_db,
        Rate(as_of_date=DAY, base="USD", quote="VES",
             rate=Decimal("250.00"), source="bcv"),
    )
    return panel_db


def _row(html: str, source: str) -> str:
    """The <dd> for one rate series."""
    return html.split(f'data-rate-row="{source}"', 1)[1].split("</dd>", 1)[0]


def test_panel_lists_all_three_series(
    panel_db: sqlite3.Connection, web_client_factory
) -> None:
    client: TestClient = web_client_factory()
    html = client.get("/_partial/triage/1/modal").text

    assert "data-rate-panel" in html
    for source in ("binance_p2p_realized", "binance_p2p_median", "bcv"):
        assert f'data-rate-row="{source}"' in html, source


def test_panel_marks_the_winning_series(
    panel_db: sqlite3.Connection, web_client_factory
) -> None:
    client: TestClient = web_client_factory()
    html = client.get("/_partial/triage/1/modal").text

    # The winner marker appears exactly once, inside the bcv row.
    assert html.count("data-rate-winner") == 1
    bcv_row = html.split('data-rate-row="bcv"', 1)[1]
    assert "data-rate-winner" in bcv_row.split("</dd>", 1)[0]


def test_absent_series_renders_a_dash_not_a_crash(
    panel_db: sqlite3.Connection, web_client_factory
) -> None:
    client: TestClient = web_client_factory()
    response = client.get("/_partial/triage/1/modal")

    assert response.status_code == 200
    realized_row = response.text.split(
        'data-rate-row="binance_p2p_realized"', 1
    )[1].split("</dd>", 1)[0]
    assert "&mdash;" in realized_row or "—" in realized_row


def test_bcv_row_says_reference_only(
    panel_db: sqlite3.Connection, web_client_factory
) -> None:
    client: TestClient = web_client_factory()
    html = client.get("/_partial/triage/1/modal").text

    bcv_row = html.split('data-rate-row="bcv"', 1)[1].split("</dd>", 1)[0]
    assert "reference only" in bcv_row.lower()


def test_native_usd_row_has_no_panel(
    panel_db: sqlite3.Connection, web_client_factory
) -> None:
    """USD/USDT/USDC rows short-circuit to native_usd; a rate panel is noise."""
    client: TestClient = web_client_factory()
    html = client.get("/_partial/triage/2/modal").text

    assert "data-rate-panel" not in html


def test_every_priced_row_shows_its_own_dollar_figure(
    priced_panel_db: sqlite3.Connection, web_client_factory
) -> None:
    """20,000 VES is $40 at 500, $50 at 400, $80 at 250 — show all three."""
    client: TestClient = web_client_factory()
    html = client.get("/_partial/triage/1/modal").text

    assert "$40.00" in _row(html, "binance_p2p_realized")
    assert "$50.00" in _row(html, "binance_p2p_median")
    assert "$80.00" in _row(html, "bcv")


def test_each_dollar_figure_is_tagged_for_its_own_row(
    priced_panel_db: sqlite3.Connection, web_client_factory
) -> None:
    client: TestClient = web_client_factory()
    html = client.get("/_partial/triage/1/modal").text

    assert html.count("data-rate-usd") == 3


def test_an_unpriced_row_shows_no_dollar_figure(
    panel_db: sqlite3.Connection, web_client_factory
) -> None:
    """Only BCV exists in this fixture; realized must not invent a $."""
    client: TestClient = web_client_factory()
    html = client.get("/_partial/triage/1/modal").text

    assert "$" not in _row(html, "binance_p2p_realized")


def test_the_winner_row_is_still_the_one_tied_to_the_header_figure(
    priced_panel_db: sqlite3.Connection, web_client_factory
) -> None:
    """Realized wins the resolver chain; its $ must match the header's."""
    client: TestClient = web_client_factory()
    html = client.get("/_partial/triage/1/modal").text

    realized = _row(html, "binance_p2p_realized")
    assert "data-rate-winner" in realized
    assert "$40.00" in realized


def test_panel_does_not_disturb_existing_modal_contract(
    panel_db: sqlite3.Connection, web_client_factory
) -> None:
    """Guard: the picker, rate field and action buttons still render."""
    client: TestClient = web_client_factory()
    html = client.get("/_partial/triage/1/modal").text

    assert 'name="set_category"' in html
    assert 'name="user_rate"' in html
    assert "data-park-btn" in html
    assert 'hx-post="/_partial/triage/1/edit"' in html
