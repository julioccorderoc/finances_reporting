"""Today says how much of the ledger rests on a plug (ADR-018 §2).

An adjustment corrects a balance by asserting that the record is
incomplete. ADR-020 §1.2 is the case for keeping that assertion in view:
three plugs sized against a corrupted balance left every check green while
income was overstated by 10,462.71 USDC. A residual that disappears into
the total is how that happens twice.

So the front page carries a line — "N adjustments · $X unexplained since
<date>" — and `finances doctor` lists the same rows.
"""

from __future__ import annotations

import sqlite3
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

import pytest

from finances.db.repos import accounts as accounts_repo
from finances.db.repos import transactions as transactions_repo
from finances.domain.models import (
    Account,
    AccountKind,
    Transaction,
    TransactionKind,
)
from finances.web.services import dashboard

TODAY = date(2026, 9, 3)


def _plug(account_id: int, *, day: date, amount: str, currency: str, ref: str):
    return Transaction(
        account_id=account_id,
        occurred_at=datetime.combine(day, datetime.min.time(), tzinfo=UTC),
        kind=TransactionKind.ADJUSTMENT,
        amount=Decimal(amount),
        currency=currency,
        description="Reconciliation to custodian balance",
        source="reconciliation",
        source_ref=ref,
        notes="history past the six-month window",
    )


@pytest.fixture
def plug_db(web_db: sqlite3.Connection) -> sqlite3.Connection:
    spot = accounts_repo.insert(
        web_db,
        Account(name="Binance Spot", kind=AccountKind.CRYPTO_SPOT, currency="USDT"),
    )
    assert spot.id is not None
    transactions_repo.insert(
        web_db,
        _plug(
            spot.id,
            day=TODAY - timedelta(days=30),
            amount="-40.00",
            currency="USDT",
            ref="reconcile:1:USDT:a",
        ),
    )
    transactions_repo.insert(
        web_db,
        _plug(
            spot.id,
            day=TODAY - timedelta(days=2),
            amount="10.00",
            currency="USDT",
            ref="reconcile:1:USDT:b",
        ),
    )
    return web_db


def test_no_plugs_means_no_line(web_db: sqlite3.Connection) -> None:
    summary = dashboard.build_plug_summary(web_db, today=TODAY)
    assert summary.count == 0
    assert summary.total_usd == Decimal("0")
    assert summary.since is None


def test_the_line_counts_the_plugs_and_dates_the_oldest(
    plug_db: sqlite3.Connection,
) -> None:
    summary = dashboard.build_plug_summary(plug_db, today=TODAY)
    assert summary.count == 2
    assert summary.since == TODAY - timedelta(days=30)


def test_the_total_is_the_magnitude_not_the_net(plug_db: sqlite3.Connection) -> None:
    """−40 and +10 are 50 USDT of unexplained ledger, not 30.

    Netting them would let two opposite plugs cancel to nothing while both
    remain assertions rather than records.
    """
    summary = dashboard.build_plug_summary(plug_db, today=TODAY)
    assert summary.total_usd == Decimal("50.00")
    assert summary.unpriced == 0


def test_a_plug_no_rate_can_price_is_counted_but_not_totalled(
    web_db: sqlite3.Connection,
) -> None:
    bank = accounts_repo.insert(
        web_db, Account(name="Bogota COP", kind=AccountKind.BANK, currency="COP")
    )
    assert bank.id is not None
    transactions_repo.insert(
        web_db,
        _plug(
            bank.id,
            day=TODAY - timedelta(days=1),
            amount="-500000",
            currency="COP",
            ref="reconcile:2:COP:a",
        ),
    )

    summary = dashboard.build_plug_summary(web_db, today=TODAY)

    assert summary.count == 1
    assert summary.unpriced == 1
    assert summary.total_usd == Decimal("0")


def test_opening_positions_are_not_plugs(web_db: sqlite3.Connection) -> None:
    """ADR-020 rows are also ``kind='adjustment'`` and a different claim."""
    spot = accounts_repo.insert(
        web_db,
        Account(name="Binance Spot", kind=AccountKind.CRYPTO_SPOT, currency="USDT"),
    )
    assert spot.id is not None
    row = _plug(
        spot.id,
        day=TODAY - timedelta(days=5),
        amount="100",
        currency="USDT",
        ref="opening:1:USDT",
    )
    transactions_repo.insert(web_db, row.model_copy(update={"source": "opening_balance"}))

    assert dashboard.build_plug_summary(web_db, today=TODAY).count == 0


# ---------------------------------------------------------------------------
# The surface
# ---------------------------------------------------------------------------


def test_kpis_carry_the_plug_summary(plug_db: sqlite3.Connection) -> None:
    kpis = dashboard.build_kpis(plug_db, today=TODAY)
    assert kpis.plugs.count == 2


def test_today_renders_the_unexplained_line(
    plug_db: sqlite3.Connection, web_client_factory
) -> None:
    html = web_client_factory().get("/").text
    assert "data-plug-summary" in html
    assert "2 adjustments" in html
    assert "unexplained since" in html


def test_today_omits_the_line_on_a_ledger_with_no_plugs(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    html = web_client_factory().get("/").text
    assert "data-plug-summary" not in html


def test_the_line_links_to_the_rows_it_counts(
    plug_db: sqlite3.Connection, web_client_factory
) -> None:
    html = web_client_factory().get("/").text
    assert "/transactions?kinds=adjustment" in html


def test_tiles_refetch_themselves_when_a_plug_is_written(
    plug_db: sqlite3.Connection, web_client_factory
) -> None:
    """``kpisDirty`` is what /accounts sends after writing an adjustment."""
    html = web_client_factory().get("/").text
    assert 'hx-get="/_partial/dashboard/kpis"' in html
    assert "kpisDirty from:body" in html


def test_the_kpis_partial_renders_the_tiles_alone(
    plug_db: sqlite3.Connection, web_client_factory
) -> None:
    resp = web_client_factory().get("/_partial/dashboard/kpis")
    assert resp.status_code == 200
    body = resp.text
    assert "today-tiles" in body
    assert "data-plug-summary" in body
    # A fragment, not a page.
    assert "<html" not in body
