"""The rate override block, and what it discloses (criteria D4, D7-D10).

The header shows a dollar figure with no indication of the divisor. Before
ADR-021 the modal answered that with a three-tier read-only panel; the
redesign turns it into a control — the row is already priced from the
nearest rate the ledger has, marked ``≈``, and this is where the owner
accepts that number or types a better one.

So the assertions moved with it: every tier still shows the dollars it
would produce and says how far off it is, but each is now a button that
fills the field, and the block only renders on a row that is actually
being asked about its rate.

The service half (which rate each tier offers, its signed age, its
resulting USD) is pinned in tests/web/test_rates_for_day.py.
"""

from __future__ import annotations

import re
import sqlite3
from datetime import UTC, date, datetime
from decimal import Decimal

import pytest
from fastapi.testclient import TestClient

from finances.db.repos import accounts as accounts_repo
from finances.db.repos import categories as categories_repo
from finances.db.repos import rates as rates_repo
from finances.db.repos import transactions as transactions_repo
from finances.domain.models import (
    Account,
    AccountKind,
    Rate,
    Transaction,
    TransactionKind,
)

#: The rate island. Every transaction below sits months away from it, so
#: the resolver reaches it through ADR-021's nearest-rate branch and the
#: row lands in *Priced roughly* — the only place this block renders.
RATE_DAY = date(2026, 4, 23)
TXN_AT = datetime(2026, 1, 1, tzinfo=UTC)


def _seed_row(conn: sqlite3.Connection) -> None:
    provincial = accounts_repo.insert(
        conn,
        Account(name="Provincial Bolivares", kind=AccountKind.BANK, currency="VES"),
    )
    groceries = categories_repo.get_by_name(
        conn, TransactionKind.EXPENSE, "Groceries"
    )
    assert groceries is not None
    transactions_repo.insert(
        conn,
        Transaction(
            account_id=provincial.id,
            occurred_at=TXN_AT,
            kind=TransactionKind.EXPENSE,
            amount=Decimal("-20000.00"),
            currency="VES",
            description="TRAV0028021997000012403",
            category_id=groceries.id,
            source="provincial",
            source_ref="hash:9a12b3992e998132",
        ),
    )


@pytest.fixture
def panel_db(web_db: sqlite3.Connection) -> sqlite3.Connection:
    """All three tiers present, each a different round number of dollars.

    500 / 400 / 250 against 20 000 Bs gives $40 / $50 / $80, so a figure
    on screen can only have come from one of them.
    """
    for base, source, rate in (
        ("USDT", "binance_p2p_realized", Decimal("500.00")),
        ("USDT", "binance_p2p_median", Decimal("400.00")),
        ("USD", "bcv", Decimal("250.00")),
    ):
        rates_repo.upsert(
            web_db,
            Rate(
                as_of_date=RATE_DAY,
                base=base,
                quote="VES",
                rate=rate,
                source=source,
            ),
        )
    _seed_row(web_db)
    return web_db


@pytest.fixture
def unpriced_db(web_db: sqlite3.Connection) -> sqlite3.Connection:
    """The same row on a ledger with no rate of any tier at all."""
    _seed_row(web_db)
    return web_db


def _modal(client: TestClient) -> str:
    response = client.get("/_partial/triage/1/modal")
    assert response.status_code == 200, response.text
    return response.text


def _hint(html: str, source: str) -> str:
    """The one suggestion button for ``source``."""
    marker = f'data-rate-hint-source="{source}"'
    assert marker in html, f"no suggestion for {source}"
    at = html.index(marker)
    start = html.rindex("<button", 0, at)
    return html[start : html.index("</button>", at)]


# ---------------------------------------------------------------------------
# The block renders only where the rate is the question (D6).
# ---------------------------------------------------------------------------


def test_the_block_renders_on_an_approximately_priced_row(
    panel_db: sqlite3.Connection, web_client_factory
) -> None:
    client: TestClient = web_client_factory()

    html = _modal(client)

    assert "data-rate-warning" in html
    assert "Rate you got" in html


def test_a_row_priced_inside_the_window_is_never_asked_about_its_rate(
    web_db: sqlite3.Connection, web_client_factory
) -> None:
    rates_repo.upsert(
        web_db,
        Rate(
            as_of_date=TXN_AT.date(),
            base="USDT",
            quote="VES",
            rate=Decimal("400.00"),
            source="binance_p2p_median",
        ),
    )
    provincial = accounts_repo.insert(
        web_db,
        Account(name="Provincial", kind=AccountKind.BANK, currency="VES"),
    )
    transactions_repo.insert(
        web_db,
        Transaction(
            account_id=provincial.id,
            occurred_at=TXN_AT,
            kind=TransactionKind.EXPENSE,
            amount=Decimal("-20000.00"),
            currency="VES",
            description="IN WINDOW",
            source="provincial",
            source_ref="in-window",
        ),
    )
    client: TestClient = web_client_factory()

    html = _modal(client)

    assert "data-rate-warning" not in html
    assert "OR TAKE ONE OF THESE" not in html


def test_a_native_usd_row_has_no_rate_block(
    web_db: sqlite3.Connection, web_client_factory
) -> None:
    """A dollar is a dollar; there is no divisor to disclose."""
    cash = accounts_repo.insert(
        web_db, Account(name="Cash USD", kind=AccountKind.CASH, currency="USD")
    )
    transactions_repo.insert(
        web_db,
        Transaction(
            account_id=cash.id,
            occurred_at=TXN_AT,
            kind=TransactionKind.EXPENSE,
            amount=Decimal("-12.50"),
            currency="USD",
            description="lunch",
            source="cash_cli",
            source_ref="cash-panel-1",
        ),
    )
    client: TestClient = web_client_factory()

    html = _modal(client)

    assert "data-rate-warning" not in html


# ---------------------------------------------------------------------------
# Every tier, with the dollars it would produce (D9).
# ---------------------------------------------------------------------------


def test_all_three_tiers_are_offered(
    panel_db: sqlite3.Connection, web_client_factory
) -> None:
    client: TestClient = web_client_factory()

    html = _modal(client)

    assert "OR TAKE ONE OF THESE" in html
    for source in ("binance_p2p_realized", "binance_p2p_median", "bcv"):
        assert f'data-rate-hint-source="{source}"' in html


def test_each_tier_shows_its_own_rate_and_its_own_dollars(
    panel_db: sqlite3.Connection, web_client_factory
) -> None:
    """A figure on screen can only have come from one divisor."""
    client: TestClient = web_client_factory()
    html = _modal(client)

    realized = _hint(html, "binance_p2p_realized")
    median = _hint(html, "binance_p2p_median")
    bcv = _hint(html, "bcv")

    assert "500.00" in realized and "−$40.00" in realized
    assert "400.00" in median and "−$50.00" in median
    assert "250.00" in bcv and "−$80.00" in bcv
    assert "official floor, reference only" in bcv


def test_each_tier_says_how_far_off_it_is(
    panel_db: sqlite3.Connection, web_client_factory
) -> None:
    """D9 — "112 days later" is the whole reason the figure is a guess."""
    client: TestClient = web_client_factory()

    median = _hint(_modal(client), "binance_p2p_median")

    assert "112 days later" in median
    assert "Outside the 14-day window" in median


def test_clicking_a_tier_fills_the_field_rather_than_saving(
    panel_db: sqlite3.Connection, web_client_factory
) -> None:
    client: TestClient = web_client_factory()

    bcv = _hint(_modal(client), "bcv")

    assert re.search(r"setRate\('250(\.0+)?'\)", bcv)
    assert "hx-post" not in bcv


# ---------------------------------------------------------------------------
# The warning, and the row it describes (D10).
# ---------------------------------------------------------------------------


def test_the_warning_names_the_winning_rate_its_tier_and_the_date(
    panel_db: sqlite3.Connection, web_client_factory
) -> None:
    client: TestClient = web_client_factory()

    html = _modal(client)
    warning = html.split("data-rate-warning", 1)[1].split("</div>", 3)[0]

    # The chain's own answer: same distance from every tier, so priority
    # breaks the tie and realized wins.
    assert "500.00" in warning
    assert "Realized, 112 days later" in warning
    assert "Thu, Jan 1" in html


def test_the_row_is_marked_approximate_wherever_its_money_appears(
    panel_db: sqlite3.Connection, web_client_factory
) -> None:
    """D4 — priced with the nearest rate, and never silently."""
    client: TestClient = web_client_factory()

    queue = client.get("/_partial/triage/queue").text
    row = re.search(r'data-item-id="txn:1".*?<!-- /triage-row -->', queue, re.S)
    assert row is not None

    assert "≈" in row.group(0)
    assert "prov-warn" in row.group(0)
    assert "−$40.00" in row.group(0)


# ---------------------------------------------------------------------------
# The genuinely unpriceable row (D5).
# ---------------------------------------------------------------------------


def test_an_unpriceable_row_offers_no_tier_and_says_so(
    unpriced_db: sqlite3.Connection, web_client_factory
) -> None:
    client: TestClient = web_client_factory()

    html = _modal(client)

    assert "Can&#39;t be priced" in html or "Can't be priced" in html
    assert "data-rate-hint" not in html
    # It still shows its bolívares, which are not in doubt.
    assert "−Bs. 20,000.00" in html
