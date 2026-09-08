"""Totals for the /transactions filter — the page's answer is money, not a count.

"What happened?" used to be answered with "28 rows". The owner filters
Groceries for August to learn what groceries cost in August, and the
count is not that answer (2026-09-08). The Doto figure is now the net
dollars of every row the filter matches, and "28 rows" moves to the meta
line beside the window.

What the figure means is the house rule from ``domain/money.py``, not a
naive sum: a paired transfer, a row filed under a transfer-kind category
and an adjustment all *moved* money rather than earning or spending it,
so they are excluded and counted ("3 transfers not counted"). A row no
rate can price is excluded too, and said so ("1 can't be priced"). The
four piles partition the matches exactly:

    counted + moved + adjustments + unpriced == rows

The native total exists only when every spending row shares a currency
the dollar figure does not already say — bolívares, pesos — and it
covers the unpriced rows too, since summing a native amount needs no
rate.
"""

from __future__ import annotations

import re
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
    Rate,
    Transaction,
    TransactionKind,
)

MINUS = "−"
NBSP = " "


@pytest.fixture
def totals_db(web_db: sqlite3.Connection) -> sqlite3.Connection:
    """One row of every pile the totals have to sort, all priced today.

    USDT/VES = 36.50, so the bolívar rows come out as round dollars:
    −365 → −$10.00, 3 650 → +$100.00. COP has no rate at all, which is the
    only way to get an unpriced row since ADR-021 (a rate anywhere in the
    table prices a row approximately).
    """
    today = datetime.now(tz=UTC)

    provincial = accounts_repo.insert(
        web_db,
        Account(name="Provincial", kind=AccountKind.BANK, currency="VES", institution="Provincial"),
    )
    binance = accounts_repo.insert(
        web_db,
        Account(
            name="Binance Spot",
            kind=AccountKind.CRYPTO_SPOT,
            currency="USDT",
            institution="Binance",
        ),
    )
    nequi = accounts_repo.insert(
        web_db,
        Account(name="Nequi", kind=AccountKind.BANK, currency="COP", institution="Nequi"),
    )

    food = categories_repo.get_by_name(web_db, TransactionKind.EXPENSE, "Groceries")
    salary = categories_repo.get_by_name(web_db, TransactionKind.INCOME, "Salary")
    internal = categories_repo.get_by_name(
        web_db, TransactionKind.TRANSFER, "Internal Transfer"
    )
    assert food is not None and salary is not None and internal is not None

    rates_repo.upsert(
        web_db,
        Rate(
            as_of_date=date.today(),
            base="USDT",
            quote="VES",
            rate=Decimal("36.50"),
            source="binance_p2p_median",
        ),
    )

    def row(account, kind, amount, currency, description, **extra) -> Transaction:
        return Transaction(
            account_id=account.id,
            occurred_at=today,
            kind=kind,
            amount=Decimal(amount),
            currency=currency,
            description=description,
            source="test",
            source_ref=f"totals-{description}",
            **extra,
        )

    rows = [
        # Counted: two bolívar rows, two dollar rows.
        row(provincial, TransactionKind.EXPENSE, "-365.00", "VES", "COM.PAGO bodega", category_id=food.id),
        row(provincial, TransactionKind.INCOME, "3650.00", "VES", "ABONO nomina", category_id=salary.id),
        row(binance, TransactionKind.EXPENSE, "-12.50", "USDT", "Binance Pay lunch"),
        row(binance, TransactionKind.INCOME, "100.00", "USDT", "Earn payout"),
        # Moved: a paired transfer (two legs) and a row filed as movement.
        row(binance, TransactionKind.TRANSFER, "-50.00", "USDT", "Spot to Funding", transfer_id="pair-1"),
        row(binance, TransactionKind.TRANSFER, "50.00", "USDT", "Funding from Spot", transfer_id="pair-1"),
        row(binance, TransactionKind.INCOME, "30.00", "USDT", "USDC convert", category_id=internal.id),
        # An adjustment (ADR-018): asserts the record is incomplete, not money.
        row(binance, TransactionKind.ADJUSTMENT, "5.00", "USDT", "Opening balance"),
        # Unpriced: pesos, and no peso rate anywhere.
        row(nequi, TransactionKind.EXPENSE, "-1000.00", "COP", "arepa"),
    ]
    for txn in rows:
        transactions_repo.insert(web_db, txn)
    return web_db


def _totals(conn: sqlite3.Connection, **filter_kwargs):
    from finances.web.services.transactions_query import (
        TransactionsFilter,
        totals_matching,
    )

    return totals_matching(conn, TransactionsFilter(**filter_kwargs))


# ---------------------------------------------------------------------------
# The service: four piles, one rule.
# ---------------------------------------------------------------------------


def test_totals_follow_the_house_rule(totals_db: sqlite3.Connection) -> None:
    """Transfers, movement-filed rows, adjustments and unpriced rows are
    excluded and counted; the rest is summed in dollars."""
    t = _totals(totals_db)

    assert t.counted == 4
    assert t.moved == 3, "two transfer legs + one row filed under Internal Transfer"
    assert t.adjustments == 1
    assert t.unpriced == 1
    assert t.in_usd == Decimal("200.00")
    assert t.out_usd == Decimal("-22.50")
    assert t.net_usd == Decimal("177.50")


def test_the_piles_partition_the_matches(totals_db: sqlite3.Connection) -> None:
    from finances.web.services.transactions_query import (
        TransactionsFilter,
        count_matching,
    )

    t = _totals(totals_db)
    assert t.counted + t.moved + t.adjustments + t.unpriced == count_matching(
        totals_db, TransactionsFilter()
    )


def test_totals_follow_the_filter(totals_db: sqlite3.Connection) -> None:
    t = _totals(totals_db, kinds=["expense"])

    assert t.counted == 2
    assert t.out_usd == Decimal("-22.50")
    assert t.in_usd == Decimal("0")
    assert t.net_usd == Decimal("-22.50")
    assert t.unpriced == 1, "the peso expense is still an expense"
    assert t.moved == 0 and t.adjustments == 0


def test_totals_cover_every_match_not_just_the_page(
    totals_db: sqlite3.Connection,
) -> None:
    """Thirty one-dollar expenses on a 25-row page still total thirty."""
    from finances.web.services.transactions_query import (
        TransactionsFilter,
        query_transactions,
    )

    provincial = accounts_repo.get_by_name(totals_db, "Provincial")
    assert provincial is not None
    for i in range(30):
        transactions_repo.insert(
            totals_db,
            Transaction(
                account_id=provincial.id,
                occurred_at=datetime.now(tz=UTC),
                kind=TransactionKind.EXPENSE,
                amount=Decimal("-36.50"),
                currency="VES",
                description=f"bulk {i}",
                source="test",
                source_ref=f"bulk-{i}",
            ),
        )

    page = query_transactions(
        totals_db, TransactionsFilter(q="bulk", page_size=25)
    )

    assert len(page.rows) == 25
    assert page.total == 30
    assert page.totals.counted == 30
    assert page.totals.net_usd == Decimal("-30.00")


def test_a_transfer_only_filter_counts_nothing(totals_db: sqlite3.Connection) -> None:
    t = _totals(totals_db, kinds=["transfer"])

    assert t.counted == 0
    assert t.moved == 2
    assert t.net_usd == Decimal("0")


def test_nothing_matched_totals_to_zero(totals_db: sqlite3.Connection) -> None:
    t = _totals(totals_db, q="no such row")

    assert t.counted == 0
    assert t.net_usd == Decimal("0")
    assert t.native_currency is None
    assert t.native_total is None


# ---------------------------------------------------------------------------
# The native total.
# ---------------------------------------------------------------------------


def test_native_total_when_every_spending_row_shares_a_currency(
    totals_db: sqlite3.Connection,
) -> None:
    t = _totals(totals_db, currencies=["VES"])

    assert t.native_currency == "VES"
    assert t.native_total == Decimal("3285.00")
    assert t.net_usd == Decimal("90.00")


def test_native_total_covers_the_rows_no_rate_can_price(
    totals_db: sqlite3.Connection,
) -> None:
    """Summing pesos needs no rate; only pricing them does."""
    t = _totals(totals_db, currencies=["COP"])

    assert t.counted == 0
    assert t.unpriced == 1
    assert t.net_usd == Decimal("0")
    assert t.native_currency == "COP"
    assert t.native_total == Decimal("-1000.00")


def test_no_native_total_across_currencies(totals_db: sqlite3.Connection) -> None:
    t = _totals(totals_db)

    assert t.native_currency is None
    assert t.native_total is None


def test_no_native_total_for_a_dollar_family_currency(
    totals_db: sqlite3.Connection,
) -> None:
    """"87.50 USDT" under "+$87.50" is the same dollars twice (D3)."""
    t = _totals(totals_db, currencies=["USDT"])

    assert t.net_usd == Decimal("87.50")
    assert t.native_currency is None
    assert t.native_total is None


def test_native_total_ignores_moved_rows(totals_db: sqlite3.Connection) -> None:
    """The transfer legs and the adjustment are USDT too; they do not make
    a USDT-only filter's native total, and they do not break a bolívar
    one's currency check either — only spending rows have a say."""
    t = _totals(totals_db, accounts=["Provincial"])

    assert t.native_currency == "VES"
    assert t.native_total == Decimal("3285.00")


# ---------------------------------------------------------------------------
# The page: the figure in the header, the count in the meta line.
# ---------------------------------------------------------------------------


def _page_answer(body: str) -> str:
    m = re.search(r'<h1 class="page-answer">(.*?)</h1>', body, re.S)
    assert m, "no page-answer figure"
    return m.group(1).strip()


def test_header_answers_with_the_net_dollar_figure(
    totals_db: sqlite3.Connection, web_client_factory
) -> None:
    resp = web_client_factory().get("/transactions")

    assert resp.status_code == 200
    assert _page_answer(resp.text) == "+$177.50"
    assert "9 rows" in resp.text, "the count survives, in the meta line"


def test_header_signs_a_net_outflow(
    totals_db: sqlite3.Connection, web_client_factory
) -> None:
    resp = web_client_factory().get("/transactions", params={"kinds": "expense"})

    assert _page_answer(resp.text) == f"{MINUS}$22.50"
    assert "3 rows" in resp.text


def test_header_breaks_net_into_out_and_in_only_when_both_exist(
    totals_db: sqlite3.Connection, web_client_factory
) -> None:
    client = web_client_factory()

    both = client.get("/transactions").text
    assert f'data-total-out>{MINUS}$22.50 out<' in both
    assert 'data-total-in>+$200.00 in<' in both

    one_way = client.get("/transactions", params={"kinds": "expense"}).text
    assert "data-total-out" not in one_way, "the headline already is the outflow"
    assert "data-total-in" not in one_way


def test_header_names_what_it_did_not_count(
    totals_db: sqlite3.Connection, web_client_factory
) -> None:
    client = web_client_factory()

    body = client.get("/transactions").text
    assert "3 transfers not counted" in body
    assert "1 adjustment not counted" in body
    assert "1 can't be priced" in body or "1 can&#39;t be priced" in body

    body = client.get("/transactions", params={"currencies": "VES"}).text
    assert "not counted" not in body
    assert "be priced" not in body


def test_header_shows_the_native_total_when_rows_share_a_currency(
    totals_db: sqlite3.Connection, web_client_factory
) -> None:
    client = web_client_factory()

    ves = client.get("/transactions", params={"currencies": "VES"}).text
    assert f"data-total-native>+Bs.{NBSP}3,285.00<" in ves

    mixed = client.get("/transactions").text
    assert "data-total-native" not in mixed


def test_list_swap_carries_the_totals_out_of_band(
    totals_db: sqlite3.Connection, web_client_factory
) -> None:
    """The filter form swaps #tx-list alone; the header twin rides along."""
    resp = web_client_factory().get(
        "/_partial/transactions/list",
        params={"kinds": "expense"},
        headers={"HX-Request": "true"},
    )

    assert resp.status_code == 200
    assert 'id="transactions-header" hx-swap-oob="true"' in resp.text
    assert _page_answer(resp.text) == f"{MINUS}$22.50"


def test_totals_ride_the_json_page(
    totals_db: sqlite3.Connection, web_client_factory
) -> None:
    resp = web_client_factory().get("/api/transactions", params={"kinds": "expense"})

    assert resp.status_code == 200
    totals = resp.json()["totals"]
    assert Decimal(totals["net_usd"]) == Decimal("-22.50")
    assert totals["unpriced"] == 1
