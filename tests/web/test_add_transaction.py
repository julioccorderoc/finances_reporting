"""Add a transaction from the viewer — built for everything, open for cash.

The owner's instruction was "build it as if it were for everything, but only
cash available to act on". So the dialog lists **every** active account and
disables all but ``Cash USD``, each disabled one saying in its own label what
does feed it. That is a shape the ledger can grow into: the day a second
account becomes hand-writable, one predicate changes and the option lights up.

Three things this file pins:

1. **The predicate is one function.** ``entry_accounts`` decides what the
   ``<option disabled>`` says AND ``add_transaction`` refuses on the same
   rule, so the disabled attribute is a courtesy and never the guard. A
   crafted POST at Provincial comes back 422 in plain words, with nothing
   written.
2. **The write goes through ``cash_cli``.** Not a second INSERT path beside
   it — ``source='cash_cli'``, a UUIDv4 ``source_ref`` (rule-010), the sign
   applied by kind. There is exactly one manual write in this system.
3. **The response tells the truth about the list.** A row the current filter
   shows is pushed into it, with the match count corrected out-of-band; a row
   the filter hides is NOT pushed — it comes back as a toast carrying a link
   to a view that does show it. Silently prepending a hidden row is how a
   filtered list starts lying.
"""

from __future__ import annotations

import json
import re
import sqlite3
from datetime import UTC, date, datetime
from decimal import Decimal

import pytest

from finances.db.repos import accounts as accounts_repo
from finances.db.repos import categories as categories_repo
from finances.db.repos import transactions as transactions_repo
from finances.domain.models import Account, AccountKind, TransactionKind

TODAY = date(2026, 9, 4)


@pytest.fixture
def entry_db(web_db: sqlite3.Connection) -> sqlite3.Connection:
    """One account of every shape the dialog has to explain.

    Migration 020 already seeds two bank accounts; this adds the Binance
    pair and one deactivated account, so "every ACTIVE account" is a claim
    with a counter-example in the fixture.
    """
    accounts_repo.insert(
        web_db,
        Account(
            name="Provincial",
            kind=AccountKind.BANK,
            currency="VES",
            institution="Provincial",
        ),
    )
    accounts_repo.insert(
        web_db,
        Account(
            name="Binance Spot",
            kind=AccountKind.CRYPTO_SPOT,
            currency="USDT",
            institution="Binance",
        ),
    )
    accounts_repo.insert(
        web_db,
        Account(
            name="Old Wallet",
            kind=AccountKind.OTHER,
            currency="USD",
            active=False,
        ),
    )
    return web_db


def _cash_id(conn: sqlite3.Connection) -> int:
    from finances.ingest.cash_cli import ensure_cash_usd_account

    account = ensure_cash_usd_account(conn)
    assert account.id is not None
    return account.id


def _account_id(conn: sqlite3.Connection, name: str) -> int:
    account = accounts_repo.get_by_name(conn, name)
    assert account is not None and account.id is not None
    return account.id


# ---------------------------------------------------------------------------
# entry_accounts — what the select offers, and why each row is closed.
# ---------------------------------------------------------------------------


def test_entry_accounts_lists_every_active_account(
    entry_db: sqlite3.Connection,
) -> None:
    from finances.web.services.transaction_add import entry_accounts

    names = [a.name for a in entry_accounts(entry_db)]

    assert "Cash USD" in names, "the writable account is offered even before its first row"
    assert "Provincial" in names
    assert "Binance Spot" in names
    assert "Bancamiga Bolivares" in names
    assert "Old Wallet" not in names, "deactivated accounts are not offered"


def test_only_the_cash_account_is_writable(entry_db: sqlite3.Connection) -> None:
    from finances.web.services.transaction_add import entry_accounts

    by_name = {a.name: a for a in entry_accounts(entry_db)}

    assert by_name["Cash USD"].writable is True
    assert by_name["Cash USD"].hint is None
    assert by_name["Provincial"].writable is False
    assert by_name["Binance Spot"].writable is False


def test_each_closed_account_says_what_does_feed_it(
    entry_db: sqlite3.Connection,
) -> None:
    """A greyed option with no reason reads as a bug. Every one carries its own."""
    from finances.web.services.transaction_add import entry_accounts

    by_name = {a.name: a for a in entry_accounts(entry_db)}

    assert by_name["Provincial"].hint == "fed by its statement"
    assert by_name["Binance Spot"].hint == "fed by the API"


def test_the_cash_account_carries_its_currency(
    entry_db: sqlite3.Connection,
) -> None:
    """The amount field's currency is the account's, never the user's choice."""
    from finances.web.services.transaction_add import entry_accounts

    by_name = {a.name: a for a in entry_accounts(entry_db)}
    assert by_name["Cash USD"].currency == "USD"
    assert by_name["Provincial"].currency == "VES"


# ---------------------------------------------------------------------------
# add_transaction — the write, and the refusal.
# ---------------------------------------------------------------------------


def test_add_transaction_writes_a_cash_expense_through_cash_cli(
    entry_db: sqlite3.Connection,
) -> None:
    from finances.ingest.cash_cli import CASH_CLI_SOURCE
    from finances.web.services.transaction_add import (
        NewTransactionRequest,
        add_transaction,
    )

    card = add_transaction(
        entry_db,
        NewTransactionRequest(
            account_id=_cash_id(entry_db),
            occurred_at=TODAY,
            kind="expense",
            amount=Decimal("12.50"),
            description="empanadas",
        ),
    )

    assert card.account_name == "Cash USD"
    assert card.amount_native == Decimal("-12.50"), "expenses are stored negative"
    assert card.currency == "USD"
    assert card.rate_source == "native_usd"

    stored = transactions_repo.get_by_id(entry_db, card.id)
    assert stored is not None
    assert stored.source == CASH_CLI_SOURCE
    assert stored.kind is TransactionKind.EXPENSE
    assert stored.occurred_at.date() == TODAY


def test_add_transaction_writes_a_cash_income(entry_db: sqlite3.Connection) -> None:
    from finances.web.services.transaction_add import (
        NewTransactionRequest,
        add_transaction,
    )

    card = add_transaction(
        entry_db,
        NewTransactionRequest(
            account_id=_cash_id(entry_db),
            occurred_at=TODAY,
            kind="income",
            amount=Decimal("40"),
            description="Andrés paid me back",
        ),
    )
    assert card.amount_native == Decimal("40")
    assert card.kind == "income"


def test_add_transaction_keeps_the_category_and_the_note(
    entry_db: sqlite3.Connection,
) -> None:
    from finances.web.services.transaction_add import (
        NewTransactionRequest,
        add_transaction,
    )

    groceries = categories_repo.get_by_name(
        entry_db, TransactionKind.EXPENSE, "Groceries"
    )
    assert groceries is not None

    card = add_transaction(
        entry_db,
        NewTransactionRequest(
            account_id=_cash_id(entry_db),
            occurred_at=TODAY,
            kind="expense",
            amount=Decimal("12.50"),
            description="market",
            category_id=groceries.id,
            notes="split with Ana",
        ),
    )
    assert card.category_name == "Groceries"
    assert card.notes == "split with Ana"


def test_add_transaction_refuses_a_non_cash_account_in_plain_words(
    entry_db: sqlite3.Connection,
) -> None:
    from finances.web.services.transaction_add import (
        NewTransactionRequest,
        add_transaction,
    )

    before = entry_db.execute("SELECT COUNT(*) AS c FROM transactions").fetchone()["c"]

    with pytest.raises(ValueError) as exc:
        add_transaction(
            entry_db,
            NewTransactionRequest(
                account_id=_account_id(entry_db, "Provincial"),
                occurred_at=TODAY,
                kind="expense",
                amount=Decimal("100"),
                description="by hand",
            ),
        )

    message = str(exc.value)
    assert "Provincial" in message
    assert "fed by its statement" in message
    assert "Cash USD" in message
    assert "ValidationError" not in message and "ValueError" not in message

    after = entry_db.execute("SELECT COUNT(*) AS c FROM transactions").fetchone()["c"]
    assert after == before, "a refused write leaves nothing behind"


def test_add_transaction_refuses_an_unknown_account(
    entry_db: sqlite3.Connection,
) -> None:
    from finances.web.services.transaction_add import (
        NewTransactionRequest,
        add_transaction,
    )

    with pytest.raises(LookupError):
        add_transaction(
            entry_db,
            NewTransactionRequest(
                account_id=99999,
                occurred_at=TODAY,
                kind="expense",
                amount=Decimal("1"),
                description="nowhere",
            ),
        )


def test_add_transaction_refuses_a_category_that_contradicts_the_kind(
    entry_db: sqlite3.Connection,
) -> None:
    """The same guard ``apply_edit`` has — the picker is scoped, this is the wall."""
    from finances.web.services.transaction_add import (
        NewTransactionRequest,
        add_transaction,
    )

    salary = categories_repo.get_by_name(entry_db, TransactionKind.INCOME, "Salary")
    assert salary is not None

    with pytest.raises(ValueError, match="Salary"):
        add_transaction(
            entry_db,
            NewTransactionRequest(
                account_id=_cash_id(entry_db),
                occurred_at=TODAY,
                kind="expense",
                amount=Decimal("10"),
                description="not a salary",
                category_id=salary.id,
            ),
        )


@pytest.mark.parametrize("amount", ["0", "-5"])
def test_add_transaction_rejects_a_non_positive_amount(
    entry_db: sqlite3.Connection, amount: str
) -> None:
    from pydantic import ValidationError

    from finances.web.services.transaction_add import NewTransactionRequest

    with pytest.raises(ValidationError):
        NewTransactionRequest(
            account_id=1,
            occurred_at=TODAY,
            kind="expense",
            amount=Decimal(amount),
            description="x",
        )


def test_add_transaction_rejects_a_blank_description(
    entry_db: sqlite3.Connection,
) -> None:
    from pydantic import ValidationError

    from finances.web.services.transaction_add import NewTransactionRequest

    with pytest.raises(ValidationError):
        NewTransactionRequest(
            account_id=1,
            occurred_at=TODAY,
            kind="expense",
            amount=Decimal("1"),
            description="   ",
        )


# ---------------------------------------------------------------------------
# The dialog.
# ---------------------------------------------------------------------------


def test_the_flow_header_offers_add_transaction(
    entry_db: sqlite3.Connection, web_client_factory
) -> None:
    html = web_client_factory().get("/transactions").text

    assert "data-add-transaction" in html
    assert "Add transaction" in html
    assert 'hx-get="/_partial/transactions/new"' in html
    assert 'hx-target="#tx-modal-host"' in html
    # In the header's actions slot, not floating somewhere in the body.
    actions = re.search(r'<div class="page-actions">(.*?)</div>', html, re.S)
    assert actions is not None and "data-add-transaction" in actions.group(1)


def test_the_dialog_lists_every_account_with_only_cash_enabled(
    entry_db: sqlite3.Connection, web_client_factory
) -> None:
    resp = web_client_factory().get("/_partial/transactions/new")
    assert resp.status_code == 200
    html = resp.text

    cash_id = _cash_id(entry_db)
    prov_id = _account_id(entry_db, "Provincial")

    assert 'class="flow-modal-over"' in html
    assert "data-new-transaction-modal" in html

    options = re.findall(r"<option\b[^>]*>[^<]*</option>", html)
    labels = " ".join(options)
    assert "Cash USD" in labels
    assert "Provincial" in labels
    assert "Binance Spot" in labels
    assert "Old Wallet" not in labels

    cash_option = next(o for o in options if f'value="{cash_id}"' in o)
    assert "disabled" not in cash_option

    prov_option = next(o for o in options if f'value="{prov_id}"' in o)
    assert "disabled" in prov_option
    assert "fed by its statement" in prov_option


def test_the_dialog_defaults_to_cash_today_and_an_expense(
    entry_db: sqlite3.Connection, web_client_factory
) -> None:
    html = web_client_factory().get("/_partial/transactions/new").text

    cash_id = _cash_id(entry_db)
    assert re.search(rf'<option[^>]*value="{cash_id}"[^>]*selected', html)
    assert re.search(r'name="occurred_at"[^>]*value="\d{4}-\d{2}-\d{2}"', html)
    assert re.search(r'name="kind"[^>]*value="expense"[^>]*checked', html)
    assert 'value="income"' in html


def test_the_dialog_posts_the_new_row_into_the_list(
    entry_db: sqlite3.Connection, web_client_factory
) -> None:
    html = web_client_factory().get("/_partial/transactions/new").text

    assert 'hx-post="/_partial/transactions"' in html
    # After the column headings, never before them: `.flow-head` is the
    # first child of `.flow-rows`, so `afterbegin` on that container puts
    # the new row above "DATE DESCRIPTION ACCOUNT …".
    assert 'hx-target="#tx-list .flow-head"' in html
    assert 'hx-swap="afterend"' in html


def test_the_dialog_scopes_the_picker_to_the_chosen_kind(
    entry_db: sqlite3.Connection, web_client_factory
) -> None:
    client = web_client_factory()

    expense = client.get("/_partial/transactions/new").text
    assert "Groceries" in expense
    assert "Salary" not in expense, "an income category on an expense is a 422 waiting"

    income = client.get(
        "/_partial/transactions/new/categories", params={"kind": "income"}
    )
    assert income.status_code == 200
    assert "Salary" in income.text
    assert "Groceries" not in income.text

    # And the kind control is what re-fetches it.
    assert 'hx-get="/_partial/transactions/new/categories"' in expense
    assert 'hx-target="#new-tx-categories"' in expense


def test_the_dialog_names_the_currency_the_account_fixes(
    entry_db: sqlite3.Connection, web_client_factory
) -> None:
    html = web_client_factory().get("/_partial/transactions/new").text
    cash_id = _cash_id(entry_db)
    assert re.search(rf'<option[^>]*value="{cash_id}"[^>]*data-currency="USD"', html)
    assert "data-entry-currency" in html


# ---------------------------------------------------------------------------
# POST /_partial/transactions
# ---------------------------------------------------------------------------


def _post(client, entry_db, **overrides):
    data = {
        "account_id": str(_cash_id(entry_db)),
        "occurred_at": datetime.now(tz=UTC).date().isoformat(),
        "kind": "expense",
        "amount": "12.50",
        "description": "empanadas",
        "notes": "",
        "category_id": "",
    }
    data.update(overrides.pop("data", {}))
    headers = {"HX-Request": "true"}
    headers.update(overrides.pop("headers", {}))
    return client.post("/_partial/transactions", data=data, headers=headers)


def test_post_returns_the_new_card_and_closes_the_dialog(
    entry_db: sqlite3.Connection, web_client_factory
) -> None:
    client = web_client_factory()
    resp = _post(
        client, entry_db, headers={"HX-Current-URL": "http://testserver/transactions"}
    )

    assert resp.status_code == 200
    row = transactions_repo.get_by_id(entry_db, 1)
    assert row is not None

    assert f'data-tx-id="{row.id}"' in resp.text
    assert 'class="flow-row"' in resp.text
    assert "data-bulk-checkbox" in resp.text, "the card keeps the list's checkbox track"

    trigger = json.loads(resp.headers["HX-Trigger"])
    assert "empanadas" in trigger["toast"]["message"]
    assert trigger["toast"]["level"] == "success"


def test_post_closes_the_dialog_by_swap_not_by_trigger(
    entry_db: sqlite3.Connection, web_client_factory
) -> None:
    """The browser-only defect this cost: an early close drops every OOB.

    htmx fires ``HX-Trigger`` events BEFORE the swap and dispatches a
    kebab-case twin of every camelCase name, so ``closeModal`` also fires
    ``close-modal`` — base.html empties the modal host, and the submitting
    form is detached while htmx is still resolving the response. htmx
    resolves OOB targets from the requesting element's root node, and a
    detached form's root node is not the document: the count, the headline
    and the empty-state swaps were all dropped in silence. Every
    server-side assertion passed the whole time.
    """
    client = web_client_factory()
    resp = _post(
        client, entry_db, headers={"HX-Current-URL": "http://testserver/transactions"}
    )

    assert "closeModal" not in resp.headers["HX-Trigger"]
    assert 'id="tx-modal-host" hx-swap-oob="innerHTML"' in resp.text


def test_post_corrects_the_page_headline_out_of_band(
    entry_db: sqlite3.Connection, web_client_factory
) -> None:
    """"3 rows" over four rows is the same lie the count would tell."""
    client = web_client_factory()
    resp = _post(
        client, entry_db, headers={"HX-Current-URL": "http://testserver/transactions"}
    )

    assert 'id="transactions-header" hx-swap-oob="true"' in resp.text
    assert "1 row" in resp.text
    assert "data-add-transaction" in resp.text, "the header keeps its own control"
    # And it keeps the date window it was showing. The filter rebuilt from
    # HX-Current-URL has to be resolved first, or the window silently goes.
    assert 'class="flow-window"' in resp.text


def test_post_corrects_the_match_count_and_clears_the_empty_state(
    entry_db: sqlite3.Connection, web_client_factory
) -> None:
    """A pushed row that leaves "No rows match these filters" above it is a lie."""
    client = web_client_factory()
    resp = _post(
        client, entry_db, headers={"HX-Current-URL": "http://testserver/transactions"}
    )

    assert 'id="tx-count"' in resp.text
    assert 'hx-swap-oob="true"' in resp.text
    assert "1 match" in resp.text
    assert 'id="tx-empty"' in resp.text
    assert 'hx-swap-oob="delete"' in resp.text


def test_post_does_not_push_a_row_the_filter_hides(
    entry_db: sqlite3.Connection, web_client_factory
) -> None:
    client = web_client_factory()
    resp = _post(
        client,
        entry_db,
        headers={"HX-Current-URL": "http://testserver/transactions?kinds=income"},
    )

    assert resp.status_code == 200
    assert "flow-row" not in resp.text, "an expense must not appear in an income-only list"
    assert "tx-count" not in resp.text, "and the count it does not belong to stands"
    assert 'id="tx-modal-host" hx-swap-oob="innerHTML"' in resp.text, "dialog still closes"

    trigger = json.loads(resp.headers["HX-Trigger"])
    toast = trigger["toast"]
    assert "filter" in toast["message"].lower()
    assert toast["href"].startswith("/transactions?")
    assert "Cash+USD" in toast["href"] or "Cash%20USD" in toast["href"]

    # It is still in the ledger — only the list declined to show it.
    assert (
        entry_db.execute("SELECT COUNT(*) AS c FROM transactions").fetchone()["c"] == 1
    )


def test_post_does_not_push_onto_a_page_other_than_the_first(
    entry_db: sqlite3.Connection, web_client_factory
) -> None:
    client = web_client_factory()
    resp = _post(
        client,
        entry_db,
        headers={"HX-Current-URL": "http://testserver/transactions?page=3"},
    )
    assert "flow-row" not in resp.text
    assert json.loads(resp.headers["HX-Trigger"])["toast"]["href"]


def test_post_refuses_a_non_cash_account_with_422(
    entry_db: sqlite3.Connection, web_client_factory
) -> None:
    """The disabled <option> is a courtesy. This is the guard."""
    client = web_client_factory()
    resp = _post(
        client,
        entry_db,
        data={"account_id": str(_account_id(entry_db, "Provincial"))},
        headers={"HX-Current-URL": "http://testserver/transactions"},
    )

    assert resp.status_code == 422
    detail = resp.json()["detail"]
    assert "Provincial" in detail
    assert "fed by its statement" in detail
    assert (
        entry_db.execute("SELECT COUNT(*) AS c FROM transactions").fetchone()["c"] == 0
    )


def test_post_refuses_an_unknown_account_with_404(
    entry_db: sqlite3.Connection, web_client_factory
) -> None:
    client = web_client_factory()
    resp = _post(client, entry_db, data={"account_id": "99999"})
    assert resp.status_code == 404
    # Not Starlette's bare "Not Found" for a route that does not exist — the
    # handler's own words, naming what it could not find.
    assert "99999" in resp.json()["detail"]


@pytest.mark.parametrize(
    ("field", "value"),
    [("amount", "0"), ("amount", "not-a-number"), ("description", "  ")],
)
def test_post_refuses_a_malformed_entry_with_422(
    entry_db: sqlite3.Connection, web_client_factory, field: str, value: str
) -> None:
    client = web_client_factory()
    resp = _post(client, entry_db, data={field: value})
    assert resp.status_code == 422
    assert (
        entry_db.execute("SELECT COUNT(*) AS c FROM transactions").fetchone()["c"] == 0
    )


def test_post_writes_one_row_per_submit(
    entry_db: sqlite3.Connection, web_client_factory
) -> None:
    """Two identical submits are two real entries — a UUID ref, not a hash (rule-010)."""
    client = web_client_factory()
    _post(client, entry_db, headers={"HX-Current-URL": "http://testserver/transactions"})
    _post(client, entry_db, headers={"HX-Current-URL": "http://testserver/transactions"})

    rows = entry_db.execute(
        "SELECT source, source_ref FROM transactions ORDER BY id"
    ).fetchall()
    assert len(rows) == 2
    assert {r["source"] for r in rows} == {"cash_cli"}
    assert rows[0]["source_ref"] != rows[1]["source_ref"]


def test_post_with_a_category_and_note_keeps_both(
    entry_db: sqlite3.Connection, web_client_factory
) -> None:
    groceries = categories_repo.get_by_name(
        entry_db, TransactionKind.EXPENSE, "Groceries"
    )
    assert groceries is not None

    client = web_client_factory()
    resp = _post(
        client,
        entry_db,
        data={"category_id": str(groceries.id), "notes": "split with Ana"},
        headers={"HX-Current-URL": "http://testserver/transactions"},
    )
    assert resp.status_code == 200
    assert "Groceries" in resp.text
    assert "split with Ana" in resp.text


# ---------------------------------------------------------------------------
# The toast has to be able to carry that link.
# ---------------------------------------------------------------------------


def test_the_toast_renders_a_link_when_the_trigger_carries_one() -> None:
    from pathlib import Path

    base = (
        Path(__file__).resolve().parents[2]
        / "finances"
        / "web"
        / "templates"
        / "base.html"
    ).read_text(encoding="utf-8")

    assert "toast-link" in base
    assert "t.href" in base
    # Closing the dialog once must not kill the button. base.html cleared
    # its ``modalDismissed`` guard only for a path containing "/modal",
    # which this dialog's URL does not — so it re-armed on the first Escape
    # and every later open was suppressed at before-swap, silently. The
    # guard now keys off the swap TARGET being a modal host.
    assert "['tx-modal-host', 'triage-modal-host'].includes(target.id)" in base
    # And it does not evaporate before it can be clicked: a linked toast
    # persists like an error one, and gains the dismiss control with it.
    assert "t.level === 'success' && !t.href" in base
    assert "t.level === 'error' || t.href" in base


# ---------------------------------------------------------------------------
# The decision is written down (ADR-008 amendment, rule-008).
# ---------------------------------------------------------------------------


def test_adr_008_records_the_viewer_as_a_cash_entry_point() -> None:
    from pathlib import Path

    adr = (
        Path(__file__).resolve().parents[2]
        / "docs"
        / "ADR"
        / "ADR-008-cash-usd-only-cli.md"
    ).read_text(encoding="utf-8")

    assert "Amendment" in adr
    assert "viewer" in adr
    assert "2026-09-03" in adr


def test_rule_008_says_the_viewer_writes_through_the_cash_module() -> None:
    from pathlib import Path

    rule = (
        Path(__file__).resolve().parents[2]
        / "docs"
        / "architecture"
        / "rules"
        / "rule-008-cash-account-scope.md"
    ).read_text(encoding="utf-8")

    assert "viewer" in rule
    assert "cash_cli" in rule
    assert "importer" in rule


def test_post_reads_the_whole_real_page_url_not_just_a_tidy_one(
    entry_db: sqlite3.Connection, web_client_factory
) -> None:
    """The URL htmx actually sends is the one the filter form serialised.

    It carries every control, ``page_size`` included — and ``page_size`` is
    ``Literal[25, 50, 100]``, which pydantic will not build from the string
    ``"50"``. The whole filter therefore failed validation and fell back to
    defaults, so a row outside a one-day window was reported as visible and
    the count came back as the unfiltered total. Only a browser sees it: a
    handwritten ``HX-Current-URL`` in a test is always tidy.
    """
    client = web_client_factory()
    real_url = (
        "http://testserver/transactions"
        "?date_from=2026-01-01&date_to=2026-01-01&q=&needs_review=any"
        "&paired=any&sort=occurred_at&direction=desc&page=1&page_size=50"
    )
    resp = _post(client, entry_db, headers={"HX-Current-URL": real_url})

    assert resp.status_code == 200
    # Today's row is nowhere near that window.
    assert "flow-row" not in resp.text
    assert "0 row" in resp.text, "the headline must describe the window, not the ledger"
    trigger = json.loads(resp.headers["HX-Trigger"])
    assert trigger["toast"]["href"]
