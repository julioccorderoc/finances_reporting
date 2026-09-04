"""«Became cash» inside the triage run.

The Flow modal has offered this since ADR-008's amendment, but triage is
where the owner actually meets these rows — one at a time, in a run they
are not supposed to leave. Sending a row to cash from /transactions meant
abandoning the sitting, finding the row again on another screen, and
coming back; the whole point of the run is that a decision about a row is
made where the row is asking.

So the same one-field panel is reachable from the triage footer, and
recording it behaves like every other triage decision: the response IS
the next dialog (ADR-012 Amendment 2026-07-26), never a closed modal
mid-run.

The write is the same domain call the Flow route makes
(``cash_conversion.convert_to_cash``). Only the answer differs.
"""

from __future__ import annotations

import json
import sqlite3
from datetime import UTC, datetime
from decimal import Decimal

import pytest
from fastapi.testclient import TestClient

from finances.db.repos import accounts as accounts_repo
from finances.db.repos import transactions as transactions_repo
from finances.domain.models import (
    Account,
    AccountKind,
    Transaction,
    TransactionKind,
)


@pytest.fixture
def cash_triage_db(web_db: sqlite3.Connection) -> sqlite3.Connection:
    """A run of three uncategorised rows, and somewhere for dollars to land.

    Ids 1..3 are the outgoing Binance rows the owner triages; id 4 is money
    arriving, which is the other half of a conversion and never its origin.
    """
    accounts_repo.insert(
        web_db, Account(name="Cash", kind=AccountKind.CASH, currency="USD")
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
    for n in range(3):
        transactions_repo.insert(
            web_db,
            Transaction(
                account_id=binance.id,
                occurred_at=datetime(2026, 5, 1 + n, tzinfo=UTC),
                kind=TransactionKind.EXPENSE,
                amount=Decimal("-300.00"),
                currency="USDT",
                description="Binance Pay C2C (outgoing)",
                source="binance",
                source_ref=f"c2c-{n}",
                needs_review=True,
            ),
        )
    transactions_repo.insert(
        web_db,
        Transaction(
            account_id=binance.id,
            occurred_at=datetime(2026, 5, 9, tzinfo=UTC),
            kind=TransactionKind.INCOME,
            amount=Decimal("300.00"),
            currency="USDT",
            description="Binance Pay C2C (incoming)",
            source="binance",
            source_ref="c2c-in",
            needs_review=True,
        ),
    )
    return web_db


def _modal(client: TestClient, txn_id: int) -> str:
    resp = client.get(f"/_partial/triage/{txn_id}/modal")
    assert resp.status_code == 200, resp.text
    return resp.text


# ---------------------------------------------------------------------------
# The footer control
# ---------------------------------------------------------------------------


def test_an_outgoing_row_offers_became_cash_in_the_run(
    cash_triage_db: sqlite3.Connection, web_client_factory
) -> None:
    client: TestClient = web_client_factory()

    assert "Became cash" in _modal(client, 1)


def test_an_incoming_row_does_not(
    cash_triage_db: sqlite3.Connection, web_client_factory
) -> None:
    client: TestClient = web_client_factory()

    assert "Became cash" not in _modal(client, 4)


def test_the_reveal_host_sits_outside_the_decision_form(
    cash_triage_db: sqlite3.Connection, web_client_factory
) -> None:
    """A ``<form>`` inside a ``<form>`` is dropped by every browser.

    The panel carries its own form, so nesting it inside the decision form
    would leave a Record button that posts nothing — silently, with every
    assertion in this file still green. Only the ordering of the two tags
    can catch it here.
    """
    client: TestClient = web_client_factory()
    html = _modal(client, 1)

    host = html.index('id="became-cash-host"')
    form_open = html.index('id="triage-decision-form"')
    form_close = html.index("</form>", form_open)

    assert not form_open < host < form_close, (
        "the became-cash host is nested inside #triage-decision-form"
    )


# ---------------------------------------------------------------------------
# The panel
# ---------------------------------------------------------------------------


def test_the_panel_posts_back_to_the_triage_endpoint(
    cash_triage_db: sqlite3.Connection, web_client_factory
) -> None:
    """Same panel, triage's answer: the next dialog, swapped into the run."""
    client: TestClient = web_client_factory()

    resp = client.get("/_partial/triage/1/became-cash")

    assert resp.status_code == 200, resp.text
    assert 'hx-post="/_partial/triage/1/became-cash"' in resp.text
    assert 'hx-target="#triage-modal-host"' in resp.text


def test_the_panel_refuses_a_row_that_cannot_have_become_cash(
    cash_triage_db: sqlite3.Connection, web_client_factory
) -> None:
    """The hidden button is a courtesy; this is the guard."""
    client: TestClient = web_client_factory()

    resp = client.get("/_partial/triage/4/became-cash")

    assert resp.status_code == 422, resp.text


# ---------------------------------------------------------------------------
# Recording it
# ---------------------------------------------------------------------------


def test_recording_pairs_the_row_with_a_cash_leg(
    cash_triage_db: sqlite3.Connection, web_client_factory
) -> None:
    client: TestClient = web_client_factory()

    resp = client.post(
        "/_partial/triage/1/became-cash", data={"usd_received": "298.00"}
    )

    assert resp.status_code == 200, resp.text
    txn = transactions_repo.get_by_id(cash_triage_db, 1)
    assert txn.transfer_id is not None


def test_recording_answers_with_the_next_dialog(
    cash_triage_db: sqlite3.Connection, web_client_factory
) -> None:
    """A triage decision advances the run; it does not end it."""
    client: TestClient = web_client_factory()

    resp = client.post(
        "/_partial/triage/2/became-cash", data={"usd_received": "298.00"}
    )

    assert resp.status_code == 200, resp.text
    assert 'data-txn-id="3"' in resp.text
    payload = json.loads(resp.headers["HX-Trigger"])
    assert "closeModal" not in payload
    assert payload["queueDirty"] == {"typeFilter": None}


def test_the_toast_names_the_dollars(
    cash_triage_db: sqlite3.Connection, web_client_factory
) -> None:
    client: TestClient = web_client_factory()

    resp = client.post(
        "/_partial/triage/1/became-cash", data={"usd_received": "298.00"}
    )

    payload = json.loads(resp.headers["HX-Trigger"])
    assert "298.00" in payload["toast"]["message"]


def test_recording_refuses_a_row_that_cannot_have_become_cash(
    cash_triage_db: sqlite3.Connection, web_client_factory
) -> None:
    client: TestClient = web_client_factory()

    resp = client.post(
        "/_partial/triage/4/became-cash", data={"usd_received": "298.00"}
    )

    assert resp.status_code == 422, resp.text


def test_an_unreadable_amount_is_refused(
    cash_triage_db: sqlite3.Connection, web_client_factory
) -> None:
    client: TestClient = web_client_factory()

    resp = client.post(
        "/_partial/triage/1/became-cash", data={"usd_received": "three hundred"}
    )

    assert resp.status_code == 422, resp.text
