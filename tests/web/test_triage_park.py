"""Park replaces the session-local Skip (spec §5.3).

The old Skip stored ids in a per-process set on app.state, which the
Stop-server button destroyed. Park writes a column, so a deferral outlives
the session — and outlives a server restart, which these tests simulate by
building a second app against the same database file.
"""

from __future__ import annotations

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
def park_db(web_db: sqlite3.Connection) -> sqlite3.Connection:
    account = accounts_repo.insert(
        web_db, Account(name="Provincial", kind=AccountKind.BANK, currency="VES")
    )
    for n in range(3):
        transactions_repo.insert(
            web_db,
            Transaction(
                account_id=account.id,
                occurred_at=datetime(2026, 5, 1 + n, tzinfo=UTC),
                kind=TransactionKind.EXPENSE,
                amount=Decimal("-100.00"),
                currency="VES",
                description=f"row {n}",
                source="provincial",
                source_ref=f"park-{n}",
                needs_review=True,
            ),
        )
    return web_db


def test_park_removes_item_from_the_main_queue(
    park_db: sqlite3.Connection, web_client_factory
) -> None:
    client: TestClient = web_client_factory()
    assert "txn:1" in client.get("/api/triage").text

    client.post("/_partial/triage/1/park")

    assert transactions_repo.get_by_id(park_db, 1).parked is True
    # Parking removes the row from the MAIN queue (Task 2's contract).
    # It still shows up under queue.parked_items (Task 3), so the check
    # is scoped to `items`, not the whole payload.
    item_ids = [i["item_id"] for i in client.get("/api/triage").json()["items"]]
    assert "txn:1" not in item_ids


def test_park_survives_a_server_restart(
    park_db: sqlite3.Connection, web_client_factory
) -> None:
    """The exact failure the in-memory skip store had."""
    first: TestClient = web_client_factory()
    first.post("/_partial/triage/1/park")

    # A brand-new app against the same DB file == a restarted server.
    second: TestClient = web_client_factory()

    # Same scoping as above: gone from the main queue, not from the
    # payload entirely (it now lives in parked_items).
    item_ids = [i["item_id"] for i in second.get("/api/triage").json()["items"]]
    assert "txn:1" not in item_ids


def test_bring_back_all_returns_every_parked_row(
    park_db: sqlite3.Connection, web_client_factory
) -> None:
    """F9 — the parked sheet's one way out.

    The redesign dropped the per-row unpark: the sheet offers *Bring back
    all N* and nothing else, and an endpoint no surface calls is worse
    than one capability fewer. Order needs no restoring — the queue sorts
    itself by ``(bucket, occurred_at, item_id)``, so rows come back
    oldest-first by construction.
    """
    client: TestClient = web_client_factory()
    client.post("/_partial/triage/1/park")
    client.post("/_partial/triage/2/park")

    response = client.post("/_partial/triage/unpark-all")

    assert response.status_code == 200
    assert transactions_repo.get_by_id(park_db, 1).parked is False
    assert transactions_repo.get_by_id(park_db, 2).parked is False
    assert "2 rows back in the queue" in response.headers["HX-Trigger"]
    assert "txn:1" in client.get("/api/triage").text


def test_the_single_row_unpark_endpoint_is_gone(
    park_db: sqlite3.Connection, web_client_factory
) -> None:
    client: TestClient = web_client_factory()

    assert client.post("/_partial/triage/1/unpark").status_code == 404


def test_the_cutoff_parks_every_uncategorised_row_before_a_date(
    park_db: sqlite3.Connection, web_client_factory
) -> None:
    """F3 — one call, through domain.triage_admin.park_before."""
    client: TestClient = web_client_factory()

    response = client.post(
        "/_partial/triage/park-before", data={"before": "2030-01-01"}
    )

    assert response.status_code == 200
    assert "Parking everything uncategorised before" in response.headers[
        "HX-Trigger"
    ]
    assert transactions_repo.get_by_id(park_db, 1).parked is True


def test_a_bad_cutoff_is_refused_rather_than_guessed(
    park_db: sqlite3.Connection, web_client_factory
) -> None:
    client: TestClient = web_client_factory()

    assert (
        client.post(
            "/_partial/triage/park-before", data={"before": "not-a-date"}
        ).status_code
        == 422
    )


def test_selected_rows_park_in_one_call(
    park_db: sqlite3.Connection, web_client_factory
) -> None:
    """F2 — the selection bar's Park, with a count in the toast."""
    client: TestClient = web_client_factory()

    response = client.post("/_partial/triage/bulk-park", data={"ids": "1,2"})

    assert response.status_code == 200
    assert "2 rows parked." in response.headers["HX-Trigger"]
    assert transactions_repo.get_by_id(park_db, 1).parked is True
    assert transactions_repo.get_by_id(park_db, 2).parked is True


def test_park_does_not_alter_needs_review(
    park_db: sqlite3.Connection, web_client_factory
) -> None:
    client: TestClient = web_client_factory()
    client.post("/_partial/triage/1/park")

    assert transactions_repo.get_by_id(park_db, 1).needs_review is True


def test_park_on_unknown_id_is_404(
    park_db: sqlite3.Connection, web_client_factory
) -> None:
    client: TestClient = web_client_factory()

    assert client.post("/_partial/triage/9999/park").status_code == 404


def test_skip_endpoint_and_store_are_gone(
    park_db: sqlite3.Connection, web_client_factory
) -> None:
    """The old surface must be removed, not left as a second way to defer."""
    import finances.web.services.triage as triage_service

    assert not hasattr(triage_service, "get_skip_store")

    client: TestClient = web_client_factory()
    assert client.post("/_partial/triage/skip/txn:1").status_code == 404


def test_modal_offers_park_not_skip(
    park_db: sqlite3.Connection, web_client_factory
) -> None:
    client: TestClient = web_client_factory()
    html = client.get("/_partial/triage/1/modal").text

    assert "data-park-btn" in html
    assert "data-skip-btn" not in html


def test_parked_items_are_collected_separately(
    park_db: sqlite3.Connection, web_client_factory
) -> None:
    from finances.web.services.triage import build_queue

    client: TestClient = web_client_factory()
    client.post("/_partial/triage/1/park")

    queue = build_queue(park_db)

    assert [i.item_id for i in queue.items] == ["txn:2", "txn:3"]
    assert [i.item_id for i in queue.parked_items] == ["txn:1"]
    assert queue.parked_count == 1


def test_the_parked_strip_appears_with_a_way_into_the_sheet(
    park_db: sqlite3.Connection, web_client_factory
) -> None:
    client: TestClient = web_client_factory()
    client.post("/_partial/triage/1/park")

    html = client.get("/triage").text

    assert "data-parked-strip" in html
    assert "parked row, out of the queue" in html
    assert "/_partial/triage/parked" in html


def test_no_parked_strip_when_nothing_is_parked(
    park_db: sqlite3.Connection, web_client_factory
) -> None:
    client: TestClient = web_client_factory()

    assert "data-parked-strip" not in client.get("/triage").text
