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
    assert "txn:1" not in client.get("/api/triage").text


def test_park_survives_a_server_restart(
    park_db: sqlite3.Connection, web_client_factory
) -> None:
    """The exact failure the in-memory skip store had."""
    first: TestClient = web_client_factory()
    first.post("/_partial/triage/1/park")

    # A brand-new app against the same DB file == a restarted server.
    second: TestClient = web_client_factory()

    assert "txn:1" not in second.get("/api/triage").text


def test_unpark_returns_the_item_to_the_queue(
    park_db: sqlite3.Connection, web_client_factory
) -> None:
    client: TestClient = web_client_factory()
    client.post("/_partial/triage/1/park")

    client.post("/_partial/triage/1/unpark")

    assert transactions_repo.get_by_id(park_db, 1).parked is False
    assert "txn:1" in client.get("/api/triage").text


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


def test_parked_group_renders_with_an_unpark_action(
    park_db: sqlite3.Connection, web_client_factory
) -> None:
    client: TestClient = web_client_factory()
    client.post("/_partial/triage/1/park")

    html = client.get("/triage").text

    assert "data-parked-group" in html
    assert "/_partial/triage/1/unpark" in html


def test_no_parked_group_when_nothing_is_parked(
    park_db: sqlite3.Connection, web_client_factory
) -> None:
    client: TestClient = web_client_factory()

    assert "data-parked-group" not in client.get("/triage").text
