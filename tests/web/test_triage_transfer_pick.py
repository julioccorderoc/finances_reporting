"""Triage offers the two transfer categories again — "moved, not spent".

Owner report 2026-09-03: *"I can't pick the transfer options on the triage
any more... sometimes money is entering in a transitional way, not for my
expenses/income."* The Wave 2 picker reads ``list_pickable()``, which
migration 021 had emptied of transfer categories; the write path never
refused them (``category_fits`` is asymmetric on purpose). Migration 022
puts them back, and the picker scopes them onto every income and expense
row, in a third group after EXPENSE and INCOME, never on a numbered chip.
"""

from __future__ import annotations

import re
import sqlite3
from collections.abc import Callable
from pathlib import Path

from starlette.testclient import TestClient

from finances.db.repos import categories as categories_repo
from finances.domain.models import TransactionKind
from finances.web.services.triage import count_blocking

JS = Path(__file__).resolve().parents[2] / "finances" / "web" / "static" / "js"


def _modal(factory: Callable[[], TestClient], txn_id: int = 1) -> str:
    with factory() as client:
        response = client.get(f"/_partial/triage/{txn_id}/modal")
    assert response.status_code == 200
    return response.text


def _rows(html: str, kind: str) -> list[str]:
    return re.findall(rf'<button[^>]*class="catrow"[^>]*data-kind="{kind}"[^>]*>', html)


def test_an_expense_row_is_offered_both_transfer_categories(
    triage_web_db: sqlite3.Connection,
    web_client_factory: Callable[[], TestClient],
) -> None:
    """txn 1 is an uncategorised bank expense; the picker on it scopes to
    expense — and to the movement categories, which fit any row."""
    modal = _modal(web_client_factory, 1)

    labels = {
        m.group(1)
        for m in re.finditer(r'data-kind="transfer"[^>]*data-label="([^"]+)"', modal)
    } | {
        m.group(1)
        for m in re.finditer(r'data-label="([^"]+)"[^>]*data-kind="transfer"', modal)
    }
    # Borrowed joined them in migration 025 (owner decision 2026-09-04):
    # money lent TO him, and what he pays back, are movement too.
    assert labels == {"Internal Transfer", "External Transfer", "Borrowed"}
    # Still scoped: no income category leaks onto an expense row.
    assert _rows(modal, "income") == []


def test_the_movement_group_comes_last_with_its_own_eyebrow(
    triage_web_db: sqlite3.Connection,
    web_client_factory: Callable[[], TestClient],
) -> None:
    modal = _modal(web_client_factory, 1)

    eyebrows = re.findall(r'class="teyebrow catgroup-eyebrow"[^>]*>([^<]+)<', modal)
    assert eyebrows == ["EXPENSE", "MOVED, NOT SPENT"]
    # The group label is also what the row's kind column reads.
    assert 'class="catrow-kind">MOVED, NOT SPENT<' in modal


def test_transfer_categories_never_take_a_numbered_chip(
    triage_web_db: sqlite3.Connection,
    web_client_factory: Callable[[], TestClient],
) -> None:
    modal = _modal(web_client_factory, 1)

    chips = re.findall(r'<button[^>]*class="catchip"[^>]*>', modal)
    assert chips, "the modal has no chips at all"
    assert not any('data-kind="transfer"' in chip for chip in chips)


def test_each_transfer_row_carries_its_test_sentence(
    triage_web_db: sqlite3.Connection,
    web_client_factory: Callable[[], TestClient],
) -> None:
    """K7 — the sentence comes from category-definitions.md, and the
    transfer table there is now machine-read like the other two."""
    modal = _modal(web_client_factory, 1)

    external = re.search(
        r'data-label="External Transfer"[^>]*data-test="([^"]+)"', modal
    )
    internal = re.search(
        r'data-label="Internal Transfer"[^>]*data-test="([^"]+)"', modal
    )
    assert external and external.group(1).strip()
    assert internal and internal.group(1).strip()


def test_saving_a_transfer_category_on_an_expense_row_clears_it_from_the_queue(
    triage_web_db: sqlite3.Connection,
    web_client_factory: Callable[[], TestClient],
) -> None:
    """The write path accepted this all along; now the dialog can reach it."""
    external = categories_repo.get_by_name(
        triage_web_db, TransactionKind.TRANSFER, "External Transfer"
    )
    assert external is not None
    before = count_blocking(triage_web_db)

    with web_client_factory() as client:
        response = client.post(
            "/_partial/triage/1/edit",
            data={"set_category": "true", "category_id": str(external.id)},
        )

    assert response.status_code == 200
    row = triage_web_db.execute(
        "SELECT category_id, kind FROM transactions WHERE id = 1"
    ).fetchone()
    assert row["category_id"] == external.id
    # The row's kind is the audit trail and stays what the bank said.
    assert row["kind"] == "expense"
    assert count_blocking(triage_web_db) == before - 1


def test_the_bulk_sheet_offers_them_too(
    triage_web_db: sqlite3.Connection,
    web_client_factory: Callable[[], TestClient],
) -> None:
    with web_client_factory() as client:
        sheet = client.get("/_partial/triage/bulk-sheet").text

    assert 'data-label="External Transfer"' in sheet
    assert 'data-label="Internal Transfer"' in sheet


def test_bulk_targets_accept_a_movement_pick_on_any_row() -> None:
    """The sheet filters the selection to rows the pick can legally land
    on. A transfer-kind category fits every income and expense row, so the
    kind check must not throw them all away."""
    js = (JS / "triage.js").read_text(encoding="utf-8")

    block = js[js.index("bulkTargets: function") :]
    block = block[: block.index("refreshQueue: function")]
    assert 'kind === "transfer"' in block
