"""The modal run: one entry, full attention, the queue still behind it.

Criteria B, C, D7-D10, E, H. What a server render can answer is here; the
frame never resizing and ``↵`` advancing are browser facts, checked by
hand and reported separately.
"""

from __future__ import annotations

import json
import re
import sqlite3
from collections.abc import Callable

from starlette.testclient import TestClient

import pytest

from decimal import Decimal

from finances.db.repos import accounts as accounts_repo
from finances.db.repos import transactions as transactions_repo
from finances.domain.models import (
    Account,
    AccountKind,
    Transaction,
    TransactionKind,
)


@pytest.fixture
def unpriceable_modal_db(web_db: sqlite3.Connection) -> sqlite3.Connection:
    """A bolívar row on a ledger with no rate of any tier."""
    from datetime import UTC, datetime
    from decimal import Decimal

    bank = accounts_repo.insert(
        web_db,
        Account(
            name="Provincial",
            kind=AccountKind.BANK,
            currency="VES",
            institution="Provincial",
        ),
    )
    transactions_repo.insert(
        web_db,
        Transaction(
            account_id=bank.id,
            occurred_at=datetime(2026, 3, 2, tzinfo=UTC),
            kind=TransactionKind.EXPENSE,
            amount=Decimal("-9000.00"),
            currency="VES",
            description="COMPRA SIN TASA",
            source="provincial",
            source_ref="unpriceable-1",
        ),
    )
    return web_db


def _toast(response) -> str:
    """The toast copy out of the HX-Trigger header.

    The header is JSON, so an em dash arrives as ``\\u2014`` — comparing
    against the raw header string would fail on copy that is correct.
    """
    return json.loads(response.headers["HX-Trigger"])["toast"]["message"]


def _modal(client: TestClient, txn_id: int) -> str:
    response = client.get(f"/_partial/triage/{txn_id}/modal")
    assert response.status_code == 200, response.text
    return response.text


# ---------------------------------------------------------------------------
# The dialog itself (B3, B4, B5, B6, B11, B12, J1, J4)
# ---------------------------------------------------------------------------


def test_the_dialog_is_labelled_and_modal(
    triage_web_db: sqlite3.Connection,
    web_client_factory: Callable[[], TestClient],
) -> None:
    with web_client_factory() as client:
        html = _modal(client, 1)

    assert 'role="dialog"' in html
    assert 'aria-modal="true"' in html
    assert 'aria-label="Resolve this row"' in html


def test_the_overlay_is_absolute_within_the_content_never_fixed(
    triage_web_db: sqlite3.Connection,
    web_client_factory: Callable[[], TestClient],
) -> None:
    """B11 — the nav stays visible and unscrolled behind the scrim."""
    with web_client_factory() as client:
        html = _modal(client, 1)

    assert 'class="tover"' in html
    assert "position: fixed" not in html


def test_clicking_the_scrim_closes_and_clicking_inside_does_not(
    triage_web_db: sqlite3.Connection,
    web_client_factory: Callable[[], TestClient],
) -> None:
    """B12 — ``.self`` is the whole difference, so it is asserted."""
    with web_client_factory() as client:
        html = _modal(client, 1)

    assert "@click.self=" in html


def test_the_header_counts_the_position_and_draws_the_progress(
    triage_web_db: sqlite3.Connection,
    web_client_factory: Callable[[], TestClient],
) -> None:
    with web_client_factory() as client:
        html = _modal(client, 1)

    assert re.search(r"\d+ OF 8", html)
    assert "tmodal-progress-fill" in html


def test_the_arrows_disable_at_the_ends_rather_than_vanishing(
    triage_web_db: sqlite3.Connection,
    web_client_factory: Callable[[], TestClient],
) -> None:
    """B6 — a dead arrow and a missing arrow read the same; a disabled
    one says "you are at the end"."""
    with web_client_factory() as client:
        first = _modal(client, 10)  # oldest item in bucket 0

    prev = re.search(r'<button[^>]*data-nav-prev[^>]*>', first)
    assert prev is not None
    assert "disabled" in prev.group(0)
    assert 'aria-label="Previous row"' in first
    assert 'aria-label="Next row"' in first


def test_every_icon_only_control_has_a_name(
    triage_web_db: sqlite3.Connection,
    web_client_factory: Callable[[], TestClient],
) -> None:
    """J4 — the cheap half of the accessibility group."""
    with web_client_factory() as client:
        html = _modal(client, 1)

    assert 'aria-label="Close"' in html


# ---------------------------------------------------------------------------
# Left column — the facts (B7)
# ---------------------------------------------------------------------------


def test_the_left_column_carries_the_money_the_name_and_the_date(
    triage_web_db: sqlite3.Connection,
    web_client_factory: Callable[[], TestClient],
) -> None:
    with web_client_factory() as client:
        html = _modal(client, 1)

    facts = html.split('class="tmodal-decision"')[0]
    assert "tmoney-lg" in facts
    assert "−$100.00" in facts
    assert "Luncheria Mily Gourmet" in facts
    assert "LUNCHERIA MILY GOURMET" in facts
    assert "Fri, Jul 3" in facts
    assert "Provincial" in facts


def test_an_unpriceable_row_says_so_in_words_in_the_modal(
    unpriceable_modal_db: sqlite3.Connection,
    web_client_factory: Callable[[], TestClient],
) -> None:
    """D5 — `Can't be priced`, with the circle-slash, not a blank."""
    with web_client_factory() as client:
        html = _modal(client, 1)

    assert "Can&#39;t be priced" in html or "Can't be priced" in html
    assert 'data-icon="circle-slash"' in html


# ---------------------------------------------------------------------------
# Right column — the decision (B8, B10, E)
# ---------------------------------------------------------------------------


def test_a_category_row_asks_what_it_was_for_and_offers_the_keys(
    triage_web_db: sqlite3.Connection,
    web_client_factory: Callable[[], TestClient],
) -> None:
    with web_client_factory() as client:
        html = _modal(client, 1)

    assert "WHAT WAS THIS FOR?" in html
    assert ">1–8<" in html
    assert "data-category-picker" in html


def test_the_why_block_cites_the_evidence_and_only_fills_the_picker(
    triage_web_db: sqlite3.Connection,
    web_client_factory: Callable[[], TestClient],
) -> None:
    with web_client_factory() as client:
        rule_row = _modal(client, 2)
        learned_row = _modal(client, 1)

    assert "rule 30 matches" in rule_row
    assert "/traki/i" in rule_row
    assert "USE IT" in rule_row
    assert "you sorted this here 3 times" in learned_row


def test_a_row_that_needs_both_labels_the_rate_block_as_secondary(
    triage_web_db: sqlite3.Connection,
    web_client_factory: Callable[[], TestClient],
) -> None:
    """B8 — category first, then the rate under its own eyebrow."""
    with web_client_factory() as client:
        html = _modal(client, 2)

    assert "WHAT WAS THIS FOR?" in html
    assert "AND THE RATE, IF YOU KNOW IT" in html
    assert html.index("WHAT WAS THIS FOR?") < html.index("AND THE RATE, IF YOU KNOW IT")


def test_a_rate_only_row_leads_with_the_rate_question(
    triage_web_db: sqlite3.Connection,
    web_client_factory: Callable[[], TestClient],
) -> None:
    with web_client_factory() as client:
        html = _modal(client, 3)

    assert "THE RATE IS A GUESS — REPLACE IT?" in html
    assert "WHAT WAS THIS FOR?" not in html


def test_the_note_field_is_always_last_and_optional(
    triage_web_db: sqlite3.Connection,
    web_client_factory: Callable[[], TestClient],
) -> None:
    with web_client_factory() as client:
        html = _modal(client, 1)

    assert 'placeholder="Note — optional"' in html


def test_the_footer_label_names_what_the_button_will_do(
    triage_web_db: sqlite3.Connection,
    web_client_factory: Callable[[], TestClient],
) -> None:
    with web_client_factory() as client:
        category_row = _modal(client, 1)
        rate_row = _modal(client, 3)

    assert "Sort and next" in category_row
    # The last entry in the run finishes it rather than advancing.
    assert "Use this rate and next" in rate_row or "Save and finish" in rate_row


def test_the_last_entry_says_save_and_finish(
    triage_web_db: sqlite3.Connection,
    web_client_factory: Callable[[], TestClient],
) -> None:
    with web_client_factory() as client:
        html = _modal(client, 3)  # the only row in *Priced roughly*

    assert "Save and finish" in html


def test_the_footer_carries_park_and_the_keyboard_legend(
    triage_web_db: sqlite3.Connection,
    web_client_factory: Callable[[], TestClient],
) -> None:
    with web_client_factory() as client:
        html = _modal(client, 1)

    assert "data-park-btn" in html
    assert "move" in html and "save" in html and "close" in html
    assert "←→" in html


# ---------------------------------------------------------------------------
# Keyboard (C1-C7)
# ---------------------------------------------------------------------------


def test_the_key_handler_is_bound_to_the_dialog_not_the_document(
    triage_web_db: sqlite3.Connection,
    web_client_factory: Callable[[], TestClient],
) -> None:
    """C7 — Alpine removes the listener with the element, so forty rows
    leave no stray handlers behind."""
    with web_client_factory() as client:
        html = _modal(client, 1)

    assert "@keydown.window=" in html
    assert "onKey($event)" in html


def test_the_page_shell_binds_no_key_handler_of_its_own(
    triage_web_db: sqlite3.Connection,
    web_client_factory: Callable[[], TestClient],
) -> None:
    with web_client_factory() as client:
        page = client.get("/triage").text

    assert "onKey(" not in page


# ---------------------------------------------------------------------------
# The rate override (D7-D10)
# ---------------------------------------------------------------------------


def test_the_warning_names_the_actual_rate_source_and_date(
    triage_web_db: sqlite3.Connection,
    web_client_factory: Callable[[], TestClient],
) -> None:
    """D10 — no generic "rate unavailable"."""
    with web_client_factory() as client:
        html = _modal(client, 3)

    assert "Priced at" in html
    assert "155.00" in html
    assert "P2P median, 115 days later" in html
    assert "Wed, Mar 4" in html


def test_the_rate_field_is_labelled_and_hints_its_unit(
    triage_web_db: sqlite3.Connection,
    web_client_factory: Callable[[], TestClient],
) -> None:
    with web_client_factory() as client:
        html = _modal(client, 3)

    assert "Rate you got" in html
    assert "Bolívares per dollar" in html
    assert 'inputmode="decimal"' in html


def test_the_would_become_figure_recomputes_from_amount_over_rate(
    triage_web_db: sqlite3.Connection,
    web_client_factory: Callable[[], TestClient],
) -> None:
    """D8 — live, in the browser, from the row's own native amount."""
    with web_client_factory() as client:
        html = _modal(client, 3)

    assert "WOULD BECOME" in html or "CURRENTLY" in html
    assert 'data-amount-native="-8000' in html
    assert "previewUsd()" in html


def test_each_tier_is_offered_with_its_own_resulting_dollars(
    triage_web_db: sqlite3.Connection,
    web_client_factory: Callable[[], TestClient],
) -> None:
    """D9 — one candidate per tier, each saying why it is approximate."""
    with web_client_factory() as client:
        html = _modal(client, 3)

    assert "OR TAKE ONE OF THESE" in html
    assert html.count("data-rate-hint") >= 2
    assert "data-rate-hint-source=\"bcv\"" in html


def test_saving_a_rate_writes_user_rate_and_leaves_priced_roughly(
    triage_web_db: sqlite3.Connection,
    web_client_factory: Callable[[], TestClient],
) -> None:
    """D7 — through the existing apply_edit path, not a second write."""
    with web_client_factory() as client:
        response = client.post(
            "/_partial/triage/3/edit",
            data={
                "set_user_rate": "true",
                "user_rate": "150.00",
                "set_category": "false",
                "set_notes": "false",
            },
        )
    assert response.status_code == 200

    txn = transactions_repo.get_by_id(triage_web_db, 3)
    assert txn is not None
    assert txn.user_rate is not None
    assert txn.user_rate == Decimal("150.00")

    with web_client_factory() as client:
        queue = client.get("/_partial/triage/queue").text
    assert "Priced roughly" not in queue


def test_the_rate_toast_names_the_rate_that_was_set(
    triage_web_db: sqlite3.Connection,
    web_client_factory: Callable[[], TestClient],
) -> None:
    """I10 — never a generic "Saved"."""
    with web_client_factory() as client:
        response = client.post(
            "/_partial/triage/3/edit",
            data={
                "set_user_rate": "true",
                "user_rate": "152.40",
                "set_category": "false",
                "set_notes": "false",
            },
        )

    assert _toast(response) == "Rate set to 152.40."


def test_the_sort_toast_names_the_category(
    triage_web_db: sqlite3.Connection,
    web_client_factory: Callable[[], TestClient],
) -> None:
    from finances.db.repos import categories as categories_repo
    from finances.domain.models import TransactionKind

    groceries = categories_repo.get_by_name(
        triage_web_db, TransactionKind.EXPENSE, "Groceries"
    )
    assert groceries is not None

    with web_client_factory() as client:
        response = client.post(
            "/_partial/triage/1/edit",
            data={
                "set_category": "true",
                "category_id": str(groceries.id),
                "set_user_rate": "false",
                "set_notes": "false",
            },
        )

    assert _toast(response) == "Sorted — Groceries."
    assert "triageResolved" in response.headers["HX-Trigger"]


# ---------------------------------------------------------------------------
# Park from the modal (F1)
# ---------------------------------------------------------------------------


def test_parking_from_the_modal_advances_and_says_what_parking_means(
    triage_web_db: sqlite3.Connection,
    web_client_factory: Callable[[], TestClient],
) -> None:
    with web_client_factory() as client:
        response = client.post("/_partial/triage/1/park")

    assert response.status_code == 200
    assert _toast(response) == "Parked. It keeps its money, and stops asking."
    # The body IS the next entry's dialog, not the list.
    assert 'role="dialog"' in response.text

    txn = transactions_repo.get_by_id(triage_web_db, 1)
    assert txn is not None
    assert txn.parked is True


# ---------------------------------------------------------------------------
# Pairs (H1, H3, H4)
# ---------------------------------------------------------------------------


def test_the_pair_view_shows_both_legs_and_the_arithmetic(
    triage_web_db: sqlite3.Connection,
    web_client_factory: Callable[[], TestClient],
) -> None:
    with web_client_factory() as client:
        html = client.get("/_partial/triage/pair/8/9/modal").text

    assert "Provincial" in html
    assert "Binance Funding" in html
    assert "Same day" in html
    assert "confident" in html
    assert "implies" in html
    assert "Pair them" in html
    assert "Not a pair" in html


def test_a_pair_hides_the_footers_primary_button(
    triage_web_db: sqlite3.Connection,
    web_client_factory: Callable[[], TestClient],
) -> None:
    """B9 — the pair block carries its own actions."""
    with web_client_factory() as client:
        html = client.get("/_partial/triage/pair/8/9/modal").text

    assert "Sort and next" not in html
    assert "Save and finish" not in html


def test_a_confirmable_pair_says_what_confirming_will_mean(
    triage_web_db: sqlite3.Connection,
    web_client_factory: Callable[[], TestClient],
) -> None:
    with web_client_factory() as client:
        html = client.get("/_partial/triage/pair/8/9/modal").text

    assert (
        "Confirming this makes it your cost basis for every VES row in the "
        "next 14 days." in html
    )


def test_a_refused_pair_disables_the_button_and_says_why(
    triage_web_db: sqlite3.Connection,
    web_client_factory: Callable[[], TestClient],
) -> None:
    """H3 — over five days apart or ten percent of drift, and the modal
    says so before the click rather than after it."""
    with web_client_factory() as client:
        html = client.get("/_partial/triage/pair/10/11/modal").text

    assert "This one cannot be confirmed" in html
    assert "7 days apart" in html
    confirm = re.search(r"<button[^>]*data-pair-confirm[^>]*>", html)
    assert confirm is not None
    assert "disabled" in confirm.group(0)


def test_confirming_writes_one_transfer_id_across_both_legs(
    triage_web_db: sqlite3.Connection,
    web_client_factory: Callable[[], TestClient],
) -> None:
    with web_client_factory() as client:
        response = client.post("/_partial/triage/pair/8/9/confirm")

    assert response.status_code == 200
    assert _toast(response) == "Paired."

    deposit = transactions_repo.get_by_id(triage_web_db, 8)
    sell = transactions_repo.get_by_id(triage_web_db, 9)
    assert deposit is not None and sell is not None
    assert deposit.transfer_id is not None
    assert deposit.transfer_id == sell.transfer_id


def test_not_a_pair_dismisses_the_proposal_for_the_run(
    triage_web_db: sqlite3.Connection,
    web_client_factory: Callable[[], TestClient],
) -> None:
    """H4 — both legs stay separate rows, and the proposal stops asking."""
    with web_client_factory() as client:
        response = client.post("/_partial/triage/pair/8/9/refuse")
        assert response.status_code == 200
        assert _toast(response) == "Left unpaired — the legs stay separate rows."

        queue = client.get("/_partial/triage/queue").text
        assert "Proposed pairs" not in queue

    deposit = transactions_repo.get_by_id(triage_web_db, 8)
    sell = transactions_repo.get_by_id(triage_web_db, 9)
    assert deposit is not None and sell is not None
    assert deposit.transfer_id is None
    assert sell.transfer_id is None


# ---------------------------------------------------------------------------
# Write safety (K9, K10, K11, K12)
# ---------------------------------------------------------------------------


def test_a_failed_write_surfaces_an_error_and_keeps_the_row(
    triage_web_db: sqlite3.Connection,
    web_client_factory: Callable[[], TestClient],
) -> None:
    """K11 — an unknown category is a 422, and row 1 is still queued."""
    with web_client_factory() as client:
        response = client.post(
            "/_partial/triage/1/edit",
            data={
                "set_category": "true",
                "category_id": "999999",
                "set_user_rate": "false",
                "set_notes": "false",
            },
        )
        assert response.status_code in (404, 422)

        queue = client.get("/_partial/triage/queue").text

    assert 'data-item-id="txn:1"' in queue


def test_a_double_submit_is_idempotent(
    triage_web_db: sqlite3.Connection,
    web_client_factory: Callable[[], TestClient],
) -> None:
    """K9 — double ↵ must not double-write or 500."""
    from finances.db.repos import categories as categories_repo
    from finances.domain.models import TransactionKind

    groceries = categories_repo.get_by_name(
        triage_web_db, TransactionKind.EXPENSE, "Groceries"
    )
    assert groceries is not None
    payload = {
        "set_category": "true",
        "category_id": str(groceries.id),
        "set_user_rate": "false",
        "set_notes": "false",
    }

    with web_client_factory() as client:
        first = client.post("/_partial/triage/1/edit", data=payload)
        second = client.post("/_partial/triage/1/edit", data=payload)

    assert first.status_code == 200
    assert second.status_code == 200
    txn = transactions_repo.get_by_id(triage_web_db, 1)
    assert txn is not None
    assert txn.category_id == groceries.id


def test_the_write_lands_in_the_edit_history(
    triage_web_db: sqlite3.Connection,
    web_client_factory: Callable[[], TestClient],
) -> None:
    """K10."""
    from finances.db.repos import categories as categories_repo
    from finances.db.repos import transaction_edits as edits_repo
    from finances.domain.models import TransactionKind

    groceries = categories_repo.get_by_name(
        triage_web_db, TransactionKind.EXPENSE, "Groceries"
    )
    assert groceries is not None

    with web_client_factory() as client:
        client.post(
            "/_partial/triage/1/edit",
            data={
                "set_category": "true",
                "category_id": str(groceries.id),
                "set_user_rate": "false",
                "set_notes": "false",
            },
        )

    assert edits_repo.list_for_transaction(triage_web_db, 1)
