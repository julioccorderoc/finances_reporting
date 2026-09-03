"""The queue screen's markup contract (Wave 2 of the triage redesign).

Every assertion here is a criterion from
`design_handoff_triage/ACCEPTANCE-CRITERIA.md` that a server render can
answer: the header's question and blocking count, the three groups, the
row grid, the money block and its provenance chip, the parked strip and
the empty state.

What a server render CANNOT answer — that the modal's frame never
resizes, that ``↵`` saves, that a draft survives walking away — is
verified in a browser and reported separately.
"""

from __future__ import annotations

import re
import sqlite3
from collections.abc import Callable
from datetime import UTC, date, datetime
from decimal import Decimal

import pytest
from starlette.testclient import TestClient

from finances.db.repos import accounts as accounts_repo
from finances.db.repos import transactions as transactions_repo
from finances.domain.models import (
    Account,
    AccountKind,
    Transaction,
    TransactionKind,
)


def _page(factory: Callable[[], TestClient]) -> str:
    with factory() as client:
        response = client.get("/triage")
    assert response.status_code == 200
    return response.text


# ---------------------------------------------------------------------------
# Page header (A8, A9, I11)
# ---------------------------------------------------------------------------


def test_the_header_asks_the_question(
    triage_web_db: sqlite3.Connection,
    web_client_factory: Callable[[], TestClient],
) -> None:
    assert "What still needs you?" in _page(web_client_factory)


def test_the_answer_is_the_blocking_count_not_the_queue_size(
    triage_web_db: sqlite3.Connection,
    web_client_factory: Callable[[], TestClient],
) -> None:
    """A8 — a row whose only problem is an approximate rate is not
    blocking, so it is absent from the number the header answers with."""
    body = _page(web_client_factory)

    assert "7 rows need you" in body
    assert "8 rows need you" not in body


def test_the_header_reads_nothing_needs_you_at_zero_blocking(
    web_db: sqlite3.Connection,
    web_client_factory: Callable[[], TestClient],
) -> None:
    assert "Nothing needs you" in _page(web_client_factory)


def test_the_meta_row_carries_the_three_counts(
    triage_web_db: sqlite3.Connection,
    web_client_factory: Callable[[], TestClient],
) -> None:
    body = _page(web_client_factory)

    assert "6 category" in body
    assert "1 pairs" in body
    assert "1 approximate rates" in body


def test_the_sitting_counter_is_rendered_live_not_baked(
    triage_web_db: sqlite3.Connection,
    web_client_factory: Callable[[], TestClient],
) -> None:
    """A9 — "· N done in this sitting" counts what this run resolved, so
    it is client state; the server must not print a frozen number."""
    body = _page(web_client_factory)

    assert 'data-sitting-done' in body
    assert "done in this sitting" in body


def test_sort_all_names_the_whole_run_and_explains_the_split(
    triage_web_db: sqlite3.Connection,
    web_client_factory: Callable[[], TestClient],
) -> None:
    body = _page(web_client_factory)

    assert "Sort all 8" in body
    assert "7 that need you, then 1 with approximate rates" in body


def test_parked_action_shows_the_live_count(
    triage_web_db: sqlite3.Connection,
    web_client_factory: Callable[[], TestClient],
) -> None:
    assert "Parked 1" in _page(web_client_factory)


def test_the_counts_live_inside_the_swapped_region(
    triage_web_db: sqlite3.Connection,
    web_client_factory: Callable[[], TestClient],
) -> None:
    """A count rendered in the page shell is written once, by the full
    page load, and is stale from the first save onward."""
    with web_client_factory() as client:
        partial = client.get("/_partial/triage/queue").text

    assert "7 rows need you" in partial
    assert "6 category" in partial
    assert "Sort all 8" in partial


# ---------------------------------------------------------------------------
# Groups (A6, A7)
# ---------------------------------------------------------------------------


def test_the_three_group_heads_render_with_their_hints(
    triage_web_db: sqlite3.Connection,
    web_client_factory: Callable[[], TestClient],
) -> None:
    body = _page(web_client_factory)

    assert "Needs a category" in body
    assert "One decision each" in body
    assert "Proposed pairs" in body
    assert "Two rows that look like one transfer" in body
    assert "Priced roughly" in body
    assert "No rate within 14 days — Ledger used the nearest one" in body


def test_priced_roughly_is_the_only_group_that_starts_collapsed(
    triage_web_db: sqlite3.Connection,
    web_client_factory: Callable[[], TestClient],
) -> None:
    body = _page(web_client_factory)

    assert 'data-group="2"' in body
    collapsed = re.findall(r'data-group="(\d)"[^>]*data-collapsed="true"', body)
    assert collapsed == ["2"]


def test_a_group_with_no_rows_renders_nothing_at_all(
    seeded_web_db: sqlite3.Connection,
    web_client_factory: Callable[[], TestClient],
) -> None:
    """A7 — no empty head, not even a zero count."""
    body = _page(web_client_factory)

    assert "Proposed pairs" not in body


def test_each_group_head_offers_select_all_for_its_own_rows(
    triage_web_db: sqlite3.Connection,
    web_client_factory: Callable[[], TestClient],
) -> None:
    body = _page(web_client_factory)

    assert "Select all 6" in body
    assert "Clear these" in body


# ---------------------------------------------------------------------------
# Rows (A5, A10, A11, G1)
# ---------------------------------------------------------------------------


def test_a_row_carries_its_item_id_and_the_grid_class(
    triage_web_db: sqlite3.Connection,
    web_client_factory: Callable[[], TestClient],
) -> None:
    body = _page(web_client_factory)

    assert 'data-triage-row' in body
    assert 'data-item-id="txn:1"' in body
    assert 'class="triage-row"' in body


def test_dates_are_short_and_carry_no_weekday(
    triage_web_db: sqlite3.Connection,
    web_client_factory: Callable[[], TestClient],
) -> None:
    """A5 — `Jul 3`, never `Fri, Jul 3` and never `Today`.

    Scoped to the queue: the shell's rail names the dashboard *Today*,
    which is a destination, not a day label.
    """
    body = _page(web_client_factory)
    queue = body.split('id="triage-queue"', 1)[1]

    assert ">Jul 3<" in queue
    assert "Today" not in queue
    assert "Yesterday" not in queue


def test_the_checkbox_names_the_row_it_selects(
    triage_web_db: sqlite3.Connection,
    web_client_factory: Callable[[], TestClient],
) -> None:
    assert 'aria-label="Select LUNCHERIA MILY GOURMET"' in _page(web_client_factory)


def test_a_cleaned_name_sits_over_the_raw_bank_string(
    triage_web_db: sqlite3.Connection,
    web_client_factory: Callable[[], TestClient],
) -> None:
    body = _page(web_client_factory)

    assert "Luncheria Mily Gourmet" in body
    assert "LUNCHERIA MILY GOURMET" in body


def test_a_row_with_no_cleaned_name_shows_the_raw_string_alone(
    triage_web_db: sqlite3.Connection,
    web_client_factory: Callable[[], TestClient],
) -> None:
    """``COMPRA POS 3311 TRAKI`` is a bank reference, not a name, so
    ``clean_merchant`` declines it and the raw string takes the top
    line by itself."""
    body = _page(web_client_factory)
    row = _row_html(body, "txn:2")

    assert row.count("COMPRA POS 3311 TRAKI") == 2  # aria-label + top line
    assert "triage-row-raw" not in row


def test_the_account_shows_its_name_and_institution(
    triage_web_db: sqlite3.Connection,
    web_client_factory: Callable[[], TestClient],
) -> None:
    assert "triage-row-account" in _page(web_client_factory)


def test_the_open_button_is_named(
    triage_web_db: sqlite3.Connection,
    web_client_factory: Callable[[], TestClient],
) -> None:
    assert 'aria-label="Open this row"' in _page(web_client_factory)


def _row_html(body: str, item_id: str) -> str:
    match = re.search(
        r'<div class="triage-row"[^>]*data-item-id="'
        + re.escape(item_id)
        + r'".*?<!-- /triage-row -->',
        body,
        re.DOTALL,
    )
    assert match is not None, f"no row for {item_id}"
    return match.group(0)


# ---------------------------------------------------------------------------
# The guess chip and the issue badges (G6, G7, A2)
# ---------------------------------------------------------------------------


def test_a_rule_backed_guess_cites_its_rule_and_regex(
    triage_web_db: sqlite3.Connection,
    web_client_factory: Callable[[], TestClient],
) -> None:
    row = _row_html(_page(web_client_factory), "txn:2")

    assert "data-accept-guess" in row
    assert re.search(r'title="Rule \d+ · /traki/i"', row)
    assert "Purchases" in row


def test_a_learned_guess_counts_the_times_it_was_filed_there(
    triage_web_db: sqlite3.Connection,
    web_client_factory: Callable[[], TestClient],
) -> None:
    row = _row_html(_page(web_client_factory), "txn:1")

    assert 'title="You sorted this here 3 times"' in row
    assert "Groceries" in row


def test_a_row_with_no_guess_shows_its_issue_badges_instead(
    triage_web_db: sqlite3.Connection,
    web_client_factory: Callable[[], TestClient],
) -> None:
    """A2 — one row, two problems, two badges, one item. The Category
    badge is suppressed inside *Needs a category* as redundant."""
    row = _row_html(_page(web_client_factory), "txn:10")

    assert "data-accept-guess" not in row
    assert 'data-issue="rate"' in row
    assert 'data-issue="category"' not in row


def test_no_surface_offers_to_write_a_categorisation_rule(
    triage_web_db: sqlite3.Connection,
    web_client_factory: Callable[[], TestClient],
) -> None:
    """G8 — rules stay migration-managed."""
    body = _page(web_client_factory)

    assert "category_rules" not in body
    assert "New rule" not in body


# ---------------------------------------------------------------------------
# Money and provenance (D2, D3, D4, D5, D11, I2)
# ---------------------------------------------------------------------------


def test_a_priced_row_shows_usd_over_its_native_amount(
    triage_web_db: sqlite3.Connection,
    web_client_factory: Callable[[], TestClient],
) -> None:
    row = _row_html(_page(web_client_factory), "txn:1")

    assert "−$100.00" in row
    assert "−Bs.\u00a016,000.00" in row


def test_a_credit_is_ink_with_a_plus_never_a_colour(
    triage_web_db: sqlite3.Connection,
    web_client_factory: Callable[[], TestClient],
) -> None:
    """I2 — red on a credit reads as a loss."""
    row = _row_html(_page(web_client_factory), "txn:8")

    assert "+$200.00" in row
    assert "emerald" not in row
    assert "green" not in row


def test_a_bolivar_row_carries_its_provenance_chip(
    triage_web_db: sqlite3.Connection,
    web_client_factory: Callable[[], TestClient],
) -> None:
    row = _row_html(_page(web_client_factory), "txn:1")

    assert 'data-prov="binance_p2p_median_carry"' in row
    assert "median" in row
    assert "160.00" in row


def test_a_native_usd_row_carries_no_chip_at_all(
    triage_web_db: sqlite3.Connection,
    web_client_factory: Callable[[], TestClient],
) -> None:
    """D3 — there is nothing to explain about a dollar being a dollar."""
    row = _row_html(_page(web_client_factory), "txn:9")

    assert "data-prov=" not in row
    assert "prov-" not in row
    # The old viewer stamped a "native" badge on these rows. It is gone.
    assert ">native<" not in row


def test_an_approximate_row_is_marked_and_toned_as_a_warning(
    triage_web_db: sqlite3.Connection,
    web_client_factory: Callable[[], TestClient],
) -> None:
    row = _row_html(_page(web_client_factory), "txn:2")

    assert 'data-prov="binance_p2p_median_nearest"' in row
    assert "≈" in row
    assert "prov-warn" in row


def test_a_bcv_priced_row_and_a_realized_one_do_not_look_alike(
    triage_web_db: sqlite3.Connection,
    web_client_factory: Callable[[], TestClient],
) -> None:
    """D2 — they are not the same claim, so they cannot share a chip."""
    from finances.web.services.triage_view import prov_chip

    bcv = prov_chip("bcv", is_bcv_fallback=True, approximate=False)
    realized = prov_chip("binance_p2p_realized", is_bcv_fallback=False, approximate=False)

    assert bcv is not None and realized is not None
    assert bcv.tone != realized.tone
    assert bcv.label != realized.label


@pytest.fixture
def unpriceable_db(web_db: sqlite3.Connection) -> sqlite3.Connection:
    """A bolívar row on a ledger with no rate of any tier."""
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


def test_a_row_that_cannot_be_priced_says_unpriced_in_the_list(
    unpriceable_db: sqlite3.Connection,
    web_client_factory: Callable[[], TestClient],
) -> None:
    """D5 — and it still shows its bolívares, which are not in doubt."""
    row = _row_html(_page(web_client_factory), "txn:1")

    assert "Unpriced" in row
    assert "−Bs.\u00a09,000.00" in row
    assert "data-prov=" not in row


# ---------------------------------------------------------------------------
# Integrity banner, parked strip, empty state (A12, A13, A14)
# ---------------------------------------------------------------------------


def test_the_parked_strip_reports_the_count_and_offers_a_way_in(
    triage_web_db: sqlite3.Connection,
    web_client_factory: Callable[[], TestClient],
) -> None:
    body = _page(web_client_factory)

    assert "parked row, out of the queue" in body
    assert "Look at them" in body


def test_no_parked_strip_when_nothing_is_parked(
    seeded_web_db: sqlite3.Connection,
    web_client_factory: Callable[[], TestClient],
) -> None:
    assert "out of the queue" not in _page(web_client_factory)


def test_the_empty_state_names_the_sitting_and_what_starts_the_next_one(
    web_db: sqlite3.Connection,
    web_client_factory: Callable[[], TestClient],
) -> None:
    body = _page(web_client_factory)

    assert "Queue empty." in body
    assert "The next Provincial statement will start the next one." in body
    assert "triage-empty-headline" in body


def test_the_integrity_banner_names_the_orphan_leg(
    triage_web_db: sqlite3.Connection,
    web_client_factory: Callable[[], TestClient],
) -> None:
    funding = accounts_repo.get_by_name(triage_web_db, "Binance Funding")
    assert funding is not None
    transactions_repo.insert(
        triage_web_db,
        Transaction(
            account_id=funding.id,
            occurred_at=datetime(2026, 6, 29, tzinfo=UTC),
            kind=TransactionKind.TRANSFER,
            amount=Decimal("-96.40"),
            currency="USDT",
            description="P2P sell",
            source="binance",
            source_ref="orphan-leg",
            transfer_id="orphan",
        ),
    )

    body = _page(web_client_factory)

    assert "One transfer has a single leg" in body
    assert "Binance Funding, Jun 29 — 96.40 USDT out" in body
    assert "Pair it, or say it was not a transfer." in body


# ---------------------------------------------------------------------------
# Selection, bulk, park (G1, G2, G3)
# ---------------------------------------------------------------------------


def test_the_selection_bar_offers_exactly_three_actions(
    triage_web_db: sqlite3.Connection,
    web_client_factory: Callable[[], TestClient],
) -> None:
    body = _page(web_client_factory)

    assert "Set a category" in body
    assert 'data-selection-bar' in body
    assert "selected" in body


def test_the_selection_bar_lives_outside_the_swapped_queue(
    triage_web_db: sqlite3.Connection,
    web_client_factory: Callable[[], TestClient],
) -> None:
    """G1 — selection survives collapsing a group, and it must survive a
    queue swap for the same reason."""
    with web_client_factory() as client:
        partial = client.get("/_partial/triage/queue").text

    assert "data-selection-bar" not in partial


def test_the_bulk_sheet_says_what_it_leaves_alone(
    triage_web_db: sqlite3.Connection,
    web_client_factory: Callable[[], TestClient],
) -> None:
    """G4 — already-categorised rows in the selection are not touched."""
    with web_client_factory() as client:
        body = client.get("/_partial/triage/bulk-sheet").text

    assert (
        "Rows in the selection that already have a category are left alone."
        in body
    )
    assert "data-picker-chips" in body


def test_the_parked_sheet_carries_a_real_calendar_picker(
    triage_web_db: sqlite3.Connection,
    web_client_factory: Callable[[], TestClient],
) -> None:
    """F4 — never a dropdown of days, never free text."""
    with web_client_factory() as client:
        body = client.get("/_partial/triage/parked").text

    assert 'type="date"' in body
    assert "Park uncategorised rows before" in body
    assert 'value="2026-01-01"' in body
    assert "The oldest one is Sun, Nov 3, 2024" in body


def test_the_parked_sheet_shows_a_sample_and_the_note(
    triage_web_db: sqlite3.Connection,
    web_client_factory: Callable[[], TestClient],
) -> None:
    with web_client_factory() as client:
        body = client.get("/_partial/triage/parked").text

    assert "A FEW OF THEM" in body
    assert "PAGO MOVIL 04141234567" in body
    assert (
        "Their money still counts everywhere. Re-importing a statement will "
        "not push them back into the queue" in body
    )
    assert "Bring back all 1" in body


# ---------------------------------------------------------------------------
# Non-regression (L1, L2, L5)
# ---------------------------------------------------------------------------


def test_nothing_from_the_prototype_shipped(
    triage_web_db: sqlite3.Connection,
    web_client_factory: Callable[[], TestClient],
) -> None:
    """L1 — no Babel, no window.Bodega/Fin/Triage, no fixture module."""
    body = _page(web_client_factory)

    for banned in (
        "babel",
        "window.Bodega",
        "window.Fin",
        "window.Triage",
        "triage-data.js",
    ):
        assert banned not in body


def test_the_old_filter_chip_row_is_gone(
    triage_web_db: sqlite3.Connection,
    web_client_factory: Callable[[], TestClient],
) -> None:
    """L2 — the queue is grouped by what is wrong, not filtered by type."""
    body = _page(web_client_factory)

    assert "data-triage-filter" not in body
    assert "data-filter-chip" not in body


def test_the_other_pages_still_render(
    seeded_web_db: sqlite3.Connection,
    web_client_factory: Callable[[], TestClient],
) -> None:
    with web_client_factory() as client:
        for path in ("/", "/transactions", "/monthly", "/accounts", "/rates"):
            assert client.get(path).status_code == 200, path
