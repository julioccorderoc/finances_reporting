"""Set balance on the Accounts page — ADR-018's viewer surface.

The domain has written plugs since 2026-08-04 and only ``finances reconcile
balances`` ever called it. This is the same act with the argument that CLI
call cannot make: **before you plug it, here is what might explain it.**

The preview is the whole point. Sitting A's lesson was ten Binance Pay
twins double-counting 2,260.72 USDT — a difference that looked exactly like
missing history and was in fact duplicated rows. A surface that turns a
number into an adjustment in one click would have cemented that error into
the ledger permanently. So the panel lists, for the last 60 days on that
account: unpaired legs, same-day same-amount twins, uncategorised rows and
rows priced from a nearest rate — each one a link to its modal — and it
demands a written reason before it will write anything.
"""

from __future__ import annotations

import re
import sqlite3
from datetime import UTC, date, datetime, timedelta
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
from finances.web.services import reconcile_view

TODAY = date(2026, 9, 3)


def _at(days_ago: int) -> datetime:
    return datetime.combine(
        TODAY - timedelta(days=days_ago), datetime.min.time(), tzinfo=UTC
    )


@pytest.fixture
def reconcile_db(web_db: sqlite3.Connection) -> sqlite3.Connection:
    """One account per shape the panel has to explain.

    * **Binance Spot (USDT)** — an unpaired transfer leg, a pair of
      same-day same-amount twins, an uncategorised row, a USDC row (the
      mixed-asset trap ``v_account_balances`` walks into), and one row far
      outside the 60-day window.
    * **Provincial (VES)** — a row dated where the only rate in the table
      is months away, so the resolver prices it ``*_nearest`` (ADR-021).
    """
    spot = accounts_repo.insert(
        web_db,
        Account(
            name="Binance Spot",
            kind=AccountKind.CRYPTO_SPOT,
            currency="USDT",
            institution="Binance",
        ),
    )
    bank = accounts_repo.insert(
        web_db,
        Account(name="Provincial", kind=AccountKind.BANK, currency="VES"),
    )
    salary = categories_repo.get_by_name(web_db, TransactionKind.INCOME, "Salary")
    assert salary is not None

    rows = [
        # Categorised, priced, paired-irrelevant. Pure balance.
        Transaction(
            account_id=spot.id,
            occurred_at=_at(5),
            kind=TransactionKind.INCOME,
            amount=Decimal("100.00"),
            currency="USDT",
            description="Earn payout",
            category_id=salary.id,
            source="binance",
            source_ref="spot-clean",
        ),
        # Orphan transfer leg — money moved with no counterpart (rule-002).
        Transaction(
            account_id=spot.id,
            occurred_at=_at(12),
            kind=TransactionKind.TRANSFER,
            amount=Decimal("-30.00"),
            currency="USDT",
            description="Withdraw to bank",
            source="binance",
            source_ref="spot-orphan-leg",
        ),
        # Twins: same day, same amount, same currency, two rows.
        Transaction(
            account_id=spot.id,
            occurred_at=_at(10),
            kind=TransactionKind.EXPENSE,
            amount=Decimal("-25.00"),
            currency="USDT",
            description="Binance Pay bodega",
            category_id=None,
            source="binance",
            source_ref="spot-twin-a",
        ),
        Transaction(
            account_id=spot.id,
            occurred_at=_at(10),
            kind=TransactionKind.EXPENSE,
            amount=Decimal("-25.00"),
            currency="USDT",
            description="Binance Pay bodega",
            category_id=None,
            source="binance",
            source_ref="spot-twin-b",
        ),
        # A USDC row on the same account. Cannot move the USDT position and
        # is shown anyway: mistaking one asset for the other is exactly how
        # a gap gets misread.
        Transaction(
            account_id=spot.id,
            occurred_at=_at(8),
            kind=TransactionKind.EXPENSE,
            amount=Decimal("-7.00"),
            currency="USDC",
            description="Binance Pay farmacia",
            category_id=None,
            source="binance",
            source_ref="spot-usdc",
        ),
        # Older than the window: uncategorised, and deliberately not listed.
        Transaction(
            account_id=spot.id,
            occurred_at=_at(200),
            kind=TransactionKind.EXPENSE,
            amount=Decimal("-5.00"),
            currency="USDT",
            description="ancient",
            category_id=None,
            source="binance",
            source_ref="spot-ancient",
        ),
        # Bank row the resolver can only price from a distant rate.
        Transaction(
            account_id=bank.id,
            occurred_at=_at(20),
            kind=TransactionKind.EXPENSE,
            amount=Decimal("-3650.00"),
            currency="VES",
            description="COM.PAGO bodega",
            category_id=None,
            source="provincial",
            source_ref="bank-rough",
        ),
    ]
    for txn in rows:
        transactions_repo.insert(web_db, txn)

    # The only VES rate in the table sits 200 days away, so the row above
    # resolves through branch 5 (``binance_p2p_median_nearest``).
    rates_repo.upsert(
        web_db,
        Rate(
            as_of_date=TODAY - timedelta(days=220),
            base="USDT",
            quote="VES",
            rate=Decimal("36.50"),
            source="binance_p2p_median",
        ),
    )
    return web_db


def _account_id(conn: sqlite3.Connection, name: str) -> int:
    account = accounts_repo.get_by_name(conn, name)
    assert account is not None and account.id is not None
    return account.id


def _reason(preview, key: str):
    return next((r for r in preview.reasons if r.key == key), None)


def _ids(preview, key: str) -> set[int]:
    reason = _reason(preview, key)
    return set() if reason is None else {row.transaction_id for row in reason.rows}


def _txn_id(conn: sqlite3.Connection, source_ref: str) -> int:
    row = conn.execute(
        "SELECT id FROM transactions WHERE source_ref = ?", (source_ref,)
    ).fetchone()
    assert row is not None, source_ref
    return int(row["id"])


# ---------------------------------------------------------------------------
# The arithmetic
# ---------------------------------------------------------------------------


def test_preview_reports_the_gap_between_ledger_and_custodian(
    reconcile_db: sqlite3.Connection,
) -> None:
    preview = reconcile_view.build_preview(
        reconcile_db,
        account_id=_account_id(reconcile_db, "Binance Spot"),
        actual=Decimal("10.00"),
        today=TODAY,
    )

    # 100 − 30 − 25 − 25 − 5 = 15 USDT. The USDC row is a different position.
    assert preview.ledger_balance == Decimal("15.00")
    assert preview.actual_balance == Decimal("10.00")
    assert preview.delta == Decimal("-5.00")
    assert preview.matches is False
    assert preview.currency == "USDT"
    assert preview.account_name == "Binance Spot"


def test_the_ledger_figure_is_the_position_not_the_mixed_account_balance(
    reconcile_db: sqlite3.Connection,
) -> None:
    """``v_account_balances`` folds USDC into Binance Spot's USDT figure.

    Reconciling against that number would size the plug by the wrong
    arithmetic. The position — one account, one asset — is what
    ``record_adjustment`` compares against, so it is what the preview must
    show and what the card must pre-fill.
    """
    preview = reconcile_view.build_preview(
        reconcile_db,
        account_id=_account_id(reconcile_db, "Binance Spot"),
        actual=Decimal("15.00"),
        today=TODAY,
    )

    assert preview.ledger_balance == Decimal("15.00")  # not 8.00
    assert preview.matches is True
    assert preview.delta == Decimal("0.00")


def test_preview_renders_the_ledger_figure_without_an_exponent(
    reconcile_db: sqlite3.Connection,
) -> None:
    """The number goes straight into an ``<input value="…">``."""
    preview = reconcile_view.build_preview(
        reconcile_db,
        account_id=_account_id(reconcile_db, "Binance Spot"),
        actual=Decimal("15"),
        today=TODAY,
    )
    assert preview.ledger_plain == "15.00"
    assert "E" not in preview.ledger_plain


def test_unknown_account_raises_lookup_error(
    reconcile_db: sqlite3.Connection,
) -> None:
    with pytest.raises(LookupError):
        reconcile_view.build_preview(
            reconcile_db, account_id=9999, actual=Decimal("1"), today=TODAY
        )


# ---------------------------------------------------------------------------
# What could explain it
# ---------------------------------------------------------------------------


def test_preview_lists_unpaired_legs(reconcile_db: sqlite3.Connection) -> None:
    preview = reconcile_view.build_preview(
        reconcile_db,
        account_id=_account_id(reconcile_db, "Binance Spot"),
        actual=Decimal("10.00"),
        today=TODAY,
    )
    assert _ids(preview, "unpaired") == {_txn_id(reconcile_db, "spot-orphan-leg")}


def test_preview_lists_same_day_same_amount_twins(
    reconcile_db: sqlite3.Connection,
) -> None:
    """The Sitting A lesson: ten Binance Pay twins, 2,260.72 USDT double-counted."""
    preview = reconcile_view.build_preview(
        reconcile_db,
        account_id=_account_id(reconcile_db, "Binance Spot"),
        actual=Decimal("10.00"),
        today=TODAY,
    )
    assert _ids(preview, "twins") == {
        _txn_id(reconcile_db, "spot-twin-a"),
        _txn_id(reconcile_db, "spot-twin-b"),
    }


def test_a_lone_row_is_not_a_twin(reconcile_db: sqlite3.Connection) -> None:
    preview = reconcile_view.build_preview(
        reconcile_db,
        account_id=_account_id(reconcile_db, "Binance Spot"),
        actual=Decimal("10.00"),
        today=TODAY,
    )
    assert _txn_id(reconcile_db, "spot-clean") not in _ids(preview, "twins")


def test_preview_lists_uncategorised_rows(reconcile_db: sqlite3.Connection) -> None:
    preview = reconcile_view.build_preview(
        reconcile_db,
        account_id=_account_id(reconcile_db, "Binance Spot"),
        actual=Decimal("10.00"),
        today=TODAY,
    )
    assert _ids(preview, "uncategorised") == {
        _txn_id(reconcile_db, "spot-twin-a"),
        _txn_id(reconcile_db, "spot-twin-b"),
        _txn_id(reconcile_db, "spot-usdc"),
    }


def test_transfers_are_never_asked_for_a_category(
    reconcile_db: sqlite3.Connection,
) -> None:
    """rule-006: a transfer leg has no category and is not missing one."""
    preview = reconcile_view.build_preview(
        reconcile_db,
        account_id=_account_id(reconcile_db, "Binance Spot"),
        actual=Decimal("10.00"),
        today=TODAY,
    )
    assert _txn_id(reconcile_db, "spot-orphan-leg") not in _ids(
        preview, "uncategorised"
    )


def test_preview_lists_approximately_priced_rows(
    reconcile_db: sqlite3.Connection,
) -> None:
    preview = reconcile_view.build_preview(
        reconcile_db,
        account_id=_account_id(reconcile_db, "Provincial"),
        actual=Decimal("0"),
        today=TODAY,
    )
    assert _ids(preview, "approximate") == {_txn_id(reconcile_db, "bank-rough")}


def test_rows_outside_the_window_are_not_listed(
    reconcile_db: sqlite3.Connection,
) -> None:
    preview = reconcile_view.build_preview(
        reconcile_db,
        account_id=_account_id(reconcile_db, "Binance Spot"),
        actual=Decimal("10.00"),
        today=TODAY,
    )
    ancient = _txn_id(reconcile_db, "spot-ancient")
    assert all(
        ancient not in {row.transaction_id for row in reason.rows}
        for reason in preview.reasons
    )
    assert preview.window_days == reconcile_view.LOOKBACK_DAYS
    assert preview.since == TODAY - timedelta(days=reconcile_view.LOOKBACK_DAYS)


def test_every_listed_row_links_to_its_modal(
    reconcile_db: sqlite3.Connection,
) -> None:
    preview = reconcile_view.build_preview(
        reconcile_db,
        account_id=_account_id(reconcile_db, "Binance Spot"),
        actual=Decimal("10.00"),
        today=TODAY,
    )
    listed = [row for reason in preview.reasons for row in reason.rows]
    assert listed
    for row in listed:
        assert row.modal_url == f"/_partial/transactions/{row.transaction_id}/modal"


def test_empty_reasons_are_omitted(web_db: sqlite3.Connection) -> None:
    """A clean account gets no list of things to check, and says so."""
    account = accounts_repo.insert(
        web_db, Account(name="Cash USD", kind=AccountKind.CASH, currency="USD")
    )
    assert account.id is not None
    preview = reconcile_view.build_preview(
        web_db, account_id=account.id, actual=Decimal("40"), today=TODAY
    )
    assert preview.reasons == []
    assert preview.delta == Decimal("40")


# ---------------------------------------------------------------------------
# Writing
# ---------------------------------------------------------------------------


def test_write_refuses_a_blank_note(reconcile_db: sqlite3.Connection) -> None:
    with pytest.raises(ValueError, match="note"):
        reconcile_view.write_adjustment(
            reconcile_db,
            account_id=_account_id(reconcile_db, "Binance Spot"),
            actual=Decimal("10"),
            note="   ",
            now=datetime(2026, 9, 3, 14, 0, tzinfo=UTC),
        )
    assert (
        reconcile_db.execute(
            "SELECT COUNT(*) AS c FROM transactions WHERE kind = 'adjustment'"
        ).fetchone()["c"]
        == 0
    )


def test_write_records_one_adjustment_dated_now(
    reconcile_db: sqlite3.Connection,
) -> None:
    result = reconcile_view.write_adjustment(
        reconcile_db,
        account_id=_account_id(reconcile_db, "Binance Spot"),
        actual=Decimal("10"),
        note="checked Binance; the 2025 transfers are past the six-month window",
        now=datetime(2026, 9, 3, 14, 0, tzinfo=UTC),
    )

    assert result is not None
    assert result.delta == Decimal("-5.00")
    written = transactions_repo.get_by_id(reconcile_db, result.transaction_id)
    assert written is not None
    assert written.kind is TransactionKind.ADJUSTMENT
    assert written.source == "reconciliation"
    # ADR-018 §2.1 / ADR-020: today, never the ledger's start.
    assert written.occurred_at.date() == date(2026, 9, 3)
    assert written.notes is not None and "six-month window" in written.notes


def test_write_returns_none_when_the_figures_agree(
    reconcile_db: sqlite3.Connection,
) -> None:
    result = reconcile_view.write_adjustment(
        reconcile_db,
        account_id=_account_id(reconcile_db, "Binance Spot"),
        actual=Decimal("15.00"),
        note="matched on the day",
        now=datetime(2026, 9, 3, 14, 0, tzinfo=UTC),
    )
    assert result is None
    assert (
        reconcile_db.execute(
            "SELECT COUNT(*) AS c FROM transactions WHERE kind = 'adjustment'"
        ).fetchone()["c"]
        == 0
    )


# ---------------------------------------------------------------------------
# The card control
# ---------------------------------------------------------------------------


def test_account_card_carries_a_set_balance_control(
    reconcile_db: sqlite3.Connection, web_client_factory
) -> None:
    html = web_client_factory().get("/accounts").text
    account_id = _account_id(reconcile_db, "Binance Spot")

    assert f'hx-post="/_partial/accounts/{account_id}/reconcile/preview"' in html
    assert f'id="reconcile-panel-{account_id}"' in html


def test_the_control_is_prefilled_with_the_position_and_fixes_the_currency(
    reconcile_db: sqlite3.Connection, web_client_factory
) -> None:
    html = web_client_factory().get("/accounts").text
    account_id = _account_id(reconcile_db, "Binance Spot")
    slot = re.search(
        rf'<div class="rpt-account-slot"[^>]*data-account-id="{account_id}".*?</div>\s*</div>',
        html,
        flags=re.DOTALL,
    )
    assert slot is not None
    block = slot.group(0)
    assert 'value="15.00"' in block
    # The asset is the account's, not a field the owner can retype.
    assert 'name="currency"' not in block
    assert "USDT" in block


def test_the_control_is_not_nested_inside_the_drill_anchor(
    reconcile_db: sqlite3.Connection, web_client_factory
) -> None:
    """A ``<form>`` inside an ``<a>`` is invalid HTML and the browser
    re-parents it, which detaches the htmx attributes from the fields."""
    html = web_client_factory().get("/accounts").text
    anchor = re.search(r'<a class="rpt-account[^"]*".*?</a>', html, flags=re.DOTALL)
    assert anchor is not None
    assert "<form" not in anchor.group(0)


# ---------------------------------------------------------------------------
# The endpoints
# ---------------------------------------------------------------------------


def test_preview_endpoint_renders_the_panel(
    reconcile_db: sqlite3.Connection, web_client_factory
) -> None:
    account_id = _account_id(reconcile_db, "Binance Spot")
    resp = web_client_factory().post(
        f"/_partial/accounts/{account_id}/reconcile/preview",
        data={"actual": "10.00"},
    )

    assert resp.status_code == 200
    body = resp.text
    assert "rpt-reconcile-preview" in body
    # The difference, the evidence, and the note the write demands.
    assert "Binance Pay bodega" in body
    assert 'name="note"' in body and "required" in body
    assert f'hx-post="/_partial/accounts/{account_id}/reconcile"' in body
    assert "/_partial/transactions/" in body


def test_preview_endpoint_rejects_a_non_numeric_figure(
    reconcile_db: sqlite3.Connection, web_client_factory
) -> None:
    account_id = _account_id(reconcile_db, "Binance Spot")
    resp = web_client_factory().post(
        f"/_partial/accounts/{account_id}/reconcile/preview",
        data={"actual": "about ten"},
    )
    assert resp.status_code == 422


def test_preview_endpoint_404s_on_an_unknown_account(
    reconcile_db: sqlite3.Connection, web_client_factory
) -> None:
    resp = web_client_factory().post(
        "/_partial/accounts/9999/reconcile/preview", data={"actual": "1"}
    )
    assert resp.status_code == 404


def test_reconcile_endpoint_writes_the_row_and_re_renders_the_card(
    reconcile_db: sqlite3.Connection, web_client_factory
) -> None:
    account_id = _account_id(reconcile_db, "Binance Spot")
    resp = web_client_factory().post(
        f"/_partial/accounts/{account_id}/reconcile",
        data={"actual": "10.00", "note": "history past the six-month window"},
    )

    assert resp.status_code == 200
    assert f'data-account-id="{account_id}"' in resp.text
    # The card now shows the reconciled position.
    assert 'value="10.00"' in resp.text

    row = reconcile_db.execute(
        "SELECT notes, occurred_at, amount FROM transactions "
        "WHERE kind = 'adjustment' AND source = 'reconciliation'"
    ).fetchone()
    assert row is not None
    assert row["notes"] == "history past the six-month window"


def test_reconcile_endpoint_toasts_and_refreshes_the_today_tiles(
    reconcile_db: sqlite3.Connection, web_client_factory
) -> None:
    account_id = _account_id(reconcile_db, "Binance Spot")
    resp = web_client_factory().post(
        f"/_partial/accounts/{account_id}/reconcile",
        data={"actual": "10.00", "note": "unrecoverable"},
    )

    trigger = resp.headers["HX-Trigger"]
    assert "kpisDirty" in trigger
    assert "toast" in trigger
    assert "-5" in trigger  # the plug names its own size


def test_reconcile_endpoint_refuses_a_blank_note(
    reconcile_db: sqlite3.Connection, web_client_factory
) -> None:
    account_id = _account_id(reconcile_db, "Binance Spot")
    resp = web_client_factory().post(
        f"/_partial/accounts/{account_id}/reconcile",
        data={"actual": "10.00", "note": "  "},
    )

    assert resp.status_code == 422
    assert (
        reconcile_db.execute(
            "SELECT COUNT(*) AS c FROM transactions WHERE kind = 'adjustment'"
        ).fetchone()["c"]
        == 0
    )


def test_reconcile_endpoint_writes_nothing_when_the_figures_agree(
    reconcile_db: sqlite3.Connection, web_client_factory
) -> None:
    account_id = _account_id(reconcile_db, "Binance Spot")
    resp = web_client_factory().post(
        f"/_partial/accounts/{account_id}/reconcile",
        data={"actual": "15.00", "note": "already matches"},
    )

    assert resp.status_code == 200
    assert "already matches" in resp.headers["HX-Trigger"].lower() or "match" in (
        resp.headers["HX-Trigger"].lower()
    )
    assert (
        reconcile_db.execute(
            "SELECT COUNT(*) AS c FROM transactions WHERE kind = 'adjustment'"
        ).fetchone()["c"]
        == 0
    )


def test_the_card_discloses_when_its_headline_sums_other_assets(
    reconcile_db: sqlite3.Connection, web_client_factory
) -> None:
    """Binance Spot's card figure folds its USDC in; the control's does not.

    Two different numbers a few pixels apart read as a bug unless the page
    says which is which. ``v_account_balances`` sums an account across
    currencies (that is its own defect); an adjustment is written per
    position, so the control has to use the position and admit the gap.
    """
    html = web_client_factory().get("/accounts").text
    account_id = _account_id(reconcile_db, "Binance Spot")
    slot = re.search(
        rf'<div class="rpt-account-slot"[^>]*data-account-id="{account_id}".*?</div>\s*</div>',
        html,
        flags=re.DOTALL,
    )
    assert slot is not None
    assert "data-mixed-assets" in slot.group(0)

    # The bank account holds one asset, so it says nothing.
    bank_id = _account_id(reconcile_db, "Provincial")
    bank_slot = re.search(
        rf'<div class="rpt-account-slot"[^>]*data-account-id="{bank_id}".*?</div>\s*</div>',
        html,
        flags=re.DOTALL,
    )
    assert bank_slot is not None
    assert "data-mixed-assets" not in bank_slot.group(0)


def test_the_page_answer_carries_the_id_the_oob_swap_targets(
    reconcile_db: sqlite3.Connection, web_client_factory
) -> None:
    html = web_client_factory().get("/accounts").text
    assert 'id="accounts-header"' in html
    # Nothing on the page itself is out-of-band.
    assert "hx-swap-oob" not in html


def test_reconcile_response_restates_the_page_answer_out_of_band(
    reconcile_db: sqlite3.Connection, web_client_factory
) -> None:
    """A plug moves net worth, and the headline is outside the swapped card.

    Without this the card says one thing and the Doto figure above it says
    another, which is the stale-chart bug the monthly filter had.
    """
    account_id = _account_id(reconcile_db, "Binance Spot")
    resp = web_client_factory().post(
        f"/_partial/accounts/{account_id}/reconcile",
        data={"actual": "10.00", "note": "unrecoverable"},
    )

    assert 'id="accounts-header"' in resp.text
    assert 'hx-swap-oob="true"' in resp.text


def test_plain_keeps_precision_the_position_actually_has() -> None:
    """A crypto position carries eight decimals; the field must not eat them.

    Rounding the pre-filled figure to cents makes an unchanged submission a
    *different* number from the ledger, and ``record_adjustment`` writes a
    dust plug for the rounding — NEGLIGIBLE is 1e-8, not a cent.
    """
    assert reconcile_view.plain(Decimal("15")) == "15.00"
    assert reconcile_view.plain(Decimal("15.5")) == "15.50"
    assert reconcile_view.plain(Decimal("673.92345678")) == "673.92345678"
    assert reconcile_view.plain(Decimal("1E+2")) == "100.00"
    assert reconcile_view.plain(Decimal("-0.5")) == "-0.50"


def test_an_untouched_eight_decimal_position_writes_nothing(
    web_db: sqlite3.Connection, web_client_factory
) -> None:
    account = accounts_repo.insert(
        web_db,
        Account(name="Binance Funding", kind=AccountKind.CRYPTO_FUNDING, currency="BTC"),
    )
    assert account.id is not None
    transactions_repo.insert(
        web_db,
        Transaction(
            account_id=account.id,
            occurred_at=_at(3),
            kind=TransactionKind.INCOME,
            amount=Decimal("0.01234567"),
            currency="BTC",
            description="deposit",
            source="binance",
            source_ref="btc-1",
        ),
    )

    client = web_client_factory()
    html = client.get("/accounts").text
    assert 'value="0.01234567"' in html

    resp = client.post(
        f"/_partial/accounts/{account.id}/reconcile",
        data={"actual": "0.01234567", "note": "unchanged"},
    )
    assert resp.status_code == 200
    assert (
        web_db.execute(
            "SELECT COUNT(*) AS c FROM transactions WHERE kind = 'adjustment'"
        ).fetchone()["c"]
        == 0
    )
