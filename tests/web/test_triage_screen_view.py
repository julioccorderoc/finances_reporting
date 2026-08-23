"""The queue screen's view model — everything the template must not compute.

Wave 2 of the triage redesign (`design_handoff_triage/`). The payload
(`services/triage.py`) answers *what is in the queue*; this module answers
*how the screen is laid out*: the three groups with their exact labels and
hints, the integrity banner in the design's own words, and the parked
panel's count / sample / oldest / cutoff.

Criteria A6, A7, A12, A13, F4, F8.
"""

from __future__ import annotations

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
from finances.web.services import triage_view


def _aware(dt: datetime) -> datetime:
    return dt if dt.tzinfo else dt.replace(tzinfo=UTC)


@pytest.fixture
def screen_db(web_db: sqlite3.Connection) -> sqlite3.Connection:
    """One uncategorised bolívar row, one parked row, one priced rate row."""
    today = datetime.now(tz=UTC)
    bank = accounts_repo.insert(
        web_db,
        Account(
            name="Provincial",
            kind=AccountKind.BANK,
            currency="VES",
            institution="Provincial",
        ),
    )
    rates_repo.upsert(
        web_db,
        Rate(
            as_of_date=today.date(),
            base="USDT",
            quote="VES",
            rate=Decimal("160.00"),
            source="binance_p2p_median",
        ),
    )
    transactions_repo.insert(
        web_db,
        Transaction(
            account_id=bank.id,
            occurred_at=_aware(today - timedelta(days=1)),
            kind=TransactionKind.EXPENSE,
            amount=Decimal("-1600.00"),
            currency="VES",
            description="COMPRA POS 3311 TRAKI",
            source="provincial",
            source_ref="scr-1",
        ),
    )
    transactions_repo.insert(
        web_db,
        Transaction(
            account_id=bank.id,
            occurred_at=_aware(datetime(2024, 11, 3, tzinfo=UTC)),
            kind=TransactionKind.EXPENSE,
            amount=Decimal("-6400.00"),
            currency="VES",
            description="PAGO MOVIL 04141234567",
            source="provincial",
            source_ref="scr-parked",
            parked=True,
        ),
    )
    return web_db


# ---------------------------------------------------------------------------
# Groups (A6, A7)
# ---------------------------------------------------------------------------


def test_the_three_groups_carry_the_designs_exact_labels_and_hints(
    screen_db: sqlite3.Connection,
) -> None:
    screen = triage_view.build_screen(screen_db)

    assert [(g.bucket, g.label, g.hint) for g in triage_view.GROUPS] == [
        (0, "Needs a category", "One decision each"),
        (1, "Proposed pairs", "Two rows that look like one transfer"),
        (
            2,
            "Priced roughly",
            "No rate within 14 days — Ledger used the nearest one",
        ),
    ]
    assert [g.bucket for g in screen.groups] == [0]


def test_priced_roughly_starts_collapsed_and_the_others_do_not(
    screen_db: sqlite3.Connection,
) -> None:
    by_bucket = {g.bucket: g for g in triage_view.GROUPS}

    assert by_bucket[2].collapsed is True
    assert by_bucket[0].collapsed is False
    assert by_bucket[1].collapsed is False


def test_a_group_with_no_rows_is_absent_from_the_screen(
    screen_db: sqlite3.Connection,
) -> None:
    """A7 — an empty head is noise; the group renders nothing at all."""
    screen = triage_view.build_screen(screen_db)

    assert all(group.items for group in screen.groups)
    assert 1 not in {g.bucket for g in screen.groups}


def test_every_queue_item_lands_in_exactly_one_group(
    screen_db: sqlite3.Connection,
) -> None:
    screen = triage_view.build_screen(screen_db)

    grouped = [item.item_id for group in screen.groups for item in group.items]
    assert grouped == [item.item_id for item in screen.queue.items]


# ---------------------------------------------------------------------------
# Parked panel (A13, F4, F8)
# ---------------------------------------------------------------------------


def test_parked_panel_counts_and_samples_the_parked_rows(
    screen_db: sqlite3.Connection,
) -> None:
    screen = triage_view.build_screen(screen_db)

    assert screen.parked.count == 1
    assert len(screen.parked.sample) == 1
    assert screen.parked.sample[0].txn_card.description == "PAGO MOVIL 04141234567"


def test_parked_sample_is_capped(screen_db: sqlite3.Connection) -> None:
    """266 rows is not a sample. The sheet shows a few of them."""
    bank = accounts_repo.list_all(screen_db)[0]
    for n in range(12):
        transactions_repo.insert(
            screen_db,
            Transaction(
                account_id=bank.id,
                occurred_at=_aware(datetime(2025, 1, 1, tzinfo=UTC)),
                kind=TransactionKind.EXPENSE,
                amount=Decimal("-10.00"),
                currency="VES",
                description=f"PARKED {n}",
                source="provincial",
                source_ref=f"scr-parked-{n}",
                parked=True,
            ),
        )

    screen = triage_view.build_screen(screen_db)

    assert screen.parked.count == 13
    assert len(screen.parked.sample) == triage_view.PARKED_SAMPLE_SIZE


def test_parked_panel_reports_the_oldest_parkable_row(
    screen_db: sqlite3.Connection,
) -> None:
    """The hint under the cutoff field: how far back the cutoff can reach."""
    screen = triage_view.build_screen(screen_db)

    assert screen.parked.oldest == date(2024, 11, 3)


def test_oldest_is_none_when_nothing_is_uncategorised(
    web_db: sqlite3.Connection,
) -> None:
    screen = triage_view.build_screen(web_db)

    assert screen.parked.oldest is None
    assert screen.parked.count == 0


def test_the_cutoff_defaults_to_january_first_of_this_year(
    screen_db: sqlite3.Connection,
) -> None:
    """F4 — the field is pre-filled, and nothing in the schema stores a
    cutoff, so the default is the start of the current year."""
    screen = triage_view.build_screen(screen_db, today=date(2026, 8, 23))

    assert screen.parked.cutoff == date(2026, 1, 1)


# ---------------------------------------------------------------------------
# Integrity banner (A12)
# ---------------------------------------------------------------------------


def test_no_banner_when_every_transfer_has_two_legs(
    screen_db: sqlite3.Connection,
) -> None:
    screen = triage_view.build_screen(screen_db)

    assert screen.banner is None


def test_banner_names_the_account_date_and_amount_of_the_orphan_leg(
    screen_db: sqlite3.Connection,
) -> None:
    funding = accounts_repo.insert(
        screen_db,
        Account(
            name="Binance Funding",
            kind=AccountKind.CRYPTO_FUNDING,
            currency="USDT",
            institution="Binance",
        ),
    )
    transactions_repo.insert(
        screen_db,
        Transaction(
            account_id=funding.id,
            occurred_at=_aware(datetime(2026, 6, 29, tzinfo=UTC)),
            kind=TransactionKind.TRANSFER,
            amount=Decimal("-96.40"),
            currency="USDT",
            description="P2P sell",
            source="binance",
            source_ref="orphan-leg",
            transfer_id="orphan-transfer",
        ),
    )

    screen = triage_view.build_screen(screen_db, today=date(2026, 8, 23))

    assert screen.banner is not None
    assert screen.banner.title == "One transfer has a single leg"
    assert screen.banner.body == (
        "Binance Funding, Jun 29 — 96.40 USDT out with nothing on the other "
        "side. Pair it, or say it was not a transfer."
    )


def test_banner_title_pluralises(screen_db: sqlite3.Connection) -> None:
    funding = accounts_repo.insert(
        screen_db,
        Account(
            name="Binance Funding",
            kind=AccountKind.CRYPTO_FUNDING,
            currency="USDT",
            institution="Binance",
        ),
    )
    for n in (1, 2):
        transactions_repo.insert(
            screen_db,
            Transaction(
                account_id=funding.id,
                occurred_at=_aware(datetime(2026, 6, 29, tzinfo=UTC)),
                kind=TransactionKind.TRANSFER,
                amount=Decimal("-96.40"),
                currency="USDT",
                description="P2P sell",
                source="binance",
                source_ref=f"orphan-{n}",
                transfer_id=f"orphan-transfer-{n}",
            ),
        )

    screen = triage_view.build_screen(screen_db, today=date(2026, 8, 23))

    assert screen.banner is not None
    assert screen.banner.title == "2 transfers have a single leg"


def test_a_positive_orphan_leg_reads_in_not_out(
    screen_db: sqlite3.Connection,
) -> None:
    funding = accounts_repo.insert(
        screen_db,
        Account(
            name="Binance Funding",
            kind=AccountKind.CRYPTO_FUNDING,
            currency="USDT",
            institution="Binance",
        ),
    )
    transactions_repo.insert(
        screen_db,
        Transaction(
            account_id=funding.id,
            occurred_at=_aware(datetime(2026, 6, 29, tzinfo=UTC)),
            kind=TransactionKind.TRANSFER,
            amount=Decimal("96.40"),
            currency="USDT",
            description="P2P buy",
            source="binance",
            source_ref="orphan-in",
            transfer_id="orphan-in",
        ),
    )

    screen = triage_view.build_screen(screen_db, today=date(2026, 8, 23))

    assert screen.banner is not None
    assert "96.40 USDT in with nothing on the other side" in screen.banner.body


# ---------------------------------------------------------------------------
# Header + picker (A8, A9, E1-E3)
# ---------------------------------------------------------------------------


def test_the_screen_carries_the_category_picker_payload(
    screen_db: sqlite3.Connection,
) -> None:
    """K1 — the modal's picker is not a second query from the template."""
    screen = triage_view.build_screen(screen_db)

    assert screen.picker.pickable_count > 0
    assert len(screen.picker.chips) == 8


def test_the_walk_list_is_the_queue_in_bucket_order(
    screen_db: sqlite3.Connection,
) -> None:
    """B2 — collapse is a reading convenience; the run walks everything."""
    screen = triage_view.build_screen(screen_db)

    assert screen.walk == list(screen.queue.items)
