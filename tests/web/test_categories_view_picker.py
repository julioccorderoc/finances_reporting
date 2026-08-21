"""``categories_view.picker_payload`` — everything CatPicker needs (rule-011 RED first).

Wave 1.2 of the triage redesign supplies the data; Wave 2 draws it. The
payload carries the eight numbered chips (E3, ranked over 12 months of
real usage), the full pickable list grouped EXPENSE / INCOME (E7), and
for every entry both the label and its disambiguating test so search can
match either one client-side (E4) and the test strip always has something
to show (E5).

Sign convention: real expense amounts are NEGATIVE. Seeds here follow it.
"""

from __future__ import annotations

import sqlite3
from datetime import UTC, datetime
from decimal import Decimal
from uuid import uuid4

from finances.db.repos import accounts as accounts_repo
from finances.db.repos import categories as categories_repo
from finances.db.repos import transactions as transactions_repo
from finances.domain.models import (
    Account,
    AccountKind,
    Transaction,
    TransactionKind,
)
from finances.web.services.categories_view import picker_payload

TODAY = datetime(2026, 8, 21, tzinfo=UTC).date()
RECENT = datetime(2026, 8, 1, tzinfo=UTC)
ANCIENT = datetime(2024, 1, 5, tzinfo=UTC)


def _account(conn: sqlite3.Connection) -> int:
    acct = accounts_repo.insert(
        conn,
        Account(
            name="Provincial",
            kind=AccountKind.BANK,
            currency="VES",
            institution="Provincial",
        ),
    )
    assert acct.id is not None
    return acct.id


def _use(
    conn: sqlite3.Connection,
    account_id: int,
    name: str,
    *,
    times: int,
    kind: TransactionKind = TransactionKind.EXPENSE,
    when: datetime = RECENT,
) -> None:
    cat = categories_repo.get_by_name(conn, kind, name)
    assert cat is not None, f"seed category {name} missing"
    amount = Decimal("-25.00") if kind is TransactionKind.EXPENSE else Decimal("25.00")
    for _ in range(times):
        transactions_repo.insert(
            conn,
            Transaction(
                account_id=account_id,
                occurred_at=when,
                kind=kind,
                amount=amount,
                currency="VES",
                description="picker seed",
                category_id=cat.id,
                source="test",
                source_ref=f"pk-{uuid4()}",
            ),
        )


def test_chips_are_numbered_one_to_eight_in_usage_order(
    web_db: sqlite3.Connection,
) -> None:
    acct = _account(web_db)
    _use(web_db, acct, "Transport", times=5)
    _use(web_db, acct, "Groceries", times=3)
    _use(web_db, acct, "Dating", times=1)

    payload = picker_payload(web_db, today=TODAY)

    assert [c.number for c in payload.chips] == [1, 2, 3, 4, 5, 6, 7, 8]
    assert [c.category.label for c in payload.chips][:3] == [
        "Transport",
        "Groceries",
        "Dating",
    ]


def test_chips_ignore_usage_older_than_twelve_months(
    web_db: sqlite3.Connection,
) -> None:
    acct = _account(web_db)
    _use(web_db, acct, "Education", times=40, when=ANCIENT)
    _use(web_db, acct, "Transport", times=2)

    payload = picker_payload(web_db, today=TODAY)

    assert payload.chips[0].category.label == "Transport"


def test_fees_never_takes_a_chip_but_stays_in_the_list(
    web_db: sqlite3.Connection,
) -> None:
    """Owner decision 2026-08-21: rules assign Fees, so it must not own key 1."""
    acct = _account(web_db)
    _use(web_db, acct, "Fees", times=99)
    _use(web_db, acct, "Transport", times=2)

    payload = picker_payload(web_db, today=TODAY)

    assert "Fees" not in [c.category.label for c in payload.chips]
    assert payload.chips[0].category.label == "Transport"
    assert "Fees" in [c.label for c in payload.categories]


def test_auto_only_and_retired_categories_never_appear(
    web_db: sqlite3.Connection,
) -> None:
    payload = picker_payload(web_db, today=TODAY)
    labels = {c.label for c in payload.categories}
    assert labels.isdisjoint(
        {
            "Internal Transfer",
            "External Transfer",
            "FX Diff",
            "Reconciliation",
            "Interest",
            "Clothing",
            "Lifestyle",
            "Tools",
        }
    )
    chip_labels = {c.category.label for c in payload.chips}
    assert chip_labels <= labels


def test_groups_are_expense_then_income(web_db: sqlite3.Connection) -> None:
    payload = picker_payload(web_db, today=TODAY)

    assert [g.kind for g in payload.groups] == ["expense", "income"]
    assert [g.label for g in payload.groups] == ["EXPENSE", "INCOME"]
    expense, income = payload.groups
    assert "Groceries" in [c.label for c in expense.categories]
    assert "Salary" in [c.label for c in income.categories]
    assert [c.label for c in expense.categories] == sorted(
        c.label for c in expense.categories
    )


def test_every_entry_carries_label_kind_icon_and_test(
    web_db: sqlite3.Connection,
) -> None:
    payload = picker_payload(web_db, today=TODAY)
    groceries = next(c for c in payload.categories if c.label == "Groceries")

    assert groceries.kind == "expense"
    assert groceries.icon == "shopping-basket"
    assert groceries.test.startswith("Food consumed at home")
    assert all(c.test for c in payload.categories), "E5: every entry needs its test"
    assert all(c.icon for c in payload.categories)


def test_counts_drive_the_other_n_disclosure(web_db: sqlite3.Connection) -> None:
    payload = picker_payload(web_db, today=TODAY)

    assert payload.pickable_count == len(payload.categories)
    assert payload.pickable_count == sum(len(g.categories) for g in payload.groups)
    assert payload.other_count == payload.pickable_count - len(payload.chips)


def test_chip_entries_are_the_same_objects_the_list_holds(
    web_db: sqlite3.Connection,
) -> None:
    """The chip must not carry a second, drifting copy of the test sentence."""
    payload = picker_payload(web_db, today=TODAY)
    by_id = {c.id: c for c in payload.categories}
    for chip in payload.chips:
        assert by_id[chip.category.id] == chip.category


def test_payload_is_stable_without_any_usage_history(
    web_db: sqlite3.Connection,
) -> None:
    """A fresh DB still fills eight chips, padded in seed order."""
    payload = picker_payload(web_db, today=TODAY)

    assert len(payload.chips) == 8
    assert len({c.category.id for c in payload.chips}) == 8
