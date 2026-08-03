"""A category may not contradict the kind of the row it is applied to.

``categories`` carries a ``kind`` column and nothing ever compared it to
``transactions.kind``. The picker offered every category on every row and
``apply_edit`` checked only that the id existed, so the live ledger
accumulated 65 rows whose category belongs to a different kind — including
6 income rows filed under ``Fees`` and 1 expense row under ``Loan
Repayment``.

The rule is deliberately not "kinds must be equal". A *transfer*-kind
category on an income or expense row is meaningful — it is the owner saying
"this money moved, it was not spent", which
:data:`finances.domain.money.SQL_NOT_CURRENCY_MOVEMENT` now acts on. What is
never meaningful is an expense category on an income row, or the reverse.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from finances.db.repos import accounts as accounts_repo
from finances.db.repos import categories as categories_repo
from finances.db.repos import transactions as txn_repo
from finances.domain.models import (
    Account,
    AccountKind,
    Transaction,
    TransactionKind,
)
from finances.web.services.transactions_write import (
    TransactionEditRequest,
    apply_edit,
)


@pytest.fixture
def row_and_categories(in_memory_db):
    conn = in_memory_db
    account = accounts_repo.insert(
        conn,
        Account(name="Binance Spot", kind=AccountKind.CRYPTO_SPOT, currency="USDT"),
    )
    expense_txn = txn_repo.insert(
        conn,
        Transaction(
            account_id=account.id,
            occurred_at=datetime(2026, 5, 12, tzinfo=UTC),
            kind=TransactionKind.EXPENSE,
            amount=Decimal("-40"),
            currency="USDT",
            description="row",
            source="binance",
            source_ref="e1",
        ),
    )
    income_txn = txn_repo.insert(
        conn,
        Transaction(
            account_id=account.id,
            occurred_at=datetime(2026, 5, 12, tzinfo=UTC),
            kind=TransactionKind.INCOME,
            amount=Decimal("90"),
            currency="USDT",
            description="row",
            source="binance",
            source_ref="i1",
        ),
    )
    cats = {
        "expense": categories_repo.get_by_name(
            conn, TransactionKind.EXPENSE, "Groceries"
        ),
        "income": categories_repo.get_by_name(conn, TransactionKind.INCOME, "Salary"),
        "transfer": categories_repo.get_by_name(
            conn, TransactionKind.TRANSFER, "Internal Transfer"
        ),
    }
    assert all(c is not None for c in cats.values())
    return conn, expense_txn, income_txn, cats


def test_matching_kind_is_accepted(row_and_categories):
    conn, expense_txn, _income, cats = row_and_categories
    card = apply_edit(
        conn,
        txn_id=expense_txn.id,
        req=TransactionEditRequest(set_category=True, category_id=cats["expense"].id),
    )
    assert card.category_name == "Groceries"


def test_transfer_category_is_accepted_on_an_expense(row_and_categories):
    """Not a mismatch — it is how the owner declares money movement."""
    conn, expense_txn, _income, cats = row_and_categories
    card = apply_edit(
        conn,
        txn_id=expense_txn.id,
        req=TransactionEditRequest(set_category=True, category_id=cats["transfer"].id),
    )
    assert card.category_name == "Internal Transfer"


def test_transfer_category_is_accepted_on_an_income(row_and_categories):
    conn, _expense, income_txn, cats = row_and_categories
    card = apply_edit(
        conn,
        txn_id=income_txn.id,
        req=TransactionEditRequest(set_category=True, category_id=cats["transfer"].id),
    )
    assert card.category_name == "Internal Transfer"


def test_income_category_on_an_expense_is_rejected(row_and_categories):
    conn, expense_txn, _income, cats = row_and_categories
    with pytest.raises(ValueError, match="income.*expense|expense.*income"):
        apply_edit(
            conn,
            txn_id=expense_txn.id,
            req=TransactionEditRequest(
                set_category=True, category_id=cats["income"].id
            ),
        )


def test_expense_category_on_an_income_is_rejected(row_and_categories):
    conn, _expense, income_txn, cats = row_and_categories
    with pytest.raises(ValueError, match="income.*expense|expense.*income"):
        apply_edit(
            conn,
            txn_id=income_txn.id,
            req=TransactionEditRequest(
                set_category=True, category_id=cats["expense"].id
            ),
        )


def test_rejection_leaves_the_row_untouched(row_and_categories):
    conn, expense_txn, _income, cats = row_and_categories
    with pytest.raises(ValueError):
        apply_edit(
            conn,
            txn_id=expense_txn.id,
            req=TransactionEditRequest(
                set_category=True,
                category_id=cats["income"].id,
                set_notes=True,
                notes="should not persist",
            ),
        )
    after = txn_repo.get_by_id(conn, expense_txn.id)
    assert after.category_id is None
    assert after.notes is None


def test_clearing_a_category_is_always_allowed(row_and_categories):
    conn, expense_txn, _income, cats = row_and_categories
    apply_edit(
        conn,
        txn_id=expense_txn.id,
        req=TransactionEditRequest(set_category=True, category_id=cats["expense"].id),
    )
    card = apply_edit(
        conn,
        txn_id=expense_txn.id,
        req=TransactionEditRequest(set_category=True, category_id=None),
    )
    assert card.category_name is None


def test_picker_offers_only_the_kinds_a_row_can_take(row_and_categories):
    """The write guard is the backstop; the picker should not offer the trap."""
    conn, _expense, _income, _cats = row_and_categories
    offered = categories_repo.list_for_kind(conn, TransactionKind.EXPENSE)
    kinds = {c.kind for c in offered}
    assert kinds == {TransactionKind.EXPENSE, TransactionKind.TRANSFER}
    assert all(c.active for c in offered)
