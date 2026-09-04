"""Cash income, and the note both cash writes now carry (ADR-008 amendment).

``finances/ingest/cash_cli.py`` could only ever spend. ``add_cash_expense``
was the single manual write in the whole system, and it hard-coded
``kind=expense`` and a negative sign — so cash coming IN (a repayment
handed over in dollars, a sale, money returned) had nowhere to go but a
plug or a lie in another account.

``add_cash_income`` is its twin, and the two are deliberately symmetric:
same account (``Cash USD``, rule-008), same ``source='cash_cli'``, same
UUIDv4 ``source_ref`` (rule-010), same repo call. The only differences are
the kind and the sign — income is stored POSITIVE, expense NEGATIVE, which
is what ``v_account_balances`` sums and what
``project_expense_sign_convention`` says the rest of the ledger assumes.

Both now accept ``notes``, because the viewer's Add-transaction dialog
offers one and a note that is silently dropped is worse than no field.
"""

from __future__ import annotations

import re
import sqlite3
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from finances.db.repos import categories as categories_repo
from finances.db.repos import transactions as transactions_repo
from finances.domain.models import TransactionKind

_UUID4_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"
)

WHEN = datetime(2026, 9, 4, 0, 0, tzinfo=UTC)


# ---------------------------------------------------------------------------
# add_cash_income
# ---------------------------------------------------------------------------


def test_add_cash_income_writes_a_positive_row_on_cash_usd(
    in_memory_db: sqlite3.Connection,
) -> None:
    from finances.ingest.cash_cli import (
        CASH_CLI_SOURCE,
        CASH_USD_ACCOUNT_NAME,
        add_cash_income,
        ensure_cash_usd_account,
    )

    txn = add_cash_income(
        in_memory_db,
        amount=Decimal("40.00"),
        description="Andrés paid me back",
        occurred_at=WHEN,
    )

    assert txn.kind is TransactionKind.INCOME
    assert txn.amount == Decimal("40.00"), "income keeps its sign; only expense flips"
    assert txn.currency == "USD"
    assert txn.source == CASH_CLI_SOURCE
    assert _UUID4_RE.match(txn.source_ref), txn.source_ref
    assert txn.needs_review is False

    cash = ensure_cash_usd_account(in_memory_db)
    assert txn.account_id == cash.id
    assert cash.name == CASH_USD_ACCOUNT_NAME


def test_add_cash_income_moves_the_balance_up(
    in_memory_db: sqlite3.Connection,
) -> None:
    """The sign is not cosmetic — it is what the balance view adds up."""
    from finances.ingest.cash_cli import add_cash_expense, add_cash_income

    add_cash_income(
        in_memory_db, amount="100", description="repaid", occurred_at=WHEN
    )
    add_cash_expense(
        in_memory_db, amount="30", description="lunch", occurred_at=WHEN
    )

    row = in_memory_db.execute(
        "SELECT balance_native FROM v_account_balances WHERE account_name = 'Cash USD'"
    ).fetchone()
    assert Decimal(str(row["balance_native"])) == Decimal("70")


def test_add_cash_income_records_the_category(
    in_memory_db: sqlite3.Connection,
) -> None:
    from finances.ingest.cash_cli import add_cash_income

    salary = categories_repo.get_by_name(
        in_memory_db, TransactionKind.INCOME, "Salary"
    )
    assert salary is not None

    txn = add_cash_income(
        in_memory_db,
        amount="500",
        description="September",
        occurred_at=WHEN,
        category_id=salary.id,
    )
    assert txn.category_id == salary.id


def test_add_cash_income_rejects_a_non_positive_amount(
    in_memory_db: sqlite3.Connection,
) -> None:
    from finances.ingest.cash_cli import add_cash_income

    with pytest.raises(ValueError, match="positive"):
        add_cash_income(
            in_memory_db, amount="0", description="nothing", occurred_at=WHEN
        )
    with pytest.raises(ValueError, match="positive"):
        add_cash_income(
            in_memory_db, amount="-5", description="backwards", occurred_at=WHEN
        )


def test_add_cash_income_honours_an_explicit_source_ref(
    in_memory_db: sqlite3.Connection,
) -> None:
    """rule-010: same (source, source_ref) twice is one row, not two."""
    from finances.ingest.cash_cli import add_cash_income

    first = add_cash_income(
        in_memory_db,
        amount="10",
        description="tip",
        occurred_at=WHEN,
        source_ref="fixed-ref",
    )
    second = add_cash_income(
        in_memory_db,
        amount="10",
        description="tip",
        occurred_at=WHEN,
        source_ref="fixed-ref",
    )
    assert first.id == second.id


# ---------------------------------------------------------------------------
# notes, on both writes
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("writer", ["add_cash_expense", "add_cash_income"])
def test_cash_writes_store_the_note(
    in_memory_db: sqlite3.Connection, writer: str
) -> None:
    import finances.ingest.cash_cli as cash_cli

    txn = getattr(cash_cli, writer)(
        in_memory_db,
        amount="12",
        description="empanadas",
        occurred_at=WHEN,
        notes="split with Ana, she owes 6",
    )
    stored = transactions_repo.get_by_id(in_memory_db, txn.id)
    assert stored is not None
    assert stored.notes == "split with Ana, she owes 6"


@pytest.mark.parametrize("writer", ["add_cash_expense", "add_cash_income"])
def test_cash_writes_leave_notes_null_when_not_given(
    in_memory_db: sqlite3.Connection, writer: str
) -> None:
    import finances.ingest.cash_cli as cash_cli

    txn = getattr(cash_cli, writer)(
        in_memory_db, amount="12", description="empanadas", occurred_at=WHEN
    )
    assert txn.notes is None
