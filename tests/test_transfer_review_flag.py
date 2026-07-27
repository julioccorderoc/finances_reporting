"""Pairing must retire the review flag it inherits (rule-002 / rule-006).

``needs_review`` means one thing: *the importer could not work out a
category, so a human should look*. Once two rows are paired into a
transfer that question is answered — a transfer is not categorised at
all, and ``finances/web/services/triage.py`` already treats
``kind='transfer'`` as evicted from the review surface.

The flag is stored, though, and ``_promote_to_transfer`` only ever wrote
``kind``/``transfer_id``/``updated_at``. So a Provincial row imported
with the flag up (``provincial.py`` sets ``needs_review = category_id is
None``) kept it forever after being paired, while its Binance twin never
had one — the Binance importer does not compute the flag at all.

Result: triage said "resolved", and six other surfaces that read the
column directly said "still needs review", about the same row. Fifteen
already-paired transfers were sitting in the production review queue.

These tests pin both halves of the fix: the write path stops leaving the
flag up, and the ledger-wide invariant that no paired transfer carries
one.
"""

from __future__ import annotations

import sqlite3
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from finances.db.repos import accounts as acc_repo
from finances.db.repos import transactions as txn_repo
from finances.domain.models import Transaction, TransactionKind
from finances.domain.transfers import create_transfer

FIXED_AT = datetime(2026, 7, 7, 12, 0, tzinfo=UTC)


def _account_id(conn: sqlite3.Connection, name: str) -> int:
    acct = acc_repo.get_by_name(conn, name)
    assert acct is not None and acct.id is not None
    return acct.id


def _flagged_row(
    conn: sqlite3.Connection,
    *,
    account_id: int,
    amount: Decimal,
    currency: str,
    kind: TransactionKind = TransactionKind.INCOME,
    source_ref: str,
    user_rate: Decimal | None = None,
) -> Transaction:
    """A row imported without a category — exactly what Provincial writes."""
    return txn_repo.insert(
        conn,
        Transaction(
            account_id=account_id,
            occurred_at=FIXED_AT,
            kind=kind,
            amount=amount,
            currency=currency,
            source="provincial",
            source_ref=source_ref,
            user_rate=user_rate,
            needs_review=True,
        ),
    )


def _needs_review(conn: sqlite3.Connection, txn_id: int) -> bool:
    row = conn.execute(
        "SELECT needs_review FROM transactions WHERE id = ?", (txn_id,)
    ).fetchone()
    assert row is not None
    return bool(row[0])


class TestPairingClearsReviewFlag:
    def test_both_anchors_mode_clears_the_flag_on_both_legs(
        self, seeded_db: sqlite3.Connection
    ):
        """The bank-anchored path: two existing rows promoted in place."""
        provincial = _account_id(seeded_db, "Provincial Bolivares")
        spot = _account_id(seeded_db, "Binance Spot")

        bank = _flagged_row(
            seeded_db,
            account_id=provincial,
            amount=Decimal("75000"),
            currency="VES",
            source_ref="hash:review-both-bank",
        )
        sell = _flagged_row(
            seeded_db,
            account_id=spot,
            amount=Decimal("-98.32"),
            currency="USDT",
            kind=TransactionKind.EXPENSE,
            source_ref="p2p:review-both-sell",
            user_rate=Decimal("762.8"),
        )
        assert bank.id is not None and sell.id is not None
        assert _needs_review(seeded_db, bank.id)
        assert _needs_review(seeded_db, sell.id)

        create_transfer(
            seeded_db,
            anchor_transaction_id=bank.id,
            counterpart_transaction_id=sell.id,
        )

        assert not _needs_review(seeded_db, bank.id)
        assert not _needs_review(seeded_db, sell.id)

    def test_anchor_only_mode_clears_the_flag_on_the_promoted_row(
        self, seeded_db: sqlite3.Connection
    ):
        provincial = _account_id(seeded_db, "Provincial Bolivares")
        spot = _account_id(seeded_db, "Binance Spot")

        anchor = _flagged_row(
            seeded_db,
            account_id=provincial,
            amount=Decimal("75000"),
            currency="VES",
            source_ref="hash:review-anchor-only",
        )
        assert anchor.id is not None
        assert _needs_review(seeded_db, anchor.id)

        create_transfer(
            seeded_db,
            anchor_transaction_id=anchor.id,
            from_account_id=provincial,
            to_account_id=spot,
            amount=Decimal("75000"),
            currency="VES",
            occurred_at=FIXED_AT,
        )

        assert not _needs_review(seeded_db, anchor.id)

    def test_freshly_inserted_legs_are_never_flagged(
        self, seeded_db: sqlite3.Connection
    ):
        provincial = _account_id(seeded_db, "Provincial Bolivares")
        spot = _account_id(seeded_db, "Binance Spot")

        pair = create_transfer(
            seeded_db,
            from_account_id=spot,
            to_account_id=provincial,
            amount=Decimal("50"),
            currency="USDT",
            occurred_at=FIXED_AT,
        )

        assert not _needs_review(seeded_db, pair.from_transaction_id)
        assert not _needs_review(seeded_db, pair.to_transaction_id)

    def test_clearing_the_flag_leaves_the_category_alone(
        self, seeded_db: sqlite3.Connection
    ):
        """Lowering the flag is not the same as erasing a human's work.

        A row the owner had already categorised keeps that category; only
        the "someone should look at this" marker is retired.
        """
        provincial = _account_id(seeded_db, "Provincial Bolivares")
        spot = _account_id(seeded_db, "Binance Spot")
        category_id = seeded_db.execute(
            "SELECT id FROM categories LIMIT 1"
        ).fetchone()[0]

        bank = _flagged_row(
            seeded_db,
            account_id=provincial,
            amount=Decimal("75000"),
            currency="VES",
            source_ref="hash:review-keeps-category",
        )
        sell = _flagged_row(
            seeded_db,
            account_id=spot,
            amount=Decimal("-98.32"),
            currency="USDT",
            kind=TransactionKind.EXPENSE,
            source_ref="p2p:review-keeps-category",
            user_rate=Decimal("762.8"),
        )
        assert bank.id is not None and sell.id is not None
        seeded_db.execute(
            "UPDATE transactions SET category_id = ? WHERE id = ?",
            (category_id, bank.id),
        )

        create_transfer(
            seeded_db,
            anchor_transaction_id=bank.id,
            counterpart_transaction_id=sell.id,
        )

        after = seeded_db.execute(
            "SELECT category_id, needs_review FROM transactions WHERE id = ?",
            (bank.id,),
        ).fetchone()
        assert after["category_id"] == category_id
        assert not bool(after["needs_review"])


class TestLedgerInvariant:
    """The property the six raw-column readers each assume, stated once.

    Every surface that filters on ``needs_review = 1`` is really asking
    "what is unresolved?". A paired transfer is resolved by definition,
    so it must never appear. Asserting it here means a future write path
    that forgets to retire the flag fails CI instead of quietly seeding
    another fifteen rows into the queue.
    """

    def test_no_paired_transfer_is_left_flagged_for_review(
        self, seeded_db: sqlite3.Connection
    ):
        provincial = _account_id(seeded_db, "Provincial Bolivares")
        spot = _account_id(seeded_db, "Binance Spot")

        for n in range(3):
            bank = _flagged_row(
                seeded_db,
                account_id=provincial,
                amount=Decimal("20000"),
                currency="VES",
                source_ref=f"hash:invariant-bank-{n}",
            )
            sell = _flagged_row(
                seeded_db,
                account_id=spot,
                amount=Decimal("-26.2"),
                currency="USDT",
                kind=TransactionKind.EXPENSE,
                source_ref=f"p2p:invariant-sell-{n}",
                user_rate=Decimal("763.36"),
            )
            assert bank.id is not None and sell.id is not None
            create_transfer(
                seeded_db,
                anchor_transaction_id=bank.id,
                counterpart_transaction_id=sell.id,
            )

        stragglers = seeded_db.execute(
            "SELECT COUNT(*) FROM transactions "
            "WHERE kind = 'transfer' AND transfer_id IS NOT NULL "
            "AND needs_review = 1"
        ).fetchone()[0]
        assert stragglers == 0


class TestMigration016:
    """The 15 rows already stuck in the production queue."""

    def test_migration_clears_flags_on_existing_paired_transfers(
        self, seeded_db: sqlite3.Connection
    ):
        from finances.db.migrate import apply_migrations

        provincial = _account_id(seeded_db, "Provincial Bolivares")
        spot = _account_id(seeded_db, "Binance Spot")

        bank = _flagged_row(
            seeded_db,
            account_id=provincial,
            amount=Decimal("75000"),
            currency="VES",
            source_ref="hash:migration-stuck",
        )
        sell = _flagged_row(
            seeded_db,
            account_id=spot,
            amount=Decimal("-98.32"),
            currency="USDT",
            kind=TransactionKind.EXPENSE,
            source_ref="p2p:migration-stuck",
            user_rate=Decimal("762.8"),
        )
        assert bank.id is not None and sell.id is not None

        # deliberate malformed fixture: reproduce the pre-fix state, a
        # paired transfer whose review flag was never retired.
        seeded_db.execute(
            "UPDATE transactions SET kind = 'transfer', transfer_id = ?, "
            "needs_review = 1 WHERE id IN (?, ?)",
            ("stuck-pair", bank.id, sell.id),
        )
        seeded_db.execute(
            "DELETE FROM _migrations WHERE filename LIKE '016_%'"
        )
        seeded_db.commit()

        apply_migrations(seeded_db)

        assert not _needs_review(seeded_db, bank.id)
        assert not _needs_review(seeded_db, sell.id)

    def test_migration_leaves_unpaired_rows_flagged(
        self, seeded_db: sqlite3.Connection
    ):
        """An expense awaiting a category is untouched — it is real work."""
        from finances.db.migrate import apply_migrations

        spot = _account_id(seeded_db, "Binance Spot")
        pending = _flagged_row(
            seeded_db,
            account_id=spot,
            amount=Decimal("-42"),
            currency="USDT",
            kind=TransactionKind.EXPENSE,
            source_ref="p2p:migration-untouched",
        )
        assert pending.id is not None
        seeded_db.execute("DELETE FROM _migrations WHERE filename LIKE '016_%'")
        seeded_db.commit()

        apply_migrations(seeded_db)

        assert _needs_review(seeded_db, pending.id)
