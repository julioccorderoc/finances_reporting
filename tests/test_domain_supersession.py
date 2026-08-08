"""RED — a backfill row that a later native-id sync re-imported must retire.

The backfill mints ``<prefix>:hash:<sha>`` for events the legacy CSV
carried no id for; the live sync mints ``<prefix>:<native-id>``. Dedup is
keyed on ``(source, source_ref)`` (rule-010), so one real event written
under both schemes produces two rows and ``UNIQUE`` never fires. That is
exactly what a deep ``--since`` re-sync did on 2026-08-04.
"""
from __future__ import annotations

import sqlite3
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from finances.domain.reconciliation import run_reconciliation_pass
from finances.domain.supersession import LegacyRefSupersession


def _insert(
    conn: sqlite3.Connection,
    *,
    account_id: int,
    occurred_at: str,
    kind: str,
    amount: str,
    currency: str,
    source_ref: str,
    source: str = "binance",
    category_id: int | None = None,
    transfer_id: str | None = None,
    user_rate: str | None = None,
    notes: str | None = None,
) -> int:
    cur = conn.execute(
        """
        INSERT INTO transactions
            (account_id, occurred_at, kind, amount, currency, description,
             category_id, transfer_id, user_rate, source, source_ref,
             needs_review, notes)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?)
        """,
        (
            account_id, occurred_at, kind, amount, currency, "row",
            category_id, transfer_id, user_rate, source, source_ref, notes,
        ),
    )
    return int(cur.lastrowid)


def _ids(conn: sqlite3.Connection) -> set[int]:
    return {int(r[0]) for r in conn.execute("SELECT id FROM transactions")}


def _row(conn: sqlite3.Connection, tid: int) -> sqlite3.Row:
    conn.row_factory = sqlite3.Row
    return conn.execute("SELECT * FROM transactions WHERE id = ?", (tid,)).fetchone()


def test_legacy_row_retires_in_favour_of_its_native_twin(
    seeded_db: sqlite3.Connection,
) -> None:
    legacy = _insert(
        seeded_db, account_id=2, occurred_at="2026-03-30T00:00:00-04:00",
        kind="income", amount="1540", currency="USDC",
        source_ref="deposit:hash:397ec66205a6792d",
    )
    native = _insert(
        seeded_db, account_id=2, occurred_at="2026-03-30T12:00:00+00:00",
        kind="income", amount="1540", currency="USDC",
        source_ref="deposit:s6o9uEuGpo366tZu",
    )

    report = run_reconciliation_pass(LegacyRefSupersession(seeded_db))

    assert report.proposals_found == 1
    assert report.proposals_applied == 1
    assert report.errors == []
    assert legacy not in _ids(seeded_db)
    assert native in _ids(seeded_db)


def test_the_native_row_inherits_what_only_the_legacy_row_carried(
    seeded_db: sqlite3.Connection,
) -> None:
    _insert(
        seeded_db, account_id=2, occurred_at="2026-03-30T00:00:00-04:00",
        kind="income", amount="1540", currency="USDC",
        source_ref="deposit:hash:aaa", category_id=1, notes="salary",
    )
    native = _insert(
        seeded_db, account_id=2, occurred_at="2026-03-30T12:00:00+00:00",
        kind="income", amount="1540", currency="USDC",
        source_ref="deposit:realtxid",
    )

    run_reconciliation_pass(LegacyRefSupersession(seeded_db))

    kept = _row(seeded_db, native)
    assert kept["category_id"] == 1
    assert kept["notes"] == "salary"


def test_a_pairing_the_legacy_row_anchored_moves_to_the_survivor(
    seeded_db: sqlite3.Connection,
) -> None:
    """A P2P sell paired to a bank deposit must not orphan that deposit."""
    bank = _insert(
        seeded_db, account_id=1, occurred_at="2025-12-06T00:00:00-04:00",
        kind="transfer", amount="13000", currency="VES",
        source="provincial", source_ref="hash:bank", transfer_id="grp-1",
    )
    legacy = _insert(
        seeded_db, account_id=3, occurred_at="2025-12-06T00:00:00-04:00",
        kind="transfer", amount="-31.18", currency="USDT",
        source_ref="p2p:hash:74d977934c9d79b4", transfer_id="grp-1",
        category_id=17,
    )
    native = _insert(
        seeded_db, account_id=2, occurred_at="2025-12-06T18:00:00+00:00",
        kind="expense", amount="-31.12", currency="USDT",
        source_ref="p2p:22830659307723481088",
    )

    report = run_reconciliation_pass(LegacyRefSupersession(seeded_db))

    assert report.errors == []
    assert legacy not in _ids(seeded_db)
    survivor = _row(seeded_db, native)
    assert survivor["transfer_id"] == "grp-1", "bank row would be orphaned"
    assert survivor["kind"] == "transfer", "a transfer leg must be kind=transfer"
    assert _row(seeded_db, bank)["transfer_id"] == "grp-1"


def test_a_legacy_pair_whose_both_legs_are_superseded_just_goes(
    seeded_db: sqlite3.Connection,
) -> None:
    """Both convert legs have native twins — the whole legacy group vanishes."""
    for leg, amount, currency in (("from", "-1666.21", "USDC"), ("to", "1666.44", "USDT")):
        _insert(
            seeded_db, account_id=2, occurred_at="2025-12-31T00:00:00-04:00",
            kind="transfer", amount=amount, currency=currency,
            source_ref=f"convert:hash:ada6bd03:{leg}", transfer_id="legacy-grp",
            category_id=17,
        )
        _insert(
            seeded_db, account_id=2, occurred_at="2025-12-31T10:00:00+00:00",
            kind="transfer", amount=amount, currency=currency,
            source_ref=f"convert:2168621303777619118:{leg}", transfer_id="native-grp",
        )

    report = run_reconciliation_pass(LegacyRefSupersession(seeded_db))

    assert report.proposals_applied == 2
    assert report.errors == []
    remaining = {
        r[0] for r in seeded_db.execute("SELECT source_ref FROM transactions")
    }
    assert not any(":hash:" in ref for ref in remaining)
    survivors = seeded_db.execute(
        "SELECT DISTINCT transfer_id FROM transactions WHERE transfer_id IS NOT NULL"
    ).fetchall()
    assert [r[0] for r in survivors] == ["native-grp"]


def test_a_native_twin_already_pledged_elsewhere_is_reported_not_forced(
    seeded_db: sqlite3.Connection,
) -> None:
    """Re-pointing would silently steal the twin from another pair."""
    _insert(
        seeded_db, account_id=1, occurred_at="2025-12-06T00:00:00-04:00",
        kind="transfer", amount="13000", currency="VES",
        source="provincial", source_ref="hash:bank-a", transfer_id="grp-a",
    )
    legacy = _insert(
        seeded_db, account_id=3, occurred_at="2025-12-06T00:00:00-04:00",
        kind="transfer", amount="-31.18", currency="USDT",
        source_ref="p2p:hash:aaa", transfer_id="grp-a",
    )
    _insert(
        seeded_db, account_id=1, occurred_at="2025-12-05T00:00:00-04:00",
        kind="transfer", amount="12754", currency="VES",
        source="provincial", source_ref="hash:bank-b", transfer_id="grp-b",
    )
    _insert(
        seeded_db, account_id=2, occurred_at="2025-12-06T18:00:00+00:00",
        kind="transfer", amount="-31.12", currency="USDT",
        source_ref="p2p:2283065930", transfer_id="grp-b",
    )

    report = run_reconciliation_pass(LegacyRefSupersession(seeded_db))

    assert report.proposals_found == 1
    assert report.proposals_applied == 0
    assert len(report.errors) == 1
    assert legacy in _ids(seeded_db), "conflicted row must survive for a human"


def test_rows_with_no_native_twin_are_left_alone(
    seeded_db: sqlite3.Connection,
) -> None:
    """187 legacy rows are the only record of their event — never sweep them."""
    lonely = _insert(
        seeded_db, account_id=3, occurred_at="2026-01-01T00:00:00-04:00",
        kind="expense", amount="-4", currency="USDT",
        source_ref="withdraw:hash:lonely",
    )

    report = run_reconciliation_pass(LegacyRefSupersession(seeded_db))

    assert report.proposals_found == 0
    assert lonely in _ids(seeded_db)


def test_a_different_event_of_similar_size_is_not_matched(
    seeded_db: sqlite3.Connection,
) -> None:
    """Tolerance covers legacy rounding, not genuinely distinct amounts."""
    legacy = _insert(
        seeded_db, account_id=2, occurred_at="2026-03-30T00:00:00-04:00",
        kind="income", amount="1540", currency="USDC",
        source_ref="deposit:hash:aaa",
    )
    _insert(
        seeded_db, account_id=2, occurred_at="2026-03-30T12:00:00+00:00",
        kind="income", amount="1200", currency="USDC",
        source_ref="deposit:realtxid",
    )

    report = run_reconciliation_pass(LegacyRefSupersession(seeded_db))

    assert report.proposals_found == 0
    assert legacy in _ids(seeded_db)


def test_the_pass_is_idempotent(seeded_db: sqlite3.Connection) -> None:
    _insert(
        seeded_db, account_id=2, occurred_at="2026-03-30T00:00:00-04:00",
        kind="income", amount="1540", currency="USDC",
        source_ref="deposit:hash:aaa",
    )
    _insert(
        seeded_db, account_id=2, occurred_at="2026-03-30T12:00:00+00:00",
        kind="income", amount="1540", currency="USDC",
        source_ref="deposit:realtxid",
    )

    run_reconciliation_pass(LegacyRefSupersession(seeded_db))
    second = run_reconciliation_pass(LegacyRefSupersession(seeded_db))

    assert second.proposals_found == 0
