"""RED — `unpaired_p2p_sells` must report only sells that could still pair.

The check read 22 on the live ledger while only 4 were actionable. Thirteen
predated every bank statement the ledger holds, and five were priced in USD,
which no Venezuelan bank deposit will ever match. A warning that overstates
by 5x is one nobody reads, which is the failure mode a check exists to avoid.
"""
from __future__ import annotations

import sqlite3

import pytest

from finances.domain.integrity import run_checks


def _finding(conn: sqlite3.Connection, name: str):
    report = run_checks(conn)
    for found in report.findings:
        if found.check == name:
            return found
    return None


def _bank_row(
    conn: sqlite3.Connection, *, occurred_at: str, amount: str = "10000"
) -> int:
    cur = conn.execute(
        """
        INSERT INTO transactions
            (account_id, occurred_at, kind, amount, currency, description,
             source, source_ref, needs_review)
        VALUES (1, ?, 'income', ?, 'VES', 'TRAV0001', 'provincial', ?, 0)
        """,
        (occurred_at, amount, f"hash:bank{occurred_at}{amount}"),
    )
    return int(cur.lastrowid)


def _sell(
    conn: sqlite3.Connection,
    *,
    occurred_at: str,
    fiat: str = "VES",
    rate: str = "300",
    amount: str = "-50",
    order: str = "1",
) -> int:
    cur = conn.execute(
        """
        INSERT INTO transactions
            (account_id, occurred_at, kind, amount, currency, description,
             user_rate, source, source_ref, needs_review)
        VALUES (2, ?, 'expense', ?, 'USDT', ?, ?, 'binance', ?, 0)
        """,
        (
            occurred_at,
            amount,
            f"P2P SELL USDT @ {rate} {fiat} (order {order})",
            rate,
            f"p2p:{order}",
        ),
    )
    return int(cur.lastrowid)


def test_a_sell_that_could_still_pair_is_reported(
    seeded_db: sqlite3.Connection,
) -> None:
    _bank_row(seeded_db, occurred_at="2025-11-01T00:00:00-04:00")
    lonely = _sell(seeded_db, occurred_at="2025-12-21T00:00:00-04:00", order="a")

    found = _finding(seeded_db, "unpaired_p2p_sells")
    assert found is not None
    assert found.sample_ids == [lonely]


def test_a_sell_predating_every_bank_statement_is_not_reported(
    seeded_db: sqlite3.Connection,
) -> None:
    """No bank row can exist for it, so it is not a backlog item."""
    _bank_row(seeded_db, occurred_at="2025-11-01T00:00:00-04:00")
    _sell(seeded_db, occurred_at="2025-10-03T00:00:00-04:00", order="b")

    assert _finding(seeded_db, "unpaired_p2p_sells") is None


def test_a_usd_priced_sell_is_not_reported(
    seeded_db: sqlite3.Connection,
) -> None:
    """A USD trade never produces a bolivar deposit."""
    _bank_row(seeded_db, occurred_at="2025-11-01T00:00:00-04:00")
    _sell(
        seeded_db,
        occurred_at="2026-06-11T00:00:00-04:00",
        fiat="USD",
        rate="1.003",
        order="c",
    )

    assert _finding(seeded_db, "unpaired_p2p_sells") is None


def test_usdt_in_the_description_is_not_mistaken_for_usd(
    seeded_db: sqlite3.Connection,
) -> None:
    """Every sell says 'SELL USDT'; only the fiat after '@' decides."""
    _bank_row(seeded_db, occurred_at="2025-11-01T00:00:00-04:00")
    ves = _sell(seeded_db, occurred_at="2026-03-18T00:00:00-04:00", order="d")

    found = _finding(seeded_db, "unpaired_p2p_sells")
    assert found is not None
    assert found.sample_ids == [ves]


def test_a_sell_whose_denomination_was_never_recorded_is_still_reported(
    seeded_db: sqlite3.Connection,
) -> None:
    """Matches transfers._fiat_is_compatible: reject only a KNOWN mismatch.

    Legacy and backfilled rows carry no '@ <rate> <FIAT>' shape. Dropping
    them would hide exactly the rows that most need a human.
    """
    _bank_row(seeded_db, occurred_at="2025-11-01T00:00:00-04:00")
    cur = seeded_db.execute(
        """
        INSERT INTO transactions
            (account_id, occurred_at, kind, amount, currency, description,
             source, source_ref, needs_review)
        VALUES (2, '2026-01-15T00:00:00-04:00', 'expense', '-40', 'USDT',
                'P2P SELL USDT', 'binance', 'p2p:hash:legacy', 0)
        """
    )
    legacy = int(cur.lastrowid)

    found = _finding(seeded_db, "unpaired_p2p_sells")
    assert found is not None
    assert legacy in found.sample_ids


def test_an_already_paired_sell_is_never_reported(
    seeded_db: sqlite3.Connection,
) -> None:
    _bank_row(seeded_db, occurred_at="2025-11-01T00:00:00-04:00")
    sell = _sell(seeded_db, occurred_at="2025-12-21T00:00:00-04:00", order="e")
    seeded_db.execute(
        "UPDATE transactions SET transfer_id = 'grp' WHERE id = ?", (sell,)
    )

    assert _finding(seeded_db, "unpaired_p2p_sells") is None
