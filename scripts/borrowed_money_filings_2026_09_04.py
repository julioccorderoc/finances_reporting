"""File the borrowed-money rows and fix the 2026-05-11 mispairing.

The owner's decisions of 2026-09-04, taken row by row against the live
ledger and written down in
``docs/plans/2026-09-04-borrowed-money-decisions.md``. Two parts:

**1. Twenty-five rows get a category.** Money lent *to* him goes to
`Borrowed` (migration 025), money he holds or forwards for someone to
`External Transfer`, money coming back from a loan he granted to
`Loan Repayment`, the company purchase he fronted to `Lending`. Nothing
here moves a balance: a category decides only whether a row counts as
income or spending (``domain/money.py``), never what an account holds.

**2. Sell 1080 is paired with the wrong deposit.** Two identical 20,000 Bs
deposits arrived on 2026-05-10; the pairer took one (6937) for sell 1081
and then matched the next sell, 1080, against 6935 — a 20,018.42 credit
from his mother, an amount no P2P deposit has. The round 20,000 (6940)
sat unclaimed. So: break 4f2dfd0a…, pair 1080 with 6940, and 6935 goes
back to Triage as the ordinary deposit it always was.

``transfers.unpair`` refuses a pair made before migration 024 recorded
what each row was beforehand, and all 286 pairs in the ledger predate it.
That refusal is right in general — restoring a leg an importer created as
``kind='transfer'`` to ``expense`` because the amount is negative invents
history. It is *not* right here, and only here, because both legs are
known: 6935 is a Provincial credit, which that ingest writes as
``income``; 1080 is a ``p2p:`` sell, which ``ingest.binance`` writes as
``expense`` (``RawBinanceP2pRow.to_transaction``). The script writes those
two pre-images, and only those two, then lets ``unpair`` do the rest —
rather than teaching ``unpair`` to guess for 286 pairs it cannot know.

Preconditions are checked before anything is written: every row must
still carry the amount, date and category the decisions document found,
and the pair must still be the one described. If one differs, nothing
runs.

Dry-run by default: the ledger is copied (sqlite backup API) to a
temporary file and the changes run THERE, so the real file is never
opened for writing — a rollback would not be enough, since the write
paths commit on their own (the connection is autocommit).

    uv run python scripts/borrowed_money_filings_2026_09_04.py --db finances.db
    uv run python scripts/borrowed_money_filings_2026_09_04.py --db finances.db --apply
"""

from __future__ import annotations

import argparse
import sqlite3
import tempfile
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

from finances.db.connection import get_connection
from finances.db.migrate import apply_migrations
from finances.db.repos import transactions as tx_repo
from finances.domain import integrity, transfers
from finances.domain.models import TransactionKind
from finances.web.services.transactions_write import (
    TransactionEditRequest,
    apply_edit,
)
from finances.web.services.triage import confirm_pair

# ---------------------------------------------------------------------------
# Part 1 — the filings
# ---------------------------------------------------------------------------

# (id, expected date, expected amount, category to file under, why).
# Straight from the decisions document, so the two can be diffed.
FILINGS: tuple[tuple[int, str, str, str, str], ...] = (
    # Hugo lent 6,000 Bs and the same day 6,000 Bs went back.
    (1871, "2026-06-11", "6000.00", "Borrowed", "prestamo de hugo hacia mi"),
    (1869, "2026-06-11", "-6000.00", "Borrowed", "pago deuda hugo"),
    # His mother lent 10,000 Bs on the 15th; he repaid it on the 17th.
    (7564, "2026-08-15", "10000.00", "Borrowed", "loan from mom"),
    (7671, "2026-08-17", "-10000.00", "Borrowed", "repaying mom's 10,000"),
    # Money that is not his: held for his sister, and sent back in part.
    (7669, "2026-08-17", "73283.60", "External Transfer", "holding it for Natalia"),
    (7654, "2026-08-22", "-9100.00", "External Transfer", "returning part of it"),
    # His mother's 44,477.90 — forwarded, not earned.
    (7573, "2026-08-14", "44477.90", "External Transfer", "mom's money passing through"),
    # Yaribel's 10,000 in and the 10,000 out the same day.
    (1731, "2026-05-29", "10000.00", "External Transfer", "passed through"),
    (1736, "2026-05-29", "-10000.00", "External Transfer", "passed through"),
    # The washing machine he pays Cashea for and she pays him back:
    # one answer for all eight legs (owner decision 2026-09-04).
    (1628, "2026-04-21", "-27818.11", "External Transfer", "cashea lavadora"),
    (1629, "2026-04-21", "27938.54", "External Transfer", "cuota cashea lavadora"),
    (1910, "2026-07-06", "-38522.14", "External Transfer", "cuota lavadora yaribel"),
    (7258, "2026-07-17", "-42300.64", "External Transfer", "cuota lavadora yaribel"),
    (7344, "2026-07-30", "-43060.54", "External Transfer", "cashea yaribel lavadora"),
    (7348, "2026-07-30", "43246.95", "External Transfer", "yaribel cashea lavadora"),
    (7382, "2026-08-06", "34015.50", "External Transfer", "deuda yaribel"),
    (7717, "2026-08-29", "45916.67", "External Transfer", "pago yaribel cashea"),
    # Money coming back from loans he granted.
    (1782, "2026-05-17", "70000.00", "Loan Repayment", "Natalia repaying"),
    (1715, "2026-05-30", "24480.00", "Loan Repayment", "Natalia repaying"),
    (1964, "2026-07-01", "25550.00", "Loan Repayment", "Natalia repaying"),
    (7188, "2026-07-12", "6000.00", "Loan Repayment", "Yaribel repaying"),
    (1875, "2026-06-09", "7096.04", "Loan Repayment", "pago moises de la playa"),
    # He bought something for the company and was reimbursed in USDC.
    (7659, "2026-08-21", "-70195.50", "Lending", "bought it for the company"),
    (7419, "2026-08-24", "90.00", "Loan Repayment", "the company reimbursing him"),
    # A 2,029.93 credit from a number that appears nowhere else.
    (1906, "2026-07-07", "2029.93", "Other Income", "unplaceable"),
)

# The reimbursement came back larger than the purchase. Naming it on the
# row is the whole record: the ledger has no way to express "the same
# movement, plus fifteen dollars".
NOTE_7419 = (
    "Reimbursement for the 70,195.50 Bs company purchase on 08-21 "
    "(row 7659, ~$75). The ~$15 over is the company's, not a second "
    "movement — owner decision 2026-09-04."
)

# ---------------------------------------------------------------------------
# Part 2 — the mispairing
# ---------------------------------------------------------------------------

WRONG_PAIR = "4f2dfd0a-ede0-46a8-a78a-a3a2ccf1b739"
SELL_ID = 1080  # P2P SELL 30.83 USDT @ 648.6 VES, 2026-05-11
WRONG_DEPOSIT_ID = 6935  # +20,018.42 VES from his mother, 2026-05-11
RIGHT_DEPOSIT_ID = 6940  # +20,000.00 VES, 2026-05-10, unclaimed

# What each leg was before that pairing promoted it — see the module
# docstring for why these two are knowable and 286 others are not.
PRE_IMAGES: tuple[tuple[int, str, int], ...] = (
    (WRONG_DEPOSIT_ID, "income", 1),  # provincial ingest writes income
    (SELL_ID, "expense", 0),  # ingest.binance writes a SELL as expense
)

# The sell's price is the third thing `unpair` puts back, and it is *not*
# NULL here. A cash conversion writes the rate it struck onto the row it
# pairs (ADR-015), so for those the pre-image rate is the one from before;
# a bank-anchored P2P pairing writes no rate at all — 648.6 is the order
# price ``ingest.binance`` recorded, and it was on the row before the
# pairing and must be on it after. Writing NULL instead wipes the price of
# a P2P sell, which is an input to the realized cost-basis tier (ADR-013):
# every bolívar row in that fortnight would then be priced off the market
# median instead of what those bolívars actually cost.
SELL_RATE = Decimal("648.6")


class Refused(RuntimeError):
    """A precondition does not hold. Nothing is written."""


def _decimal(raw: object) -> Decimal:
    return raw if isinstance(raw, Decimal) else Decimal(str(raw))


def _category_id(conn: sqlite3.Connection, name: str) -> int:
    row = conn.execute(
        "SELECT id FROM categories WHERE name = ? AND active = 1", (name,)
    ).fetchone()
    if row is None:
        raise Refused(f"no active category named {name!r} — is migration 025 applied?")
    return int(row["id"])


def _balances(conn: sqlite3.Connection) -> dict[tuple[int, str], Decimal]:
    """Every position, so the script can prove it moved none of them."""
    rows = conn.execute(
        "SELECT account_id, currency, SUM(CAST(amount AS REAL)) AS total "
        "FROM transactions GROUP BY account_id, currency"
    ).fetchall()
    return {
        (int(r["account_id"]), str(r["currency"])): _decimal(r["total"]) for r in rows
    }


# ---------------------------------------------------------------------------
# Preconditions
# ---------------------------------------------------------------------------


def check(conn: sqlite3.Connection) -> None:
    for txn_id, date, amount, category, _why in FILINGS:
        txn = tx_repo.get_by_id(conn, txn_id)
        if txn is None:
            raise Refused(f"row {txn_id} is not in the ledger")
        if txn.occurred_at.date().isoformat() != date:
            raise Refused(
                f"row {txn_id} is dated {txn.occurred_at.date()}, expected {date}"
            )
        if txn.amount != Decimal(amount):
            raise Refused(f"row {txn_id} is {txn.amount}, expected {amount}")
        if txn.transfer_id is not None:
            raise Refused(
                f"row {txn_id} is part of transfer {txn.transfer_id} — a paired "
                "row is neither income nor spending already, and tagging one "
                "leg would say something the pair does not"
            )
        _category_id(conn, category)

    sell = tx_repo.get_by_id(conn, SELL_ID)
    wrong = tx_repo.get_by_id(conn, WRONG_DEPOSIT_ID)
    right = tx_repo.get_by_id(conn, RIGHT_DEPOSIT_ID)
    if sell is None or wrong is None or right is None:
        raise Refused("one of the three rows in the mispairing is gone")
    if sell.transfer_id != WRONG_PAIR or wrong.transfer_id != WRONG_PAIR:
        raise Refused(
            f"rows {SELL_ID} and {WRONG_DEPOSIT_ID} are no longer the pair "
            f"{WRONG_PAIR} ({sell.transfer_id} / {wrong.transfer_id})"
        )
    if right.transfer_id is not None:
        raise Refused(
            f"row {RIGHT_DEPOSIT_ID} is already paired ({right.transfer_id}) — "
            "the deposit this repair means to claim is spoken for"
        )
    if right.amount != Decimal("20000.00"):
        raise Refused(f"row {RIGHT_DEPOSIT_ID} is {right.amount}, expected 20000.00")
    existing = conn.execute(
        "SELECT COUNT(*) AS c FROM transfer_pairings WHERE transfer_id = ?",
        (WRONG_PAIR,),
    ).fetchone()["c"]
    if existing:
        raise Refused(
            f"{WRONG_PAIR} already has a pre-image on file — it was paired "
            "after migration 024 and unpair() needs no help from this script"
        )


# ---------------------------------------------------------------------------
# The repair
# ---------------------------------------------------------------------------


def file_categories(conn: sqlite3.Connection) -> int:
    """Give each row its category. Returns how many actually changed."""
    changed = 0
    for txn_id, _date, _amount, category, why in FILINGS:
        target = _category_id(conn, category)
        before = tx_repo.get_by_id(conn, txn_id)
        assert before is not None  # check() ran
        if before.category_id == target:
            print(f"  {txn_id:>5}  already {category}")
            continue
        apply_edit(
            conn,
            txn_id=txn_id,
            req=TransactionEditRequest(set_category=True, category_id=target),
        )
        was = "—"
        if before.category_id is not None:
            row = conn.execute(
                "SELECT name FROM categories WHERE id = ?", (before.category_id,)
            ).fetchone()
            was = row["name"] if row else str(before.category_id)
        print(f"  {txn_id:>5}  {was} → {category}   ({why})")
        changed += 1

    apply_edit(
        conn,
        txn_id=7419,
        req=TransactionEditRequest(set_notes=True, notes=NOTE_7419),
    )
    print("   7419  note written: what the extra ~$15 is")
    return changed


def repair_pairing(conn: sqlite3.Connection) -> None:
    """Break the wrong pair, then pair the sell with the round deposit."""
    now = datetime.now(UTC).isoformat()
    for txn_id, prior_kind, prior_needs_review in PRE_IMAGES:
        leg = tx_repo.get_by_id(conn, txn_id)
        assert leg is not None  # check() ran
        rate = None if leg.user_rate is None else str(leg.user_rate)
        conn.execute(
            "INSERT INTO transfer_pairings (transfer_id, transaction_id, "
            "prior_kind, prior_needs_review, prior_user_rate, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (WRONG_PAIR, txn_id, prior_kind, prior_needs_review, rate, now),
        )
    print(f"  pre-images written for {SELL_ID} and {WRONG_DEPOSIT_ID}")

    legs = transfers.unpair(conn, transfer_id=WRONG_PAIR)
    print(f"  {WRONG_PAIR[:8]}… broken — {len(legs)} rows back on their own")

    result = confirm_pair(conn, deposit_id=RIGHT_DEPOSIT_ID, sell_id=SELL_ID)
    print(
        f"  {RIGHT_DEPOSIT_ID} ↔ {SELL_ID} paired as "
        f"{result['transfer_id'][:8]}…, realized basis rebuilt"
    )


def verify(conn: sqlite3.Connection) -> None:
    """Everything the repair claims, asserted."""
    for txn_id, _date, _amount, category, _why in FILINGS:
        txn = tx_repo.get_by_id(conn, txn_id)
        assert txn is not None
        name = conn.execute(
            "SELECT name FROM categories WHERE id = ?", (txn.category_id,)
        ).fetchone()
        if name is None or name["name"] != category:
            raise Refused(f"row {txn_id} did not end up in {category}")

    sell = tx_repo.get_by_id(conn, SELL_ID)
    freed = tx_repo.get_by_id(conn, WRONG_DEPOSIT_ID)
    claimed = tx_repo.get_by_id(conn, RIGHT_DEPOSIT_ID)
    assert sell is not None and freed is not None and claimed is not None

    if sell.transfer_id is None or sell.transfer_id != claimed.transfer_id:
        raise Refused(f"{SELL_ID} is not paired with {RIGHT_DEPOSIT_ID}")
    if sell.transfer_id == WRONG_PAIR:
        raise Refused("the sell is still in the pair this repair meant to break")
    if freed.transfer_id is not None:
        raise Refused(f"{WRONG_DEPOSIT_ID} is still paired")
    if freed.kind is not TransactionKind.INCOME:
        raise Refused(
            f"{WRONG_DEPOSIT_ID} came back as {freed.kind.value}, not the "
            "income a bank credit is"
        )
    if freed.needs_review != 1:
        raise Refused(
            f"{WRONG_DEPOSIT_ID} is uncategorised but not flagged — it would "
            "never reach Triage"
        )
    if sell.user_rate != SELL_RATE:
        raise Refused(
            f"the sell's price is {sell.user_rate}, expected {SELL_RATE} — "
            "the rate is an input to the realized cost basis (ADR-013), not "
            "a by-product of the pairing"
        )
    if not transfers.validate(conn, sell.transfer_id):
        raise Refused("the new pair is not a well-formed transfer (rule-002)")
    left = conn.execute(
        "SELECT COUNT(*) AS c FROM transfer_pairings WHERE transfer_id = ?",
        (WRONG_PAIR,),
    ).fetchone()["c"]
    if left:
        raise Refused("the broken pair's pre-images were not consumed")


def repair(conn: sqlite3.Connection) -> None:
    check(conn)
    before_balances = _balances(conn)
    before_findings = {f.check: f.count for f in integrity.run_checks(conn).findings}

    print("filings:")
    changed = file_categories(conn)
    print("\npairing:")
    repair_pairing(conn)

    verify(conn)

    after_balances = _balances(conn)
    if after_balances != before_balances:
        moved = {
            k: (before_balances.get(k), v)
            for k, v in after_balances.items()
            if before_balances.get(k) != v
        }
        raise Refused(f"a balance moved, which nothing here should do: {moved}")

    after_findings = {f.check: f.count for f in integrity.run_checks(conn).findings}
    worse = {
        name: (before_findings.get(name, 0), count)
        for name, count in after_findings.items()
        if count > before_findings.get(name, 0)
    }
    if worse:
        raise Refused(f"doctor got worse: {worse}")

    print(
        f"\nok: {changed} row(s) filed, the mispairing fixed, every balance "
        "unchanged, no new doctor findings"
    )
    for name, count in sorted(before_findings.items()):
        now = after_findings.get(name, 0)
        if now != count:
            print(f"  doctor {name}: {count} → {now}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", default="finances.db", help="Ledger path.")
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Write to --db. Without it, everything runs on a copy.",
    )
    args = parser.parse_args()

    source = Path(args.db)
    if not source.exists():
        print(f"no such ledger: {source}")
        return 2

    if args.apply:
        target = source
        print(f"APPLYING to {target}\n")
    else:
        tmp = Path(tempfile.mkdtemp(prefix="borrowed-money-")) / "ledger.db"
        src = sqlite3.connect(f"file:{source}?mode=ro", uri=True)
        dst = sqlite3.connect(tmp)
        src.backup(dst)
        dst.close()
        src.close()
        target = tmp
        print(f"DRY RUN on a copy at {target}\n")

    conn = get_connection(target)
    apply_migrations(conn)
    try:
        repair(conn)
    except Refused as exc:
        print(f"\nrefused: {exc}")
        return 1
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
