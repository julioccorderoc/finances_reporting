"""Delete the ten Binance Pay twins and restate the Spot opening (2026-09-03).

Binance serves one send from two endpoints under two ids: the backfill
read the *withdraw* history (``withdraw:hash:<sha>``) and the live sync
read *Pay* history (``pay:<orderId>``). Dedup is keyed on
``(source, source_ref)`` (rule-010), so it cannot see they are one
movement — ten rows, 2,260.72 USDT, counted twice. The legacy row carries
the real meaning (a category, a cash pairing); the Pay row carries
nothing but the duplicate. The table is in
``docs/plans/2026-09-03-ledger-actions-decisions.md`` §2, and the owner
said "delete duplicated stuff" the same day.

Two writes, in this order:

1. **Delete the ten Pay rows** through ``transactions_repo.delete``
   (ADR-022), which retires each ``(source, source_ref)`` in
   ``deleted_transactions``. Without that tombstone the next deep
   ``finances ingest binance --since`` would insert all ten again.
2. **Restate the Binance Spot USDT opening position** through
   ``opening_positions.record_opening`` (ADR-020 §2: an opening row has a
   stable ``source_ref``, so restating replaces the prior statement
   instead of layering a correction on it). The custodian figure is the
   position as the ledger reports it *before* this repair — the ledger
   agreed with Binance when the opening was computed on 2026-08-08, and
   nothing here changes what Binance holds. The arithmetic therefore
   moves the opening down by exactly the 2,260.72 the twins added, and
   the Spot balance does not move at all.

Deleting alone would silently *raise* the Spot position by 2,260.72;
restating alone would silently lower it. Neither half is correct on its
own, which is why this is one script with one assertion at the end.

Every precondition is checked before anything is written: each id must
still be an unpaired ``pay:`` row of the expected amount on Binance Spot,
and each must still have its legacy twin on the books. If one differs,
nothing runs.

Dry-run by default: the ledger is copied (sqlite backup API) to a
temporary file and the changes run THERE, so the real file is never
opened for writing — a rollback would not be enough, since the write
paths commit on their own.

    uv run python scripts/dedupe_binance_pay_twins_2026_09_03.py --db finances.db
    uv run python scripts/dedupe_binance_pay_twins_2026_09_03.py --db finances.db --apply
"""

from __future__ import annotations

import argparse
import sqlite3
import tempfile
from decimal import Decimal
from pathlib import Path

from finances.db.connection import get_connection
from finances.db.migrate import apply_migrations
from finances.db.repos import accounts as accounts_repo
from finances.db.repos import transactions as tx_repo
from finances.domain.opening_positions import record_opening
from finances.domain.reconciliation_adjustments import position_balance

SPOT_ACCOUNT_NAME = "Binance Spot"
CURRENCY = "USDT"

# (pay row id, legacy twin id, amount, what the legacy row says).
# Straight from the decisions document's table, so the two can be diffed.
TWINS: tuple[tuple[int, int, str, str], ...] = (
    (5775, 859, "-700", "Cambio $700 efectivo Jorge — paired with Cash"),
    (5774, 863, "-500", "Cambio $500 efectivo Terán — paired with Cash"),
    (5773, 893, "-193.22", "Compras en TeLoComproEnUSA (Purchases)"),
    (5799, 912, "-642", "Pago parcial de iPhone (Purchases)"),
    (5798, 943, "-25", "Counterparty 227985716 (Lending)"),
    (5866, 963, "-4", "Suscripción Netflix Enero (Subscriptions)"),
    (6137, 1031, "-142.50", "Counterparty 205781774 (Lending)"),
    (6136, 1035, "-30", "Counterparty 205781774 (Lending)"),
    (6135, 1036, "-4", "Counterparty 205781774 (Lending)"),
    (1120, 1076, "-20", "Counterparty 386971640 (External Transfer)"),
)

EXPECTED_TOTAL = Decimal("-2260.72")

REASON = (
    "Binance Pay twin of legacy withdraw row {legacy} — one send served "
    "twice by two endpoints under two ids (see ERRORS.md 2026-09-03)"
)


class Refused(RuntimeError):
    """A precondition does not hold. Nothing is written."""


def _decimal(raw: object) -> Decimal:
    return raw if isinstance(raw, Decimal) else Decimal(str(raw))


def check(conn: sqlite3.Connection, spot_id: int) -> None:
    """Every twin still looks exactly as the decisions document found it."""
    total = Decimal("0")
    for pay_id, legacy_id, amount, note in TWINS:
        pay = tx_repo.get_by_id(conn, pay_id)
        if pay is None:
            raise Refused(f"row {pay_id} is not in the ledger")
        if pay.source_ref is None or not pay.source_ref.startswith("pay:"):
            raise Refused(f"row {pay_id} is not a pay: row ({pay.source_ref})")
        if pay.account_id != spot_id or pay.currency != CURRENCY:
            raise Refused(
                f"row {pay_id} is not {CURRENCY} on {SPOT_ACCOUNT_NAME}"
            )
        if pay.amount != Decimal(amount):
            raise Refused(
                f"row {pay_id} is {pay.amount}, the table says {amount}"
            )
        if pay.transfer_id is not None:
            raise Refused(
                f"row {pay_id} has been paired since — a leg of a transfer is "
                "not a duplicate; re-read it before deleting"
            )

        legacy = tx_repo.get_by_id(conn, legacy_id)
        if legacy is None:
            raise Refused(
                f"the legacy twin {legacy_id} of row {pay_id} is gone — "
                "deleting the Pay row would delete the event itself"
            )
        if legacy.amount != pay.amount:
            raise Refused(
                f"twin mismatch: {legacy_id} is {legacy.amount}, "
                f"{pay_id} is {pay.amount}"
            )
        if legacy.source_ref is None or ":hash:" not in legacy.source_ref:
            raise Refused(
                f"row {legacy_id} is not a legacy backfill row "
                f"({legacy.source_ref}) — the twin reading rests on that"
            )
        total += pay.amount
        print(f"  {pay_id} ← {legacy_id}  {pay.amount:>10} {CURRENCY}  {note}")

    if total != EXPECTED_TOTAL:
        raise Refused(f"the ten rows sum to {total}, expected {EXPECTED_TOTAL}")
    print(f"  ten rows, {total} {CURRENCY} counted twice — as documented")


def repair(conn: sqlite3.Connection) -> None:
    spot = accounts_repo.get_by_name(conn, SPOT_ACCOUNT_NAME)
    if spot is None or spot.id is None:
        raise Refused(f"no account named {SPOT_ACCOUNT_NAME!r}")

    print("preconditions:")
    check(conn, spot.id)

    balance_before = position_balance(
        conn, account_id=spot.id, currency=CURRENCY
    )
    opening_before = conn.execute(
        "SELECT amount FROM transactions WHERE source = 'opening_balance'"
        " AND source_ref = ?",
        (f"opening:{spot.id}:{CURRENCY}",),
    ).fetchone()
    if opening_before is None:
        raise Refused(
            f"no opening position for {SPOT_ACCOUNT_NAME} {CURRENCY} — "
            "ADR-020 restatement has nothing to restate"
        )
    opening_before_amount = _decimal(opening_before["amount"])
    print(
        f"\nbefore: balance {balance_before} {CURRENCY}, "
        f"opening {opening_before_amount} {CURRENCY}"
    )

    print("\ndeleting the twins:")
    for pay_id, legacy_id, _amount, _note in TWINS:
        tomb = tx_repo.delete(
            conn, pay_id, reason=REASON.format(legacy=legacy_id)
        )
        print(f"  {pay_id} deleted, {tomb.source}/{tomb.source_ref} retired")

    # The custodian figure: what the ledger reported for this position
    # before the repair. The opening row was sized on 2026-08-08 to make
    # the ledger agree with Binance, and nothing here changes what Binance
    # holds — so restating against this figure moves the opening down by
    # exactly what the twins added and leaves the balance alone.
    result = record_opening(
        conn,
        account_id=spot.id,
        currency=CURRENCY,
        actual=balance_before,
    )
    if result is None:
        raise Refused(
            "the opening restatement wrote nothing — the position already "
            "matched, which means the deletes did not take effect"
        )
    print(
        f"\nopening restated: {opening_before_amount} → {result.delta} "
        f"{CURRENCY} (shape {result.shape.value})"
    )

    balance_after = position_balance(conn, account_id=spot.id, currency=CURRENCY)
    print(f"after:  balance {balance_after} {CURRENCY}")

    # The two assertions this repair is only correct if both hold.
    if balance_after != balance_before:
        raise Refused(
            f"the Spot {CURRENCY} balance moved: {balance_before} → "
            f"{balance_after}. The repair is wrong; nothing about what "
            "Binance holds has changed."
        )
    expected_opening = opening_before_amount + EXPECTED_TOTAL
    if result.delta != expected_opening:
        raise Refused(
            f"the opening is {result.delta}, expected {expected_opening} "
            f"({opening_before_amount} − {-EXPECTED_TOTAL})"
        )

    tombstones = conn.execute(
        "SELECT COUNT(*) AS c FROM deleted_transactions"
    ).fetchone()["c"]
    print(
        f"\nok: balance unchanged, opening down by {-EXPECTED_TOTAL}, "
        f"{tombstones} tombstone(s) on file"
    )


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
        tmp = Path(tempfile.mkdtemp(prefix="dedupe-twins-")) / "ledger.db"
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
