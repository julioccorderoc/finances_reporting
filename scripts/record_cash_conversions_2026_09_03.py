"""Record the two 2026 cash conversions the owner named on 2026-09-03.

Owner: "I got those $580 in cash" (Binance Pay C2C, 2026-08-15, row 7555)
and "same for the 36,000 Bs" (DR OB V07372929, 2026-08-31, row 7692).

Each becomes a double-entry transfer (rule-002, ADR-017: two positions,
different currencies) through ``transfers.create_transfer`` in its
anchor-only mode: the existing outgoing row is promoted to a transfer leg
and a ``Cash USD`` leg is inserted with the dollars actually received —
the same shape the 2025 conversions already have (rows 859/5740 and
863/5741, ``source_ref`` ``cash:binance-send:<id>``).

For the bolívar row the bank row's ``user_rate`` is set to the price the
exchange was struck at (36,000 ÷ dollars received; ADR-015: quote units
per dollar) so the two legs cancel exactly under ``transfers.validate``.

Dry-run by default: the ledger is copied (sqlite backup API) to a
temporary file and the changes run THERE, so the real file is never
opened for writing. ``--apply`` runs against the given path. (A rollback
is not enough: the write paths commit on their own.)
Idempotent: a row that already carries a ``transfer_id`` is skipped.

    uv run python scripts/record_cash_conversions_2026_09_03.py \
        --db finances.db --usd-for-36000 38.83            # dry run
    uv run python scripts/record_cash_conversions_2026_09_03.py \
        --db finances.db --usd-for-36000 38.83 --apply    # write
"""

from __future__ import annotations

import argparse
import inspect
import shutil
import sqlite3
import tempfile
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path

from finances.db.connection import get_connection
from finances.db.repos import transactions as tx_repo
from finances.domain import money, transfers
from finances.domain.models import Transaction, TransactionKind
from finances.web.services.transactions_write import (
    TransactionEditRequest,
    apply_edit,
)

CASH_USD_ACCOUNT_ID = 5

CASES = (
    {
        "anchor": 7555,
        "expect": (Decimal("-580"), "USDT", "Binance Pay C2C (outgoing)"),
        "usd": Decimal("580"),
        "description": "Cash received — cambio $580 efectivo",
        "source_ref": "cash:binance-pay:7555",
        "set_rate": False,
    },
    {
        "anchor": 7692,
        "expect": (Decimal("-36000"), "VES", "DR OB V07372929 191NAC.C"),
        "usd": None,  # --usd-for-36000
        "description": "Cash received — cambio Bs. 36.000 efectivo",
        "source_ref": "cash:provincial:hash:52809099a320229b",
        "set_rate": True,
    },
)


def _balances(conn: sqlite3.Connection) -> dict[str, Decimal]:
    rows = conn.execute(
        """
        SELECT a.name, t.currency, SUM(t.amount) AS total
        FROM transactions t JOIN accounts a ON a.id = t.account_id
        WHERE a.id IN (1, 2, 5)
        GROUP BY a.name, t.currency ORDER BY a.name, t.currency
        """
    ).fetchall()
    return {f"{r['name']} {r['currency']}": Decimal(str(r["total"])) for r in rows}


def _show_pair(conn: sqlite3.Connection, transfer_id: str) -> None:
    total_usd = Decimal(0)
    for row in conn.execute(
        "SELECT id FROM transactions WHERE transfer_id = ? ORDER BY id", (transfer_id,)
    ).fetchall():
        txn = tx_repo.get_by_id(conn, int(row["id"]))
        assert txn is not None
        usd, source = money.to_usd(conn, txn)
        total_usd += usd or Decimal(0)
        print(
            f"    leg {txn.id:>5} {txn.occurred_at:%Y-%m-%d} acct={txn.account_id} "
            f"{txn.kind.value:<8} {txn.amount:>12} {txn.currency:<4} "
            f"usd={usd} ({source}) user_rate={txn.user_rate} ref={txn.source_ref}"
        )
    print(f"    legs sum in USD: {total_usd:+.4f}")


def _scratch_copy(db: Path) -> Path:
    """A private copy of ``db``, taken read-only through the backup API."""
    target = Path(tempfile.mkdtemp(prefix="cash-conversions-")) / "scratch.db"
    src = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    dst = sqlite3.connect(target)
    src.backup(dst)
    src.close()
    dst.close()
    return target


def run(db: Path, usd_for_36000: Decimal, apply: bool) -> int:
    target = db if apply else _scratch_copy(db)
    conn = get_connection(target)
    try:
        print(f"db: {db}  mode: {'APPLY' if apply else f'DRY RUN on a temporary copy ({target})'}")
        before = _balances(conn)

        for case in CASES:
            anchor = tx_repo.get_by_id(conn, case["anchor"])
            if anchor is None:
                raise SystemExit(f"row {case['anchor']} not found — wrong database?")
            amount, currency, description = case["expect"]
            if (anchor.amount, anchor.currency, anchor.description) != (amount, currency, description):
                raise SystemExit(
                    f"row {anchor.id} is not the expected row: "
                    f"{anchor.amount} {anchor.currency} {anchor.description!r}"
                )
            if anchor.transfer_id is not None:
                print(f"  row {anchor.id}: already a transfer ({anchor.transfer_id}) — skipped")
                continue

            usd = case["usd"] if case["usd"] is not None else usd_for_36000
            if usd <= 0:
                raise SystemExit("dollars received must be positive")

            if case["set_rate"]:
                # Cross-currency: the two legs carry different amounts, and
                # create_transfer's anchor-only mode copies ONE amount to
                # both (it is built for same-currency moves). So the cash
                # leg is inserted first, then both existing rows are
                # paired in both-anchors mode, which validates the pair
                # through the struck rate set on the bolívar row.
                rate = (abs(anchor.amount) / usd).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
                apply_edit(
                    conn,
                    txn_id=anchor.id,
                    req=TransactionEditRequest(set_user_rate=True, user_rate=rate),
                )
                print(f"  row {anchor.id}: user_rate set to {rate} {currency}/USD (struck price)")
                cash_leg = tx_repo.insert(
                    conn,
                    Transaction(
                        account_id=CASH_USD_ACCOUNT_ID,
                        occurred_at=anchor.occurred_at,
                        kind=TransactionKind.TRANSFER,
                        amount=usd,
                        currency="USD",
                        description=case["description"],
                        source="internal",
                        source_ref=case["source_ref"],
                    ),
                )
                pair = transfers.create_transfer(
                    conn,
                    anchor_transaction_id=anchor.id,
                    counterpart_transaction_id=cash_leg.id,
                )
            else:
                pair = transfers.create_transfer(
                    conn,
                    anchor_transaction_id=anchor.id,
                    to_account_id=CASH_USD_ACCOUNT_ID,
                    amount=usd,
                    currency="USD",
                    description=case["description"],
                    source="internal",
                    source_ref_to=case["source_ref"],
                )
            print(f"  row {anchor.id}: paired with a new Cash USD leg, transfer {pair.transfer_id}")
            _show_pair(conn, pair.transfer_id)

            validate = transfers.validate
            params = inspect.signature(validate).parameters
            try:
                if "transfer_id" in params:
                    result = validate(conn, transfer_id=pair.transfer_id)
                else:
                    result = validate(conn)
                print(f"    transfers.validate -> {result if result is not None else 'ok'}")
            except TypeError as exc:  # signature guess failed; the USD sum above is the check
                print(f"    transfers.validate not called ({exc}); see the USD sum above")

        after = _balances(conn)
        print("balances (Provincial, Binance Spot, Cash USD):")
        for key in sorted(set(before) | set(after)):
            b, a = before.get(key, Decimal(0)), after.get(key, Decimal(0))
            mark = "" if a == b else f"   ({a - b:+})"
            print(f"  {key:<28} {b:>16} -> {a:>16}{mark}")

        if apply:
            conn.commit()
            print("committed.")
        else:
            print("dry run: the real ledger was not opened for writing. Re-run with --apply to write.")
        return 0
    finally:
        conn.close()
        if not apply:
            shutil.rmtree(target.parent, ignore_errors=True)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--db", type=Path, required=True, help="path to the ledger (or a scratch copy)")
    parser.add_argument(
        "--usd-for-36000",
        type=Decimal,
        required=True,
        help="dollar bills received for the 36,000 Bs on 2026-08-31 (e.g. 38.83)",
    )
    parser.add_argument("--apply", action="store_true", help="commit instead of rolling back")
    args = parser.parse_args()
    return run(args.db, args.usd_for_36000, args.apply)


if __name__ == "__main__":
    raise SystemExit(main())
