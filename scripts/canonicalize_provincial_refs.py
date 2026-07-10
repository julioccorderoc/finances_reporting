"""One-time: recompute provincial source_refs with the canonical 2-decimal
amount scale (see compute_source_ref) so every container the bank exports
dedups against the same hash.

Why: rows backfilled from legacy sheets with 1-decimal amounts ('-14,4')
were hashed as '-14.4' while the canonical form is '-14.40'; re-ingesting
the same transactions from bank exports would insert duplicates.

Usage:
    uv run python scripts/canonicalize_provincial_refs.py           # dry-run
    uv run python scripts/canonicalize_provincial_refs.py --apply   # write
"""
from __future__ import annotations

import sqlite3
import sys
from collections import defaultdict
from datetime import datetime
from decimal import Decimal
from pathlib import Path

from finances.config import DB_PATH
from finances.ingest.provincial import compute_source_ref


def canonicalize(conn: sqlite3.Connection, *, apply: bool) -> tuple[int, int]:
    rows = conn.execute(
        """
        SELECT id, occurred_at, amount, description, source_ref
        FROM transactions WHERE source = 'provincial'
        ORDER BY id
        """
    ).fetchall()

    # Occurrence must mirror ingest: Nth twin of the same
    # (date, canonical amount, description) tuple, in insertion (id) order.
    occurrence: dict[tuple[str, str, str], int] = defaultdict(int)
    changes: list[tuple[int, str, str]] = []
    for row_id, occurred_at, amount, description, old_ref in rows:
        dt = datetime.fromisoformat(occurred_at)
        amt = Decimal(str(amount))
        key = (dt.date().isoformat(), format(amt.quantize(Decimal("0.01")), "f"), description)
        occ = occurrence[key]
        occurrence[key] += 1
        new_ref = compute_source_ref(
            occurred_at=dt, amount=amt, description=description, occurrence=occ
        )
        if new_ref != old_ref:
            changes.append((row_id, old_ref, new_ref))

    print(f"provincial rows: {len(rows)}, refs to rewrite: {len(changes)}")
    for row_id, old, new in changes[:5]:
        print(f"  id={row_id}  {old} -> {new}")
    if len(changes) > 5:
        print(f"  ... and {len(changes) - 5} more")

    if not apply:
        print("(dry-run) re-run with --apply to write. Back up first:")
        print('  sqlite3 finances.db ".backup finances-backup-$(date +%Y%m%d).db"')
        return len(rows), len(changes)

    new_refs = {new for _, _, new in changes}
    if len(new_refs) != len(changes):
        raise SystemExit("ABORT: recomputed refs collide with each other")
    clash = conn.execute(
        f"""
        SELECT count(*) FROM transactions
        WHERE source = 'provincial'
          AND source_ref IN ({",".join("?" * len(new_refs))})
          AND id NOT IN ({",".join("?" * len(changes))})
        """,
        [*new_refs, *[c[0] for c in changes]],
    ).fetchone()[0] if changes else 0
    if clash:
        raise SystemExit(f"ABORT: {clash} recomputed ref(s) already used by other rows")

    conn.execute("BEGIN")
    try:
        # Two-phase so a row's new ref never transiently collides with a
        # not-yet-rewritten row's old ref under UNIQUE(source, source_ref).
        for row_id, _, _ in changes:
            conn.execute(
                "UPDATE transactions SET source_ref = 'tmp:' || id WHERE id = ?",
                (row_id,),
            )
        for row_id, _, new in changes:
            conn.execute(
                "UPDATE transactions SET source_ref = ? WHERE id = ?", (new, row_id)
            )
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    print(f"rewrote {len(changes)} source_ref(s)")
    return len(rows), len(changes)


if __name__ == "__main__":
    apply = "--apply" in sys.argv
    db = Path(DB_PATH)
    if not db.exists():
        raise SystemExit(f"no database at {db}")
    conn = sqlite3.connect(db)
    conn.execute("PRAGMA foreign_keys = ON")
    try:
        canonicalize(conn, apply=apply)
    finally:
        conn.close()
