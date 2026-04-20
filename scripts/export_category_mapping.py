"""Side-by-side legacy vs v1 category audit for the full ledger.

Writes review/category_mapping.csv with one row per transaction showing
its legacy Sub-Category / Category (from the CSV) alongside the v1
category that ended up in the DB. Intended for eyeball review of every
mapping decision — no silent differences.
"""
from __future__ import annotations

import csv
import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from finances.migration.backfill import iter_legacy_annotations


def main() -> None:
    db_path = ROOT / "finances.db"
    data_dir = ROOT / "data"
    out_path = ROOT / "review" / "category_mapping.csv"
    out_path.parent.mkdir(exist_ok=True)

    annotations: dict[tuple[str, str], tuple[str, str]] = {}
    for source, source_ref, sub_cat, category in iter_legacy_annotations(data_dir):
        annotations[(source, source_ref)] = (sub_cat, category)

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        """
        SELECT t.id, date(t.occurred_at) AS date, a.name AS account,
               t.kind, t.amount, t.currency, t.description,
               t.source, t.source_ref,
               c.name AS v1_category,
               t.needs_review
        FROM transactions t
        LEFT JOIN accounts a ON a.id = t.account_id
        LEFT JOIN categories c ON c.id = t.category_id
        ORDER BY t.occurred_at, t.id
        """
    ).fetchall()

    headers = [
        "id", "date", "account", "kind", "amount", "currency",
        "description", "legacy_sub_category", "legacy_category",
        "v1_category", "mapping_source", "needs_review",
    ]
    total = 0
    no_annotation = 0
    mismatch = 0

    with out_path.open("w", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(headers)
        for r in rows:
            total += 1
            ann = annotations.get((r["source"], r["source_ref"]))
            legacy_sub, legacy_cat = (ann if ann is not None else ("", ""))
            v1 = r["v1_category"] or ""

            if ann is None:
                source = "no_annotation"
                no_annotation += 1
            elif legacy_sub == "":
                source = "legacy_empty"
            else:
                # Heuristic: if the legacy_sub maps to exactly the v1 name
                # via the closed table, this was the legacy path. Otherwise
                # either the rules engine or reconciliation ran.
                from finances.migration.backfill import LEGACY_SUB_CATEGORY_TO_V1
                expected = LEGACY_SUB_CATEGORY_TO_V1.get(legacy_sub)
                if expected is None:
                    source = "legacy_unmapped"
                elif expected == v1:
                    source = "legacy_mapping"
                else:
                    source = "rules_or_recon"
                    mismatch += 1

            writer.writerow([
                r["id"], r["date"], r["account"], r["kind"],
                r["amount"], r["currency"], r["description"],
                legacy_sub, legacy_cat, v1, source, r["needs_review"],
            ])

    print(f"wrote {total} rows to {out_path}")
    print(f"  rows without legacy annotation: {no_annotation}")
    print(f"  rows where v1 differs from legacy-expected mapping: {mismatch}")


if __name__ == "__main__":
    main()
