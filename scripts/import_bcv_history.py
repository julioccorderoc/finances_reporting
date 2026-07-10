#!/usr/bin/env python3
"""One-time BCV rate-gap import — docs/plans/revival/02-rate-gap.md.

The live BCV scraper (``finances/ingest/bcv.py``) only reads *today's* rate off
the homepage, so a gap in the ``rates`` table (daily BCV rows end 2026-04-17)
cannot be re-fetched. The owner saved ``tasas-bcv-july-9.html`` — a multi-day
BCV table spanning 2026-01-02 → 2026-07-10, Spanish dates and Venezuelan
decimals, one row per banking day. This script parses that file and inserts the
missing daily rows through the existing rates repo, so the imported rows are
indistinguishable from what the live scraper would have written and the rate
resolver (ADR-005 / rule-005) treats them identically.

Design notes
------------
* Reuses ``RawBcvRow``, ``parse_spanish_date`` and ``clean_currency`` from the
  production scraper so the parsed rows are shaped identically to live ones.
* Routes every write through ``rates_repo.insert`` (Pydantic in/out; rule-009).
  No raw SQL inserts.
* Idempotent: skips any (as_of_date, base, quote, source) already present and
  never overwrites an existing row. Re-running inserts 0.
* Imports both USD/VES and EUR/VES — the schema, the ``Rate`` model, and the
  live scraper already support the EUR pair cleanly (no schema change needed).

Usage
-----
    python scripts/import_bcv_history.py                 # dry-run (default)
    python scripts/import_bcv_history.py --apply         # write

Before ``--apply``, back up the DB::

    sqlite3 finances.db ".backup finances-backup-$(date +%Y%m%d).db"
"""
from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path
from typing import NamedTuple

from bs4 import BeautifulSoup

from finances.db.connection import get_connection
from finances.db.repos import rates as rates_repo
from finances.domain.models import Rate
from finances.ingest.bcv import (
    BcvParseError,
    RawBcvRow,
    SOURCE_NAME,
    clean_currency,
    parse_spanish_date,
)

QUOTE = "VES"
DEFAULT_HTML = "tasas-bcv-july-9.html"
DEFAULT_DB = "finances.db"


def _cell_currency(cell) -> str:
    """Extract the ``Bs.S …`` value from a rate ``<td>``.

    Each rate cell holds two spans: the value (``<span> Bs.S 709,69 </span>``)
    and a change marker (``<span> ▲ +1.35% </span>`` / ``▼ 0.00%``). We read the
    first span only, so the ``▲/▼`` percentage can never leak into the value.
    """
    span = cell.find("span")
    if span is None:
        raise BcvParseError("BCV history rate cell has no <span> value")
    return clean_currency(span.get_text(strip=True))


def parse_bcv_history(html: str) -> list[RawBcvRow]:
    """Parse the multi-day BCV table into one ``RawBcvRow`` per banking day.

    Each ``<tr>`` in the table body has three ``<td>``: the Spanish date, the
    USD rate, and the EUR rate. Rows whose first cell is not a parseable
    Spanish date (the ``<thead>`` row, stray markup) are skipped rather than
    raised on. Raises ``BcvParseError`` only when *no* usable row is found.
    """
    soup = BeautifulSoup(html, "html.parser")
    rows: list[RawBcvRow] = []
    for tr in soup.find_all("tr"):
        cells = tr.find_all("td")
        if len(cells) < 3:
            continue  # header row (<th>) or malformed
        as_of = parse_spanish_date(cells[0].get_text(strip=True))
        if as_of is None:
            continue
        rows.append(
            RawBcvRow(
                as_of_date=as_of,
                usd=_cell_currency(cells[1]),
                eur=_cell_currency(cells[2]),
            )
        )
    if not rows:
        raise BcvParseError("no BCV history rows parsed from the supplied HTML")
    return rows


class ImportResult(NamedTuple):
    planned: list[Rate]  # rows missing from the DB (would-insert set)
    inserted: int  # rows actually written (0 when apply=False)


def _plan_inserts(conn: sqlite3.Connection, rows: list[RawBcvRow]) -> list[Rate]:
    """Rows not already present, in (date, base) order. Never includes an
    existing (as_of_date, base, quote, source) — that guards against overwrite.
    """
    planned: list[Rate] = []
    for row in rows:
        for base, value in (("USD", row.usd), ("EUR", row.eur)):
            existing = rates_repo.get(
                conn, as_of_date=row.as_of_date, base=base, quote=QUOTE, source=SOURCE_NAME
            )
            if existing is not None:
                continue
            planned.append(
                Rate(
                    as_of_date=row.as_of_date,
                    base=base,
                    quote=QUOTE,
                    rate=value,
                    source=SOURCE_NAME,
                )
            )
    return planned


def import_history(
    conn: sqlite3.Connection, rows: list[RawBcvRow], *, apply: bool
) -> ImportResult:
    """Insert the missing daily rates. When ``apply`` is False, plan only.

    The write runs in a single transaction; a duplicate slipping through the
    pre-check (``rates_repo.get``) is caught via ``IntegrityError`` against the
    ``UNIQUE (as_of_date, base, quote, source)`` constraint and skipped, so the
    operation stays idempotent under any race.
    """
    planned = _plan_inserts(conn, rows)
    if not apply:
        return ImportResult(planned=planned, inserted=0)

    conn.execute("BEGIN")
    try:
        inserted = 0
        for rate in planned:
            try:
                rates_repo.insert(conn, rate)
                inserted += 1
            except sqlite3.IntegrityError:
                continue
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    return ImportResult(planned=planned, inserted=inserted)


def _format_rate(rate: Rate) -> str:
    return f"{rate.as_of_date.isoformat()}  {rate.base}/{rate.quote}  {rate.rate}"


def _report(result: ImportResult, *, apply: bool) -> None:
    planned = result.planned
    verb = "Inserted" if apply else "Would insert"
    print(f"BCV rate-gap import ({'APPLY' if apply else 'DRY-RUN'})")
    print(f"  {verb}: {len(planned)} row(s)  "
          f"({len({p.as_of_date for p in planned})} day(s), USD+EUR)")
    if planned:
        print(f"  first : {_format_rate(planned[0])}")
        print(f"  last  : {_format_rate(planned[-1])}")
        mid = planned[len(planned) // 2]
        print(f"  sample: {_format_rate(mid)}")
    if apply:
        print(f"  written: {result.inserted} row(s)")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--html", default=DEFAULT_HTML, help=f"BCV history HTML (default: {DEFAULT_HTML})"
    )
    parser.add_argument(
        "--db", default=DEFAULT_DB, help=f"SQLite DB path (default: {DEFAULT_DB})"
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="write to the DB (default is a dry-run that writes nothing)",
    )
    args = parser.parse_args(argv)

    html_path = Path(args.html)
    if not html_path.exists():
        print(f"error: HTML file not found: {html_path}", file=sys.stderr)
        return 2
    html = html_path.read_text(encoding="utf-8")

    try:
        rows = parse_bcv_history(html)
    except BcvParseError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    conn = get_connection(args.db)
    try:
        result = import_history(conn, rows, apply=args.apply)
    finally:
        conn.close()

    _report(result, apply=args.apply)
    if not args.apply:
        print("\n(dry-run) re-run with --apply to write. Back up first:")
        print('  sqlite3 finances.db ".backup finances-backup-$(date +%Y%m%d).db"')
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
