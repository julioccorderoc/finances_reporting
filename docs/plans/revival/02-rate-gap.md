# Thing 2 — BCV rate-gap fill (one-time import)

## Context (assume no prior knowledge)

Personal finances ledger, SQLite (`finances.db`), Python 3.13 + uv, Typer CLI.
The `rates` table has daily BCV exchange rates ending 2026-04-17 — then a gap.
Live scraper only captures "today", so the gap can't be re-fetched. The owner
saved `tasas-bcv-july-9.html` (repo root): daily BCV rates for USD and EUR,
2026-01-02 → 2026-07-10, Spanish dates ("Viernes, 10 de julio de 2026"),
Venezuelan decimal format ("Bs.S 709,69" = 709.69), one block per day.

## Task

Write a one-time import script `scripts/import_bcv_history.py` that parses
that file and inserts the missing daily rates through the existing rates repo.

## Rules

1. **Read first**: `finances/ingest/bcv.py` (what a live BCV rate row looks
   like: source, base/quote, fields), `finances/db/repos/rates.py`,
   `finances/domain/models.py` (Rate model). The imported rows must be
   indistinguishable from rows the live scraper would have written, so the
   rate resolver treats them identically.
2. Route through the rates repo (Pydantic in/out). No raw SQL inserts.
3. Idempotent: re-running inserts 0. Respect whatever uniqueness the repo/
   schema has; skip dates that already exist — never overwrite existing rows.
4. TDD: parser gets a pytest file first (fixture = a saved excerpt of the real
   HTML, 3-4 day blocks incl. an accented weekday and a `▼` day). Then implement.
5. Script supports `--dry-run` (default!) printing count + first/last/sample
   rows; `--apply` to write.
6. Before `--apply`: `sqlite3 finances.db ".backup finances-backup-$(date +%Y%m%d).db"`.
7. EUR rows: import them too only if the schema/model already supports the
   pair cleanly; otherwise USD only and say so in the report. Do not extend
   the schema for this.

## Gate (all must pass)

- [ ] Dry-run count ≈ Venezuelan banking days in the file range (~125 for
      Jan-Jul; the DB-missing subset Apr 18 → Jul 10 is ~55).
- [ ] Spot-check 3 dates against the HTML by eye: 2026-07-09 → 700.22 USD,
      2026-07-10 → 709.69 USD, 2026-01-02 → 301.37 USD.
- [ ] Second `--apply` run inserts 0.
- [ ] Pre-existing rows (≤ 2026-04-17) byte-identical (compare counts + a
      sampled row before/after).
- [ ] `uv run pytest -q` fully green.

## Out of scope

Historical P2P rates (unobtainable), scraper changes, any transactions-table
work, doc updates.
