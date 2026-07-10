# Thing 3 — Provincial bank catch-up

## Context (assume no prior knowledge)

Personal finances ledger, SQLite (`finances.db`), Typer CLI (`finances`).
Bank transactions (Banco Provincial) are ingested from CSV files the owner
downloads manually from the bank website into `inputs/` (gitignored).
Last ingested bank transaction: **2026-04-19**. Binance side is already
caught up through 2026-07-09, so the bank-anchored P2P transfer pairing
(`finances/domain/transfers.py`) has Binance rows waiting to pair.

## Prerequisite (owner does this, not you)

Julio downloads statement CSV(s) covering 2026-04-19 → today into `inputs/`.
If `inputs/` has no new files, STOP and tell him exactly that.

## Task

Ingest every new CSV, verify pairing and dedup, then hand off to triage.

## Rules

1. **Read first**: `finances/ingest/provincial.py` (expected CSV columns +
   date format validator) and `finances/cli/main.py` ingest section.
2. Backup: `sqlite3 finances.db ".backup finances-backup-$(date +%Y%m%d).db"`.
3. Per file: `uv run finances ingest provincial <file> --dry-run` first
   (check the exact CLI signature — read the code). Sane numbers → real run.
4. If the bank changed its CSV format (dry-run validation errors): extend the
   existing parser/validators with a failing test first, minimal change, no
   parallel parser. If the change is big, stop and report instead.
5. Overlap with already-ingested rows is safe (dedup on
   `UNIQUE(source, source_ref)`); expect `inserted=0` for overlap portions.
6. New rows land with `needs_review=1` (by design — rule: unmatched rows are
   never auto-categorized). Do NOT auto-categorize them; do NOT guess
   categories (standing owner rule: ask, never guess legacy meanings).
7. After ingest, report: rows inserted per file, transfer pairs created,
   needs_review count. Then tell Julio to run his triage session
   (`uv run finances serve` → /triage) to categorize.

## Gate

- [ ] Every CSV in `inputs/` ingested with 0 errors.
- [ ] Re-running each ingest inserts 0 (idempotency).
- [ ] P2P SELL Binance rows in the window paired where a matching bank leg
      exists (report paired/unpaired counts).
- [ ] `uv run pytest -q` green.
- [ ] Short summary for Julio: what came in, what needs his triage.

## Out of scope

Categorizing on his behalf, touching Binance ingest, report generation, docs.
