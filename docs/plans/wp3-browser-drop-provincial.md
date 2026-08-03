# WP3 — Drag-and-drop Provincial statement upload in the viewer

Paste this whole file as the opening prompt of a fresh session.

---

Work in an isolated git worktree. Create it **before your first commit** —
several sessions are active in this repo and a shared working tree has
already been clobbered once (a concurrent `git reset` discarded staged work).
Do not work directly in `/Users/juliocordero/Documents/finances_reporting`.

Read `CLAUDE.md` first. Follow rule-011 (TDD: test commit precedes impl
commit), rule-009 (Pydantic at trust boundaries), rule-010 (deterministic
`source_ref`), and rule-004 (no forked ingest logic).

## Why this exists

The owner downloads a Provincial statement and today must move it into
`inputs/` by hand and run `finances update` in a terminal. They want to drop
it on the web viewer instead, with no renaming or naming convention.

## What to build

A dropzone on the transactions page that ingests a Provincial statement.

**Decisions already locked by the owner — do not relitigate:**

- **Preview, then confirm.** Drop → dry-run parse → summary → owner presses
  Import. Not immediate-import, and not auto-import-when-clean.
- The summary shows: filename, row count, date range, how many rows are new
  vs already known, and any parse errors.
- Filename is irrelevant. Only the suffix matters.

**Shape:**

1. `POST /_partial/uploads/provincial/preview`
   - Accepts `UploadFile`. Suffix allowlist `.csv` / `.xls` only. Enforce a
     size cap.
   - Stage the bytes in a **temp dir, not `inputs/`** — a previewed-but-
     unconfirmed file left in `inputs/` would be swept up by the next
     `finances update`.
   - Call `finances.ingest.provincial.ingest_csv(conn, path, dry_run=True)`.
     It already supports `dry_run`. Return a summary partial.
2. `POST /_partial/uploads/provincial/import`
   - Takes the staged token, runs the real ingest, archives to
     `inputs/processed/` (reuse `reports/update._archive_processed`, which is
     already collision-safe), and returns a success toast via the existing
     `_hx_trigger_json` / `show-toast` contract in
     `finances/web/routers/partials.py`.
3. Dropzone partial on the transactions page.

**Already handled — do not rebuild:**

- The bank's "`.xls`" is really an HTML table.
  `finances/ingest/provincial.py:297` already sniffs this and routes to
  `_iter_html_rows`. Verified working: `~/Downloads/provincial-july-2026.xls`
  parses to 99 rows, 14 Jul – 31 Jul.
- Re-dropping the same statement is harmless —
  `UNIQUE(source, source_ref)` absorbs it. Preview exists to catch the *wrong
  file*, not duplicates.
- P2P pairing runs automatically inside `ingest_csv` via
  `BankAnchoredP2pPairing`. Do not invoke it separately.

**Explicitly out of scope:** rate refresh (WP2), any change to rate
resolution (WP1), Binance or BCV ingest.

## Testing

- Round trip: preview reports N rows and writes nothing to the DB; import
  then writes exactly those rows.
- Re-import of the same file inserts 0 new rows.
- A `.txt` or oversized upload is rejected with a clear message, not a 500.
- A file that fails to parse leaves the DB untouched and the file unarchived.
- Assert on the **rendered HTML**, not just the service return value.

## Gotchas that have already cost time here

- Use CSS Grid card-rows, not `<table>`, for any data list — house style.
- The vendored `finances/web/static/css/tailwind.css` is a fixed extract with
  **no build step**. Only use classes already present in it; anything new goes
  in `static/css/app.css` by hand.
- `{{ x | tojson }}` inside a double-quoted HTML attribute truncates the JS
  while every server-side test still passes.
- Adding a column to `transactions` breaks every hand-listed SELECT feeding
  `_row_to_transaction` (repo, triage, monthly, consolidated). You probably
  don't need one — but if you do, grep it and run the full suite.
- `rtk` strips pytest's summary line — count dots, don't read "N passed".
- Never run a real `finances ingest` / `update` against the live
  `finances.db` without asking the owner first. Read-only SELECT is fine.
