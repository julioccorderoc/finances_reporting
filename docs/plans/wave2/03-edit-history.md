# Wave 2 — Thing 3: Edit history

**Goal:** every manual transaction edit (category, rate, note) is recorded — old value, new value, when — and visible in the transaction modal. Today edits are in-place and unrecoverable.

**Run in a fresh session. TDD per rule-011 (test commit before impl commit). Tests via `uv run pytest -q`, never against the real `finances.db`. Pydantic v2 at boundaries (rule-009). You never mark this complete — Julio does.**

## Design (locked with Julio 2026-07-13)

- New table `transaction_edits`: `id INTEGER PK`, `transaction_id INTEGER NOT NULL REFERENCES transactions(id) ON DELETE CASCADE`, `edited_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP`, `field TEXT NOT NULL CHECK (field IN ('category_id','user_rate','notes'))`, `old_value TEXT`, `new_value TEXT`; index on `transaction_id`. **Use the next free migration prefix — check `finances/db/migrations/` first (010 expected if saved-views took 009; renumber freely, prefix order is whatever is free).**
- **Recording hook lives in `transactions_repo.update()`** — the single sanctioned write path (rule-012), so the viewer modal, triage, PATCH API, and bulk-edit are all covered automatically with zero endpoint changes. Inside the same DB transaction: read current values first, insert one `transaction_edits` row per field that actually CHANGED (passed-but-equal values record nothing). Do NOT record `needs_review` — it is resolver-derived noise.
- Pydantic `TransactionEdit` model; repo `finances/db/repos/transaction_edits.py` with `list_for_transaction(conn, transaction_id) -> list[TransactionEdit]` (newest first). Inserts happen only inside `transactions_repo.update()` — do not expose a public insert for web code.
- UI: collapsible "History" section at the bottom of the transactions edit modal (`<details><summary>History (N)</summary>…</details>` — standard element, no JS). Each row: `fmt_date`-formatted timestamp, field label, `old → new`. Resolve `category_id` values to category names for display (unknown/deleted id → show the raw id). Empty history → omit the section entirely.
- Ingest/backfill paths (`insert`, `upsert_by_source_ref`) record NOTHING — history is for manual edits only.

## Out of scope

Undo/revert, history for transfers/pair-confirm, a standalone history page, retention pruning.

## Gates (all must pass)

- `uv run pytest -q` — full suite green.
- New tests: single-field edit → exactly one history row with correct old/new; multi-field edit → one row per changed field; no-op update (same values) → zero rows; bulk-edit N transactions → N rows; re-ingest/upsert → zero rows; `ON DELETE CASCADE` verified; modal renders history section with resolved category names and hides it when empty.
- Manual (Julio): edit a category twice, open the modal, see both entries newest-first.
