# UX Overhaul — Design Spec

Date: 2026-07-11
Status: approved by Julio (pending final spec review)
Scope decision: **Full UX overhaul** — viewer + static report + launcher. CLI command set stays as-is underneath (no teardown; that is wave 2).

## Problem

The engine (ingest, dedup, rates, double-entry) is solid, but the presentation layer never got its last-mile polish. Day-to-day use is hard:

- Dates render as bare `YYYY-MM-DD` everywhere; no weekday.
- Thousands separators exist only on the 4 dashboard KPI tiles; every other number is ungrouped `%.2f`.
- Sign renders after the currency symbol on KPI tiles (`$-1,200.00`) — wrong; must be `-$1,200.00`.
- Triage category picker is a native `<select>` with 26 optgroup options — no search, no quick picks, one transaction at a time, no bulk. Backlog: 412 uncategorized rows + 341 missing-rate rows.
- No way to write a comment/note on a transaction (no DB column, no UI).
- Silent data-loss bug: the edit modal always sends `set_category=true`, so saving with the picker on "— no category —" clears an existing category with no warning (`modal_transaction.html:103-104`).
- Zero feedback: no toast on save, no error handler for failed HTMX requests.
- Double-clicking `Finances.command` only starts the viewer; it never runs `finances update`, so data refresh requires a terminal. (Thing 7 was planned in `docs/plans/revival/07-one-launcher.md` but never built.)
- `finances update` summary says "run Finances.command to sort them" instead of printing a clickable URL.
- Filters: ctrl/cmd-click `<select multiple>`, free-text `YYYY-MM` inputs, no clear button.

## Decisions (locked with Julio 2026-07-11)

| Decision | Choice |
| --- | --- |
| Scope | Full UX overhaul (no CLI teardown) |
| Number format | US grouping: `1,234.56` |
| Date style | English, abbreviated weekday: `Mon, Jul 7` |
| Sign position | Sign before symbol: `-$1,200.00` |
| Category picker | Chips (top used) + search hybrid |
| Comments | Single free-text `notes` column per transaction |

## Design

### 1. Shared formatting layer (foundation)

New module `finances/format.py` — single source of truth, importable by web viewer, `html_export.py`, and CLI:

- `fmt_number(value, places=2) -> str` — grouped, sign-preserving: `-1,234.56`.
- `fmt_money(value, symbol="$", places=2) -> str` — sign before symbol: `-$1,200.00`, `$3,450.00`. Non-symbol currencies (VES) render as `-Bs. 45,231.10` (sign first, then label).
- `fmt_date(dt) -> str` — `Mon, Jul 7`; append year only when ≠ current year: `Mon, Jul 7, 2025`. `None` → em dash.
- `fmt_month(key) -> str` — `"2026-07"` / date → `Jul 2026`.

Wiring:

- Register as Jinja filters/globals in `finances/web/app.py`.
- Rewire `_macros.html` `format_amount` / `format_date` internals to the new filters (all card rows, modals, account cards, triage cards, rate cards inherit automatically).
- Hunt and replace every inline `'%.2f' | format(...)` site: `monthly_pivot.html` (cells + totals), `monthly_mobile_inner.html`, `monthly_mobile_card.html`, `_macros.html:197,200` (pivot_cell).
- Month labels in pivot headers and mobile month nav use `fmt_month`.
- Replace `services/dashboard.py::_format_money` with `finances.format` calls — fixes `$-1,200.00` → `-$1,200.00`.
- Apply same helpers inside `finances/reports/html_export.py` so the static `report.html` matches the viewer.

Testing: unit tests on all four functions with negative values (real expenses are negative — do NOT copy the positive-amount habit of the `seeded_web_db` fixture), zero, None, >1M values, year boundary.

### 2. Safety + feedback

- **Fix category-wipe bug**: edit form only submits `set_category` when the user actually changed the field. Clearing a category becomes an explicit control (an "× remove category" affordance), never a side effect of an untouched empty select. Same for `set_user_rate`.
- **Toast system**: small fixed-position toast container in `base.html`. Success ("Saved") on edit/bulk/pair-confirm via `HX-Trigger` events; failure toast from a global `htmx:responseError` listener showing the server's error detail. No more silent failures.
- **Focus management**: modal opens with the category picker (or search box) focused.

### 3. Triage redesign

- **Picker component** (`partials/category_picker.html`, one shared partial used by both the triage modal and the transactions edit modal):
  - Top 8 most-used categories rendered as large tappable chip buttons. "Most used" computed per kind from actual usage counts (`COUNT(*) GROUP BY category_id` over the last 12 months) in a small service function; falls back to seed order when history is thin.
  - Below chips: a text input that type-filters the full active-category list (client-side filtering; Alpine.js, already vendored).
  - Selecting a chip or list item sets the hidden `category_id` input and highlights the selection.
- **Keyboard**: `1`–`8` select chips, `Enter` = Save & next, `s` = Skip → bottom, `Esc` = close (exists). Scoped to the open modal only, never while typing in the search/notes/rate inputs.
- **Bulk categorize** on `/transactions`:
  - Checkbox per row + "select all on page".
  - Selection reveals an action bar: category picker + Apply.
  - New endpoint `POST /api/transactions/bulk-edit` — body: `{ids: [...], category_id: N}`, validated by Pydantic. Implementation loops through the sanctioned `transactions_repo.update()` per row inside one DB transaction (rule-012: no parallel UPDATE logic). Returns per-row result; UI refreshes the list and toasts "N updated".
- Triage queue keeps its existing All/Rates/Categories/Pairs chips and Save-&-next auto-advance. Skip store stays in-memory (known, acceptable limitation for v1).

### 4. Notes on transactions

Full-stack thread:

1. Migration `007_add_transaction_notes.sql`: `ALTER TABLE transactions ADD COLUMN notes TEXT;` (nullable, no default).
2. `Transaction` Pydantic model: `notes: str | None = None`.
3. `db/repos/transactions.py`: add `notes` to every SELECT column list + `_row_to_transaction`, to `insert`, to `update()` via the `_UNSET` sentinel pattern, and to `upsert_by_source_ref` with a `COALESCE(transactions.notes, excluded.notes)`-style preserve clause so **re-ingest never wipes a manually written note** (same enrichment-preservation contract as category/rate — the ERRORS.md wipe lesson).
4. `TransactionEditRequest`: `set_notes` / `notes` fields; `apply_edit` passes through.
5. UI: textarea in both edit modals; a small note indicator + snippet on the transaction row card; note text included in the transactions text-search filter (`q` matches description OR notes).

Testing: notes round-trip; **re-ingest-preservation test is mandatory** (ingest same file over a noted row → note survives); search-by-note test.

### 5. One launcher (ships Thing 7, adjusted)

Rework `Finances.command` (keep the name — muscle memory + existing docs):

1. Same PATH/uv guards and port-reuse check as today.
2. Run `uv run finances update` in the foreground with its summary visible (VPN hint included on Binance geo-block). Update failure does NOT abort launch — viewer still opens on stale data with the sync strip showing staleness.
3. Start `finances serve` and open the browser (existing behavior), regen static report on exit (existing trap).
4. `finances update` summary change: when needs-review count > 0, print `→ http://localhost:8765/triage` instead of "run Finances.command to sort them".

One double-click = fresh data + open viewer.

### 6. Filter polish

- `/transactions` multi-selects (accounts, kinds, currencies, sources) → tappable checkbox-chip groups (plain checkboxes styled as chips; works on mobile, no ctrl-click).
- `/monthly` since/until free-text → `<input type="month">`.
- "Clear filters" button on both filter forms, resetting to defaults (transactions: last 30 days).

## Non-goals (wave 2 candidates)

- CLI teardown (trap commands `categorize`/`cleanup*`, one-off `scripts/*.py`, `inputs/` archiving).
- Category admin UI (categories remain migration-managed per rule-006).
- Edit history / audit table.
- Saved filter views; persistent skip store.

## Constraints honored

- rule-009: Pydantic at boundaries (bulk-edit request model, notes field).
- rule-012 (viewer writes only via `transactions_repo.update`): bulk edit loops the sanctioned repo call.
- rule-011 TDD: test commits precede impl; hypothesis not required here (no priority/rate logic touched).
- ADR-005 untouched: `apply_edit` continues re-running `rates.resolve` after edits; `needs_review` stays derived, never a manual toggle.
- Offline-first: no new CDN assets; picker uses vendored Alpine.

## Execution shape

Big change → separate plan doc(s) under `docs/plans/ux-overhaul/`, executed one work package per session (same pattern as the revival plan). Dependency order:

1. **WP1 Formatting layer** (`finances/format.py` + filter wiring + template sweep + sign fix + static report) — foundation, no schema change.
2. **WP2 Safety + feedback** (wipe bug, toasts, focus) — independent of WP1, small.
3. **WP3 Notes** (migration 007 + full-stack thread) — independent.
4. **WP4 Triage picker + keyboard + bulk** — depends on WP2's toast infra; picker lands with it.
5. **WP5 Launcher + update hint** — independent, shell + one string.
6. **WP6 Filter polish** — independent, templates only.

Verification gate per package; Julio marks complete (execution rule 3).
