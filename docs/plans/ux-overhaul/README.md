# UX overhaul — index

Goal: dummy-proof viewer. Design + locked decisions: [00-design.md](00-design.md)
(approved by Julio 2026-07-11).

## How to run a work package

Open a **fresh** Claude Code session in this repo and say:

> Execute `docs/plans/ux-overhaul/0X-{name}.md`

One work package per session. No bundling ("while I'm in here" is banned).
Every plan uses TDD (test commit before impl commit) and ends with a
pass/fail gate; the agent never marks a package complete — Julio does.

## Status

| # | Work package | File | Depends on | Status |
| --- | --- | --- | --- | --- |
| 1 | Formatting layer — `finances/format.py`, thousands separators, weekday dates, `-$` sign fix, static report parity | [01-formatting.md](01-formatting.md) | — | ✅ 2026-07-12 (validated: full suite green, no stale `%.2f` in templates, `_format_money` gone, filters registered, html_export on shared fmt_*, live output spot-checked) |
| 2 | Safety + feedback — category-wipe bugfix, toasts, error handler, focus | [02-safety-feedback.md](02-safety-feedback.md) | — | ✅ 2026-07-12 (validated: suite green, 17 safety tests pass, sentinels replaced by dirty-tracking, toast host + show-toast + error listener live, HX-Trigger toast JSON, remove-control + autofocus present) |
| 3 | Transaction notes — migration 008, full-stack thread, re-ingest-safe | [03-notes.md](03-notes.md) | — | ⬜ |
| 4 | Triage picker + keyboard + bulk categorize | [04-triage-picker-bulk.md](04-triage-picker-bulk.md) | WP2 (toast infra; has fallbacks if WP2 absent) | ⬜ |
| 5 | Launcher reorder (update foreground-first) + triage URL in summary | [05-launcher.md](05-launcher.md) | — | ⬜ |
| 6 | Filter polish — checkbox chips, month picker, clear button | [06-filters.md](06-filters.md) | — | ⬜ |

Suggested order: 1 → 2 → 3 → 4 → 5 → 6 (2 before 4; the rest are independent).

Update the Status column (⬜ → ✅ + date) when a package's gate passes.

## Shared interface contracts

Cross-package names are pinned in each plan's Global Constraints. The
load-bearing ones:

- `finances/format.py`: `fmt_number`, `fmt_money` (sign **before** symbol),
  `fmt_date` (weekday, year only when not current), `fmt_month` — registered
  as Jinja filters under the same names.
- Toast: window CustomEvent **`show-toast`** with
  `detail = {level: "success"|"error", message}`; `base.html` parses
  `HX-Trigger` JSON and re-dispatches it.
- Picker: `partials/category_picker.html`; hidden `set_category` defaults
  `"false"` (untouched form never wipes); explicit "× remove category"
  control is the only clear path.
- Bulk: `POST /api/transactions/bulk-edit` `{ids, category_id}` →
  `{"updated": N}`; loops `transactions_repo.update()` (rule-012).
- Notes migration is `008_add_transaction_notes.sql` (007 was taken).
- Tailwind CSS is a compiled artifact — rebuild
  `finances/web/static/css/tailwind.css` after adding utility classes
  (recipe in `tailwind/README.md`; WP4 step 2.5b).
