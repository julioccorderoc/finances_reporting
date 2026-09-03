# Prompt — finish the viewer reskin (handoff, 2026-09-03)

Paste everything below the line into a fresh session, run from the repo root.

---

## Where things stand

The viewer reskin shipped on `main` (branch `worktree-viewer-reskin-shell`,
merged 2026-09-03). Read `docs/plans/redesign/shell-notes.md` first — it is
the track's log: the shell contract, the rail composition, every deviation
and what was verified in a browser. Then `CLAUDE.md`.

Done and merged:

- The SIGNAL shell: `partials/rail.html` (244px rail, live Triage badge fed
  by `rail_state(request)` → `services/rail.py` → `count_blocking`), the
  1196px content cap (`--content-cap` in `static/css/shell.css`), the
  `page_header(question, answer, meta)` macro, 25 new icons in `_icons.html`.
- All eight destinations reskinned on signal tokens: Today (`today.css`),
  Flow (`flow.css`), Monthly/Accounts/Rates (`reports.css`), Plans/Ahead
  placeholders (`placeholders.css`). app.css holds only the screen-reader
  helper, x-cloak and the toast; `_macros.html` only `pivot_cell` and
  `page_header`. `tests/web/test_reskin_sweep.py` fails the suite if a rule
  or a macro goes dead again.
- Migration 022: Internal/External Transfer pickable again in the triage
  picker under "MOVED, NOT SPENT", never on a chip (owner decision; see
  `design_handoff_triage/NOTES.md` § 2026-09-03).
- Full suite green; browser walk of every page at 2560/1440/1200 with zero
  console errors/warnings/≥400s; contrast sweep clean; one triage sitting
  walked inside the shell.

## What is left (in order)

1. **Hand-walk the Flow interactions in a browser** against a scratch copy
   of the ledger (never the live `finances.db`): open a row's modal, edit
   category + note, save (toast, row swaps in place), Esc/scrim close with
   the dirty guard; select-all → bulk bar → Apply with and without a
   category; the rail's *Upload a statement* → panel opens → drop a real
   Provincial CSV → preview → import (point the server at a COPY first);
   saved view save/delete; the rates range toggle (address bar must read
   `/rates?range_days=N`); `/monthly?layout=mobile` chevrons. Fix anything
   that only a browser shows (MEMORY.md "browser-only defects").
   Scratch server recipe:
   `FINANCES_WEB_RELOAD_CHILD=1 FINANCES_WEB_HOST=127.0.0.1 FINANCES_WEB_PORT=8766 FINANCES_WEB_TOKEN= FINANCES_WEB_DB_PATH=<copy.db> FINANCES_WEB_REGEN_ON_SHUTDOWN=0 FINANCES_WEB_REFRESH_ON_START=0 uv run uvicorn finances.web.app:create_app_from_env --factory --host 127.0.0.1 --port 8766`
   (copy the DB with sqlite's backup API over `file:...?mode=ro`, then
   `apply_migrations(conn)` on the copy — migration 022 is new).
2. **Apply migration 022 to the live ledger** the normal way
   (`python -m finances.db.migrate` from the repo root) so the picker shows
   the transfer group on the real data. Then take `finances backup --label
   post-reskin`.
3. **Keyboard arrows in the triage modal**: a scripted sitting saw → advance
   the run but ← not return to the previous entry within 30s. Pre-existing
   Wave 3 behaviour, untouched by this track — reproduce by hand before
   deciding it is a bug (the script may have pressed ← before the swap
   settled).
4. **Small follow-ups, each its own TDD pair**: `KpiTile.value` and the
   accounts/mobile totals still render `fmt_money`'s ASCII minus because
   `test_formatting.py` pins them — switch to `fmt_usd` and move the pins;
   rename the three Flow aliases (`cards--selectable`, `choice-chip(s)`,
   `tx-modal-form`) together with their tests; the monthly filter form only
   swaps the pivot, so the chart goes stale after a filter change.
5. **Borrowed money** — a separate sitting:
   `docs/plans/2026-09-03-borrowed-money-prompt.md`.
6. **Next design track**: the monthly pivot's real redesign (the reskin
   deliberately left its structure untouched).

## Rules that still bind

TDD per rule-011 (test commit before impl); `uv run pytest -q` and judge by
dots — the runner strips the summary line; never open the live DB from the
suite (the conftest guard fails it); the vendored `tailwind.css` has no
build step — delete utilities, never add one; new styling goes in the
page's own sheet on `signal.css` tokens; `{{ x | tojson }}` inside a quoted
attribute truncates JS while every server test stays green — use a
`<script type="application/json">` block; one Doto figure per view, red only
as the accent, positive money is ink with a `+`; drive Playwright before
calling any viewer change done. Work in a worktree (EnterWorktree) if
another session may be active on this repo.
