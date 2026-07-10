# Thing 5 — one-command UX: `finances update` + desktop launcher + offline viewer

## Context (assume no prior knowledge)

Personal finances ledger, SQLite, Typer CLI, FastAPI viewer (`finances serve`),
static report via `finances html` (built in Thing 4 — verify it exists; if not,
STOP and run Thing 4 first). Owner is a solo non-ops user on macOS: everything
must be double-click or one command. Binance API is geo-blocked without VPN.

## Task — three pieces

### A. `finances update` — the weekly ritual as one command

Runs, in order, each step isolated (one failing step doesn't stop the rest):
1. `bcv` rate scrape
2. `p2p-rates`
3. `binance` incremental sync (uses stored cursor; print a hint about VPN if
   it fails with 451/network errors)
4. `provincial` for any not-yet-ingested files in `inputs/` (skip silently
   if none)
5. regenerate `report.html`

Ends with a plain-language summary block: per-source inserted/updated/errors,
needs_review total ("N rows waiting for triage — run Finances.command"), and
data freshness per source. Add `--dry-run` that passes dry-run through to
every step.

### B. `Finances.command` — double-click edit sessions

A small executable script committed at repo root (`Finances.command`, chmod +x):
- cd into the repo, start `finances serve` if the port is free (else reuse),
- open the browser at the viewer,
- on exit (Ctrl-C / window close), regenerate `report.html` so the static
  file reflects the edits.
Keep it dumb: plain bash, no launchd, no daemons. Also regenerate report.html
after write-back operations OR on server shutdown — pick the simplest reliable
hook in the FastAPI app (shutdown event is fine).

### C. Viewer fully offline

The viewer currently loads Tailwind from a CDN → unstyled without internet.
Fix using the standard approach: compile the used utility classes once with the
Tailwind standalone CLI into a static CSS file vendored under the web static
dir, referenced locally. htmx/alpine/chart.js are already vendored. After the
change: `grep -rn "cdn\|https://" finances/web/templates/` → only local refs.

## Rules

1. TDD where testable (update-command orchestration with mocked step
   functions; shutdown-regen hook). The .command script itself: manual test.
2. Reuse existing CLI functions — `update` calls the same code paths as the
   individual commands, no forked logic.
3. Don't redesign the viewer; CSS swap must be visually identical (spot-check
   dashboard + triage pages).

## Gate

- [ ] `uv run finances update --dry-run` runs end-to-end, prints summary,
      writes nothing.
- [ ] `uv run finances update` works; report.html regenerated.
- [ ] Double-click `Finances.command` in Finder → browser opens viewer →
      Ctrl-C → report.html mtime updated.
- [ ] Viewer renders styled with Wi-Fi off.
- [ ] `uv run pytest -q` green.

## Out of scope

cron/launchd scheduling, LAN/mobile access changes, auth changes, Sheets sync
(stays manual), doc teardown (Thing 6).
