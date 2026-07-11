# Thing 7 — one entry point: `finances.command`

## Context (assume no prior knowledge)

Personal finances ledger (SQLite, Typer CLI, FastAPI viewer). Owner is a solo
non-ops user on macOS. Today there are TWO user-facing files and he hates it:

- `report.html` — generated static report (double-click to view)
- `Finances.command` — launcher: starts `finances serve`, opens browser,
  regenerates report.html on exit

Owner decision (final): **one double-clickable entry point that does
everything**. `report.html` stays as an auto-generated artifact (offline /
"Python is broken" fallback) but is never again something he opens as part of
the normal flow.

## Task

Replace `Finances.command` with a single launcher `finances.command`
(Finder shows it as "finances"; the `.command` suffix is what makes
macOS execute it on double-click — do not fight this, just name it so).

On double-click it must, in this order:

1. `cd` into the repo (wherever the script lives), activate the right
   environment the same way the current launcher does.
2. Start `finances serve` if the port is free; reuse a running server
   otherwise (same logic as the current launcher — read it before writing).
3. Open the browser at the viewer immediately — the owner should never wait
   on network syncs to start looking at his money.
4. THEN run `finances update` as a background job in the same terminal, so
   its per-source summary (inserted counts, errors, needs-review total)
   prints while he's already browsing. `finances update` already regenerates
   report.html itself and is safe to run concurrently with the server
   (SQLite WAL). If it fails (offline, VPN off for Binance), that's fine —
   its output says so; the launcher must not die because of it.
5. On exit (Ctrl-C or closing the terminal window): regenerate report.html
   (same belt-and-suspenders the current launcher has; the server's
   shutdown hook also does it).

Then:

- `git rm Finances.command` (replaced, not kept alongside — the whole point
  is ONE file).
- Update `docs/plans/revival/README.md`: add a Thing 7 row (this file) and
  mark it done when the gate passes.
- Grep docs for `Finances.command` references (05-ux-launcher.md, update
  command hint strings in `finances/reports/update.py`, CLAUDE.md if it
  mentions it) and update them to the new name.

## Rules

1. Plain bash, standard patterns, no launchd/daemons/AppleScript apps.
   Read the existing `Finances.command` first and keep everything it already
   does right (port reuse, env resolution, exit trap).
2. The `finances update` hint text printed to the user ("run
   Finances.command to sort them") must point at the new name — that string
   lives in the update summary code and has tests; update both.
3. Python changes (if any) are test-first. The bash script itself: manual
   gate below.
4. Do not touch report.html generation, the viewer, or ingest logic.

## Gate

- [ ] Exactly ONE user-facing launcher file at repo root
      (`finances.command`, executable). `Finances.command` gone.
- [ ] Double-click in Finder: browser opens the viewer within ~2s, update
      summary appears in the terminal while browsing, Ctrl-C exits cleanly
      and report.html's mtime is fresh.
- [ ] Second double-click while a server is already running: reuses it, no
      port error.
- [ ] `grep -rn "Finances.command" . --include="*.py" --include="*.md"
      --include="*.command"` → no stale references (except git history and
      docs/archive if any).
- [ ] `uv run pytest -q` green.

## Out of scope

Ceremony teardown (Thing 6), triage, opening balances, new viewer features,
Windows/Linux launchers.
