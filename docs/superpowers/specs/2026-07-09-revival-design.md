# Revival Design — finances_reporting (2026-07-09)

Owner: Julio. Approved in the 2026-07-09 planning session.

## Problem

Project was abandoned: process ceremony (~28k words of docs governing ~12k LOC)
made every change slower than the old spreadsheet. The code itself works:
539 tests green, clean 584KB SQLite ledger, working web viewer.

## Goal (owner's words, distilled)

1. **One HTML I can open anytime** — no server, no internet, double-click.
2. **One clean portable SQLite file** — already true (`finances.db`), keep it so.
3. **Easy catch-up import** after months idle.
4. **Less ceremony than the spreadsheet**, in product AND process.

## Decisions (all confirmed with owner)

| # | Decision | Choice |
|---|----------|--------|
| D1 | HTML model | **Combo**: static `report.html` for viewing + double-click `Finances.command` launcher for edit sessions in the existing viewer. Static file cannot write to the DB (browser sandbox); edits need the server alive. |
| D2 | Live sources | All four stay: Provincial CSV, Binance API, cash CLI, Sheets mirror. |
| D3 | Rate gap Apr–Jul 2026 | One-time import from owner-provided `tasas-bcv-july-9.html` (daily BCV USD+EUR, Jan 2 → Jul 10 2026). No third-party API. |
| D4 | Process docs | Archive everything to `docs/archive/`; replace CLAUDE.md with a 1-page version (invariants + commands only). No epics, no ADR-first, no TDD-commit-ordering. Tests must still pass. |
| D5 | Binance geo-block | VPN + API pull. **Done live in the planning session**: +130 transactions, +2 earn positions, idempotency verified. |
| D6 | report.html build | Reuse the viewer's query services (`finances/web/services/*`) + one self-contained template. No duplicated math. |
| D7 | Packaging of work | Directory of self-contained prompt files: `docs/plans/revival/` — one prompt per session, orchestrator evaluates between sessions. |

## End state

- Look at money → double-click `report.html` (offline, self-contained).
- Change something → double-click `Finances.command` → browser viewer → close.
- Weekly ritual (~10 min): drop bank CSV in `inputs/`, VPN on, `finances update`.
  Prints per-source summary + how many rows need triage. Regenerates report.html.
- `report.html` regenerates after every successful ingest and after edit sessions.

## The six things

1. **Commit backlog** — review + commit April's uncommitted diff (BCV homepage
   scrape + truststore SSL + uniform --dry-run) and tonight's Binance live-API
   fixes (real SDK names, real convert schema, ≤29-day window chunking,
   tz-aware --since, spec'd mocks). *Executed in the planning session itself.*
2. **Rate gap fill** — parse `tasas-bcv-july-9.html` → rates table via repos.
   Idempotent, dry-run first. (`docs/plans/revival/02-rate-gap.md`)
3. **Provincial catch-up** — ingest fresh bank CSVs, P2P pairing, triage.
   (`03-provincial-catchup.md`)
4. **Static report** — `finances html` → self-contained report.html reusing
   web services. Auto-regen post-ingest. (`04-static-report.md`)
5. **One-command UX** — `finances update` wrapper, `Finances.command` launcher,
   vendor CSS offline, regen after edit sessions. (`05-ux-launcher.md`)
6. **Ceremony teardown** — archive docs, 1-page CLAUDE.md (un-gitignored),
   remove stale worktrees/stray DB copies/dead deps. (`06-ceremony-teardown.md`)

## Non-goals

Electron/Tauri, mobile app, cron scheduling, receipt OCR, new frameworks,
epic/roadmap system, rewriting the viewer, historical P2P rates (impossible).

## Quality bar

- New code arrives with tests written first; mocks spec'd against real
  libraries (lesson: fake SDK method names survived 535 green tests).
- Every DB-writing step: `.backup` + `--dry-run` before the real run.
- Gate for report.html: opens with networking disabled; numbers equal the
  viewer's for the same queries.
