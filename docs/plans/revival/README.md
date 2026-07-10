# Revival plan — index

Goal: finances easier than the spreadsheet. Design + decisions:
[`docs/superpowers/specs/2026-07-09-revival-design.md`](../../superpowers/specs/2026-07-09-revival-design.md)

## How to run a thing

Open a **fresh** Claude Code session in this repo and say:

> Execute `docs/plans/revival/0X-{name}.md`

One thing per session. No bundling ("while I'm in here" is banned).
After each session, report results back to the orchestrator session (or just
check the gate yourself — every prompt ends with a pass/fail gate).

## Status

| # | Thing | File | Status |
|---|-------|------|--------|
| 1 | Commit backlog (April diff + Binance live-API fixes) | — | ✅ done 2026-07-09, in planning session |
| 2 | BCV rate-gap fill from tasas-bcv-july-9.html | [02-rate-gap.md](02-rate-gap.md) | ✅ 2026-07-10 (validated: 104 rows, spot-checks exact, idempotent) |
| 3 | Provincial catch-up + triage | [03-provincial-catchup.md](03-provincial-catchup.md) | ✅ ingest done 2026-07-10 (327 rows, 17 P2P pairs, HTML-XLS support added); ⬜ Julio's triage of 386 needs_review rows pending |
| 4 | `finances html` static report | [04-static-report.md](04-static-report.md) | ⬜ |
| 5 | `finances update` + desktop launcher + offline CSS | [05-ux-launcher.md](05-ux-launcher.md) | ⬜ |
| 6 | Ceremony teardown (archive docs, 1-page CLAUDE.md) | [06-ceremony-teardown.md](06-ceremony-teardown.md) | ⬜ run last |

Order: 2 → 3 → 4 → 5 → 6. (2/3 are data, 4/5 are product, 6 is cleanup.)
3 can slip anywhere before 6; it's the only one needing Julio's input
(download CSVs from the bank site into `inputs/`).

Update the Status column (⬜ → ✅ + date) when a thing's gate passes.
