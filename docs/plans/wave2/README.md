# Wave 2 — index

Follow-ups parked during the UX overhaul (spec: `../ux-overhaul/00-design.md`
non-goals). Prompt-file format — compact spec + gates, executor works from
house rules (CLAUDE.md, rule files) like the revival plan did.

## How to run a thing

Fresh Claude Code session in this repo:

> Execute `docs/plans/wave2/0X-{name}.md`

One thing per session. TDD (test commit before impl). Julio judges the gates.

## Status

| # | Thing | File | Status |
| --- | --- | --- | --- |
| 1 | CLI teardown — drop trap commands, home the scripts, self-cleaning inputs/ | [01-cli-teardown.md](01-cli-teardown.md) | ✅ 2026-07-13 (validated: suite green, categorize/cleanup* gone from help + code + docs, archive-move tests in test_update_cli.py, scripts archived, import_bcv_history documented, inputs/ gitignored) |
| 2 | Saved filter views — DB-backed, chip recall on /transactions | [02-saved-views.md](02-saved-views.md) | ✅ 2026-07-13 (validated: suite green, migration 010, repo + Pydantic + 3 HTMX endpoints, chips partial, CSS compiled) |
| 3 | Edit history — audit rows in `transactions_repo.update()`, modal History section | [03-edit-history.md](03-edit-history.md) | ✅ 2026-07-13 (validated: suite green, migration 009 excludes needs_review, hook in single write path, upsert records nothing, modal `<details>` History) |

Order: 1 first (removes code the others would otherwise have to respect);
2 and 3 in either order after. Migration prefixes: whoever runs first takes
the next free number — always check `finances/db/migrations/` (009 free as
of writing; 008 = notes).

Also pending, not part of wave 2: revival Thing 6 (ceremony teardown) —
`docs/plans/revival/06-ceremony-teardown.md`, run whenever.

Update Status (⬜ → ✅ + date) when a thing's gate passes.
