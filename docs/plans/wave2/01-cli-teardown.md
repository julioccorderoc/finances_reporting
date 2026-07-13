# Wave 2 — Thing 1: CLI teardown

**Goal:** remove the migration-era trap commands, give the one-off scripts a home, and make `inputs/` self-cleaning. After this, every command left in `finances --help` is safe to run and part of the real workflow.

**Run in a fresh session. TDD per rule-011 (test commit before impl commit). Tests via `uv run pytest -q`, never against the real `finances.db`. You never mark this complete — Julio does.**

## Context

- The viewer's triage + bulk-categorize (UX overhaul, 2026-07) replaced the whole CSV-round-trip cleanup path.
- `finances categorize` is a trap: its non-dry path refuses to write and exits 2.
- `inputs/` holds every statement ever dropped; all are re-parsed on every `finances update`, relying on hash dedup alone.

## Tasks

1. **Delete trap/migration-era commands** from `finances/cli/main.py`: `categorize`, `cleanup`, `cleanup-export`, `cleanup-apply`. Delete `finances/migration/interactive_cleanup.py` (keep `finances/migration/backfill.py` — `backfill` stays). Delete their tests (`tests/test_migration_cleanup.py`; prune `categorize` tests wherever they live — grep first). CSV escape hatch remains `finances report needs-review --csv`.
2. **Scripts get a home:** move `scripts/canonicalize_provincial_refs.py` and `scripts/export_category_mapping.py` (completed one-times) to `archive/` (gitignored — `git rm` them, they stay in history). Keep `scripts/import_bcv_history.py` (still the rate-gap tool) and document it in README under a "Maintenance tools" section (one line: what it does, how to run).
3. **Auto-archive ingested inputs:** in `finances/reports/update.py` `_step_provincial`, after a file ingests successfully (non-dry-run only), move it to `inputs/processed/` (create dir; add to `.gitignore`; collision-safe: append `-1`, `-2` on name clash). Failed or dry-run files stay put. The explicit `finances ingest provincial <path>` command does NOT move files — only the `update` sweep. Update the `update` summary to mention moved files (`archived N file(s) → inputs/processed/`).
4. **Docs:** update README + CLAUDE.md "Key Commands" (remove deleted commands, note `inputs/processed/`). Check `docs/runbooks/` for stale references.

## Gates (all must pass)

- `uv run pytest -q` — full suite green.
- `uv run finances --help` no longer lists categorize/cleanup*; `rg -n "interactive_cleanup" finances/ tests/` → no hits.
- New tests prove: successful ingest moves file to `inputs/processed/`; dry-run moves nothing; failed parse moves nothing; name collision gets a suffix.
- `rg -n "cleanup-export|cleanup-apply" README.md CLAUDE.md docs/runbooks/` → no hits.
