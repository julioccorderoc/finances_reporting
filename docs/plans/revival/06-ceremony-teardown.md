# Thing 6 — ceremony teardown (run LAST, after 2-5 are done)

## Context (assume no prior knowledge)

This repo accumulated ~28,000 words of process docs (PRD, 27-epic roadmap,
12 ADRs, 12 rule files, TDD/coverage gates, file-ownership matrices) governing
~12,000 lines of code. The owner abandoned the project because of exactly this.
Decision (already made): archive everything, replace with a 1-page CLAUDE.md.
Nothing is deleted from history — git keeps it all.

## Task

### A. Archive process docs

`git mv` into `docs/archive/`: `docs/PRD.md`, `docs/roadmap.md`, `docs/ADR/`,
`docs/architecture/`, and any plan/prompt docs that are finished (including
`docs/plans/revival/` itself once its README shows all ✅ — else leave it).
Add `docs/archive/README.md`: one paragraph — "historical process docs,
reference only, superseded by CLAUDE.md invariants on <date>".

### B. New 1-page CLAUDE.md (versioned!)

- Remove `CLAUDE.md` and `ERRORS.md` from `.gitignore` (keep `MEMORY.md`
  ignored — it's personal auto-memory). Commit CLAUDE.md from now on.
- Rewrite CLAUDE.md to fit ONE page:
  - What this is (2 lines) + the weekly ritual (3 lines: CSV → VPN →
    `finances update`; edit via `Finances.command`; view via `report.html`).
  - **Invariants** (the only rules that survive — they protect data):
    1. SQLite is the only source of truth; `report.html` and Sheets are
       generated, never hand-edited.
    2. Every transaction has `source_ref`; dedup via `UNIQUE(source, source_ref)`;
       re-ingesting anything must insert 0 duplicates.
    3. Transfers: two rows, shared `transfer_id`, sum to zero.
    4. Headline USD rate: `user_rate` → P2P median → BCV → needs_review;
       headline is never BCV alone.
    5. Uncategorizable rows get `needs_review=1`, never a guessed category.
    6. Pydantic models at every boundary; repos never take/return raw dicts.
    7. Tests must pass (`uv run pytest -q`); mocks for external SDKs must be
       spec'd against the real client class.
    8. `legacy/` is read-only; DB files never committed.
  - Command list (the ~8 that matter).
  - "Historical docs: docs/archive/ — reference only. New big decision?
    Note it in CLAUDE.md under Decisions, one line."
- Delete the dead references (epics, waves, ownership matrix, ADR-first,
  TDD-commit-ordering, coverage thresholds as gates).

### C. Cruft removal (list everything first, then act)

- Stale git worktrees: `git worktree list` → remove stale ones
  (`git worktree remove --force` only after showing the list in the session
  and confirming they hold no unique uncommitted work — check with
  `git -C <wt> status`).
- Stray DB copies at repo root (`finances.db.bak-*`, old backups >30 days) —
  list, then delete.
- `pandas` appears unused: verify `grep -rn "import pandas\|from pandas" finances/ tests/`
  is empty → remove from pyproject.toml, `uv lock`, run tests.
- Any other dead deps the same way (verify by grep before removing).

## Rules

1. Archive = `git mv` (history preserved). Never `rm` a doc.
2. Worktree/DB deletion: show the list and get an explicit yes in-session
   before removing anything.
3. After every step: `uv run pytest -q` green.

## Gate

- [ ] `docs/` contains only: archive/, superpowers/ (specs), and any still-live
      plans. Nothing else at top level.
- [ ] New CLAUDE.md ≤ 60 lines, committed (not gitignored).
- [ ] `git worktree list` → only the main worktree.
- [ ] No stray `.db` files at repo root except `finances.db` (+ current backup).
- [ ] `uv run pytest -q` green; `uv pip install -e .` still works.
