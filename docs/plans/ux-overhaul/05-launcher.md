# WP5 One Launcher + Update Hint Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** One double-click on `finances.command` syncs every source in the foreground (summary visible first), then opens the viewer — and the `finances update` summary points at `http://localhost:8765/triage` whenever rows need review.

**Architecture:** Two independent, tiny changes. (1) Reorder the plain-bash launcher: the `finances update` step moves from a background job *after* the browser opens to a guarded foreground step *before* `finances serve` starts; a failed update never aborts the launch (the viewer opens on stale data; its sync strip shows staleness). (2) One string in `finances/reports/update.py::render_summary` — the needs-review > 0 line prints the triage URL instead of "run finances.command to sort them" — driven test-first.

**Tech Stack:** bash (macOS `.command` double-click launcher), Typer CLI (`finances update` / `finances serve` / `finances html`), pytest via `uv run pytest -q`.

## Global Constraints

- The launcher's git-tracked filename is **`finances.command`** (lowercase — Thing 7 renamed it on 2026-07-11; the design spec's "Finances.command" is the same file on case-insensitive APFS). Do **not** rename it. Rework in place.
- Update failure must **not** abort the launch; guard with `|| echo …` (the `|| true` equivalent) so `set -euo pipefail` stays intact.
- Contract string (consumed by other WPs): when needs-review count > 0, `render_summary` prints a line containing exactly `http://localhost:8765/triage`.
- Run tests only as `uv run pytest -q <path>` — never bare `pytest`.
- TDD per rule-011 / CLAUDE.md execution rule 5: the test commit lands **before** the implementation commit.
- Tests never touch the real `finances.db`; the new tests build `UpdateReport` dataclasses directly and use `tmp_path` (same pattern as the existing `test_render_summary_zero_needs_review_names_launcher`).
- Never run `finances update` / ingest against the real DB as an automated step (owner rule: read-only by default). Live launcher runs are the **owner's manual gate**, listed in Task 3.
- Plain bash, standard patterns; no launchd, daemons, AppleScript, or new dependencies.
- The shell script has no pytest harness: its automated checks are `bash -n finances.command` plus the non-interactive failtest in Task 2 Step 4 (background launch → log poll → SIGINT → log assertions → unconditional cleanup). Anything that genuinely needs a human — a real Finder double-click, the browser visibly opening, a real Ctrl-C in a Terminal window — is the owner's manual gate in Task 2 Step 6.
- Scope: only `finances.command`, `finances/reports/update.py`, `tests/test_update_cli.py`, and a `docs/runbooks/` audit. No "while I'm in here" changes (execution rule 1).
- The agent never marks the work package complete; Julio does (execution rule 3).

---

### Task 1: Triage URL in the `finances update` summary

**Files:**
- Modify: `finances/reports/update.py:314-319` (the `if report.needs_review_total:` branch inside `render_summary`)
- Test: `tests/test_update_cli.py` (add one test after line 352; add one assertion to `test_render_summary_zero_needs_review_names_launcher` at lines 338-352)

**Interfaces:**
- Consumes: `finances.reports.update.UpdateReport` dataclass — constructor fields `outcomes: list[SourceOutcome]`, `needs_review_total: int`, `freshness: list[Any]`, `dry_run: bool`, `report_regenerated: bool`, `report_path: Path`, `regen_error: str | None = None` (all already exist; no signature changes).
- Consumes: `finances.reports.update.render_summary(report: UpdateReport) -> str` (existing signature, unchanged).
- Produces: the summary contract — when `needs_review_total > 0` the rendered text contains `http://localhost:8765/triage` and no longer contains the "sort them" wording. Task 2's launcher makes this line the last thing on screen before the server starts, so the user can click/copy it.

Background you need (read once, don't re-derive): `render_summary` currently renders the needs-review block like this (`finances/reports/update.py:314-323`):

```python
    lines.append("")
    if report.needs_review_total:
        lines.append(
            f"Needs review: {report.needs_review_total} row(s) waiting for "
            "triage — run finances.command to sort them."
        )
    else:
        lines.append(
            "Needs review: 0 rows — nothing waiting. (triage via finances.command)"
        )
```

The two existing `render_summary` tests (`test_render_summary_is_plain_language`, `test_render_summary_zero_needs_review_names_launcher`) both exercise the **zero** branch (`needs_review_total == 0`), so they keep passing — the zero-branch wording does not change in this WP. They are updated only to pin that the triage URL does **not** leak into the zero branch.

- [ ] **Step 1: Write the failing test (and strengthen the zero-branch test)**

In `tests/test_update_cli.py`, add this test directly after `test_render_summary_zero_needs_review_names_launcher` (i.e. after line 352, before the `# CLI smoke` comment block):

```python
def test_render_summary_needs_review_points_at_triage_url(tmp_path) -> None:
    from finances.reports.update import UpdateReport, render_summary

    report = UpdateReport(
        outcomes=[],
        needs_review_total=7,
        freshness=[],
        dry_run=False,
        report_regenerated=True,
        report_path=tmp_path / "report.html",
    )
    text = render_summary(report)

    assert "Needs review: 7" in text
    assert "http://localhost:8765/triage" in text
    # Old wording is gone — the URL replaces the "sort them" instruction.
    assert "sort them" not in text
```

And inside the existing `test_render_summary_zero_needs_review_names_launcher` (lines 338-352), extend the assertions block. **Careful — the two assert lines alone are NOT a unique anchor:** the identical pair also appears in `test_render_summary_is_plain_language` (lines 332-333). Anchor the edit on the preceding `text = render_summary(report)` line, which is unique to the zero test (the plain-language test calls `render_summary(result)` and its assert pair is followed by a `for src in (...)` loop). Replace exactly this — the second occurrence of the assert pair, at lines 349-352:

```python
    text = render_summary(report)

    assert "finances.command" in text
    assert "Finances.command" not in text
```

with:

```python
    text = render_summary(report)

    assert "finances.command" in text
    assert "Finances.command" not in text
    # The triage URL only appears when something is actually waiting.
    assert "http://localhost:8765/triage" not in text
```

(This anchor stays unique even if you add the new test above first: the new test also calls `render_summary(report)`, but the assertions that follow it differ.)

- [ ] **Step 2: Run the test file to verify the new test fails**

Run: `uv run pytest -q tests/test_update_cli.py`

Expected: `1 failed, 10 passed`. The failure is `test_render_summary_needs_review_points_at_triage_url` with:

```
AssertionError: assert 'http://localhost:8765/triage' in 'finances update\n\nSources:\n\nNeeds review: 7 row(s) waiting for triage — run finances.command to sort them.\n\nReport: regenerated ...'
```

(`test_render_summary_zero_needs_review_names_launcher` still passes — the URL isn't in the output at all yet.)

- [ ] **Step 3: Commit the test (before any implementation — rule-011)**

```bash
git add tests/test_update_cli.py
git commit -m "test(update): needs-review summary points at the triage URL"
```

- [ ] **Step 4: Minimal implementation**

In `finances/reports/update.py`, replace the `if` branch of the needs-review block (lines 315-319). Old:

```python
    if report.needs_review_total:
        lines.append(
            f"Needs review: {report.needs_review_total} row(s) waiting for "
            "triage — run finances.command to sort them."
        )
```

New:

```python
    if report.needs_review_total:
        lines.append(
            f"Needs review: {report.needs_review_total} row(s) waiting for "
            "triage → http://localhost:8765/triage"
        )
```

The `else` branch (zero rows) is untouched.

- [ ] **Step 5: Run the test file to verify everything passes**

Run: `uv run pytest -q tests/test_update_cli.py`

Expected: `11 passed`

- [ ] **Step 6: Run the full suite to catch any other test pinned to the old wording**

Run: `uv run pytest -q`

Expected: all green (the "sort them" string exists nowhere else — verified by `grep -rn "sort them" finances tests`, which matches only `finances/reports/update.py` before this change).

- [ ] **Step 7: Commit the implementation**

```bash
git add finances/reports/update.py
git commit -m "feat(update): print triage URL in needs-review summary line"
```

---

### Task 2: `finances.command` — sync in the foreground, then serve

**Files:**
- Modify: `finances.command` (repo root; full-file rewrite shown below; keep mode `755` — editing in place preserves it)

**Interfaces:**
- Consumes: `uv run finances update` — Typer command; **exits 0 even when individual sources fail** (each step is isolated inside `run_update`; failures show as `error` rows in the summary, plus the VPN hint on Binance geo-block). A non-zero exit only happens on a hard crash (broken env, DB open failure, unexpected exception) — that is what the `|| echo` guard covers.
- Consumes: `uv run finances serve --port "$PORT"` (foreground server, default port 8765), `uv run finances html` (read-only static-report regen used by the exit trap), `FINANCES_PORT` env override.
- Consumes: Task 1's summary line — with the new ordering, `→ http://localhost:8765/triage` prints in the terminal *before* the server starts.
- Produces: the one-double-click behavior contract: guards → port-reuse check → foreground update (never aborts launch) → background browser-open → foreground serve → regen-on-exit trap.

Behavior change vs. the current file (read it first — `finances.command` at repo root): today the update runs as a `( sleep 3; … ) &` background job *after* the browser-open job, so its summary interleaves with uvicorn logs while you browse. This task moves it to a guarded foreground step *before* the server starts, per the approved spec (`docs/plans/ux-overhaul/00-design.md` §5). Everything else — shebang, `set -euo pipefail`, `REPO_DIR` resolution, PATH export, uv guard, `port_in_use` reuse-and-exit path, `regen_report` EXIT trap, `( sleep 2; open "$URL" … ) &` browser job, foreground `uv run finances serve` — is kept.

- [ ] **Step 1: Replace the file contents**

Write `finances.command` (repo root) with exactly this content:

```bash
#!/usr/bin/env bash
#
# finances.command — double-click this in Finder: the ONE entry point.
# (Finder hides the .command suffix and shows it as "finances"; the suffix
# is what makes macOS execute it on double-click.)
#
# What it does (deliberately dumb — plain bash, no launchd, no daemons):
#   1. cd into the repo (wherever this file lives),
#   2. if the port is already bound, reuse the running server: re-open the
#      browser and exit,
#   3. run `finances update` in the FOREGROUND so its per-source summary
#      (inserted counts, errors, VPN hint, needs-review total + triage URL)
#      is on screen before the viewer opens; if it fails (offline, VPN off
#      for Binance) the launch continues — the viewer opens on existing data
#      and the sync strip shows what's stale,
#   4. start `finances serve` and open the browser at the viewer,
#   5. on exit (Ctrl-C or closing the window), regenerate report.html so the
#      static file reflects any edits made this session.
#
# The server's own shutdown hook also regenerates report.html; the EXIT trap
# below is a harmless belt-and-suspenders for the cases where it can't (e.g. a
# hard window close before the server finishes teardown). `finances update`
# regenerates report.html too.

set -euo pipefail

# Resolve the repo dir from this script's location and work from there.
REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]:-$0}")" && pwd)"
cd "$REPO_DIR"

# Finder launches .command with a bare PATH; add the usual spots so `uv` resolves.
export PATH="$HOME/.local/bin:/opt/homebrew/bin:/usr/local/bin:$PATH"

PORT="${FINANCES_PORT:-8765}"
URL="http://localhost:${PORT}/"

if ! command -v uv >/dev/null 2>&1; then
  echo "Error: 'uv' is not on PATH. Install uv (https://docs.astral.sh/uv/), then re-run." >&2
  read -r -p "Press Enter to close… " _ || true
  exit 1
fi

port_in_use() {
  lsof -nP -iTCP:"$PORT" -sTCP:LISTEN >/dev/null 2>&1
}

if port_in_use; then
  echo "Finances viewer already running at ${URL} — reusing it."
  open "$URL" || true
  exit 0
fi

# Refresh the static report on the way out (idempotent; read-only export).
regen_report() {
  echo "Refreshing report.html …"
  uv run finances html >/dev/null 2>&1 || true
}
trap regen_report EXIT

# Sync every source BEFORE the viewer opens, in the foreground, so the
# summary (per-source counts, errors, needs-review total + triage URL) is
# the first thing on screen. The `|| echo` guard means a hard update failure
# never aborts the launch under `set -e` — the viewer still opens on
# existing data and its sync strip shows the staleness.
echo "── Syncing sources (finances update) ──"
uv run finances update || echo "finances update failed — opening the viewer on existing data."
echo ""

# Open the browser once the server has had a moment to bind.
( sleep 2; open "$URL" >/dev/null 2>&1 || true ) &

echo "Starting Finances viewer at ${URL}"
echo "Press Ctrl-C (or close this window) to stop — report.html is refreshed on exit."
uv run finances serve --port "$PORT"
```

- [ ] **Step 2: Automated syntax check**

Run: `bash -n finances.command`

Expected: no output, exit code 0. (Check with `echo $?` → `0`.)

- [ ] **Step 3: Verify the file mode and diff scope**

Run: `ls -l finances.command && git diff --stat`

Expected: mode `-rwxr-xr-x` (755 preserved), and the diff touches only `finances.command`.

- [ ] **Step 4: Prove the "update failure must not abort launch" guard and the EXIT trap, non-interactively (no real sync, no browser, no Ctrl-C)**

`finances update` exits 0 even when sources fail, so to exercise the `||` path deterministically, run a throwaway copy whose update invocation is a guaranteed hard failure (an unknown subcommand). The copy must live at the repo root (the script resolves `REPO_DIR` from its own location).

Two hard rules for this step:

- **Never run `./failtest.command` in the foreground.** The script ends in a blocking `uv run finances serve`; in a non-interactive shell a foreground run hangs until the tool timeout kills it, which can strand a listener on the port and leave the untracked `failtest.command` in the working tree.
- **Run the whole check as the single script block below** (background launch → poll the log → SIGINT the server → assert the log → clean up). Shell state does not persist between tool invocations, so `$SCRIPT_PID`, `$PORT`, and the cleanup trap must all live in one block.

The throwaway copy is doctored twice by `sed`: the update call becomes a guaranteed hard failure, and `open "$URL"` becomes `true` (both occurrences) so no browser window pops mid-automation. The port precondition is handled inside the block: if 8765 is already bound (a live viewer), the failtest runs on 8799 via `FINANCES_PORT` instead — reusing the live server would take the early-exit path and prove nothing — and if both ports are bound it aborts and tells you to stop the running server(s) first. Cleanup (remove `failtest.command`, kill any straggling listener) runs unconditionally via a `trap … EXIT`, on success, assertion failure, or timeout.

```bash
set -uo pipefail   # deliberately no -e: assertions and cleanup are explicit control flow
cd "$(git rev-parse --show-toplevel)"

# --- Precondition: pick a port that is actually free ------------------------
PORT=8765
if lsof -nP -iTCP:"$PORT" -sTCP:LISTEN >/dev/null 2>&1; then
  echo "port 8765 is bound (live viewer?) — running the failtest on 8799 instead"
  PORT=8799
  if lsof -nP -iTCP:"$PORT" -sTCP:LISTEN >/dev/null 2>&1; then
    echo "ABORT: 8765 and 8799 are both bound. Stop the running server(s), then re-run this step." >&2
    exit 1
  fi
fi

# --- Throwaway copy: hard-fail the update; neutralize the browser open ------
sed -e 's|uv run finances update |uv run finances definitely-not-a-command |' \
    -e 's|open "$URL"|true|g' \
    finances.command > failtest.command
chmod +x failtest.command

LOG="$(mktemp -t wp5-failtest)"
GUARD='finances update failed — opening the viewer on existing data.'

cleanup() {  # unconditional: success, assertion failure, or timeout
  rm -f failtest.command
  lsof -t -nP -iTCP:"$PORT" -sTCP:LISTEN 2>/dev/null | xargs kill 2>/dev/null
  sleep 1
  lsof -t -nP -iTCP:"$PORT" -sTCP:LISTEN 2>/dev/null | xargs kill -9 2>/dev/null
  [ -n "${SCRIPT_PID:-}" ] && kill -9 "$SCRIPT_PID" 2>/dev/null
  true
}
trap cleanup EXIT

# --- Launch in the BACKGROUND (the script blocks on `finances serve`) -------
FINANCES_PORT="$PORT" ./failtest.command > "$LOG" 2>&1 &
SCRIPT_PID=$!

# --- Poll (max 30s) until the guard fired AND the server is bound -----------
ready=""
for _ in $(seq 1 30); do
  if grep -qF "$GUARD" "$LOG" \
      && lsof -nP -iTCP:"$PORT" -sTCP:LISTEN >/dev/null 2>&1; then
    ready=1
    break
  fi
  sleep 1
done
if [ -z "$ready" ]; then
  echo "FAILTEST FAILED: guard line or listener never appeared. Log ($LOG):" >&2
  cat "$LOG" >&2
  exit 1
fi

# --- Ctrl-C stand-in: SIGINT the server so the EXIT trap fires ---------------
# uvicorn shuts down gracefully, `uv run finances serve` returns, the script
# reaches its own exit, and its `trap regen_report EXIT` runs.
lsof -t -nP -iTCP:"$PORT" -sTCP:LISTEN | xargs kill -INT
for _ in $(seq 1 20); do
  kill -0 "$SCRIPT_PID" 2>/dev/null || break
  sleep 1
done

# --- Assert the observable contract from the log ----------------------------
pass=1
while IFS= read -r needle; do
  grep -qF "$needle" "$LOG" || { echo "MISSING from log: $needle" >&2; pass=0; }
done <<EOF
── Syncing sources (finances update) ──
$GUARD
Starting Finances viewer at http://localhost:${PORT}/
Refreshing report.html …
EOF
if [ "$pass" -ne 1 ]; then
  echo "FAILTEST FAILED — full log ($LOG):" >&2
  cat "$LOG" >&2
  exit 1
fi
echo "FAILTEST OK (log: $LOG)"
```

Expected: the block prints `FAILTEST OK (log: …)` and exits 0. The four asserted log lines prove, in order: the sync banner printed, the doctored "update" crashed but the `||` guard fired (the script did **not** die under `set -e`), the script still reached `Starting Finances viewer …`, and the SIGINT-triggered EXIT trap printed `Refreshing report.html …`. Afterwards verify the cleanup held:

```bash
git status --short            # no failtest.command in the working tree
lsof -nP -iTCP:8765 -sTCP:LISTEN || echo "port free"
lsof -nP -iTCP:8799 -sTCP:LISTEN || echo "port free"
```

Expected: no `failtest.command` line in `git status`, and `port free` for the port(s) the failtest used.

(Note: this flow runs `finances serve` and the read-only `finances html` regen against the real DB — both are read-safe. It never runs a real ingest: the doctored command fails before touching anything. The parts that genuinely need a human — the browser visibly opening, a real Ctrl-C in a Terminal window — are covered by the owner's manual gate in Step 6.)

- [ ] **Step 5: Commit**

```bash
git add finances.command
git commit -m "feat(launcher): sync sources in the foreground before the viewer opens"
```

- [ ] **Step 6: Owner's manual gate (Julio runs this — it performs a real sync)**

Not for the executing agent (read-only-by-default rule): these steps run a live `finances update` against the real DB and are how Julio verifies the WP before marking it complete. Items 1 and 4 are also the human half of Step 4's failtest — they confirm what the non-interactive check cannot observe: the browser actually opening, and a real Ctrl-C driving uvicorn's shutdown followed by the trap's regen line.

1. Double-click `finances.command` in Finder (no server running). Expected: the terminal shows the update summary **first** (Sources / Needs review / Data freshness / Report lines); if needs-review > 0 the line ends with `→ http://localhost:8765/triage`; only then `Starting Finances viewer at http://localhost:8765/` appears and the browser opens.
2. If offline / VPN off: the summary shows `error` rows (Binance gets the VPN hint), the launch still proceeds, the viewer opens on stale data, and the dashboard sync strip shows the stale sources.
3. Double-click again while the server is running. Expected: `Finances viewer already running at http://localhost:8765/ — reusing it.`, browser opens, no second sync, no port error.
4. Ctrl-C (or close the window) in the first terminal. Expected: uvicorn's shutdown logs, then `Refreshing report.html …` from the EXIT trap; `ls -l report.html` shows a fresh mtime.

---

### Task 3: Runbook audit + WP gate

**Files:**
- Audit only (no expected edits): `docs/runbooks/web-viewer-lan.md` — the sole file in `docs/runbooks/`

**Interfaces:**
- Consumes: Task 1's summary contract and Task 2's launcher behavior.
- Produces: grep evidence that no doc under `docs/runbooks/` describes the old two-step flow (open viewer, then sync separately) or the old summary wording, plus a green full test suite.

At plan-writing time (2026-07-11), `docs/runbooks/` contains exactly one file, `web-viewer-lan.md` (iPhone/LAN access). It mentions neither the launcher, `finances update`, `report.html`, nor the two-step flow — so the expected outcome of this task is **zero edits and zero commits**. The greps below make that an evidenced check rather than an assumption.

- [ ] **Step 1: Grep the runbooks for stale flow references**

```bash
grep -rni -e "finances.command" -e "finances update" -e "report.html" -e "sort them" docs/runbooks/
```

Expected: no output, exit code 1 (grep's no-match exit). If a match *does* appear (doc drift since plan-writing), replace the matched sentence with: ``Double-click `finances.command` — it syncs every source first (summary in the terminal), then opens the viewer.`` and commit that one file as `docs(runbooks): point at the one-launcher flow`.

- [ ] **Step 2: Grep the whole repo for the retired wording**

```bash
grep -rn "sort them" finances tests docs/runbooks README.md 2>/dev/null
```

Expected: no output, exit code 1. (The only pre-WP occurrence was `finances/reports/update.py:318`, removed in Task 1.)

- [ ] **Step 3: Full suite**

Run: `uv run pytest -q`

Expected: all tests pass (same green baseline as before this WP, plus the one new test from Task 1).

- [ ] **Step 4: Hand off**

Report Tasks 1-3 done with the command outputs from Steps 1-3 above, and point Julio at Task 2 Step 6 (the live double-click gate). Do **not** mark the work package complete — that's Julio's call (execution rule 3).
