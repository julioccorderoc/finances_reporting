# WP2 — Background rate refresh on viewer start

Paste this whole file as the opening prompt of a fresh session.

---

Work in an isolated git worktree. Create it **before your first commit** —
several sessions are active in this repo and a shared working tree has
already been clobbered once (a concurrent `git reset` discarded staged work).
Do not work directly in `/Users/juliocordero/Documents/finances_reporting`.

Read `CLAUDE.md` first. Follow rule-011 (TDD: test commit precedes impl
commit), rule-009 (Pydantic at trust boundaries), and rule-004 (no forked
ingest logic — sequence the production functions, never reimplement them).

## Why this exists

ADR-016 capped the `binance_p2p_median`
tier at 14 days. Before that cap, a stale median carried forward forever and
silently converged on BCV. Now it expires instead — which makes refresh
frequency a **correctness requirement**, not a nicety. Today `rates` holds 8
median rows spanning 2026-04-27 to 2026-08-02, because `finances update` has
only ever been run by hand.

## What to build

When the web viewer starts, refresh stale rate sources in the background.

**Decisions already locked by the owner — do not relitigate:**

- Trigger: viewer startup, gated on staleness. No cron, no launchd, no daemon.
- Non-blocking: the page must render immediately. On the last real run bcv
  took ~2s, p2p ~2s, binance ~6s; ten seconds of blocked page load is not
  acceptable.
- Threshold: refresh when the newest successful run for a source is older
  than 12 hours.
- Binance failing without a VPN is a normal outcome, not an error state that
  blocks anything.

**Shape:**

1. New `finances/web/services/refresh.py`.
   - `maybe_refresh(...)` reads the newest `import_runs.finished_at` per
     source; if stale, dispatches a worker thread.
   - The worker calls the **existing** step functions in
     `finances/reports/update.py` (`_step_bcv`, `_step_p2p`, `_step_binance`).
     Do not copy their logic. If they need to be made reusable, refactor them
     in place rather than duplicating.
   - The worker opens its **own** sqlite connection. Do not hand it a
     request-scoped one.
   - Module-level lock plus an in-flight flag so a page reload cannot
     double-fire. Note that `finances serve` runs under a watchfiles
     supervisor that restarts the child process on file edits — the staleness
     gate must absorb those restarts.
2. Hook into the `lifespan` handler in `finances/web/app.py` (~line 84).
3. Surface status through the existing sync-status strip
   (`partials/sync_status_strip.html`, `/_partial/dashboard/sync-status`,
   `services/dashboard.build_sync_status`). Have it poll via htmx while a
   refresh is in flight. Reuse `reports/update.VPN_HINT` for the Binance
   geo-block case.

**Explicitly out of scope:** provincial ingest (that is WP3), any change to
rate resolution (WP1 owns that), BCV historical backfill (deliberately cut —
the homepage scraper only publishes today's rate, and BCV is reference-only
with zero transactions resolving to it).

## Testing

- Staleness gate: fresh data → no dispatch; stale data → exactly one dispatch.
- Concurrency: two near-simultaneous starts dispatch once, not twice.
- Isolation: a failing Binance step must not prevent bcv/p2p from completing,
  and must not raise into the lifespan.
- Do not hit the network in tests — inject clients/fakes the way
  `tests/test_ingest_p2p_rates.py` and `tests/test_update_ritual.py` already do.

## Gotchas that have already cost time here

- The vendored `finances/web/static/css/tailwind.css` is a fixed extract with
  **no build step**. Only use classes already present in it; anything new goes
  in `static/css/app.css` by hand.
- `{{ x | tojson }}` inside a double-quoted HTML attribute truncates the JS
  while every server-side test still passes. Assert on the exact rendered
  output, and check it in a real browser.
- `rtk` strips pytest's summary line — you will not see "N passed". Count dots
  or use `--collect-only`.
- Run the **full** suite before declaring done, not just your new file.
