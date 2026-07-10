# Thing 4 — `finances html`: the one static report file

## Context (assume no prior knowledge)

Personal finances ledger, SQLite, Typer CLI, plus an existing FastAPI+Jinja+HTMX
web viewer under `finances/web/` (`finances serve`). The owner's #1 wish:
**one self-contained HTML file he can double-click anytime** — no server, no
internet, still works if Python breaks next year. Read-only by design (a static
file cannot write to the DB); edits happen in the viewer.

## Task

New command `finances html [--output PATH]` (default `report.html` at repo
root, already gitignored) that renders a single self-contained file.

## Architecture (decided — don't relitigate)

- **Reuse the viewer's query services** (`finances/web/services/` — dashboard,
  monthly_view, net_worth, rates_view, transactions_query). Zero duplicated
  math: report numbers must equal viewer numbers by construction.
- New module `finances/reports/html_export.py` + one Jinja template. The
  template is standalone (inline CSS, inline JS, data embedded) — it does NOT
  extend the viewer's base templates.
- Charts: vendored Chart.js already exists under the web static dir — inline
  its source into the file (`<script>…</script>`). No CDN, no `http(s)://`
  URLs anywhere in the output. Data as embedded JSON.

## Content (sections, top to bottom)

1. Header: net worth in USD (headline rate = USDT/P2P per the resolver — never
   BCV), per-account balances, generated-at timestamp + data freshness (last
   transaction date per source; stale >35 days → visible warning).
2. Monthly income / expense / net — last 12 months, bar chart + numbers.
3. Category breakdown — current month + previous month.
4. Rate trend — BCV vs P2P, last 90 days, line chart.
5. Needs-review count (big and red if >0) + 20 most recent transactions.

Layout rule (standing owner preference): CSS-grid card-rows, **no `<table>`**
for data lists. Match the viewer's existing look (`app.css`) loosely; light +
dark via `prefers-color-scheme`.

## Rules

1. TDD: tests first for the export service (numbers match the corresponding
   web service outputs on a seeded fixture DB; output contains no external
   URLs; file is valid standalone HTML).
2. Auto-regen: after every **successful non-dry-run** ingest CLI command
   (binance, provincial, bcv, p2p-rates, cash add, backfill), regenerate the
   default report.html. Failure to regen must not fail the ingest — warn only.
3. Read-only: the export path must never write to the DB (SELECTs only).
4. Keep it one file on disk; no assets directory.

## Gate

- [ ] `uv run finances html` produces report.html; opens in a browser with
      Wi-Fi off; charts render; no console errors about blocked requests.
- [ ] Numbers spot-checked equal to the running viewer for: net worth, current
      month net, needs_review count.
- [ ] `grep -c "https\?://" report.html` → 0 (data URIs excluded).
- [ ] An ingest run regenerates the file (mtime changes).
- [ ] `uv run pytest -q` green, new code covered.

## Out of scope

Launcher script, `finances update` wrapper, offline CSS for the *viewer*
(all Thing 5), any write-back, PDF export.
