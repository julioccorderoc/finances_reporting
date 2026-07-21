# ADR-012: Local Web Viewer (FastAPI + Jinja + HTMX), Read/Write, LAN-Accessible

**Date:** 2026-05-05
**Status:** Accepted

## 1. Context

The pre-v1 workflow that this project replaces was a hand-edited Google Sheets workbook that supported three distinct activities in one place: (a) browsing transactions with filters and drill-down, (b) visualizing trends, and (c) editing rows (categorize, set rate, mark reconciled). The current v1 stack — Typer CLI plus a destructively re-rendered, read-only Sheets mirror (EPIC-014) — covers (a) and (b) acceptably for terminal-bound use but loses (c) entirely. There is no UI for the daily triage motions (`needs_review` rate-fill, category fixup, transfer pairing confirmation), and the CLI's text tables don't scale past ~50 rows.

The user wants a local-only, mobile-accessible (over home LAN, including iPhone Safari) web app that closes that gap. PRD §10 left web UI as a future extension point; PRD §2 listed it as a v1 non-goal. This ADR upgrades it to an in-scope v1.5 feature without disturbing the existing CLI/ingest contract.

Three architecture options were considered:

1. **Static SPA + sql.js in the browser.** No server. Rejected: writes-back-to-disk are awkward (file picker dance per save), and reimplementing `categorization`/`rates`/`transfers` in JavaScript would be a parallel-logic violation of rule-004 (and rule-005, rule-006).
2. **Streamlit / Marimo.** Single-file Python web apps. Rejected: triage workflows (modal edits, transfer pairing, drill-down navigation) fight the framework's model. Acceptable for read-only dashboards, not for the spreadsheet-replacement scope.
3. **Local Python server + browser frontend.** A FastAPI app importing `finances.domain.*` and `finances.db.repos.*` directly; writes flow through the same Pydantic-validated repo methods the CLI calls. **Selected.**

## 2. Decision

Build a local web viewer under `finances/web/` with the following architectural commitments:

### Server

- **FastAPI** for the HTTP layer. Existing Pydantic v2 domain/report models become FastAPI request/response models with no translation layer (consistent with ADR-009 / rule-009).
- **Sync route handlers.** The repos and report functions are synchronous (`sqlite3` stdlib); FastAPI runs sync handlers on a threadpool. No `aiosqlite` migration.
- **Jinja2** templates served via `fastapi.templating.Jinja2Templates`.
- **uvicorn** run programmatically from a new `finances serve` Typer command.
- **Three route surfaces**:
  - `GET /...` — full HTML pages.
  - `GET /_partial/...` — HTML fragments for HTMX swaps.
  - `GET|POST|PATCH /api/...` — JSON endpoints. This surface is the foundation for the deferred EPIC-016 mobile API; the viewer's HTMX layer does not depend on it.

### Frontend

- **HTMX** for partial-page swaps (filter changes, row re-render after edit, modal load).
- **Alpine.js** for client-only interactivity (modal open/close, expand/collapse, optimistic UI).
- **Chart.js** for charts.
- **Tailwind CSS** via the standalone CLI binary or Play CDN runtime — *no node toolchain*. The build process is `uv pip install -e .`; nothing else.
- **All client libraries vendored locally** under `finances/web/static/vendor/`. The viewer must work fully offline.
- **Card-row layout** (CSS Grid + `subgrid`), not HTML `<table>` elements, for all data lists. This makes the same component render correctly on iPhone Safari (≤640px) and desktop (≥1024px) without separate templates, except for one inherently-grid view (`/monthly` pivot) which has a dedicated mobile template.

### Writes

- All write paths (categorize, set `user_rate`, mark transfer pair) call the existing repo APIs in `finances/db/repos/` and the existing domain functions in `finances/domain/` (`rates.resolve`, `create_transfer`). The viewer adds **zero parallel write logic**. This preserves rule-004 and ensures CLI and viewer cannot diverge.
- After any transaction write, the handler re-runs `rates.resolve(conn, txn)` and stores its `needs_review` verdict. There is no manual `needs_review` toggle in the UI; the flag is fully derived. (See ADR-005 for the resolver chain.)
- Edits in the row modal are **atomic per Save click**, not per-keystroke. One `PATCH /api/transactions/{id}` carries all dirty fields.

### Triage

- A unified `/triage` queue surfaces three issue types — missing rate (`needs_review=1`), missing category (`category_id IS NULL`), high-confidence pair candidates from `BankAnchoredP2pPairing.match()` — sorted oldest-issue-first. Each card carries one or more issue badges; the modal exposes all editable fields for that transaction (or a side-by-side pair-confirm modal for pair candidates). Actions on every modal: `[Skip → bottom of session queue]` `[Cancel]` `[Save & next]`.
- The unified queue is preferred over per-issue tabs to avoid one-off UI special cases and to keep a single component, sort, and skip handler. Adding a future issue type (e.g., EPIC-017 receipt-matching) is a discriminator value plus one modal variant.

### Auth & LAN access

- **Default bind:** `127.0.0.1` (localhost-only). No authentication required in this mode.
- **LAN bind (`--host 0.0.0.0`):** requires a static bearer token, supplied via `--token` flag or `FINANCES_WEB_TOKEN` env var. Token is enforced by an ASGI middleware that accepts `Authorization: Bearer <token>` (API/HTMX) or a `?token=<token>` query parameter on first visit (which is then stored as a cookie). No user model, no login form.
- The CLI prints the URL with the token query string at boot for convenience.
- Self-signed HTTPS is explicitly out of scope; the LAN is a trusted home network.

### Net-worth aggregation (amends ADR-005 for headline display)

The dashboard's "Net worth (USD)" tile aggregates balances across currencies using only the **USDT-derived** rate stream (`binance_p2p_median`, `latest_on_or_before(today, native, USDT)`), treating USD/USDT/USDC accounts as 1:1 with USDT. BCV is **never** consulted for the net-worth tile — if a USDT P2P rate is missing for a needed pair, the tile shows `—` with a small warning indicator. This is a narrower rule than the per-transaction `rates.resolve` chain (which still falls back to BCV) because the dashboard tile is a single headline number whose mixing of rate sources would be misleading. See ADR-005 amendment 2026-05-05 (this date) for the full rule.

## 3. Consequences (The "Why")

### Positive

- The CLI ingest contract is unchanged. The viewer is purely additive.
- All write logic lives in one place (`finances.domain.*`, `finances.db.repos.*`); CLI and viewer cannot drift.
- Pydantic models double as FastAPI DTOs, avoiding boilerplate at every endpoint.
- `/api/...` JSON surface is reused as-is when EPIC-016 (mobile API) lands; no rewrite.
- HTMX + Alpine + Chart.js + Tailwind (CDN/standalone) means **zero Node toolchain** in the build path. The whole stack is `uv pip install -e .`.
- Card-row layout collapses to mobile naturally; one component renders on desktop and iPhone.

### Negative

- Adds `fastapi`, `uvicorn`, `jinja2` as runtime dependencies.
- LAN access without HTTPS is plaintext on the home network. Acceptable trade-off given the scope (personal use, trusted Wi-Fi).
- The token model is a single shared secret; loss = full ledger access. Mitigated by short-lived bind windows and the `127.0.0.1` default.
- Pivot view on `/monthly` requires a dedicated mobile template (single-month list); pivot semantics don't card-collapse.

## 4. Rule Extraction (The "How" for Agents)

**Target File:** `docs/architecture/rules/rule-012-web-viewer-uses-existing-domain.md`

**Injected Constraint:** Any code under `finances/web/` that performs a write must call existing functions in `finances/db/repos/*` or `finances/domain/*`. The viewer may not execute SQL `INSERT`/`UPDATE`/`DELETE` directly, may not implement its own categorization, rate-resolution, or transfer-pairing logic, and may not bypass Pydantic validation at the request boundary. The `needs_review` flag on transactions is derived from `rates.resolve` and must be recomputed on every transaction write — the viewer must not expose a manual toggle. The viewer is a thin HTTP/HTML wrapper over the existing domain; it adds no business logic of its own.

The viewer must default to binding `127.0.0.1`. Binding `0.0.0.0` requires a non-empty bearer token enforced via middleware before any route handler runs.

## Amendment — 2026-07-21: triage ordering, navigation, and durable parking

**Status:** Accepted. Supersedes the queue-ordering and action-row clauses of
the "Triage" subsection (§2 Decision, above) for the `/triage` surface only.
The rest of ADR-012 stands.

**Context.** The original decision optimised for auditability: strict
oldest-issue-first ordering, and a skip that was deliberately session-local so
no deferral could silently become permanent. In practice the queue holds 243
items, of which only 25 need nothing but a category. Chronological interleaving
forces the owner to context-switch between one-click rows and rows requiring
recall of an eight-month-old exchange rate, and the session-local skip is erased
by the Stop-server button that is the designed way to end a session.

**Decision.**

1. **Ordering** is by triage difficulty first, then chronology:
   `(bucket, occurred_at, item_id)` where bucket 0 = missing category only,
   1 = missing a rate, 2 = pair proposal. Within a bucket the original
   oldest-first rule is preserved. `item_id` is a mandatory tiebreak, not a
   preference: 204 of 243 live items share a timestamp, so without it the queue
   order is undefined.
2. **Deferral is durable.** `Skip → bottom of session queue` is replaced by
   `Park`, backed by a `transactions.parked` column. Parked items leave the main
   run and collect in a labelled group. Parking is a triage-queue grouping only;
   it does not alter `needs_review` and does not remove rows from any
   needs-review count.
3. **The action row** becomes `[← →] [Park] [Cancel] [Save & next]`. Saving
   marks an item done in place instead of removing it, so navigation indices
   stay stable.

**Consequences.** Chronological reading of the queue is no longer the default;
`/transactions` remains the date-ordered surface. A parked item can outlive the
condition that caused it to be parked, so the parked group shows its live issue
badges. Ordering is now reproducible across renders, which is a prerequisite for
the `N of M` counter and prev/next navigation.

**References.** `docs/superpowers/specs/2026-07-21-triage-speedrun-design.md`
§3.2, §4, §5.3, §5.5.
