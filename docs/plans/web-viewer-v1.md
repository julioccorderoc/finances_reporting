# Plan — Local Web Viewer v1

**Status:** Active
**Owner:** Julio Cordero
**ADRs:** ADR-012 (this feature) · amends ADR-005 (net-worth USDT-only rule)
**Rules:** rule-012 (this feature) · respects rule-001, rule-004, rule-005, rule-006, rule-009, rule-011

## Goal

A local, LAN-accessible web viewer that replaces the pre-v1 hand-edited Google Sheets workflow: browse + filter + drill-down + visualize + edit-in-place (categorize, set rate, confirm transfer pair). Mobile-friendly via card-row layouts; no Node toolchain.

## Locked decisions (from grilling, 2026-05-04 → 2026-05-05)

| # | Decision |
|---|---|
| Q1 | Scope = D: explorer + dashboard + triage with write-back. |
| Q2 | Architecture = local Python server + browser. |
| Q3 | Backend = **FastAPI** (Pydantic v2 reuse, sync handlers on threadpool, programmatic uvicorn). |
| Q4 | Frontend = **Jinja2 + HTMX + Alpine.js + Chart.js**, all vendored, no Node. Tailwind via standalone CLI or Play CDN. |
| Q5 | 6 pages: `/` `/transactions` `/monthly` `/triage` `/accounts` `/rates`. Sync status as dashboard widget, not a separate page. No "trigger ingest" button. |
| Q6 | `/transactions` cards show date · account · description · amount native · USD · rate-source badge · category · needs-review badge. Sort newest-first. Default range = last 30 days. Modal (not drawer) for detail/edit. Description un-editable in v1. |
| Q7 | Dashboard: 4 KPI tiles (net worth USDT, this-month spend, this-month income, needs-review count) → sync status strip → recent activity (last 10 income/expense txns) → 6-month spend stacked-bar. |
| Q8 | `/monthly` pivot: rows = category (sorted desc), cols = months. Kind tabs: Expense / Income / Net. BCV-fallback shown as overlay. Mobile = single-month list view. |
| Q9 | `/triage` = **unified queue**, oldest-issue-first, type badges (Rate / Category / Pair). Two modal variants. Actions: `[Skip → bottom]` `[Cancel]` `[Save & next]`. `needs_review` is fully derived from `rates.resolve` — no manual toggle. |
| Q10 | `/accounts` = card grid → drills into filtered `/transactions`. `/rates` = small chart + latest-per-pair card list. No edits. |
| Q11 | Charts library = **Chart.js**, vendored. |
| Q12 | Layout = `finances/web/{app.py, deps.py, auth.py, routers/, templates/, static/}`. |
| Q13 | API surfaces = HTML pages at `/...`, HTMX fragments at `/_partial/...`, JSON at `/api/...`. |
| Q14 | Auth = `127.0.0.1` default no-auth; `0.0.0.0` requires `--token` / `FINANCES_WEB_TOKEN`. Bearer or cookie. |
| Q15 | Launch = `finances serve --host 127.0.0.1 --port 8765 [--open] [--token]`. |
| Q16 | Tests = pytest + FastAPI TestClient, in-memory SQLite. ≥75% coverage on web layer. Tests precede impl per rule-011. |
| Q17 | New: ADR-012 + rule-012. Amend: ADR-005 (USDT-only for net-worth tile). |
| Q18 | Build = 5 phases below. |

## Net-worth math (locked)

```
For each active account a:
    if a.currency in {"USD", "USDT", "USDC"}: rate = Decimal(1)
    else:
        rate = rates_repo.latest_on_or_before(today, a.currency, "USDT", source="binance_p2p_median")
        if rate is None: contribution = None  # tile shows "—" + warning
    contribution = a.balance_native * rate

net_worth_usdt = sum(contributions where not None)
```

If any contribution is None → the tile shows the partial sum AND a warning chip listing missing pairs.

## Page → data → endpoint matrix

| Page | Data source | HTML route | Fragment route | JSON route |
|---|---|---|---|---|
| `/` | `balances.get_balances`, `rates_repo`, `monthly.build_report`, `transactions_repo`, `import_runs` | `GET /` | `GET /_partial/dashboard/sync-status` | `GET /api/dashboard/kpis`, `GET /api/dashboard/recent`, `GET /api/dashboard/spend-trend` |
| `/transactions` | `transactions_repo` (filtered SQL), `categories_repo`, `rates.resolve` | `GET /transactions` | `GET /_partial/transactions/list`, `GET /_partial/transactions/{id}/card`, `GET /_partial/transactions/{id}/modal` | `GET /api/transactions`, `GET /api/transactions/{id}`, `PATCH /api/transactions/{id}` |
| `/monthly` | `monthly.build_report` | `GET /monthly` | `GET /_partial/monthly/pivot`, `GET /_partial/monthly/chart`, `GET /_partial/monthly/mobile` | `GET /api/monthly` |
| `/triage` | `needs_review.get_needs_review` + `transactions WHERE category_id IS NULL` + `BankAnchoredP2pPairing.match()` | `GET /triage` | `GET /_partial/triage/queue`, `GET /_partial/triage/{id}/modal`, `GET /_partial/triage/pair/{deposit_id}/{sell_id}/modal` | `GET /api/triage`, `POST /api/transfers` (pair confirm) |
| `/accounts` | `balances.get_balances` | `GET /accounts` | — | `GET /api/accounts` |
| `/rates` | `rates_repo` | `GET /rates` | `GET /_partial/rates/chart` | `GET /api/rates` |

## Package layout

```
finances/web/
├── __init__.py
├── app.py                          # create_app(settings) -> FastAPI
├── deps.py                         # get_conn (per-request sqlite3.Connection)
├── auth.py                         # BearerTokenMiddleware (skip if 127.0.0.1)
├── settings.py                     # WebSettings (Pydantic): host, port, token
├── services/
│   ├── __init__.py
│   ├── dashboard.py                # tile + chart aggregations (calls existing reports)
│   ├── transactions_query.py       # filter parsing + paginated query
│   ├── triage.py                   # builds unified queue (calls existing modules)
│   └── net_worth.py                # USDT-only aggregation per ADR-005 amendment
├── routers/
│   ├── __init__.py
│   ├── pages.py                    # full HTML pages (Jinja render)
│   ├── partials.py                 # HTMX fragment endpoints
│   └── api.py                      # JSON endpoints
├── templates/
│   ├── base.html                   # layout, vendored JS, viewport meta
│   ├── _macros.html                # shared Jinja macros (kpi tile, badge, etc.)
│   ├── partials/
│   │   ├── card_transaction.html   # the canonical row card (used everywhere)
│   │   ├── modal_transaction.html  # edit modal (category, user_rate)
│   │   ├── modal_pair.html         # pair-confirm modal
│   │   ├── sync_status_strip.html
│   │   └── ...
│   └── pages/
│       ├── dashboard.html
│       ├── transactions.html
│       ├── monthly.html
│       ├── monthly_mobile.html
│       ├── triage.html
│       ├── accounts.html
│       └── rates.html
└── static/
    ├── vendor/
    │   ├── htmx.min.js             # vendored, version pinned
    │   ├── alpine.min.js
    │   └── chart.umd.min.js
    ├── css/
    │   └── app.css                 # local styles (subgrid card layout, etc.)
    └── js/
        └── app.js                  # tiny app glue (chart instantiation, etc.)
```

## CLI integration

`finances/cli/main.py` adds:

```python
@app.command("serve")
def serve_cmd(
    host: str = typer.Option("127.0.0.1", "--host"),
    port: int = typer.Option(8765, "--port"),
    open_browser: bool = typer.Option(False, "--open"),
    token: Optional[str] = typer.Option(None, "--token", envvar="FINANCES_WEB_TOKEN"),
) -> None:
    ...
```

If `host != "127.0.0.1"` and no token is set → exit non-zero with a clear error.

## Build phases

### Phase 1 — Foundation (sequential, blocks all others)

**Files (single-agent):**

- `pyproject.toml` (add `fastapi`, `uvicorn[standard]`, `jinja2`, `python-multipart` deps)
- `finances/web/__init__.py`
- `finances/web/settings.py` — Pydantic `WebSettings`
- `finances/web/app.py` — `create_app(settings)` factory; mounts `/static`; includes routers (empty stubs)
- `finances/web/deps.py` — `get_conn` yields a per-request `sqlite3.Connection` from the configured DB path
- `finances/web/auth.py` — `BearerTokenMiddleware`
- `finances/web/routers/{pages,partials,api}.py` — empty `APIRouter` stubs
- `finances/web/templates/base.html` — minimal layout: `<head>` with viewport meta, vendored JS `<script>` tags, Tailwind Play CDN, `<body>` with one `{% block content %}` and a placeholder header
- `finances/web/static/vendor/{htmx.min.js,alpine.min.js,chart.umd.min.js}` — fetched and committed (with version pin in a `VERSIONS.txt`)
- `finances/web/static/css/app.css` — empty stub
- `finances/cli/main.py` — `serve` command
- `tests/test_web_app.py` — TestClient asserts: app boots, `/static/vendor/htmx.min.js` is served, auth middleware allows `127.0.0.1` without token, `0.0.0.0` rejects without token, accepts with bearer
- Update `docs/roadmap.md` — add **Wave 5** with EPIC-022 (this phase) marked Complete on landing

**Definition of Done (Phase 1):**
- `uv pip install -e .` succeeds with new deps
- `finances serve --port 8765` boots; `curl http://localhost:8765/` returns the base layout
- `pytest tests/test_web_app.py` green
- ADR-012, rule-012, this plan committed

### Phase 2 — Read-only pages (parallel, 4 agents)

Foundation must be merged first. Then four agents work in parallel on disjoint files:

| Agent | Page | Owns (files) | Reuses (read-only) |
|---|---|---|---|
| **2a** | `/` Dashboard | `services/dashboard.py`, `services/net_worth.py`, `templates/pages/dashboard.html`, `templates/partials/sync_status_strip.html`, `templates/partials/kpi_tile.html`, `routers/pages.py::dashboard`, `routers/api.py::dashboard_*`, `tests/web/test_dashboard.py` | `balances.get_balances`, `monthly.build_report`, `rates_repo`, `transactions_repo`, `import_state` |
| **2b** | `/transactions` (read only — modal in Phase 3) | `services/transactions_query.py`, `templates/pages/transactions.html`, `templates/partials/card_transaction.html`, `templates/partials/transactions_filters.html`, `routers/pages.py::transactions`, `routers/api.py::transactions_list`, `routers/partials.py::transactions_*`, `tests/web/test_transactions_read.py` | `transactions_repo`, `categories_repo`, `rates.resolve` (read-only) |
| **2c** | `/monthly` | `templates/pages/monthly.html`, `templates/pages/monthly_mobile.html`, `templates/partials/monthly_pivot.html`, `templates/partials/monthly_chart.html`, `routers/pages.py::monthly`, `routers/api.py::monthly`, `tests/web/test_monthly.py` | `monthly.build_report` |
| **2d** | `/accounts` + `/rates` | `templates/pages/{accounts,rates}.html`, `templates/partials/{account_card,rates_chart}.html`, `routers/pages.py::{accounts,rates}`, `routers/api.py::{accounts,rates}`, `tests/web/test_accounts_rates.py` | `balances.get_balances`, `rates_repo` |

**Coordination:** all four agents share `templates/base.html` (Phase 1 output), `templates/_macros.html`, and `static/css/app.css`. To avoid collisions, the **card-row CSS** for `card_transaction.html` is owned by Agent 2b and merged first; agents 2a, 2c, 2d only consume it. Macros are append-only.

**Definition of Done (Phase 2):**
- All four pages render with real data from the working DB
- Filters on `/transactions` work via HTMX (no full page reloads)
- Pivot drill-down on `/monthly` navigates to filtered `/transactions`
- Charts render on `/`, `/monthly`, `/rates`
- Mobile breakpoints verified at 375px, 768px, 1024px

### Phase 3 — Transaction edit modal (sequential, single agent)

**Files:**
- `templates/partials/modal_transaction.html`
- `routers/partials.py` — `GET /_partial/transactions/{id}/modal`, `GET /_partial/transactions/{id}/card`
- `routers/api.py` — `PATCH /api/transactions/{id}` (calls `transactions_repo.update`, then `rates.resolve`, returns updated card HTML or JSON depending on `Accept`)
- `services/transactions_write.py` — encapsulates the "patch + re-resolve" sequence
- `tests/web/test_transactions_write.py`

**DoD:** click a transaction card → modal opens → edit category and/or user_rate → Save → row swaps in place with new USD value, rate source, and (re-derived) needs_review badge. Description, amount, account, source, source_ref are not editable.

### Phase 4 — Triage page + pair-confirm modal (sequential, single agent)

**Files:**
- `services/triage.py`
- `templates/pages/triage.html`
- `templates/partials/{triage_card,modal_pair,triage_empty}.html`
- `routers/pages.py::triage`
- `routers/partials.py::triage_*`
- `routers/api.py::{triage_list,transfers_create}`
- `tests/web/test_triage.py`

**DoD:** queue lists rate/category/pair items oldest-first; transaction cards open the Phase 3 modal with `Save & next`; pair candidates open pair-confirm modal that calls `create_transfer(...)` on confirm. Skip is session-local.

### Phase 5 — Mobile polish + LAN docs (sequential, small)

**Files:**
- `static/css/app.css` — mobile refinements after on-device testing
- `docs/runbooks/web-viewer-lan.md` — how to bind `0.0.0.0`, set token, find your `<host>.local` from iPhone, common Bonjour gotchas
- README touch-up referencing `finances serve`

**DoD:** the viewer is genuinely usable from iPhone Safari on the home Wi-Fi for browse + edit + triage.

## Test strategy (per rule-011)

- **Unit tests** for `services/*` modules — pure functions of `Connection` + filter parameters. Fast.
- **Route tests** via `TestClient` — assert response status, content-type, and key markers in HTML (`assert b'data-card-row' in resp.content`).
- **HTMX tests** — `TestClient.get(..., headers={"HX-Request": "true"})` to assert fragment-only responses.
- **Auth tests** — middleware behavior at the `127.0.0.1` boundary and `0.0.0.0` boundary (with and without token).
- **Snapshot tests for templates:** skip — too brittle. Assert key elements present.
- Tests are committed *before* implementation in each phase per rule-011. Coverage gate: ≥75% on `finances/web/`.

## Out of scope for v1 (deferred)

- `/earn` page (waiting on EPIC-007 producing snapshots)
- Manual `find pair` from `/transactions` modal (only auto-proposals on `/triage` for v1)
- Bulk-select / bulk-apply
- Keyboard shortcuts
- "Run sync" button (CLI-only)
- HTTPS / self-signed certs
- A `note` or `flag` column on transactions for non-rate review reasons (future schema change if real friction emerges)
- Editing `description`, `amount`, `account`, `occurred_at`, `source`, `source_ref` (audit-trail / dedup-key invariants)

## Roadmap delta

Add **Wave 5** to `docs/roadmap.md`:

- **EPIC-022 — Web Viewer Foundation** (Phase 1 above)
- **EPIC-023 — Read-Only Web Pages** (Phase 2 above)
- **EPIC-024 — Transaction Edit Modal** (Phase 3 above)
- **EPIC-025 — Triage Page + Pair Confirm** (Phase 4 above)
- **EPIC-026 — Mobile Polish & LAN Docs** (Phase 5 above)

Wave 5 depends on Wave 3 completion (already met).
