# Wave 2 — Thing 2: Saved filter views

**Goal:** save the current `/transactions` filter combination under a name; recall it with one tap from any device (Mac or iPhone LAN).

**Run in a fresh session. TDD per rule-011 (test commit before impl commit). Tests via `uv run pytest -q`, never against the real `finances.db`. Pydantic v2 at every boundary (rule-009); web writes only via repos (rule-012 spirit). Vendored htmx/Alpine only. You never mark this complete — Julio does.**

## Design (locked with Julio 2026-07-13)

- Storage = DB table (works across devices; SQLite = truth): migration `saved_views` — `id INTEGER PK`, `name TEXT NOT NULL UNIQUE`, `query_string TEXT NOT NULL` (the raw `/transactions` querystring, no leading `?`), `created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP`. **Use the next free migration prefix — check `finances/db/migrations/` first (009 expected; 008 = notes).**
- Pydantic `SavedView` model in `finances/domain/models.py`; repo `finances/db/repos/saved_views.py` with `insert`, `list_all`, `get_by_id`, `delete` (accept/return Pydantic only).
- UI on `/transactions` (filters area):
  - Chip row of saved views above the filter form; clicking a chip navigates to `/transactions?<query_string>`; an `×` on each chip deletes it (with `hx-confirm`) and shows a `show-toast` success.
  - "Save view" control: small inline form (name input + button) that posts the CURRENT querystring (read from `window.location.search` via a hidden input set by Alpine) — duplicate name → 422 with error toast.
- Endpoints (HTMX partials, follow `finances/web/routers/partials.py` house style): `GET /_partial/views` (chip row), `POST /_partial/views` (create; form fields `name`, `query_string`), `POST /_partial/views/{id}/delete`. Responses re-render the chip row; toasts via `HX-Trigger` JSON (WP2 contract: `{"toast": {"level": ..., "message": ...}}`).
- Chips are styled like the WP6 `.choice-chip` pattern — reuse/extend it in `app.css`; if any NEW Tailwind utility class is introduced in templates, rebuild the vendored sheet per `tailwind/README.md` and commit it (this was forgotten once — grep the compiled css for your new classes before the gate).

## Out of scope

Renaming/reordering views, saved views for `/monthly`, export/import.

## Gates (all must pass)

- `uv run pytest -q` — full suite green.
- New tests: repo round-trip (insert/list/get/delete, Pydantic in/out, UNIQUE name violation); endpoint tests — create renders chip with name, duplicate name → 422 + error toast in `HX-Trigger`, delete removes chip + success toast; chip `href` reproduces the exact saved querystring.
- Manual (Julio): save a view on a filtered list, open it from the iPhone, delete it.
