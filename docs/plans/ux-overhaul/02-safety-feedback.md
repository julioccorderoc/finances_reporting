# WP2 Safety and Feedback Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Kill the silent category-wipe bug in both edit modals, add a toast feedback system (success on save, error on any failed HTMX request), and open modals with the category control focused.

**Architecture:** Client-side dirty-tracking with the vendored Alpine.js in the two edit modals so `set_category`/`set_user_rate` submit `true` only when the user actually touched that field; clearing a category becomes an explicit "× remove category" button. The server write path (`apply_edit` honoring the `set_*` flags) is untouched. A window-level toast pipeline in `base.html` converts JSON `HX-Trigger` payloads and `htmx:responseError` events into visible toasts; the three save endpoints in `finances/web/routers/partials.py` emit the toast payload alongside their existing `closeModal` trigger.

**Tech Stack:** FastAPI + Jinja2 partials, vendored htmx + Alpine.js (`finances/web/static/vendor/`), plain CSS in `finances/web/static/css/app.css`, pytest with the existing `tests/web/conftest.py` fixtures (`seeded_web_db`, `web_client_factory`).

## Global Constraints

- TDD per rule-011 / CLAUDE.md execution rule 5: each task's **test commit lands before its implementation commit** (`test(scope): ...` then `feat(scope): ...` or `fix(scope): ...`).
- Run tests with `uv run pytest -q <path>` — never bare `pytest`.
- Pydantic v2 at all trust boundaries (rule-009); `TransactionEditRequest` stays the only edit payload model; repos accept/return Pydantic models, never raw dicts.
- The web viewer's ONLY transaction write path is `transactions_repo.update()` (rule-012). This WP adds **no new write paths and no new UPDATE SQL** — server behavior in `finances/web/services/transactions_write.py::apply_edit` stays exactly as-is.
- No new dependencies, no CDN assets. Frontend uses only the already-vendored htmx + Alpine.js under `finances/web/static/vendor/`.
- The offline Tailwind sheet (`/static/css/tailwind.css`) is compiled from the utility classes already in use — do NOT introduce new Tailwind utility classes; new styling goes into `finances/web/static/css/app.css`.
- Toast contract (shared interface, consumed by WP4): `<div id="toast-host">` in `base.html`; server-side saves send an `HX-Trigger` response header whose value is JSON containing `{"toast": {"level": "success", "message": "Saved"}}` (level is `"success"` or `"error"`); a global `htmx:responseError` listener in `base.html` raises an error toast with the server's error detail.
- `needs_review` is derived by the rate resolver (`apply_edit` re-runs `rates.resolve`); it is never exposed as a manual toggle.
- Tests must never touch the real `finances.db` — use the tmp-DB fixtures from `tests/web/conftest.py` (`web_db`, `seeded_web_db`, `web_client_factory`).
- Real expense amounts are NEGATIVE; the `seeded_web_db` fixture wrongly stores them positive. New seed rows added by this plan use genuine negative amounts where an expense is involved (the Binance sell leg).
- Never run `finances ingest`/`cash`/`backfill`/`sync` as part of this work. Read-only SELECTs against tmp test DBs are fine.
- Agents never mark the work package Complete; Julio does (execution rule 3).

**Sequencing note:** Task 1 must land before Task 2. Task 2 switches the `HX-Trigger` header from a plain comma list to JSON, and the current `base.html` listener comma-splits the raw header — Task 1 upgrades that parser (with a plain-string fallback kept for the skip endpoint, which stays `HX-Trigger: closeModal`).

---

### Task 1: Toast infrastructure in base.html + app.css

**Files:**
- Create: `tests/web/test_safety_feedback.py`
- Modify: `finances/web/templates/base.html:21-31` (body tag listeners) and `:50-56` (add toast host after the modal host)
- Modify: `finances/web/static/css/app.css` (append toast styles at end of file, after the `=== END Phase 5 mobile polish ===` marker)

**Interfaces:**
- Consumes: nothing new — existing `@htmx:after-on-load.window` / `@close-modal.window` pattern in `base.html`, vendored Alpine.
- Produces (Tasks 2-4 and WP4 rely on these exact names):
  - `<div id="toast-host">` element in `base.html`.
  - Window `CustomEvent` named `show-toast` with `detail = {level: "success"|"error", message: string}` — dispatching it from anywhere shows a toast.
  - `base.html` body listener that parses the `HX-Trigger` response header as JSON first (keys → events; `toast` key → `show-toast` dispatch), falling back to the legacy comma-separated plain form.
  - Global `@htmx:response-error.window` listener that dispatches an error `show-toast` using the JSON `detail` field of the failed response (falls back to `Error <status>` / `Request failed`).
  - CSS classes `.toast-host`, `.toast`, `.toast-success`, `.toast-error` in `app.css`.

- [ ] **Step 1: Write the failing tests**

Create `tests/web/test_safety_feedback.py` with exactly:

```python
"""WP2 — safety + feedback tests (ux-overhaul, docs/plans/ux-overhaul/00-design.md §2).

Covers:

* base.html toast infrastructure (toast host div, JSON HX-Trigger
  parsing, htmx:responseError listener, show-toast plumbing),
* HX-Trigger toast JSON on the edit / triage-edit / pair-confirm POSTs,
* edit-modal dirty tracking (an untouched select must NOT clear the
  category), the explicit "remove category" control, and autofocus on
  the category control — for both modal_transaction.html and
  modal_transaction_triage.html.

House style notes: template behavior is pinned via string markers on the
rendered partials (same approach as tests/web/test_transactions_write.py),
endpoint semantics via form POSTs + repo re-reads. All DB access goes
through the tmp-DB fixtures in tests/web/conftest.py — never the real
finances.db.
"""

from __future__ import annotations

import sqlite3


# ---------------------------------------------------------------------------
# Task 1 — base.html toast infrastructure.
# ---------------------------------------------------------------------------


def test_base_html_has_toast_host(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    client = web_client_factory()
    resp = client.get("/")
    assert resp.status_code == 200
    assert 'id="toast-host"' in resp.text


def test_base_html_parses_json_hx_trigger_and_dispatches_show_toast(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    """The <body> listener must parse JSON HX-Trigger headers (not only the
    legacy comma list) and re-dispatch closeModal / toast as window events."""
    client = web_client_factory()
    body = client.get("/").text
    assert "JSON.parse" in body
    assert "show-toast" in body
    assert "close-modal" in body  # legacy close path must survive


def test_base_html_has_htmx_response_error_listener(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    client = web_client_factory()
    body = client.get("/").text
    assert "htmx:response-error" in body
    # The listener surfaces the server's error body (JSON detail field).
    assert "responseText" in body


def test_app_css_has_toast_styles(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    client = web_client_factory()
    resp = client.get("/static/css/app.css")
    assert resp.status_code == 200
    assert ".toast-host" in resp.text
    assert ".toast-success" in resp.text
    assert ".toast-error" in resp.text
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest -q tests/web/test_safety_feedback.py`

Expected: `4 failed` — each with an `AssertionError` on its marker (`assert 'id="toast-host"' in resp.text`, `assert 'JSON.parse' in body`, `assert 'htmx:response-error' in body`, `assert '.toast-host' in resp.text`).

- [ ] **Step 3: Commit the failing tests**

```bash
git add tests/web/test_safety_feedback.py
git commit -m "test(web): toast host, error listener, HX-Trigger JSON parsing markers"
```

- [ ] **Step 4: Implement — base.html listeners**

In `finances/web/templates/base.html`, replace the `<body ...>` opening tag (currently lines 21-31):

```html
  <body
    class="min-h-screen bg-slate-50 text-slate-900"
    x-data="{}"
    @close-modal.window="document.getElementById('tx-modal-host').replaceChildren()"
    @htmx:after-on-load.window="
      const raw = $event.detail && $event.detail.xhr && $event.detail.xhr.getResponseHeader('HX-Trigger');
      if (raw) {
        let names = [];
        let toast = null;
        try {
          const parsed = JSON.parse(raw);
          names = Object.keys(parsed);
          toast = parsed.toast || null;
        } catch (err) {
          names = raw.split(',').map(s => s.trim());
        }
        if (names.includes('closeModal')) {
          window.dispatchEvent(new CustomEvent('close-modal'));
        }
        if (toast) {
          window.dispatchEvent(new CustomEvent('show-toast', { detail: toast }));
        }
      }
    "
    @htmx:response-error.window="
      const xhr = $event.detail && $event.detail.xhr;
      let message = 'Request failed';
      if (xhr) {
        message = 'Error ' + xhr.status;
        try {
          const parsed = JSON.parse(xhr.responseText);
          if (parsed && parsed.detail) {
            message = typeof parsed.detail === 'string' ? parsed.detail : JSON.stringify(parsed.detail);
          }
        } catch (err) {}
      }
      window.dispatchEvent(new CustomEvent('show-toast', { detail: { level: 'error', message: message } }));
    "
  >
```

(Why: `HX-Trigger` becomes a JSON object in Task 2 — `{"closeModal": true, "toast": {...}}` — which the old `raw.split(',')` exact-match parser cannot read. JSON is tried first; the comma-split fallback keeps the triage **skip** endpoint working, which continues to send the plain string `closeModal`. FastAPI's `HTTPException` renders `{"detail": "..."}`, which is what the error listener extracts; Pydantic-422 list details fall back to `JSON.stringify`.)

- [ ] **Step 5: Implement — toast host in base.html**

Still in `finances/web/templates/base.html`, directly after `<div id="tx-modal-host"></div>` (currently line 55), insert:

```html
    {# Toast host (WP2 / ux-overhaul §2). Server saves put a "toast" key
       in the JSON HX-Trigger header; the <body> listener above re-emits
       it as the window-level "show-toast" CustomEvent, and the global
       htmx:response-error listener raises error toasts. Toasts
       auto-dismiss after 4 s. #}
    <div
      id="toast-host"
      class="toast-host"
      aria-live="polite"
      x-data="{ toasts: [], nextId: 1 }"
      @show-toast.window="
        const t = {
          id: nextId++,
          level: ($event.detail && $event.detail.level === 'error') ? 'error' : 'success',
          message: ($event.detail && $event.detail.message) || 'Done'
        };
        toasts.push(t);
        setTimeout(() => { toasts = toasts.filter(x => x.id !== t.id) }, 4000);
      "
    >
      <template x-for="t in toasts" :key="t.id">
        <div
          class="toast"
          :class="t.level === 'error' ? 'toast-error' : 'toast-success'"
          role="status"
          x-text="t.message"
        ></div>
      </template>
    </div>
```

- [ ] **Step 6: Implement — toast styles in app.css**

Append to the end of `finances/web/static/css/app.css` (after the `/* === END Phase 5 mobile polish === */` marker):

```css
/* === toasts (WP2 / ux-overhaul §2) ====================================
 * Fixed stack, bottom-right on desktop, full-width bottom on phones.
 * Only custom classes here — the compiled Tailwind sheet is static, so
 * no new utility classes may be introduced in templates.
 */
.toast-host {
  position: fixed;
  bottom: 1rem;
  right: 1rem;
  z-index: 100;                 /* above .tx-modal-overlay (z-index 50) */
  display: flex;
  flex-direction: column;
  gap: 0.5rem;
  align-items: flex-end;
  pointer-events: none;         /* host never blocks clicks... */
}

.toast {
  max-width: 22rem;
  padding: 0.625rem 1rem;
  border-radius: 0.5rem;
  font-size: 0.875rem;
  color: #ffffff;
  box-shadow: 0 8px 24px -8px rgba(15, 23, 42, 0.4);
  pointer-events: auto;         /* ...but toasts themselves do */
}

.toast-success { background: #0f766e; }  /* teal-700 */
.toast-error   { background: #be123c; }  /* rose-700 */

@media (max-width: 640px) {
  .toast-host {
    left: 1rem;
    right: 1rem;
    align-items: stretch;
  }
  .toast {
    max-width: 100%;
  }
}
/* === END toasts === */
```

- [ ] **Step 7: Run tests to verify they pass**

Run: `uv run pytest -q tests/web/test_safety_feedback.py`

Expected: `4 passed`

- [ ] **Step 8: Regression-check the rest of the web suite**

Run: `uv run pytest -q tests/web`

Expected: all pass, `0 failed` (the base.html `close-modal` path and modal-host markers asserted by `tests/web/test_transactions_write.py::test_base_html_has_modal_host_div` are preserved).

- [ ] **Step 9: Commit the implementation**

```bash
git add finances/web/templates/base.html finances/web/static/css/app.css
git commit -m "feat(web): toast system — host div, Alpine component, htmx error listener"
```

---

### Task 2: HX-Trigger toast JSON on edit / triage-edit / pair-confirm endpoints

**Files:**
- Modify: `finances/web/routers/partials.py:14-16` (imports), `:197-201` (add helper after `_parse_form_bool`), `:291` (edit), `:423` (triage edit), `:509` (pair confirm)
- Test: `tests/web/test_safety_feedback.py` (append)

**Interfaces:**
- Consumes (from Task 1): `base.html` JSON `HX-Trigger` parsing + `show-toast` window event.
- Produces (WP4 relies on these exact shapes):
  - `_hx_trigger_json(*events: str, toast_message: str) -> str` — module-private helper in `finances/web/routers/partials.py` returning e.g. `{"closeModal": true, "toast": {"level": "success", "message": "Saved"}}`.
  - `POST /_partial/transactions/{txn_id}/edit` → `HX-Trigger` = JSON with keys `closeModal` + `toast` (message `"Saved"`).
  - `POST /_partial/triage/{txn_id}/edit` → JSON with keys `closeModal`, `advanceQueue` + `toast` (message `"Saved"`).
  - `POST /_partial/triage/pair/{deposit_id}/{sell_id}/confirm` → JSON with keys `closeModal` + `toast` (message `"Pair confirmed"`).
  - `POST /_partial/triage/skip/{item_id}` keeps the plain string `closeModal` (no toast — skipping is not a save).

- [ ] **Step 1: Write the failing tests**

Replace the import block at the top of `tests/web/test_safety_feedback.py` (currently `from __future__ import annotations` + `import sqlite3`) with exactly:

```python
from __future__ import annotations

import json
import sqlite3
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from finances.db.repos import transactions as transactions_repo
from finances.domain.models import Transaction, TransactionKind
```

Then append to the end of the file:

```python
# ---------------------------------------------------------------------------
# Shared helpers (Tasks 2-4).
# ---------------------------------------------------------------------------


def _txn_id(conn: sqlite3.Connection, source_ref: str) -> int:
    row = conn.execute(
        "SELECT id FROM transactions WHERE source_ref = ?", (source_ref,)
    ).fetchone()
    assert row is not None, f"missing seeded txn {source_ref}"
    return int(row["id"])


@pytest.fixture
def pair_candidates(seeded_web_db: sqlite3.Connection) -> tuple[int, int]:
    """Insert an unpaired Provincial deposit + matching Binance sell.

    Mirrors the pair shape proven in tests/web/test_triage.py:
    expected VES = abs(sell amount) * user_rate = 1000 * 36.50 = 36500.
    The sell leg uses a REAL negative amount (expense sign convention —
    do not copy seeded_web_db's positive-amount wart).
    """
    prov_row = seeded_web_db.execute(
        "SELECT id FROM accounts WHERE name = 'Provincial'"
    ).fetchone()
    bin_row = seeded_web_db.execute(
        "SELECT id FROM accounts WHERE name = 'Binance Spot'"
    ).fetchone()
    assert prov_row is not None and bin_row is not None
    yesterday = datetime.now(tz=UTC) - timedelta(days=1)

    transactions_repo.insert(
        seeded_web_db,
        Transaction(
            account_id=int(prov_row["id"]),
            occurred_at=yesterday,
            kind=TransactionKind.INCOME,
            amount=Decimal("36500.00"),
            currency="VES",
            description="ABONO P2P sell",
            source="provincial",
            source_ref="wp2-bank-deposit-1",
        ),
    )
    transactions_repo.insert(
        seeded_web_db,
        Transaction(
            account_id=int(bin_row["id"]),
            occurred_at=yesterday,
            kind=TransactionKind.EXPENSE,
            amount=Decimal("-1000.00"),
            currency="USDT",
            description="P2P sell USDT",
            user_rate=Decimal("36.50"),
            source="binance",
            source_ref="wp2-binance-sell-1",
        ),
    )
    return (
        _txn_id(seeded_web_db, "wp2-bank-deposit-1"),
        _txn_id(seeded_web_db, "wp2-binance-sell-1"),
    )


# ---------------------------------------------------------------------------
# Task 2 — HX-Trigger carries the toast JSON payload.
# ---------------------------------------------------------------------------


def test_edit_endpoint_hx_trigger_carries_toast_json(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    client = web_client_factory()
    txn_id = _txn_id(seeded_web_db, "prov-1")

    resp = client.post(
        f"/_partial/transactions/{txn_id}/edit",
        data={
            "set_category": "false",
            "category_id": "",
            "set_user_rate": "true",
            "user_rate": "36.5",
        },
    )
    assert resp.status_code == 200, resp.text
    payload = json.loads(resp.headers["HX-Trigger"])
    assert payload["closeModal"] is True
    assert payload["toast"] == {"level": "success", "message": "Saved"}


def test_triage_edit_hx_trigger_carries_toast_and_advance(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    client = web_client_factory()
    txn_id = _txn_id(seeded_web_db, "prov-needs-review")

    resp = client.post(
        f"/_partial/triage/{txn_id}/edit",
        data={
            "set_category": "false",
            "category_id": "",
            "set_user_rate": "true",
            "user_rate": "36.5",
        },
    )
    assert resp.status_code == 200, resp.text
    payload = json.loads(resp.headers["HX-Trigger"])
    assert payload["closeModal"] is True
    assert payload["advanceQueue"] is True
    assert payload["toast"] == {"level": "success", "message": "Saved"}


def test_pair_confirm_hx_trigger_carries_toast_json(
    pair_candidates: tuple[int, int], web_client_factory
) -> None:
    deposit_id, sell_id = pair_candidates
    client = web_client_factory()

    resp = client.post(f"/_partial/triage/pair/{deposit_id}/{sell_id}/confirm")
    assert resp.status_code == 200, resp.text
    payload = json.loads(resp.headers["HX-Trigger"])
    assert payload["closeModal"] is True
    assert payload["toast"] == {"level": "success", "message": "Pair confirmed"}
```

- [ ] **Step 2: Run tests to verify the new ones fail**

Run: `uv run pytest -q tests/web/test_safety_feedback.py`

Expected: `3 failed, 4 passed` — the three new tests fail inside `json.loads(...)` with `json.decoder.JSONDecodeError: Expecting value: line 1 column 1 (char 0)` (headers are currently the plain strings `closeModal` / `closeModal, advanceQueue`).

- [ ] **Step 3: Commit the failing tests**

```bash
git add tests/web/test_safety_feedback.py
git commit -m "test(web): edit and pair-confirm responses carry toast JSON in HX-Trigger"
```

- [ ] **Step 4: Implement — helper + header changes in partials.py**

In `finances/web/routers/partials.py`:

(a) Add `import json` to the stdlib imports (line 14 area), so the block reads:

```python
import json
import sqlite3
from decimal import Decimal, InvalidOperation
```

(b) Directly after `_parse_form_bool` (currently ends line 200), add:

```python
def _hx_trigger_json(*events: str, toast_message: str) -> str:
    """Build an ``HX-Trigger`` header value: named events + a success toast.

    htmx accepts a JSON object in ``HX-Trigger``: each key is dispatched
    as an event, its value as the event detail. The base.html <body>
    listener re-dispatches ``closeModal`` as the ``close-modal`` window
    event and ``toast`` as ``show-toast`` (WP2 toast contract; error
    toasts come from the global htmx:responseError listener instead).
    """
    payload: dict[str, object] = {name: True for name in events}
    payload["toast"] = {"level": "success", "message": toast_message}
    return json.dumps(payload)
```

(c) In `transactions_edit_partial` (currently line 291), replace

```python
    response.headers["HX-Trigger"] = "closeModal"
```

with

```python
    response.headers["HX-Trigger"] = _hx_trigger_json("closeModal", toast_message="Saved")
```

(d) In `triage_edit_partial` (currently line 423), replace

```python
    response.headers["HX-Trigger"] = "closeModal, advanceQueue"
```

with

```python
    response.headers["HX-Trigger"] = _hx_trigger_json(
        "closeModal", "advanceQueue", toast_message="Saved"
    )
```

(e) In `triage_pair_confirm_partial` (currently line 509), replace

```python
    response.headers["HX-Trigger"] = "closeModal"
```

with

```python
    response.headers["HX-Trigger"] = _hx_trigger_json(
        "closeModal", toast_message="Pair confirmed"
    )
```

(f) Leave `triage_skip_partial` (line 523) as the plain `"closeModal"` — skipping is not a save, and the base.html fallback parser handles the plain form.

- [ ] **Step 5: Run tests to verify they pass**

Run: `uv run pytest -q tests/web/test_safety_feedback.py`

Expected: `7 passed`

- [ ] **Step 6: Regression-check the web suite**

Run: `uv run pytest -q tests/web`

Expected: all pass, `0 failed`. (Existing assertions like `assert "closeModal" in trigger` and `assert "advanceQueue" in trigger` in `tests/web/test_transactions_write.py` / `tests/web/test_triage.py` are substring checks — they still pass against the JSON header.)

- [ ] **Step 7: Commit the implementation**

```bash
git add finances/web/routers/partials.py
git commit -m "feat(web): send toast JSON via HX-Trigger on edit and pair-confirm saves"
```

---

### Task 3: Dirty-tracking + remove-category control + autofocus in the transactions edit modal

**Files:**
- Modify: `finances/web/templates/partials/modal_transaction.html:8-10` (header comment) and `:95-138` (form)
- Modify: `finances/web/templates/_macros.html:141-154` (`category_select` gains `autofocus` param)
- Modify: `finances/web/routers/partials.py:160-166` (stale form-encoding comment)
- Modify: `finances/web/services/transactions_write.py:34-42` (stale `TransactionEditRequest` docstring — docs only, zero behavior change)
- Modify: `finances/web/static/css/app.css` (append `.remove-category-btn`)
- Test: `tests/web/test_safety_feedback.py` (append)

**Interfaces:**
- Consumes: `TransactionEditRequest(set_category, category_id, set_user_rate, user_rate)` semantics from `finances/web/services/transactions_write.py` (unchanged: `set_*=False` → field untouched); `_parse_form_bool` in partials.py maps `"false"` → `False`.
- Produces (Task 4 and WP4 rely on these exact names):
  - Jinja macro signature `category_select(name, options, selected=None, autofocus=False)` in `_macros.html`.
  - Modal form pattern: `x-data="{ catDirty: false, rateDirty: false }"` on the `<form>`; hidden inputs `name="set_category"` / `name="set_user_rate"` with static `value="false"` plus Alpine `:value="... ? 'true' : 'false'"` binding; `@change`/`@input` wrappers flip the dirty flags.
  - `.remove-category-btn` CSS class in `app.css`.

- [ ] **Step 1: Write the tests**

Append to `tests/web/test_safety_feedback.py`:

```python
# ---------------------------------------------------------------------------
# Task 3 — transactions edit modal: dirty tracking, remove control, focus.
#
# The wipe bug is a TEMPLATE bug (hard-coded set_category=true sentinels);
# apply_edit already honors set_*=false. So the template-marker tests below
# are the red ones; the endpoint tests pin the (already-correct) server
# contract that the new untouched-form payload relies on.
# ---------------------------------------------------------------------------


def test_modal_no_hardcoded_set_sentinels(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    client = web_client_factory()
    txn_id = _txn_id(seeded_web_db, "prov-1")
    body = client.get(f"/_partial/transactions/{txn_id}/modal").text

    # The old always-true sentinels are gone...
    assert 'name="set_category" value="true"' not in body
    assert 'name="set_user_rate" value="true"' not in body
    # ...replaced by Alpine dirty-tracking bindings.
    assert "catDirty" in body
    assert "rateDirty" in body


def test_modal_untouched_fields_do_not_wipe(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    """Payload an untouched dirty-tracked form now submits: both set_*
    false, both values empty. Nothing may be cleared."""
    client = web_client_factory()
    txn_id = _txn_id(seeded_web_db, "prov-3")  # has category AND user_rate
    before = transactions_repo.get_by_id(seeded_web_db, txn_id)
    assert before is not None
    assert before.category_id is not None
    assert before.user_rate is not None

    resp = client.post(
        f"/_partial/transactions/{txn_id}/edit",
        data={
            "set_category": "false",
            "category_id": "",
            "set_user_rate": "false",
            "user_rate": "",
        },
    )
    assert resp.status_code == 200, resp.text

    after = transactions_repo.get_by_id(seeded_web_db, txn_id)
    assert after is not None
    assert after.category_id == before.category_id
    assert after.user_rate == before.user_rate


def test_modal_has_remove_category_control_when_categorized(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    client = web_client_factory()
    txn_id = _txn_id(seeded_web_db, "prov-1")  # categorized (Groceries)
    body = client.get(f"/_partial/transactions/{txn_id}/modal").text
    assert "remove category" in body


def test_modal_hides_remove_category_control_when_uncategorized(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    client = web_client_factory()
    txn_id = _txn_id(seeded_web_db, "cash-1")  # no category
    body = client.get(f"/_partial/transactions/{txn_id}/modal").text
    assert "remove category" not in body


def test_modal_explicit_remove_payload_clears_category(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    """Payload the '× remove category' control produces: set_category=true
    with an empty category_id. This — and only this — clears."""
    client = web_client_factory()
    txn_id = _txn_id(seeded_web_db, "prov-1")
    before = transactions_repo.get_by_id(seeded_web_db, txn_id)
    assert before is not None and before.category_id is not None

    resp = client.post(
        f"/_partial/transactions/{txn_id}/edit",
        data={
            "set_category": "true",
            "category_id": "",
            "set_user_rate": "false",
            "user_rate": "",
        },
    )
    assert resp.status_code == 200, resp.text

    after = transactions_repo.get_by_id(seeded_web_db, txn_id)
    assert after is not None
    assert after.category_id is None


def test_modal_category_control_has_autofocus(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    client = web_client_factory()
    txn_id = _txn_id(seeded_web_db, "prov-1")
    body = client.get(f"/_partial/transactions/{txn_id}/modal").text
    assert "autofocus" in body
```

- [ ] **Step 2: Run tests to verify the red ones fail**

Run: `uv run pytest -q tests/web/test_safety_feedback.py`

Expected: `3 failed, 10 passed`. Failing (AssertionError):
- `test_modal_no_hardcoded_set_sentinels` — `'name="set_category" value="true"'` IS currently in the body,
- `test_modal_has_remove_category_control_when_categorized` — no "remove category" text yet,
- `test_modal_category_control_has_autofocus` — no "autofocus" yet.

Passing by design (server-contract pins, documented in the section comment): `test_modal_untouched_fields_do_not_wipe`, `test_modal_hides_remove_category_control_when_uncategorized`, `test_modal_explicit_remove_payload_clears_category`.

- [ ] **Step 3: Commit the tests**

```bash
git add tests/web/test_safety_feedback.py
git commit -m "test(web): edit modal dirty-tracking — untouched select must not clear category"
```

- [ ] **Step 4: Implement — `category_select` macro gains `autofocus`**

In `finances/web/templates/_macros.html`, replace the macro head (currently lines 141-154) with:

```jinja
{%- macro category_select(name, options, selected=None, autofocus=False) -%}
{#- Render a <select> for the modal's category field.

    ``options`` should be a list of Pydantic ``Category`` objects (the
    output of ``categories_repo.list_all``). Categories are grouped by
    ``kind`` to mimic the existing CLI flow.

    ``autofocus=True`` renders the HTML autofocus attribute — htmx
    focuses autofocused elements inside swapped-in content, so the modal
    opens with the category control focused (WP2).

    The "no category" option carries an empty value; since WP2 the
    modals dirty-track the select, so an untouched empty option never
    clears anything — clearing is the explicit remove-category control.
-#}
<select
  name="{{ name }}"
  {% if autofocus %}autofocus{% endif %}
  class="mt-1 w-full border border-slate-300 rounded px-2 py-1 text-sm bg-white"
>
```

Leave the rest of the macro (the `<option>` loop and closing tag) untouched. The new parameter defaults to `False`, so the macro stays backward compatible.

- [ ] **Step 5: Implement — modal_transaction.html**

(a) Replace the header-comment lines 8-10 (`Encoding choice: ... Documented in routers/partials.py.`) with:

```jinja
   Encoding choice (WP2 / ux-overhaul §2): ``set_category`` and
   ``set_user_rate`` are dirty-tracked hidden inputs bound to Alpine
   state — they submit ``true`` only when the user actually changed
   that control. Clearing a category is the explicit "× remove
   category" button, never a side effect of an untouched select.
   Documented in routers/partials.py.
```

(b) Replace the whole form section (currently lines 95-138, from `{# 4. Editable form ...` through `</form>`) with:

```html
    {# 4. Editable form --------------------------------------------------- #}
    <form
      class="tx-modal-form"
      hx-post="/_partial/transactions/{{ txn.id }}/edit"
      hx-target="[data-tx-id='{{ txn.id }}']"
      hx-swap="outerHTML"
      x-data="{ catDirty: false, rateDirty: false }"
      x-init="$nextTick(() => { const el = $root.querySelector('[name=category_id]'); if (el) el.focus(); })"
    >
      {# Dirty-tracked sentinels (WP2): submit true only when the user
         actually touched the matching control. An untouched empty
         select can no longer silently clear an existing category. #}
      <input type="hidden" name="set_category" value="false" :value="catDirty ? 'true' : 'false'">
      <input type="hidden" name="set_user_rate" value="false" :value="rateDirty ? 'true' : 'false'">

      <label class="block" @change="catDirty = true">
        <span class="text-xs uppercase tracking-wide text-slate-500">Category</span>
        {{ category_select("category_id", categories, selected=txn.category_id, autofocus=True) }}
      </label>

      {% if txn.category_id is not none %}
        <button
          type="button"
          class="remove-category-btn"
          @click="$root.querySelector('[name=category_id]').value = ''; catDirty = true"
        >&times; remove category</button>
      {% endif %}

      <label class="block" @input="rateDirty = true">
        <span class="text-xs uppercase tracking-wide text-slate-500">User rate</span>
        <input
          type="text"
          name="user_rate"
          inputmode="decimal"
          autocomplete="off"
          value="{{ txn.user_rate if txn.user_rate is not none else '' }}"
          placeholder="{% if card.amount_usd is not none and card.rate_source != 'user_rate' and card.rate_source != 'native_usd' %}fallback: {{ card.rate_source }}{% else %}native amount in {{ txn.currency }}/USD{% endif %}"
          class="mt-1 w-full border border-slate-300 rounded px-2 py-1 text-sm font-mono"
        >
        <span class="text-[11px] text-slate-500">
          empty = clear (re-derives needs_review)
        </span>
      </label>

      <div class="tx-modal-actions">
        <button
          type="button"
          class="px-3 py-1.5 text-sm border border-slate-300 rounded bg-white hover:bg-slate-50"
          @click="window.dispatchEvent(new CustomEvent('close-modal'))"
        >Cancel</button>
        <button
          type="submit"
          class="px-3 py-1.5 text-sm border border-slate-900 rounded bg-slate-900 text-white hover:bg-slate-800"
        >Save</button>
      </div>
    </form>
```

Mechanics: `change` events from the `<select>` bubble to the wrapping `<label>` (so the macro output needs no event attributes); `$root` inside the button/`x-init` is the `<form>` (the Alpine component root). The static `value="false"` is the pre-Alpine-init fallback — a submit before Alpine initializes sends `false`, which is the safe no-op. `user_rate` clearing still works without a dedicated control: emptying the input fires `input`, marks `rateDirty`, and submits `set_user_rate=true` with an empty value.

(c) Append to the end of `finances/web/static/css/app.css` (after the toasts block from Task 1):

```css
/* === remove-category control (WP2) ===================================== */
.remove-category-btn {
  align-self: flex-start;       /* .tx-modal-form is a flex column */
  font-size: 0.75rem;
  color: #be123c;               /* rose-700 */
  background: none;
  border: 0;
  padding: 0;
  cursor: pointer;
  text-decoration: underline;
}
.remove-category-btn:hover {
  color: #9f1239;               /* rose-800 */
}
/* === END remove-category control === */
```

- [ ] **Step 6: Implement — fix the two stale doc comments (no behavior change)**

(a) In `finances/web/routers/partials.py`, replace the "Form encoding choice" comment paragraph (currently lines 161-166) with:

```python
# Form encoding choice (WP2 / ux-overhaul §2):
#   The modals dirty-track their controls with Alpine and submit
#   ``set_category=true`` / ``set_user_rate=true`` only for fields the
#   user actually touched; clearing a category is the explicit
#   "× remove category" control. The API still accepts any ``set_*``
#   combination from JSON callers via the same Pydantic model.
```

(b) In `finances/web/services/transactions_write.py`, replace the `TransactionEditRequest` docstring (currently lines 34-42) with:

```python
    """Modal save payload.

    The two ``set_*`` flags disambiguate "field omitted → leave alone"
    from "field present with value None → clear it". Since WP2
    (ux-overhaul) the modals dirty-track their controls and submit
    ``set_*=True`` only for fields the user actually touched; clearing
    a category is an explicit control. JSON callers may still send any
    ``set_*`` combination for partial updates.
    """
```

- [ ] **Step 7: Run tests to verify they pass**

Run: `uv run pytest -q tests/web/test_safety_feedback.py`

Expected: `13 passed`

- [ ] **Step 8: Regression-check the web suite**

Run: `uv run pytest -q tests/web`

Expected: all pass, `0 failed`. (Existing `tests/web/test_transactions_write.py` modal tests assert `name="category_id"`, `name="user_rate"`, `Save`, `Cancel`, `data-tx-id` — all preserved; its edit-POST tests send explicit `set_*` values, so the server path is unchanged for them.)

- [ ] **Step 9: Commit the implementation**

```bash
git add finances/web/templates/partials/modal_transaction.html \
        finances/web/templates/_macros.html \
        finances/web/routers/partials.py \
        finances/web/services/transactions_write.py \
        finances/web/static/css/app.css
git commit -m "fix(web): edit modal submits set_* only when touched; explicit remove-category control; autofocus"
```

---

### Task 4: Same dirty-tracking + remove control + autofocus in the triage modal

**Files:**
- Modify: `finances/web/templates/partials/modal_transaction_triage.html:71-120` (form)
- Test: `tests/web/test_safety_feedback.py` (append)

**Interfaces:**
- Consumes (from Task 3): `category_select(name, options, selected=None, autofocus=False)` macro; the dirty-tracking form pattern (`x-data="{ catDirty: false, rateDirty: false }"`, `:value`-bound hidden sentinels, `@change`/`@input` label wrappers); `.remove-category-btn` CSS class.
- Produces: `modal_transaction_triage.html` emitting the same untouched-form payload (`set_category=false&set_user_rate=false`) to `POST /_partial/triage/{txn_id}/edit`.

- [ ] **Step 1: Write the tests**

Append to `tests/web/test_safety_feedback.py`:

```python
# ---------------------------------------------------------------------------
# Task 4 — triage modal: same dirty tracking, remove control, focus.
# ---------------------------------------------------------------------------


def test_triage_modal_no_hardcoded_set_sentinels(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    client = web_client_factory()
    txn_id = _txn_id(seeded_web_db, "prov-1")
    body = client.get(f"/_partial/triage/{txn_id}/modal").text

    assert 'name="set_category" value="true"' not in body
    assert 'name="set_user_rate" value="true"' not in body
    assert "catDirty" in body
    assert "rateDirty" in body


def test_triage_edit_untouched_fields_do_not_wipe(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    """Server-contract pin for the triage edit endpoint: the untouched
    dirty-tracked payload must clear nothing (passes pre-impl; guards
    the payload shape the new template emits)."""
    client = web_client_factory()
    txn_id = _txn_id(seeded_web_db, "prov-3")  # has category AND user_rate
    before = transactions_repo.get_by_id(seeded_web_db, txn_id)
    assert before is not None
    assert before.category_id is not None
    assert before.user_rate is not None

    resp = client.post(
        f"/_partial/triage/{txn_id}/edit",
        data={
            "set_category": "false",
            "category_id": "",
            "set_user_rate": "false",
            "user_rate": "",
        },
    )
    assert resp.status_code == 200, resp.text

    after = transactions_repo.get_by_id(seeded_web_db, txn_id)
    assert after is not None
    assert after.category_id == before.category_id
    assert after.user_rate == before.user_rate


def test_triage_modal_has_remove_category_control_when_categorized(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    client = web_client_factory()
    txn_id = _txn_id(seeded_web_db, "prov-1")  # categorized (Groceries)
    body = client.get(f"/_partial/triage/{txn_id}/modal").text
    assert "remove category" in body


def test_triage_modal_category_control_has_autofocus(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    client = web_client_factory()
    txn_id = _txn_id(seeded_web_db, "prov-1")
    body = client.get(f"/_partial/triage/{txn_id}/modal").text
    assert "autofocus" in body
```

- [ ] **Step 2: Run tests to verify the red ones fail**

Run: `uv run pytest -q tests/web/test_safety_feedback.py`

Expected: `3 failed, 14 passed`. Failing (AssertionError): `test_triage_modal_no_hardcoded_set_sentinels`, `test_triage_modal_has_remove_category_control_when_categorized`, `test_triage_modal_category_control_has_autofocus`. Passing by design: `test_triage_edit_untouched_fields_do_not_wipe` (server-contract pin, noted in its docstring).

- [ ] **Step 3: Commit the tests**

```bash
git add tests/web/test_safety_feedback.py
git commit -m "test(web): triage modal dirty-tracking, remove control, autofocus"
```

- [ ] **Step 4: Implement — modal_transaction_triage.html form**

Replace the form section (currently lines 71-120, from `{# Editable form: Save & next variant. #}` through `</form>`) with:

```html
    {# Editable form: Save & next variant. Dirty-tracked sentinels (WP2):
       submit true only when the user actually touched the matching
       control — an untouched empty select can no longer silently clear
       an existing category. #}
    <form
      class="tx-modal-form"
      hx-post="/_partial/triage/{{ txn.id }}/edit"
      hx-target="#triage-queue"
      hx-swap="innerHTML"
      x-data="{ catDirty: false, rateDirty: false }"
      x-init="$nextTick(() => { const el = $root.querySelector('[name=category_id]'); if (el) el.focus(); })"
    >
      <input type="hidden" name="set_category" value="false" :value="catDirty ? 'true' : 'false'">
      <input type="hidden" name="set_user_rate" value="false" :value="rateDirty ? 'true' : 'false'">

      <label class="block" @change="catDirty = true">
        <span class="text-xs uppercase tracking-wide text-slate-500">Category</span>
        {{ category_select("category_id", categories, selected=txn.category_id, autofocus=True) }}
      </label>

      {% if txn.category_id is not none %}
        <button
          type="button"
          class="remove-category-btn"
          @click="$root.querySelector('[name=category_id]').value = ''; catDirty = true"
        >&times; remove category</button>
      {% endif %}

      <label class="block" @input="rateDirty = true">
        <span class="text-xs uppercase tracking-wide text-slate-500">User rate</span>
        <input
          type="text"
          name="user_rate"
          inputmode="decimal"
          autocomplete="off"
          value="{{ txn.user_rate if txn.user_rate is not none else '' }}"
          placeholder="{% if card.rate_source not in ('user_rate', 'native_usd') and card.amount_usd is not none %}fallback: {{ card.rate_source }}{% else %}native amount in {{ txn.currency }}/USD{% endif %}"
          class="mt-1 w-full border border-slate-300 rounded px-2 py-1 text-sm font-mono"
        >
        <span class="text-[11px] text-slate-500">
          empty = clear (re-derives needs_review)
        </span>
      </label>

      <div class="tx-modal-actions">
        <button
          type="button"
          class="px-3 py-1.5 text-sm border border-slate-300 rounded bg-white hover:bg-slate-50"
          @click="window.dispatchEvent(new CustomEvent('close-modal'))"
        >Cancel</button>
        <button
          type="button"
          class="px-3 py-1.5 text-sm border border-amber-300 rounded bg-amber-50 text-amber-800 hover:bg-amber-100"
          hx-post="/_partial/triage/skip/txn:{{ txn.id }}"
          hx-target="#triage-queue"
          hx-swap="innerHTML"
        >Skip → bottom</button>
        <button
          type="submit"
          class="px-3 py-1.5 text-sm border border-slate-900 rounded bg-slate-900 text-white hover:bg-slate-800"
        >Save &amp; next</button>
      </div>
    </form>
```

(Only the form opening tag, the two sentinels, the label wrappers, the remove button, and the leading comment differ from the current file — Cancel / Skip / Save & next buttons and the user_rate placeholder logic are byte-identical to today's.)

- [ ] **Step 5: Run tests to verify they pass**

Run: `uv run pytest -q tests/web/test_safety_feedback.py`

Expected: `17 passed`

- [ ] **Step 6: Regression-check the web suite**

Run: `uv run pytest -q tests/web`

Expected: all pass, `0 failed`. (Existing `tests/web/test_triage.py` modal tests assert `Save`/`next`/`Skip`/`Cancel` text and post explicit `set_*` values — all preserved.)

- [ ] **Step 7: Commit the implementation**

```bash
git add finances/web/templates/partials/modal_transaction_triage.html
git commit -m "fix(web): triage modal dirty-tracking, remove-category control, autofocus"
```

---

### Task 5: Full-suite verification + owner gate

**Files:**
- No file changes. Verification only — nothing to commit.

**Interfaces:**
- Consumes: everything produced by Tasks 1-4.
- Produces: evidence for Julio's completion decision (execution rule 3 — the agent never marks the work package Complete).

- [ ] **Step 1: Run the whole web suite**

Run: `uv run pytest -q tests/web`

Expected: all pass, `0 failed` (includes the 17 tests in `tests/web/test_safety_feedback.py`).

- [ ] **Step 2: Run the full test suite**

Run: `uv run pytest -q`

Expected: all pass, `0 failed`. If anything outside `tests/web` fails, stop and investigate before reporting — nothing in this WP should touch non-web behavior.

- [ ] **Step 3: Hand the manual verification gate to Julio**

Do NOT perform these against the real DB yourself; report the checklist for Julio to run (viewer writes on the real `finances.db` are owner actions):

1. Start the viewer (`uv run finances serve` or `Finances.command`), open `/transactions`, click a **categorized** transaction, and press Save without touching anything → toast "Saved" appears bottom-right, modal closes, **category is still set** (this was the wipe bug).
2. Re-open the same transaction → the category `<select>` is focused on open; click "× remove category", Save → category cleared, toast "Saved".
3. Open a transaction, type `abc` into User rate, Save → red error toast showing `user_rate must be a decimal or empty: 'abc'` (from the global `htmx:responseError` listener), modal stays open.
4. On `/triage`, save an item via "Save & next" → toast "Saved", queue advances; confirm a pair (if one is proposed) → toast "Pair confirmed".
5. Phone-width check (≤640px): toasts span the bottom edge and don't block the modal buttons.

Julio marks WP2 complete; the agent only reports results.
