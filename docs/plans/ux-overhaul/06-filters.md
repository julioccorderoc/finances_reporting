# WP6 Filter Polish Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the ctrl-click `<select multiple>` filters on `/transactions` with tappable checkbox chips, upgrade `/monthly` since/until to native `<input type="month">` pickers, and add a "Clear filters" link to both forms — templates + CSS only, query-param contract frozen.

**Architecture:** Multiple checkboxes sharing one `name` serialize to the exact same repeated query params (`?accounts=A&accounts=B`) that `<select multiple>` produces, so `finances/web/routers/_tx_filter_dep.py` (which parses them via `Query(default_factory=list)`) stays byte-for-byte unchanged; likewise `<input type="month">` submits exactly `YYYY-MM`, the only format `_MONTH_RE` in `finances/web/routers/_monthly_filter_dep.py` accepts. "Clear filters" is a plain anchor back to the bare page URL — the server re-derives defaults (`resolve_defaults` in `finances/web/services/transactions_query.py` fills the last-30-days window; `MonthlyFilter` defaults to the `6m` preset). No Python file changes anywhere in this work package.

**Tech Stack:** Jinja2 templates, plain CSS appended to `finances/web/static/css/app.css`, vendored htmx (unchanged — checkbox/month `change` events bubble to the form's existing `hx-trigger="change"`), pytest + FastAPI `TestClient`.

## Global Constraints

- TDD per rule-011 / CLAUDE.md execution rule 5: each task commits its failing test (`test(web): ...`) BEFORE its implementation (`feat(web): ...`).
- Run tests only with `uv run pytest -q <path>` — never bare `pytest`.
- Query-param contract is frozen: names `accounts`, `kinds`, `currencies`, `sources` (repeated params) and `since`/`until` (`YYYY-MM` strings). `_tx_filter_dep.py` and `_monthly_filter_dep.py` must NOT be modified.
- Tests never touch the real `finances.db` — use `seeded_web_db` + `web_client_factory` from `tests/web/conftest.py` (per-test tmp file at `web_db_path`).
- No new dependencies, no CDN assets; this WP needs zero new JavaScript (native form controls + existing htmx triggers only).
- Data lists stay CSS Grid card-rows, never `<table>` — untouched here; do not regress.
- `app.css` convention: new rules are APPENDED below the `additional rules: append below this line` marker (line 85); never modify the core card-row grid. `.filter-chips` is already taken (Phase 5 strip rule at app.css:539-548) — the new chip classes are `.choice-chips` / `.choice-chip`.
- `needs_review` stays a single-choice `<select>` (`any`/`yes`/`no`) and stays resolver-derived — no manual toggle semantics added.
- WP6 consumes NO shared contracts from WP1-WP5 (no `finances/format.py`, no toast infra, no category picker) — it must run green even if executed first.
- Execution rule 3: the agent never marks WP6 complete; Julio does, after the Task 4 gate.

---

### Task 1: Checkbox-chip groups on /transactions

**Files:**
- Test (create): `tests/web/test_filters_polish.py`
- Modify: `finances/web/templates/partials/transactions_filters.html` (replace the second grid `<div>`, currently lines 60-101)
- Modify: `finances/web/static/css/app.css` (append a new section at end of file, after `/* === END Phase 5 mobile polish === */`, line 618)

**Interfaces:**
- Consumes: `filter_from_query` (`finances/web/routers/_tx_filter_dep.py:21` — unchanged; `accounts`/`kinds`/`currencies`/`sources` are `list[str]` / `list[Literal[...]]` via `Query(default_factory=list)`); template context from `transactions_page` (`finances/web/routers/pages.py:87-125`): `filter: TransactionsFilter`, `accounts_options: list[str]`, `kinds_options = ["income", "expense", "transfer", "adjustment"]`, `currencies_options: list[str]`, `sources_options: list[str]`; fixtures `seeded_web_db` (accounts `Provincial`/`Cash USD`/`Binance Spot`, currencies `USD`/`USDT`/`VES`, sources `binance`/`cash_cli`/`provincial`) and `web_client_factory` from `tests/web/conftest.py`.
- Produces: CSS classes `.choice-chips` (wrapping flex container) and `.choice-chip` (label wrapping a visually-hidden checkbox + visible `<span>` pill) — reused verbatim by any later filter UI; rendered chip markup shape `<input type="checkbox" name="{group}" value="{opt}" checked><span>{opt}</span>` that Task 3's tests coexist with.

**Steps:**

- [ ] **Step 1.1 — branch.** From a clean `main`:

```bash
cd /Users/juliocordero/Documents/finances_reporting
git checkout -b ux-wp6-filters
```

- [ ] **Step 1.2 — write the failing tests.** Create `tests/web/test_filters_polish.py` with exactly:

```python
"""Filter-polish tests for WP6 (docs/plans/ux-overhaul/00-design.md §6).

Per rule-011 these land before the implementation. They cover:

* /transactions: the four multi-selects (accounts, kinds, currencies,
  sources) are replaced by checkbox-chip groups that keep the SAME
  query-param names, render checked state from the URL, and still
  narrow the result set through the unchanged filter_from_query dep,
* /monthly: since/until are native <input type="month"> controls that
  round-trip the YYYY-MM format _monthly_filter_dep already parses,
* both filter forms carry a plain "Clear filters" link back to the bare
  page URL (transactions default = last-30-days window via
  transactions_query.resolve_defaults; monthly default = 6m preset).

Uses the tmp-DB fixtures from tests/web/conftest.py — never the real
finances.db.
"""

from __future__ import annotations

import sqlite3

from fastapi.testclient import TestClient

_DESKTOP_UA = {"User-Agent": "Mozilla/5.0 desktop"}


# ---------------------------------------------------------------------------
# Task 1 — /transactions checkbox-chip groups.
# ---------------------------------------------------------------------------


def test_transactions_filters_render_checkbox_chip_groups(
    seeded_web_db: sqlite3.Connection,
    web_client_factory,
) -> None:
    client: TestClient = web_client_factory()
    resp = client.get("/transactions")
    assert resp.status_code == 200
    body = resp.text
    # The four multi-selects are gone (needs_review / page_size selects stay).
    for name in ("accounts", "kinds", "currencies", "sources"):
        assert f'<select name="{name}"' not in body
    # ...replaced by checkboxes with the SAME param names (unchecked by
    # default: the default filter constrains dates only, not these lists).
    assert '<input type="checkbox" name="accounts" value="Provincial">' in body
    assert '<input type="checkbox" name="kinds" value="expense">' in body
    assert '<input type="checkbox" name="currencies" value="VES">' in body
    assert '<input type="checkbox" name="sources" value="provincial">' in body
    # Chips are styled via the shared classes.
    assert 'class="choice-chips"' in body
    assert 'class="choice-chip"' in body


def test_transactions_filter_chips_reflect_checked_state_from_url(
    seeded_web_db: sqlite3.Connection,
    web_client_factory,
) -> None:
    client: TestClient = web_client_factory()
    resp = client.get(
        "/transactions",
        params=[
            ("accounts", "Provincial"),
            ("accounts", "Cash USD"),
            ("kinds", "expense"),
        ],
    )
    assert resp.status_code == 200
    body = resp.text
    assert '<input type="checkbox" name="accounts" value="Provincial" checked>' in body
    assert '<input type="checkbox" name="accounts" value="Cash USD" checked>' in body
    assert (
        '<input type="checkbox" name="accounts" value="Binance Spot" checked>'
        not in body
    )
    assert '<input type="checkbox" name="kinds" value="expense" checked>' in body
    assert '<input type="checkbox" name="kinds" value="income" checked>' not in body


def test_checkbox_repeated_params_still_narrow_the_list(
    seeded_web_db: sqlite3.Connection,
    web_client_factory,
) -> None:
    """Contract guard: repeated checkbox params == repeated select params.

    filter_from_query (unchanged) must keep narrowing rows. Expected to
    pass BEFORE the template change too — it pins the param contract.
    """
    client: TestClient = web_client_factory()
    resp = client.get(
        "/transactions",
        params=[("accounts", "Provincial"), ("date_from", "2000-01-01")],
    )
    assert resp.status_code == 200
    body = resp.text
    assert 'data-account="Provincial"' in body
    assert 'data-account="Cash USD"' not in body
    assert 'data-account="Binance Spot"' not in body
```

- [ ] **Step 1.3 — run, expect FAIL.**

```bash
uv run pytest -q tests/web/test_filters_polish.py
```

Expected: `2 failed, 1 passed`. The two failures are `AssertionError` in `test_transactions_filters_render_checkbox_chip_groups` (first failing line: `assert f'<select name="accounts"' not in body` — the multi-selects still exist) and in `test_transactions_filter_chips_reflect_checked_state_from_url` (no checkbox markup yet). `test_checkbox_repeated_params_still_narrow_the_list` passes already — it is the frozen-contract guard.

- [ ] **Step 1.4 — commit the test.**

```bash
git add tests/web/test_filters_polish.py
git commit -m "test(web): checkbox-chip filter groups on /transactions render checked state from URL" -m "Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

- [ ] **Step 1.5 — replace the multi-selects.** In `finances/web/templates/partials/transactions_filters.html`, replace the entire second grid block — from `<div class="grid grid-cols-2 sm:grid-cols-4 gap-3">` (line 60) through its closing `</div>` (line 101) — with:

```html
  <div class="grid grid-cols-1 sm:grid-cols-2 gap-3">
    {% if accounts_options %}
      <fieldset class="text-xs">
        <legend class="text-slate-500 mb-1">Accounts</legend>
        <div class="choice-chips">
          {% for opt in accounts_options %}
            <label class="choice-chip"><input type="checkbox" name="accounts" value="{{ opt }}"{% if opt in filter.accounts %} checked{% endif %}><span>{{ opt }}</span></label>
          {% endfor %}
        </div>
      </fieldset>
    {% endif %}
    {% if kinds_options %}
      <fieldset class="text-xs">
        <legend class="text-slate-500 mb-1">Kinds</legend>
        <div class="choice-chips">
          {% for opt in kinds_options %}
            <label class="choice-chip"><input type="checkbox" name="kinds" value="{{ opt }}"{% if opt in filter.kinds %} checked{% endif %}><span>{{ opt }}</span></label>
          {% endfor %}
        </div>
      </fieldset>
    {% endif %}
    {% if currencies_options %}
      <fieldset class="text-xs">
        <legend class="text-slate-500 mb-1">Currencies</legend>
        <div class="choice-chips">
          {% for opt in currencies_options %}
            <label class="choice-chip"><input type="checkbox" name="currencies" value="{{ opt }}"{% if opt in filter.currencies %} checked{% endif %}><span>{{ opt }}</span></label>
          {% endfor %}
        </div>
      </fieldset>
    {% endif %}
    {% if sources_options %}
      <fieldset class="text-xs">
        <legend class="text-slate-500 mb-1">Sources</legend>
        <div class="choice-chips">
          {% for opt in sources_options %}
            <label class="choice-chip"><input type="checkbox" name="sources" value="{{ opt }}"{% if opt in filter.sources %} checked{% endif %}><span>{{ opt }}</span></label>
          {% endfor %}
        </div>
      </fieldset>
    {% endif %}
  </div>
```

Notes for the implementer: the `<form>` element (lines 5-12) keeps its `hx-trigger="change, search delay:300ms from:input[name='q']"` — checkbox `change` events bubble to the form, so tapping a chip fires the same `#tx-list` swap the selects did; zero JS added. The hidden `sort`/`direction`/`page` inputs (lines 104-106) stay untouched. `fieldset`/`legend` is the standard, accessible grouping for checkbox sets (owner preference: standard patterns over clever ones).

- [ ] **Step 1.6 — append the chip CSS.** At the very end of `finances/web/static/css/app.css` (after `/* === END Phase 5 mobile polish === */`, line 618), append:

```css

/* === WP6 filter polish: checkbox chips =================================
 * Plain <input type="checkbox"> rendered as a tappable pill. The input
 * stays in the DOM (native form serialization + keyboard a11y) but is
 * visually hidden; the sibling <span> is the visible chip. Selected
 * state is pure CSS (`input:checked + span`) — no JS, and htmx sees the
 * native change event bubble to the filter form.
 *
 * NOTE: `.filter-chips` (Phase 5) is a different, pre-existing strip
 * rule — do not merge these.
 */
.choice-chips {
  display: flex;
  flex-wrap: wrap;
  gap: 0.375rem;
}

.choice-chip {
  position: relative;
  cursor: pointer;
}

.choice-chip input[type="checkbox"] {
  position: absolute;
  opacity: 0;
  width: 1px;
  height: 1px;
  margin: 0;
}

.choice-chip span {
  display: inline-flex;
  align-items: center;
  min-height: 28px;
  padding: 0.125rem 0.625rem;
  border: 1px solid #cbd5e1;      /* slate-300 */
  border-radius: 9999px;
  background: #ffffff;
  color: #334155;                 /* slate-700 */
  font-size: 0.75rem;
  user-select: none;
  transition: background-color 80ms ease, border-color 80ms ease,
              color 80ms ease;
}

.choice-chip:hover span {
  border-color: #94a3b8;          /* slate-400 */
}

.choice-chip input:checked + span {
  background: #0369a1;            /* sky-700 */
  border-color: #0369a1;
  color: #ffffff;
}

.choice-chip input:focus-visible + span {
  outline: 2px solid #0ea5e9;     /* sky-500 */
  outline-offset: 1px;
}

/* Phones: >=44px tap target (same convention as Phase 5 mobile polish). */
@media (max-width: 640px) {
  .choice-chip span {
    min-height: 44px;
    padding: 0.5rem 0.875rem;
    font-size: 0.8125rem;
  }
}
/* === END WP6 filter polish === */
```

- [ ] **Step 1.7 — run, expect PASS.**

```bash
uv run pytest -q tests/web/test_filters_polish.py
```

Expected: `3 passed`.

- [ ] **Step 1.8 — regression check on the existing transactions suite** (its filter tests already send `accounts`/`kinds` params — they must be untouched by the markup change):

```bash
uv run pytest -q tests/web/test_transactions_read.py
```

Expected: `14 passed`.

- [ ] **Step 1.9 — commit the implementation.**

```bash
git add finances/web/templates/partials/transactions_filters.html finances/web/static/css/app.css
git commit -m "feat(web): replace /transactions multi-selects with checkbox-chip groups" -m "Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 2: Native month inputs on /monthly

**Files:**
- Test (modify): `tests/web/test_filters_polish.py` (append)
- Modify: `finances/web/templates/partials/monthly_filters.html` (the two since/until `<label>` blocks, currently lines 23-42)

**Interfaces:**
- Consumes: `monthly_filter_from_query` + `_validate_month` / `_MONTH_RE = ^\d{4}-(0[1-9]|1[0-2])$` (`finances/web/routers/_monthly_filter_dep.py:19-30` — unchanged; 422 on malformed month); template context from `monthly_page` (`finances/web/routers/pages.py:219-268`): `filter: MonthlyFilter` where `since`/`until` are `str | None` in `YYYY-MM` (`finances/web/services/monthly_view.py:75-87`). The desktop template `pages/monthly.html` is the only page including `partials/monthly_filters.html`, so tests send `_DESKTOP_UA`.
- Produces: nothing new — pure control-type swap. `<input type="month">` submits exactly `YYYY-MM` (WHATWG month state), matching `_MONTH_RE`.

**Steps:**

- [ ] **Step 2.1 — write the failing tests.** Append to `tests/web/test_filters_polish.py`:

```python
# ---------------------------------------------------------------------------
# Task 2 — /monthly native month inputs.
# ---------------------------------------------------------------------------


def test_monthly_since_until_render_as_month_inputs_and_round_trip(
    seeded_web_db: sqlite3.Connection,
    web_client_factory,
) -> None:
    client: TestClient = web_client_factory()
    resp = client.get(
        "/monthly",
        params={"range_preset": "custom", "since": "2026-01", "until": "2026-05"},
        headers=_DESKTOP_UA,
    )
    assert resp.status_code == 200
    body = resp.text
    # Native month inputs round-trip the YYYY-MM value from the URL.
    assert '<input type="month" name="since" value="2026-01"' in body
    assert '<input type="month" name="until" value="2026-05"' in body
    # The free-text placeholders are gone.
    assert 'placeholder="2026-01"' not in body
    assert 'placeholder="2026-05"' not in body

    # Empty state renders an empty value attribute, never "None".
    resp_default = client.get("/monthly", headers=_DESKTOP_UA)
    assert resp_default.status_code == 200
    assert '<input type="month" name="since" value=""' in resp_default.text
    assert '<input type="month" name="until" value=""' in resp_default.text


def test_monthly_month_param_validation_is_unchanged(
    seeded_web_db: sqlite3.Connection,
    web_client_factory,
) -> None:
    """Contract guard: _monthly_filter_dep still 422s malformed months.

    <input type="month"> submits exactly YYYY-MM — the only format
    _MONTH_RE accepts. Expected to pass BEFORE the template change too.
    """
    client: TestClient = web_client_factory()
    resp = client.get("/monthly", params={"since": "2026-13"}, headers=_DESKTOP_UA)
    assert resp.status_code == 422
```

- [ ] **Step 2.2 — run, expect FAIL.**

```bash
uv run pytest -q tests/web/test_filters_polish.py -k month
```

Expected: `1 failed, 1 passed` (plus `3 deselected`). The failure is `AssertionError` in `test_monthly_since_until_render_as_month_inputs_and_round_trip` on `assert '<input type="month" name="since" value="2026-01"' in body` — the template still renders `type="text"`. The validation guard passes already.

- [ ] **Step 2.3 — commit the test.**

```bash
git add tests/web/test_filters_polish.py
git commit -m "test(web): monthly since/until as native month inputs round-trip YYYY-MM" -m "Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

- [ ] **Step 2.4 — swap the inputs.** In `finances/web/templates/partials/monthly_filters.html`, replace the two label blocks (lines 23-42):

```html
    <label class="text-xs flex flex-col gap-1">
      <span class="text-slate-500">Since (YYYY-MM)</span>
      <input
        type="text"
        name="since"
        value="{{ filter.since or '' }}"
        placeholder="2026-01"
        class="border border-slate-300 rounded px-2 py-1 text-sm"
      >
    </label>
    <label class="text-xs flex flex-col gap-1">
      <span class="text-slate-500">Until (YYYY-MM)</span>
      <input
        type="text"
        name="until"
        value="{{ filter.until or '' }}"
        placeholder="2026-05"
        class="border border-slate-300 rounded px-2 py-1 text-sm"
      >
    </label>
```

with (single-line inputs so the rendered attribute order matches the test assertions):

```html
    <label class="text-xs flex flex-col gap-1">
      <span class="text-slate-500">Since</span>
      <input type="month" name="since" value="{{ filter.since or '' }}" class="border border-slate-300 rounded px-2 py-1 text-sm">
    </label>
    <label class="text-xs flex flex-col gap-1">
      <span class="text-slate-500">Until</span>
      <input type="month" name="until" value="{{ filter.until or '' }}" class="border border-slate-300 rounded px-2 py-1 text-sm">
    </label>
```

Notes: the form's `hx-trigger="change"` (line 12) fires natively when the month picker commits a value — no JS. Since/until are only honoured when `range_preset == custom` (documented on `MonthlyFilter`, monthly_view.py:81) — that behaviour is out of scope and unchanged.

- [ ] **Step 2.5 — run, expect PASS.**

```bash
uv run pytest -q tests/web/test_filters_polish.py
```

Expected: `5 passed`.

- [ ] **Step 2.6 — regression check on the existing monthly suite:**

```bash
uv run pytest -q tests/web/test_monthly.py
```

Expected: all pass, `0 failed`.

- [ ] **Step 2.7 — commit the implementation.**

```bash
git add finances/web/templates/partials/monthly_filters.html
git commit -m "feat(web): use input type=month for /monthly since/until" -m "Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 3: "Clear filters" links on both forms

**Files:**
- Test (modify): `tests/web/test_filters_polish.py` (append)
- Modify: `finances/web/templates/partials/transactions_filters.html` (insert before the closing `</form>`, after the hidden `page` input)
- Modify: `finances/web/templates/partials/monthly_filters.html` (insert before the closing `</form>`, after the hidden `kind` input)

**Interfaces:**
- Consumes: `resolve_defaults` (`finances/web/services/transactions_query.py:125-132` — bare `/transactions` re-derives the last-30-days window server-side) and `MonthlyFilter` field defaults (`range_preset=6m`, `since=None`, `until=None`, `kind=expense`, `include_bcv_fallback=True` — monthly_view.py:75-87). A plain full-navigation `<a>` is deliberately NOT htmx-boosted: it must drop every query param, including the hidden `sort`/`direction`/`kind` state.
- Produces: `data-clear-filters` attribute marker on both anchors (test hook + future styling hook).

**Steps:**

- [ ] **Step 3.1 — write the failing tests.** Append to `tests/web/test_filters_polish.py`:

```python
# ---------------------------------------------------------------------------
# Task 3 — Clear-filters links.
# ---------------------------------------------------------------------------


def test_transactions_clear_filters_link_resets_to_bare_url(
    seeded_web_db: sqlite3.Connection,
    web_client_factory,
) -> None:
    client: TestClient = web_client_factory()
    resp = client.get("/transactions", params={"q": "COM.PAGO"})
    assert resp.status_code == 200
    body = resp.text
    # Plain link to the bare page URL — the server re-derives the
    # last-30-days default via transactions_query.resolve_defaults.
    assert '<a href="/transactions" data-clear-filters' in body
    assert ">Clear filters</a>" in body


def test_monthly_clear_filters_link_resets_to_bare_url(
    seeded_web_db: sqlite3.Connection,
    web_client_factory,
) -> None:
    client: TestClient = web_client_factory()
    resp = client.get(
        "/monthly",
        params={"range_preset": "custom", "since": "2026-01"},
        headers=_DESKTOP_UA,
    )
    assert resp.status_code == 200
    body = resp.text
    assert '<a href="/monthly" data-clear-filters' in body
    assert ">Clear filters</a>" in body
```

- [ ] **Step 3.2 — run, expect FAIL.**

```bash
uv run pytest -q tests/web/test_filters_polish.py -k clear_filters
```

Expected: `2 failed` (plus `5 deselected`); both fail with `AssertionError` on the `'<a href="..." data-clear-filters'` assertion — the anchors don't exist yet.

- [ ] **Step 3.3 — commit the test.**

```bash
git add tests/web/test_filters_polish.py
git commit -m "test(web): clear-filters link on transactions + monthly filter forms" -m "Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

- [ ] **Step 3.4 — add the link to the transactions form.** In `finances/web/templates/partials/transactions_filters.html`, immediately after the hidden-inputs block:

```html
  {# Hidden fields preserve sort + direction across filter changes. #}
  <input type="hidden" name="sort" value="{{ filter.sort }}">
  <input type="hidden" name="direction" value="{{ filter.direction }}">
  <input type="hidden" name="page" value="1">
```

and before `</form>`, insert:

```html
  <div class="flex justify-end">
    {# Full navigation on purpose: dropping ALL query params (incl. the
       hidden sort/direction) resets to the server-side defaults —
       last-30-days window per transactions_query.resolve_defaults. #}
    <a href="/transactions" data-clear-filters class="text-xs text-sky-700 hover:underline">Clear filters</a>
  </div>
```

- [ ] **Step 3.5 — add the link to the monthly form.** In `finances/web/templates/partials/monthly_filters.html`, immediately after:

```html
  {# Preserve the active kind across filter changes. #}
  <input type="hidden" name="kind" value="{{ filter.kind.value }}">
```

and before `</form>`, insert:

```html
  <div class="flex justify-end">
    {# Full navigation on purpose: resets range_preset to 6m, clears
       since/until/accounts/currencies, kind back to expense. #}
    <a href="/monthly" data-clear-filters class="text-xs text-sky-700 hover:underline">Clear filters</a>
  </div>
```

- [ ] **Step 3.6 — run, expect PASS.**

```bash
uv run pytest -q tests/web/test_filters_polish.py
```

Expected: `7 passed`.

- [ ] **Step 3.7 — commit the implementation.**

```bash
git add finances/web/templates/partials/transactions_filters.html finances/web/templates/partials/monthly_filters.html
git commit -m "feat(web): add clear-filters links resetting both filter forms to defaults" -m "Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 4: Verification gate (no code)

**Files:** none created or modified — commands only.
**Interfaces:** Consumes the whole test suite; produces the evidence Julio needs to mark WP6 complete (execution rule 3 — the agent must NOT mark it complete).

- [ ] **Step 4.1 — full web suite:**

```bash
uv run pytest -q tests/web
```

Expected: all pass, `0 failed`.

- [ ] **Step 4.2 — full unit suite (guards against accidental cross-module breakage):**

```bash
uv run pytest -q
```

Expected: all pass, `0 failed`.

- [ ] **Step 4.3 — manual eyeball (read-only; do NOT run `finances ingest`/`update`/`sync`):** start the viewer with `uv run finances serve`, open `http://localhost:8765/transactions` — chips toggle on tap/click, the list swaps via htmx on each toggle, URL updates (`hx-push-url`), "Clear filters" returns to the default 30-day view; open `http://localhost:8765/monthly` — since/until open the native month picker, "Clear filters" returns to the 6-month expense pivot. Check at a phone-width viewport (~375px) that chips hit the 44px tap-target rule. Stop the server when done.

- [ ] **Step 4.4 — stop and report.** Present the branch `ux-wp6-filters` and the six commits (3 test + 3 feat) to Julio. He runs the manual gate, marks WP6 complete, and decides merge — not the agent.
