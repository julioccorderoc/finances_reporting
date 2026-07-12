# WP4 Triage Picker, Keyboard, Bulk — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the 26-option native category `<select>` with a shared chips + type-to-filter picker in both edit modals, add modal-scoped keyboard shortcuts (1-8 / Enter / s), and add bulk categorize (row checkboxes + action bar + `POST /api/transactions/bulk-edit`) to `/transactions`.

**Architecture:** A new read-only service `finances/web/services/category_stats.py` ranks categories by usage; a new shared Jinja partial `partials/category_picker.html` (Alpine.js, vendored) renders chips + filter list and owns the `set_category`/`category_id` hidden inputs with a dirty flag so an untouched picker can never wipe a category. A new JSON endpoint in `finances/web/routers/api.py` loops the sanctioned `transactions_repo.update()` per id inside one DB transaction and reports success via the WP2 `HX-Trigger` toast contract.

**Tech Stack:** Python 3.13 + uv, FastAPI + Jinja2, Pydantic v2, sqlite3 stdlib, htmx + Alpine.js (vendored under `finances/web/static/vendor/`), pytest with the existing `tests/web/` tmp-DB fixtures.

## Global Constraints

- Run every command from the repo root: `/Users/juliocordero/Documents/finances_reporting`.
- Run tests with `uv run pytest -q <path>` — never bare `pytest`.
- TDD per rule-011 + CLAUDE.md execution rule 5: per task — write failing test → run → commit test (`test(scope): ...`) → minimal impl → run green → commit impl (`feat(scope): ...`).
- Pydantic v2 at every trust boundary (rule-009); repos accept/return Pydantic models, never raw dicts.
- The web viewer's ONLY transaction write path is `transactions_repo.update()` (rule-012) — no new `UPDATE transactions` SQL anywhere in `finances/web/`.
- `needs_review` is derived by the rate resolver — never exposed as a manual toggle (the bulk request model forbids it via `extra="forbid"`).
- No new dependencies, no CDN assets — only the already-vendored htmx + Alpine.js.
- Data lists are CSS Grid card-rows, never `<table>`.
- Real expense amounts are NEGATIVE; the `seeded_web_db` fixture stores them positive — new seed helpers in these tests use negative expense amounts and never rely on the fixture's sign.
- Tests never touch the real `finances.db` — only the tmp-DB fixtures in `tests/conftest.py` / `tests/web/conftest.py` (`web_db`, `seeded_web_db`, `web_client_factory`).
- Picker contract: shared partial `finances/web/templates/partials/category_picker.html`; sets a hidden input named `category_id`; chips come from `finances/web/services/category_stats.py::top_categories(conn, kind=None, limit=8, months=12) -> list[Category]`.
- Bulk contract: `POST /api/transactions/bulk-edit`; Pydantic `BulkEditRequest` with `ids: list[int]` (min length 1) and `category_id: int | None`; loops `transactions_repo.update()` per id in one DB transaction; JSON response `{"updated": N}`.
- Toast contract (WP2, consumed here): `<div id="toast-host">` in `base.html`; server sets `HX-Trigger` response header with JSON `{"toast": {"level": "success", "message": "..."}}` (`level` is `"success"` or `"error"`); a global `htmx:responseError` listener in `base.html` handles htmx failures.
- **Precondition check before Task 1:** WP2 must be landed. Verify with `grep -c 'id="toast-host"' finances/web/templates/base.html` → must print `1`. If `0`, STOP and report — this plan depends on WP2's toast infra.
- **Source-drift note:** templates below are quoted as of 2026-07-11 (pre-WP2 modal shape). WP2 touches the two hidden sentinel inputs (`set_category` / `set_user_rate`) in both modals. Where a task says "replace the category control block", remove *whatever* category-related control WP2 left (hidden `set_category` input + the category `<label>`), and leave WP2's `set_user_rate` handling exactly as found. Everything else matches source verbatim. WP2's modal-markup tests in `tests/web/test_safety_feedback.py` (`catDirty` sentinel, `remove category` string-absence, category `autofocus`) are **intentionally superseded** by this WP — step 3.8b updates them in the same commit that swaps the picker in; the server-contract tests there stay untouched.

---

### Task 1: `category_stats` service — `top_categories`

**Files:**
- Create: `finances/web/services/category_stats.py`
- Test: `tests/web/test_category_stats.py`

**Interfaces:**
- Consumes: `tests/web/conftest.py::web_db` fixture (file-backed tmp sqlite, migrations applied); repos `accounts_repo.insert`, `categories_repo.get_by_name/insert`, `transactions_repo.insert`; `finances.domain.models.Category / TransactionKind`.
- Produces: `top_categories(conn: sqlite3.Connection, kind: TransactionKind | str | None = None, limit: int = 8, months: int = 12) -> list[Category]` — consumed by Tasks 3 and 6.

- [ ] **1.1 Write the failing test file** `tests/web/test_category_stats.py` with exactly this content:

```python
"""WP4 — category usage stats service (tests precede impl per rule-011).

``top_categories`` ranks active categories by usage count over a trailing
window of calendar months, and pads with seed (id) order when history is
thin. Uses the tmp-DB web fixtures — never the real finances.db.

Seed helper note: real expense amounts are NEGATIVE (project sign
convention). Do not copy the positive-amount habit of the seeded_web_db
fixture.
"""

from __future__ import annotations

import sqlite3
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import uuid4

from finances.db.repos import accounts as accounts_repo
from finances.db.repos import categories as categories_repo
from finances.db.repos import transactions as transactions_repo
from finances.domain.models import (
    Account,
    AccountKind,
    Category,
    Transaction,
    TransactionKind,
)
from finances.web.services.category_stats import top_categories


def _seed_account(conn: sqlite3.Connection) -> int:
    acct = accounts_repo.insert(
        conn,
        Account(
            name="Provincial",
            kind=AccountKind.BANK,
            currency="VES",
            institution="Provincial",
        ),
    )
    assert acct.id is not None
    return acct.id


def _cat(conn: sqlite3.Connection, kind: TransactionKind, name: str) -> Category:
    cat = categories_repo.get_by_name(conn, kind, name)
    assert cat is not None, f"seed category {name} missing"
    return cat


def _use(
    conn: sqlite3.Connection,
    account_id: int,
    category_id: int | None,
    *,
    when: datetime,
    times: int = 1,
    kind: TransactionKind = TransactionKind.EXPENSE,
) -> None:
    """Insert ``times`` transactions in ``category_id`` dated ``when``."""
    amount = Decimal("-25.00") if kind == TransactionKind.EXPENSE else Decimal("25.00")
    for _ in range(times):
        transactions_repo.insert(
            conn,
            Transaction(
                account_id=account_id,
                occurred_at=when,
                kind=kind,
                amount=amount,
                currency="VES",
                description="category-stats seed",
                category_id=category_id,
                source="test",
                source_ref=f"cs-{uuid4()}",
            ),
        )


def test_orders_by_usage_count_desc(web_db: sqlite3.Connection) -> None:
    acct = _seed_account(web_db)
    yesterday = datetime.now(tz=UTC) - timedelta(days=1)
    groceries = _cat(web_db, TransactionKind.EXPENSE, "Groceries")
    transport = _cat(web_db, TransactionKind.EXPENSE, "Transport")
    health = _cat(web_db, TransactionKind.EXPENSE, "Health")
    _use(web_db, acct, groceries.id, when=yesterday, times=3)
    _use(web_db, acct, transport.id, when=yesterday, times=2)
    _use(web_db, acct, health.id, when=yesterday, times=1)

    result = top_categories(web_db, kind=TransactionKind.EXPENSE, limit=3)

    assert [c.name for c in result] == ["Groceries", "Transport", "Health"]


def test_usage_outside_window_does_not_rank(web_db: sqlite3.Connection) -> None:
    acct = _seed_account(web_db)
    two_years_ago = datetime.now(tz=UTC) - timedelta(days=730)
    yesterday = datetime.now(tz=UTC) - timedelta(days=1)
    dating = _cat(web_db, TransactionKind.EXPENSE, "Dating")
    groceries = _cat(web_db, TransactionKind.EXPENSE, "Groceries")
    _use(web_db, acct, dating.id, when=two_years_ago, times=10)
    _use(web_db, acct, groceries.id, when=yesterday, times=1)

    result = top_categories(web_db, kind=TransactionKind.EXPENSE, months=12)

    names = [c.name for c in result]
    # Groceries (1 in-window use) ranks first; Dating's 10 uses are outside the
    # window so it does not rank — and at the default limit=8 the seed-order pad
    # (Transport id 6 ... Fees id 12) fills the list before Dating (id 15).
    assert names[0] == "Groceries"
    assert "Dating" not in names


def test_kind_filter_limits_to_that_kind(web_db: sqlite3.Connection) -> None:
    acct = _seed_account(web_db)
    yesterday = datetime.now(tz=UTC) - timedelta(days=1)
    salary = _cat(web_db, TransactionKind.INCOME, "Salary")
    groceries = _cat(web_db, TransactionKind.EXPENSE, "Groceries")
    _use(web_db, acct, salary.id, when=yesterday, times=5, kind=TransactionKind.INCOME)
    _use(web_db, acct, groceries.id, when=yesterday, times=1)

    result = top_categories(web_db, kind=TransactionKind.EXPENSE)

    assert result
    assert all(c.kind == TransactionKind.EXPENSE for c in result)
    assert "Salary" not in [c.name for c in result]


def test_kind_accepts_plain_string(web_db: sqlite3.Connection) -> None:
    result = top_categories(web_db, kind="income", limit=4)

    assert len(result) == 4
    assert all(c.kind == TransactionKind.INCOME for c in result)


def test_default_limit_is_8(web_db: sqlite3.Connection) -> None:
    # 18 active expense categories exist from the seed migrations (002-005).
    result = top_categories(web_db, kind=TransactionKind.EXPENSE)

    assert len(result) == 8


def test_thin_history_falls_back_to_seed_order(web_db: sqlite3.Connection) -> None:
    # No transactions at all → first 8 active categories in id (seed) order.
    result = top_categories(web_db, limit=8)

    assert len(result) == 8
    expected = [
        int(r["id"])
        for r in web_db.execute(
            "SELECT id FROM categories WHERE active = 1 ORDER BY id ASC LIMIT 8"
        ).fetchall()
    ]
    assert [c.id for c in result] == expected


def test_inactive_categories_never_returned(web_db: sqlite3.Connection) -> None:
    acct = _seed_account(web_db)
    zombie = categories_repo.insert(
        web_db,
        Category(kind=TransactionKind.EXPENSE, name="Zombie", active=False),
    )
    _use(
        web_db,
        acct,
        zombie.id,
        when=datetime.now(tz=UTC) - timedelta(days=1),
        times=5,
    )

    result = top_categories(web_db, kind=TransactionKind.EXPENSE)

    assert "Zombie" not in [c.name for c in result]


def test_returns_pydantic_category_models(web_db: sqlite3.Connection) -> None:
    result = top_categories(web_db, limit=3)

    assert result
    assert all(isinstance(c, Category) for c in result)
```

- [ ] **1.2 Run it — expect FAIL** (module does not exist yet):

```bash
uv run pytest -q tests/web/test_category_stats.py
```

Expected: collection error — `ModuleNotFoundError: No module named 'finances.web.services.category_stats'`.

- [ ] **1.3 Commit the test:**

```bash
git add tests/web/test_category_stats.py
git commit -m "test(web): category usage stats service top_categories (WP4)"
```

- [ ] **1.4 Write the implementation** `finances/web/services/category_stats.py` with exactly this content:

```python
"""Category usage statistics for the shared picker (UX overhaul WP4).

``top_categories`` powers the top-8 chips in
``partials/category_picker.html``. Ranking = usage count over a trailing
window of calendar months; when history is thin the remainder is padded
with active categories in seed (id) order so chips are never empty on a
fresh DB.

Read-only module — SELECTs only (rule-012 governs writes, which stay in
``transactions_repo.update``).
"""

from __future__ import annotations

import sqlite3
from datetime import date

from finances.domain.models import Category, TransactionKind


def _cutoff_iso(months: int, *, today: date | None = None) -> str:
    """First day of the month ``months`` calendar months before today.

    Returned as an ISO ``YYYY-MM-DD`` string. ``occurred_at`` is stored
    as ISO text, so lexicographic ``>=`` comparison is correct.
    """
    anchor = today or date.today()
    total = anchor.year * 12 + (anchor.month - 1) - months
    year, month0 = divmod(total, 12)
    return date(year, month0 + 1, 1).isoformat()


def _row_to_category(row: sqlite3.Row) -> Category:
    return Category(
        id=row["id"],
        kind=TransactionKind(row["kind"]),
        name=row["name"],
        active=bool(row["active"]),
    )


def top_categories(
    conn: sqlite3.Connection,
    kind: TransactionKind | str | None = None,
    limit: int = 8,
    months: int = 12,
) -> list[Category]:
    """Most-used active categories over the trailing ``months`` window.

    Ordered by usage count (desc; ties broken by id = seed order). When
    fewer than ``limit`` categories have any usage in the window, the
    list is padded with the remaining active categories in seed (id)
    order. ``kind`` filters to one ``TransactionKind`` (enum or plain
    string); ``None`` mixes all kinds (used by the bulk action bar).
    """
    kind_value = kind.value if isinstance(kind, TransactionKind) else kind

    params: list[object] = [_cutoff_iso(months)]
    kind_sql = ""
    if kind_value is not None:
        kind_sql = "AND c.kind = ?"
        params.append(kind_value)
    params.append(limit)

    ranked_rows = conn.execute(
        f"""
        SELECT c.id AS id, c.kind AS kind, c.name AS name, c.active AS active,
               COUNT(t.id) AS uses
        FROM categories c
        JOIN transactions t ON t.category_id = c.id
        WHERE c.active = 1
          AND t.occurred_at >= ?
          {kind_sql}
        GROUP BY c.id
        ORDER BY uses DESC, c.id ASC
        LIMIT ?
        """,
        params,
    ).fetchall()
    result = [_row_to_category(r) for r in ranked_rows]

    if len(result) < limit:
        seen = {c.id for c in result}
        pad_sql = "SELECT id, kind, name, active FROM categories WHERE active = 1"
        pad_params: list[object] = []
        if kind_value is not None:
            pad_sql += " AND kind = ?"
            pad_params.append(kind_value)
        pad_sql += " ORDER BY id ASC"
        for row in conn.execute(pad_sql, pad_params):
            if row["id"] in seen:
                continue
            result.append(_row_to_category(row))
            if len(result) >= limit:
                break

    return result


__all__ = ["top_categories"]
```

- [ ] **1.5 Run — expect PASS:**

```bash
uv run pytest -q tests/web/test_category_stats.py
```

Expected: `8 passed`.

- [ ] **1.6 Commit the implementation:**

```bash
git add finances/web/services/category_stats.py
git commit -m "feat(web): add category_stats.top_categories usage-ranked chips service (WP4)"
```

---

### Task 2: Shared picker partial `category_picker.html`

**Files:**
- Create: `finances/web/templates/partials/category_picker.html`
- Test: `tests/web/test_category_picker.py`

**Interfaces:**
- Consumes: Jinja context vars `categories: list[Category]` (all active), `top_categories: list[Category]` (≤ 8), `picker_selected: int | None` (optional).
- Produces DOM contract (relied on by Tasks 3, 4, 6): hidden `<input name="set_category" value="false">` (Alpine-bound to a `dirty` flag), hidden `<input name="category_id">`, one `[data-chip="N"]` button per top category in DOM order (keys 1-8), `[data-picker-search]` input that swallows Enter, `[data-picker-remove]` explicit clear control, `[data-picker-item]` per category in the full list, root marker `data-category-picker`.

- [ ] **2.1 Write the failing test file** `tests/web/test_category_picker.py` with exactly this content:

```python
"""WP4 — shared category picker partial (tests precede impl per rule-011).

Direct template rendering, same pattern as
tests/web/test_transactions_write.py::test_card_partial_*. DOM contract:

* hidden <input name="set_category" value="false"> — an untouched picker
  can never wipe a category (WP2 safety contract),
* hidden <input name="category_id"> initialised from ``picker_selected``,
* one [data-chip] button per top_categories entry (DOM order = keys 1-8),
* [data-picker-search] type-to-filter input that swallows Enter,
* [data-picker-remove] explicit clear control,
* every active category rendered in the full list ([data-picker-item]).
"""

from __future__ import annotations

from finances.domain.models import Category, TransactionKind
from finances.web.app import create_app
from finances.web.settings import WebSettings


def _mk_cats() -> list[Category]:
    names = [
        "Groceries",
        "Transport",
        "Health",
        "Leisure",
        "Subscriptions",
        "Purchases",
        "Fees",
        "Clothing",
        "Dating",
        "Gifts",
    ]
    return [
        Category(id=i + 1, kind=TransactionKind.EXPENSE, name=name, active=True)
        for i, name in enumerate(names)
    ]


def _render(
    categories: list[Category],
    top: list[Category],
    selected: int | None = None,
) -> str:
    app = create_app(WebSettings(host="127.0.0.1"))
    return app.state.templates.get_template(
        "partials/category_picker.html"
    ).render(categories=categories, top_categories=top, picker_selected=selected)


def test_renders_hidden_inputs_with_safe_defaults() -> None:
    cats = _mk_cats()
    rendered = _render(cats, cats[:8])

    assert "data-category-picker" in rendered
    assert 'name="set_category" value="false"' in rendered  # untouched → never wipes
    assert 'name="category_id"' in rendered


def test_initial_selection_prefills_hidden_input() -> None:
    cats = _mk_cats()
    rendered = _render(cats, cats[:8], selected=3)

    assert 'name="category_id" value="3"' in rendered


def test_renders_one_chip_per_top_category_in_order() -> None:
    cats = _mk_cats()
    rendered = _render(cats, cats[:8])

    assert rendered.count("data-chip=") == 8
    for n in range(1, 9):
        assert f'data-chip="{n}"' in rendered
    # Chip order follows top_categories order.
    assert rendered.index("Groceries") < rendered.index("Transport")


def test_renders_search_input_and_full_list() -> None:
    cats = _mk_cats()
    rendered = _render(cats, cats[:8])

    assert "data-picker-search" in rendered
    # Enter in the search box must never submit the surrounding form.
    assert "@keydown.enter.prevent" in rendered
    for cat in cats:
        assert cat.name in rendered
    assert rendered.count("data-picker-item=") == len(cats)


def test_renders_explicit_remove_control() -> None:
    cats = _mk_cats()
    rendered = _render(cats, cats[:8], selected=1)

    assert "data-picker-remove" in rendered
    assert "remove category" in rendered
```

- [ ] **2.2 Run it — expect FAIL:**

```bash
uv run pytest -q tests/web/test_category_picker.py
```

Expected: `5 failed` — each with `jinja2.exceptions.TemplateNotFound: partials/category_picker.html`.

- [ ] **2.3 Commit the test:**

```bash
git add tests/web/test_category_picker.py
git commit -m "test(web): shared category picker partial DOM contract (WP4)"
```

- [ ] **2.4 Write the partial** `finances/web/templates/partials/category_picker.html` with exactly this content. Attribute-quoting convention: attributes whose value embeds `| tojson` output use SINGLE quotes (tojson escapes `'` but not `"`); everything else uses double quotes.

```html
{# Shared category picker (UX overhaul WP4).

   Top-used chips + Alpine type-to-filter list over all active
   categories. Sets the hidden ``category_id`` input; ``set_category``
   stays "false" until the user actually picks or removes a category,
   so an untouched picker can never wipe an existing category (WP2
   safety contract). Clearing is the explicit "remove category" control.

   Context:
     categories       list[Category] — all active categories (full list)
     top_categories   list[Category] — most-used chips (<= 8),
                      from services/category_stats.top_categories
     picker_selected  int | None     — initial selection (optional)

   Keyboard: chips carry data-chip="1".."8" in DOM order so the
   modal-level handler (Task 4) maps keys 1-8 onto them.
#}
{% set _initial = picker_selected if picker_selected is defined and picker_selected is not none else none %}
{% set ns = namespace(initial_name=none) %}
{% for cat in categories %}
  {%- if _initial is not none and cat.id == _initial %}{% set ns.initial_name = cat.name %}{% endif -%}
{% endfor %}
<div
  data-category-picker
  class="space-y-2"
  x-data='{
    selected: {{ _initial if _initial is not none else "null" }},
    selectedName: {{ ns.initial_name | tojson }},
    dirty: false,
    query: "",
    pick(id, name) { this.selected = id; this.selectedName = name; this.dirty = true; },
    matches(name) { return name.includes(this.query.toLowerCase().trim()); },
  }'
>
  <input type="hidden" name="set_category" value="false" :value="dirty ? 'true' : 'false'">
  <input type="hidden" name="category_id" value="{{ _initial if _initial is not none else '' }}" :value="selected === null ? '' : selected">

  {# Top-used chips (keys 1-8). #}
  <div class="flex flex-wrap gap-1.5" data-picker-chips>
    {% for cat in top_categories %}
      <button
        type="button"
        data-chip="{{ loop.index }}"
        @click='pick({{ cat.id }}, {{ cat.name | tojson }})'
        class="px-2.5 py-1.5 text-sm border rounded-full bg-white text-slate-700 border-slate-300 hover:bg-slate-50"
        :class="selected === {{ cat.id }} ? 'ring-2 ring-slate-900 border-slate-900 bg-slate-100' : ''"
      ><span class="text-[10px] text-slate-400 mr-1">{{ loop.index }}</span>{{ cat.name }}</button>
    {% endfor %}
  </div>

  {# Current selection + explicit clear control. #}
  <div class="flex items-center gap-2 text-xs text-slate-600" data-picker-current>
    <span x-show="selected !== null">Selected: <strong x-text="selectedName"></strong></span>
    <span x-show="selected === null" class="text-slate-400">no category</span>
    <button
      type="button"
      data-picker-remove
      x-show="selected !== null"
      @click="pick(null, null)"
      class="px-1.5 py-0.5 border border-rose-300 text-rose-700 rounded hover:bg-rose-50"
    >&times; remove category</button>
  </div>

  {# Type-to-filter list over all active categories. #}
  <input
    type="text"
    data-picker-search
    x-model="query"
    @keydown.enter.prevent
    placeholder="Filter categories…"
    autocomplete="off"
    class="w-full border border-slate-300 rounded px-2 py-1 text-sm"
  >
  <div class="max-h-40 overflow-y-auto border border-slate-200 rounded divide-y divide-slate-100" data-picker-list>
    {% for cat in categories %}
      <button
        type="button"
        data-picker-item="{{ cat.id }}"
        x-show='matches({{ cat.name | lower | tojson }})'
        @click='pick({{ cat.id }}, {{ cat.name | tojson }})'
        class="w-full flex items-baseline justify-between gap-2 px-2 py-1 text-left text-sm hover:bg-slate-50"
        :class="selected === {{ cat.id }} ? 'bg-slate-100 font-semibold' : ''"
      >
        <span>{{ cat.name }}</span>
        <span class="text-[10px] uppercase tracking-wide text-slate-400">{{ cat.kind.value }}</span>
      </button>
    {% endfor %}
  </div>
</div>
```

- [ ] **2.5 Run — expect PASS:**

```bash
uv run pytest -q tests/web/test_category_picker.py
```

Expected: `5 passed`.

- [ ] **2.5b Rebuild the vendored Tailwind sheet.** `finances/web/static/css/tailwind.css` is a compiled artifact containing only utility classes found in the templates at build time (see `tailwind/README.md`). The picker and the Task 6 bulk bar introduce classes NOT in the current artifact (`ring-2`, `ring-slate-900`, `divide-y`, `divide-slate-100`, `max-h-40`, `overflow-y-auto`, `space-y-2`, `hover:bg-rose-50`) — without a rebuild the chip selection ring is invisible and the filter list won't scroll. All pytest gates are markup-only and will NOT catch this. Run the README recipe exactly:

```bash
npx -y tailwindcss@3.4.17 \
  -c tailwind/tailwind.config.js \
  -i tailwind/input.css \
  -o finances/web/static/css/tailwind.css \
  --minify

# Strip Tailwind's license banner so the vendored CSS carries no external URL.
python - <<'PY'
import re, pathlib
p = pathlib.Path("finances/web/static/css/tailwind.css")
p.write_text(re.sub(r"/\*!.*?\*/", "", p.read_text(), flags=re.S))
PY
```

Then verify the new classes compiled in:

```bash
grep -c "max-h-40" finances/web/static/css/tailwind.css
```

Expected: `1` (or more). If `0`: the picker template is outside `tailwind.config.js` `content` globs — confirm `templates/**/*.html` covers `partials/category_picker.html`, fix the glob, rebuild. Repeat this step at the end of Task 6 (its `transactions.html` rewrite also adds classes) — same rebuild command, then verify with `grep -c "bg-rose-50" finances/web/static/css/tailwind.css` (expected ≥ 1; the compiled selector escapes the `hover:` prefix, so grep for the unprefixed class name).

- [ ] **2.6 Commit the implementation:**

```bash
git add finances/web/templates/partials/category_picker.html finances/web/static/css/tailwind.css
git commit -m "feat(web): shared category picker partial - top chips + type-to-filter (WP4)"
```

---

### Task 3: Wire the picker into BOTH edit modals

**Files:**
- Modify: `finances/web/routers/partials.py` (imports ~line 25; `transactions_modal_partial` context ~line 243; `triage_modal_partial` context ~line 380)
- Modify: `finances/web/templates/partials/modal_transaction.html` (macro import lines 20-22; form block lines 102-109)
- Modify: `finances/web/templates/partials/modal_transaction_triage.html` (macro import lines 14-16; form block lines 78-84)
- Test: `tests/web/test_modal_picker.py`

**Interfaces:**
- Consumes: `top_categories(conn, kind=txn.kind)` from Task 1; the picker partial + its DOM contract from Task 2; existing context var `categories` (already `categories_repo.list_all(conn)` in both modal endpoints).
- Produces: both modal responses contain `data-category-picker` markup with `top_categories` in context; the native `category_select` macro is no longer rendered (the macro itself stays in `_macros.html`, unused).

- [ ] **3.1 Write the failing test file** `tests/web/test_modal_picker.py` with exactly this content:

```python
"""WP4 — both edit modals render the shared category picker (tests first).

The 26-option native ``category_select`` macro is replaced by
partials/category_picker.html in modal_transaction.html AND
modal_transaction_triage.html. The server passes ``top_categories``
computed per the transaction's kind.

Two tests here are regression GUARDS and pass before the impl commit
(full category list still present; untouched picker never wipes) — the
other three fail first, per rule-011.
"""

from __future__ import annotations

import sqlite3

from finances.db.repos import categories as categories_repo
from finances.db.repos import transactions as transactions_repo


def _txn_id(conn: sqlite3.Connection, source_ref: str) -> int:
    row = conn.execute(
        "SELECT id FROM transactions WHERE source_ref = ?", (source_ref,)
    ).fetchone()
    assert row is not None, f"seeded txn {source_ref} not present"
    return int(row["id"])


def test_edit_modal_renders_picker_not_native_select(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    client = web_client_factory()
    txn_id = _txn_id(seeded_web_db, "prov-1")

    resp = client.get(f"/_partial/transactions/{txn_id}/modal")
    assert resp.status_code == 200
    body = resp.text

    assert "data-category-picker" in body
    assert "data-picker-search" in body
    assert 'name="category_id"' in body
    # The native select's empty-option label was unique to it — gone now.
    assert "— no category —" not in body


def test_edit_modal_picker_has_top_chips(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    client = web_client_factory()
    txn_id = _txn_id(seeded_web_db, "prov-1")  # expense txn

    resp = client.get(f"/_partial/transactions/{txn_id}/modal")
    assert resp.status_code == 200
    body = resp.text

    # Groceries is the most-used expense category in seeded_web_db → a chip.
    assert 'data-chip="1"' in body
    assert "Groceries" in body


def test_triage_modal_renders_picker(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    client = web_client_factory()
    txn_id = _txn_id(seeded_web_db, "prov-needs-review")

    resp = client.get(f"/_partial/triage/{txn_id}/modal")
    assert resp.status_code == 200
    body = resp.text

    assert "data-category-picker" in body
    assert "data-picker-search" in body
    assert "— no category —" not in body


def test_edit_modal_still_lists_every_active_category(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    """GUARD (passes pre-impl too): the full list survives the swap."""
    client = web_client_factory()
    txn_id = _txn_id(seeded_web_db, "prov-1")

    body = client.get(f"/_partial/transactions/{txn_id}/modal").text

    for cat in categories_repo.list_all(seeded_web_db):
        assert cat.name in body


def test_untouched_picker_submission_preserves_category(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    """GUARD: the picker's default form shape (set_category=false) must not wipe."""
    client = web_client_factory()
    txn_id = _txn_id(seeded_web_db, "prov-1")
    before = transactions_repo.get_by_id(seeded_web_db, txn_id)
    assert before is not None and before.category_id is not None

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

    after = transactions_repo.get_by_id(seeded_web_db, txn_id)
    assert after is not None
    assert after.category_id == before.category_id
```

- [ ] **3.2 Run it — expect FAIL:**

```bash
uv run pytest -q tests/web/test_modal_picker.py
```

Expected: `3 failed, 2 passed` (the two GUARD tests pass; the three picker-markup tests fail with `AssertionError` on `data-category-picker`).

- [ ] **3.3 Commit the test:**

```bash
git add tests/web/test_modal_picker.py
git commit -m "test(web): edit + triage modals render shared category picker (WP4)"
```

- [ ] **3.4 Edit `finances/web/routers/partials.py` — import the service.** Replace:

```python
from finances.web.services.dashboard import build_sync_status
from finances.web.services.transactions_query import _project_card
```

with:

```python
from finances.web.services.category_stats import top_categories
from finances.web.services.dashboard import build_sync_status
from finances.web.services.transactions_query import _project_card
```

- [ ] **3.5 Edit `finances/web/routers/partials.py` — pass chips to the transactions modal.** In `transactions_modal_partial`, replace:

```python
    return templates.TemplateResponse(
        request,
        "partials/modal_transaction.html",
        {
            "txn": txn,
            "card": card,
            "categories": categories,
            "account_name": account_name,
        },
    )
```

with:

```python
    return templates.TemplateResponse(
        request,
        "partials/modal_transaction.html",
        {
            "txn": txn,
            "card": card,
            "categories": categories,
            "top_categories": top_categories(conn, kind=txn.kind),
            "account_name": account_name,
        },
    )
```

- [ ] **3.6 Edit `finances/web/routers/partials.py` — same for the triage modal.** In `triage_modal_partial`, replace:

```python
    return templates.TemplateResponse(
        request,
        "partials/modal_transaction_triage.html",
        {
            "txn": txn,
            "card": card,
            "categories": categories,
            "account_name": account_name,
        },
    )
```

with:

```python
    return templates.TemplateResponse(
        request,
        "partials/modal_transaction_triage.html",
        {
            "txn": txn,
            "card": card,
            "categories": categories,
            "top_categories": top_categories(conn, kind=txn.kind),
            "account_name": account_name,
        },
    )
```

- [ ] **3.7 Edit `finances/web/templates/partials/modal_transaction.html`.** Two changes (see Source-drift note in Global Constraints — if WP2 reshaped the sentinel inputs, remove WP2's category control instead; keep WP2's `set_user_rate` control verbatim):

Change A — drop `category_select` from the macro import. Replace:

```html
{% from "_macros.html" import rate_source_badge, needs_review_badge,
                              kind_class, format_date, format_amount,
                              category_select %}
```

with:

```html
{% from "_macros.html" import rate_source_badge, needs_review_badge,
                              kind_class, format_date, format_amount %}
```

Change B — replace the category control block. Replace:

```html
      {# Always-set sentinels: empty value clears the field. #}
      <input type="hidden" name="set_category" value="true">
      <input type="hidden" name="set_user_rate" value="true">

      <label class="block">
        <span class="text-xs uppercase tracking-wide text-slate-500">Category</span>
        {{ category_select("category_id", categories, selected=txn.category_id) }}
      </label>
```

with:

```html
      {# user_rate keep-set sentinel; the picker owns set_category +
         category_id and only marks set_category=true once the user
         actually picks or removes a category. #}
      <input type="hidden" name="set_user_rate" value="true">

      <div class="block">
        <span class="text-xs uppercase tracking-wide text-slate-500">Category</span>
        {% with picker_selected = txn.category_id %}
          {% include "partials/category_picker.html" %}
        {% endwith %}
      </div>
```

- [ ] **3.8 Edit `finances/web/templates/partials/modal_transaction_triage.html`.** Same two changes:

Change A — replace:

```html
{% from "_macros.html" import rate_source_badge, needs_review_badge,
                              kind_class, format_date, format_amount,
                              category_select %}
```

with:

```html
{% from "_macros.html" import rate_source_badge, needs_review_badge,
                              kind_class, format_date, format_amount %}
```

Change B — replace:

```html
      <input type="hidden" name="set_category" value="true">
      <input type="hidden" name="set_user_rate" value="true">

      <label class="block">
        <span class="text-xs uppercase tracking-wide text-slate-500">Category</span>
        {{ category_select("category_id", categories, selected=txn.category_id) }}
      </label>
```

with:

```html
      <input type="hidden" name="set_user_rate" value="true">

      <div class="block">
        <span class="text-xs uppercase tracking-wide text-slate-500">Category</span>
        {% with picker_selected = txn.category_id %}
          {% include "partials/category_picker.html" %}
        {% endwith %}
      </div>
```

- [ ] **3.8b Supersede WP2's modal-markup tests (same commit as the picker swap).** Skip this step only if WP2 has not landed (no `tests/web/test_safety_feedback.py`). The picker intentionally replaces three markup contracts WP2's tests pinned, so those tests must be updated here — Task 7's "no fixing while in here" rule does not apply to failures this task itself causes. In `tests/web/test_safety_feedback.py`, make these exact substitutions (the server-contract endpoint tests — `test_modal_untouched_fields_do_not_wipe`, `test_modal_explicit_remove_payload_clears_category` — stay untouched; the picker keeps the same form contract):

In `test_modal_no_hardcoded_set_sentinels`, replace:

```python
    # ...replaced by Alpine dirty-tracking bindings.
    assert "catDirty" in body
    assert "rateDirty" in body
```

with:

```python
    # ...replaced by the picker's untouched-default sentinel (WP4) and
    # WP2's rate dirty-tracking, which the picker leaves in place.
    assert 'name="set_category" value="false"' in body
    assert "rateDirty" in body
```

In `test_modal_hides_remove_category_control_when_uncategorized`, replace:

```python
    assert "remove category" not in body
```

with:

```python
    # WP4's picker always server-renders the control and hides it at
    # runtime via Alpine — assert the binding, not string absence.
    assert 'x-show="selected !== null"' in body
```

Replace `test_modal_category_control_has_autofocus` entirely (the focus contract moves to the `tabindex="-1"` modal card in Task 4, covered by `tests/web/test_modal_keyboard.py`):

```python
def test_modal_category_control_is_the_shared_picker(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    client = web_client_factory()
    txn_id = _txn_id(seeded_web_db, "prov-1")
    body = client.get(f"/_partial/transactions/{txn_id}/modal").text
    assert "data-category-picker" in body
```

Apply the same first two substitutions to their triage-modal duplicates in the same file (the `catDirty`/`rateDirty` assertion and the `"remove category" not in body` assertion against `/_partial/triage/{id}/modal`).

- [ ] **3.9 Run new tests AND the pre-existing modal tests — expect PASS:**

```bash
uv run pytest -q tests/web/test_modal_picker.py tests/web/test_transactions_write.py tests/web/test_triage.py tests/web/test_safety_feedback.py
```

Expected: all pass (`test_modal_partial_includes_all_categories_in_dropdown` keeps passing because the picker's full list renders every active category name; `'name="category_id"'` still present via the hidden input). Drop `tests/web/test_safety_feedback.py` from the command if WP2 has not landed.

- [ ] **3.10 Commit the implementation:**

```bash
git add finances/web/routers/partials.py finances/web/templates/partials/modal_transaction.html finances/web/templates/partials/modal_transaction_triage.html tests/web/test_safety_feedback.py
git commit -m "feat(web): wire shared category picker into edit + triage modals (WP4)"
```

---

### Task 4: Modal-scoped keyboard — 1-8 chips, Enter save, s skip

**Files:**
- Modify: `finances/web/templates/partials/modal_transaction.html` (overlay `<div>` lines 24-31)
- Modify: `finances/web/templates/partials/modal_transaction_triage.html` (overlay `<div>` lines 18-25; Skip button lines 108-114)
- Test: `tests/web/test_modal_keyboard.py`

**Interfaces:**
- Consumes: `[data-chip]` DOM contract from Task 2; existing `form.tx-modal-form` + submit button in both modals; existing Skip button (`hx-post="/_partial/triage/skip/txn:{{ txn.id }}"`) in the triage modal.
- Produces: `[data-skip-btn]` marker on the triage Skip button; a `@keydown.window` handler on the overlay that is inert while typing (target is `input/textarea/select/button`); `tabindex="-1"` + focus on `.tx-modal-card` so keys land on the handler immediately. The handler lives on the modal element, which only exists while the modal is open — that is the scoping.

- [ ] **4.1 Write the failing test file** `tests/web/test_modal_keyboard.py` with exactly this content:

```python
"""WP4 — keyboard shortcuts on the transaction modals (tests first).

Server-rendered markup contract only (JS behaviour is not executable
under pytest; the manual gate covers it): the overlay carries a
window-scoped keydown handler that (1) ignores keystrokes while typing
in form controls, (2) maps keys 1-8 to [data-chip] clicks, (3) maps
Enter to the form's submit button (= Save & next in the triage modal),
(4) maps s to [data-skip-btn] — triage modal only. Esc close pre-exists.
"""

from __future__ import annotations

import sqlite3


def _txn_id(conn: sqlite3.Connection, source_ref: str) -> int:
    row = conn.execute(
        "SELECT id FROM transactions WHERE source_ref = ?", (source_ref,)
    ).fetchone()
    assert row is not None
    return int(row["id"])


def test_edit_modal_has_scoped_keydown_handler(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    client = web_client_factory()
    txn_id = _txn_id(seeded_web_db, "prov-1")

    body = client.get(f"/_partial/transactions/{txn_id}/modal").text

    assert "@keydown.window=" in body
    assert "isTyping($event)" in body      # inert while typing in inputs
    assert "data-chip" in body             # 1-8 targets exist
    assert 'tabindex="-1"' in body         # card takes focus → keys land here
    assert "data-skip-btn" not in body     # skip is triage-only


def test_triage_modal_has_keydown_handler_and_skip_key(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    client = web_client_factory()
    txn_id = _txn_id(seeded_web_db, "prov-needs-review")

    body = client.get(f"/_partial/triage/{txn_id}/modal").text

    assert "@keydown.window=" in body
    assert "isTyping($event)" in body
    assert "data-skip-btn" in body
    assert 'tabindex="-1"' in body
```

- [ ] **4.2 Run it — expect FAIL:**

```bash
uv run pytest -q tests/web/test_modal_keyboard.py
```

Expected: `2 failed` — `AssertionError` on `"@keydown.window=" in body`.

- [ ] **4.3 Commit the test:**

```bash
git add tests/web/test_modal_keyboard.py
git commit -m "test(web): modal keyboard shortcuts markup - 1-8 chips, Enter save, s skip (WP4)"
```

- [ ] **4.4 Edit `finances/web/templates/partials/modal_transaction.html`.** Replace the overlay opening block:

```html
<div
  class="tx-modal-overlay"
  data-tx-modal
  data-tx-id="{{ txn.id }}"
  x-data="{}"
  @keydown.escape.window="window.dispatchEvent(new CustomEvent('close-modal'))"
>
  <div class="tx-modal-card" role="dialog" aria-modal="true">
```

with:

```html
<div
  class="tx-modal-overlay"
  data-tx-modal
  data-tx-id="{{ txn.id }}"
  x-data="{
    isTyping(e) {
      const t = e.target;
      return t instanceof Element && t.matches('input, textarea, select, button');
    }
  }"
  x-init="$nextTick(() => { const c = $el.querySelector('.tx-modal-card'); if (c) c.focus(); })"
  @keydown.escape.window="window.dispatchEvent(new CustomEvent('close-modal'))"
  @keydown.window="
    if (isTyping($event)) return;
    const key = $event.key;
    if (key.length === 1 && key >= '1' && key <= '8') {
      const chip = $el.querySelectorAll('[data-chip]')[Number(key) - 1];
      if (chip) { chip.click(); $event.preventDefault(); }
    } else if (key === 'Enter') {
      const save = $el.querySelector('form.tx-modal-form button[type=submit]');
      if (save) { save.click(); $event.preventDefault(); }
    }
  "
>
  <div class="tx-modal-card" role="dialog" aria-modal="true" tabindex="-1">
```

- [ ] **4.5 Edit `finances/web/templates/partials/modal_transaction_triage.html`.** Two changes:

Change A — replace the overlay opening block:

```html
<div
  class="tx-modal-overlay"
  data-tx-modal="triage"
  data-tx-id="{{ txn.id }}"
  x-data="{}"
  @keydown.escape.window="window.dispatchEvent(new CustomEvent('close-modal'))"
>
  <div class="tx-modal-card" role="dialog" aria-modal="true">
```

with:

```html
<div
  class="tx-modal-overlay"
  data-tx-modal="triage"
  data-tx-id="{{ txn.id }}"
  x-data="{
    isTyping(e) {
      const t = e.target;
      return t instanceof Element && t.matches('input, textarea, select, button');
    }
  }"
  x-init="$nextTick(() => { const c = $el.querySelector('.tx-modal-card'); if (c) c.focus(); })"
  @keydown.escape.window="window.dispatchEvent(new CustomEvent('close-modal'))"
  @keydown.window="
    if (isTyping($event)) return;
    const key = $event.key;
    if (key.length === 1 && key >= '1' && key <= '8') {
      const chip = $el.querySelectorAll('[data-chip]')[Number(key) - 1];
      if (chip) { chip.click(); $event.preventDefault(); }
    } else if (key === 'Enter') {
      const save = $el.querySelector('form.tx-modal-form button[type=submit]');
      if (save) { save.click(); $event.preventDefault(); }
    } else if (key === 's' || key === 'S') {
      const skip = $el.querySelector('[data-skip-btn]');
      if (skip) { skip.click(); $event.preventDefault(); }
    }
  "
>
  <div class="tx-modal-card" role="dialog" aria-modal="true" tabindex="-1">
```

Change B — tag the Skip button. Replace:

```html
        <button
          type="button"
          class="px-3 py-1.5 text-sm border border-amber-300 rounded bg-amber-50 text-amber-800 hover:bg-amber-100"
          hx-post="/_partial/triage/skip/txn:{{ txn.id }}"
          hx-target="#triage-queue"
          hx-swap="innerHTML"
        >Skip → bottom</button>
```

with:

```html
        <button
          type="button"
          data-skip-btn
          class="px-3 py-1.5 text-sm border border-amber-300 rounded bg-amber-50 text-amber-800 hover:bg-amber-100"
          hx-post="/_partial/triage/skip/txn:{{ txn.id }}"
          hx-target="#triage-queue"
          hx-swap="innerHTML"
        >Skip → bottom</button>
```

- [ ] **4.6 Run — expect PASS (plus modal regression):**

```bash
uv run pytest -q tests/web/test_modal_keyboard.py tests/web/test_modal_picker.py tests/web/test_transactions_write.py
```

Expected: all pass.

- [ ] **4.7 Commit the implementation:**

```bash
git add finances/web/templates/partials/modal_transaction.html finances/web/templates/partials/modal_transaction_triage.html
git commit -m "feat(web): scoped modal keyboard - 1-8 pick chip, Enter save, s skip (WP4)"
```

---

### Task 5: `POST /api/transactions/bulk-edit`

**Files:**
- Modify: `finances/web/routers/api.py` (imports lines 11-20 and 51-61; new models + handler inserted directly after `transactions_patch`, ~line 99)
- Test: `tests/web/test_bulk_edit.py`

**Interfaces:**
- Consumes: `transactions_repo.update(conn, *, id, category_id=...)` (the sanctioned write path, raises `LookupError` on unknown id), `categories_repo.get_by_id`, `deps.get_conn` (autocommit connection → explicit `BEGIN`/`COMMIT` makes the loop atomic).
- Produces: `BulkEditRequest` (`ids: list[int]` min length 1, `category_id: int | None`), `BulkEditResponse` (`updated: int`), route `POST /api/transactions/bulk-edit` returning `{"updated": N}` + `HX-Trigger` toast header — consumed by Task 6's Apply button.
- **Unknown-id decision (shown in code):** any unknown transaction id rolls back the WHOLE batch and returns 404 — all-or-nothing, no partial bulk applies.

- [ ] **5.1 Write the failing test file** `tests/web/test_bulk_edit.py` with exactly this content:

```python
"""WP4 — POST /api/transactions/bulk-edit (tests precede impl per rule-011).

Contract:
* Pydantic ``BulkEditRequest`` — ids: list[int] (min length 1),
  category_id: int | None (None = explicit bulk clear),
* handler loops the sanctioned ``transactions_repo.update()`` per id
  inside ONE DB transaction (rule-012 — no parallel UPDATE SQL),
* unknown txn id → 404 and the whole batch rolls back (all-or-nothing),
* unknown category id → 422, empty ids → 422,
* JSON response {"updated": N} + HX-Trigger toast header (WP2 contract),
* needs_review untouched (derived-only, never a manual toggle).

The rule-012 tripwire test passes pre-impl (guard); everything else
fails first because the route does not exist yet (404 vs expected).
"""

from __future__ import annotations

import json
import sqlite3

from finances.db.repos import categories as categories_repo
from finances.db.repos import transactions as transactions_repo
from finances.domain.models import TransactionKind


def _txn_id(conn: sqlite3.Connection, source_ref: str) -> int:
    row = conn.execute(
        "SELECT id FROM transactions WHERE source_ref = ?", (source_ref,)
    ).fetchone()
    assert row is not None, f"seeded txn {source_ref} not present"
    return int(row["id"])


def _transport_id(conn: sqlite3.Connection) -> int:
    cat = categories_repo.get_by_name(conn, TransactionKind.EXPENSE, "Transport")
    assert cat is not None and cat.id is not None
    return cat.id


def test_bulk_assign_happy_path(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    client = web_client_factory()
    ids = [_txn_id(seeded_web_db, r) for r in ("prov-1", "prov-2", "cash-1")]
    target = _transport_id(seeded_web_db)

    resp = client.post(
        "/api/transactions/bulk-edit", json={"ids": ids, "category_id": target}
    )

    assert resp.status_code == 200, resp.text
    assert resp.json() == {"updated": 3}
    for txn_id in ids:
        txn = transactions_repo.get_by_id(seeded_web_db, txn_id)
        assert txn is not None and txn.category_id == target


def test_bulk_edit_sends_hx_trigger_toast(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    client = web_client_factory()
    ids = [_txn_id(seeded_web_db, "prov-1")]

    resp = client.post(
        "/api/transactions/bulk-edit",
        json={"ids": ids, "category_id": _transport_id(seeded_web_db)},
    )

    assert resp.status_code == 200
    trigger = json.loads(resp.headers["HX-Trigger"])
    assert trigger == {"toast": {"level": "success", "message": "1 updated"}}


def test_bulk_edit_empty_ids_is_422(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    client = web_client_factory()

    resp = client.post(
        "/api/transactions/bulk-edit", json={"ids": [], "category_id": 1}
    )

    assert resp.status_code == 422


def test_bulk_edit_unknown_txn_rolls_back_whole_batch(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    client = web_client_factory()
    good = _txn_id(seeded_web_db, "prov-1")
    before = transactions_repo.get_by_id(seeded_web_db, good)
    assert before is not None
    target = _transport_id(seeded_web_db)
    assert target != before.category_id

    resp = client.post(
        "/api/transactions/bulk-edit",
        json={"ids": [good, 999_999], "category_id": target},
    )

    assert resp.status_code == 404
    after = transactions_repo.get_by_id(seeded_web_db, good)
    assert after is not None
    assert after.category_id == before.category_id  # rolled back, all-or-nothing


def test_bulk_edit_unknown_category_is_422(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    client = web_client_factory()
    good = _txn_id(seeded_web_db, "prov-1")

    resp = client.post(
        "/api/transactions/bulk-edit", json={"ids": [good], "category_id": 999_999}
    )

    assert resp.status_code == 422


def test_bulk_clear_with_null_category(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    client = web_client_factory()
    good = _txn_id(seeded_web_db, "prov-1")
    before = transactions_repo.get_by_id(seeded_web_db, good)
    assert before is not None and before.category_id is not None

    resp = client.post(
        "/api/transactions/bulk-edit", json={"ids": [good], "category_id": None}
    )

    assert resp.status_code == 200
    after = transactions_repo.get_by_id(seeded_web_db, good)
    assert after is not None and after.category_id is None


def test_bulk_edit_never_touches_needs_review(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    client = web_client_factory()
    flagged = _txn_id(seeded_web_db, "prov-needs-review")

    resp = client.post(
        "/api/transactions/bulk-edit",
        json={"ids": [flagged], "category_id": _transport_id(seeded_web_db)},
    )

    assert resp.status_code == 200
    after = transactions_repo.get_by_id(seeded_web_db, flagged)
    assert after is not None and after.needs_review is True  # still derived, still flagged


def test_bulk_edit_rejects_extra_fields(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    client = web_client_factory()
    good = _txn_id(seeded_web_db, "prov-1")

    resp = client.post(
        "/api/transactions/bulk-edit",
        json={"ids": [good], "category_id": 1, "needs_review": False},
    )

    assert resp.status_code == 422  # extra=forbid; needs_review is never a manual toggle


def test_web_router_contains_no_raw_transaction_update_sql() -> None:
    """GUARD (rule-012 tripwire): the API router never writes UPDATE SQL."""
    import inspect

    from finances.web.routers import api as api_module

    src = inspect.getsource(api_module)
    assert "UPDATE transactions" not in src
```

- [ ] **5.2 Run it — expect FAIL:**

```bash
uv run pytest -q tests/web/test_bulk_edit.py
```

Expected: `8 failed, 1 passed` — the endpoint tests get `404` from the missing route (assert on status code fails); the rule-012 tripwire GUARD passes.

- [ ] **5.3 Commit the test:**

```bash
git add tests/web/test_bulk_edit.py
git commit -m "test(web): POST /api/transactions/bulk-edit contract - atomic, rule-012, toast (WP4)"
```

- [ ] **5.4 Edit `finances/web/routers/api.py` — imports.** Three replacements:

Replace:

```python
import sqlite3
from datetime import UTC, date, datetime
from typing import Literal

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from pydantic import BaseModel, ConfigDict
```

with:

```python
import json
import sqlite3
from datetime import UTC, date, datetime
from typing import Literal

from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response
from pydantic import BaseModel, ConfigDict, Field
```

Then replace:

```python
from finances.web.deps import get_conn
```

with:

```python
from finances.db.repos import categories as categories_repo
from finances.db.repos import transactions as transactions_repo
from finances.web.deps import get_conn
```

- [ ] **5.5 Edit `finances/web/routers/api.py` — add models + handler.** Insert the following block immediately AFTER the closing lines of the `transactions_patch` function (after `raise HTTPException(status_code=422, detail=str(exc)) from exc` and before the `# Dashboard endpoints (Phase 2a).` comment banner):

```python
# ---------------------------------------------------------------------------
# Bulk edit (UX overhaul WP4).
# ---------------------------------------------------------------------------


class BulkEditRequest(BaseModel):
    """Body for POST /api/transactions/bulk-edit.

    ``category_id: null`` is an explicit bulk clear — the UI only sends
    it via the picker's "remove category" control, never as a side
    effect of an untouched picker. ``extra="forbid"`` keeps
    ``needs_review`` (derived-only) and everything else out.
    """

    model_config = ConfigDict(extra="forbid")

    ids: list[int] = Field(min_length=1)
    category_id: int | None = None


class BulkEditResponse(BaseModel):
    """JSON shape returned by POST /api/transactions/bulk-edit."""

    model_config = ConfigDict(extra="forbid")

    updated: int


@router.post("/transactions/bulk-edit", response_model=BulkEditResponse)
def transactions_bulk_edit(
    body: BulkEditRequest,
    response: Response,
    conn: sqlite3.Connection = Depends(get_conn),
) -> BulkEditResponse:
    """Assign (or explicitly clear) one category on many transactions.

    Per rule-012 every row goes through the sanctioned
    ``transactions_repo.update()`` — no parallel UPDATE SQL in web code.
    The loop runs inside ONE DB transaction: an unknown id rolls the
    whole batch back and returns 404 (all-or-nothing — a partially
    applied bulk edit would be invisible to the user).

    Category is the only bulk-editable field in v1. ``needs_review`` is
    untouched: it is derived from the rate resolver, and category
    changes cannot affect it (see ADR-005 / ADR-012).
    """
    if body.category_id is not None:
        if categories_repo.get_by_id(conn, body.category_id) is None:
            raise HTTPException(
                status_code=422, detail=f"unknown category id={body.category_id}"
            )

    # deps.get_conn opens autocommit connections (isolation_level=None);
    # explicit BEGIN/COMMIT makes the per-row loop atomic.
    conn.execute("BEGIN")
    try:
        for txn_id in body.ids:
            transactions_repo.update(conn, id=txn_id, category_id=body.category_id)
    except LookupError as exc:
        conn.execute("ROLLBACK")
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception:
        conn.execute("ROLLBACK")
        raise
    conn.execute("COMMIT")

    updated = len(body.ids)
    response.headers["HX-Trigger"] = json.dumps(
        {"toast": {"level": "success", "message": f"{updated} updated"}}
    )
    return BulkEditResponse(updated=updated)
```

- [ ] **5.6 Run — expect PASS:**

```bash
uv run pytest -q tests/web/test_bulk_edit.py
```

Expected: `9 passed`.

- [ ] **5.7 Commit the implementation:**

```bash
git add finances/web/routers/api.py
git commit -m "feat(web): bulk-edit endpoint looping sanctioned repo update in one transaction (WP4)"
```

---

### Task 6: Bulk-select UI on /transactions

**Files:**
- Modify: `finances/web/routers/pages.py` (imports ~lines 18-22; `transactions_page` context ~lines 113-125)
- Modify: `finances/web/routers/partials.py` (`transactions_edit_partial` card-swap context ~line 286)
- Modify: `finances/web/templates/pages/transactions.html` (whole content block)
- Modify: `finances/web/templates/partials/transactions_list.html` (`.cards` block lines 35-54)
- Modify: `finances/web/templates/partials/card_transaction.html` (insert checkbox cell after the opening `<article ...>` tag, line 31)
- Modify: `finances/web/static/css/app.css` (append at end)
- Test: `tests/web/test_bulk_ui.py`

**Interfaces:**
- Consumes: picker partial + DOM contract (Task 2) via page-level context vars `categories` + `top_categories`; `top_categories(conn, kind=None)` (Task 1); `POST /api/transactions/bulk-edit` (Task 5); WP2 toast — the Apply handler dispatches the `window` CustomEvent **`show-toast`** with `detail = {level, message}` (the event WP2's `#toast-host` actually listens for; WP2's `base.html` parses `HX-Trigger` JSON and re-dispatches `show-toast`). The Apply POST uses `fetch`, which bypasses htmx's `HX-Trigger` handling, so it dispatches the event itself. Verify the event name against the landed `base.html` during the Task 1 precondition check.
- Produces: `[data-bulk-checkbox]` per row (value = txn id), `[data-bulk-select-all]` header checkbox, `#bulk-bar` action bar with `[data-bulk-apply]`, CSS variant `cards--selectable` (prepends a checkbox track to the card grid — still CSS Grid card-rows, no `<table>`).

- [ ] **6.1 Write the failing test file** `tests/web/test_bulk_ui.py` with exactly this content:

```python
"""WP4 — bulk-select UI on /transactions (tests precede impl per rule-011).

Markup contract (JS behaviour is covered by the manual gate):
* per-row checkbox [data-bulk-checkbox] with the txn id as value,
* header select-all [data-bulk-select-all],
* the .cards grid gains the cards--selectable variant (checkbox track),
* action bar #bulk-bar with the shared picker + [data-bulk-apply]
  posting to /api/transactions/bulk-edit,
* dashboard recent-activity cards stay checkbox-free (GUARD),
* the single-edit card swap keeps the checkbox cell so the subgrid row
  stays aligned after a modal save.
"""

from __future__ import annotations

import sqlite3


def _txn_id(conn: sqlite3.Connection, source_ref: str) -> int:
    row = conn.execute(
        "SELECT id FROM transactions WHERE source_ref = ?", (source_ref,)
    ).fetchone()
    assert row is not None
    return int(row["id"])


def test_transactions_page_has_bulk_bar_with_picker(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    client = web_client_factory()

    resp = client.get("/transactions", params={"date_from": "2000-01-01"})
    assert resp.status_code == 200
    body = resp.text

    assert 'id="bulk-bar"' in body
    assert "data-category-picker" in body
    assert "data-bulk-apply" in body
    assert "/api/transactions/bulk-edit" in body


def test_list_partial_rows_have_checkboxes(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    client = web_client_factory()

    resp = client.get(
        "/_partial/transactions/list", params={"date_from": "2000-01-01"}
    )
    assert resp.status_code == 200
    body = resp.text

    assert "data-bulk-checkbox" in body
    assert "data-bulk-select-all" in body
    assert "cards--selectable" in body


def test_checkbox_value_is_txn_id(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    client = web_client_factory()
    txn_id = _txn_id(seeded_web_db, "prov-1")

    body = client.get(
        "/_partial/transactions/list", params={"date_from": "2000-01-01"}
    ).text

    assert f'data-bulk-checkbox value="{txn_id}"' in body


def test_dashboard_cards_have_no_checkboxes(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    """GUARD: card_transaction.html stays checkbox-free outside /transactions."""
    client = web_client_factory()

    body = client.get("/").text

    assert "data-bulk-checkbox" not in body


def test_single_edit_card_swap_keeps_checkbox_cell(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    client = web_client_factory()
    txn_id = _txn_id(seeded_web_db, "prov-1")

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
    assert "data-bulk-checkbox" in resp.text
```

- [ ] **6.2 Run it — expect FAIL:**

```bash
uv run pytest -q tests/web/test_bulk_ui.py
```

Expected: `4 failed, 1 passed` (the dashboard GUARD passes; the rest fail with `AssertionError` on the missing markers).

- [ ] **6.3 Commit the test:**

```bash
git add tests/web/test_bulk_ui.py
git commit -m "test(web): bulk-select UI markup on /transactions (WP4)"
```

- [ ] **6.4 Edit `finances/web/routers/pages.py` — imports.** Replace:

```python
from finances.db.repos import accounts as accounts_repo
from finances.web.deps import get_conn
```

with:

```python
from finances.db.repos import accounts as accounts_repo
from finances.db.repos import categories as categories_repo
from finances.web.deps import get_conn
```

and replace:

```python
from finances.web.services.accounts_view import build_account_cards
```

with:

```python
from finances.web.services.accounts_view import build_account_cards
from finances.web.services.category_stats import top_categories
```

- [ ] **6.5 Edit `finances/web/routers/pages.py` — `transactions_page` context.** Replace:

```python
        {
            "title": "Transactions",
            "page": page,
            "filter": page.filter,
            "accounts_options": accounts_options,
            "kinds_options": kinds_options,
            "currencies_options": currencies_options,
            "sources_options": sources_options,
        },
```

with:

```python
        {
            "title": "Transactions",
            "page": page,
            "filter": page.filter,
            "accounts_options": accounts_options,
            "kinds_options": kinds_options,
            "currencies_options": currencies_options,
            "sources_options": sources_options,
            # Bulk action bar (WP4): mixed kinds on this page → kind=None.
            "categories": categories_repo.list_all(conn),
            "top_categories": top_categories(conn, kind=None),
        },
```

- [ ] **6.6 Edit `finances/web/routers/partials.py` — keep the swapped card aligned.** In `transactions_edit_partial`, replace:

```python
    response = templates.TemplateResponse(
        request,
        "partials/card_transaction.html",
        {"card": card},
    )
    response.headers["HX-Trigger"] = "closeModal"
    return response
```

with:

```python
    response = templates.TemplateResponse(
        request,
        "partials/card_transaction.html",
        # bulk_select keeps the checkbox cell so the swapped-in card
        # stays aligned with the /transactions subgrid (WP4). This
        # endpoint is only invoked from the /transactions modal.
        {"card": card, "bulk_select": True},
    )
    response.headers["HX-Trigger"] = "closeModal"
    return response
```

- [ ] **6.7 Rewrite `finances/web/templates/pages/transactions.html`** with exactly this content (double-quoted `x-data` attribute → all inner JS string literals use single quotes):

```html
{% extends "base.html" %}
{% block title %}Transactions{% endblock %}
{% block content %}
  <header class="mb-4">
    <h1 class="text-2xl font-semibold">Transactions</h1>
    <p class="mt-1 text-sm text-slate-600">
      Browse, filter, and drill into the ledger. Default range is the last 30 days.
    </p>
  </header>

  {# Filters (top of page). #}
  {% include "partials/transactions_filters.html" %}

  {# Bulk-select scope (WP4). The action bar lives OUTSIDE #tx-list so it
     survives HTMX list swaps; the selection intentionally resets on any
     swap (filter / sort / page change). Apply posts JSON via fetch to
     the WP4 bulk endpoint, dispatches the WP2 toast event itself
     (fetch bypasses htmx's HX-Trigger handling), then refreshes the
     list from the current URL query (URL = filter source of truth). #}
  <section
    class="mt-4"
    x-data="{
      selected: [],
      toggleAll(checked) {
        this.selected = checked
          ? Array.from(document.querySelectorAll('[data-bulk-checkbox]')).map(el => el.value)
          : [];
      },
      async applyBulk() {
        const dirty = document.querySelector('#bulk-bar input[name=set_category]');
        if (!dirty || dirty.value !== 'true') {
          window.dispatchEvent(new CustomEvent('show-toast', {
            detail: { level: 'error', message: 'Pick a category first' },
          }));
          return;
        }
        const raw = document.querySelector('#bulk-bar input[name=category_id]').value;
        const payload = {
          ids: this.selected.map(Number),
          category_id: raw === '' ? null : Number(raw),
        };
        const resp = await fetch('/api/transactions/bulk-edit', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(payload),
        });
        if (!resp.ok) {
          let detail = '';
          try { detail = (await resp.json()).detail || ''; } catch (e) {}
          window.dispatchEvent(new CustomEvent('show-toast', {
            detail: { level: 'error', message: 'Bulk edit failed. ' + detail },
          }));
          return;
        }
        const data = await resp.json();
        window.dispatchEvent(new CustomEvent('show-toast', {
          detail: { level: 'success', message: data.updated + ' updated' },
        }));
        this.selected = [];
        htmx.ajax('GET', '/_partial/transactions/list' + window.location.search,
                  { target: '#tx-list', swap: 'outerHTML' });
      },
    }"
    @htmx:after-swap="selected = []"
  >
    {# Bulk action bar — revealed while at least one row is selected. #}
    <div
      id="bulk-bar"
      x-show="selected.length > 0"
      x-cloak
      class="mb-3 p-3 border border-slate-300 rounded-lg bg-white space-y-2"
    >
      <div class="flex items-center justify-between text-sm">
        <span><strong x-text="selected.length"></strong> selected</span>
        <button
          type="button"
          data-bulk-apply
          @click="applyBulk()"
          class="px-3 py-1.5 text-sm border border-slate-900 rounded bg-slate-900 text-white hover:bg-slate-800"
        >Apply</button>
      </div>
      {% include "partials/category_picker.html" %}
    </div>

    {# List body. Initial server render; HTMX swaps subsequent updates. #}
    {% include "partials/transactions_list.html" %}
  </section>
{% endblock %}
```

- [ ] **6.8 Edit `finances/web/templates/partials/transactions_list.html`.** Replace:

```html
  {# Column header (hidden on mobile via CSS). #}
  <div class="cards">
    <div class="hidden sm:grid grid-cols-subgrid col-span-full text-[10px] uppercase tracking-wide text-slate-400 px-3">
      <span>Date</span>
      <span>Account</span>
      <span>Description</span>
      <span class="text-right">Amount</span>
      <span>Category</span>
      <span class="text-right">Flags</span>
    </div>

    {% if page.rows %}
      {% for card in page.rows %}
        {% include "partials/card_transaction.html" %}
      {% endfor %}
    {% else %}
```

with:

```html
  {# Column header (hidden on mobile via CSS). WP4: leading checkbox
     track + select-all; rows opt into their checkbox via bulk_select. #}
  {% set bulk_select = true %}
  <div class="cards cards--selectable">
    <div class="hidden sm:grid grid-cols-subgrid col-span-full text-[10px] uppercase tracking-wide text-slate-400 px-3">
      <span>
        <input type="checkbox" data-bulk-select-all @change="toggleAll($event.target.checked)" title="Select all on page">
      </span>
      <span>Date</span>
      <span>Account</span>
      <span>Description</span>
      <span class="text-right">Amount</span>
      <span>Category</span>
      <span class="text-right">Flags</span>
    </div>

    {% if page.rows %}
      {% for card in page.rows %}
        {% include "partials/card_transaction.html" %}
      {% endfor %}
    {% else %}
```

- [ ] **6.9 Edit `finances/web/templates/partials/card_transaction.html`.** Insert the checkbox cell directly after the opening `<article ...>` tag. Replace:

```html
  hx-swap="innerHTML"
>
  <time class="text-xs text-slate-500 tabular-nums" datetime="{{ card.occurred_at.isoformat() }}">
```

with:

```html
  hx-swap="innerHTML"
>
  {% if bulk_select is defined and bulk_select %}
    {# WP4 bulk select. @click.stop keeps the checkbox from opening the
       row's edit modal (the article's hx-get click trigger). #}
    <span @click.stop>
      <input type="checkbox" data-bulk-checkbox value="{{ card.id }}" x-model="selected" @click.stop>
    </span>
  {% endif %}
  <time class="text-xs text-slate-500 tabular-nums" datetime="{{ card.occurred_at.isoformat() }}">
```

- [ ] **6.10 Append to `finances/web/static/css/app.css`** (after the `/* === END Phase 5 mobile polish === */` line):

```css
/* === WP4: bulk select ================================================== */

/* Alpine x-cloak: hide the action bar until Alpine initialises. */
[x-cloak] {
  display: none !important;
}

/* Selectable variant: prepend a checkbox track to the card grid.
 * Mirrors .cards (6 tracks desktop / 3 tablet) with one extra 1.5rem
 * leading column. Still CSS Grid card-rows — no <table>.
 */
.cards--selectable {
  grid-template-columns: 1.5rem 9rem 8rem 1fr 10rem 9rem 7rem;
}
@media (max-width: 1023px) {
  .cards--selectable {
    grid-template-columns: 1.5rem 7rem 1fr 9rem;
  }
}
/* Phone: .cards is display:block, the checkbox stacks as the first
 * element inside each card — keep it compact. */
@media (max-width: 640px) {
  .cards--selectable [data-bulk-checkbox] {
    width: 1rem;
    height: 1rem;
  }
}

/* === END WP4 bulk select === */
```

- [ ] **6.11 Run — expect PASS, plus list/dashboard regressions:**

```bash
uv run pytest -q tests/web/test_bulk_ui.py tests/web/test_transactions_read.py tests/web/test_dashboard.py tests/web/test_transactions_write.py
```

Expected: all pass.

- [ ] **6.12 Commit the implementation:**

```bash
git add finances/web/routers/pages.py finances/web/routers/partials.py finances/web/templates/pages/transactions.html finances/web/templates/partials/transactions_list.html finances/web/templates/partials/card_transaction.html finances/web/static/css/app.css finances/web/static/css/tailwind.css
git commit -m "feat(web): bulk categorize on /transactions - checkboxes, action bar, apply via bulk-edit (WP4)"
```

---

### Task 7: Full-suite regression + manual verification gate

**Files:** none created; read-only verification.

**Interfaces:** Consumes everything above. Produces the evidence Julio needs to mark WP4 complete (CLAUDE.md execution rule 3 — the agent never marks it complete).

- [ ] **7.1 Full test suite:**

```bash
uv run pytest -q
```

Expected: 0 failures (suite grows by 34 tests: 8 + 5 + 5 + 2 + 9 + 5). If anything unrelated fails, STOP and report — do not "fix while in here".

- [ ] **7.2 Coverage gate (rule-011):**

```bash
uv run pytest --cov -q
```

Expected: green; domain+db ≥ 85%, ingest ≥ 70% (unchanged by this WP — all new code is web-layer with direct tests).

- [ ] **7.3 Manual gate for Julio** (read-only serve against the real DB is fine; do NOT run ingest/backfill/sync):

```bash
uv run finances serve
```

Then verify in the browser at `http://localhost:8765`:

- [ ] `/triage` → open a category item → chips render with number hints; pressing `3` selects chip 3; typing in the filter box narrows the list and digits typed there do NOT select chips; `Enter` (focus outside inputs) saves and advances; `s` skips to bottom; `Esc` closes.
- [ ] Open an item, touch nothing, press `Esc`, reopen → category unchanged (dirty-flag safety).
- [ ] "× remove category" then Save → category cleared (explicit clear only).
- [ ] `/transactions` → tick 3 rows → action bar appears with count → pick a category → Apply → "3 updated" toast, list refreshes, rows show the new category; selection cleared.
- [ ] Select-all-on-page checkbox selects every visible row; changing a filter resets the selection.
- [ ] Clicking a checkbox does NOT open the row's edit modal.
- [ ] Report the results; Julio marks WP4 complete.
