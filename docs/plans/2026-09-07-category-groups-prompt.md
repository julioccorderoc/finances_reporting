# Category groups — implementation prompt

Paste the section below into a fresh session. It is self-contained; it
assumes no memory of the session that wrote it.

Decisions are settled in
[ADR-023](../ADR/ADR-023-category-groups.md) (Accepted). Do not re-open them.
If you believe one is wrong, stop and say so before writing code.

---

## The task

Implement ADR-023: categories roll up into groups, and the /monthly chart
draws groups instead of raw categories.

Read first, in this order:

1. `docs/ADR/ADR-023-category-groups.md` — the decision, in full.
2. `finances/web/services/monthly_view.py` — `build_chart`, `_assign_color_slots`,
   `_resolve_slot_collisions`, `CHART_TOP_N`, `OTHER_COLOR_SLOT`.
3. `finances/web/templates/partials/monthly_chart.html` — the payload block
   and the hover overlay's `render()`.
4. `tests/web/test_monthly_chart_palette.py` — the guards that already exist.
   Several of them constrain what you are about to change.

## Why this exists

The chart draws the top 5 categories and folds the rest into "Other". On the
owner's real ledger Other is the **largest block in four of six months** —
45.8% of July 2026 — because fixed household costs are split across four
categories, none individually large enough to reach the top 5. So the single
biggest fact about a month is the one thing the chart cannot show.

The cap cannot simply be raised: `CHART_TOP_N` equals the number of
validated palette hues (5), and no six-hue set clears the colour-separation
floors on all pairs. Grouping is the fix.

## Scope

### 1. Migration 026 — `categories.group_name`

`finances/db/migrations/026_category_groups.sql`.

- `ALTER TABLE categories ADD COLUMN group_name TEXT NULL`.
- Seed exactly this mapping, by category name, expense kind only:

  | `group_name` | Categories |
  |---|---|
  | `Home` | Rent, Utilities, Transport, Personal Care |
  | `Social` | Dating, Going Out, Leisure, Family, Gifts |

  Everything else stays NULL. **Purchases, Groceries and Health stay
  ungrouped on purpose** — they stand alone as their own series. Lending,
  Fees, Other Expense, Education and Subscriptions stay ungrouped and will
  fall into the computed Other.

- Must be idempotent and re-runnable: re-applying changes no rows. Follow
  the existing migrations' style for the guard (see `021`, `022`, `025`).
- The names `Home` and `Social` are data, not code. The owner may rename
  them later with an UPDATE; nothing may hard-code them except this seed.

### 2. Domain + repo

- `Category` (Pydantic, `finances/domain/models.py`) gains
  `group_name: str | None = None`. rule-009 — repos accept and return
  Pydantic, never raw dicts.
- The categories repo must select and round-trip the new column.
- **Grep for hand-listed SELECT column lists before you finish.** Adding a
  column to a table has broken every hand-written SELECT in this repo before;
  the same hazard exists here. Run the *full* suite, not just the web tests.

### 3. `build_chart` groups

In `finances/web/services/monthly_view.py`:

- A series is now a **group** (for grouped categories) or a **category** (for
  ungrouped ones). Rank groups and ungrouped categories together, then apply
  `CHART_TOP_N` and the Other remainder to *that* list.
- `MonthlyChartSeries` gains `members: list[MonthlyChartSeries]` — what is
  inside this series, empty when nothing is. A group fills it with its
  categories; Other fills it with what missed the cap.
- **Fold away `MonthlyChart.other_members`**, added earlier the same day. It
  becomes the `members` of the Other series. One mechanism, per ADR §2.4.
- The colour-slot basis (`_assign_color_slots`) now ranks **series labels**
  (groups and ungrouped categories), not raw categories. Everything else
  about slots is unchanged and is already pinned by tests: the basis ignores
  the category filter so survivors are never repainted, and
  `_resolve_slot_collisions` keeps two visible series off the same hue.
- The category filter still filters *categories* (ADR §2.5). Filtering to
  Rent alone must still draw a **Home** series containing Rent — the chart
  must not change shape because a filter is on.
- A group's drill URL carries one `categories=` parameter per member.

### 4. The overlay

`monthly_chart.html`: the payload carries `members` per series; `render()`
indents a series' members beneath it. The existing Other-expansion code
already does exactly this — generalise it rather than adding a second path.

`.mth-tip-row.is-member` / `.mth-tip-num.is-member` in `reports.css` already
style the indented rows. Reuse them.

### 5. `doctor` check

`finances/domain/integrity.py` — add a check that every non-NULL
`group_name` sits on a category the seed knows. This is the guard against a
category being renamed while `group_name` still points at the old label
(ADR §3). Follow the existing `CHECKS` tuple's shape.

## Out of scope

Do not touch these. They are deliberate, not oversights:

- **The pivot stays per-category.** It is the detail view; collapsing it
  would hide the rows the owner used to diagnose this.
- **No `Other` group.** Other stays computed and keeps the neutral colour.
- **No group filter on /transactions**, no group column elsewhere, no
  grouping on the mobile monthly view. The column is available; wiring it up
  is a later decision.
- **The palette.** `--series-1..5` in signal.css are validated values; do not
  add, reorder or edit them. See `project_monthly_chart_palette` in memory,
  and note that every `tok()` read must keep its fallback argument — a bare
  `tok(name)` shipped a colourless chart on 2026-09-07 and there are two
  guard tests pinning it.

## How to work

- **Use a worktree.** Two other sessions have been active in this repo today
  and swept each other's staged files into their commits twice. Isolate
  before your first commit, and never `git add -A` in the shared checkout.
- **TDD, rule-011: the test commit precedes the implementation commit.**
- Verify in a real browser before calling it done — see
  `feedback_browser_only_defects` in memory. Serve a scratch copy:
  `finances serve --db-path <copy> --port 8099 --no-open --no-reload`.
  Never point it at the live `finances.db`.
- Playwright's own screenshot writes do not land on this filesystem. Assert
  on values returned inline from `browser_evaluate`, and drive headless
  Chrome directly if you need an image.

## Done means

- Migration 026 applied and re-runnable with no row changes.
- July 2026: Other is under 5% of the month (was 45.8%).
- Hovering **Home** lists Rent, Utilities, Transport, Personal Care.
- Filtering to Rent alone still draws a **Home** series.
- The pivot still lists every category.
- Full suite green — `pytest -q`, not just the web subset.
- Confirmed in a browser, not only in tests.
