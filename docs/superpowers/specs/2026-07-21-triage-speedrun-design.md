# Triage Speedrun — design

- **Date:** 2026-07-21
- **Owner:** Julio Cordero
- **Status:** Approved design, implementation blocked (see Prerequisites)
- **Touches:** EPIC-025 (unified triage queue), ADR-012, ADR-005/ADR-013, rule-012
- **Supersedes nothing.** Amends ADR-012 §50 and roadmap EPIC-025 verification criteria.

## 1. Problem

Triage is the inbox of transactions the ingest pipeline could not finish on its
own. Today it holds **243 items** on the live ledger:

| Population | Count |
|---|---|
| `needs_review = 1` (no trustworthy USD rate) | 218 |
| `category_id IS NULL` and kind not in (transfer, adjustment) | 238 |
| Both issues on the same row (merged to one card) | 213 |
| **Distinct queue items** | **243** |

By source: 211 Provincial/VES, 27 Binance/USDT, 5 Binance/USDC.

The owner's stated goal, verbatim: *"freely (and FASTER) record/categorize my
transactions … I don't want to be forced to tackle one in specific, but first
run MOST of the ones I have, and tackle the ambiguous ones at the end."*

Five concrete obstacles stand in the way.

1. **You cannot tell which exchange rate produced the dollar figure.** The modal
   header shows `20,000.00 VES  $41.38` with no indication of the divisor. The
   `rate_source` badge below discloses it, but the competing rates for that day
   are not shown, so there is no way to judge whether the figure is sane.
2. **There is no way back.** The modal has `Save & next`, `Skip → bottom` and
   `Cancel`. A misclick is unrecoverable without leaving triage and finding the
   row again in `/transactions`.
3. **Deferring is a lie.** `Skip → bottom` stores the item id in a per-process
   `set[str]` on `app.state`, which is destroyed on every server stop. Since
   `base.html` renders an always-visible **Stop server** button that calls
   `os.kill(os.getpid(), SIGINT)`, the designed way to end a session is also the
   thing that erases every skip.
4. **Hard and easy items are interleaved.** The queue is strictly oldest-first,
   so a row needing only a category click sits between rows requiring the owner
   to remember what a bolívar was worth eight months ago.
5. **Half-finished edits are silently discardable.** `Cancel` and `Escape`
   close the modal with no warning, losing typed input.

## 2. Non-goals

- Bulk categorization from the queue list. `/transactions` already has
  bulk-edit; triage stays one-item-at-a-time.
- Changing what lands in the queue. The two membership predicates in
  `_collect_txn_items` are unchanged.
- Removing parked rows from needs-review **counts**. Dashboard, `report.html`,
  Sheets sync and `finances report needs-review` keep counting parked rows.
  Parking is a triage-queue grouping only. This deliberately keeps the change
  off six unrelated call sites (`web/services/dashboard.py:143`,
  `reports/update.py:296`, `reports/html_export.py:319`,
  `reports/needs_review.py:96`, `reports/sheets_sync.py:220`,
  `cli/main.py:588`).
- Wiring `realized_rates.rebuild()` into ingest or the CLI. See §8.
- Touching the pair-confirm flow beyond what §5.2 requires.

## 3. Prerequisites (blocking — no code until these land)

### 3.1 The manual-pair-picker branch must merge first

The working tree is on `feat/manual-pair-picker`, nine commits ahead of `main`,
with untracked `finances/db/migrations/013_deactivate_lifestyle_tools.sql` and
its test. The approved plan
`docs/superpowers/plans/2026-07-21-manual-pair-picker.md` claims
`finances/web/routers/_tx_filter_dep.py`,
`finances/web/services/transactions_query.py` and
`finances/web/templates/partials/transactions_filters.html` — the same files
§5.4 needs. Two plans editing the two-place filter enumeration in
`_tx_filter_dep.py` in one working tree is the collision MEMORY.md warns about.

**Decision:** finish and merge the pair picker, then branch this work fresh from
`main`.

### 3.2 ADR-012 amendment + roadmap edit

ADR-012 §50 states the queue is *"sorted oldest-issue-first"* with actions
*"`[Skip → bottom of session queue]` `[Cancel]` `[Save & next]`"*. Roadmap
EPIC-025's Technical Boundary states the skip store is *"session-local,
intentionally not persisted"*, and its Verification Criteria require oldest-first
ordering and skip-to-bottom.

Sections 5.3 (persistent parking), 5.5 (easy-first ordering) and 5.2
(stay-in-place instead of vanish) each reverse a clause of a user-owned gate.
Per CLAUDE.md execution rule 4, an ADR amendment lands **before** code:

- Amend ADR-012 §50: replace "oldest-issue-first" with the §5.5 ordering, and
  replace the action row with `[← →] [Park] [Cancel] [Save & next]`. Record the
  rationale (throughput over chronology) and that parking is now durable.
- Amend roadmap EPIC-025 Technical Boundary and Verification Criteria to match.

### 3.3 Migration number

Next free prefix is **014**. `012_leisure_food_split.sql` is committed;
`013_deactivate_lifestyle_tools.sql` exists untracked. Note that two `011_*`
files already share a prefix — harmless, because `_migrations` keys on full
filename — and follow the 008/010 header convention of documenting the
collision in a comment.

## 4. Phase 0 — deterministic queue order (ships alone, first)

**This is a bugfix and a hard precondition for §5.2 and §5.5.**

Neither query in `_collect_txn_items` (`finances/web/services/triage.py:143-153`)
carries an `ORDER BY`, and `build_queue` sorts on `sort_key` — a bare
`datetime` — with no tiebreak:

```python
all_items.sort(key=lambda it: it.sort_key)
```

On the live ledger **211 of 243 items have `occurred_at` at exactly
`00:00:00`** (Provincial CSV carries no time component) and **204 of 243 fall
inside 39 tied-timestamp groups**. Roughly 84% of the queue therefore has an
order that no code pins down; it resolves to whatever SQLite row order the
query plan produces and can change when an index is added.

Numbering items "23 of 187" or freezing a snapshot on top of this would freeze a
random permutation.

**Fix:**

- Append `ORDER BY t.occurred_at, t.id` to both queries in
  `_collect_txn_items`, and to the pair-item collection order.
- Change the Python sort to a total order: `key=lambda it: (it.sort_key, it.item_id)`.

**Test (RED first):** seed three transactions sharing one `occurred_at`, build
the queue twice against connections opened in different orders, assert identical
`item_id` sequences. A property test over N tied rows is optional; a fixed seed
is sufficient here since the invariant is a total order, not a distribution.

## 5. Design

### 5.1 Three-rate panel in the modal

**What the user sees**, inserted between the header and the Provenance block of
`modal_transaction_triage.html`, rendered **only** when
`card.rate_source != 'native_usd'` (the 32 USDT/USDC items skip it entirely):

```text
RATES FOR APR 23
  Realized     —                         (no P2P sell within 14 days)
  USDT P2P     483.31   ← used for $41.38
  BCV           36.55   reference only
```

Carry-forward is disclosed inline: `USDT P2P  481.00  (from Apr 21)`.

**Why three series.** Commit `cb2fc5c` (ADR-013) inserted a new **tier 2** into
`rates.resolve` ahead of the P2P median. The chain is now five tiers, not four:

1. `txn.user_rate` → `user_rate`
2. `rates(USDT, VES, 'binance_p2p_realized')`, gated by
   `REALIZED_MAX_AGE_DAYS = 14` → `binance_p2p_realized` / `…_carry`
3. `rates(USDT, VES, 'binance_p2p_median')` → `binance_p2p_median` / `…_carry`
4. `rates(USD, VES, 'bcv')` → `bcv` / `bcv_carry`
5. none → `needs_review`

A two-series panel would mark neither winner the moment tier 2 has data.

**New service function** in `finances/web/services/rates_view.py`:

```python
class DayRate(BaseModel):
    label: str            # "Realized" | "USDT P2P" | "BCV"
    source: str           # canonical rates.source value
    rate: Decimal | None
    as_of_date: date | None
    is_carry: bool        # as_of_date < requested day
    is_winner: bool       # produced the card's amount_usd
    is_reference_only: bool  # True for bcv (ADR-005)

def rates_for_day(conn, *, day: date, winning_source: str) -> list[DayRate]: ...
```

It reads each series through the existing `rates_repo.latest_on_or_before` — the
same primitive `rates.resolve` uses — so no resolver logic is duplicated
(rule-012). `is_winner` is decided by comparing `winning_source` (i.e.
`card.rate_source`, which came from `resolve`) against each series' `source`
after stripping a trailing `_carry`. **The panel never re-derives the winner.**

Extend `_CHART_SERIES_SPEC` to three entries so `/rates` and the modal share one
source of truth for labels.

**Badge gap.** `rate_source_badge` in `_macros.html` has a `label_map` keyed on
exactly `user_rate, binance_p2p_median, binance_p2p_median_carry, bcv,
bcv_carry, native_usd, needs_review`. Unknown sources fall through to raw
unstyled snake_case. Add `binance_p2p_realized` and
`binance_p2p_realized_carry`.

**Data reality check.** The live `rates` table holds `bcv` n=474,
`binance_p2p_median` n=5, `binance_p2p_median_buy` n=5,
`binance_p2p_median_sell` n=5, and **zero** `binance_p2p_realized` rows. The
Realized row will render as `—` for every transaction until §8 happens. That is
correct and informative, not a defect.

### 5.2 Client-side navigation, in-place save

**Rejected approach:** a server-held ordered snapshot in `app.state`, mirroring
`get_skip_store`. It dies on every **Stop server** press (the designed way to end
a session), needs a rebuild-on-empty path, and goes stale against confirmed
pairs — `_collect_pair_items` re-runs `BankAnchoredP2pPairing.match()` on every
`build_queue`, so `pair:{deposit}:{sell}` ids are derived from a live matching
pass.

**Chosen approach:** the rendered queue *is* the snapshot. `#triage-queue`
already contains every card in order; navigation walks that DOM. No server
state, nothing to invalidate, nothing to hydrate. This is also the plainer HTMX
pattern, per the owner's standing preference for conventional over clever.

**Mechanics:**

- Both card partials gain `data-item-id="{{ item.item_id }}"` — the canonical
  `TriageItem.item_id` (`txn:{id}` or `pair:{deposit}:{sell}`), so navigation
  handles both card types through one selector. The existing `data-tx-id` on
  `triage_card_txn.html` stays untouched; tests pin it.
  An Alpine component on the queue host exposes `items()`, `indexOf(id)`,
  `prev(id)`, `next(id)`.
- The modal renders `← 23 of 187 →`, computed from the opened item's index.
  Arrows are **real buttons** (`hx-get` the neighbour's modal URL), with
  `ArrowLeft` / `ArrowRight` as accelerators. Buttons are primary because the
  existing `@keydown.window` handler bails via `isTyping($event)` whenever focus
  sits on an `input, textarea, select, button` — which is exactly where focus
  lands after clicking a category chip. Arrow keys alone would feel dead at
  random.
- Arrow keys do not collide with existing bindings: `Escape`, `1`–`8`,
  `Enter`, `s`/`S` are the complete current set. Inside the notes textarea the
  arrow handler must yield to cursor movement.
- **Save swaps one card, not the queue.** `triage_edit_partial` returns the
  single re-rendered card (targeted at `#triage-card-{id}`) marked done —
  greyed, green ✓, badges cleared — instead of the whole queue partial. The item
  keeps its slot, so indices never shift and `←` reaches it again.

**advanceQueue must be redesigned, not extended.** `base.html` currently
auto-advances by clicking the *first* `#triage-queue [hx-get*='/modal']` after
an `advanceQueue` trigger, gated on the settle target id being `triage-queue`.
Under stay-in-place that first element is the item just saved, and it reopens
itself in a loop. Replace the DOM heuristic with an explicit payload:
`HX-Trigger: {"advanceQueue": {"nextItemId": "txn:1234"}}`, computed server-side
from the ordered queue, with the listener opening that id and closing the modal
when it is `null`. Three assertions in `tests/web/test_triage_advance.py` pin the
old contract by raw string (`htmx:after-settle.window`, `triage-queue`, the
`[hx-get*='/modal']` selector) and must be rewritten in the same change.

### 5.3 Park — durable deferral

Replaces `Skip → bottom` and deletes the in-memory skip store entirely
(`get_skip_store`, `app.state.skipped_triage_ids`, and `build_queue`'s
`skipped_ids` parameter).

**Migration `014_add_transaction_parked.sql`** — one statement, following the
008 precedent:

```sql
ALTER TABLE transactions ADD COLUMN parked INTEGER NOT NULL DEFAULT 0
  CHECK (parked IN (0, 1));
```

Verified against SQLite: `ADD COLUMN` accepts `NOT NULL` given a non-null
default, the `CHECK` is accepted and enforced on subsequent writes, and existing
rows backfill to `0`. No index — 1,850 rows makes it irrelevant, and adding one
would perturb the query plan the §4 tiebreak exists to stabilize.

**Re-ingest safety.** `upsert_by_source_ref` lists its `ON CONFLICT DO UPDATE
SET` columns explicitly. Omitting `parked` from that list leaves it untouched on
re-ingest — no `COALESCE` needed. Add `parked` to the `INSERT` column list in
**both** `insert()` and `upsert_by_source_ref()`, which each carry an
independent 13-column list.

Caveat inherited from rule-010, worth stating plainly: Provincial `source_ref`
is `"hash:" + sha256(occurred_at || amount || description)[:16]`. If a re-drop
produces a byte-different description for the same movement, a new row is
inserted and the parked row is orphaned — the user sees "the thing I parked came
back". Pre-existing; parking makes it visible.

**Fan-out of the column add.** Nine hand-listed SELECT sites feed the two
byte-identical `_row_to_transaction` copies, and none uses `SELECT *`, so a miss
is a runtime `sqlite3.Row` `IndexError` on one code path only:

| File | Purpose |
|---|---|
| `db/repos/transactions.py:114` | `get_by_id` |
| `db/repos/transactions.py:129` | `get_by_source_ref` |
| `db/repos/transactions.py:221` | `update` re-read |
| `reports/consolidated_usd.py:111` | consolidated report |
| `reports/monthly.py:231` | monthly report |
| `web/services/transactions_query.py:323` | transactions page |
| `web/services/triage.py:106` | `_TXN_QUERY_BASE` (serves 3 queries) |

Audited safe (aggregate/narrow SELECTs, no mapper): `ingest/cash_cli.py:72`,
`reports/html_export.py:207`, `web/services/monthly_view.py:218`.

**Model + factory, same commit.** Add `parked: bool = False` to `Transaction`.
`TransactionFactory` in `tests/conftest.py:275-291` pins every optional field by
hand, and `__allow_none_optionals__ = 0.0` does **not** pin booleans —
polyfactory would randomly park ~half of all factory-built rows, producing
intermittent queue-test failures that look like ordering bugs. Add
`parked = False` to the factory in the same commit as the model field.

**Write path.** `transactions_repo.update()` gains a `parked` sentinel parameter
following the existing `_UNSET` pattern. Per rule-012 the web layer issues no
SQL of its own; the route calls the repo. Parking is a plain durable flag and is
**not** `needs_review` — rule-012 forbids exposing a manual `needs_review`
toggle, and this design never touches it.

**Queue behaviour.** Parked items leave the main run and collect under a
`Parked · N` disclosure at the top of `/triage`, collapsed by default. Unparking
is one click from inside that group.

### 5.4 Dirty guard

- **Nothing touched** → arrows navigate immediately.
- **Something touched** → navigation is intercepted and an inline bar offers
  **Save & go** / **Discard & go**. Escape and Cancel route through the same bar.

**Implementation constraint.** `catDirty` is declared in the `x-data` of both
`modal_transaction_triage.html:99` and `modal_transaction.html:120` and is
**never read or assigned** — dead code. The picker's real `dirty` lives in its
own nested `x-data` inside `category_picker.html`, and Alpine scope inheritance
runs child-reads-parent, so the form cannot see it.

Read the three rendered DOM sentinels instead:

```js
['set_category', 'set_user_rate', 'set_notes']
  .some(n => $el.querySelector(`[name=${n}]`)?.value === 'true')
```

This touches no picker markup. Hoisting `dirty` out of the picker would break
its reuse across both modals and trip the raw-string assertions in
`tests/web/test_category_picker.py` and `test_safety_feedback.py`, which pin
`name="set_category" value="false"` and `data-category-picker`. Delete the dead
`catDirty` declarations as part of this change.

**Focus hazard to preserve.** Two `x-init` handlers race in `$nextTick`: the
overlay focuses `.tx-modal-card` (`tabindex="-1"`), and the form focuses
`[name=category_id]` — hidden since WP4, so that `.focus()` is a silent no-op.
Keyboard shortcuts work today *only* because focus falls back to the card.
Autofocusing the picker's search box would silently kill every shortcut,
including the new arrows. Do not change the focus chain in this work.

### 5.5 Easy-first ordering and header split

Sort key becomes `(bucket, occurred_at, item_id)`:

| Bucket | Contents | Live count |
|---|---|---|
| 0 | Missing category only; USD figure already trustworthy | 25 |
| 1 | Missing a rate (`needs_review`), with or without category | 218 |
| 2 | Pair proposals | reviewed last |
| — | Parked | separate collapsed group |

Buckets 0 and 1 partition the 243 txn items exactly: `238 − 213 = 25` rows are
category-only, and all 218 `needs_review` rows fall in bucket 1 regardless of
category. Pair proposals are additional to the 243.

Header, rendered **inside** `#triage-queue`:

```text
Ready to categorize · 25      Missing a rate · 218      Pairs · N      Parked · N
```

Placing it inside the swapped region is deliberate. `_render_queue_partial`
hardcodes `type_filter=None`, so every save/skip/confirm already swaps an
unfiltered queue in while the chip still renders active and `hx-push-url` has
written `?type_filter=…` into the URL. Counts living outside the swap would go
stale on every write. **Fix `_render_queue_partial` to preserve `type_filter`
in the same change** — it is a live bug the header would otherwise amplify.

The existing type-filter chips stay; the header counts are informational, not a
second filter control.

## 6. Data flow

```text
GET /triage
  └─ build_queue(conn)                       [§4 total order, §5.5 buckets]
       ├─ _collect_txn_items   WHERE parked = 0, ORDER BY t.occurred_at, t.id
       ├─ _collect_pair_items  BankAnchoredP2pPairing.match
       └─ parked group         WHERE parked = 1
  └─ triage.html → triage_queue.html (header + cards, in one swap region)

GET /_partial/triage/{id}/modal
  └─ _project_card → rates.resolve            (winning source)
  └─ rates_for_day(conn, day, winning_source) (§5.1 three series)
  └─ modal_transaction_triage.html            (panel, ← N of M →, dirty guard)

POST /_partial/triage/{id}/edit
  └─ transactions_write (repo only, rule-012)
  └─ 200 + single card partial → #triage-card-{id}   (in place, marked done)
  └─ HX-Trigger: {"advanceQueue": {"nextItemId": "txn:…"|null}}

POST /_partial/triage/{id}/park      → repo update(parked=True) → card partial
POST /_partial/triage/{id}/unpark    → repo update(parked=False) → card partial
```

## 7. Testing

Per rule-011, test commits precede implementation commits, matching the recent
`1126edc test → cb2fc5c feat` cadence. Coverage gates apply to
`finances/domain/**` (≥85%) and `finances/ingest/**` (≥70%);
`finances/web/**` is ungated, which is where most of this lands.

| Area | Assertions |
|---|---|
| §4 order | Tied `occurred_at` rows produce a stable `item_id` sequence across repeated builds |
| §5.1 panel | Three series render; winner marked from `card.rate_source`; carry shows origin date; panel absent for `native_usd`; both realized badge keys resolve to styled labels |
| §5.2 nav | `← N of M →` indices; save returns one card not the queue; saved card keeps its slot; `advanceQueue` payload carries the correct `nextItemId` and `null` at the end; rewrite the three raw-string assertions in `test_triage_advance.py` |
| §5.3 park | Migration is idempotent and backfills 0; `upsert_by_source_ref` preserves `parked` across re-ingest; parked rows leave the main queue and appear in the group; unpark restores; `TransactionFactory` emits `parked = False` |
| §5.4 guard | Untouched modal navigates; each of the three sentinels independently trips the guard; Save & go persists then navigates; Discard & go navigates without writing |
| §5.5 order | Bucket precedence holds; header counts match rendered groups; `_render_queue_partial` preserves `type_filter` |

Run with `rtk proxy uv run pytest` (the plain `pytest` and `uv run rtk pytest`
forms both fail in this environment).

## 8. Known adjacent debt (flagged, not fixed here)

- **ADR-013 does not exist.** `docs/ADR/` holds ADR-001..012, yet
  `finances/domain/rates.py:7` and four merged commits cite ADR-013. rule-005
  still documents the old four-tier chain. This design depends on the five-tier
  reality and should not proceed past §3.2 without at least a note recording it.
- **`realized_rates.rebuild()` has zero callers** and the live table has zero
  realized rows, so tier 2 is inert. It is a loaded gun: wiring it into ingest
  or adding the planned `finances rates rebuild-realized` CLI re-prices the
  majority of VES rows in one shot and changes what every triage modal shows.
  §5.1 is built to display that correctly whenever it fires.
- **`v_transactions_usd`** (`001_initial.sql:166-200`) computes `amount_usd` with
  its own hardcoded `binance_p2p_median` SQL and now silently disagrees with the
  five-tier Python resolver. The realized design called for dropping it in
  migration 012; 012 became the leisure split instead, so the view is still
  live and `tests/test_db_schema.py:352` still asserts it.

## 9. Build order

Each step is independently shippable and reviewable.

0. **Prerequisites** — merge pair picker; ADR-012 amendment + roadmap edit (§3).
1. **Deterministic order** (§4). Bugfix, no UI change.
2. **Three-rate panel** (§5.1). Read-only, no schema change, immediately useful.
3. **Migration 014 + `parked` fan-out** (§5.3, data layer only).
4. **Park UI + Parked group** (§5.3, UI half).
5. **Client-side nav + in-place save + advanceQueue redesign** (§5.2).
6. **Dirty guard** (§5.4).
7. **Easy-first buckets + header + `type_filter` fix** (§5.5).

Steps 2 and 3 are independent and may run in either order. Step 6 depends on 5.
Step 7 depends on 1 and 3.
