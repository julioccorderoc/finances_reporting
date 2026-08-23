# Handoff: Triage — the ledger clean-up surface

## Overview

Triage is where a messy ledger gets fixed. On a real sitting it faces **34 live
rows** (24 needing a category, 4 proposed transfer pairs, 10 priced with an
approximate rate) on top of **266 parked rows** — a backlog nobody is going to
walk chronologically.

It answers one question — *what still needs you?* — and it is built so a sitting
of 10–40 rows is one opening and one closing: a dense list you sweep, and a modal
you walk with the keyboard. Three things a row can be missing (a category, a
trustworthy rate, its other transfer leg) are handled in the same run, and an
unpriceable row is priced anyway rather than blocking the queue.

## About the design files

The files in `design/` are **design references created in HTML** — a prototype
showing intended look and behaviour, not production code to copy. They run on
React UMD + Babel-in-the-browser, inline styles, and fixture data
(`triage-data.js`); `window.Bodega` / `window.Fin` / `window.Triage` globals are a
prototyping convenience, nothing more.

The task is to **recreate these designs in this codebase's existing environment**
— its templating, routing, state and API patterns — driven by real SQLite data.
If the target has no suitable environment yet, choose the most appropriate one for
the project and say what you chose before building.

Open `design/ui_kits/finances/triage.html` in a browser: it is fully interactive
(keyboard included) and is the fastest way to understand anything ambiguous below.

## Fidelity

**High-fidelity.** Final colours, typography, spacing, radii, motion and copy.
Recreate the UI to the pixel using this repo's libraries and patterns. Every hex
and px in this document is authoritative; the design system is **SIGNAL** (see
`context/DESIGN-SYSTEM-README.md`) and its tokens are in `design/tokens/`.

---

## Screens / views

There are five surfaces: the **queue screen**, the **modal run** over it, the
**bulk sort sheet**, the **parked sheet**, and the transient **selection bar +
toast**.

### 1. Queue screen

**Purpose** — see everything that needs a decision, resolve the easy ones in
place, and start a run on the rest.

**Layout** — fills the content area right of the app rail. Column,
`background: #ececea` (`--surface-canvas`).

- **Page header** (shared `PageHeader`): the question `What still needs you?`
  above the answer `34 rows need you` (or `Nothing needs you` when the blocking
  count is 0 — note that rows with only an approximate rate do **not** count as
  blocking).
  - Meta row: badge `24 category` (neutral), badge `4 pairs` (info), badge with
    dot `10 approximate rates` (warning), then `· 7 done in this sitting` at
    12.5px `#6d6d69`.
  - Actions, right: primary button `Sort all 34` with a `play` icon, tooltip
    `24 that need you, then 10 with approximate rates`; secondary button
    `Parked 266` with an `archive` icon.
- **Integrity banner** (warning tone), inset `0 32px 8px`, title *One transfer has
  a single leg*, body: `Binance Funding, Jun 29 — 96.40 USDT out with nothing on
  the other side. Pair it, or say it was not a transfer.`
- **Three groups**, in this order, each with a head and its rows:

  | Order | Label | Hint | Default |
  | --- | --- | --- | --- |
  | 1 | Needs a category | One decision each | expanded |
  | 2 | Proposed pairs | Two rows that look like one transfer | expanded |
  | 3 | Priced roughly | No rate within 14 days — Ledger used the nearest one | **collapsed** |

- **Parked strip** (below the groups, only when a parked count exists):
  `archive` icon 17px `#9a9a95`, `266 parked rows, out of the queue` (the number
  in mono), the note beneath at 12.5px `#6d6d69`, and a ghost button
  `Look at them`. Surface `#f5f5f3`, hairline `#dbdad5`, radius 4px, padding
  `16px 20px`, margin `24px 32px 0`.
- **Empty state** when the queue clears: `check-check` icon 28px `#131312`,
  headline `Queue empty.` in Doto at 26px/500, then `N rows sorted in this
  sitting. The next Provincial statement will start the next one.` at 14px/1.55
  `#4b4b46`, max-width 420px, centred.

**Group head** — `padding: 18px 32px 8px`, items in a 12px-gap flex row:
chevron (`chevron-down` open / `chevron-right` closed, 15px, `#9a9a95`), label at
16px/600/-0.006em, count in mono 13px `#6d6d69`, hint at 12.5px `#6d6d69`,
spacer, then a text button on the right — `Select all 12` / `Clear these` — in
`#c51a13` at 13px. The whole label block is the collapse toggle. A group with no
rows renders nothing.

**Row** — `border-top: 1px solid #dbdad5`, min-height 44px, `padding: 0 32px`,
`display: grid`, `gap: 12px`,
`grid-template-columns: 26px 64px minmax(0,1fr) 138px 138px 186px 26px`:

1. **Checkbox** — selection, `aria-label` `Select {raw description}`.
2. **Date** — mono 11.5px `#6d6d69`, `Jul 7` (year appended only when not the
   current year), never wrapped. No "Today / Yesterday": bank rows have no time
   component and 204 of 243 live rows share a timestamp, so day labels carry no
   signal.
3. **Merchant** — a button (opens the modal). Cleaned name at 13px `#131312`;
   under it, the raw bank string in mono 10.5px `#9a9a95`, ellipsised. When there
   is no cleaned name the raw string takes the top line alone.
4. **Account** — 12px `#6d6d69`, `Provincial` + ` · 0108 · 4471` in `#9a9a95`.
5. **Guess or issues** — if the row needs a category and has a guess: the
   **accept-the-guess chip** (see below). Otherwise the issue badges.
6. **Money** — the `Money` block, right-aligned, `size="sm"`.
7. **Open** — `maximize-2` icon button, `aria-label` `Open this row`.

Row backgrounds: `transparent` default, `#f5f5f3` when it is the row the modal is
on, `#e4e4e1` when selected (selection is grey — a red row in a ledger reads as an
error).

**Accept-the-guess chip** — inline-flex, `check` icon 11px + category label, 11.5px
`#4b4b46`, `padding: 2px 8px 3px`, radius 2px, transparent fill, **1px dashed
`#c7c6c0`** border. Clicking it resolves the row without opening the modal.
Tooltip: `Rule 21 · /excelsior gama|gran gama|plazas/i` or
`You sorted this here 6 times`.

### 2. Modal run — the core of the design

**Purpose** — one entry, full attention, with the queue still behind it.

**Overlay** — `position: absolute; inset: 0` over the app content (never
`fixed`: the prototype renders inside a device frame, and the real page must keep
the rail visible). Scrim `rgba(19,19,18,0.34)` + `backdrop-filter: blur(10px)`,
padding 24px, `bodega-fade` 150ms `cubic-bezier(0,0,0.2,1)`. Clicking the scrim
closes; clicking inside does not.

**Dialog** — `role="dialog"`, `aria-modal="true"`, `aria-label="Resolve this row"`.
`width: 100%; max-width: 880px; height: min(680px, calc(100% - 24px))`, surface
`#f5f5f3`, radius 8px, `box-shadow: 0 24px 48px -24px rgba(19,19,18,0.34)`,
`overflow: hidden`, `bodega-rise` 220ms `cubic-bezier(0.1,0.9,0.2,1)`.
**The height is fixed on purpose** — paging through entries must not resize the
frame.

**Header** — `padding: 16px 16px 12px 20px`, `border-bottom: 1px solid #dbdad5`,
12px gap:
- `3 OF 34` in mono caps 11px/0.14em `#6d6d69`.
- Progress track `flex: 0 1 140px; min-width: 64px; height: 4px; radius: 2px`,
  fill `#131312` at `(index+1)/total`, `transition: width 150ms cubic-bezier(0,0,0.2,1)`.
- Spacer, then `chevron-left` / `chevron-right` icon buttons (**disabled**, not
  hidden, at the ends) and `x`.

**Body** — `display: grid; grid-template-columns: minmax(0,0.78fr) minmax(0,1.22fr)`,
no gap; each column `padding: 20px`, `overflow-y: auto`, `min-height: 0`. Left
column has `border-right: 1px solid #dbdad5`. **Both columns scroll
independently**; the dialog itself never scrolls.

- **Left — the facts.** `Money` at `size="lg"`, left-aligned; merchant block at
  `size="lg"` (19px/600 name, mono 12px raw string); then
  `Fri, Jul 3 · Provincial · 0108 · 4471` at 13px `#6d6d69`; then the issue
  badges.
- **Right — the decision.** Depends on what the row needs:
  - **Needs a category**: eyebrow `WHAT WAS THIS FOR?` with `1–8` in mono 11px
    `#9a9a95` on the right; the *Why* block; the category picker at
    `columns={2}`.
  - **Approximate rate** (either alone, or under the category block with the
    eyebrow `AND THE RATE, IF YOU KNOW IT` — `THE RATE IS A GUESS — REPLACE IT?`
    when it is the only issue): the rate override block.
  - **Pair proposal**: the pair view, and the footer's primary button is hidden
    (the pair block carries its own actions).
  - Always last: a small `Note — optional` input.

**Footer** — `padding: 16px`, `border-top: 1px solid #dbdad5`, background
`#f5f5f3`: ghost button `Park` with `archive` icon; the keyboard legend
`←→ move · ↵ save · esc close` at 11px `#6d6d69` with the keys in mono; spacer;
primary button, label by situation — `Sort and next`, `Use this rate and next`,
or `Save and finish` on the last entry. Disabled until the row is resolvable.

### 3. Bulk sort sheet

`Fin.Sheet` at `size="md"` (560 × 520, fixed height, body scrolls). Title
`Sort 9 rows at once`, description `Rows in the selection that already have a
category are left alone.` Body: the category picker at `columns={4}`, keyboard
numbers off. Footer: ghost `Cancel`, primary `Sort 9 rows` with a `check` icon,
disabled until a category is chosen.

### 4. Parked sheet

`Fin.Sheet` at `size="md"`. Title `266 parked rows`, description `Out of the
queue, still in every balance and every report.`

- **Calendar picker** (`<input type="date">`), label `Park uncategorised rows
  before`, value `2026-01-01`, hint `The oldest one is Mon, Nov 3, 2024`.
- Eyebrow `A FEW OF THEM` over a hairline-bordered list of sample rows: mono date
  (62px fixed), raw string ellipsised in mono 11px `#9a9a95`, native amount in
  mono 11.5px `#6d6d69`.
- Closing note at 12.5px `#6d6d69`: `Their money still counts everywhere.
  Re-importing a statement will not push them back into the queue, and every badge
  they were carrying is still on them when you come back.`
- Footer: ghost `Bring back all 266` with `undo-2`; primary `Done` (applies the
  cutoff).

### 5. Selection bar and toast

- **Selection bar** appears whenever ≥1 row is selected: absolutely positioned,
  centred, `bottom: 20px`, `background: #131312`, `color: #f5f5f3`, radius 2px,
  `box-shadow: 0 24px 48px -24px rgba(19,19,18,0.34)`, `padding: 8px 10px 8px 16px`,
  `bodega-rise` 150ms. Contents: `9 selected` (number in mono), a 1px ×20px
  divider at `rgba(255,255,255,0.18)`, `Set a category` (tag icon, on a
  `rgba(255,255,255,0.12)` fill), `Park` (archive icon), and `Clear` at
  `rgba(255,255,255,0.7)`.
- **Toast**: same inverse surface, radius 4px, `padding: 10px 14px`, `check` icon
  14px, 13px text, `bottom: 22px` — or `74px` when the selection bar is up.
  Auto-dismiss at **2600ms**. Copy is specific, never "Saved":
  `Sorted — Groceries.`, `Sorted — Groceries. Rate set to 165.40.`,
  `Rate set to 152.40.`, `Parked. It keeps its money, and stops asking.`,
  `9 rows parked.`, `9 rows sorted into Groceries.`, `Paired.`,
  `Left unpaired — the legs stay separate rows.`,
  `266 rows back in the queue — oldest first.`,
  `Parking everything uncategorised before Thu, Jan 1.`

---

## Shared components

### `Money` — the consolidated USD figure with its native amount under it

Top line: consolidated USD in mono, `font-variant-numeric: tabular-nums`, sizes
`sm 13 / md 15.5 / lg 22 / xl 30`; weight 500 and tracking -0.03em at lg/xl, else
400/-0.01em. Positive values are **`#131312` with a leading `+`** — never green,
never red. Unpriceable rows show `Unpriced` (12.5px `#a5140e`) at `sm`, or
`circle-slash` icon + `Can't be priced` at larger sizes.

Second line: native amount in mono (`sm` 11px, else 12px) `#6d6d69` —
`Bs. 45,231.10`, `277.90 USDT`, `$18.40` — followed by the provenance chip for
VES rows.

**Formatting is locked** (`finances/format.py`, `docs/plans/ux-overhaul/00-design.md`):
US grouping `1,234.56`; **sign before symbol** (`−$1,200.00`, never `$−1,200.00`);
minus is U+2212; VES is `Bs.` + non-breaking space; dates `Mon, Jul 7` with the
year appended only when it isn't the current year.

### `Prov` — the provenance chip

`padding: 1px 6px 2px`, radius 2px, 10.5px, `cursor: help`, tooltip carries the
explanation. Three treatments:

| Case | Fill | Border | Text |
| --- | --- | --- | --- |
| Trusted (`user`, `realized`) | `#e4e4e1` | `#dbdad5` | `#22221f` |
| Quiet (`median`) | `#ececea` | `#dbdad5` | `#6d6d69` |
| Warn (`bcv`, `none`, any approximate) | `#fbe9e8` | `#efc4c1` | `#a5140e` |

Content: `triangle-alert` 10px on a BCV fallback; `≈` prefix when the figure is an
approximation; the short source label (`yours`, `realized`, `median`, `BCV`); then
the rate itself in mono at 75% opacity.

**The rate ladder**, in resolver order — the chip must make these visibly
different:

1. `user_rate` — the rate you typed for that row.
2. `binance_p2p_realized` — your actual P2P cost basis, valid 14 days.
3. `binance_p2p_median` — 14-day median of Binance P2P sells.
4. `bcv` — the official floor, `is_bcv_fallback`.
5. unpriceable — no `amount_usd` at all.

When nothing exists within 14 days, Ledger prices the row with the **nearest**
rate it has, marks it `≈`, and files it under *Priced roughly*.

### `Issues` — the badges

`Category` (neutral), `Rate` (warning, with dot), `Pair` (info). One row can carry
two. Badges are mono, uppercase, 10.5px/0.05em. In the *Needs a category* group
the `Category` badge is suppressed as redundant.

### `Why` — the guess, and why

Full-width button, `padding: 8px 10px`, radius 4px, fill `#e4e4e1`, **1px dashed
`#c7c6c0`**. `file-code` icon for a rule, `history` for learned behaviour. Body:
**bold category** then `— rule 21 matches /excelsior gama|gran gama|plazas/i` (plus
` over $1,000` where the rule has an amount bound) or `— you sorted this here 6
times`. Right: `USE IT` in mono caps 10.5px/0.08em `#c51a13`. Clicking fills the
picker; it does not save.

### `CatPicker` — 26 categories, 20 pickable

- **Search** input (`size="sm"`, `search` icon), placeholder `Search 20
  categories`. Matches on both the label **and the disambiguating test**.
- **Eight chips** (`grid`, `columns` prop = 2 in the modal, 4 in the sheet, gap
  6px): icon 15px, label, and the number key `1`–`8` in mono 10.5px. Chip
  `min-height: 38px`, `padding: 8px 9px`, radius 4px, surface `#f5f5f3`, border
  `#c7c6c0`. Selected: surface `#e4e4e1`, border `#c7c6c0`, label 600, icon and
  key `#131312`. Transition `--transition-control` (150ms).
- **`The other 12`** disclosure (chevron, `#c51a13`) with `Top eight by your last
  12 months` beside it — expands to the full list grouped `EXPENSE` / `INCOME`,
  `max-height: 268px`, scrolling, each row showing label + its test + kind.
- **The test strip** at the bottom, `min-height: 44px` (reserved, so nothing
  jumps): on hover or selection, `scale` icon + **Category** — *its test*. This is
  the point of the picker: `docs/architecture/category-definitions.md` says the
  undefined edges, not the count, caused the mis-tagging.
- Top eight are computed from **12 months of usage**, not hardcoded. Auto-only
  categories (Fees, Interest, Transfer, Opening position, Reconciliation) never
  appear; `Clothing` is deactivated (`off`). Retired categories are deactivated,
  never deleted.

### `RateEntry` — overriding an approximate rate

1. **Warning box** — fill `#fbe9e8`, border `#efc4c1`, `triangle-alert` 16px
   `#a5140e`: `Priced at 144.60 — BCV, same week · Nearest scraped official rate.
   No P2P sell or BCV scrape within 14 days of Mon, Mar 2, so the dollar figure is
   an approximation.`
2. **The field** — `Rate you got`, hint `Bolívares per dollar`, `inputMode="decimal"`,
   mono, `flex: 0 0 176px`, placeholder = the current approximate rate. Beside it,
   eyebrow `WOULD BECOME` (or `CURRENTLY`) over the recomputed USD figure at 20px
   mono/500/-0.03em, live as you type.
3. **`OR TAKE ONE OF THESE`** — the nearest known rates as tappable rows: rate in
   mono 14px (62px min-width), label + note, and the resulting USD on the right.
   Each says why it is approximate (`BCV, 3 days later`, `P2P median, 21 days
   later · Outside the 14-day window`).

### `PairView` — confirming a transfer

Two legs in a hairline card: account icon, account name 13px/500, mono 11px
`{raw} · Jul 7`, and the signed native amount in mono 14px (positive `#131312`).
Under it, a 12.5px `#6d6d69` metadata row: `git-compare-arrows` icon +
`96% confident`, `Same day` / `1 day apart`, `0.4% rate drift` / `Exact amounts`,
and `implies 162.76 Bs./$` in mono. Then either the note
(`Confirming this makes it your cost basis for every VES row in the next 14
days.`) or, when refused, a **danger banner** titled *This one cannot be
confirmed*: `Seven days apart and 12.4% of drift. Confirming is refused above five
days or ten percent.` Actions: primary `Pair them` (`link` icon, disabled when
refused) and ghost `Not a pair`.

---

## Interactions & behaviour

**Opening a run** — clicking a row (merchant button or open icon) opens the modal
on that row; `Sort all 34` opens on the first. The modal walks a **flat list built
in group order, regardless of what is collapsed** — collapsing is a reading
convenience, not a filter.

**Keyboard** (document-level, active only while the modal is open):

| Key | Effect |
| --- | --- |
| `←` / `→` | previous / next entry (`preventDefault`) |
| `1`–`8` | pick the nth top-eight category (only when the row needs a category) |
| `↵` | save and advance — only when the row is resolvable |
| `esc` | close |

Typing in an input swallows everything except `esc`. Arrow buttons **disable** at
the ends rather than disappearing.

**Resolvable** means: needs a category → a category is chosen (typed or guessed);
needs only a rate → a rate greater than 0 is entered.

**Save** — resolves whatever the row was asking for. A typed rate replaces the
approximation and the row leaves *Priced roughly*. The row is removed from the
list, the sitting counter increments, a specific toast fires, and **the modal
lands on whatever now occupies that slot** (falling back to the previous entry at
the end of the list, and closing when the queue empties). A partial fix — rate
saved, category still missing — keeps its place rather than reopening.

**Park** — from the modal footer (single row, auto-advances) or the selection bar
(many). Parked rows leave the queue, keep their money in every balance and report,
survive re-ingest, and keep their badges.

**Cutoff** — the parked sheet's `Done` applies the date: everything uncategorised
before it is parked in one call. **Bring back all N** returns them to the queue in
canonical order (oldest first) and zeroes the parked count.

**Bulk sort** — the selection bar's `Set a category` opens the sheet; applying
affects only selected rows that actually need a category, and says how many it
touched.

**Accept the guess** — the dashed chip in the row resolves it in one click, no
modal.

**Motion** — `--dur-fast` 150ms, `--dur-base` 220ms; house curve
`cubic-bezier(0,0,0.2,1)`; overlays and toasts use `cubic-bezier(0.1,0.9,0.2,1)`
with `bodega-rise` (6px up, fade). Nothing bounces. Everything collapses to 1ms
under `prefers-reduced-motion: reduce`.

**Focus** — `:focus-visible` gets
`0 0 0 2px #f5f5f3, 0 0 0 4px #e5231b` and a 2px radius. The modal traps nothing
today; adding a proper focus trap is expected in the real implementation (J
criteria).

**Empty and error states** — queue empty gets the Doto headline above. A row that
cannot be priced *and* has no category shows both badges and both blocks in the
modal. A pair that fails the plausibility test cannot be confirmed at all.

---

## State management

Screen-level state in the prototype (`TriageScreen.jsx`), and what each becomes in
a real implementation:

| State | Type | Role |
| --- | --- | --- |
| `list` | item[] | the live queue, in canonical order |
| `sel` | id[] | checkbox selection (survives collapse, cleared on apply) |
| `open` | id \| null | which entry the modal is on; `null` = closed |
| `vals` | `{ [id]: { cat, rate, note } }` | per-row draft edits, kept while the run walks away and back |
| `done` | int | resolved this sitting (display only) |
| `parked` | int | parked count |
| `parkedOpen`, `bulkOpen`, `bulkCat` | bool / bool / key | sheet visibility and bulk choice |
| `collapsed` | bucket[] | starts `[3]` — *Priced roughly* collapsed |
| `toast` | string \| null | 2600ms timer |

Derived, not stored: `flat` (group-ordered walk list), `idx`, `cur`, the header
counts, `ready`, and the bulk target set.

**Data the server must supply** — per item: `id`, `occurred_at`, formatted date +
short date, `raw` description, cleaned `merchant`, account (`name`, `detail`,
`kind`, `currency`, `icon`, `source`), `amount`, `currency`, `rate`,
`rate_source`, `is_bcv_fallback`, `amount_usd` (nullable), `rough` label,
`needs {cat, rate, pair}`, `bucket`, `guess {cat, rule|times}`, `kind`,
`source_ref`, and for pairs: `legs[]`, `confidence`, `days`, `drift`, `implied`,
`note`, `refuse`.

**Writes**: save row (category and/or `user_rate` and/or note), accept guess,
park rows, `park_before(date)`, bring back, bulk category, confirm pair (write one
`transfer_id` across both legs), reject pair.

**Repo anchors** (from `context/REPO-RECONCILE.md`): `finances/web/services/triage.py`
(three item types, `next_item_after()`, `neighbours_of()`),
`finances/domain/rates.py` (the ladder), `finances/domain/triage_admin.py`
(`park_before()`), `finances/domain/categorization.py` + `category_rules`
(regex, priority, `min_amount`/`max_amount`), `transactions.parked` (migration
015), `transaction_edits` (009), `POST /api/transactions/bulk-edit`,
`docs/architecture/category-definitions.md` (the tests), `finances/format.py`
(formatting).

---

## Design tokens

Full source in `design/tokens/`. Everything the design uses:

**Ink** `#131312` `#22221f` `#33332f` `#4b4b46` `#6d6d69` `#9a9a95` `#bebdb8`
`#c7c6c0` `#dbdad5` `#e4e4e1` (900→050)
**Paper** `#f5f5f3` (raised) · `#ececea` (canvas) · `#e4e4e1` (sunken)
**Red — the one accent** `#fbe9e8` 050 · `#efc4c1` 200 · `#e88b85` 300 ·
**`#e5231b` 500 (brand, fills and rules only)** · **`#c51a13` 600 (text-safe)** ·
`#a5140e` 700 · `#7f0f0a` 800

**Semantic** — text `#131312` / `#4b4b46` / `#6d6d69` / `#9a9a95`, inverse
`#f5f5f3`, accent + signal `#c51a13`; surfaces canvas `#ececea`, raised `#f5f5f3`,
sunken `#e4e4e1`, selected `#e4e4e1`, inverse `#131312`, accent fill `#e5231b`
(hover `#c51a13`, active `#a5140e`); borders subtle `#dbdad5`, default `#c7c6c0`,
strong `#33332f`; warning trio `#fbe9e8` / `#efc4c1` / `#a5140e`; neutral + info
trio `#e4e4e1` / `#c7c6c0`–`#dbdad5` / `#6d6d69`.

**Type** — Doto (display, ≥22px only, one headline figure per screen), Inter
(reading), JetBrains Mono (every figure and every label). Sizes: display-3 26px,
title-2 19px, title-3 16px, body 14/1.55, body-sm 13/1.5, caption 12.5/1.45,
eyebrow 11px/0.14em mono caps 500, micro 10.5px/0.08em, num 13px,
figure 34px/700.

**Spacing** — 2 4 6 8 10 12 16 20 24 32 48 64 80 96. Rows 36 / 44 / 56. Controls
26 / 32 / 40. Sidebar 244.

**Radii** — 2 (xs/sm/**pill**) · 4 (md) · 8 (lg/xl). Nothing softer; nothing is a
lozenge.

**Shadows** — effectively none. `sm` is a 1px ring, `md` `0 1px 2px
rgba(19,19,18,0.06)`, `lg` `0 8px 24px -12px rgba(19,19,18,0.22)`, `pop`
`0 24px 48px -24px rgba(19,19,18,0.34)`. Depth comes from hairlines and from
raised sitting **lighter** than canvas. Scrim `rgba(19,19,18,0.34)` + `blur(10px)`.

**Motion** — 90 / 150 / 220 / 320ms; standard `cubic-bezier(0,0,0.2,1)`, exit
`cubic-bezier(0.4,0,1,1)`, lift `cubic-bezier(0.1,0.9,0.2,1)`.

**Three deliberate divergences from SIGNAL** (don't "fix" them): positive money is
ink with a `+`, never a colour; red **text** is `#c51a13`, not the brand red, which
fails AA at label sizes; **buttons stay in Inter** — mono caps break on labels like
*Bring back all 266*.

## Assets

- **Fonts** — Doto, Inter, JetBrains Mono via Google Fonts (`design/tokens/fonts.css`).
  Swap the `@import` for local `@font-face` rules when licensed files land.
- **Icons** — Lucide, used as stand-ins: `archive`, `check`, `check-check`,
  `chevron-down`, `chevron-left`, `chevron-right`, `circle-slash`, `file-code`,
  `git-compare-arrows`, `history`, `link`, `maximize-2`, `play`, `scale`, `search`,
  `tag`, `triangle-alert`, `undo-2`, `x`, plus the 26 category icons in
  `triage-data.js`. Sizes used: 10–17px in the UI, 28px in the empty state.
- No images, no illustrations.

## Files

In `design/`:

| File | What it holds |
| --- | --- |
| `ui_kits/finances/triage.html` | Entry point — open this |
| `ui_kits/finances/TriageScreen.jsx` | The queue screen: groups, rows, selection, sheets, toast |
| `ui_kits/finances/TriageKit.jsx` | `Money`, `Prov`, `Issues`, `Merchant`, `Why`, `CatPicker`, `RateEntry`, `PairView`, `ParkedSheet`, `TriageModal` |
| `ui_kits/finances/triage-data.js` | Fixtures: accounts, the 5 rate tiers, 26 categories with tests, rules, the 34-row queue, 4 pairs, parked, integrity |
| `ui_kits/finances/Chrome.jsx` | App shell: `Rail`, `PageHeader`, `Sheet` (560×520 at md), `Amount`, `LaptopFrame` (1440×900) |
| `assets/bodega-runtime.jsx` | The design-system components (Button, Badge, Input, Checkbox, Tooltip, Banner, Icon, SideNav…) |
| `styles.css`, `tokens/*.css` | The token layer — the authoritative values |

Reference reading in `context/`: `REPO-RECONCILE.md` (why Triage looks like this),
`KIT-HANDOFF.md` (every settled decision), `DESIGN-SYSTEM-README.md` (SIGNAL),
`KIT-README.md`, `github.md`.
