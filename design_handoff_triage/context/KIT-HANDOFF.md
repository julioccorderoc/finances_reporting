# Handoff — Ledger (personal finances kit)

Pick this up cold. Everything below is current as of the last edit.

## What this is

`ui_kits/finances/` is a working prototype of a personal-finances app, built on the
**Bodega** design system that lives in this same project (`components/`, `tokens/`,
`guidelines/`, compiled to `assets/bodega-runtime.jsx`). Open
`ui_kits/finances/index.html`. `interactions.html` holds three older variations of the
sort-a-transaction interaction (`1a` in-row, `1b` beside, `1c` one-at-a-time) — keep it,
it is referenced in conversation.

Read `ui_kits/finances/README.md` first: it states the structural thesis (destinations
named after the question they answer, one number per screen) that every change must
respect.

## How the code works

- No bundler, no JSX build. `index.html` loads React UMD + Babel standalone, then
  `data.js`, then the Bodega runtime, then each screen file **in dependency order**.
  Every file is an IIFE that reads its dependencies off `window.Bodega` / `window.Fin`
  **at load time**, so a file must be loaded *after* anything it destructures.
  (`ForecastView.jsx` must load before `PlansScreen.jsx` for exactly this reason.)
- Styling is inline only, from Bodega CSS custom properties. Never invent a token —
  grep `tokens/*.css` first; an undefined `var()` silently renders nothing.
  (`--shadow-raised` does not exist; `--shadow-lg` does.) The one exception is
  `index.html`'s `<style>` block, which holds the `input.fin-slider` rules — a range
  input's track and thumb are pseudo-elements and cannot be styled inline. Fill level
  comes in as a `--fill` percentage set inline on each slider.
- **All overlays use `position: absolute`, never `fixed`.** The mobile preview renders
  the app inside a phone frame, and fixed positioning escapes it. `Fin.Sheet`
  (in `Chrome.jsx`) is the shared modal shell — use it for anything new. The Bodega
  `Dialog` component is fixed-position and is therefore *not* used in this kit.
- Every screen takes a `compact` prop (true in the mobile viewport) and pads itself with
  `Fin.gutter(compact)`. All money renders through `Fin.Amount`.

## Files

| File | What it is |
| --- | --- |
| `App.jsx` | Routing, review-queue state, desktop/mobile switch, toast, and the shared `oneOffs` list (dated one-off bills) passed to both Plans and Ahead |
| `Chrome.jsx` | Rail, mobile tab bar, phone frame, viewport toggle, `Sheet`, `PageHeader`, `Amount`, `SplitBar`, `gutter` |
| `TodayScreen.jsx` | Safe-to-spend hero, the arithmetic bar, Needs you, Coming up, Latest |
| `FlowScreen.jsx` | Transaction history: 5 filter dropdowns + search, chips, grouping |
| `PlansScreen.jsx` | Tabs: **This month** (plan groups) / **Next twelve months** (mounts `ForecastView`). Holds `NewPlanSheet` — three kinds (bill / everyday / saving for); a bill is **every month or just once** and takes a real date off a native calendar picker, never a day-of-month dropdown |
| `ForecastView.jsx` | The forecast: paired in/out bars, month overlay, assumption sliders, one what-if control, committed plans, `AddPlanSheet`. Also mounted standalone as the `ahead` route |
| `AccountsScreen.jsx` | Net worth trend, accounts by kind |
| `ReviewModal.jsx` | Sorting a transaction — modal, two panels, keyboard `1`–`8`/`←→`/`↵` |
| `AddTxModal.jsx` | Add by hand: out / in / transfer, plus installment plans |
| `data.js` | All fixture data and the derived date/schedule helpers |

## Decisions already made (do not relitigate)

1. **Review is a modal**, not a side sheet. The old `ReviewSheet.jsx` is deleted.
2. **The forecast models, and the shape of it is settled.** Two bars a month across
   six past months and twelve future ones: **money in** (with the part you keep marked
   inside the same bar — the user asked for income and savings to be one bar, not two)
   against **money out** (split committed vs discretionary). Clicking any month opens
   an **overlay** with the breakdown — it used to be an inline card; the user wanted an
   overlay. Assumptions are four **compact sliders, two per row**. There is exactly one
   what-if control: amount + label + date + spend/get, added as removable chips — the
   three hardcoded scenario switches were cut as "stupid" (not editable without AI).
   `project(vals, tests)` runs twice per render so every number can show its delta.
3. **Installments/BNPL are first-class.** The user pays with Cashea. `data.js` holds
   `installments` (plans) and `D.expand(plans)` derives the schedule, the per-month
   totals and the total left — call it again with extra plans and the forecast picks
   them up. **Ahead ▸ Already committed ▸ Add a plan** is where a Cashea purchase gets
   entered (app, store, total, how many payments, how many already paid, next due
   date, fortnight/month); the sheet previews the derived dates and the new plan flows
   straight into the bars, the drill-down and the balance. `AddTxModal` can also create
   one while adding a transaction.
4. **Forecast placement is undecided on purpose.** It is currently mounted in *both*
   places — as the `Ahead` rail destination and as the Plans "Next twelve months" tab —
   because the user asked to see both. Only one should ship. **Ask which, by showing
   them, not by describing it.** My recommendation: the Plans tab, which restores the
   four-destination story.
5. **Notifications**: the user wants exactly two — *a plan went over* and *a weekly
   summary*. Everything else stays off by default (`data.js` ▸ `notices`).

6. **A one-off bill is not a subscription.** *New plan ▸ A bill ▸ Just once* does not
   create a monthly plan: it creates a dated one-off that lives in `App.jsx` state
   (`oneOffs`) and is handed to **both** `PlansScreen` and `ForecastView`. In Plans it
   renders as an `OnceRow` at the foot of the Bills group — date, `once` badge, remove
   — and is deliberately **excluded from this month's assigned/spent totals**. In Ahead
   it joins the projection for its month and shows up in the overlay as **Dated bills**,
   separate from **What-ifs**: one-offs are commitments (Reset leaves them, and the
   baseline includes them so the delta chip isolates the sliders), what-ifs are
   experiments. If you add another kind of dated commitment, follow this split.

## How this user wants to be asked

They said it plainly: **do not ask design questions as text forms — build the options
and show them on screen.** Use `ask_user` with a `file-options` question over 2–4 real,
complete files they can look at. Reserve wording questions for things that genuinely
cannot be seen (scope, priorities, facts about their money). This is the single most
important process note in this document.

Two more things they are sharp about: **any date the user picks is a calendar picker**
(`<Input type="date">`), never a dropdown of days or a text field; and **controls must
be editable by them, not by an AI** — a fixed list of clever options is worth less than
one blank field they can put anything into.

## What is still to build

In the user's stated priority order:

1. **Settings, with sorting rules.** The gear in the rail footer does nothing. Build a
   `settings` route: **Sorting rules** (the list — `data.js` ▸ `rules` is already there:
   match, category, how many transactions it caught, since when, auto-vs-ask; editable
   and deletable via `Fin.Sheet`), **Notifications** (`data.js` ▸ `notices`, two on),
   accounts & sync, and an entry to export. The rules list matters because the
   *"Always sort {merchant} this way"* switch in `ReviewModal` currently creates a rule
   the user can never see again.
2. **Export, both halves.** The Export button on Flow is dead. It should open a sheet
   (range, accounts, CSV or PDF) *and* lead to a **Year in review** screen —
   `data.js` ▸ `year` has the numbers ready (12 months of spend, category totals,
   biggest/quietest month, subscriptions, top merchant, one "what changed" line).
   Make it read as a report, editorial, not a dashboard.
3. **The mobile pass.** The viewport toggle proves every screen survives 390px, but the
   user asked for mobile *designed properly, with its own patterns*, and specifically
   chose **`1c` — one transaction at a time, full screen, swipe to skip** for sorting on
   mobile. `ReviewModal` currently just becomes a bottom sheet; give `compact` its own
   one-at-a-time flow. Also worth reconsidering on mobile: the Flow filter row (wants a
   sheet), and the forecast strip (wants to be swipeable).

## Triage — the replacement for `ReviewModal` (Aug 2026)

New files: `triage-data.js` (repo-shaped fixtures — currency and rate provenance on
every amount, three item types, parked rows, 26 categories with their edge tests),
`TriageKit.jsx` (money pair, provenance chip, category picker, rate override, pair
confirm, parked sheet), `TriageScreen.jsx`, `triage.html`. Standalone for now — it is
**not** wired into `App.jsx`, because `data.js` is still the old single-currency
fixture world. Wiring it in means migrating Flow first (`REPO-RECONCILE.md` §5.4).

Shaped by `REPO-RECONCILE.md` and the user's answers, all settled:

1. **The queue is a list you sweep**, and each entry resolves in a **modal you walk**.
   The list carries the fast paths (checkboxes, bulk sort, accept-the-guess chip,
   park); clicking a row — or "Sort all N" in the header — opens `TriageModal`
   (`TriageKit.jsx`) on that entry. ←/→ move, 1–8 pick a top category, ↵ saves and
   advances, esc closes; saving or parking auto-advances, so a sitting is one opening
   and one closing. The modal's height is **fixed** (680px, capped by the frame) and
   both columns scroll internally — the frame must not resize as you page through
   entries. `Fin.Sheet` is fixed-height per size for the same reason (420/520/640).
2. **An unpriceable row is priced anyway.** No P2P sell or BCV scrape within 14 days
   → Ledger uses the nearest rate it has, marks the figure `≈BCV`, and puts the row
   in **Priced roughly** where the rate can be overridden. A rate never blocks a
   sitting; only a missing category does.
3. **Old rows are parked by default** — everything uncategorised before Jan 1, 2026
   (266 rows). Out of the queue, still in every balance and report, reachable through
   the Parked control, cutoff date editable on a calendar picker.
4. **Clothing is out of the picker** (`off: true` in `CATS`) — 20 pickable, 26 defined.
   Fees, Interest, Transfer and the two adjustment categories stay auto-only.
5. **No rule-writing from the UI.** Rules are migration-managed, so the old "always
   sort {merchant} this way" switch is gone; a guess cites the rule id and its regex,
   or the number of times you sorted that merchant there.

Next in their order: the **month × category pivot** — it has to find the month
something blew up, give a monthly average per category worth trusting, compare a
category against what was meant, separate steady from noisy, and read one month in
full.

## SIGNAL — the design system adaptation (Aug 2026)

The user supplied their own system (`uploads/design-system-final.html`, "SIGNAL" — the
Nothing school) and asked the Bodega tokens to move toward it. What changed lives
entirely in `tokens/` + `assets/bodega-runtime.jsx`; `readme.md` carries the full
rationale. The short version, and the three judgement calls:

- Palette is **greyscale + one red** (`#e5231b`). The four hue families kept their
  **names** and lost their hues: `--thyme-*` is the ink ramp (action, selection,
  positive figures), `--ochre-*` is the red signal, `--clay-*` a deeper red, `--slate-*`
  grey. Renaming would have broken every component and card.
- **Positive money is ink with a `+`, never a colour.** Red on a credit reads as a loss.
- **Attention and the primary action share the hue**, separated by weight: a solid red
  fill is the way forward, a red hairline on a faint tint is something that needs you.
- Red **text** uses `--red-600`; the brand red is 3.9:1 on canvas and fails AA at label
  sizes.
- Type is Doto (display/dot-matrix, ≥22px only — one headline figure per screen, via
  `Amount size="hero"`), Inter (reading), JetBrains Mono (every figure and every label).
  **Buttons stayed in the sans** — SIGNAL sets them in mono caps, which breaks on labels
  like "Bring back all 266".
- Near-square radii (2/4/8); `--radius-pill` retired to 2px rather than deleted so old
  pill markup collapses instead of breaking. Shadows are effectively gone — hairlines
  and the lighter-raised-on-darker-canvas inversion carry depth.

Outstanding debt: three specimen cards (brand, states, semantic colour) still describe
the pre-SIGNAL palette in prose. Their swatches render from live tokens and are correct.

## Known state

Clean console apart from the expected Babel dev-transformer warning. Verified: review
modal and its keyboard shortcuts, add-transaction (including installments), all five
Flow filters, the forecast strip and savings dial, phone frame clipping overlays.
Fonts are Google Fonts and icons are Lucide stand-ins — both one-file swaps.
