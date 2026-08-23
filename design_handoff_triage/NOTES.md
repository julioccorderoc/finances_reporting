# Build notes — where the ledger disagreed with the design

The design was drawn against a stale read of `categories`. Each entry below is
a place the shipped code deliberately does something other than what
`README.md` / `ACCEPTANCE-CRITERIA.md` describe, and why.

---

## Wave 3 — accessibility, cleanup, and the criteria walk

Built 2026-08-23. Three new deviations, and the report they came out of is
`ACCEPTANCE-REPORT.md`.

### Two text tokens darkened to reach AA — and they land on the same grey

**Deviation.** Criterion J7 wants every body text token to clear 4.5:1
against its own background at its own size. Measured in a browser against
each element's real fill, two failed:

| Token | Was | Canvas | Raised | Sunken | Now | Canvas | Raised | Sunken |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `--text-tertiary` | `#6d6d69` | 4.39 | 4.76 | 4.08 | `#666662` | 4.90 | 5.31 | 4.53 |
| `--text-placeholder` | `#9a9a95` | 2.39 | 2.59 | 2.22 | `#666662` | 4.90 | 5.31 | 4.53 |

`--text-placeholder` was not decoration: it carried the raw bank string under
every cleaned merchant name, the account detail (`· Provincial`), the chip
shortcut numbers, the `1–8` hint and the picker's disclosure note. All of it
is information, so none of it is WCAG-exempt.

They land on the **same** value, which flattens a two-step ramp into one.
That is forced, not chosen: 4.5:1 is a floor, so at 10.5–13px there is exactly
one lightest legible grey and any two tiers that must both clear it converge
on it. `#666662` is that grey — the lightest warm neutral clearing 4.5:1 on
all three paper surfaces, including `--surface-sunken`, which is what a
*selected* row is. The hierarchy those two tiers used to carry is now size and
weight, which was already doing most of the work.

The **ink ramp is untouched**: `--ink-400` and `--ink-500` still hold their
drawn values, so anything using them as a rule, an icon or a fill is
unchanged. Only the two semantic text tokens moved. No other page reads
either one (app.css references six custom properties, and these are not among
them).

**Left failing, deliberately:** the primary button's label, `#f5f5f3` on the
`#e5231b` fill, at 4.19:1. Darkening the text makes it worse. White reaches
4.58:1 but SIGNAL bans harsh white and `--text-inverse` is shared with the ink
toast; `#f5f5f3` on `#c51a13` reaches 5.45:1 but that changes the resting
fill. Both are fill decisions, and this wave was told not to change fills.
Owner's call — recorded as the one ❌ in the report.

### The rise keyframes are `triage-rise` / `toast-rise`, not `bodega-rise`

Criterion I9 names `bodega-rise`. That is the prototype's design-system
prefix; this repo's system is SIGNAL, and signal.css already renamed the
`bodega`/`thyme`/`ochre`/`clay` families on the way in. The motion is
byte-identical to `design/tokens/base.css:121` — 6px up, opacity 0→1 — under
two local names: `triage-rise` in triage.css (modal, sheet, selection bar) and
`toast-rise` in app.css.

The toast's copy is deliberate rather than shared. The toast shows on every
page and app.css is the sheet that survives if triage.css is ever dropped; a
component that ships everywhere must not depend on a stylesheet that exists
for one screen.

### I8's "at most once per view" is looser than the prototype

Criterion I8 says Doto appears "at most once per view (the empty-state
headline)". On the empty queue two Doto elements are on screen at once: the
page header's answer (`Nothing needs you`, 34px) and the empty headline
(`Queue empty.`, 26px). That is exactly what the prototype renders —
`Chrome.jsx:200` sets `--font-display` on the `PageHeader` answer and
`TriageScreen.jsx:189` on the empty headline. The parenthetical is the loose
half of the criterion, not a description of the design. Both are ≥22px,
neither is in a row or a sentence, and A8 requires the header to keep saying
`Nothing needs you` at zero.

### Two console defects the L walk turned up, both now fixed

Neither is a design deviation; recorded because both were invisible to every
server-side test.

* `hx-disabled-elt="find button[type=submit]"` on the save form matched
  nothing — `find` searches inside the form and the design puts the primary
  button in the footer, associated by `form=` id. htmx logged *"returned no
  matches!"* on every save and the double-submit guard was never armed. Now
  `[data-modal-primary]`. A Wave 2 test asserted the broken literal and was
  green; it now asserts the working one.
* No icon was declared, so every browser probed `/favicon.ico` and 404'd on
  every page load. `static/favicon.svg` — ink paper, one red rule, 32px —
  linked from `base.html`. Served from `/static` like everything else, so it
  works offline and needs no route.

### `confirm_pair` now rebuilds the realized cost basis (H5)

Not a deviation — a hole. `apply_edit` has rebuilt the materialised
`binance_p2p_realized` tier since ADR-013's 2026-07-26 amendment;
`confirm_pair` never joined that bargain, so the tier was only as fresh as
the last ingest happened to leave it. Verified end to end on the scratch DB:
a VES row three days downstream of a confirmed sell went from `bcv 784.66`
to `binance_p2p_realized_carry 240`.

---

## Wave 2 — the surface (queue, modal run, sheets, writes)

Built 2026-08-23. Everything below is a place the shipped screen does
something other than the README, with the reason.

### The picker is scoped to the row's kind

**Deviation.** The design's CatPicker is one list of 20 pickable
categories, kind-agnostic, ranked over twelve months of usage.
`transactions_write.apply_edit` refuses a category whose kind contradicts
the row's — the guard added after the ledger accumulated 65 such
contradictions, six of them income rows filed under `Fees`. Unscoped, the
dialog put `Salary` on keyboard shortcut 2 of an expense row: a 422 behind
one keystroke, found in the browser.

`picker_payload` now takes a `kind` and the modal passes the row's own, so
"Search 17 categories" and "The other 9" describe what is actually on
screen. The bulk sheet stays unscoped (it has no single row to scope to);
instead its target count filters by the chosen category's kind, so the
number on the button and the rows the write touches are one set.

### `Fees` can reach a chip on a kind with few pickable categories

Wave 1.2 kept `Fees` pickable but off the chips, and `chip_eligible` still
does that job. With the picker scoped by kind, a kind with fewer than
eight `chip_eligible` categories gets padded from the rest of its pickable
set, which can include `Fees`. It is pickable, the owner does file rows
there by hand, and a blank chip slot would be worse.

### Parked strip and sheet pluralise

README copy is `266 parked rows, out of the queue` and `266 parked rows`.
Rendered verbatim at a count of one that reads "1 parked rows". Both
strings pluralise; every other copy string is verbatim.

### The per-row unpark is gone

The old viewer let you un-park one row from the list. The parked sheet
offers `Bring back all N` and nothing else, so the endpoint had no caller —
and an endpoint no surface calls is worse than one capability fewer. If
per-row bring-back is wanted it belongs on the sheet's sample rows, which
is a design question rather than a port.

### "Not a pair" is remembered in the process, not the database

Criterion H4 says both legs stay separate rows and the UI says so. The
matcher is a pure function of the ledger and would propose the same two
rows on the very next build, so a refusal that wrote nothing would be a
button that does nothing. Writing something is worse: declining a *guess*
is not a fact about the money, and there is no column for "I looked at
this and said no". The dismissal lives on `app.state` for the run — the
same lifetime the design gives a sitting. A restart forgets it.

### `fmt_usd` / `fmt_native` sit beside `fmt_money`, they do not replace it

D11 wants U+2212, sign before symbol and an explicit `+` on a credit.
`fmt_money` emits an ASCII hyphen and no `+`, and it is what the
transactions, monthly and accounts pages, the static report, the CSV
export and the CLI all render through. Changing it would redesign five
surfaces that are not in scope. The two new functions are the SIGNAL
renderings and are used by the triage templates alone. The live rate
preview in `triage.js` mirrors `fmt_usd` in JavaScript; that is the one
duplicated formatter, and it exists because D8 is a live recompute.

### The queue's date column: `Nov 3 24`, not `Nov 3, 2024`

The README fixes `Jul 7` and says the year is appended off the current
one, without saying how. The prototype's own `shortDate` appends a
two-digit year with no comma, and that is what shipped.

### The cutoff's default, and the date under it

Nothing in the schema stores a cutoff, so the parked sheet pre-fills
January 1 of the current year — the design's own `2026-01-01` on a 2026
ledger. `The oldest one is …` names the oldest *uncategorised income or
expense* row, parked or not: the floor the cutoff could actually reach,
and exactly the set `park_before` scopes itself to.

### The content area owns the viewport

The prototype renders inside a device frame whose content area scrolls
internally. Ported literally, the page scrolled as a whole and the nav
scrolled off the top when the dialog opened — which contradicts B11. The
triage screen is now `calc(100vh - 55px)` with the queue scrolling inside
it. No other page is affected.

### The dialog carries no Cancel button

The design's footer is Park, the legend, and the primary. Closing is esc,
the scrim, or the header's `x` — all three are wired. The old modal's
Cancel button has no place in the new footer and was not smuggled back in.

### Not built, deliberately — DONE in Wave 3

The J group (focus trap, `aria-live` announcements, an AA audit of the
10.5px chip) and the L cleanup of app.css's old triage block were Wave 3.
The cheap parts of J shipped here: `role="dialog"`, `aria-modal`, an
`aria-label` on the dialog and on every icon-only control, meaningful
checkbox labels, and focus moving into the dialog on open. Everything
else landed 2026-08-23 — see §Wave 3 above and `ACCEPTANCE-REPORT.md`.

### FIXED 2026-08-23 — a rate the resolver should not have used

`rates.resolve`'s tiers are hard-coded to `quote = "VES"` and were not
checked against the transaction's own currency, so a COP row was priced
with a bolívar rate. Found while seeding a genuinely unpriceable row for
D5 (the only way to reach `amount_usd IS NULL` today is an empty `rates`
table), and carried through Waves 1.1–2 as a known defect out of scope.

Fixed in `57b7558` (`fix(rates): GREEN — the fallback ladder is scoped to
its own quote currency`), with the reasoning recorded as **ADR-021 §2.5**.
`LADDER_QUOTE_CURRENCIES` is derived from `_FALLBACK_TIERS` and
`_tiers_for()` narrows the tiers each branch walks, so the scope is the
tier table rather than a second `"VES"` literal. A non-native row outside
it — `user_rate` included — resolves unpriceable and lands in the queue as
a bucket-2 rate item. Zero live rows change: the ledger holds only
VES/USDT/USDC/USD.

The D5 fixtures can still seed no rates at all; that path is unchanged and
is now one of two ways to reach `amount_usd IS NULL`.

---

## Wave 1.2 — CatPicker data (migration 021)

Owner decisions taken 2026-08-21, before migrating.

### Counts: 26/20 in the design, 29/21 in the ledger

The design says 26 categories, 20 pickable. The table holds **29** (26 active).
After migration 021 the pickable set is **21** — 17 expense + 4 income. The
"The other N" disclosure therefore reads *The other 13*, not *The other 12*; the
number is computed, so nothing needs changing when it moves again.

### `Fees` is pickable, but never on a chip — a third state the design lacks

- Design: `Fees` is auto-only, out of the picker entirely.
- Ledger: migration 018 (owner decision 2026-08-05) put `Fees` **back** in the
  picker on purpose — hand-triage and the ADR-019 reversal cleanup both need to
  file a row there.
- Also true: `Fees` is the most-used category in the ledger by a factor of 2.5
  (371 rows in 12 months) because `category_rules` assigns nearly all of them.
  Ranking chips by usage would hand a bank commission keyboard key `1`.

**Decision:** keep it pickable, keep it off the chips. That needed a flag the
design does not model, so migration 021 adds `chip_eligible` alongside
`auto_only`:

| Flag | Means |
| --- | --- |
| `active = 0` | Retired. Never deleted (criterion E9). |
| `auto_only = 1` | System-written; a human never chooses it. |
| `chip_eligible = 0` | Pickable, but its usage count reflects rules, not choices. |

Pickable = `active AND NOT auto_only`. A chip additionally needs
`chip_eligible`. `Fees` is the only `chip_eligible = 0` row today.

### `Clothing` retires *into* `Purchases`

- Design: `Clothing` is deactivated (`off`).
- Ledger: it was active, added deliberately by migration 005, with 10 rows in
  the last 12 months.
- Owner, 2026-08-21: *"all the clothing transactions must be purchase, and yes
  we won't be using this any longer"*.

So 021 does more than the design's `off`: it moves the ten rows to `Purchases`
(and would redirect any rule pointing at it — none do), **then** deactivates.
`needs_review` is untouched, so re-filed rows do not re-enter the triage queue.
`Purchases`'s test sentence in `category-definitions.md` was widened to cover
apparel, since it is now the honest answer for it.

Side effect worth knowing: `Purchases` absorbed enough rows to overtake
`Leisure` in the chip ranking.

### `Opening position` does not exist

The design fixture lists an `Opening position` adjustment category. There is no
such category — opening balances are handled by ADR-020 restatement. The real
adjustment categories are `FX Diff` and `Reconciliation`; both are `auto_only`.

### `Interest` keeps `active = 0` for now

Under the new reading `Interest` is not retired — Binance Earn writes to it
daily — so `active = 1, auto_only = 1` would be the honest row. It is left at
`active = 0` because the **existing** viewer's pickers read
`categories.list_all()`, which filters on `active` alone; flipping it would drop
`Interest` into every old picker the same week the new one ships. Both readings
exclude it, so nothing is wrong today. The flag flip belongs with the Wave 2
cutover, once `list_all()` is no longer a picker source.

### Transfer categories are not offered by the new picker

`Internal Transfer` / `External Transfer` are `auto_only`, so `list_pickable()`
never returns them — a transfer is confirmed as a **pair**, not declared by
tagging one leg. The older `categories.list_for_kind()` still offers them, and
is deliberately left alone: it backs the surfaces where the owner says "this
money moved, it was not spent" (`finances.domain.money`).

### Icons are constrained to what `_icons.html` vendors

The fixture's names are seeded verbatim where they exist. The five categories
the fixture never modelled needed names chosen here, and they are chosen from
the 48 the Wave 1.3 icon macro actually inlines — an unknown name renders
*nothing*, deliberately, so a typo would surface as a blank square rather than
an error. `External Transfer` → `banknote`, `FX Diff` → `percent`, and the two
retired ones → `tag` / `package`, rather than the more obvious `arrow-up-right`
/ `coins` / `sparkles` / `wrench`, none of which are vendored. A test pins the
two sides together.

### The tests come from the doc, not the fixture

Criterion K7. `triage-data.js` carries its own, friendlier test sentences
(*"Food you cook at home"*). The shipped picker shows the ones in
`docs/architecture/category-definitions.md` instead — that file is authoritative
and is what gets re-ruled when an edge case recurs. A second copy in a template
would drift the first time it changed. `finances.domain.category_definitions`
parses it once per process; a pickable category with no sentence there fails the
suite by name.

---

## Wave 1.1 — rate resolver + triage payload (ADR-021)

### A3 vs the README's group order — bucket 1 is **pairs**, bucket 2 is *priced roughly*

**Deviation.** Criterion A3 says the sort is `(bucket, occurred_at, item_id)`
with "bucket 0 category, 1 rate, 2 pair". `README.md` §"Queue screen" lists the
three groups in the order *Needs a category → Proposed pairs → Priced roughly*,
and A8/D6 both say an approximate rate never blocks a sitting. Those two
orderings cannot both hold.

**Resolution: the README wins.** Rate items walk last. `bucket 0 = needs a
category`, `bucket 1 = pair proposals`, `bucket 2 = priced roughly`. The group
order and the modal's walk order come from one server-assigned bucket (K5), and
A3's own rationale — cheap decisions first, non-blocking work last — is what the
README's order actually implements. A row missing both a category and a rate
sits in bucket 0, because the category is the half that blocks.

**Where.** `finances/web/services/triage.py::_bucket_for`,
`docs/ADR/ADR-021-nearest-rate-approximate-pricing.md` §3.

### D4/K2 — "priced roughly" is a computed state, and today it is empty

The queue no longer reads `transactions.needs_review` to decide what needs a
rate; it reads the projection (`amount_usd is None`, or an ADR-021 `*_nearest`
source). On the live ledger that means the 25 rows flagged in the database
produce **no** rate item, and — because the deepest carry any bolívar row needs
is six days — the *Priced roughly* group is currently **empty**. The group,
the `≈` treatment and the nearest-rate suggestions are all real and tested;
they will not show anything until a rate gap opens. Nothing was written to the
database to achieve this.

### K1 — `merchant` is a typographic cleanup, not a merchant database

The README wants a cleaned name over the raw bank string. The repo has no
merchant table and no mapping, and live Provincial descriptions mix real names
(`LUNCHERIA MILY GOURMET`), bank jargon (`COM. PAGO MOVIL`) and pure references
(`CAR.DRV0013196230`, `TRAV003126437900…`).

`finances/format.py::clean_merchant` title-cases a string only when it already
reads like a name — all caps, two or more alphabetic words, no run of four or
more digits — and returns `None` otherwise. `None` is a supported state: the
README says the raw string then takes the top line alone. No canonical merchant
identity is inferred; that would be guessing at the owner's data.

### K1 — account `detail` carries `institution`; there is no account-number column

The design shows `Provincial · 0108 · 4471`. `accounts` has `name`, `kind`,
`currency`, `institution`, `active` and nothing else. `TriageAccount.detail` is
`institution` (nullable). Adding a column is a migration, and migrations were
owned by the Wave 1.2 session this wave.

### D9 — nearest-rate suggestions are one per tier, not an open list

"OR TAKE ONE OF THESE" implies an arbitrary set of nearby rates.
`rates_view.rates_for_day` returns exactly one candidate per tier (realized /
median / BCV) — that tier's nearest usable rate — each with its source, a signed
age (negative = published *after* the transaction) and the USD it would produce.
Three labelled candidates is what the resolver can defend; a longer list would
need a ranking rule nothing in the repo owns.

This also reverses one ADR-016 display decision on purpose: an expired tier used
to render **no** dollar figure, on the grounds that the chain had refused the
rate. The chain now approximates with it instead, and the panel is where the
owner accepts that number — so blanking it would hide the offer. It stays marked
expired.

### H1/H3 — `refused` is exposed but never true for an automatic proposal

The payload carries `refused` + `refuse_reason` per pair, from the same
`assess_pair` the write path raises on. The automatic matcher only proposes
within ±1 day and ±2% drift, so a queue proposal is never refusable; the field
earns its place on the **manual** pair picker, whose window is wider, and it is
what lets the modal grey out the button and say why before the click.

### Interim template edit — `partials/triage_queue.html` bucket labels

The Wave 2 redesign replaces these templates. Their bucket labels ("Missing a
rate" on bucket 1) became untrue the moment the buckets were reordered, so those
three strings were updated in place and pointed at the payload's own named
counts. No other template was touched by this wave.
