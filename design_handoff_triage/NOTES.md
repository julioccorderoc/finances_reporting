# Build notes — where the ledger disagreed with the design

The design was drawn against a stale read of `categories`. Each entry below is
a place the shipped code deliberately does something other than what
`README.md` / `ACCEPTANCE-CRITERIA.md` describe, and why.

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
