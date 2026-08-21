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

### The tests come from the doc, not the fixture

Criterion K7. `triage-data.js` carries its own, friendlier test sentences
(*"Food you cook at home"*). The shipped picker shows the ones in
`docs/architecture/category-definitions.md` instead — that file is authoritative
and is what gets re-ruled when an edge case recurs. A second copy in a template
would drift the first time it changed. `finances.domain.category_definitions`
parses it once per process; a pickable category with no sentence there fails the
suite by name.
