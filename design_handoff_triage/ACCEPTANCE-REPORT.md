# Acceptance report — Triage

> **Provenance.** The Wave 3 session walked all 113 numbered criteria in the
> browser (scratch copy of the ledger, seeded so every state existed) and
> committed its measurements to `NOTES.md` — but the per-criterion report file
> was written only inside its worktree and lost at teardown; commit `67e7d8a`
> names it without containing it. This file reconstructs the per-criterion
> verdicts from that session's verified outputs (its report, `NOTES.md`, and
> the test suite that pins each behaviour). Measurements and reasons live in
> `NOTES.md`; this is the checklist view. Reconstructed 2026-08-23.

**113 criteria (the header of `ACCEPTANCE-CRITERIA.md` says 74; the file
carries 113). 113 ✅ · 0 ❌ · 0 N-A.**

The last ❌ — J7's primary button, `#f5f5f3` on `#e5231b` at 4.19:1 — was
resolved 2026-08-24 with the owner's delegated call: the resting accent fill
is now the text-safe `#c51a13` (5.45:1). `NOTES.md` §Wave 3 has the ladder.

## A · Queue and ordering — 14/14 ✅

- A1 ✅ three item types in one list
- A2 ✅ both-issues row is one item, two badges
- A3 ✅ — with the logged deviation: bucket order is category → **pairs** →
  priced-roughly (README wins over A3's own text; NOTES §Wave 1.1)
- A4 ✅ `item_id` tiebreak present and load-bearing
- A5 ✅ no Today/Yesterday; off-year dates render `Nov 3 24` (prototype's own
  `shortDate`; NOTES §Wave 2)
- A6 ✅ exact labels/hints/order; Priced roughly starts collapsed
- A7 ✅ zero-row group renders nothing
- A8 ✅ header counts blocking only (category + pair)
- A9 ✅ live meta counts + sitting counter
- A10 ✅ exact row grid, ellipsis, no reflow
- A11 ✅ open/selected/default backgrounds; selection grey
- A12 ✅ integrity banner with real account/date/amount
- A13 ✅ parked strip, live count, gone at zero (copy pluralises; NOTES §Wave 2)
- A14 ✅ empty state, Doto headline, sitting count

## B · The modal run — 13/13 ✅

- B1 ✅ opens on the clicked row; Sort all N on the first
- B2 ✅ walk list ignores collapse
- B3 ✅ 880 × min(680, 100%−24), never resizes
- B4 ✅ two independent scroll columns, dialog never scrolls
- B5 ✅ `N OF M` + 150ms progress fill
- B6 ✅ arrows disable at ends
- B7 ✅ left column facts (USD lg, native + chip, merchant, date, account, badges)
- B8 ✅ right column blocks in spec order
- B9 ✅ pair items hide the footer primary
- B10 ✅ situational primary labels, disabled until resolvable
- B11 ✅ overlay absolute within content; rail/nav visible behind scrim
- B12 ✅ scrim closes, inside doesn't
- B13 ✅ drafts survive walking away and back

## C · Keyboard — 7/7 ✅

- C1 ✅ ←/→ move, preventDefault
- C2 ✅ 1–8 only when a category is needed
- C3 ✅ ↵ only when resolvable, silent otherwise
- C4 ✅ esc from anywhere, including inputs
- C5 ✅ inputs swallow every other shortcut
- C6 ✅ footer legend, keys in mono
- C7 ✅ handler bound only while open, removed on close

## D · Rates and provenance — 11/11 ✅

- D1 ✅ ladder order per ADR-021 (plus the §2.5 currency-scope fix: the ladder
  only prices its own quote currency)
- D2 ✅ every VES amount chips; BCV vs realized visibly different
- D3 ✅ non-VES rows show no chip
- D4 ✅ nearest rate, `≈`, filed under Priced roughly (empty on live data —
  verified on seeded fixtures; NOTES §Wave 2)
- D5 ✅ unpriceable renders Unpriced / Can't be priced (COP fixture)
- D6 ✅ approximate never blocks
- D7 ✅ typed rate writes `user_rate`, recomputes, leaves the group
- D8 ✅ WOULD BECOME recomputes live
- D9 ✅ real suggestions, one per tier with signed age (NOTES §Wave 1.1)
- D10 ✅ warning box names rate, source, row date
- D11 ✅ format.py rules: U+2212, sign before symbol, `Bs. `, `+` on credits
  (via `fmt_usd`/`fmt_native`; NOTES §Wave 2)

## E · Categories — 9/9 ✅

- E1 ✅ from the database (29 today, 26 active; NOTES §Wave 1.2)
- E2 ✅ pickable set only (21 today); auto-only and retired never render
- E3 ✅ chips computed from 12 months of usage, numbered as the shortcuts —
  with the logged deviations: picker kind-scoped; `chip_eligible` padding
  (NOTES §Wave 2)
- E4 ✅ search matches label and test
- E5 ✅ test visible at the moment of choosing (strip + expanded list)
- E6 ✅ strip reserves its 44px
- E7 ✅ The other N, grouped, 268px scroll
- E8 ✅ no-match copy points at the tests
- E9 ✅ retired categories deactivated, never deleted (Clothing → Purchases;
  NOTES §Wave 1.2)

## F · Park — 9/9 ✅

- F1 ✅ modal park auto-advances
- F2 ✅ bulk park with count in the toast
- F3 ✅ `park_before(date)` in one call
- F4 ✅ calendar picker, pre-filled, oldest-row hint (NOTES §Wave 2 on default)
- F5 ✅ parked money stays in every balance and report
- F6 ✅ survives re-ingest
- F7 ✅ badges kept
- F8 ✅ sheet: live count, real sample, the note
- F9 ✅ bring back all, canonical order, count zeroed (per-row unpark removed;
  NOTES §Wave 2)

## G · Bulk and in-list resolution — 8/8 ✅

- G1 ✅ checkboxes with meaningful labels; selection survives collapse
- G2 ✅ per-group select/clear scoped to the group
- G3 ✅ selection bar at ≥1: count, set category, park, clear
- G4 ✅ bulk touches only rows needing a category, sheet says so
- G5 ✅ one transaction through the bulk endpoint, reports touched count
- G6 ✅ accept-the-guess chip resolves without the modal
- G7 ✅ tooltip cites rule id + regex, or the sorted-here count
- G8 ✅ no rule-writing UI anywhere

## H · Pairs — 5/5 ✅

- H1 ✅ real confidence, days apart, drift, implied rate from the service
- H2 ✅ one `transfer_id` across both legs, sums to zero
- H3 ✅ refused above 5 days / 10% drift, button disabled, banner says why
  (never true for automatic proposals; NOTES §Wave 1.1)
- H4 ✅ Not a pair leaves both legs and says so (process-memory dismissal;
  NOTES §Wave 2)
- H5 ✅ confirm rebuilds the realized cost basis — was genuinely broken, fixed
  in Wave 3 (`e0ff5d5`), verified end-to-end on a seeded pair

## I · Visual fidelity — 11/11 ✅

- I1 ✅ greyscale + the one red only
- I2 ✅ positive money is ink with `+`
- I3 ✅ red text `#c51a13`; `#e5231b` never text — since the J7 resolution it
  carries the focus ring and the danger fill, while the accent fill rests on
  `#c51a13` (NOTES §Wave 3)
- I4 ✅ weight separates attention from action
- I5 ✅ raised lighter than canvas; hairlines, not shadows
- I6 ✅ radii 2/4/8 only
- I7 ✅ mono figures/labels with tabular-nums; buttons in Inter
- I8 ✅ Doto ≥22px; empty view carries two Doto figures exactly as the
  prototype does (NOTES §Wave 3)
- I9 ✅ 150/220ms house curves, rise + fade, 1ms under reduced motion
  (keyframes named `triage-rise`/`toast-rise`, motion byte-identical;
  NOTES §Wave 3)
- I10 ✅ specific toast copy, 2600ms
- I11 ✅ copy verbatim (except the logged pluralisation)

## J · Accessibility and input — 7/7 ✅

- J1 ✅ role, aria-modal, label
- J2 ✅ focus in on open, trapped, returns to the originating row by item id
- J3 ✅ two-layer ring on every interactive element (was broken — cascade
  losses fixed in `9ba0dcc`)
- J4 ✅ accessible names on every icon-only control
- J5 ✅ polite toast + queue announcements
- J6 ✅ tab order = visual order; chips keyboard-operable
- J7 ✅ everything measured AA at size, after darkening
  `--text-tertiary`/`--text-placeholder` to `#666662` and (2026-08-24, owner's
  delegated call) moving the resting accent fill to `#c51a13` — the primary
  button label now sits at 5.45:1 (NOTES §Wave 3)

## K · Data and writes — 12/12 ✅

- K1 ✅ server supplies everything (merchant = typographic cleanup only;
  account detail = institution — both logged; NOTES §Wave 1.1)
- K2 ✅ rate need computed from the projection, stored flag ignored
- K3 ✅ `rate_source` + `is_bcv_fallback` per row; UI never infers
- K4 ✅ accounts carry currency/kind; balances stay derived
- K5 ✅ bucket server-assigned
- K6 ✅ guesses from the real engine (scope, priority, active, amount bounds)
- K7 ✅ tests parsed from `category-definitions.md`, suite fails on a missing one
- K8 ✅ staleness per source available (sync status on the dashboard)
- K9 ✅ transactional + idempotent under double-submit (guard re-armed in
  `eb78bf6` — the Wave 2 selector matched nothing)
- K10 ✅ edits land in `transaction_edits`
- K11 ✅ failed write toasts and keeps the row
- K12 ✅ advance computed from the server's fresh order

## L · Non-regression and finish — 7/7 ✅

- L1 ✅ no Babel, no prototype globals, no fixture module
- L2 ✅ old review UI replaced, not left beside
- L3 ✅ 1440×900 and 1200px wide, no horizontal scroll
- L4 ✅ a sitting is one opening and one closing
- L5 ✅ console clean across six pages: 0 errors, 0 warnings, 0 ≥400
  (favicon added; `hx-disabled-elt` selector fixed; every `var(--…)` resolves,
  guarded by a parametrised test)
- L6 ✅ nothing out of scope built
- L7 ✅ every deviation in `NOTES.md` with a reason — none silent
