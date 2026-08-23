# Implementation prompt — Triage

Paste this whole file as the first message of a fresh Claude Code session in
`finances_reporting`, with this folder available (drop it at the repo root as
`design_handoff_triage/`).

---

## Mission

Implement **Triage** — the surface where I clean up my ledger — in this repo, as
the replacement for the review UI in `finances/web/`. The design is done and is
in this folder. Your job is to build it for real: against SQLite, the real
categories, the real rate resolver, the real `parked` flag.

Work to `ACCEPTANCE-CRITERIA.md`. It is the definition of done — 74 numbered,
checkable criteria. Nothing ships as "done" until you can point at the criterion
it satisfies.

## What is in this folder

| Path | What it is |
| --- | --- |
| `README.md` | The design spec. Every screen, component, exact hex, exact px, every interaction. Self-sufficient — read it before the code. |
| `ACCEPTANCE-CRITERIA.md` | The 74 criteria, grouped A–L. Your checklist and my review sheet. |
| `design/` | The HTML prototype. Open `design/ui_kits/finances/triage.html` in a browser and use it — it works, keyboard and all. |
| `context/REPO-RECONCILE.md` | What this repo has that the old prototype got wrong or missed. The reason Triage looks like this. |
| `context/KIT-HANDOFF.md` | Every design decision already settled, with reasons. Constraints, not open questions. |
| `context/DESIGN-SYSTEM-README.md` | The SIGNAL design system: palette, type, the three deliberate divergences. |
| `context/github.md`, `context/KIT-README.md` | Repo association and the prototype's structural thesis. |

## The design files are references, not code to copy

`design/` is a browser prototype: React UMD + Babel-in-the-browser, inline
styles, fixture data in `triage-data.js`. It exists to show intended look and
behaviour precisely. **Do not lift it into the repo as-is** — no Babel-standalone
in production, no `window.Triage` globals, no fixture module.

Recreate it in this repo's own environment, using its established patterns:
whatever `finances/web/` already uses for templating, routing, state and API
calls. If the answer is "the viewer is too thin to build this on", say so and
propose the stack in one paragraph before writing code — do not silently
introduce a framework.

**Fidelity is high (hi-fi).** Colours, type, spacing, radii, motion and copy in
`README.md` are final. Match them. Where this repo's existing viewer disagrees
with the tokens, the tokens win — and tell me what else in the viewer now looks
out of place.

## Non-negotiables

These were decided with reasons. Do not relitigate them; if one is impossible
against the real data, stop and tell me which and why.

1. **One list you sweep, one modal you walk.** The queue is a dense list grouped
   by what is wrong. Clicking a row — or *Sort all N* — opens the modal on that
   row. ←/→ walk it, 1–8 pick a category, ↵ saves and advances, esc closes. A
   sitting is one opening and one closing.
2. **The modal never resizes.** Fixed 680px height, 880px wide, both columns
   scroll internally. Paging through 34 rows must not move the frame by a pixel.
3. **A rate never blocks a sitting; only a missing category does.** Unpriceable
   rows get the nearest rate Ledger has, are marked `≈`, and land in *Priced
   roughly* where the rate can be overridden.
4. **Advance holds position.** Resolving a row lands you on whatever now occupies
   its slot — never back at the top. Arrow keys go *disabled* at the ends, not
   dead.
5. **Park is a first-class control**, including park-everything-uncategorised-
   before-a-date, with the date on a real calendar picker. 266 rows are parked
   today; they stay in every balance and report and stay reachable.
6. **26 categories, 20 pickable.** Eight usage-ranked chips + type-to-filter
   search over the rest, and each category's disambiguating test visible at the
   moment of choosing. Auto-only categories never appear. Clothing is off.
7. **No rule-writing from the UI.** Rules stay migration-managed. A guess cites
   its rule id and regex, or the number of times I sorted that merchant there.
8. **Positive money is ink with a `+`**, never a colour. Red on a credit reads as
   a loss.
9. **Every VES amount shows its provenance.** A BCV-priced row and a realized-
   rate row must not look identical — they are not the same claim.
10. **Pair confirmation refuses the implausible** — over 5 days apart or over 10%
    drift cannot be confirmed, and says why.

## Build order

Each phase ends at a gate. Don't start the next until the gate passes.

**Phase 1 — data and reads.** Queue endpoint: three item types, the
`(bucket, occurred_at, item_id)` sort, `needs` flags, per-row currency +
`amount_usd` + `rate_source` + `is_bcv_fallback`, the nearest-rate fallback with
its `≈` label, parked count, integrity warnings. *Gate: A1–A9, K1–K8.*

**Phase 2 — the list.** Groups, rows, the accept-the-guess chip, checkboxes,
selection bar, parked strip, banner, empty state, header counts. *Gate: A, G, I.*

**Phase 3 — the modal run.** Two columns, keyboard, progress, save/park/advance,
category picker, rate override, pair confirm. *Gate: B, C, D, E, H.*

**Phase 4 — Park and bulk writes.** `park_before`, bring-back, bulk category.
*Gate: F, G, K9–K12.*

**Phase 5 — polish and proof.** Motion, focus rings, reduced motion, tab order,
the 74 criteria walked one by one with the prototype open beside it.
*Gate: I, J, L.*

## Working agreement

- **Show me options, don't describe them.** When there's a real choice, build 2–4
  and put them on screen. Wording questions only for things that can't be seen —
  scope, priorities, facts about my money.
- **Any date I pick is a calendar picker.** Never a dropdown of days, never a
  text field.
- **Controls must be editable by me.** One blank field I can put anything into
  beats a clever fixed list.
- Don't touch the design. If you think something needs to change, list it as a
  recommendation with a one-line reason and keep going.
- When you finish a phase, report which criteria numbers now pass.

## Out of scope

Plans, Ahead/forecast, the monthly pivot, Settings, Export, Year in review, the
mobile pass. The pivot is next after this, and it is designed separately — don't
improvise it here.
