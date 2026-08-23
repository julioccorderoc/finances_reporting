# Bodega Design System

Bodega is an original design system built for **money software** — interfaces where
numbers are the content, density is a feature, and a wrong reading has real
consequences. It was generated from scratch for this project: no external brand,
codebase, or Figma file was supplied.

Its house style follows **SIGNAL** — the Nothing school: a soft off-white canvas with
no harsh white anywhere, greyscale carrying every state, hairlines instead of shadows,
near-square corners, monospace caps as the labelling voice, dot-matrix numerals for the
one figure that matters, and **exactly one red**, used only on what needs you.

**Ledger**, the personal-finances app in `ui_kits/finances/`, is the system's
reference implementation and the deliverable for the brief.

---

## Sources

None were provided. There was no attached codebase, Figma file, brand kit, or logo,
so every token, component, and asset here is original work created for this project.
Two consequences worth knowing:

- **There is no logo.** The mark is a red square — the system's one geometric
  signature — and the wordmark is set in type (Doto, with a red full stop). Nothing has
  been drawn or reconstructed.
- **The fonts are Google Fonts substitutes**, chosen for character rather than
  licensed from a foundry. Swap them if you have real brand faces — see *Type* below.

---

## CONTENT FUNDAMENTALS

The voice is the reason the app feels calm. It does more work here than any colour.

**Say the thing, then explain it.** Every screen leads with a plain question and
answers it with one number. "What can I spend without breaking anything?" →
`$1,284.60`. Never a dashboard of numbers with no claim attached.

**Second person, present tense, active voice.** "You've given $4,035 a job this
month." Not "Budget allocations totalling $4,035 have been assigned."

**Sentence case for anything you read.** Titles, buttons, menu items, body: sentence
case. The exception is the labelling layer — eyebrows, kickers, badges, tags, dates,
column heads — which is set in mono caps at 11px, tracked 0.14em. That contrast between
mono caps labels and sentence-case prose is most of the personality.

**Name the outcome, not the mechanism.** Buttons read `Mark as fulfilled`,
`Move money`, `Sort it` — never `Submit`, `OK`, `Confirm`. In a confirmation
dialog *both* buttons are verbs on the same axis: "Keep order" / "Cancel order".

**Numbers in copy are real.** Consequences are stated with the actual figures:
"This restocks 3 items and refunds $84.00." Vague warnings get ignored.

**Errors say what to do next**, not what went wrong. "That address is already on
file." Hints pre-empt a constraint before the user hits it: "Only your team can see this."

**Empty states distinguish two cases.** Nothing exists yet → "Your first order will
appear here." Nothing matches → "No orders match these filters," plus a way back out.

**Plain words over product jargon.** Ledger says *Plans*, not *Budgets*; *Flow*, not
*Transactions*; *Needs sorting*, not *Uncategorised*. Where a term of art is
unavoidable, the subtitle glosses it: "Bills — due on a date, same-ish every month."

**No emoji.** Ever, anywhere in product. Tone comes from word choice and the serif.

**Length discipline.** Button labels ≤ 3 words. Badges 1–2 words. Card subtitles one
clause. Body paragraphs ≤ 2 sentences before a break.

---

## VISUAL FOUNDATIONS

### Colour

**Greyscale plus one red.** That is the entire palette, and the discipline is in how
little of the red gets used.

- **Ink** — the neutral ramp, `#131312` → `#e4e4e1`. Warm-grey, flat, never blue.
  Every state that used to be coloured is now a step on this ramp.
- **Paper** — the canvas family: `#f5f5f3`, `#ececea`, `#e4e4e1`. No pure white
  anywhere; `--paper-000` is off-white by definition. Note the inversion — a **raised
  surface sits lighter than the canvas**, which is what gives cards their edge without
  a shadow.
- **Red** — `#e5231b`. The signal. It fills the one primary action on a screen, the
  brand square, and a filled badge; it tints and hairlines whatever needs attention.
  Nothing decorative is ever red. Use `--red-600` (`#c51a13`) for red *text*: the
  brand red is 3.9:1 on canvas and fails AA at label sizes.
- **The legacy hue families keep their names, not their hues.** `--thyme-*` is now the
  ink ramp (action states, selection, positive figures), `--ochre-*` is the red signal
  (attention), `--clay-*` a deeper red (destructive), `--slate-*` plain grey
  (informational). Renaming would have broken every component and card; re-pointing
  them kept one vocabulary.

Two rules this palette forces, both of which matter in money software:

1. **Positive money is ink, not colour.** Income reads with a `+` and tabular figures.
   Red on a credit reads as a loss, and green does not exist here.
2. **Attention and the primary action share the hue, and are separated by weight.**
   A solid red rectangle is the way forward; a red hairline on a faint red tint is
   something that needs you. Same distinction as a filled versus outlined badge.

Backgrounds are flat colour. **No gradients** anywhere except a single low-opacity fade
under chart areas. There is one texture — `.bodega-grain`, a 20px dot matrix in
`--ink-200` — used as optional atmosphere on canvas-level surfaces.

Imagery: the system ships none. Where a product image would sit, Ledger uses a paper
tile with a Lucide glyph at `--ink-400`.

### Type

Three families, each with one job.

- **Doto** (dot-matrix) — display only, weights 700–900. Hero lines, three-word empty
  states, the wordmark, and single big figures (a month total, a KPI tile). **Never in a
  table row, never in a sentence, never under 22px.** The dot grid stops resolving.
- **Inter** — everything you read: titles, body, labels, buttons. 16/1.55 is the
  reading spec; the dense product sizes sit under it.
- **JetBrains Mono** — every figure, ID, date, and every label in the system. Tabular
  figures mean money columns align down a table. This carries both the numeric
  discipline *and* the mono-caps labelling voice.

Scale runs 46 / 34 / 26 display, 23 / 19 / 16 title, 16 / 14 / 13 / 12.5 body, 11
eyebrow at 0.14em. Nothing goes below 12.5px except the 11px eyebrow and 10.5px mono
metadata.

**Buttons stay in the sans — a deliberate deviation.** SIGNAL sets its buttons in mono
caps, which works for `EMAIL →`; it fails for `Bring back all 266` and `Use this rate
and next`. Ledger's labels are short sentences, so the mono-caps voice is spent on
labels, badges, dates and column heads instead, where length is bounded.

### Space, radius, density

4px base, with 6 and 10 added because dense money rows genuinely need them.

Radii are **near-square: 2 / 4 / 8**, and nothing softer. Buttons, inputs, badges and
chips are 2; banners and tiles 4; cards and dialogs 8. `--radius-pill` is retired to
2px rather than deleted, so existing pill markup collapses to the right shape instead
of breaking — nothing in this system is a lozenge.

Controls come in three heights (26 / 32 / 40) and rows in three densities
(36 / 44 / 56). Picking a row density is a statement about the task: compact when
the user is hunting for one row among hundreds, relaxed when each row is a thing
they'll look at.

### Borders, shadows, depth

**Hairlines do all the structural work.** `--border-subtle` (`#dbdad5`) between rows,
`--border-default` (`#c7c6c0`) around controls. Shadows are close to absent: `xs` is
`none`, `sm` a 1px ring, `md` a 1px lift on hover. Only things that genuinely float —
dialogs, popovers, toasts — cast anything (`lg`, `pop`), and even those are a wide,
soft, low-opacity neutral. Depth comes from the lighter-raised-on-darker-canvas
inversion, not from blur.

**Transparency and blur appear exactly once**: the dialog scrim, a 34% ink wash with a
10px blur. The primary button is a flat red rectangle — no gloss, no inner highlight.

The only dark surface in the system is the Toast (and the Tooltip, the same treatment
smaller). Inverse ink says "this is a temporary layer".

### Motion

Three curves, four durations, and a strong bias toward *not moving*.

- `--ease-standard` `cubic-bezier(0,0,.2,1)` — plain ease-out, the house curve.
- `--ease-exit` `cubic-bezier(.4,0,1,1)` — things leaving accelerate away.
- `--ease-lift` `cubic-bezier(.1,.9,.2,1)` — a trace of overshoot for popovers and
  toasts. Nothing in this system bounces.

Durations: 90ms for control state changes, **150ms is the default**, 220ms base, 320ms slow.
Everything collapses to 1ms under `prefers-reduced-motion`.

### Interaction states

- **Hover** — a 4% ink wash on quiet controls; one step darker on filled ones.
  Cards that navigate lift 1px and gain `--shadow-md`.
- **Press** — a **0.5px downward nudge plus an inset shadow**. The system never
  scales a control on press; scaling reads as toy-like and hurts at this density.
- **Focus** — a two-ring halo: paper first, then red, so it survives on any surface.
  Fields switch their border to red-500 instead, so the field never changes size.
- **Disabled** — 50% opacity, no colour change, cursor `not-allowed`.
- **Selected** — a grey `--paper-200` fill with an `--ink-200` border. Selection is
  never tinted red: a red row in a ledger reads as an error, not a choice.

---

## ICONOGRAPHY

Bodega ships **no drawn iconography**. Nothing in this project is a hand-authored SVG
glyph, and nothing should be.

- The icon set is **[Lucide](https://lucide.dev) 0.454.0, loaded from CDN**
  (`unpkg.com/lucide@0.454.0/dist/umd/lucide.min.js`). This is a substitution: no
  icon set was supplied with the brief, and Lucide was chosen because its 24px grid
  and round caps match the system's hairline, warm-neutral character. **Flagging it
  as a substitution** — if you have a house set, swap it and `Icon` is the only file
  that changes.
- Access it only through the **`Icon`** component, which pins the house stroke weight
  of **1.75** and accepts names in any case (`shopping-basket`, `ShoppingBasket`).
- Sizes step **14 / 16 / 18 / 20 / 24**. Above 24, drop `strokeWidth` to 1.5 so the
  glyph doesn't read as chunky.
- Icons inherit `currentColor` by default, so they take their parent's text colour.
  Pass `color` only when the glyph must diverge from its label.
- **No emoji, ever.** No unicode characters used as icons — with one deliberate
  exception, the `−` (U+2212 minus) used in negative figures, because the ASCII
  hyphen is too short to read as a minus in tabular mono.
- Icons never carry meaning alone. Every icon-only control has a `label` that becomes
  both its accessible name and its tooltip.
- Category glyphs in Ledger sit in a **paper tile** (32px, 7px radius, hairline
  border) rather than floating — that tile is also what turns red when a
  transaction needs attention.

---

## Components

No source defined a component inventory, so this is an authored standard set, grouped
by concern. All 21 live under `components/`, each as `<Name>.jsx` + `<Name>.d.ts` +
`<Name>.prompt.md`, with one `@dsCard` HTML per directory.

| Group | Components |
| --- | --- |
| `core/` | Icon, Button, IconButton |
| `forms/` | Input, TextArea, Select, Checkbox, Radio, Switch |
| `data/` | Card, Badge, Tag, StatTile, DataTable |
| `navigation/` | Tabs, SideNav |
| `feedback/` | Banner, Toast, Tooltip, EmptyState, Dialog |

### Intentional additions

Beyond the conventional primitive set, five components exist because money software
cannot be built without them:

- **Icon** — a wrapper over the CDN glyph set, so stroke weight and naming are
  enforced in one place rather than at 200 call sites.
- **StatTile** — the headline-metric tile, because a mono tabular figure with a
  delta is a distinct, repeated pattern.
- **DataTable** — the dense record table, with a `numeric` column flag that switches
  the column to mono tabular figures. Without this, money columns don't align.
- **SideNav** — the persistent rail, because the raised-chip-on-sunken-rail pattern
  is a system-level figure/ground decision, not a per-app one.
- **Banner** — a persistent in-page message, distinct from the transient Toast.

### The preview runtime

`assets/bodega-runtime.jsx` is **generated, not hand-written**. It wraps each
component source in its own IIFE and hangs the exports off `window.Bodega` so the
cards and UI kits render in a plain browser with no bundler. The `.jsx` files under
`components/` remain the source of truth — edit those, then regenerate.

---

## Index

| Path | What it is |
| --- | --- |
| `styles.css` | The single entry point. `@import` list only. |
| `tokens/` | `fonts`, `colors`, `typography`, `spacing`, `elevation`, `motion`, `base`. |
| `components/` | 21 React primitives in five groups, each with types, prompt notes, and a card. |
| `guidelines/` | 20 foundation specimen cards — Colours, Type, Spacing, Brand. |
| `assets/bodega-runtime.jsx` | Generated bundle powering every preview in the project. |
| `ui_kits/finances/` | **Ledger** — the personal-finances app. Start at `index.html`. |
| `ui_kits/finances/interactions.html` | Three live variations on the key interaction. |
| `SKILL.md` | Agent-skill wrapper, for use in Claude Code. |

---

## Caveats

- **Fonts are Google Fonts stand-ins.** Doto, Inter, and JetBrains Mono are loaded by
  `@import` in `tokens/fonts.css` rather than shipped as binaries. Send real font
  files and that one file becomes `@font-face` rules.
- **The guideline prose still says "Bodega" in places, and a few specimen cards
  (brand, states, semantic colour) still describe the pre-SIGNAL palette in words.**
  Every card *renders* from live tokens, so the swatches are correct; the labels on
  those three are the outstanding debt.
- **Lucide is a substituted icon set**, as flagged above.
- **No logo exists** and none was invented.
- The system is built and verified against a desktop viewport (≈1400px). Responsive
  and mobile behaviour is deliberately unspecified — it needs its own pass.
