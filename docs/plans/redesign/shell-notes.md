# Shell notes — the viewer reskin (2026-09)

The deviations log for the shell track, in the discipline of
`design_handoff_triage/NOTES.md`: what shipped, what the design showed that
the repo has no data for, and every place the shipped code does something
other than the reference, with the reason.

Scope of the track: a shell plus a reskin, not a rethink. Same data, same
services, same structure per page. The per-surface redesigns (the monthly
pivot first) are separate, later design tracks.

---

## The shell contract (read this before touching a page)

Every full page extends `base.html`, which renders:

```text
body.shell                      flex row, canvas colour, Inter 14/1.55
  nav.rail                      244px, sticky, 100vh — partials/rail.html
  div.shell-main                the content column, flex column
    #server-restarted           the restart banner, in flow, hidden until needed
    main.shell-content          width 100%, max-width 1196px, centred,
                                padding 0 32px 48px (inside the cap)
      {% block content %}
  #tx-modal-host, #toast-host   unchanged
```

* **The cap** is `--content-cap: 1196px` (`shell.css`): the design's own
  1440×900 LaptopFrame minus its 244px rail. Padding sits inside it, so a
  page's content aligns to the same 32px gutter the triage rows use, and the
  canvas runs full-bleed behind it on a wide monitor.
* A page that owns its whole column overrides `{% block main_class %}` (triage
  does: `triage-main`) and caps itself with `.shell-cap` on the element that
  should hold the width.
* **Open every page with `page_header(question, answer, meta)`** from
  `_macros.html`. The question is the mono-caps kicker with the red square;
  the answer is THE one Doto figure of the view (34px). At most one per view.
  `meta` is rendered as given — build it with a `{% set %}` block when it
  carries badges; the `{% call %}` block is the actions slot on the right.
* Tokens: `signal.css` only. Styling: plain CSS in the page's own sheet
  (`today.css`, `flow.css`, `reports.css`, `placeholders.css`), on those
  tokens. You may delete Tailwind classes from a template; never add one —
  the vendored `tailwind.css` has no build step and an unknown class renders
  unstyled while every server test stays green.
* Icons: `{{ icon(name, size) }}` from `_icons.html`. Unknown names render
  NOTHING. The kit was widened for this track (see below); if a page needs
  another glyph, it is added to the macro and its test — by the main
  session, not by a page agent.
* The house idiom is the shipped `/triage`: hairlines not shadows, raised
  (`#f5f5f3`) lighter than canvas (`#ececea`), mono figures with
  `tabular-nums`, radii 2/4/8, red only as THE accent, positive money is ink
  with a `+`. Reusable classes already global: `.tbtn`/`.tbtn-sm`/
  `.tbtn-primary`/`.tbtn-ghost`, `.tbadge`(+`-info`/`-warning`/`-dot`),
  `.teyebrow`, `.tinput`, `.tmoney`/`.tmoney-usd`/`.tmoney-native`, `.prov-*`,
  `.tbanner`, `.tlink`, `.ticon` — all in `triage.css`, all loaded on every
  page.

## Rail composition (shipped)

Top to bottom, `partials/rail.html`:

| Slot | What | Where it goes |
| --- | --- | --- |
| Wordmark | red square + `Finances.` in mono, red full stop | `/` |
| Triage | `list-checks`, live blocking count as a red-600 mono badge | `/triage` |
| Today | `sun` | `/` |
| Flow | `arrow-left-right` | `/transactions` |
| Monthly | `calendar` | `/monthly` |
| Accounts | `landmark` | `/accounts` |
| — hairline — | | |
| Plans | `target` | `/plans` (placeholder) |
| Ahead | `route` | `/ahead` (placeholder) |
| Footer: Upload a statement | full-width secondary button, `upload` | `/transactions?upload=1#upload` |
| Footer: Rates | minor link, `percent` | `/rates` |
| Footer: Stop server | small ghost, `power`, still a plain form POST with confirm | `/shutdown` |

Active destination: `aria-current="page"`, drawn as weight 600 + ink-900
(icon too). No fill.

## Icons added to the macro for this track

`arrow-right`, `arrow-up-right`, `bookmark`, `calendar`, `chart-column`,
`circle-alert`, `circle-check`, `circle-help`, `clock`, `coins`, `database`,
`filter`, `inbox`, `info`, `list-checks`, `minus`, `pencil`, `plus`, `power`,
`route`, `sun`, `table`, `target`, `trending-up`, `upload` — Lucide 0.469.0,
pinned in `tests/web/test_icons_macro.py::SHELL_ICONS`.

## Deviations (with reasons)

_Filled in as the track lands; each entry is one line of what, then why._

- **Triage is a rail destination; the design put the review count on Flow.**
  Chrome.jsx's five destinations have no Triage — the prototype folded the
  queue into Flow with a count. This repo's Triage is its own shipped
  surface, and the brief names it first in the rail. The badge moved with it.
- **The upload control is a secondary button, not a red fill.** The brief
  makes it first-class; SIGNAL allows one red fill per screen and `/triage`
  already spends it on *Sort all N*. First-class here means size and
  position (a full-width 40px control at the foot of the rail on every
  page), not hue.
- **The Triage badge is the red-600, not the brand 500.** Same J7 reasoning
  as the primary button: paper on `#e5231b` is 4.19:1 at 10.5px.
- **`.triage-screen` is `height: 100vh`**, not the design frame's 900px:
  the rail is beside the content, not above it, and the old
  `calc(100vh - 55px)` was the top nav this shell replaced.
- **The triage scrim covers the whole content column, not just the 1196px
  cap.** The screen is full-column and positioned; only its queue is capped
  (`.shell-cap` on `#triage-queue`, inside a full-width `.triage-scroll` so
  the scrollbar sits at the viewport edge). "Covers only the content area"
  (B11) reads as everything right of the rail.
- **Narrow widths: the rail becomes a top bar under 720px.** Mobile is out of
  scope for this track; this only keeps a phone no worse than the old
  wrapping nav.

## Flow and reports — what stayed old, and why

- **Three class names on Flow are not `flow-`**: `cards--selectable`,
  `choice-chip(s)` and `tx-modal-form` — pinned by `test_bulk_ui.py`,
  `test_filters_polish.py` and base.html's dirty guard respectively. Each
  is defined in flow.css so nothing leans on app.css; rename them together
  with those tests.
- **The /transactions modal has no prev/next arrows**: the router supplies
  no `prev_url`/`next_url`, and reading them would fail the template
  contract. It is content-sized (`max-height: min(760px, 100% − 24px)`)
  rather than the triage's fixed frame — there is no paging to hold still
  for — and stays `position: fixed` because its host is at the end of body.
- **Accounts' USD line and the monthly mobile totals keep `fmt_money`**
  (ASCII minus): `test_formatting.py` pins those strings. The Flow row and
  its modal moved to `fmt_usd` (U+2212) and their pins moved with them.
- **The unpriced account line reads "Unpriced — no P2P rate"** with the em
  dash a pre-existing test asserts.
- **Range presets on Monthly are radios inside tab labels**; the active fill
  uses `:has()` (Safari ≥15.4, Chrome ≥105, Firefox ≥121).
- **/rates with no P2P median at all** shows the newest row of any pair
  under a warning badge rather than a blank figure.
- **The rates range toggle now pushes `/rates?range_days=N`**, not the
  partial's URL — a pre-existing bug the reskin surfaced. Still pre-existing
  and left alone: the monthly filter form swaps only the pivot, so the
  monthly chart goes stale after a filter change.
- **The wordmark letter is Doto at 16px**, the one Doto glyph under 22px,
  exactly as Chrome.jsx's Wordmark sets it; on the 600 red for AA.

## Verified in a browser (2026-09-03, scratch copy of the ledger)

Eight destinations at 2560×1200, 1440×900 and 1200×900: the column caps at
1196px and centres on wide, nothing scrolls horizontally, every page has one
Doto answer (triage's is its own header), `aria-current` lands on the right
rail link, and the console shows 0 errors, 0 warnings and 0 responses ≥400
on every page. An automated contrast sweep over every text node found two
pairs under 4.5:1 (fixed above); everything else clears. A triage sitting
inside the shell: the scrim covers exactly the content column with the rail
visible, focus lands in the dialog, → walks, `1` picks, ↵ saves and advances
in place ("2 OF 107"), Park advances, Esc closes and refreshes the queue and
the rail badge. Not walked by hand yet (see the handoff prompt): the Flow
modal save, bulk apply, a real CSV drop, and the rates toggle click.

## Not brought over (REPO-RECONCILE §B2)

Invented in the prototype and left there, on purpose:

- bank feeds / "Bank says $X" / "Link an account" — there is no feed;
  staleness (when each source last landed) is the honest state and is what
  Today shows;
- property + mortgage — not in the schema;
- credit cards / statement dates — no such account kind;
- the notification list — nothing can fire one;
- the fourteen hardcoded net-worth points — `services/net_worth.py` computes
  the real figure and Today uses it.

## Placeholders (REPO-RECONCILE §B1)

`/plans` and `/ahead` exist so the rail is honest about the roadmap. Each is
a SIGNAL empty state that says plainly what has to exist before the page
can. No fake data, no dead controls, one real link each.

**/plans** — kicker *Where is the money going next?* over **No plans yet**;
`target` icon; headline *Plans need a plan table.*; body: *A plan is a
monthly amount with a category behind it, and what you spent is that
category's rows for the month. That table does not exist yet, and it only
means something once Triage has sorted the rows it would read. When it
exists, this page shows what you gave a job and what is left.* Link: *Go to
Triage* → `/triage`.

**/ahead** — kicker *What happens if nothing changes?* over **Nothing to
project yet**; `route` icon; headline *Ahead needs assumptions from
history.*; body: *A forecast here runs on four assumptions: monthly income,
monthly spend, the rate, and a cushion. Each can come from the ledger's own
history rather than from you typing it, and that derivation does not exist
yet. When it does, this page carries twelve months forward from it with
every assumption shown.* Link: *See your months* → `/monthly`.

Two Doto elements per page (the header's answer and the empty headline),
the same pair the empty triage queue shows — accepted there under I8, and
here for the same reason. "Sorted" rather than "categorised" in the Plans
copy: SIGNAL's plain-words rule, and the verb Triage already ships.

## Today — what stayed old, and why

- **Tile figures carry an ASCII hyphen** (`-$48.59`), not U+2212: the
  service pre-formats `KpiTile.value` with `fmt_money`, and `test_formatting`
  pins that string. Switching the service to `fmt_usd` is a one-line change
  plus test expectations, deliberately left for the Today design track
  rather than done under a reskin ("same services").
- **The needs-you card keeps `/triage?type_filter=rate`** — the href an
  existing dashboard test asserts. The card copy is *N rows need you* over
  the tile's own count.
- **Staleness chips show the day, not the time**: `last_run_at` is UTC and
  an `HH:MM` would read four hours wrong to a Caracas owner. The full
  instant is in the chip's `title` and `<time datetime>`.
- **The flows chart has no red series at all** (ink-900 income, ink-300
  spend): a red bar on every month would have been decoration.
- **Section headings keep the words existing tests assert** ("Recent
  activity", "Net worth", "Needs review").

## Triage — one change that is not a reskin

The transfer categories are offered again in the picker (migration 022,
owner decision 2026-09-03, *"money is entering in a transitional way, not
for my expenses/income"*). Recorded in full in
`design_handoff_triage/NOTES.md` § 2026-09-03; the open half of that
question — money someone lends the owner — has its own prompt at
`docs/plans/2026-09-03-borrowed-money-prompt.md`.
