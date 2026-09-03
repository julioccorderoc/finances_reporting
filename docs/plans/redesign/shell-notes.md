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
can: Plans needs a plan table over categorised data; Ahead needs
assumptions derived from history. No fake data, no dead controls.
