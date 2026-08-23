# Acceptance criteria — Triage

74 criteria. Each is checkable by someone with the prototype open beside the
implementation. `README.md` has the values; `context/REPO-RECONCILE.md` has the
repo anchors.

Mark each ✅ / ❌ / N-A-with-reason. A phase is done when its whole group passes.

---

## A · Queue and ordering

- [ ] **A1** The queue carries **three item types** — category, rate, pair — in one
      list, not three screens.
- [ ] **A2** A transaction missing both a category and a rate is **one item with two
      badges**, not two rows.
- [ ] **A3** Sort is `(bucket, occurred_at, item_id)`: bucket 0 category, 1 rate, 2
      pair; **oldest first** inside a bucket. Not newest-first.
- [ ] **A4** The `item_id` tiebreak is present and load-bearing — 204 of 243 live
      rows share a timestamp.
- [ ] **A5** No day-grouped "Today / Yesterday" labels anywhere. Dates render
      `Jul 7`, with the year appended only when it isn't the current year.
- [ ] **A6** Three groups render with the exact labels, hints and order in the spec;
      **Priced roughly starts collapsed**.
- [ ] **A7** A group with zero rows renders nothing at all (no empty head).
- [ ] **A8** Header answers with the **blocking** count (category + pair). Rows whose
      only problem is an approximate rate are excluded from it, and the header reads
      `Nothing needs you` when blocking hits 0 even if approximate rows remain.
- [ ] **A9** Header meta shows the three counts (`N category`, `N pairs`, `N
      approximate rates` with a dot) plus `· N done in this sitting`, live.
- [ ] **A10** Row grid is `26px 64px minmax(0,1fr) 138px 138px 186px 26px`, 12px
      gap, 44px min-height, 32px horizontal padding, hairline `#dbdad5` between
      rows. Long merchant and raw strings ellipsise; nothing wraps or reflows the
      grid.
- [ ] **A11** Row background states: open `#f5f5f3`, selected `#e4e4e1`, otherwise
      transparent. Selection is **grey, never red**.
- [ ] **A12** The integrity banner shows for an unpaired leg (`leg_count ≠ 2` or
      `transfer_id IS NULL`) with the real account, date and amount in the copy.
- [ ] **A13** The parked strip shows the live parked count, the note, and a `Look at
      them` button; it disappears at zero.
- [ ] **A14** Clearing the queue shows the empty state: Doto headline at 26px, the
      sitting count in the body copy.

## B · The modal run

- [ ] **B1** Clicking a row (merchant or open icon) opens the modal **on that row**;
      `Sort all N` opens on the first.
- [ ] **B2** The walk list is built in **group order regardless of collapse** —
      collapsing never removes an entry from the run.
- [ ] **B3** Dialog geometry: max-width 880px, **height `min(680px, calc(100% −
      24px))`**, radius 8px. Paging through every entry in the queue does not change
      the frame's size by a pixel.
- [ ] **B4** Body is two columns `minmax(0,0.78fr) / minmax(0,1.22fr)` with a
      hairline between; **each scrolls independently** and the dialog itself never
      scrolls. Header and footer stay put.
- [ ] **B5** Header shows `N OF M` in mono caps and a 4px progress bar at
      `(index+1)/total` that animates over 150ms.
- [ ] **B6** Prev/next buttons **disable** at the ends — they do not vanish and are
      not dead.
- [ ] **B7** Left column shows the USD figure at `lg`, the native amount with its
      provenance chip, cleaned + raw merchant, full date, account and detail, and the
      issue badges.
- [ ] **B8** Right column shows exactly the blocks the row needs, in order: category,
      then rate (with the `AND THE RATE, IF YOU KNOW IT` eyebrow when both), then the
      optional note.
- [ ] **B9** Pair items show the pair view and **hide the footer's primary button**.
- [ ] **B10** Footer primary label is situational: `Sort and next` / `Use this rate
      and next` / `Save and finish` on the last entry; disabled until resolvable.
- [ ] **B11** Overlay is positioned within the app content (the rail stays visible
      and unscrolled behind the scrim) — scrim `rgba(19,19,18,0.34)` + 10px blur.
- [ ] **B12** Clicking the scrim closes; clicking inside the dialog does not.
- [ ] **B13** Draft edits (`cat`, `rate`, `note`) survive walking away to another
      entry and back.

## C · Keyboard

- [ ] **C1** `→` / `←` move one entry and `preventDefault` (no page scroll).
- [ ] **C2** `1`–`8` select the nth top-eight category, only when the row needs a
      category.
- [ ] **C3** `↵` saves and advances **only when resolvable**; otherwise nothing
      happens (no error flash, no submit).
- [ ] **C4** `esc` closes from anywhere, **including from inside a text field**.
- [ ] **C5** While focus is in an input, every other shortcut is swallowed — typing
      `3` in the rate field does not change the category.
- [ ] **C6** The legend `←→ move · ↵ save · esc close` is visible in the footer with
      keys in mono.
- [ ] **C7** The handler is bound only while the modal is open and is removed on
      close (no stray listeners after 40 rows).

## D · Rates and provenance

- [ ] **D1** The ladder resolves in order: `user_rate` → `binance_p2p_realized`
      (14-day cost basis) → `binance_p2p_median` (14 days) → `bcv` → unpriceable.
- [ ] **D2** Every VES amount displays a provenance chip. A BCV-priced row and a
      realized-rate row are **visibly different**.
- [ ] **D3** Non-VES rows (USD, USDT) show **no** chip — there is nothing to explain.
- [ ] **D4** With no rate inside 14 days, the row is priced with the **nearest** rate
      available, marked `≈`, and filed under *Priced roughly* — it is never left with
      no dollar figure when a nearest rate exists.
- [ ] **D5** A genuinely unpriceable row (`amount_usd IS NULL`) renders `Unpriced` in
      the row and `Can't be priced` with the `circle-slash` icon in the modal.
- [ ] **D6** An approximate rate **never blocks** the sitting; only a missing
      category does.
- [ ] **D7** Typing a rate writes `user_rate` for that row, recomputes `amount_usd`,
      and the row leaves *Priced roughly*.
- [ ] **D8** The `WOULD BECOME` figure recomputes live as the rate is typed, from
      `amount / rate`.
- [ ] **D9** The nearest-rate suggestions are real (source + how far off), each
      showing its resulting USD, and one click fills the field.
- [ ] **D10** The warning box names the actual rate, its source label and the row's
      date — no generic "rate unavailable" copy.
- [ ] **D11** Formatting matches `finances/format.py`: US grouping, **sign before
      symbol** (`−$1,200.00`), U+2212 minus, `Bs.` + non-breaking space, positive
      money prefixed `+`.

## E · Categories

- [ ] **E1** All active categories load from the database (26 today), with `kind` and
      the `active` flag respected.
- [ ] **E2** Exactly the pickable set appears (20 today): auto-only categories (Fees,
      Interest, Transfer, Opening position, Reconciliation) and deactivated ones
      (Clothing) never render in the picker.
- [ ] **E3** The eight chips are **computed from 12 months of usage**, not hardcoded,
      and are numbered `1`–`8` in the order the shortcuts use.
- [ ] **E4** Type-to-filter searches label **and** the disambiguating test, over the
      full pickable list.
- [ ] **E5** Every category's **test is visible at the moment of choosing** — the
      bottom strip on hover/selection, and inline in the expanded list.
- [ ] **E6** The test strip reserves its 44px height, so hovering chips does not
      shift the layout.
- [ ] **E7** `The other N` discloses the rest, grouped `EXPENSE` / `INCOME`,
      scrolling at 268px max-height.
- [ ] **E8** A no-match search says so and points at the tests, not a bare "no
      results".
- [ ] **E9** Retired categories are deactivated, never deleted, and existing rows
      keep them.

## F · Park

- [ ] **F1** A single row parks from the modal footer and **auto-advances**.
- [ ] **F2** Selected rows park in bulk from the selection bar, with a count in the
      toast.
- [ ] **F3** `park_before(date)` parks every **uncategorised income/expense** row
      older than the date in one call.
- [ ] **F4** The cutoff date is a **calendar picker** (`<input type="date">`) —
      never a dropdown of days, never free text — pre-filled with the current cutoff
      and hinting the oldest row's date.
- [ ] **F5** Parked rows leave the queue but **keep their money in every balance and
      report**.
- [ ] **F6** Parked rows **survive re-ingest** — re-importing a statement does not
      push them back into the queue.
- [ ] **F7** Parked rows keep their badges, so they are still legible on return.
- [ ] **F8** The parked sheet shows the live count, a real sample of rows, and the
      note about what parking means.
- [ ] **F9** `Bring back all N` returns them **in canonical order (oldest first)**
      and zeroes the count.

## G · Bulk and in-list resolution

- [ ] **G1** Every row has a checkbox with a meaningful `aria-label`; selection
      survives collapsing a group.
- [ ] **G2** Per-group `Select all N` / `Clear these` affects only that group's rows.
- [ ] **G3** The selection bar appears at ≥1 selected, shows the count in mono, and
      offers exactly: set a category, park, clear.
- [ ] **G4** Bulk sort affects **only selected rows that actually need a category** —
      already-categorised rows are left alone, and the sheet says so.
- [ ] **G5** Bulk writes go through the repo's bulk endpoint, in one transaction, and
      report the number actually touched.
- [ ] **G6** The accept-the-guess chip resolves a row in one click without opening
      the modal.
- [ ] **G7** The chip's tooltip cites the rule id **and its regex**, or the number of
      times that merchant was sorted there.
- [ ] **G8** No UI anywhere writes or edits a categorization rule.

## H · Pairs

- [ ] **H1** Pair proposals come from the service with a real confidence score, days
      apart, rate drift and implied rate.
- [ ] **H2** Confirming writes **one `transfer_id` across both legs**; the pair sums
      to zero, is excluded from income/expense, and stays in balances.
- [ ] **H3** Confirmation is **refused** above 5 days apart or 10% drift, the button
      is disabled, and the banner says why.
- [ ] **H4** `Not a pair` leaves both legs as separate rows and says so.
- [ ] **H5** Confirming a P2P sell pairing makes it the **realized cost basis** for
      VES rows in the following 14 days — visible in later rows' provenance.

## I · Visual fidelity

- [ ] **I1** Only greyscale plus the one red. No blue, green, amber or any second
      hue anywhere on the surface.
- [ ] **I2** **Positive money is `#131312` with a `+`** — never red, never green.
- [ ] **I3** Red **text** is `#c51a13`; `#e5231b` is used only for fills, rules and
      the focus ring.
- [ ] **I4** Attention vs action separate by **weight, not hue**: solid red fill =
      the way forward; red hairline on `#fbe9e8` = something needs you.
- [ ] **I5** Raised surfaces (`#f5f5f3`) sit **lighter** than the canvas
      (`#ececea`); structure comes from 1px hairlines, not shadows. Only the modal,
      the selection bar and the toast cast anything.
- [ ] **I6** Radii are 2 / 4 / 8 only. Nothing is a pill or a lozenge.
- [ ] **I7** Every figure, label, badge, tag, date and column head is JetBrains Mono
      — figures with `tabular-nums`, labels uppercase with letter-spacing. **Buttons
      stay in Inter.**
- [ ] **I8** Doto appears at ≥22px only, at most once per view (the empty-state
      headline) — never in a row, never in a sentence.
- [ ] **I9** Motion: 150ms/220ms with the house curves; the modal rises 6px and
      fades; nothing bounces; everything collapses to 1ms under
      `prefers-reduced-motion`.
- [ ] **I10** Toast copy is the specific line from the spec — never a generic
      "Saved" — and dismisses at 2600ms.
- [ ] **I11** All copy matches the spec verbatim, including the question header, the
      group hints and the parked note.

## J · Accessibility and input

- [ ] **J1** The dialog has `role="dialog"`, `aria-modal="true"` and a label.
- [ ] **J2** Focus moves into the dialog on open, is **trapped** while it is open,
      and returns to the row that opened it on close.
- [ ] **J3** `:focus-visible` shows the two-layer ring
      (`0 0 0 2px #f5f5f3, 0 0 0 4px #e5231b`) on every interactive element,
      including chips and rows.
- [ ] **J4** Every icon-only control has an accessible name (`Open this row`,
      `Previous row`, `Close`, …).
- [ ] **J5** Toasts announce politely to assistive tech; the queue count updates are
      announced, not silent.
- [ ] **J6** Tab order follows visual order in both modal columns; the category
      chips are reachable and operable by keyboard alone.
- [ ] **J7** All body text meets AA against its own background at its own size —
      check the provenance chip (10.5px) and the micro labels specifically.

## K · Data and writes

- [ ] **K1** The queue endpoint returns everything listed under *Data the server must
      supply* in `README.md` — nothing the UI needs is computed client-side from
      guesses.
- [ ] **K2** `needs` flags come from the service, and a rate item is shown as needing
      review **whatever the stored flag says** when `amount_usd IS NULL`.
- [ ] **K3** `rate_source` and `is_bcv_fallback` are carried per row and drive the
      chip; the UI never infers provenance.
- [ ] **K4** Accounts carry their own `currency` and `kind`; balances stay derived
      (`v_account_balances`) — nothing writes a stored balance.
- [ ] **K5** `bucket` is server-assigned, so list and modal agree on order.
- [ ] **K6** Guesses come from the real categorization engine, honouring regex
      scope, `priority` (lower wins), `active`, and `min_amount`/`max_amount` on
      `abs(amount)`.
- [ ] **K7** Category tests are read from the source of truth
      (`docs/architecture/category-definitions.md` or a table derived from it), not
      retyped into the UI layer.
- [ ] **K8** A stale-source indication is available for the four real sources
      (Provincial CSV drop, Binance API, P2P scraper, BCV scraper) — the design shows
      staleness, never a fake live feed.
- [ ] **K9** Every write is transactional and idempotent under a double-submit
      (double `↵`, double click).
- [ ] **K10** Writes land in `transaction_edits` so the history exists.
- [ ] **K11** A failed write surfaces an error and **does not** remove the row from
      the queue.
- [ ] **K12** After save, the advance target is computed **server-side or from the
      server's fresh order** (`next_item_after`), so it survives concurrent changes.

## L · Non-regression and finish

- [ ] **L1** No Babel-in-the-browser, no `window.Bodega` / `window.Fin` /
      `window.Triage` globals, no `triage-data.js` fixtures in the shipped code.
- [ ] **L2** The old review UI is replaced, not left running beside this one.
- [ ] **L3** Works at 1440×900 without horizontal scrolling, and degrades sanely down
      to 1200px wide. (Mobile is explicitly out of scope.)
- [ ] **L4** A 34-row sitting is **one opening and one closing** — no state is lost
      mid-run, no re-render sends the run back to the top.
- [ ] **L5** Console is clean: no errors, no warnings, no unresolved CSS custom
      properties (an undefined token renders nothing and is a silent bug).
- [ ] **L6** Nothing out of scope was built: no Plans, Ahead, pivot, Settings,
      Export, Year in review, mobile.
- [ ] **L7** Every design deviation is listed in the final report with a one-line
      reason — none applied silently.
