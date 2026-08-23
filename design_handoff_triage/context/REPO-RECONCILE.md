# Repo reconcile — Ledger prototype vs `finances_reporting`

Read at commit-of-record `main`, 2026-08-17. Skim depth, as asked: schema, the
triage service, the categorization engine, the rate resolver, the category
definitions, the UX-overhaul design spec. No design was changed.

You answered: this prototype is **the replacement UI** for `finances/web/`;
money reads as **one consolidated USD number everywhere**; Cashea is **real but
tracked by hand**. You left the two chips questions to me — my calls are at the
bottom under *Calls I made for you*.

---

## The headline

**Triage is the product.** It is the one surface in the repo that works, it is
the reason this replacement exists, and the prototype is currently missing most
of what makes it good — three item types, difficulty ordering, hold-position
advance, and Park. Fix that first; it is section A1–A3 and C1–C2 below.

`docs/PRD.md` lists budgeting and forecasting as non-goals **of the repo as it
stands today**. Plans and Ahead are not fiction to be cut — they are the next
improvement, deliberately ahead of the data. What that costs is written down in
B1: each one names the table it needs. Read that as a build order, not a
verdict.

The other thing to say plainly: the repo's remaining surfaces — the dashboard,
the monthly pivot, the consolidated-USD report, the rates page — are the ones
you couldn't get value from. The replacement's job is not to port them. It is to
make checking your data easy, and every one of those surfaces is a candidate for
being replaced by something legible rather than reproduced faithfully.

Second technical headline: **every amount in your ledger has a currency and a
rate provenance**, and the prototype has neither field. That is not a formatting
detail — an unpriceable row is a distinct UI state that the prototype cannot
render at all, and it is a third of the real backlog.

---

## A. Wrong shape

The repo has this data; the prototype models it differently.

**A1 · Triage is three queues in one, not one review inbox.**
`finances/web/services/triage.py` builds a unified queue of three item types:
`RATE` (resolver could not price the row), `CATEGORY` (no category), `PAIR` (a
proposed transfer pairing between a bank deposit and a Binance P2P sell). One
transaction carrying both issues merges into **one item with two badges**.
Prototype: `ReviewModal.jsx` knows only "pick a category".

**A2 · Queue order is by difficulty, then age — not newest-first.**
Sort is `(bucket, occurred_at, item_id)`: bucket 0 = category only, 1 = needs a
rate, 2 = pair proposals. Oldest first inside a bucket. The prototype's queue is
"latest needing you". Also load-bearing: **204 of 243 live items share a
timestamp** because the Provincial CSV has no time component — the `item_id`
tiebreak exists for that reason, and it means day-grouped "Today / Yesterday"
labels carry almost no signal on bank rows.

**A3 · Advance after save holds position.**
`next_item_after()` lands you on whatever now occupies the resolved row's slot,
never back at the top; a **partial** fix (rate saved, category still missing)
deliberately keeps its place rather than reopening. Arrow keys walk one slot and
go *disabled*, not dead, at the ends (`neighbours_of`). The prototype advances
naively and treats every save as a resolution.

**A4 · Categories: 26 active across four kinds, with written tests.**
`002_seed_categories.sql` plus migrations 003–019; `docs/architecture/category-definitions.md`
is authoritative. Expense alone is 18 categories (Groceries, Going Out, Leisure,
Dating, Gifts, Family, Lending, Transport, Health, Personal Care, Clothing,
Purchases, Subscriptions, Utilities, Rent, Education, Fees, Other Expense), plus
income (Salary, Gigs, Loan Repayment, Other Income, Interest) and never-hand-picked
transfer/adjustment ones. `Fees` and `Interest` are `active = 0` — auto-only,
hidden from the picker. Retired categories are deactivated, never deleted.
Prototype: 8 flat categories + 2 "self-sorting", no kinds, no active flag.
The important part is not the count — that doc says plainly that **the undefined
edges, not the category count, were the source of mis-tagging**, and it carries
the edge rulings (dinner you paid for a friend → Going Out; pharmacy → Personal
Care unless prescription; money you expect back → Lending, always). Those tests
exist in a markdown file and nowhere in any UI.

**A5 · Rules are scoped regex with priority and amount bounds.**
`category_rules`: `pattern` (regex, case-insensitive), `category_id`, optional
`source`, optional `account_id`, integer `priority` (lower wins, ties broken
toward the more specific rule), `active`, and `min_amount`/`max_amount` on
`abs(amount)` (migration 017 — your salary rule is "a Binance deposit over
$1,000", which is unwriteable as text alone). Prototype: `match: 'Sunrise Market'`
plain strings with a matched count and a since-date. Also — rules are
**migration-managed on purpose** (rule-006; a category admin UI is a stated
non-goal), so `ReviewModal`'s "always sort {merchant} this way" switch promises a
write the repo does not currently sanction from the UI.

**A6 · Accounts are six kinds, each with its own currency, and balances are derived.**
Schema kinds: `bank`, `crypto_spot`, `crypto_funding`, `crypto_earn`, `cash`,
`other`; every account row carries `currency`. `v_account_balances` is
`SUM(transactions.amount)` per account — a balance is not a stored number.
Prototype: Cash / Credit / Investments / Property, no per-account currency, and
a Property row with a mortgage that has no equivalent anywhere in the schema.
**This directly affects the override UI I built last turn**: hand-correcting a
balance cannot overwrite a field, because there is no field. The repo's real
mechanism is ADR-018 reconciliation adjustments (write an adjustment row that
explains the difference) and ADR-020 opening positions. The interaction I built
is right; the write it implies is wrong. That is the one place where a design
change is genuinely needed, and I did not make it.

**A7 · Transactions carry more than the prototype's row.**
`transactions`: `currency`, `user_rate`, `transfer_id`, `source`, `source_ref`,
`needs_review`, `parked`, `notes`, plus `kind` in (income, expense, transfer,
adjustment). The projected card adds `amount_usd`, `rate_source`,
`is_bcv_fallback`. Prototype row: date, merchant, account, amount, cat, review,
note. Missing: currency, rate, rate provenance, kind, source, transfer link,
parked.

**A8 · Formatting is already decided, and differs from the prototype.**
`docs/plans/ux-overhaul/00-design.md` locks: US grouping `1,234.56`; sign
**before** symbol (`-$1,200.00`, never `$-1,200.00`); dates as `Mon, Jul 7` with
the year appended only when it is not the current year; VES as `-Bs. 45,231.10`.
`finances/format.py` is the single source of truth. `Fin.Amount` should be held
to those rules.

**A9 · "Today" is a fiction date.**
`data.js` pins today to 2026-03-14; live ledger data runs to roughly Jul–Aug 2026.

---

## B. Ahead of the data

The prototype shows it; the repo has nothing behind it yet.

### B1 · The roadmap — what each one costs

These are the next improvement, not a mistake. Each row is the table it needs.

| What | What has to exist first |
| --- | --- |
| **Plans / envelopes** (Bills, Everyday, Saving for, assigned vs spent) | A `plan` table (name, group, monthly amount, category link) and a rule for what "spent" counts — almost certainly `SUM` of that category's rows in the month, which means every plan is really a saved query over categorised data. Triage feeding it is the dependency. |
| **Safe to spend** (`checking − billsDue − assigned`) | Falls out of plans plus a due date per bill. No new machinery beyond B1's first row — it is a read of it. |
| **Installments / Cashea** | A real table: plan, quota amount, count, paid count, next due, cadence, and the down payment and fees the prototype currently ignores. Nothing exists today but two categorization rules (`cuota cashea|cashea` → Loan Repayment / Purchases, migrations 002/006). **This is the clearest new table the replacement justifies** — you are tracking it by hand right now. |
| **One-off dated bills** (`oneOffs`) | A dated row on the same table as plans, flagged once. Cheap once plans exist. |
| **Ahead** — 12-month forecast, assumption sliders, what-ifs | The four assumptions can be *derived* from history today (`v_monthly_summary`, `monthly_view.py`, `category_stats.py`) rather than typed — which would make them honest instead of invented. The sweep and the cushion floor are genuine inventions and need a stated rule. |
| **Year in review** (`year`) | Mostly available already: `monthly.py`, `consolidated_usd.py`, `category_stats.py`. The least-blocked thing on this list. |

### B2 · Invented, and probably should go

| What | Why |
| --- | --- |
| **Bank feeds** — "Bank says $X", "Link an account", live sync | There is no feed and there won't be one. Data arrives from a Provincial CSV drop, the Binance API, the cash CLI, and two rate scrapers. **Staleness** is the real state to show — when did each source last land — not a feed lag. |
| **Property + mortgage** | Not in the schema, and not something the ledger tracks. |
| **Credit cards** (`kind: 'Credit'`, statement dates, "Visa Signature") | No credit account kind, no statement concept. Cashea is doing this job in your real life. |
| **Notification list** (`notices`) | No table, and nothing in the repo can currently fire one. |
| **Net worth, 14 hardcoded points** | Invented in the prototype only — `services/net_worth.py` (8.4k) computes this for real. Wire it. |

---

## C. Ignored

The repo tracks it; the prototype has nowhere to show it. Ranked by how much of
your real usage it covers.

1. **Park — the durable "not now".** `transactions.parked` (migration 015) plus
   `domain/triage_admin.park_before()`, which parks every uncategorised
   income/expense row older than a date in one call. The docstring names your
   actual decision: *"I am not going back through 2025."* Parked rows leave the
   queue, keep their money in every balance and report, survive re-ingest, and
   show in a separate group with their own count and their live badges. With a
   backlog in the hundreds and sittings of 10–40 rows, this is the single most
   important missing control in the prototype.
2. **Rate triage.** A row the resolver cannot price has `amount_usd = None` and
   is shown as needing review *whatever the stored flag says*. Fixing it means
   typing a `user_rate` for that row. The prototype has no rate field, so a
   third of the real backlog (341 missing-rate rows against 412 uncategorised,
   at the time of the UX audit) is unworkable in it.
3. **Rate provenance on every amount.** Five tiers, in order: `user_rate` →
   `binance_p2p_realized` (your cost basis, valid 14 days) →
   `binance_p2p_median` (14 days) → `bcv` (uncapped floor) → unpriceable. The
   card exposes `rate_source` and `is_bcv_fallback`. Since you want one
   consolidated USD number everywhere, that number needs a visible source and a
   stale/fallback treatment — otherwise a BCV-priced row and a realized-rate row
   look identical and they are not remotely the same claim.
4. **Transfer pairing.** Double-entry: a transfer is two rows sharing a
   `transfer_id`, summing to zero, excluded from income/expense but included in
   balances. Triage proposes pairings with a confidence score; confirming refuses
   implausible ones (>5 days apart, >10% drift). Unpaired legs raise an integrity
   warning banner (`leg_count ≠ 2`, or `transfer_id IS NULL`). The prototype's
   `AddTxModal` has a transfer mode with no second leg, and Flow has no notion
   of pairing at all.
5. **Bulk edit.** Checkbox per row, select-all, action bar, `POST /api/transactions/bulk-edit`.
   Prototype: one at a time only.
6. **Category picker as designed.** Top 8 most-used as chips (computed from 12
   months of usage) **plus a type-to-filter search** over the full active list —
   because 26 options in a native select was the original complaint. Prototype's
   fixed eight with no search reproduces the old problem from the other
   direction: it's fast, and it can't reach the other 18.
7. **Edit history.** `transaction_edits` (migration 009). No surface in the
   prototype.
8. **Saved views.** Migration 010 — saved filter sets. Flow has five live filters
   and no way to keep one.
9. **Monthly pivot.** `services/monthly_view.py` (21k) — month × category, with a
   mobile card variant. The prototype has no equivalent report view.
10. **Ingest and staleness.** `import_state` / `import_runs`, refresh-on-open,
    browser drop for a Provincial statement, cash-add by hand, VPN hint on a
    Binance geo-block. The prototype's Accounts screen implies always-fresh data.
11. **Earn positions.** `earn_positions` (principal, APY, started/ended) and
    realized cost basis (ADR-013). Nothing in the prototype.

---

## The five to fix first

1. **Add rate to triage.** A rate field and a visible provenance chip per row —
   without it the prototype cannot work a third of your real queue.
2. **Add Park, including park-everything-before-a-date.** The backlog is in the
   hundreds; you work 10–40 at a sitting. Park is how the queue becomes finite.
3. **Rebuild the category picker to 26 categories:** eight usage-ranked chips
   plus search, with each category's disambiguating test visible at the moment
   of choice — the definitions doc says the edges, not the count, cause the
   mis-tagging.
4. **Make Flow currency-aware:** native amount, consolidated USD, rate source,
   and an explicit "can't be priced" state. Then bulk-select and saved views on
   top of it.
5. **Don't port the rest of the viewer — replace it.** The dashboard, monthly
   pivot, consolidated-USD report and rates page are the surfaces you couldn't
   read. Each one is worth rebuilding around a single question the way Today and
   Flow are, and the pivot in particular (month × category, 21k of service code)
   is the one report that would tell you whether a plan in B1 is even realistic.
   Fidelity to those screens is not a goal.

---

## Calls I made for you

You skipped both chips questions, so:

**What goes wrong in triage** — I did not guess; the repo already says it, in
`docs/plans/ux-overhaul/00-design.md`: a 26-option native select with no search,
one row at a time, no bulk, a 412-row uncategorised backlog with 341 missing
rates, a silent category-wipe bug on save, and no toast or error feedback at all.
The prototype fixes the picker's speed and the feedback, and keeps the
one-at-a-time and no-bulk problems.

**Which of today's features must survive** — all of them, in this order: park,
rate triage, pair confirm, bulk edit, category chips + search, notes, saved
views, edit history, category stats, statement upload, cash-add, refresh-on-open.
I found no candidate for dropping; the viewer is thin already, and each of these
answers a decision you make repeatedly. If something here is dead weight in
practice, that is knowledge only you have — say which and I will mark it
droppable.

**Depth** — skimmed, as asked. I did not read the 55k roadmap, the 36k logic
review, the 20 ADRs in full, or the six ux-overhaul work packages (49k–74k
each). If any of the five above gets picked up, the matching ADR is worth
reading first: ADR-005 and ADR-016 for rates, ADR-006 and rule-006 for
categorization, ADR-012 for the viewer, ADR-018 for balance corrections.
