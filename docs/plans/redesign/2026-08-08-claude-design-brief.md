# Redesign a personal finances app

You are redesigning the entire interface of a personal finances application. The current one works but is confusing to use, and I want you to rethink its structure from scratch — not restyle what exists.

**Design for clarity.** The measure of success is that I can look at any screen and immediately know what it is telling me and what I can do next. Minimal, calm, uncluttered. I would rather have four obvious screens than eight clever ones.

---

## 1. Who uses this

One person. Me. There are no other users, no accounts, no login, no sharing, no onboarding, no marketing surface. It runs on my own machine and I reach it from my laptop and from my phone on the same Wi-Fi network.

I am the owner and the only operator. Assume I am competent but that I open this app maybe twice a week and forget what every control does in between.

---

## 2. What the app is

A ledger of every movement of my money — bank accounts, crypto, cash. It is not a budgeting app, not an investment tracker, not a bill reminder. It answers three questions: *what happened*, *what did it cost me in real terms*, and *where do I stand*.

A local database is the single source of truth. All figures come from actual account movements, never from estimates.

---

## 3. The domain — read this or the design will not make sense

I live in Venezuela. This creates one complication that shapes the entire product:

**My money exists in two currencies with an unstable exchange rate between them.**

- Bank accounts hold **VES** (bolívares). Prices there are in the thousands —  a phone bill is `3,000 VES`, a grocery run `30,270 VES`.
- Crypto and cash hold **USDT / USDC / USD** — effectively dollars.
- To know what a VES expense actually *cost me*, it must be converted to dollars. **The exchange rate used for that conversion is a judgment call, not a fact.**

There are three candidate rates on any given day, and they disagree:

| Rate | Today | Meaning |
|---|---|---|
| My own realized rate | varies | What I actually got when I last sold USDT for VES. Most truthful. |
| Binance P2P median | `861.89` | The street market rate. Good default. |
| BCV (government official) | `757.54` | Reference only. Using it overstates what I have by ~14%. |

The app resolves a rate automatically in priority order and marks a transaction **"needs review"** when it could not find a trustworthy one. Reviewing those is a big part of my routine.

**A second complication:** when I sell USDT for bolívares, that is *one* economic event recorded as *two* rows — a crypto sale on Binance and a deposit in my bank a few minutes later. The app proposes matches between them, and I confirm or reject each proposal. An unmatched pair inflates my income and corrupts my rate history, so this also needs regular attention.

You do not need to solve these problems. You need to design an interface where resolving them is fast and obvious.

---

## 4. What the app does today — complete feature inventory

Nothing here may be silently dropped. You may merge, relocate, demote, or
combine any of it — but tell me what you did with each item.

**Viewing**

- Headline net worth, in dollar terms, split across bank / crypto / cash
- This month's spending and income
- Income-vs-expense trend over recent months (chart)
- Category × month breakdown grid, with expense / income / net views
- Per-account balances, in native currency and dollar equivalent
- Full transaction list with search, date range, and filters for account, kind, currency, source, categorized-or-not, paired-or-not
- Exchange rate history chart and latest rate per currency pair
- When each data source last synced successfully

**Working**

- A queue of unresolved items, oldest first, of three kinds:
  - **rate** — no trustworthy dollar value
  - **category** — uncategorized
  - **pair** — a proposed match between a crypto sale and a bank deposit
- Resolving one item advances to the next, with keyboard shortcuts
- "Park" an item to remove it from the queue durably without resolving it
- Edit a transaction: **category**, **exchange rate override**, and **notes**
  only. Amount, date, description, account, and source are permanently
  read-only — they are the audit trail
- Bulk-assign a category to many transactions at once
- Manually pick which deposit pairs with which sale, when the proposal is wrong
- View a transaction's edit history
- Upload a bank statement CSV, preview what will be imported, then confirm
- Warnings when the data is internally inconsistent
- Stop the server from the browser

**Not currently in the interface at all** (command line only — consider whether any deserve a home): logging a cash expense, triggering a data sync manually, running a health check on the database.

---

## 5. What I actually do, ranked

**The weekly ritual** — the reason I open this app:

```text
upload the bank statement  →  clear whatever it flagged  →  see where I stand
```

Then, less often, in rough order of frequency:

1. **Clear the queue.** 20–50 flagged items after an import. This is the bulk of my time in the app and the only part of the current design I actually like
2. **Check where I stand.** Net worth, this month's burn, is it going the wrong way
3. **Answer a specific question.** "Why was March's income so high?" "What did I spend on groceries this year?" This is where the current app fails worst.
4. **Find one transaction.** I remember roughly the amount and roughly when.
5. **Sanity-check the data.** Is the rate current, did the last sync work, does anything look wrong.

**On my phone** I am not doing any of the above properly — I am glancing at where I stand, or checking one thing. Deep work happens on the laptop. Design accordingly: the phone should not be a shrunken desktop, it should be the subset that makes sense standing up.

---

## 6. What is wrong today — diagnosed, with evidence

Solve these. Do not preserve them.

**1. Six flat navigation links with no hierarchy.** Dashboard, Transactions, Monthly, Triage, Accounts, Rates. Nothing signals which of these I need weekly versus twice a year. Exchange-rate history sits at the same level as the work queue.

**2. Four screens are secretly the same screen.** Accounts is *transactions filtered by account*. The category grid drills into *transactions filtered by category and month*. Recent activity is *transactions, most recent*. They all funnel into the same list but present as four unrelated products.

**3. Drill-down loses the thing you clicked.** I clicked March on the trend chart to find out why income was high. It dropped me into a six-month grid, defaulted to the Expense tab, and never showed me the income I was asking about. Three separate failures in one click.

**4. The filter bar is a control panel.** Ten controls visible at once, all equal weight, four of them chip groups. Finding one transaction means reading a form first. Saved views were added on top to paper over this.

**5. Too many controls with no evident purpose.** There are buttons on these screens whose function I genuinely do not know, and I built this.

**6. The most important action is buried.** Uploading the bank statement is the reason I open the browser. It is currently a collapsed accordion partway down a secondary page.

**7. The queue is orphaned.** The one screen that works well is reachable only via a nav link and one small tile.

**8. Two competing charts.** The overview has a trend chart; the breakdown page has another. Neither is clearly the canonical one.

**9. No stated mental model.** Nothing anywhere tells me the shape of the system: *money comes in → it gets categorized → it gets valued in dollars → it gets verified → it gets reported.* Without that spine, six screens read as six unrelated tools.

---

## 7. Non-negotiable requirements

1. **Responsive, phone and desktop.** I reach this from my phone over local Wi-Fi. Every screen needs a designed phone layout — not a reflow afterthought. Touch targets sized for thumbs.
2. **Uploading a bank statement is a primary, always-reachable action.** Today the only permanently-visible control in the app is a "Stop server" button pinned in the navigation bar. Upload deserves at least that tier: reachable in one action from any screen, at any width. It is the reason I opened the browser.
3. **The weekly ritual must be a visible path,** not three unrelated destinations I have to know to string together.
4. **Every drill-down lands on exactly what was clicked,** with that context shown and reversible. Click March income, see March income.
5. **The answer is never hidden behind a tab I have to guess at.**
6. **Amount, date, description, account and source are permanently read-only** everywhere. Only category, rate override, and notes are editable. This is an audit trail and the design must make that boundary feel intentional rather
   than broken.
7. **Every number must be traceable.** For any dollar figure I must be able to see which exchange rate produced it and why that rate was chosen. Values derived from the government BCV rate must be visually distinguishable — they are not trustworthy.
8. **Signed amounts.** Expenses are negative and must read as negative.

---

## 8. Anti-goals — do not do these

- Do not keep the current six-screen structure. Rethink the information architecture from the jobs.
- Do not add budgets, goals, forecasts, projections, savings targets, streaks, gamification, or advice. This is a ledger.
- Do not add multi-user anything: no login, no profiles, no permissions, no sharing, no invites, no notifications.
- Do not add onboarding, empty-state marketing, tours, or tooltips-as-crutch. If a screen needs a tour, redesign the screen.
- Do not use decorative illustration, gradients, glassmorphism, or animated flourish. Restraint reads as trustworthy here; money interfaces that look playful read as unserious.
- Do not use color as the only carrier of meaning.
- Do not put dense data in a plain HTML `<table>` for the list views — they must adapt from a narrow phone column to a wide desktop row. Use CSS Grid card rows. (A true matrix grid, like category × month, may of course be a grid.)
- Do not require horizontal page scrolling on any screen at any width.

---

## 9. Technical constraints

- Deliverable is **pure static HTML with Tailwind utility classes**. No framework, no build step, no JavaScript beyond what a mockup needs to be legible.
- It gets implemented as server-rendered HTML fragments swapped in place. Favor designs where one region updates at a time; avoid patterns that need client-side state management.
- Must work fully offline — no external fonts, CDNs, icon services, or remote images. System font stack, inline SVG only.
- Charts render through a simple canvas charting library. Keep chart types ordinary: bars, lines, stacked bars.
- Light mode only is fine. Dark mode is welcome but not required.

---

## 10. Real data — use these values in the mockups

Do not invent generic sample data. Realistic content is how I will judge whether the layouts actually hold up.

**Scale:** 2,772 transactions total, roughly 300 per month, 11 months of history. A weekly bank import adds 35–60 rows.

**Current backlog:** 24 items need a rate decision · 50 uncategorized · 30 parked. Assume a working queue of 20–80 items and design for the high end.

**Accounts and balances:**

| Account | Type | Balance |
|---|---|---|
| Binance Earn | crypto savings | 6,752.51 USDT |
| Binance Funding | crypto | 455.18 USDT |
| Binance Spot | crypto | 0.19 USDT |
| Cash USD | cash | 2,184.00 USD |
| Provincial Bolívares | bank | 1,644.89 VES |
| Bancamiga Bolívares | bank | 0.00 VES |
| Venezuela Bolívares | bank | 0.00 VES |

Headline net worth: **$9,394** (dollar-denominated, using the P2P rate).

**Rates today:** USDT/VES P2P median `861.89` · BCV USD/VES `757.54`.

**Categories**

- expenses: Groceries, Rent, Transport, Utilities, Health, Going Out, Dating, Family, Gifts, Purchases, Clothing, Personal Care, Subscriptions, Education, Leisure, Lending, Fees, Other Expense
- Income: Salary, Gigs, Loan Repayment, Other Income
- Transfers: Internal Transfer, External Transfer
- Adjustments: Reconciliation, FX Diff.

**Real transaction descriptions** — bank statement text is cryptic and
uppercase, which is exactly why categorizing it is manual work:

```text
041202903600 DIGITEL PRO          -3,000 VES      expense
CARLOS MANUEL GARCIA N            -5,400 VES      expense
DR OB V07497286 105MERCA         -11,500 VES      expense
DR OB V21141963 102BANCO         -30,270 VES      expense
COM. PAGO MOVIL                      -34.50 VES   expense (bank fee)
ABO.DRV0027142544                +14,630 VES      income
Binance deposit USDC              +1,540.00 USDC  income
Earn reward REALTIME USDT              +0.0347 USDT
```

**Formatting conventions to keep** (deliberately chosen, they work):

- US number grouping: `1,234.56`
- Negative sign before the currency symbol: `-$1,234.56`
- Dates include the weekday: `Fri, Aug 8`
- Tabular/monospaced figures so columns of numbers align

---

## 11. What to deliver

**Phase 1 — the thinking, before any pixels.**

Propose the information architecture and defend it:

- How many screens, what each one is for, and the one sentence I would use to describe it to someone else
- Where every feature from §4 went — a mapping, so nothing is lost by accident
- The navigation model, and how it makes the weekly ritual (§5) obvious
- How the phone version differs from the desktop version, and why
- Which of §6's nine problems each decision solves

If you think the right answer is three screens, argue for three. If it is five, argue for five. I am not attached to any number — I am attached to never again being unable to answer a simple question about my own money.

**Wait for my approval before Phase 2.**

**Phase 2 — static HTML mockups.**

Every screen, at both phone and desktop widths, populated with the real data from §10. Include the states that actually occur: the queue mid-work, the queue empty, a transaction being edited, a pair proposal awaiting a decision, an import preview, and a value the app could not resolve a rate for.

---

## Handoff (after Phase 2 is approved)

Once the design is locked, produce a **handoff prompt** for an AI coding agent that will implement it in an existing codebase. It should state the component inventory, the layout rules, the spacing and type scale, the color tokens and what each means semantically, the interaction behaviors, and the responsive breakpoints — precisely enough that the implementation matches without needing to reverse-engineer the mockups.

---

*One forward-looking note, not part of this design: entries may eventually arrive from my phone via a messaging bot before the matching bank movement is imported. Do not design for it — but avoid an architecture where a future "unconfirmed entry" concept would have nowhere to live.*
