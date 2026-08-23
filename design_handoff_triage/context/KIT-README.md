# Ledger — personal finances

The deliverable for the brief: *"rethink its structure from scratch — not restyle what
exists. The measure of success is that I can look at any screen and immediately know
what it is telling me and what I can do next."*

Open `index.html` for the working prototype, `interactions.html` for the three
variations on the key interaction.

---

## The restructure

Most personal-finance apps are organised around **the shapes of financial data** —
Accounts, Transactions, Budgets, Goals, Bills, Reports, Cashflow, Recurring,
Categories. Nine destinations, each a container of records, none of which answers a
question a person actually has. That's the confusion.

Ledger has **five destinations, each phrased as the question it answers**, and each
screen opens by asking that question in words and answering it with exactly one number.

| Destination | The question | The answer |
| --- | --- | --- |
| **Today** | What can I spend without breaking anything? | Safe to spend — `$1,284.60` |
| **Flow** | Where did the money actually go? | `$2,835.82` out of `$2,600.00` in |
| **Plans** | Is it going where I meant it to? | "Yes, except Fun" |
| **Ahead** | If nothing changes, where does this land? | `$8,557.60` by end of August |
| **Accounts** | What do I own, and what do I owe? | Net worth — `$261,270.35` |

Three structural decisions do most of the work:

**1. Safe to spend replaces "balance."** A balance is a true number that misleads —
it counts money already promised to rent. Today leads with what's genuinely free,
and immediately shows the arithmetic as one horizontal bar: balance = bills still
due + set aside for plans + safe to spend. The user can see *why* the number is
what it is without leaving the screen.

**2. Budgets, goals, and bills collapse into one idea: Plans.** They were three
features because they're three data shapes; they're one concept to a person —
*money with a job*. One screen, three groups (Bills, Everyday, Saving for), each
with a one-line gloss of what belongs there. "Move money" between plans is a
first-class action, because that's the thing people actually do when they overspend.

**3. Every screen has a "needs you" ceiling of three.** Today shows at most three
things requiring action, then says "then you're done for the day." An app that can
be finished is an app people open.

Two supporting rules run throughout: **colour is a judgement**, so a paid bill at
100% is thyme, not amber — only discretionary money earns a "nearly gone" warning;
and **every figure is mono tabular**, so columns align and the eye can compare
without reading.

---

## Screens

| File | Screen |
| --- | --- |
| `TodayScreen.jsx` | Safe-to-spend hero, the arithmetic bar, Needs you, Coming up, Latest |
| `FlowScreen.jsx` | In/out/saved split, six filters, transactions grouped by day |
| `PlansScreen.jsx` | Bills / Everyday / Saving for, with move-money and per-plan editing |
| `AheadScreen.jsx` | Six-month forecast: projected balance, month-by-month table, editable assumptions, what-ifs, goal ETAs |
| `AccountsScreen.jsx` | Net worth trend, accounts grouped by kind |
| `ReviewModal.jsx` | The key interaction as shipped — a modal, two columns, keyboard-driven |
| `AddTxModal.jsx` | Adding a transaction by hand: out / in / transfer |
| `Chrome.jsx` | Rail, bottom tab bar, phone frame, viewport toggle, `Sheet` modal shell, page header, and the shared `Amount` / `SplitBar` money primitives |
| `App.jsx` | Routing and the review queue state machine |
| `data.js` | Fixture data. Plausible, not real. |

## Things to click

- **Desktop / Mobile** bottom-right — the same screens in a 390×844 frame, rail
  swapped for a bottom bar, modals becoming bottom sheets. Every overlay is
  positioned inside the viewport being previewed, so the phone clips it like a
  real device.
- **Review 3** on Today, or any "Needs sorting" row in Flow — opens the modal and
  walks the queue. `1`–`8` pick a category, `←`/`→` move, `↵` saves; finishing
  fires a toast.
- **Add transaction** anywhere (or the `+` on mobile) — out / in / transfer, with
  the plan it comes out of shown before you commit.
- **Filters** on Flow — account, category, month, week, sort, plus search; active
  filters appear as removable chips with a running count and total.
- **Ahead** — edit any assumption, or stack the what-if chips, and the projection,
  the tight-month warning, and both goal dates move together.
- **Move money** on Plans — the sheet that fixes an overspend.

## Why a fifth destination

Ahead earns one because it answers a question none of the other four can: not what
is true now, but what is true if nothing changes. It keeps the rule — one question,
one number, stated in words. It is also the only screen whose numbers are guesses,
so it says so out loud, in the subhead, in a badge on the table, and by making every
assumption an editable field rather than a fact.

## Not done yet

- **Mobile is a pass, not a port.** The toggle validates that every screen and
  overlay survives 390px, but the phone deserves its own decisions: the Flow filter
  panel wants a sheet, the Ahead table wants swipeable month cards, and `1c`
  (one transaction at a time) is still the better key interaction there.
- Fonts are Google Fonts and icons are Lucide stand-ins — both one-file swaps.
