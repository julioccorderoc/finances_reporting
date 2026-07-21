# Category Definitions

Status: **authoritative** for what each category means. Owner decisions,
2026-07-21. The taxonomy itself is governed by [ADR-006](../ADR/ADR-006-category-taxonomy-revamp.md)
and [rule-006](rules/rule-006-categorization-pipeline.md); this file exists because ADR-006 lists the
*names* but never wrote down the *tests*, and the undefined edges — not
the category count — were the actual source of mis-tagging.

Rule of thumb: a category earns its place only if it changes a decision.
When two could fit, apply the disambiguating test below; when no test
settles it, leave `needs_review = 1` rather than guessing.

## Expense

| Category | Test |
|---|---|
| **Groceries** | Food consumed at home. Supermarkets, abastos, butchers, bakeries when stocking up. |
| **Going Out** | Food + drink consumed out. You consumed it — alone, or splitting with someone. |
| **Leisure** | **Non-food** recreation and experiences: tours, events, cinema, hobbies. Never a restaurant. |
| **Dating** | You paid, consumed **together**, romantic. |
| **Gifts** | You paid, **they** consume it, not romantic-shared, not a relative. |
| **Family** | You paid, a relative benefits. |
| **Lending** | You paid and expect it back. Pairs with income `Loan Repayment`. |
| **Transport** | Getting around: fuel, taxi, bus, vehicle upkeep. |
| **Health** | Prescription, doctor, lab, clinic, hospital. |
| **Personal Care** | Things you'd buy while *not* sick: pharmacy counter, grooming, barber, gym, skincare. |
| **Clothing** | Apparel + footwear. Deliberately not folded into `Purchases` (migration 005). |
| **Purchases** | Durable goods that aren't clothes: gadgets, furniture, household items. |
| **Subscriptions** | Recurring digital services. Auto-assigned (Netflix/Spotify/Disney/YT Premium). |
| **Utilities** | Electricity, water, gas, internet, phone service. |
| **Rent** | Monthly payment to the landlord. Distinct from `Utilities`. |
| **Education** | Courses, tuition, books bought to learn. |
| **Fees** | Bank commissions. **Auto-only** — `active = 0`, hidden from the picker (migration 011). |
| **Other Expense** | Escape hatch. Should trend toward zero; a large count here is a triage backlog, not a category. |

## Income

| Category | Test |
|---|---|
| **Salary** | Employment pay. |
| **Gigs** | Freelance / one-off work. |
| **Loan Repayment** | Money coming back from a `Lending` row. |
| **Other Income** | Everything else. |
| **Interest** | Binance Earn rewards. **Auto-only** — `active = 0` (migration 011). |

## Transfer + adjustment — never picked by hand

| Category | Meaning |
|---|---|
| **Internal Transfer** | Between the owner's own accounts. Double-entry, shared `transfer_id`, sums to zero (rule-002). |
| **External Transfer** | Money leaving to a third party that is *not* spending. |
| **FX Diff** / **Reconciliation** | System-written only (EPIC-006). |

## Edge rulings

These are the calls that kept recurring. Decided once — do not re-litigate
per row.

- **Dinner you paid for a friend** → `Going Out`. Not romantic, so not
  `Dating`; no object changes hands, so not `Gifts`.
- **Waterfall tour, concert, event ticket** → `Leisure`. If multi-day
  trips with lodging + flights start appearing, that is a new `Travel`
  category — add it when the rows exist, not before.
- **Pharmacy** → `Personal Care` by default; `Health` when it is a
  prescription or tied to treatment.
- **Supermarket run for a party** → genuinely ambiguous. This is why the
  `SUPERMERCADO` / `PANADERIA` auto-rules were killed in migration 006:
  merchant names do not encode intent. Tag by hand.
- **Money you expect back** → `Lending`, always, even if it feels like a
  favor. Forgetting this makes the later `Loan Repayment` income look
  unexplained.

## Retired categories

Deactivated (`active = 0`), never deleted — ids stay valid, reviving one
is a flag flip.

| Category | Why | Migration |
|---|---|---|
| `Interest`, `Fees` | Always auto-assigned; only added picker noise. | 011 |
| `Lifestyle` | Zero rows in nine months, no definition; every example routes to `Leisure` / `Personal Care` / `Purchases` / `Clothing`. | 013 |
| `Tools` | Zero rows in nine months; subset of `Purchases`. | 013 |

## History

- **004** — `Food` → `Groceries`; `Leisure` introduced meaning *going-out
  food*.
- **007** — `Going Out` added for going-out food, colliding with `Leisure`.
- **012** — collision resolved: `Leisure` redefined as non-food recreation.
  25 named-food-merchant rows moved to `Going Out`, 5 supermarket/butcher
  rows to `Groceries`. Opaque legacy rows (`CAR.DRV*`, `DR OB *`, bare
  person names) were left in `Leisure` for hand triage — they carry no
  intent signal.
- **013** — `Lifestyle` + `Tools` deactivated.
