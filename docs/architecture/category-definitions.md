# Category Definitions

Status: **authoritative** for what each category means. Owner decisions,
2026-07-21. The taxonomy itself is governed by [ADR-006](../ADR/ADR-006-category-taxonomy-revamp.md)
and [rule-006](rules/rule-006-categorization-pipeline.md); this file exists because ADR-006 lists the
*names* but never wrote down the *tests*, and the undefined edges — not
the category count — were the actual source of mis-tagging.

Rule of thumb: a category earns its place only if it changes a decision.
When two could fit, apply the disambiguating test below; when no test
settles it, leave `needs_review = 1` rather than guessing.

The **Test** column is machine-read. `finances.domain.category_definitions`
parses the `## Expense` and `## Income` tables below and the triage picker
shows the sentence at the moment of choosing, so the wording here is the
wording on screen. A pickable category with no row here fails the suite by
name — write the sentence, never hardcode one in a template.

## Which categories can be picked by hand

Three flags on `categories`, since migration 021:

| Flag | Means |
|---|---|
| `active = 0` | **Retired.** Nothing new should land here. Never deleted — existing rows keep the id, and reviving one is a flag flip. |
| `auto_only = 1` | **System-written.** Real rows land here constantly; a human just never chooses it. All transfer and adjustment kinds, plus `Interest`. |
| `chip_eligible = 0` | Pickable, but kept off the eight numbered chips — its usage count reflects rules, not choices. `Fees` only. |

**Pickable = `active = 1 AND auto_only = 0`.** A chip additionally needs
`chip_eligible = 1`.

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
| **Purchases** | Durable goods: gadgets, furniture, household items — and clothes and footwear, which used to have their own category (migration 021). |
| **Subscriptions** | Recurring digital services. Auto-assigned (Netflix/Spotify/Disney/YT Premium). |
| **Utilities** | Electricity, water, gas, internet, phone service. |
| **Rent** | Monthly payment to the landlord. Distinct from `Utilities`. |
| **Education** | Courses, tuition, books bought to learn. |
| **Fees** | Bank commissions. Nearly always auto-assigned by `category_rules`, but pickable by hand since migration 018 — the ADR-019 reversal cleanup needs it. Never occupies a numbered chip (`chip_eligible = 0`, migration 021). |
| **Other Expense** | Escape hatch. Should trend toward zero; a large count here is a triage backlog, not a category. |

## Income

| Category | Test |
|---|---|
| **Salary** | Employment pay. |
| **Gigs** | Freelance / one-off work. |
| **Loan Repayment** | Money coming back from a `Lending` row. |
| **Other Income** | Everything else. |
| **Interest** | Binance Earn rewards. **Auto-only** — `auto_only = 1` (migration 021); still `active = 0` from migration 011 until the Wave 2 picker cutover. Not retired: rows land here every day. |

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
| `Interest`, `Fees` | Always auto-assigned; only added picker noise. `Fees` came back in 018 and `Interest` is now expressed as `auto_only`, not retirement. | 011 |
| `Lifestyle` | Zero rows in nine months, no definition; every example routes to `Leisure` / `Personal Care` / `Purchases` / `Clothing`. | 013 |
| `Tools` | Zero rows in nine months; subset of `Purchases`. | 013 |
| `Clothing` | Owner decision 2026-08-21: apparel is just `Purchases`. The split (migration 005) never changed a decision in nine months, which is the bar this file sets. The ten rows were moved to `Purchases`. | 021 |

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
- **018** — `Fees` pickable again (reversal cleanup, ADR-019).
- **021** — `auto_only` / `chip_eligible` / `icon` added, so `active` stops
  carrying three meanings at once. `Clothing` retired into `Purchases`.
