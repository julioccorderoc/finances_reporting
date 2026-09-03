# Borrowed money — findings and a recommendation (2026-09-03)

Answers `docs/plans/2026-09-03-borrowed-money-prompt.md`. Read-only: the
ledger was opened with `?immutable=1`; nothing was written, no category
was changed. **Your yes/no is the next step** — the migration, doc row and
tests are written only after it.

## 1. What the ledger says today, in plain words

Every row is *income* or *expense* (what the bank said), and every
category has a kind. Income-kind categories count towards your monthly
income; expense-kind ones towards your spending. A **transfer-kind**
category (`Internal Transfer`, `External Transfer`) is you saying "this
money moved, it was not earned or spent" — such rows drop out of both
figures but still move the account balance, which is right, because the
money really did land in or leave the account.

Net worth is the sum of account balances. **No category changes it.** So
every option below changes only what the monthly income and spending
figures say; the net-worth headline is untouched by all of them.

## 2. The rows this decision is about

Dollar figures are what the resolver prices each row at (rule-005).

### 2.1 The one row that is unambiguously borrowed money

| id | date | description | amount | USD | filed as | your note |
|---|---|---|---|---|---|---|
| 1871 | 2026-06-11 | CR.I/REC 0105 V014591892 | +6,000 VES | +$7.94 | Loan Repayment (income) | *prestamo de hugo hacia mi* |
| 1869 | 2026-06-11 | DR OB V14591892 105MERCA | −6,000 VES | −$7.94 | Lending (expense) | *pago deuda hugo* |

Hugo lent you 6,000 Bs. and the same day 6,000 Bs. went back to him. As
filed, June shows +$7.94 income and −$7.94 spending that never happened.
Small in dollars; exactly the error the prompt describes.

### 2.2 Deposits nobody has claimed yet (candidates — only you know)

Thirteen bank credits since May have no category and no P2P pair. They
are the rows a `Borrowed` category would most likely be applied to — or
`Loan Repayment`, or `Gigs`, or `External Transfer`; the sender's account
number is the only clue, and your notes on older rows suggest who is who:

| sender (from the reference) | seen before as | uncategorised rows | USD |
|---|---|---|---|
| V033404180 | *pago cuota natalia* (Loan Repayment, 74,636 Bs. on 06-15) | 1782, 1715, 1964, 7669 | $256.26 |
| V014648189 | *deuda yaribel*, *Yaribel Cashea Lavadora* | 7188, 7717 | $56.44 |
| 04165936089 | Gigs (818) and External Transfer (7271) | 7573, 7564 | $61.97 |
| V027142544 | *pago moises de la playa* (Loan Repayment) | 1875 | $9.39 |
| others (TRAV…, 04244347368, 04145360396) | — | 6940, 1731, 1906, 7718 | $72.43 |

Total **$456.48**: May $181.39, June $9.39, July $44.70, August $221.00.
Today none of it counts anywhere (uncategorised rows sit in Triage). If
they are repayments they *add* to income when sorted; if any of them is
a loan *to* you, `Borrowed` is what keeps it out.

### 2.3 The mirror image — money you lend out — as it stands

- **Lending** (expense-kind): 23 rows, **−$917.46** in total; the big ones
  are the $375 phone loan to Naty (05-30), the $142.50 + $30 + $4 USDT
  sends in March, the 27,000 Bs. on 03-18, and *prestamo $60 yaribel*
  (07-20). May's spending figure carries **$382.44** of it.
- **Loan Repayment** (income-kind): 17 rows, **+$722.76**; March's income
  carries **$298.17** (a $256.50 USDC deposit), June **$102.24**.
- Outstanding by the ledger's own arithmetic: $917 lent − $723 back ≈
  **$195 still out**, though 1871 above is filed on the wrong side and a
  few `Lending` rows (JINGFENG WU, MULTIMAX, MODAVENCA) look like purchases
  you fronted, not cash loans.

### 2.4 A related pattern worth one ruling: *cuotas* and Cashea

Forty-odd rows carry *cuota* / *cashea* in their notes. Most are your own
instalment purchases (Purchases, Groceries, Gifts) and are correctly
spending. The **washing machine you pay for Yaribel** is not: its
instalments are filed as `Family` (1910, 7258), `External Transfer`
(7344), and her reimbursements as `External Transfer` (1629, 7348) or
`Loan Repayment` (7382). Whatever you decide below, that thread wants one
answer (it is lending: you paid, you expect it back).

## 3. The options, in dollars, on the rows above

| | Option 1 — new transfer-kind **Borrowed** | Option 2 — file both legs as **External Transfer** | Option 3 — income *Loan Received* + expense *Loan Payback* |
|---|---|---|---|
| June income | −$7.94 (1871 stops counting) | −$7.94 | unchanged (still counted) |
| June spend | +$7.94 less spend (1869 stops counting) | +$7.94 | unchanged |
| August income, if the $221 of unclaimed deposits turn out to be loans to you | −$221.00 | −$221.00 | unchanged — the error, at scale |
| Net worth | unchanged | unchanged | unchanged |
| Can the ledger still tell "I owe this" from "I forwarded this for someone"? | **yes** | no | yes, but both inflate the reports |
| Cost | one migration, one doc row, picker tests | nothing | one migration; wrong by construction |

## 4. Recommendation

**Option 1.** A `Borrowed` category of kind `transfer`, pickable from the
picker's *Moved, not spent* group, never on a numbered chip — the shape
migration 022 already gave the two transfer categories. Its one-line
test for `category-definitions.md`:

> **Borrowed** — money someone lent *you*, and what you later pay them
> back. Neither income (you owe it) nor spending (you are returning it).
> What you still owe is what came in minus what went back. Money *you*
> lend out is `Lending` / `Loan Repayment`, not this.

No `category_rules` row: nothing in a bank string says "this is a loan";
the sender's account number is the only signal and it is a person, not a
merchant. It stays a triage decision.

**On the mirror image** (`Lending` / `Loan Repayment`): I recommend
leaving them expense/income for now, as the legacy taxonomy had them and
as you asked me not to assume. Two honest consequences of that choice:
money you lend *does* show as spending the month it leaves (May: $382 of
it), and its return shows as income (March: $298). If you would rather
lending not count as spending at all — "it is money out until it comes
back, but I have not spent it" — say so and both become transfer-kind in
the same migration; the `$195 still out` figure then becomes something
the Accounts page could carry as a line. That is a taxonomy change to
what an existing category *kind* means, so it would get an ADR first.

## 5. What happens after your yes

1. `023_borrowed_category.sql` inserts `Borrowed` (`kind='transfer'`,
   `auto_only=0`, `chip_eligible=0`, `icon='hand-coins'` — to be vendored
   into `_icons.html` with its test), plus `tests/test_migration_023_*.py`.
2. The doc row and test above go into the `## Transfer` table of
   `docs/architecture/category-definitions.md`, plus a `## History` line.
3. `tests/web/test_triage_transfer_pick.py` grows a case: `Borrowed` in
   *Moved, not spent*, never on a chip.
4. Then, with you in Triage: 1871 and 1869 → `Borrowed`; the thirteen
   unclaimed deposits sorted one by one; the Yaribel washing-machine
   thread given its one answer.

Answer needed: **Option 1 — yes or no?** And: **should `Lending` /
`Loan Repayment` stop counting as spending/income (become transfer-kind),
or stay as they are?**
