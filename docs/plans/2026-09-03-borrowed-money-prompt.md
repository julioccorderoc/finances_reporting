# Prompt — where does borrowed money live?

Paste everything below the line into a fresh session, run from the repo root.
Owner decision 2026-09-03: this is a taxonomy question, so it gets its own
sitting rather than a quick migration inside the viewer reskin.

---

## Your job

Decide — with me, not for me — how the ledger records **money someone lends
me** and **the repayments I make later**, then implement whatever we decide
as a migration + doc + tests, the way every category change in this repo has
landed.

Read `CLAUDE.md` first, then `docs/architecture/category-definitions.md`
(authoritative for what every category means), then
`finances/domain/money.py` (the "is this spending?" rule), then
`finances/web/services/transactions_write.py::category_fits` (what a save
accepts), and `design_handoff_triage/NOTES.md` § "2026-09-03 — the transfer
categories are offered again".

## Who you are talking to

I am the sole owner. I am not a software engineer. Explain each option from
first principles and in terms of what my reports will say, then recommend
one. Do not change the taxonomy without my explicit yes — every category
mapping in this project has been confirmed by me before it was applied.

## The state of play

- The ledger has `Lending` (expense: *you paid and expect it back*) and
  `Loan Repayment` (income: *money coming back from a Lending row*). Both are
  about money **I** lend out.
- Money lent **to me** has no category. Filed as income it inflates my
  income; the repayment filed as an expense inflates my burn. Neither is true.
- The machinery for "this money moved, it was not earned or spent" already
  exists and is wired end to end: a **transfer-kind** category on an income
  or expense row is excluded from every spending and income figure by
  `finances.domain.money.SQL_NOT_CURRENCY_MOVEMENT`; `category_fits` accepts
  it on any income/expense row; migration 022 made the two transfer
  categories (`Internal Transfer`, `External Transfer`) pickable from the
  triage picker under **Moved, not spent**, never on a numbered chip.
- The row's `kind` (income/expense) is the audit trail and is never changed
  by a category.

## The options on the table

1. **A new transfer-kind category, e.g. `Borrowed`.** The loan coming in and
   my repayments going out both land there, so neither counts as income or
   spending; outstanding debt is what came in minus what went back. Keeps the
   distinction from "money passing through for someone" (`External
   Transfer`). Costs one migration, one doc table row with a disambiguating
   test, picker tests, and the definitions-loader coverage test.
2. **File both legs under `External Transfer`.** No new category. Also not
   income/spending, but the ledger can no longer tell "I owe this" from "I
   forwarded this".
3. **Income `Loan Received` + expense `Loan Payback`.** Do not recommend:
   income- and expense-kind categories count as income and spending in
   every report, which is the exact error we are trying to avoid.

Also worth deciding while here: should `Lending` / `Loan Repayment`
(the mirror image) stay expense/income, as the legacy taxonomy had them,
or become transfer-kind too? Today lending out **does** count as spending
and its return as income. That may be what I want (it is money gone until it
comes back) — ask, do not assume.

## Before proposing anything

- Count the real cases. Search the ledger (read-only, `?immutable=1`) for
  rows that look like borrowed money: income rows from a person rather than
  an employer, `PAGO MOVIL` credits with a person's name, deposits the P2P
  pairing never claimed, and their later mirror-image debits. Show me the
  candidates with dates and amounts so the decision is about my actual rows,
  not a hypothetical.
- Say how each option changes this month's spend, this month's income and
  the net-worth headline on those candidates, in dollars.

## If we add a category

- Follow migration 021/022: a new `0NN_*.sql` that INSERTs the category with
  `kind = 'transfer'`, `auto_only = 0`, `chip_eligible = 0`, an `icon` that
  `templates/_icons.html` actually vendors (`hand-coins` is a candidate), and
  a test file `tests/test_migration_0NN_*.py`.
- Add the row and its disambiguating test to the `## Transfer` table in
  `docs/architecture/category-definitions.md` (the picker reads that table;
  `tests/test_category_definitions_loader.py` fails by name if it is
  missing), and a `## History` line.
- Extend `tests/web/test_triage_transfer_pick.py` so the new category shows
  in the **Moved, not spent** group and never on a chip.
- Consider a `category_rules` row only if a bank string reliably identifies
  the lender; otherwise leave it to triage.
- TDD per rule-011: tests first, `uv run pytest -q` green, never open the
  live `finances.db` from the suite.

## Deliverable

A short written recommendation with the candidate rows and the dollar
impact, my yes/no, and then the migration + doc + tests as a TDD pair of
commits. If the decision turns out to need an ADR (it changes what a
category *kind* means), draft it and stop for my confirmation first.
