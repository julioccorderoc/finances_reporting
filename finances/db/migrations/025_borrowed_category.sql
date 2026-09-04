-- 025_borrowed_category.sql
--
-- `Borrowed` — money someone lends the owner, and what he pays back.
--
-- The ledger had `Lending` (expense) and `Loan Repayment` (income), both
-- about money he lends OUT. Money lent TO him had nowhere to go: filed as
-- income it inflated his income, and the repayment filed as an expense
-- inflated his burn. Neither happened. Rows 1871/1869 are the clean case --
-- Hugo lent 6,000 Bs and the same day 6,000 Bs went back -- and June was
-- reporting $7.94 of income and $7.94 of spending against it.
--
-- Owner decision 2026-09-04 (docs/plans/2026-09-03-borrowed-money-findings.md,
-- Option 1): kind='transfer'. `finances.domain.money.SQL_NOT_CURRENCY_MOVEMENT`
-- then drops such rows from every income and spending figure while the
-- balance still moves, which is right -- the money really did arrive and
-- leave. What he still owes is what came in minus what has gone back.
--
-- Why not `External Transfer`: that is money passing through FOR SOMEONE
-- ELSE. Collapsing the two would lose the distinction between "I owe this"
-- and "I forwarded this", which is the only thing the new category is for.
--
-- Why not an income/expense pair (`Loan Received` / `Loan Payback`): income-
-- and expense-kind categories count in every report, which is the error
-- being fixed.
--
-- Flags follow migration 022's shape for the transfer categories:
-- pickable from the LIST only (`auto_only = 0`), never on a numbered chip
-- (`chip_eligible = 0`) -- a loan is recognised by who sent the money, so
-- usage would never earn it a shortcut. No `category_rules` row for the
-- same reason: nothing in a bank string says "this is a loan", the sender's
-- account number is the only signal, and it is a person, not a merchant.
-- It stays a triage decision (rule-006).
--
-- The mirror image is deliberately untouched: `Lending` stays expense-kind
-- and `Loan Repayment` income-kind (same owner decision). Changing what an
-- existing category kind MEANS would get an ADR first.
--
-- `hand-coins` is already vendored in templates/_icons.html; an unknown
-- icon name renders nothing.
--
-- Re-running is a no-op.

INSERT OR IGNORE INTO categories (kind, name) VALUES
    ('transfer', 'Borrowed');

UPDATE categories
   SET active = 1,
       auto_only = 0,
       chip_eligible = 0,
       icon = 'hand-coins'
 WHERE kind = 'transfer'
   AND name = 'Borrowed';
