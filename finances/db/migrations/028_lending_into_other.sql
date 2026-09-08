-- 028_lending_into_other.sql
--
-- Lending is folded into the chart's Other, by data (ADR-026 §2.6 as
-- amended 2026-09-08).
--
-- The owner's request was "lending can go into others". Migration 027
-- honoured it by leaving Lending ungrouped, expecting it to miss the top
-- five on its own. It did not: the /monthly chart ranks series by their
-- total over the whole window, and a single large loan kept Lending in
-- the top five for six months while Health — bigger in the month being
-- read — fell into Other.
--
-- `group_name = 'Other'` is the instruction to fold a category into the
-- computed remainder whatever its rank. It is NOT a group: it never
-- competes for a colour slot, never holds a palette rank, and the
-- remainder is still computed for everything else that misses the cap.
-- Both land in the one Other series, flat, largest first.
--
-- Only Lending carries it. Fees, Other Expense, Education and Subscriptions
-- still fall into Other on their own and nothing is written down for them.
-- `finances doctor` knows Lending as a grouped category from here on.
--
-- Re-running is a no-op.

UPDATE categories
   SET group_name = 'Other'
 WHERE kind = 'expense'
   AND name = 'Lending';
