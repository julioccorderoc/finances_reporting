-- 011: hide Interest (income) and Fees (expense) from the manual pickers.
--
-- Owner decision 2026-07-13: both are almost always auto-assigned —
-- Binance earn rewards resolve Interest via categories.get_by_name()
-- (which ignores ``active``), and bank commission markers resolve Fees
-- via category_rules rows (which store category_id directly). Keeping
-- them in the picker only adds noise to triage.
--
-- Deactivate, never DELETE: hundreds of historical transactions
-- reference these ids, and both auto-assignment paths keep working
-- because neither filters on ``active``. Only list_all() (the picker /
-- top-chips source) hides them.
--
-- Idempotent: UPDATE to the same value is a no-op on re-run.

UPDATE categories SET active = 0 WHERE kind = 'income'  AND name = 'Interest';
UPDATE categories SET active = 0 WHERE kind = 'expense' AND name = 'Fees';
