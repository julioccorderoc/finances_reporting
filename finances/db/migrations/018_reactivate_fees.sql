-- 018: Fees returns to the manual pickers.
--
-- Migration 011 hid Fees because bank commissions were auto-assigned by
-- category_rules and the picker entry was pure noise. Owner decision
-- 2026-08-05: bring it back — hand-triage wants to file odd bank charges
-- under Fees, and the reversal cleanup (ADR-019) surfaces rows a human
-- must be able to categorize as Fees explicitly.
--
-- Interest (income) stays hidden: still auto-assigned only.
--
-- Idempotent: UPDATE to the same value is a no-op on re-run.

UPDATE categories SET active = 1 WHERE kind = 'expense' AND name = 'Fees';
