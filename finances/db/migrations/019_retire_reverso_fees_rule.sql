-- 019: the REVERSO CARGO → Fees rule retires (ADR-019).
--
-- A reversal is the bank returning a rejected payment (or its
-- commission). Rule 27 stamped these income rows "Fees", which polluted
-- the one category that should only ever contain charges. Under ADR-019
-- a reversal pairs with the failed charge it undoes (BankReversalPairing)
-- and needs no category; one that fails to pair belongs in triage where
-- a human can see it — not silently filed as fee income.
--
-- Deactivate, never DELETE: historical audit, same as migrations 006/011/013.
--
-- Idempotent: UPDATE to the same value is a no-op on re-run.

UPDATE category_rules SET active = 0 WHERE pattern = 'REVERSO CARGO';
