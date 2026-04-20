-- 004_taxonomy_v1_1.sql
-- v1.1 taxonomy amendment (ADR-006 Amendment 2026-04-20):
--   * Rename expense `Food` -> `Groceries` (Groceries = home food; going-out
--     food now lives under `Leisure`).
--   * Add 5 new expense categories: Leisure, Personal Care, Utilities,
--     Lending, Education.
--   * Add 1 new income category: Loan Repayment.
--
-- Rule-006 requires new categories to land as a forward migration. Existing
-- rules reference categories by id, so the rename is transparent to them.

-- Rename Food -> Groceries on the existing row (preserves category_id, so
-- any rules already pointing at Food continue to match). Guarded so this
-- migration is safe to re-run on a DB where the rename already happened.
UPDATE categories
   SET name = 'Groceries'
 WHERE kind = 'expense'
   AND name = 'Food';

-- New categories (INSERT OR IGNORE so the migration is safe on partial DBs).
INSERT OR IGNORE INTO categories (kind, name) VALUES
    ('expense', 'Leisure'),
    ('expense', 'Personal Care'),
    ('expense', 'Utilities'),
    ('expense', 'Lending'),
    ('expense', 'Education'),
    ('income',  'Loan Repayment');
