-- 005_add_clothing_category.sql
-- v1.2 taxonomy amendment (ADR-006 Amendment 2026-04-20):
--   * Add expense category `Clothing`. Julio's legacy ledger labels apparel
--     purchases with Sub-Category=`Clothing` (10 rows in the backfill).
--     Mapping to `Purchases` or `Lifestyle` was rejected — Clothing keeps
--     its own category.
--
-- Rule-006 requires new categories to land as a forward migration.

INSERT OR IGNORE INTO categories (kind, name) VALUES
    ('expense', 'Clothing');
