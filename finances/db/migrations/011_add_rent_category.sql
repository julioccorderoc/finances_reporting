-- 011_add_rent_category.sql
-- Taxonomy amendment (ADR-006, 2026-07-13):
--   * Add expense category `Rent`. Julio pays his landlord monthly and no
--     housing category existed. `Rent` chosen over `Housing` — taxonomy
--     favors precise labels, and it stays distinct from `Utilities`.
--
-- Rule-006 requires new categories to land as a forward migration.

INSERT OR IGNORE INTO categories (kind, name) VALUES
    ('expense', 'Rent');
