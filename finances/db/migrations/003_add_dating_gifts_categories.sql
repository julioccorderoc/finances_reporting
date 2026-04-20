-- 003_add_dating_gifts_categories.sql
-- Forward migration to add `Dating` and `Gifts` as v1 expense categories on
-- existing databases where migration 002 has already been applied. Per
-- rule-006, new categories must land as a forward migration; editing 002 in
-- place only affects fresh DBs.

INSERT OR IGNORE INTO categories (kind, name) VALUES
    ('expense', 'Dating'),
    ('expense', 'Gifts');
