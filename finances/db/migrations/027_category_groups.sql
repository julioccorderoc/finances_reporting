-- 027_category_groups.sql
--
-- Categories roll up into groups (ADR-023).
--
-- The /monthly chart draws the top five categories and folds the rest into
-- "Other". On the owner's ledger Other was the largest block in four of six
-- months — 45.8% of July 2026 — because the fixed household costs are split
-- across four categories, none individually large enough to reach the top
-- five. The cap cannot be raised: it equals the number of validated palette
-- hues. Grouping is the fix.
--
-- `group_name` is a label ON the category, not a table of its own: no group
-- id, no second foreign key, and a transaction still carries exactly one
-- category_id. NULL means "this category stands for itself" — the majority
-- case for income and transfer categories, and not an error state. Nothing
-- about ingest, categorization (ADR-006), triage or the rate chain has to
-- know groups exist; only the chart reads the column.
--
-- The mapping below is the owner's (confirmed 2026-09-07):
--
--     Home     Rent, Utilities, Transport, Personal Care
--     Social   Dating, Going Out, Leisure, Family, Gifts
--     (none)   Purchases, Groceries, Health — each stands alone, on purpose
--     (none)   Lending, Fees, Other Expense, Education, Subscriptions
--
-- "Lending can go into others" is honoured by leaving it ungrouped, not by
-- storing an `Other` group: Other stays what the chart computes when more
-- things are in play than there are slots (ADR-023 §2.6).
--
-- The names `Home` and `Social` are data, not code (§2.7). This seed is the
-- only place that spells them; the owner may rename them with an UPDATE and
-- nothing downstream needs a deploy. `finances doctor` checks that a
-- non-NULL group sits on a category this seed knows, which is how a later
-- rename of a *category* gets caught.
--
-- Bare ALTER TABLE ADD COLUMN is safe: the runner keys _migrations on the
-- filename and applies each file exactly once. The UPDATEs are idempotent
-- on their own — replayed by hand they set what is already set.

ALTER TABLE categories ADD COLUMN group_name TEXT NULL;

UPDATE categories
   SET group_name = 'Home'
 WHERE kind = 'expense'
   AND name IN ('Rent', 'Utilities', 'Transport', 'Personal Care');

UPDATE categories
   SET group_name = 'Social'
 WHERE kind = 'expense'
   AND name IN ('Dating', 'Going Out', 'Leisure', 'Family', 'Gifts');
