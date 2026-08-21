-- 021_category_picker_metadata.sql
--
-- Picker metadata for the triage redesign (CatPicker, criteria E1-E9).
--
-- The table has only ever carried `active`, which conflates two different
-- questions: "is this category retired?" and "would a human ever choose it?".
-- Interest and the transfer/adjustment categories are not retired — rows are
-- written to them every day — they simply must never appear in a hand picker.
-- Answering both with one flag is what forced migration 011 to deactivate
-- Fees and migration 018 to undo it.
--
-- So `active` keeps its one meaning — not retired — and the new columns carry
-- the rest:
--
--     auto_only      1 = written by the system only, never offered by hand.
--     chip_eligible  0 = may be picked, but never ranks into the eight
--                    numbered chips.
--     icon           Lucide glyph name; presentation, but seeded here so the
--                    template layer has nothing to invent.
--
-- Pickable is therefore `active = 1 AND auto_only = 0`, and the chip ranking
-- adds `AND chip_eligible = 1`. One definition, in SQL, in one place.
--
-- Bare ALTER TABLE ADD COLUMN is safe: the runner keys _migrations on the
-- filename and applies each file exactly once. NOT NULL is permitted because
-- a non-null DEFAULT backfills the existing rows.

ALTER TABLE categories ADD COLUMN auto_only INTEGER NOT NULL DEFAULT 0
  CHECK (auto_only IN (0, 1));
ALTER TABLE categories ADD COLUMN chip_eligible INTEGER NOT NULL DEFAULT 1
  CHECK (chip_eligible IN (0, 1));
ALTER TABLE categories ADD COLUMN icon TEXT;

-- ── auto_only ──────────────────────────────────────────────────────────────
-- Kind-driven, not name-driven: every transfer and adjustment category is
-- system-written by definition (rule-002, EPIC-006), so a category added
-- under those kinds later inherits the rule without another migration.
UPDATE categories SET auto_only = 1 WHERE kind IN ('transfer', 'adjustment');

-- Interest is the one income category the system always assigns itself
-- (Binance Earn rewards resolve it via categories.get_by_name, which ignores
-- `active`). Migration 011 could only express that as active = 0; auto_only
-- is what it meant.
--
-- Its `active = 0` is deliberately NOT flipped back here. Under the new
-- reading Interest is not retired and `active = 1` would be the honest
-- value, but the existing viewer's pickers read categories.list_all(), which
-- filters on `active` alone — flipping it would drop Interest into every
-- old picker the same week the new one ships. The new picker excludes it on
-- auto_only, so both readings agree; the flag flip belongs with the Wave 2
-- cutover, once list_all() is no longer a picker source.
UPDATE categories SET auto_only = 1 WHERE kind = 'income' AND name = 'Interest';

-- ── chip_eligible ──────────────────────────────────────────────────────────
-- Owner decision 2026-08-21. Fees stays hand-pickable: migration 018 brought
-- it back on purpose, and the ADR-019 reversal cleanup surfaces rows only a
-- human can file there. But it is the most-used category in the ledger by a
-- factor of two and a half (371 rows in the last twelve months) because
-- category_rules assigns nearly all of them. Ranking chips by usage would
-- put a bank commission on keyboard key 1, which is the key the owner
-- presses most. It belongs in the list and the search, not on a chip.
UPDATE categories SET chip_eligible = 0 WHERE kind = 'expense' AND name = 'Fees';

-- ── Clothing retires ───────────────────────────────────────────────────────
-- Owner decision 2026-08-21: "all the clothing transactions must be purchase,
-- and yes we won't be using this any longer". Migration 005 split Clothing
-- out of Purchases; nine months later the distinction never changed a
-- decision, which is the bar category-definitions.md sets.
--
-- Move first, deactivate second, so no row is left pointing at a hidden
-- category. `needs_review` is deliberately untouched: these rows are being
-- re-filed, not un-categorized, and must not re-enter the triage queue.
-- Re-running is a no-op — after the first pass nothing points at Clothing.
UPDATE transactions
   SET category_id = (SELECT id FROM categories WHERE kind = 'expense' AND name = 'Purchases'),
       updated_at  = CURRENT_TIMESTAMP
 WHERE category_id = (SELECT id FROM categories WHERE kind = 'expense' AND name = 'Clothing');

-- Rules could point at it too (none do today); redirect any that appear.
UPDATE category_rules
   SET category_id = (SELECT id FROM categories WHERE kind = 'expense' AND name = 'Purchases')
 WHERE category_id = (SELECT id FROM categories WHERE kind = 'expense' AND name = 'Clothing');

-- Deactivated, never DELETEd (criterion E9): the id stays valid and reviving
-- it is a flag flip.
UPDATE categories SET active = 0 WHERE kind = 'expense' AND name = 'Clothing';

-- ── icons ──────────────────────────────────────────────────────────────────
-- Lucide names. Taken from the design kit
-- (design_handoff_triage/design/ui_kits/finances/triage-data.js) wherever the
-- category exists there; the five it never modelled — the two real transfer
-- categories, FX Diff, and the two retired ones — are chosen here. Retired
-- categories get one too, so reviving one is still just a flag flip.
UPDATE categories SET icon = CASE name
    WHEN 'Groceries'        THEN 'shopping-basket'
    WHEN 'Going Out'        THEN 'utensils'
    WHEN 'Transport'        THEN 'car'
    WHEN 'Utilities'        THEN 'zap'
    WHEN 'Personal Care'    THEN 'scissors'
    WHEN 'Purchases'        THEN 'package'
    WHEN 'Leisure'          THEN 'ticket'
    WHEN 'Subscriptions'    THEN 'repeat'
    WHEN 'Health'           THEN 'heart-pulse'
    WHEN 'Family'           THEN 'users'
    WHEN 'Lending'          THEN 'hand-coins'
    WHEN 'Dating'           THEN 'heart'
    WHEN 'Clothing'         THEN 'shirt'
    WHEN 'Rent'             THEN 'house'
    WHEN 'Gifts'            THEN 'gift'
    WHEN 'Education'        THEN 'graduation-cap'
    WHEN 'Other Expense'    THEN 'circle-dashed'
    WHEN 'Fees'             THEN 'receipt'
    WHEN 'Lifestyle'        THEN 'sparkles'
    WHEN 'Tools'            THEN 'wrench'
    WHEN 'Salary'           THEN 'briefcase'
    WHEN 'Gigs'             THEN 'laptop'
    WHEN 'Loan Repayment'   THEN 'rotate-ccw'
    WHEN 'Other Income'     THEN 'arrow-down-left'
    WHEN 'Interest'         THEN 'percent'
    WHEN 'Internal Transfer' THEN 'arrow-left-right'
    WHEN 'External Transfer' THEN 'arrow-up-right'
    WHEN 'FX Diff'          THEN 'coins'
    WHEN 'Reconciliation'   THEN 'scale'
    ELSE icon
END;

-- A category the CASE above does not name (one added between this migration
-- being written and applied) still needs a glyph rather than a blank square.
UPDATE categories SET icon = 'circle-dashed' WHERE icon IS NULL OR icon = '';
