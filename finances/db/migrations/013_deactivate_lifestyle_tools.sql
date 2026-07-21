-- 013_deactivate_lifestyle_tools.sql
-- Taxonomy amendment (ADR-006, owner decision 2026-07-21).
--
-- `Lifestyle` and `Tools` were seeded by migration 002 and never used:
-- zero transactions in nine months, and no definition anywhere in ADR-006
-- beyond the name. Every concrete example routes somewhere sharper:
--
--     tour / event / cinema / hobby  -> Leisure (non-food recreation)
--     gym / barber / skincare        -> Personal Care
--     gadget / furniture             -> Purchases
--     apparel                        -> Clothing (migration 005 already
--                                       rejected Lifestyle for this)
--
-- A bucket that cannot take an example it does not share with a better
-- bucket is not a bucket. Removing them from the picker also shortens
-- triage, which is the point of the ADR-006 chain.
--
-- Deactivate, never DELETE (same reasoning as migration 011): the rows
-- keep their ids, so reviving either is an `active = 1` flip, DELETE
-- would cascade `category_rules`, and `get_by_name()` — which ignores
-- `active` — keeps resolving for any auto-assignment path. Only
-- `list_all()` (picker + top-chips source) hides them.
--
-- Idempotent: UPDATE to the same value is a no-op on re-run.

UPDATE categories SET active = 0 WHERE kind = 'expense' AND name = 'Lifestyle';
UPDATE categories SET active = 0 WHERE kind = 'expense' AND name = 'Tools';
