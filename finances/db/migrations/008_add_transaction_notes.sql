-- 008_add_transaction_notes.sql
-- UX overhaul WP3 (2026-07-11): single free-text notes column per
-- transaction (docs/plans/ux-overhaul/00-design.md §4).
-- Nullable, no default. Written only through transactions_repo.update()
-- (rule-012); re-ingest preserves it via the upsert COALESCE clause in
-- finances/db/repos/transactions.py::upsert_by_source_ref.
--
-- NOTE: the design spec named this 007_add_transaction_notes.sql, but
-- 007_going_out_and_bank_fee_rules.sql already existed, so this file
-- takes the next free prefix. The runner applies each file exactly once
-- (tracked in _migrations), so ALTER TABLE here is safe.

ALTER TABLE transactions ADD COLUMN notes TEXT;
