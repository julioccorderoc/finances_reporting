-- 015_add_transaction_parked.sql
--
-- Adds transactions.parked — durable triage deferral (ADR-012 Amendment
-- 2026-07-21, spec 2026-07-21-triage-speedrun-design §5.3).
--
-- Replaces the per-process in-memory skip set, which was destroyed on every
-- server stop — including the always-visible Stop-server button that is the
-- designed way to end a session.
--
-- Follows 008_add_transaction_notes.sql: a single ALTER TABLE. The runner
-- applies each file exactly once (keyed on full filename in _migrations), so
-- a bare ADD COLUMN is safe. NOT NULL is permitted here because a non-null
-- DEFAULT is supplied; existing rows backfill to 0.
--
-- Deliberately NOT added to upsert_by_source_ref's ON CONFLICT DO UPDATE SET
-- list: a column absent from that list is left untouched on re-ingest, which
-- is exactly the survival guarantee Park promises.

ALTER TABLE transactions ADD COLUMN parked INTEGER NOT NULL DEFAULT 0
  CHECK (parked IN (0, 1));
