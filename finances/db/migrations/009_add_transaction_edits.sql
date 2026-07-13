-- 009_add_transaction_edits.sql
-- Wave 2 Thing 3 (2026-07-13): audit trail for manual transaction edits
-- (docs/plans/wave2/03-edit-history.md).
--
-- One row per field that actually CHANGED, written ONLY from inside
-- transactions_repo.update() (rule-012, the single sanctioned write path),
-- so the viewer modal, triage, PATCH API and bulk-edit are all covered with
-- no endpoint changes. Ingest/backfill (insert / upsert_by_source_ref)
-- record NOTHING — history is for manual edits only.
--
-- ``needs_review`` is deliberately excluded from the recordable set: it is
-- resolver-derived (ADR-005), not a manual field, and would be pure noise.
-- edited_at defaults to CURRENT_TIMESTAMP (UTC-naive text); the TIMESTAMP
-- converter in finances/db/connection.py reads it back as an aware UTC
-- datetime. ON DELETE CASCADE keeps history from outliving its transaction.

CREATE TABLE transaction_edits (
    id             INTEGER PRIMARY KEY,
    transaction_id INTEGER   NOT NULL REFERENCES transactions(id) ON DELETE CASCADE,
    edited_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    field          TEXT      NOT NULL CHECK (field IN ('category_id', 'user_rate', 'notes')),
    old_value      TEXT,
    new_value      TEXT
);

CREATE INDEX idx_transaction_edits_transaction_id
    ON transaction_edits (transaction_id);
