-- 023: the tombstone a delete leaves behind (ADR-022 §2.1).
--
-- Deleting a row the ingest wrote is not enough: dedup is keyed on
-- (source, source_ref) (rule-010, ADR-010), so the next `finances update`
-- or statement drop would insert it again, silently, and the owner would
-- have to delete it forever. This table retires the pair.
--
-- `upsert_by_source_ref` skips any incoming row whose (source, source_ref)
-- is here, so the invariant "re-ingest same day = 0 new rows" survives a
-- delete. Backfill goes through the same call (rule-004), so it is honoured
-- there too.
--
-- `snapshot` holds the deleted row as JSON. Nothing is truly lost: an undo
-- is a re-insert plus a DELETE from this table. `reason` is the owner's own
-- words, optional — a delete with nothing to say is still a delete.
--
-- Rows written by the cash CLI carry no tombstone (ADR-022 §2.2): nothing
-- re-ingests them, and two legitimately identical cash entries hash to the
-- same ref, so a tombstone would block the second one.

CREATE TABLE IF NOT EXISTS deleted_transactions (
    source      TEXT NOT NULL,
    source_ref  TEXT NOT NULL,
    deleted_at  TEXT NOT NULL,           -- UTC ISO-8601
    reason      TEXT,                    -- optional, the owner's words
    snapshot    TEXT NOT NULL,           -- the row as JSON, for the record
    PRIMARY KEY (source, source_ref)
);
