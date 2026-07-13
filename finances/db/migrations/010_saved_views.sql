-- 010_saved_views.sql
-- Wave 2 Thing 2 (2026-07-13): saved /transactions filter views
-- (docs/plans/wave2/02-saved-views.md). Stores the raw querystring
-- WITHOUT the leading '?' so chips can rebuild the exact URL. Lives in
-- the DB (not localStorage) so a view saved on the Mac recalls on the
-- iPhone over LAN — SQLite = truth (rule-001).
--
-- NOTE: the plan expected prefix 009, but the concurrently-built Thing 3
-- (edit history) claimed 009_add_transaction_edits.sql first, so this
-- file takes the next free prefix per docs/plans/wave2/README.md.

CREATE TABLE IF NOT EXISTS saved_views (
    id            INTEGER PRIMARY KEY,
    name          TEXT NOT NULL UNIQUE,
    query_string  TEXT NOT NULL,
    created_at    TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
