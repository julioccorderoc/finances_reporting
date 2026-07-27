-- 016_clear_review_flag_on_paired_transfers.sql
--
-- Retires needs_review on transfers that are already fully paired.
--
-- needs_review records one thing: the importer could not work out a
-- category, so a human should look. Pairing answers that question --
-- transfers are not categorised at all (rule-006), and
-- finances/web/services/triage.py drops kind='transfer' from the review
-- surface outright.
--
-- The flag is a stored column, though, and _promote_to_transfer wrote only
-- kind/transfer_id/updated_at until this migration's companion fix. So a
-- Provincial row imported without a category (provincial.py sets
-- needs_review = category_id IS NULL) kept the flag forever once paired.
-- Triage called those rows resolved; the six surfaces that read the column
-- directly -- dashboard count, transactions filter, report needs-review,
-- Sheets mirror, html export, report update -- kept queueing them.
--
-- 15 rows in the production ledger, all Provincial legs dated 2026-04-22
-- onward, i.e. the window where ingest paired automatically and nobody
-- hand-cleared the queue.
--
-- Scoped deliberately narrowly:
--
--   * transfer_id IS NOT NULL -- a kind='transfer' row without one is an
--     orphan half-leg (rule-002 forbids it), genuinely broken, and must
--     stay visible rather than be silently cleared.
--   * category_id is untouched. Lowering a marker is not the same as
--     erasing a category the owner set by hand.
--   * unpaired rows are untouched. An expense still awaiting a category
--     is real work and stays in the queue.
--
-- Idempotent: the runner applies each file once (keyed on filename in
-- _migrations), and re-running the statement is a no-op anyway.

UPDATE transactions
   SET needs_review = 0,
       updated_at = CURRENT_TIMESTAMP
 WHERE kind = 'transfer'
   AND transfer_id IS NOT NULL
   AND needs_review = 1;
