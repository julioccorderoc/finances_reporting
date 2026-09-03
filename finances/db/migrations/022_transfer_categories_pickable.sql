-- 022_transfer_categories_pickable.sql
--
-- The two transfer categories may be picked by hand again.
--
-- Migration 021 marked every transfer-kind category auto_only on the
-- reasoning that a transfer is confirmed as a PAIR (rule-002), never
-- declared by tagging one leg. The ledger has rows the pairing can never
-- reach: money that enters or leaves transitionally — a deposit forwarded
-- on, money held for someone, a withdrawal to cash whose other leg is not
-- in the ledger. Those rows are neither income nor spending. The write
-- path had always accepted the tag: transactions_write.category_fits is
-- asymmetric on purpose (a transfer-kind category fits any income or
-- expense row), and domain/money.py's SQL_NOT_CURRENCY_MOVEMENT excludes
-- such rows from every spending and income figure. The picker was the
-- only surface refusing what everything under it accepts.
--
-- Owner decision 2026-09-03: both come back, from the LIST only.
-- chip_eligible = 0 keeps them off the eight numbered keys, where usage
-- would otherwise rank Internal Transfer (written by every confirmed
-- pair) onto a shortcut nobody presses on purpose. Adjustment categories
-- and Interest stay system-written; a properly paired transfer still
-- needs no tag; the row's kind is untouched — it is the audit trail.
--
-- Re-running is a no-op.

UPDATE categories
   SET auto_only = 0,
       chip_eligible = 0
 WHERE kind = 'transfer'
   AND name IN ('Internal Transfer', 'External Transfer');
