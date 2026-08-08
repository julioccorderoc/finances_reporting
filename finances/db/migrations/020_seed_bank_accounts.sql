-- Migration 020: Bancamiga and Banco de Venezuela join the ledger.
--
-- Accounts have never been seeded by a migration: the five that exist were
-- created by the backfill, because each one carried legacy CSV history. These
-- two carry none — they hold zero today and are being opened so future
-- statements and reconciliations have somewhere to land. There is no CSV to
-- backfill them from, so the seed belongs here.
--
-- INSERT OR IGNORE, not INSERT: `name` is UNIQUE, and a live DB may already
-- have these rows from a hand insert. Re-running must never rewrite an
-- account the owner has since edited.

INSERT OR IGNORE INTO accounts (name, kind, currency, institution, active)
VALUES
    ('Bancamiga Bolivares', 'bank', 'VES', 'Bancamiga', 1),
    ('Venezuela Bolivares', 'bank', 'VES', 'Banco de Venezuela', 1);
