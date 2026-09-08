-- 026: what the exchange says it holds (ADR-023).
--
-- Every integrity check the ledger owns reasons about the ledger's own rows.
-- That is enough to catch a lost leg or a broken pair, and structurally
-- unable to catch a booking sent to the wrong wallet: the rows stay
-- internally consistent, double-entry still balances, and the total across
-- the Binance accounts stays nearly right. Only a figure from outside can
-- see it. `earn_positions` (ADR-003) is that figure for Earn and proved Earn
-- correct; Spot and Funding had nothing, and were out by two thousand
-- dollars each, in opposite directions, for about a year.
--
-- This is deliberately not a column on `accounts`. A balance is a claim
-- about a *moment*, and the check compares it to the ledger as of that
-- moment — which is what stops a snapshot from a week ago reading as a
-- defect the day after any sync. Keeping the history also answers "when did
-- this start?" for the next occurrence, which nothing could answer for this
-- one.
--
-- Not `earn_positions` either: its columns are an Earn product's shape
-- (product_id, apy, started_at) and a wallet balance has none of them.
--
-- `balance` is TEXT, like every other money column here — the number the
-- ledger is measured against must not pass through a float.
--
-- One row per (account, currency, captured_at): two syncs in the same second
-- must not leave the check a choice of two answers for one moment, so the
-- repo upserts on that key.

CREATE TABLE IF NOT EXISTS exchange_balances (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    account_id  INTEGER NOT NULL REFERENCES accounts(id) ON DELETE RESTRICT,
    currency    TEXT    NOT NULL,
    balance     TEXT    NOT NULL,   -- decimal string, as the exchange gave it
    captured_at TEXT    NOT NULL,   -- ISO-8601, offset-aware
    source      TEXT    NOT NULL DEFAULT 'binance',
    UNIQUE (account_id, currency, captured_at)
);

-- The check reads the newest capture per position and nothing else.
CREATE INDEX IF NOT EXISTS idx_exchange_balances_latest
    ON exchange_balances(account_id, currency, captured_at DESC);
