-- 001_initial.sql
-- Initial schema per PRD §5 and ADR-001/002/003/009/010.
-- All monetary amounts are stored as TEXT to preserve Decimal precision end-to-end.
-- All timestamps are ISO-8601 strings with timezone offset (UTC or Caracas); naive timestamps are rejected at the Pydantic boundary.

PRAGMA foreign_keys = ON;

--------------------------------------------------------------------------------
-- accounts
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS accounts (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    name          TEXT NOT NULL UNIQUE,
    kind          TEXT NOT NULL,
    currency      TEXT NOT NULL,
    institution   TEXT,
    active        INTEGER NOT NULL DEFAULT 1 CHECK (active IN (0, 1)),
    created_at    TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at    TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CHECK (kind IN ('bank', 'crypto_spot', 'crypto_funding', 'crypto_earn', 'cash', 'other'))
);

CREATE INDEX IF NOT EXISTS idx_accounts_active ON accounts(active);

--------------------------------------------------------------------------------
-- categories
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS categories (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    kind          TEXT NOT NULL,
    name          TEXT NOT NULL,
    active        INTEGER NOT NULL DEFAULT 1 CHECK (active IN (0, 1)),
    created_at    TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CHECK (kind IN ('income', 'expense', 'transfer', 'adjustment')),
    UNIQUE (kind, name)
);

CREATE INDEX IF NOT EXISTS idx_categories_kind ON categories(kind);

--------------------------------------------------------------------------------
-- category_rules
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS category_rules (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    pattern        TEXT NOT NULL,
    category_id    INTEGER NOT NULL REFERENCES categories(id) ON DELETE CASCADE,
    source         TEXT,
    account_id     INTEGER REFERENCES accounts(id) ON DELETE CASCADE,
    priority       INTEGER NOT NULL DEFAULT 100,
    active         INTEGER NOT NULL DEFAULT 1 CHECK (active IN (0, 1)),
    created_at     TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_category_rules_priority ON category_rules(priority, active);
CREATE INDEX IF NOT EXISTS idx_category_rules_scope ON category_rules(source, account_id);

--------------------------------------------------------------------------------
-- transactions
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS transactions (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    account_id      INTEGER NOT NULL REFERENCES accounts(id) ON DELETE RESTRICT,
    occurred_at     TIMESTAMP NOT NULL,
    kind            TEXT NOT NULL,
    amount          DECIMAL NOT NULL,
    currency        TEXT NOT NULL,
    description     TEXT,
    category_id     INTEGER REFERENCES categories(id) ON DELETE SET NULL,
    transfer_id     TEXT,
    user_rate       DECIMAL,
    source          TEXT NOT NULL,
    source_ref      TEXT,
    needs_review    INTEGER NOT NULL DEFAULT 0 CHECK (needs_review IN (0, 1)),
    created_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CHECK (kind IN ('income', 'expense', 'transfer', 'adjustment')),
    UNIQUE (source, source_ref)
);

CREATE INDEX IF NOT EXISTS idx_transactions_account ON transactions(account_id);
CREATE INDEX IF NOT EXISTS idx_transactions_occurred_at ON transactions(occurred_at);
CREATE INDEX IF NOT EXISTS idx_transactions_transfer_id ON transactions(transfer_id);
CREATE INDEX IF NOT EXISTS idx_transactions_needs_review ON transactions(needs_review);
CREATE INDEX IF NOT EXISTS idx_transactions_kind_occurred ON transactions(kind, occurred_at);

--------------------------------------------------------------------------------
-- rates
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS rates (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    as_of_date      DATE NOT NULL,
    base            TEXT NOT NULL,
    quote           TEXT NOT NULL,
    rate            DECIMAL NOT NULL,
    source          TEXT NOT NULL,
    created_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (as_of_date, base, quote, source)
);

CREATE INDEX IF NOT EXISTS idx_rates_lookup ON rates(base, quote, as_of_date);

--------------------------------------------------------------------------------
-- earn_positions
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS earn_positions (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    account_id      INTEGER NOT NULL REFERENCES accounts(id) ON DELETE RESTRICT,
    product_id      TEXT NOT NULL,
    asset           TEXT NOT NULL,
    principal       DECIMAL NOT NULL,
    apy             DECIMAL,
    started_at      TIMESTAMP NOT NULL,
    ended_at        TIMESTAMP,
    snapshot_at     TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (product_id, started_at)
);

CREATE INDEX IF NOT EXISTS idx_earn_positions_active ON earn_positions(ended_at);
CREATE INDEX IF NOT EXISTS idx_earn_positions_account ON earn_positions(account_id);

--------------------------------------------------------------------------------
-- import_state
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS import_state (
    source              TEXT PRIMARY KEY,
    last_synced_at      TIMESTAMP,
    cursor              TEXT,
    updated_at          TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

--------------------------------------------------------------------------------
-- import_runs
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS import_runs (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    source              TEXT NOT NULL,
    started_at          TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    finished_at         TIMESTAMP,
    rows_inserted       INTEGER NOT NULL DEFAULT 0,
    rows_updated        INTEGER NOT NULL DEFAULT 0,
    rows_skipped        INTEGER NOT NULL DEFAULT 0,
    status              TEXT NOT NULL DEFAULT 'running',
    error               TEXT,
    CHECK (status IN ('running', 'success', 'error'))
);

CREATE INDEX IF NOT EXISTS idx_import_runs_source ON import_runs(source, started_at);

--------------------------------------------------------------------------------
-- Views
-- Use DROP + CREATE so schema evolutions take effect on re-apply when
-- migration runner treats the file as idempotent.
--------------------------------------------------------------------------------

DROP VIEW IF EXISTS v_account_balances;
CREATE VIEW v_account_balances AS
SELECT
    a.id                                   AS account_id,
    a.name                                 AS account_name,
    a.currency                             AS currency,
    COALESCE(SUM(CAST(t.amount AS REAL)), 0.0) AS balance_native
FROM accounts a
LEFT JOIN transactions t ON t.account_id = a.id
GROUP BY a.id, a.name, a.currency;

DROP VIEW IF EXISTS v_transactions_usd;
CREATE VIEW v_transactions_usd AS
SELECT
    t.id                AS transaction_id,
    t.account_id        AS account_id,
    t.occurred_at       AS occurred_at,
    t.kind              AS kind,
    t.amount            AS amount,
    t.currency          AS currency,
    t.description       AS description,
    t.category_id       AS category_id,
    t.transfer_id       AS transfer_id,
    t.source            AS source,
    t.needs_review      AS needs_review,
    CASE
        WHEN t.currency = 'USD' OR t.currency = 'USDT' OR t.currency = 'USDC'
            THEN CAST(t.amount AS REAL)
        WHEN t.user_rate IS NOT NULL AND CAST(t.user_rate AS REAL) > 0
            THEN CAST(t.amount AS REAL) / CAST(t.user_rate AS REAL)
        ELSE (
            SELECT CAST(t.amount AS REAL) / CAST(r.rate AS REAL)
            FROM rates r
            WHERE r.base = 'USDT' AND r.quote = t.currency
              AND r.source LIKE 'binance_p2p_median%'
              AND r.as_of_date <= DATE(t.occurred_at)
            ORDER BY r.as_of_date DESC
            LIMIT 1
        )
    END                 AS amount_usd,
    CASE
        WHEN t.currency IN ('USD', 'USDT', 'USDC') THEN 'native_usd'
        WHEN t.user_rate IS NOT NULL THEN 'user_rate'
        ELSE 'rates_table'
    END                 AS rate_source
FROM transactions t;

DROP VIEW IF EXISTS v_monthly_summary;
CREATE VIEW v_monthly_summary AS
SELECT
    strftime('%Y-%m', t.occurred_at)       AS month,
    t.account_id                           AS account_id,
    t.category_id                          AS category_id,
    t.kind                                 AS kind,
    COUNT(*)                               AS tx_count,
    SUM(CAST(t.amount AS REAL))            AS total_native
FROM transactions t
WHERE t.kind <> 'transfer'
GROUP BY month, t.account_id, t.category_id, t.kind;

DROP VIEW IF EXISTS v_unreconciled_transfers;
CREATE VIEW v_unreconciled_transfers AS
SELECT
    t.transfer_id                          AS transfer_id,
    COUNT(*)                               AS leg_count,
    GROUP_CONCAT(t.id)                     AS transaction_ids,
    GROUP_CONCAT(t.account_id)             AS account_ids
FROM transactions t
WHERE t.kind = 'transfer'
GROUP BY t.transfer_id
HAVING t.transfer_id IS NULL OR COUNT(*) <> 2;
