# Rule 003 — Earn Position Writes Are Single-Sourced

**Source ADR:** [ADR-003](../../ADR/ADR-003-earn-positions-table.md)
**Scope:** All inserts/updates into `earn_positions` and all `Interest` income rows on the `Binance Earn` account.

**Constraint:** `earn_positions` is written exclusively by `finances/ingest/binance.py` after a successful `simple_earn_flexible_position` fetch. Every Earn reward must be inserted as `kind='income'` on the `Binance Earn` account with category `Interest`. No other module may insert into `earn_positions` or backfill Earn rewards through a different path.

**Lint check:** `grep -rn "earn_positions" finances/ | grep -v "^finances/db\|^finances/ingest/binance.py\|^finances/reports"` must return empty (db = repo, reports = read-only consumers).
