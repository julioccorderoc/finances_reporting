# Rule 002 — Transfers Must Be Paired

**Source ADR:** [ADR-002](../../ADR/ADR-002-double-entry-transfers.md)
**Scope:** All inserts into `transactions` with `kind='transfer'`.

**Constraint:** Every `transactions` row with `kind='transfer'` must be created by `finances.domain.transfers.create_transfer`, which atomically inserts both legs sharing a non-null `transfer_id`. No other code path may insert a `kind='transfer'` row. When a Provincial bank deposit is identified as the receiving leg of a P2P sell (per ADR-002 amendment), the bank row is the anchor and the Binance leg is paired to it.

**Invariants enforced in CI:**

- `SELECT COUNT(*) FROM transactions WHERE kind='transfer' AND transfer_id IS NULL` = 0
- For each `transfer_id`: exactly two rows, on different accounts, summing to zero in their USD-equivalents within tolerance 0.01.

**Bank-anchored P2P pairing (per ADR-002 amendment 2026-04-19):** For transfers originating from a Binance P2P sell, the Provincial bank deposit is the canonical anchor. The pairing algorithm finds unpaired Provincial deposits first, then searches for the matching Binance P2P sell within a ±2-day window (configurable). The Binance leg is paired *to* the bank leg, not the other way around.
