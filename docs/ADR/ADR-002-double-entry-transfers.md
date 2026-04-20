# ADR-002: Double-Entry Model for Transfers Between Own Accounts

**Date:** 2026-04-19
**Status:** Accepted

## 1. Context

Today, money moving between the user's own accounts (Binance Funding ↔ Spot, Binance → P2P sell → Bank, Bank → ATM cash) is recorded inconsistently:

- Binance internal transfers appear as paired rows on the Binance side but never on the bank side.
- P2P sells decrease Binance USDT but the corresponding bolívar deposit is missing from the bank export.
- Cash withdrawals vanish entirely.

This causes two distinct failure modes: (a) account balances do not match reality, and (b) income/expense aggregations double-count or miss flows that are pure movement.

Two modeling options:

1. **Double-entry**: each transfer is two linked transactions sharing a `transfer_id` — one negative on the source account, one positive on the destination.
2. **Single-row**: one transaction with `from_account` and `to_account` columns.

## 2. Decision

Use **double-entry**. A transfer is two rows in `transactions`, each on its own account, summing to zero in their respective USD-equivalents (within tolerance), grouped by a UUID `transfer_id` column. Both rows have `kind='transfer'`. Reports filter `kind <> 'transfer'` for income/expense aggregations and use the per-account sum (which includes transfers) for balances.

## 3. Consequences (The "Why")

### Positive

- Per-account balances are a trivial `SUM(amount) GROUP BY account_id` and naturally include transfers in/out.
- Income/expense aggregations exclude transfers cleanly with a single `WHERE kind <> 'transfer'`.
- Each transfer leg can carry its own `user_rate`, capturing the realized rate of a P2P sell on the receiving side independently from the sending side.
- Schema stays uniform — one `transactions` table, no special-case columns.
- Reconciliation gap detection is trivial: `transfer_id` rows that do not have a sibling are findable via `v_unreconciled_transfers`.

### Negative

- Every transfer requires inserting two rows transactionally; `domain.transfers.create_transfer` must be the only path that creates them.
- Backfill must detect implicit transfers in legacy CSVs (e.g. P2P sell without a paired bank deposit) and either generate the missing leg or flag it for review.
- Beginners reading the table see "duplicate" rows; documentation must explain the model.

## 4. Rule Extraction (The "How" for Agents)

**Target File:** `docs/architecture/rules/rule-002-transfers-must-be-paired.md`
**Injected Constraint:** Any insert with `kind='transfer'` must originate from `domain.transfers.create_transfer`, which atomically writes both legs and shares a non-null `transfer_id`. Direct inserts of `kind='transfer'` rows from any other module are forbidden. CI/test must assert `SELECT COUNT(*) FROM transactions WHERE kind='transfer' AND transfer_id IS NULL = 0`.

---

## Amendment 2026-04-19 — Provincial Bank Is the Pairing Anchor

**Context:** The Provincial bank statement is the ledger with ultimate canonical authority for bolívar flows; Binance P2P sells produce inflows that *must* match a Provincial deposit on or near the same date. Anchoring on Binance and looking outward to the bank produces stale pairs when the bank statement is delayed; anchoring on the bank produces correct pairs because the bank row is the ground truth that money actually arrived.

**Amendment:** When pairing a P2P sell to its bolívar receipt, the **Provincial deposit row is the anchor**. The pairing algorithm runs as part of `finances/ingest/provincial.py` (or as a post-pass in `finances/migration/backfill.py`) and walks unpaired Provincial deposits matching the shape of a P2P inflow (large amount, source description matching known counterparties or "transfer recibido"), then searches Binance P2P sell rows within a configurable date window (default ±2 days) for a match by amount × rate. On match, `domain.transfers.create_transfer` writes the pair with the Binance leg pointing at the bank-side leg as the canonical receipt.

**Rule update:** `docs/architecture/rules/rule-002-transfers-must-be-paired.md` is updated to require that, for P2P-derived transfer pairs, the Provincial leg is created/identified first and the Binance leg is paired to it — not vice versa.
