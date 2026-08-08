# Rule 002 — Transfers Must Be Paired

**Source ADR:** [ADR-002](../../ADR/ADR-002-double-entry-transfers.md)
**Scope:** All inserts into `transactions` with `kind='transfer'`.

**Constraint:** Every `transactions` row with `kind='transfer'` must carry a non-null `transfer_id` shared with exactly one counterpart leg. `finances.domain.transfers.create_transfer` is the only path that may *pair two existing rows* into a transfer, and it inserts or promotes both legs atomically; every reconciliation strategy, the web triage pairing and the backfill go through it. When a Provincial bank deposit is identified as the receiving leg of a P2P sell (per ADR-002 amendment), the bank row is the anchor and the Binance leg is paired to it.

**One exception, per [ADR-017](../../ADR/ADR-017-same-account-conversions.md) §2.4:** the Binance convert ingest writes a conversion's two legs already paired, via `transactions_repo.upsert_by_source_ref`, because its idempotency comes from upserting on `(source, source_ref)` per rule-010. No other ingest may do this. Well-formedness does not rest on a single writer — `finances doctor` enforces it after the fact, on any row, via `transfer_leg_count`, `transfer_legs_same_account` and `transfer_same_currency_imbalance`.

**Invariants enforced in CI:**

- `SELECT COUNT(*) FROM transactions WHERE kind='transfer' AND transfer_id IS NULL` = 0
- For each `transfer_id`: exactly two rows, on different **positions** — `(account_id, currency)`, per [ADR-017](../../ADR/ADR-017-same-account-conversions.md) — summing to zero in their USD-equivalents within tolerance 0.01. Two legs may share an account when their currencies differ, which is what a currency conversion is. Same account **and** same currency remains forbidden: it nets to nothing, so it records a movement that did not happen. `finances doctor` enforces this via `transfer_legs_same_account`.

**Bank-anchored P2P pairing (per ADR-002 amendment 2026-04-19):** For transfers originating from a Binance P2P sell, the Provincial bank deposit is the canonical anchor. The pairing algorithm finds unpaired Provincial deposits first, then searches for the matching Binance P2P sell within a ±2-day window (configurable). The Binance leg is paired *to* the bank leg, not the other way around.

**Triage guidance — the shape of a real P2P deposit (owner's practice, 2026-08-08).** Julio sells USDT for *round bolivar amounts*: he picks 10k, 20k, 70k, 100k, not a quantity of USDT that happens to produce some rate-derived figure. The ledger bears this out — of 119 bank legs paired to a P2P sell, **115 are multiples of 1,000** and 79 are multiples of 10,000. Among *unpaired* Provincial inflows the distribution inverts, ragged amounts dominating.

This is a **prior for a human reading the triage queue, and deliberately not a rule in code.** Two of the 119 are ragged (10,600 and 20,018.42) and both are genuine — verified because the implied rate `bank_VES / sell_USDT` reproduces the rate the sell states. A roundness filter would have rejected real trades.

Use it in the direction it is reliable: **a ragged bank inflow is evidence against a P2P reading.** When a non-round deposit sits unpaired, the answer is usually that it is not a P2P settlement at all — it is a transfer between the owner's own banks, or someone paying him — and no sell should be hunted for it. A 12,754 Bs deposit was mistakenly queued for pairing on exactly this misreading.

The decisive test remains the rate, not the roundness, and it is already load-bearing: `BankAnchoredP2pPairing._score_candidates` requires `bank_amount ≈ |sell.amount| × sell.user_rate` within tolerance before proposing anything, and `web/services/triage._reject_implausible_pair` re-applies it (looser) on the manual path.

**Same-account currency conversions (per ADR-017):** A Binance USDC→USDT convert writes two legs on Binance Spot in different currencies. `finances.domain.transfers.SameAccountConvertPairing` links them — by the order id both legs share, and for legacy rows whose halves were hashed separately, by same account, same calendar day, opposite sign, different currency and amounts within 2%. It runs as a post-pass in `finances/ingest/binance.py` and on demand via `finances reconcile converts`. Conversions arriving from a fresh sync are written already paired by the ingest itself and so have nothing for the pass to do.
