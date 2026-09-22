# ADR-027: The Ingest Reads What Binance Names

**Date:** 2026-09-22
**Status:** Accepted (owner, 2026-09-22)
**Extends:** [ADR-003](./ADR-003-earn-positions-table.md) — a reward is still `Interest` income, but BONUS is paid into Spot, not Earn; [ADR-024](./ADR-024-p2p-and-pay-debit-funding.md) — the taker commission is part of the same movement, so the amount that moved is the amount to book; [ADR-025](./ADR-025-converts-read-their-wallet.md) — the wallet map gains the third wallet Binance names
**Related:** [ADR-023](./ADR-023-exchange-balance-reconciliation.md) — the measurement that found all three and the check that judges the answer; [ADR-010](./ADR-010-idempotent-reingestion.md) — why none of this touches `source_ref`
**Rule:** [rule-002](../architecture/rules/rule-002-transfers-double-entry.md), [rule-003](../architecture/rules/rule-003-earn-position-source.md)

## 1. Context

One principle: **when Binance names the fact on the record, the ledger reads the name instead of assuming it.** Every earlier repair in this family followed that rule — ADR-024 for the wallet with no field, ADR-025 for the wallet with one. Three fields the API has been sending were still being ignored, and each was measurable before it was fixed:

| field | what the ledger assumed | what Binance named | cost of the assumption |
| --- | --- | --- | --- |
| `convert/tradeFlow.walletType` | `SPOT` or `SPOT_FUNDING`, nothing else | `FUNDING` | the 2026-09-15 conversion refused outright; the sync erroring since 2026-09-08 |
| `c2c_trade_history.takerAmount` | the order's `amount` | `amount` ± `takerCommission` | 0.48 USDT of commission unrecorded across eight sells |
| `get_flexible_rewards_history.type` | every reward is Earn interest | BONUS is paid into Spot | 30 BONUS rows (1.282086 USDT + 0.61641 USDC) on the wrong wallet |

## 2. Decision

**The ingest reads the wallet, the taker amount, and the reward's destination from the record.**

### 2.1 A `FUNDING` conversion books both legs to Funding

`_CONVERT_SOURCE_WALLETS` mapped a wallet to a *source* only. It becomes `_CONVERT_WALLETS`, a map to `(from_kind, to_kind)`:

- `SPOT` → `(spot, spot)` — unchanged, 7 conversions.
- `SPOT_FUNDING` → `(funding, spot)` — unchanged from ADR-025.
- `FUNDING` → `(funding, funding)` — new.
- Anything else still raises `ValueError`, and the ingest loop turns that into an `import_runs` error (rule-007's shape). The rename is deliberate: a map whose value is now both legs cannot keep a name that says "source".

**The measurement.** Convert order `2360187977735278790` (2026-09-15 18:47:23Z) carries `walletType: "FUNDING"`: 14.561618 USDC → 14.56478663 USDT. The ledger's own rows say the same thing without the field: an Earn redemption of 14 USDC landed in Funding at 18:46:18 (`destAccount=FUNDING`), a 0.561618 USDC transfer went Spot → Funding at 18:47:06, and Funding USDC's ledger balance before this repair is exactly **14.561618** — the whole convert input, sitting in the wallet that spent it. Refusing the wallet was the right failure (ADR-025 §2's "report it, do not guess"), and it froze `import_state.last_synced_at` at `2026-09-08T13:51:36.616368+00:00` while every later sync committed its rows and recorded `status='error'` with `convert: unknown convert walletType: FUNDING`.

### 2.2 A P2P trade moves `takerAmount`, not the order's `amount`

`RawBinanceP2pRow` gains `takerAmount: Decimal | None` and `takerCommission: Decimal | None`, and uses `takerAmount` for the signed amount when present. `amount` remains the fallback, because older payloads predate the fields — absence is the old behaviour, not a zero. `user_rate` stays `unitPrice` and the description is byte-identical: the bank-anchored pairing and `unpaired_p2p_sells` match its shape.

**The measurement.** The C2C history reports `takerAmount` = `amount` ± `takerCommission` — SELL: `amount + 0.06`, BUY: `amount − 0.06`. The taker commission is charged in crypto, so the order's size is not the amount that moved. Eight sells since 2026-09-08 (orders `22931509527389683712` … `22935673214577852416`) left **0.48 USDT** unrecorded — exactly 8 × 0.06. Funding USDT's ledger balance before this repair is 82.65632664 against the exchange's 96.74111327; the 14.56478663 from 2.1 and the 0.48 commission close it to the cent.

### 2.3 BONUS rewards are paid into Spot

`RawBinanceEarnRewardRow.to_transaction` takes `spot_account_id` and `earn_account_id`. `type == "BONUS"` books to Spot; `REALTIME` and `REWARDS` stay on Earn. The `source_ref` scheme (`earn-reward:<projectId>:<asset>:<time>`) is untouched — it is the dedup key (ADR-010), and rewriting it would re-import every reward as new. Because `upsert_by_source_ref` treats `account_id` as statement-sourced, re-ingesting the window *moves* the existing rows.

**The measurement.** BONUS rewards surface in `/sapi/v1/asset/assetDividend` as "Flexible" dividends, and the Spot balance grows by exactly the BONUS amount daily. Thirty BONUS rows since 2026-09-08 (15 USDT summing 1.282086, 15 USDC summing 0.61641) were booked to Earn. Spot USDT's ledger balance is −0.668438; moving the BONUS rows and nothing else makes it 0.613648 — the exchange's figure to the last digit.

### What the API cannot settle

Two assumptions ride on these decisions, and both are named rather than hidden:

1. **`REWARDS` → Earn.** The rewards endpoint names no destination for the REWARDS stream. It is treated as Earn accrual, like REALTIME, because that is where an in-place yield accrues. If Binance actually pays it to Spot, the assumption is wrong by the REWARDS total and nothing else here changes.
2. **The owner is the P2P taker.** `c2c_trade_history` reports `takerAmount`/`takerCommission` without saying which side of the trade the account holder was. Every row here is the owner's own account, so the taker reading is assumed; on a maker fill the commission belongs to the counterparty and the amount would be wrong by it.

Neither assumption is verifiable from the payload. Both are **judged by `position_disagrees_with_exchange`** (ADR-023): the exchange snapshot is the outside figure, it compares per `(account, currency)`, and it will report the error the moment the assumption stops holding. That is the difference between these two and the three fields above, which the record itself settled.

## 3. Alternatives considered

- **Widen `_CONVERT_SOURCE_WALLETS` with `"FUNDING": "funding"` and leave the destination implicit.** The destination is not implicit any more: `SPOT_FUNDING` already sends its two legs to different wallets, and `FUNDING` sends both to the same one. A source-only map would have to special-case the destination inside `to_transactions`, which is how the previous assumption survived.
- **Keep booking `amount` on P2P and absorb the commission in an adjustment.** The commission is a real movement on a wallet the exchange reports; an adjustment row would hide it in exactly the way ADR-018 was written against. Rejected.
- **Move BONUS rewards by a repair command instead of re-reading them.** The rows already carry a stable `source_ref` and the upsert moves an account on re-ingest, so a one-off command would be a second writer for a movement the ingest already owns. Rejected on rule-003's single-source principle.
- **Change the reward `source_ref` to encode the destination.** It is the dedup key; the change would re-import every reward in the window as a new row. Named only to be refused.

## 4. Consequences

- The 2026-09-15 conversion is ingested as an ordinary Funding-internal transfer pair under rule-002 — two legs, one `transfer_id`, sum zero.
- The watermark can advance again. The frozen `last_synced_at` is the symptom that made all three defects visible at once; the repair is an explicit `finances ingest binance --since 2026-09-07` over the window the failed runs covered.
- Funding USDC goes to 0.00, Spot USDT to 0.613648 and Funding USDT to 96.74111327 — each matching the exchange snapshot to the last digit. Spot USDC lands at 65.324792 against 65.287658: a 0.037134 residual, inside the 0.06 the owner accepted as dust. `finances doctor --strict` exits 0.
- BONUS income still counts as `Interest` income in every report — only the account it sits on changes, and with it per-account balances. Earn's ledger balance now carries the principal movements and the REALTIME/REWARDS accrual, and no longer the BONUS rows; `earn_positions` remains the principal's source of truth (rule-003).
- Pre-2026-09-08 BONUS rows remain on Earn. They are history the owner will restate deliberately in a later wave; this ADR does not reach back past the frozen window.
