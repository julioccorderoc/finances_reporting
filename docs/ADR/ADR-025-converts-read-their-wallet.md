# ADR-025: A Conversion Is Booked To The Wallet Binance Says It Drew From

**Date:** 2026-09-07
**Status:** Accepted (owner, 2026-09-08)
**Related:** [ADR-024](./ADR-024-p2p-and-pay-debit-funding.md) — the same defect class, where no field exists to read; [ADR-017](./ADR-017-same-account-conversions.md) — the conversion-as-transfer shape this widens; [ADR-023](./ADR-023-exchange-balance-reconciliation.md) — what found it and what will judge the answer
**Rule:** [rule-002](../architecture/rules/rule-002-transfers-double-entry.md)

## 1. Context

After ADR-024 the two USDT positions agree with Binance to the cent. One difference survived, and `position_disagrees_with_exchange` named both halves of it:

| | Exchange | Ledger |
| --- | ---: | ---: |
| Spot USDC | 0.23 | −400.38 |
| Funding USDC | 0.00 | 400.00 |

Four hundred dollars in one wallet in the ledger and in neither at Binance. The rows say what happened, one minute apart:

```text
2026-08-22 19:32  earn-redeem:989421227  +400 USDC   -> Funding   (destAccount=FUNDING)
2026-08-22 19:33  convert:2342399485914713482  -400.191772 USDC  -> Spot
```

The redemption is right: Binance's own redemption record says `destAccount=FUNDING`, and the ingest honoured it. The convert is where it breaks — it consumed the USDC that had just landed in **Funding**, and `RawBinanceConvertRow.to_transactions` books every leg against Spot.

The same week shows the control case. On 2026-08-15 the identical sequence ran — redeem 600 USDC, convert it a moment later — except the redemption's `destAccount` was `SPOT`. Both rows landed on Spot, and that position has always reconciled.

Binance names the wallet on the convert record, and the ledger has never read it. Across every conversion in the ledger's history:

| `walletType` | conversions |
| --- | ---: |
| `SPOT` | 7 |
| `SPOT_FUNDING` | 1 — 2026-08-22, this one |

This is ADR-024's defect again, with one difference that matters: there, no field existed and a constant was the honest encoding. Here the field has been in the payload the whole time.

## 2. Decision

**A conversion reads `walletType` and books its outgoing leg against the wallet named.**

- `SPOT` — both legs on Spot. Unchanged, and 7 of 8 conversions.
- `SPOT_FUNDING` — Binance's combined-wallet mode, where a conversion may draw across both. The outgoing leg is booked against **Funding** and the incoming leg against **Spot**, which is where Binance credits the proceeds. The live instance drew 400.19 USDC when Funding held 400.00 and Spot held dust, and 250.17 USDT moved Spot→Funding a minute after the conversion completed — the proceeds landed in Spot and were partly moved on.
- Anything else — recorded as an ingest error (rule-007's shape: report it, do not guess). A wallet the ledger does not understand must not be silently filed as Spot, which is the whole failure being corrected.

A `SPOT_FUNDING` conversion is therefore a **cross-account** transfer pair. That is not a new shape: rule-002 defines a transfer as movement between two `(account, currency)` positions, and ADR-017 already widened "two accounts" to include two positions on one. Value genuinely left Funding and arrived in Spot.

No repair command ships with this. `upsert_by_source_ref` treats `account_id` as statement-sourced, so re-ingesting the conversion moves the leg — but it has to be asked for. **The 35-day lookback is a fallback, not a floor**: `_resolve_time_window` starts from `import_state.last_synced_at` whenever there is one, so an ordinary sync covers only the days since the last one and will never revisit 2026-08-22. This ADR first claimed otherwise; the ordinary sync was run, reported `updated=0`, and the leg did not move.

The repair is therefore an explicit `finances ingest binance --since 2026-08-20`, which reported `inserted=0 updated=93` and left the row count unchanged at 3,046. The duplication hazard of a deep `--since` — 105 events on 2026-08-08 — needs legacy `:hash:` source_refs to collide with native ids, and there are none in this window; that was checked before running, not after.

## 3. Alternatives considered

- **Book both legs of a `SPOT_FUNDING` conversion against Funding.** Simpler, and it puts 400.05 USDT into Funding that Binance says is not there — the USDT positions currently agree to the cent, and this would break them. Rejected by the measurement.
- **Split the outgoing leg across both wallets in proportion to what each held.** Closest to the mechanics, and unknowable: the record reports one `fromAmount` and no split. Inventing one would be a derived figure dressed as a fact. Rejected.
- **Hand-move the one row and leave the ingest alone.** The next `SPOT_FUNDING` conversion recreates the bug, silently. Rejected.
- **Keep the constant and widen the tolerance until it passes.** Named only to be refused: the check would go quiet and the ledger would stay wrong.

## 4. Consequences

- The USDC difference closed to dust, as predicted: Spot −0.19 against 0.27, Funding −0.19 against 0.00, both inside `EXCHANGE_BALANCE_TOLERANCE`, and `position_disagrees_with_exchange` went quiet.
- That dust is this ADR's own approximation, and it is worth naming: the conversion drew 400.191772 USDC when Funding held exactly 400.00, so 0.191772 of it came from Spot and is booked to Funding anyway. `negative_asset_balance` has no dust floor and reported both positions at −0.19. The two USDC opening positions were restated against the custodian figure (Spot 5.58, Funding 0.191772), which is ADR-018 §2.3's protocol carried forward and the same step ADR-024 took for USDT — not a tolerance widened to make a check pass. The Funding figure is exactly the over-draw, and is an approximation absorbed at the ledger's start rather than pre-ledger value.
- `finances doctor` reads **0 errors** for the first time, so `--strict` is usable as a gate again.
- The rule for `SPOT_FUNDING` rests on a single observation, and this is stated rather than hidden. If it is wrong, `position_disagrees_with_exchange` says so the next time one happens — which is the reason ADR-023 was built first, and the difference between this and every earlier repair in this ledger's history.
- An unrecognised `walletType` now fails loudly instead of defaulting, so the next wallet Binance invents surfaces as an ingest error rather than a slow drift.
