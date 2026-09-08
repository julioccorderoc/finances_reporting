# ADR-024: P2P Trades And Binance Pay Belong To The Funding Wallet

**Date:** 2026-09-07
**Status:** Proposed (awaiting owner)
**Related:** [ADR-023](./ADR-023-exchange-balance-reconciliation.md) — the measurement that made this visible and that verifies the repair; [ADR-020](./ADR-020-opening-positions.md) — what must be restated afterwards; [ADR-010](./ADR-010-idempotent-reingestion.md) — why the repair does not touch `source_ref`
**Rule:** [rule-003](../architecture/rules/rule-003-earn-position-source.md)

## 1. Context

`finances doctor` reported `negative_asset_balance` on Binance Spot for months. With ADR-023 in place the ledger can finally state the difference instead of the symptom:

| | Exchange | Ledger | Off by |
| --- | ---: | ---: | ---: |
| Spot USDT | 0.55 | −2,017.45 | −2,018.00 |
| Funding USDT | 272.56 | 2,335.56 | +2,063.00 |

Two wallets wrong by two thousand in opposite directions, with a combined total nearly right, is not missing money. It is money booked to the wrong wallet.

`ingest/binance.py` hardcodes the wallet for two event classes. `RawBinanceP2pRow.to_transaction` and `RawBinancePayRow.to_transaction` both take `spot_account_id`, so **every P2P order and every Binance Pay event is filed against Spot** — 158 P2P rows and 15 Pay rows, −13,748.63 USDT between them. On Binance, P2P (C2C) trades settle in the **Funding** wallet and Binance Pay spends from it. Every other event class already reads its wallet from the record: internal transfers from `type`, Earn subscriptions from `sourceAccount`, Earn redemptions from `destAccount`. These two never did.

The ledger's own rows say the same thing without any outside help. USDT moves Spot→Funding and barely comes back — Funding receives 13,448.75 across transfers and redemptions and pays out 225.50, ever. A wallet money enters and never leaves is the missing half of a booking.

### What discriminates this from a guess

The repair cannot flatter the totals, and it is worth being precise about why. Excluding the ADR-020 rows, the ledger's own Binance history sums to −3,058.94 USDT against the exchange's 273.11, so **3,332.05 of pre-ledger value has to be claimed either way** — and moving rows between two accounts does not change their sum, so that figure is invariant to this decision. What the repair changes is only *where the unrecorded internal movement sits*, which ADR-020 must state as an opening transfer:

| moved to Funding | unrecorded Spot↔Funding movement ADR-020 must assert |
| --- | --- |
| nothing (today) | 12,950.69 **Funding → Spot** |
| `p2p:` only | 2,561.17 Funding → Spot |
| `pay:` only | 9,591.58 Funding → Spot |
| **`p2p:` + `pay:`** | **797.94 Spot → Funding** |

Today's ledger has to claim that 12,950.69 USDT left Funding for Spot without a record — **more than the 9,842.05 that every recorded transfer ever put into Funding.** A wallet cannot pay out more than it took in and still be at 272.56. Moving both classes shrinks the unexplained residue sixteenfold and flips it to the same direction as all 75 recorded transfers, which is the direction money actually travels here: Spot earns and converts, Funding spends.

This is corroboration, not the argument. The argument is that Binance settles P2P in Funding and spends Binance Pay from it, and the ledger has been recording the consumption of the Funding wallet against Spot for a year.

### What this is not

The USDC side looks similar and is a different question. Spot USDC reads −400.38 against the exchange's 0.23, and the +400 sits on Funding as a single Earn redemption (row 7549, 2026-08-22). That row is **correct as ingested**: Binance's own `destAccount` said `FUNDING` and the ingest honoured it. The 400 has since left Funding by a path the ledger never saw. No P2P or Pay row is involved, and this ADR does not move it. ADR-023's check keeps reporting it until it is explained, which is the check working.

## 2. Decision

**P2P and Pay rows are booked against Binance Funding**, at ingest and in history.

1. `RawBinanceP2pRow.to_transaction` and `RawBinancePayRow.to_transaction` take `funding_account_id`. The parameter is renamed, not just repointed, so the next reader cannot mistake the wallet for an accident.
2. A repair command, `finances reconcile wallets [--dry-run]`, moves existing `p2p:` and `pay:` rows off Spot and onto Funding. Consistent with `reconcile converts` / `legacy-dupes` / `reversals`: history is repaired by an explicit command, never by a migration that runs itself the next time any CLI command touches the ledger.
3. `source_ref` is untouched. It is the dedup key (ADR-010), and rewriting it would make every repaired row re-import as new.

Ordering matters and is part of the decision: **repair first, restate the opening positions second.** Sizing an opening against a ledger that is still misfiled is exactly what produced the 2026-08-04 plugs, which fit the corruption and had to be reversed.

## 3. Alternatives considered

- **Leave history, fix ingest only.** New rows land correctly and the two thousand dollars stays wrong forever, in a position that is still negative. Rejected.
- **A migration that moves the rows.** Every `finances` command auto-migrates the live ledger, so the repair would run itself the first time the owner typed anything — no dry run, no reading it first. Rejected on process, not on effect.
- **Close the difference with an ADR-018 adjustment.** It would make `doctor` green while the rows stay on the wrong wallet, and freeze the error into history. This is the failure ADR-020 was written about. Rejected.
- **Restate the opening transfer and leave the rows.** Arithmetically available — the totals can be made to agree without moving anything. It requires asserting that 12,950.69 USDT left Funding unrecorded when only 9,842.05 ever entered it, and it leaves every P2P and Pay row on a wallet that did not fund it. Rejected: the numbers would agree and the ledger would still be wrong.
- **Read the wallet from the P2P/Pay payload the way every other class does.** Preferable in principle, and neither endpoint reports one — P2P settles in Funding by product design and `pay_history` names no wallet at all. A constant is the honest encoding of a fact that has no field.

## 4. Consequences

- 173 rows change account. No amount, date, category, rate, pairing or `source_ref` changes, so nothing about reports, dedup or triage moves — except per-account balances, which is the point.
- Spot and Funding both remain wrong until the opening positions are restated; the ADR-023 check is what says when that is done.
- P2P pairing is unaffected: the bank leg anchors it (ADR-002 amendment) and the Binance leg simply sits on a different account, still a cross-account pair.
- The Pay twin guard is unaffected — it already matched on "one of the Binance accounts", because the backfill had put five of ten legacy Pay rows on Funding.
