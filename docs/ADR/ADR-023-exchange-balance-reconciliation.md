# ADR-023: The Ledger Reconciles Its Exchange Positions Against The Exchange

**Date:** 2026-09-07
**Status:** Proposed (awaiting owner)
**Related:** [ADR-003](./ADR-003-earn-positions-table.md) — the precedent this generalises; [ADR-018](./ADR-018-reconciliation-adjustments.md) — what to do with a difference once it is measured; [ADR-020](./ADR-020-opening-positions.md) — the other way a position gets closed
**Rule:** [rule-003](../architecture/rules/rule-003-earn-position-source.md)

## 1. Context

`finances doctor` reported `negative_asset_balance` for months: Binance Spot holding **−2,017.45 USDT** and **−400.38 USDC**. A position below zero is not overspending, it is a missing or misfiled arrival — the check says so in its own description. What it could not say was *which*, because the ledger had no idea what Binance actually held.

It had the means and never used them. `earn_positions` (ADR-003) exists precisely so the Earn account can be checked against Binance's own position endpoint, and on 2026-09-03 that check is what proved Earn was right to within a few tenths of accrued reward. Spot and Funding had no equivalent. Grepping `finances/` for `funding_wallet` or `.account()` returns **nothing**: the SDK exposes both, `tests/conftest.py` even mocks both, and production has never called either.

The cost of that gap, measured today:

| | Exchange | Ledger | Off by |
| --- | ---: | ---: | ---: |
| Spot USDT | 0.55 | −2,017.45 | −2,018.00 |
| Funding USDT | 272.56 | 2,335.56 | +2,063.00 |
| Spot USDC | 0.23 | −400.38 | −400.62 |
| Funding USDC | 0.00 | 400.00 | +400.00 |

The *combined* free USDT is nearly right — 273.11 against the ledger's 318.11 — while each wallet individually is out by two thousand. That is the signature of a booking sent to the wrong wallet, not of missing money, and it is diagnosable only because both numbers are finally in the same place. ADR-024 acts on it.

The general failure is worth naming separately from the specific bug: **a defect in wallet attribution is invisible to every check the ledger has**, because every check reasons about the ledger's own rows and they are internally consistent. Double-entry cannot catch it. Only an outside figure can.

## 2. Decision

The ledger **records what the exchange says it holds**, and `finances doctor` compares.

1. A new table, `exchange_balances` — one row per `(account, currency, captured_at)`, holding the balance the exchange reported. Same shape and same purpose as `earn_positions`: a dated external claim, never a derived figure.
2. `finances ingest binance` captures a snapshot for Spot and Funding on every run, from `client.account()` and `client.funding_wallet()`. It is two read-only calls at the end of a sync that already makes a dozen.
3. A new integrity check, `position_disagrees_with_exchange` (ERROR): for each position with a snapshot, sum the ledger's own rows **dated at or before the capture** and compare. Beyond `EXCHANGE_BALANCE_TOLERANCE` (1 unit of the asset — dust and accrued reward live below it) the check reports.

Comparing as-of the capture time, rather than against today's ledger, is what makes a stale snapshot harmless: rows that landed after the capture are not counted against it. A snapshot is a claim about a moment, and it is compared to that moment.

The check stays offline, like every other one. `doctor` never makes a network call; it reads what the last sync wrote. That keeps `doctor` runnable on a plane and keeps the API failure mode inside ingest, where `import_runs.error` already handles it (rule-007).

Severity is ERROR, and it will fire on the live ledger today. That is correct: the difference is real, it is $2,000 wide, and every per-account report is wrong until it is closed.

## 3. Alternatives considered

- **Have `doctor` call the API directly.** Simplest to write, and wrong: it makes the health check require credentials and a network, and it means the number is never recorded, so nothing can say when the drift started. Rejected.
- **Compare only the total across Binance accounts, not per wallet.** The totals are nearly right today — that is exactly why this defect survived a year. A check that only sees the total cannot see the bug that motivated it. Rejected.
- **Reconcile automatically: write an ADR-018 plug whenever the ledger and the exchange disagree.** This is how the 2026-08-04 plugs were made, sized against a ledger that was already corrupt, and they had to be reversed. Measure first; close a difference only when it is understood. The check reports; a human decides. Rejected.
- **Extend `earn_positions` to hold Spot and Funding too.** Its columns (`product_id`, `apy`, `started_at`) are an Earn product's shape, and a wallet balance has none of them. Rejected.

## 4. Consequences

- The Spot/Funding split defect becomes visible and dated, and stays visible until it is fixed. It is the evidence base for ADR-024.
- After that repair, this check is what proves the repair worked — and what would catch the same class of defect on the day it appears rather than a year later.
- `finances doctor` gains one ERROR today. `--strict` stays red until the ledger and the exchange agree, which is the point of a gate.
- The snapshot history accumulates, so "when did this start?" becomes answerable for the next occurrence.
- Nothing about rate resolution, categorisation or reporting changes. This ADR adds an observation, not an opinion.
