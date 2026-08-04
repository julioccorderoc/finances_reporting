# ADR-018: Reconciliation Adjustments for History the Custodian Will Not Return

**Date:** 2026-08-04
**Status:** Accepted
**Related:** [ADR-003](./ADR-003-earn-positions-table.md) — the unbuilt half whose repair exposed this; [ADR-002](./ADR-002-double-entry-transfers.md), [ADR-017](./ADR-017-same-account-conversions.md) — the transfer model this deliberately does not use
**Rule:** [rule-012](../architecture/rules/rule-012-reconciliation-adjustments.md)

## 1. Context

The ledger's Binance positions do not match Binance. After repairing the Earn
principal gap (ADR-003, never implemented) and re-syncing every endpoint back
to 2025-10-01:

| position | ledger | custodian | gap |
|---|---:|---:|---:|
| Binance Spot USDT | −10,084.27 | 0.05 | **+10,084.32** |
| Binance Funding USDT | 9,398.51 | 490.68 | **−8,907.83** |
| Binance Spot USDC | 2,583.70 | 0.05 | **−2,583.65** |
| Binance Earn USDC | 6,005.75 | 6,000.53 | −5.22 |
| Binance Earn USDT | 754.07 | 750.14 | −3.93 |

A ledger holding **−10,084 USDT** is not merely inaccurate, it is impossible:
you cannot spend an asset you never received. `finances doctor` says so
(`negative_asset_balance`, ERROR).

### 1.1 Why the history cannot simply be fetched

Binance serves `user_universal_transfer_history` — the Spot↔Funding movements
that would explain most of the imbalance — for **six months only**. Probed
directly on 2026-08-03:

```text
start ~6mo ago (2026-02-04): OK, total=13
start ~7mo ago (2026-01-05): FAIL (400, -5026, 'Start time query records range is too large')
```

The ledger's history begins 2025-10-30. Everything between then and roughly
February 2026 is gone from the API and is not coming back. The ingest is
correct — it chunks at 29 days, well inside the documented 30-day range cap,
logs the failure, and declines to advance its watermark. There is simply no
source left to read.

That is the whole of the problem: **the ledger is missing movements whose
records no longer exist anywhere.**

### 1.2 Two different gaps that must not be treated alike

The Earn rows above differ in kind from the other three. Their gap is interest
already earned and not yet ingested — the next sync closes it. Writing an
adjustment against them would double-count the moment those reward rows
arrive.

The other three have no pending source. Nothing will ever close them.

**Only a gap with no remaining source may be adjusted.** A gap that is merely
un-synced is a sync that has not run.

## 2. Decision

Introduce a **reconciliation adjustment**: a single `kind='adjustment'` row
per `(account, currency)`, recording the difference between what the ledger
computes and what the custodian reports, on the date the reconciliation was
performed.

This is the ordinary bookkeeping response to unrecoverable history, and it is
what `kind='adjustment'` — permitted by the schema since migration 001 and
unused until now — was reserved for. An adjustment asserts that a previous
record is incomplete. That is exactly the claim being made.

```text
2026-08-04  adjustment  +10,084.32 USDT  Binance Spot
            "Reconciliation to custodian balance: ledger -10084.27,
             Binance 0.05 as of 2026-08-04. Source history unavailable."
```

### 2.1 It is dated today, not at the ledger's start

An opening-balance row — dated 2025-10-29, "what I held on day one" — was the
obvious first idea and is wrong here, for two reasons.

It would be **false**. The gap is not only a missing starting position; it is
mostly missing *mid-history* transfers between Spot and Funding. Dating the
whole difference to day one asserts an opening position the owner never held.
Binance Spot USDC would need an opening balance of **−2,583.65** — a negative
opening balance is not a fact about the world.

It would also be **unfalsifiable**. Dated today, the entry makes a checkable
claim: *from this date forward, ledger and custodian agree.* Any future
divergence is then a new defect with a known start date, not more sediment.

The cost is accepted and stated: **month-by-month history before the
reconciliation date stays wrong**, and no report should be read as accurate
across that boundary. The alternative was not a correct history — it was a
history that looked correct.

### 2.2 Adjustments are excluded from income and expense

An unexcluded +10,084 adjustment would be the largest single "income" in the
ledger's life. `finances/domain/money.py` holds one predicate that both report
builders share (`SQL_NOT_CURRENCY_MOVEMENT`); it gains `kind <> 'adjustment'`.
Because that consolidation already happened, this is one string, not the
four-site fan-out ADR-017 rejected a new `kind` over.

Balances are unaffected by the exclusion: they are `SUM(amount) GROUP BY
account_id` over every row, which is precisely why the adjustment corrects
them.

### 2.3 The owner supplies the custodian figure

`finances reconcile balances --account "Binance Spot" --currency USDT --actual
0.049314` computes the delta and writes the row. The custodian balance is an
input, never inferred: reading it from an API would make the ledger agree with
the API by construction, which is not a reconciliation but a tautology. The
owner reads it from Binance and states it.

Re-running with the ledger already matching writes nothing.

## 3. Consequences

**`finances doctor` reaches zero errors.** `negative_asset_balance` was the
last one, and it was correct to fire.

**Every Binance position matches the custodian from 2026-08-04.** The three
adjustments are +10,084.32 USDT (Spot), −8,907.83 USDT (Funding), −2,583.65
USDC (Spot). The two Earn gaps are deliberately left for the next sync (§1.2).

**Pre-reconciliation monthly reports remain wrong** and are now *knowably*
wrong, with a date attached. This is the deliberate trade in §2.1.

**`kind='adjustment'` acquires a meaning and a guard.** It was permitted and
unused, so nothing constrained it. rule-012 now states that an adjustment may
only be written by the reconciliation command, must carry a description naming
both figures and the date, and may never be used to make a number look better
in the absence of a custodian statement.

**A second use is now possible and is not endorsed here.** Cash USD is
CLI-entered (ADR-008) and has no custodian to reconcile against; counting
notes in a drawer is not the same act. If that is ever wanted it needs its own
decision.

## 4. Rejected alternatives

**Opening-balance rows dated before the ledger begins.** Rejected in §2.1: it
would attribute mid-history losses to a starting position, and would require
asserting a negative opening balance.

**Synthesise the missing transfers.** Invent Spot→Funding rows until the
balances agree. Rejected: it fabricates transactions that would appear in
history, be indistinguishable from real ones, and pair under `create_transfer`
as though the movement were evidenced. An adjustment is honest about being an
adjustment.

**Leave it and silence the check.** Rejected: `negative_asset_balance` is
correct and was added precisely because an impossible balance had gone
unnoticed. Suppressing the alarm to avoid the repair is the failure mode the
check exists to prevent.

**Reconstruct from the Binance UI export.** The web export covers a longer
window than the API. Rejected for now on effort and fragility — a new parser
for a format outside `rule-010`'s source-ref discipline, to recover a period
whose only consumer is a report already known to be unreliable. If the older
history is ever wanted, this is the route, and this ADR does not foreclose it:
the adjustment row can be reversed and replaced by the real movements.

## 5. Rule extraction

**Target file:** `docs/architecture/rules/rule-012-reconciliation-adjustments.md`

**Injected constraint:** `kind='adjustment'` rows may be written only by
`finances.domain.reconciliation_adjustments.record_adjustment`, invoked from
`finances reconcile balances`. Each carries a description naming the ledger
figure, the custodian figure and the reconciliation date. Adjustments are
excluded from every income/expense aggregate via
`money.SQL_NOT_CURRENCY_MOVEMENT` and included in every balance. An adjustment
may not be written for a gap that a pending ingest would close.
