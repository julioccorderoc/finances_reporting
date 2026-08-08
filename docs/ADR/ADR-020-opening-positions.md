# ADR-020: Opening Positions, Not Dated Plugs

**Date:** 2026-08-08
**Status:** Accepted
**Supersedes:** [ADR-018](./ADR-018-reconciliation-adjustments.md) §2.1, §4 (first item), and the dating half of §2.3
**Related:** [ADR-002](./ADR-002-double-entry-transfers.md) — the transfer model this one *does* use; [ADR-010](./ADR-010-idempotent-reingestion.md) — stable `source_ref` is what makes a restatement idempotent
**Rule:** [rule-012](../architecture/rules/rule-012-reconciliation-adjustments.md)

## 1. Context

ADR-018 closed the Binance gap with an adjustment row dated the day the
reconciliation was performed. Four days of use produced three findings, one of
them serious.

### 1.1 Dated plugs accumulate

Three adjustments on 2026-08-04. Four more on 2026-08-08. Each corrects a
balance *on its date* and then freezes into history. Nothing about writing one
repairs the underlying hole, so the next reconciliation writes another.

### 1.2 A dated plug silently absorbed a real defect

This is the serious one, and ADR-018 did not anticipate it.

On 2026-08-04 at 13:00, a Binance sync run with `--since` reaching back to
2025-10-03 re-imported 105 events that were already on the books under the
backfill's hashed `source_ref` scheme. At 13:09 — nine minutes later — the
ADR-018 reconciliation ran and wrote plugs sized against those **corrupted**
balances.

The plugs fit the corruption exactly. Every position netted to roughly zero and
`finances doctor` reported a healthy ledger, while the ledger carried 105
duplicate rows and overstated income by 10,462.71 USDC.

A plug is a residual, and a residual absorbs whatever is wrong upstream without
distinguishing *history that no longer exists* from *a bug that landed nine
minutes ago*. ADR-018 §2.1 argued that dating the entry to today makes the claim
checkable. It does — but what it checks is only that the totals agree, and
duplicated data agrees with a plug built from duplicated data.

The failure is not hypothetical: it is why the March 2026 salary appeared twice
and was not caught by any check.

### 1.3 The restatement problem

Because the 2026-08-04 plugs encoded the duplication, retiring the duplicates
made the balances *worse*, not better — Spot USDC went from `0.05` to
`−2,588.72` — until the plugs were retired too. Any repair to historical rows
invalidates every plug written after the defect landed, and there is nothing in
a dated plug that says which ones.

## 2. Decision

Close an unrecoverable gap with an **opening position** dated at the ledger's
start, decomposed so that no entry asserts something untrue.

Two row shapes, both `source='opening_balance'` with stable `source_ref`s:

**A transfer, for value that moved between positions the owner holds.**

```text
2025-10-03  transfer  −10,887.69 USDT  Binance Funding   opening-transfer:3:2:USDT:from
2025-10-03  transfer  +10,887.69 USDT  Binance Spot      opening-transfer:3:2:USDT:to
```

**An opening balance, for value the ledger never saw arrive.**

```text
2025-10-03  adjustment  +5,637.77 USDT  Binance Spot     opening:2:USDT
2025-10-03  adjustment      +5.12 USDC  Binance Spot     opening:2:USDC
```

### 2.1 The decomposition is what answers ADR-018's objection

ADR-018 §2.1 rejected opening balances because Binance Spot USDC "would need an
opening balance of −2,583.65 — a negative opening balance is not a fact about
the world." That is correct, and it remains correct.

It is also avoidable. A position the ledger *overstates* does not need a
negative opening balance; it needs the outbound movement that was never
recorded. Funding is overstated and Spot understated by the same mechanism —
the Spot↔Funding transfers Binance stops serving after six months. That is
movement, and the ledger has had a way to express movement since ADR-002.

Once the movement is recorded as movement, every remaining opening balance is
**positive**: value held before the books began. So the constraint becomes an
invariant rather than an obstacle — **an opening balance may never be
negative**, and if the arithmetic demands one, the gap has been mis-modelled and
the command refuses.

### 2.2 This is not the "synthesise the missing transfers" ADR-018 rejected

ADR-018 §4 rejected inventing Spot→Funding rows "until the balances agree,"
because they "would appear in history, be indistinguishable from real ones, and
pair under `create_transfer` as though the movement were evidenced."

The distinction is quantity and labelling. That alternative meant many rows,
dated across history, wearing the same `source` as real ingested transfers. This
is **one** pair, dated at the ledger's start where no real movement sits, under
`source='opening_balance'` — a source no ingest writes — with a description
saying exactly what it is. It is not indistinguishable from a real transfer; it
is distinguishable by the same means every other row in this ledger is
distinguishable, which is its `source`.

The claim being made is narrow and true: *value moved between two of the
owner's own positions, in an amount we can compute, on a date we cannot
recover.*

### 2.3 What is given up

ADR-018 §2.1's second argument survives and is the real cost.

A dated plug asserts *from this date forward, ledger and custodian agree*, so a
later divergence is a new defect with a known start date. An opening row makes
no dated claim, so that particular tripwire is gone.

Accepted, for two reasons. First, §1.2 showed the tripwire catching the wrong
thing — it certified a duplicated ledger as healthy. Second, the check it
provided is still available on demand and always current: `finances reconcile
opening --dry-run` recomputes the delta against a stated custodian figure at any
time. The claim moves from being frozen into a row to being re-runnable, which
is strictly more useful when historical rows can change.

The trade is stated plainly: **an opening position tells you what the books do
not know; it does not tell you when you last checked.** If a dated record of a
reconciliation is wanted, it belongs in `import_runs` or its own log, not as a
transaction.

### 2.4 Restatement is the normal case, not an exception

Every opening row carries a stable `source_ref` (`opening:<account>:<currency>`,
`opening-transfer:<from>:<to>:<currency>:<leg>`) rather than ADR-018's uuid. A
restatement replaces the prior rows for that position instead of layering a new
correction on top, so the ledger holds **one** opening position per
`(account, currency)` no matter how many times it is restated.

This inverts ADR-018's uuid reasoning. That ADR wanted each reconciliation to be
its own event because reconciliations are legitimately repeated. Here the
repeated act is *restating the same fact more accurately*, and accumulating those
is exactly the sediment §1.1 objects to.

### 2.5 The custodian figure is still an input

Unchanged from ADR-018 §2.3, and for the same reason: reading the balance from
the API that filled the ledger makes it agree by construction. The owner reads
it and states it.

## 3. Consequences

**`finances reconcile opening` replaces `finances reconcile balances`** for
positions with unrecoverable history. The older command is not removed — a dated
adjustment remains the right answer for a gap that genuinely arose on a known
date.

**Adjustments and transfers are already excluded from income and expense** via
`money.SQL_NOT_CURRENCY_MOVEMENT`, so opening rows need no new exclusion. This is
the payoff of ADR-018 §2.2 having consolidated that predicate.

**Reports over the ledger's full span become correct in aggregate**, which they
were not under dated plugs: a plug dated 2026-08-04 leaves every month before it
wrong. An opening position dated at the start is inside every reporting window,
so period balances reconcile.

**A restatement is not an audit trail.** Replacing the prior opening rows
discards what was previously stated. `transaction_edits` does not cover deletes.
If the history of restatements ever matters, it needs its own record; it is not
captured today.

**`negative_asset_balance` remains the guard.** An opening position that fails to
close a gap still surfaces there, and the command's refusal to write a negative
opening balance means a mis-modelled gap fails loudly rather than netting out.

## 4. Rejected alternatives

**Keep dated plugs and re-plug after every repair.** The status quo. Rejected on
§1.1 and §1.2: the sediment is unbounded, and a plug written over a fresh defect
conceals it.

**Retire plugs automatically when historical rows change.** Detect that a repair
invalidated a plug and recompute it. Rejected: recomputing requires a custodian
figure, which is an input (§2.5), so this can only ever prompt — and prompting on
every historical change is the churn being removed.

**One opening balance per position, allowing negatives.** Simplest to implement
and rejected in ADR-018 §2.1 on grounds that still hold. A negative opening
balance states the owner began with less than nothing.

**Reconstruct the real movements from the Binance UI export.** Still the correct
answer if the older history is ever wanted, and still not foreclosed — ADR-018 §4
already reasoned this through. Opening rows are as reversible as adjustments:
delete `WHERE source='opening_balance'` and ingest the real movements.

## 5. Rule extraction

**Target file:** `docs/architecture/rules/rule-012-reconciliation-adjustments.md`

**Injected constraint:** Rows with `source='opening_balance'` may be written only
by `finances.domain.opening_positions.record_opening`, invoked from `finances
reconcile opening`. They are dated at the ledger's start. An opening balance
(`kind='adjustment'`) must be strictly positive; a gap requiring a negative one
must be expressed as a transfer (`kind='transfer'`) to the position the value
moved to, or refused. `source_ref` is stable, so restating a position replaces
its opening rows rather than adding to them. The custodian figure remains an
owner-supplied input.
