# ADR-019: Bank Reversals (RETORNOS) Pair With Their Failed Charge and Leave Spending

**Date:** 2026-08-04
**Status:** Accepted (owner, 2026-08-05)
**Related:** [ADR-002](./ADR-002-double-entry-transfers.md) — reuses the shared-`transfer_id`, sum-to-zero mechanism; [ADR-017](./ADR-017-same-account-conversions.md) — precedent for a same-account zero-sum pair
**Rule:** [rule-002](../architecture/rules/rule-002-double-entry-transfers.md)

## 1. Context

When Provincial rejects a pago móvil, the statement records the failed attempt, then the bank returns the money, then the owner retries. The commission is charged and returned in exactly the same shape. A single successful payment of 1,250 Bs therefore lands as **six rows**:

| id | description | kind | amount |
| --- | --- | --- | --- |
| 7193 | DR OB V27209763 102BANCO | expense | −1,250.00 |
| 7192 | COM. PAGO MOVIL | expense | −3.75 |
| 7196 | REVERSO CARGO | income | +1,250.00 |
| 7195 | REVERSO CARGO | income | +3.75 |
| 7198 | DR OB V27209763 102BANCO | expense | −1,250.00 |
| 7197 | COM. PAGO MOVIL | expense | −3.75 |

The ledger currently treats all six at face value, which distorts every report:

- **Spending is double-counted** — both attempts land in a category (or triage).
- **Income is inflated** — the reversal is booked as income, and rule 27 (`REVERSO CARGO` → Fees) labels it *Fees income*, polluting the one category that should only ever contain charges.
- **Triage is noisier** — the failed attempt sits in `needs_review` waiting for a human to categorize spending that never happened.

The live DB holds 14 `REVERSO CARGO` rows (7 payment/commission reversal pairs). Signed net totals happen to cancel, but gross expense, gross income, and per-category numbers are all wrong.

Deleting duplicates is not an option: every row exists on the bank statement, the statement-integrity check walks the running `saldo`, and re-ingest would resurrect anything deleted (ADR-010).

## 2. Decision

Treat a reversal and the failed charge it undoes as a **zero-sum pair sharing a `transfer_id`**, exactly like a transfer's two legs (rule-002: two rows, shared id, signed, sum to zero — here −X and +X on the *same* account). Both legs flip to `kind='transfer'`, so `domain/money.py` already excludes them from spending and income everywhere, with **no schema change and no new report logic**. The successful retry stays a normal expense — the only one counted.

Mechanically, a new reconciliation strategy `ReversalPairing` (EPIC-006 Strategy protocol, the designed extension point) runs after Provincial ingest, like `BankAnchoredP2pPairing`:

1. Collect unpaired `REVERSO CARGO` income rows (the recognizable-name list is a module constant, `REVERSAL_MARKERS`; extendable when other RETORNO wordings appear).
2. For each, find candidate expenses on the same account: exact opposite amount, occurred at most 4 days before (or on) the reversal's day, not already paired.
3. **Claim greedily, each charge at most once** — the ADR-002 amendment's reasoning applies verbatim: when the failed attempt and the successful retry both qualify (same amount, same day), any assignment yields identical totals. Preference keeps the most information: an uncategorized charge over a hand-triaged one, then the closest day, then the newest row. A reversal with no candidate is left alone.
4. Paired legs: `kind='transfer'`, shared fresh `transfer_id`, `needs_review=0`. The reversal leg sheds its category (rule-27 noise); the charge leg keeps its own — it may be hand triage, and reports ignore categories on transfer rows.

Backlog cleanup ships as `finances reconcile reversals [--dry-run]`, consistent with `reconcile converts` / `reconcile categories`.

Rule 27 (`REVERSO CARGO` → Fees) is **deactivated**: a reversal that pairs is a transfer and needs no category; one that fails to pair is exactly the case a human should see in triage, not a silent "Fees income".

## 3. Alternatives considered

- **Skip/delete duplicate rows at ingest.** Breaks ledger-mirrors-statement, the saldo integrity walk, and idempotent re-ingest. Rejected.
- **Categorize the reversal into the same category as the charge (negative expense).** Signed category nets come out right, but gross expense/income stay inflated and every "expenses by month" view needs a special case. Rejected.
- **New `kind='reversal'`.** Honest naming, but every `kind`-driven switch (money.py, reports, triage, viewer) grows a case for ~14 rows. The transfer mechanism already models "money moved, nothing was spent". Rejected.

## 4. Consequences

- Monthly/consolidated reports drop the phantom expense and phantom income for all 7 historical reversal pairs; only successful retries count.
- Reversal pairs disappear from the triage queue.
- A reversal with no matching charge (e.g. a genuine refund from a merchant weeks later) surfaces in triage as uncategorized income — a human decision, as it should be.
- `UNIQUE(source, source_ref)` and re-ingest behavior are untouched; pairing survives re-ingest because the upsert preserves `transfer_id` and the kind of a paired row.
