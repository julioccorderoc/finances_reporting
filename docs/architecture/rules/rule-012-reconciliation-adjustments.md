# Rule 012 — Reconciliation Adjustments

**Source ADR:** [ADR-018](../../ADR/ADR-018-reconciliation-adjustments.md)
**Scope:** All inserts into `transactions` with `kind='adjustment'`.

**Constraint:** A `kind='adjustment'` row may only be written by `finances.domain.reconciliation_adjustments.record_adjustment`, invoked from `finances reconcile balances`. No ingest, backfill, or web write path may create one.

**What an adjustment means:** the difference between what the ledger computes for one `(account, currency)` position and what the custodian holding it reports, on the date the reconciliation was performed. It is the bookkeeping response to history that no longer exists — Binance serves internal-transfer records for six months only, and this ledger's history predates that.

**Invariants:**

- Every adjustment carries a description naming the ledger figure, the custodian figure, and the reconciliation date.
- Adjustments are excluded from every income and expense aggregate, via `kind <> 'adjustment'` in `finances.domain.money.SQL_NOT_CURRENCY_MOVEMENT`. An unexcluded adjustment would be reported as the largest single earning in the ledger.
- Adjustments are included in every balance — correcting the balance is their entire purpose.
- `source = 'reconciliation'`; `source_ref` is `reconcile:<account_id>:<currency>:<uuid>`. The uuid is deliberate: reconciling the same position again later is legitimate, and a stable ref would collide.

**An adjustment may not be written for a gap a pending ingest would close.** Un-synced interest is a sync that has not run; adjusting it double-counts the moment those rows arrive. Run the ingest first, reconcile what remains.

**The custodian figure is always supplied by the owner, never read from an API.** A ledger reconciled against the same API that filled it agrees with itself by construction, which proves nothing.

**Reports before a reconciliation date remain wrong**, and knowably so. Dating the entry to the day it was performed — rather than to the ledger's start — is what makes the claim checkable: from that date forward, ledger and custodian agree, and any later divergence is a new defect with a known start.
