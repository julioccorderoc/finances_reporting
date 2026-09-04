# Rule 012 — Reconciliation Adjustments

**Source ADR:** [ADR-018](../../ADR/ADR-018-reconciliation-adjustments.md), [ADR-020](../../ADR/ADR-020-opening-positions.md)
**Scope:** All inserts into `transactions` with `kind='adjustment'`, and all rows with `source='opening_balance'`.

> **Two shapes, two situations.** A gap that arose on a *known* date gets a dated adjustment (ADR-018, below). A gap from history that no longer exists anywhere gets an **opening position** dated at the ledger's start (ADR-020, at the end of this file). ADR-020 supersedes ADR-018's dating argument; everything else in ADR-018 stands.

**Constraint:** A `kind='adjustment'` row may only be written by `finances.domain.reconciliation_adjustments.record_adjustment`, invoked from `finances reconcile balances` or from the Accounts page's Set balance control (ADR-018 amendment 2026-09-03, at the end of this file). No ingest or backfill may create one, and no web path may write one except by calling that function.

**What an adjustment means:** the difference between what the ledger computes for one `(account, currency)` position and what the custodian holding it reports, on the date the reconciliation was performed. It is the bookkeeping response to history that no longer exists — Binance serves internal-transfer records for six months only, and this ledger's history predates that.

**Invariants:**

- Every adjustment carries a description naming the ledger figure, the custodian figure, and the reconciliation date.
- Adjustments are excluded from every income and expense aggregate, via `kind <> 'adjustment'` in `finances.domain.money.SQL_NOT_CURRENCY_MOVEMENT`. An unexcluded adjustment would be reported as the largest single earning in the ledger.
- Adjustments are included in every balance — correcting the balance is their entire purpose.
- `source = 'reconciliation'`; `source_ref` is `reconcile:<account_id>:<currency>:<uuid>`. The uuid is deliberate: reconciling the same position again later is legitimate, and a stable ref would collide.

**An adjustment may not be written for a gap a pending ingest would close.** Un-synced interest is a sync that has not run; adjusting it double-counts the moment those rows arrive. Run the ingest first, reconcile what remains.

**The custodian figure is always supplied by the owner, never read from an API.** A ledger reconciled against the same API that filled it agrees with itself by construction, which proves nothing.

**Reports before a reconciliation date remain wrong**, and knowably so. Dating the entry to the day it was performed is what makes the claim checkable: from that date forward, ledger and custodian agree, and any later divergence is a new defect with a known start. This holds for a gap with a known onset date; for unrecoverable history, ADR-020 trades this tripwire away deliberately — see below.

## Opening positions (ADR-020)

**Constraint:** Rows with `source='opening_balance'` may only be written by `finances.domain.opening_positions.record_opening`, invoked from `finances reconcile opening`. No ingest, backfill, or web write path may create one.

**When to use which.** A dated adjustment answers *"this position drifted, and I noticed on this date."* An opening position answers *"the books began mid-story and the earlier chapters are gone."* Binance serves internal-transfer history for six months only; this ledger starts before that.

**Invariants:**

- Opening rows are dated at the ledger's start — `MIN(occurred_at)` across `transactions` — so they fall inside every reporting window.
- An opening balance (`kind='adjustment'`) is **strictly positive**. A gap that would require a negative one must instead be recorded as the movement it actually was: a `kind='transfer'` pair to the position the value moved to. If no counterpart is supplied, the command **refuses** rather than writing a negative opening balance.
- `source_ref` is **stable**: `opening:<account_id>:<currency>` for a balance, `opening-transfer:<from_id>:<to_id>:<currency>:<from|to>` for a transfer pair. Restating a position therefore *replaces* its opening rows — the ledger holds one opening position per `(account, currency)` however many times it is restated. This is the deliberate inverse of the uuid rule for dated adjustments.
- Transfer legs still obey rule-002: two rows, shared `transfer_id`, summing to zero, on different accounts.
- Like adjustments, opening rows are excluded from income and expense aggregates and included in balances — `kind IN ('adjustment','transfer')` is already outside `money.SQL_NOT_CURRENCY_MOVEMENT`, so no new exclusion is needed.

**The custodian figure is still an owner-supplied input**, exactly as for dated adjustments, and for the same reason.

**A restatement is not an audit trail.** Replacing prior opening rows discards what was previously stated, and `transaction_edits` does not record deletes. If the history of restatements ever matters it needs its own record.

## The viewer surface (ADR-018 amendment, 2026-09-03)

**Constraint:** `finances.web.services.reconcile_view.write_adjustment` is the
web viewer's only path to an adjustment, and it delegates the write to
`record_adjustment` unchanged. The constraint at the top of this file is
untouched: the domain function is still the only writer.

**Invariants:**

- **A preview precedes every write.** `POST
  /_partial/accounts/{id}/reconcile/preview` states the gap and lists, for the
  last 60 days on that account, the rows that could produce a false one:
  unpaired legs, same-day same-amount twins, uncategorised rows and rows
  priced `*_nearest`. It is scoped by account, deliberately **not** by
  currency — a USDC row is a way to misread a USDT gap.
- **A note is required from the viewer**, stored in `transactions.notes`, and
  optional from the CLI. It never enters `description`, which must stay
  machine-readable per the invariant above.
- **The figure reconciled is the position**, `position_balance(account,
  account.currency)` — never `v_account_balances`, which sums an account
  across currencies.
- **Dated now, in Caracas.** The viewer offers only the dated adjustment.
  Opening positions stay `finances reconcile opening`.
- **A plug stays visible.** Today carries `N adjustments · $X unexplained
  since <date>` (magnitudes, not net) for as long as any exist, and
  `finances doctor` reports `reconciliation_adjustments` as a warning.
