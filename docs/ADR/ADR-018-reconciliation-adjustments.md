# ADR-018: Reconciliation Adjustments for History the Custodian Will Not Return

**Date:** 2026-08-04
**Status:** Partially superseded by [ADR-020](./ADR-020-opening-positions.md) (2026-08-08)
**Superseded parts:** §2.1 (dating at reconciliation time), §4's rejection of
opening-balance rows, and the dating half of §2.3. The rest stands: §1.2's
distinction between an unrecoverable gap and an un-synced one, §2.2's exclusion
from income and expense, and §2.3's rule that the custodian figure is always an
owner-supplied input are all carried forward unchanged.

> **Why it was revisited.** Nine minutes before the first reconciliation ran, a
> deep `--since` re-sync duplicated 105 events. The plugs written here were
> sized against those corrupted balances, fit the corruption exactly, and left
> `finances doctor` reporting a healthy ledger that overstated income by
> 10,462.71 USDC. A residual absorbs whatever is wrong upstream without
> distinguishing missing history from a bug. See ADR-020 §1.2.
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

---

## Amendment, 2026-09-03 — the viewer surface, and its guard rails

**Status:** Accepted 2026-09-03. Owner request: *"'Set balance' on the
Accounts page (ADR-018 gets a surface) — let's make it happen."*

Everything above still holds. This amendment records what a *click* has to
carry that a CLI invocation carried for free, and it changes nothing about
what an adjustment means.

### A.1 Why the CLI was not enough

`finances reconcile balances` has existed since this ADR shipped and had
been used three times. It is the right shape and the wrong door: the owner
reads a balance in the Binance app and is looking at `/accounts`, not a
terminal, and the account whose figure disagrees is the card in front of
them. A surface that is one screen away from the moment of noticing is a
surface that does not get used.

### A.2 The preview is the feature; the write is the afterthought

The obvious build — a number field and a Save button — is the dangerous
one, and this project has the receipts. ADR-020 §1.2: three adjustments
sized against balances a duplicate sync had corrupted fit the corruption
exactly and left `finances doctor` reporting a healthy ledger that
overstated income by 10,462.71 USDC. Then again on 2026-09-03: ten Binance
Pay twins double-counting 2,260.72 USDT (ADR-022). **Both times the ledger
looked exactly like a ledger missing history.** A plug cannot tell the
difference, because absorbing the difference is the whole of what it does.

So `POST /_partial/accounts/{id}/reconcile/preview` answers with the gap
*and the case against writing it*: every row in the last 60 days on that
account that is

- **unpaired** — a `kind='transfer'` leg or a `p2p:` leg with no
  counterpart (rule-002),
- a **same-day, same-amount twin**,
- **uncategorised**, or
- **priced from a nearest rate** (ADR-021 `*_nearest`),

each one a link that opens its own modal, because the answer to "that is a
duplicate" is to delete it (ADR-022), not to plug around it. The window is
scoped to the account but **not** to the reconciled currency: Binance
Spot's USDC rows are exactly the kind of thing that gets mistaken for a
USDT gap, and `v_account_balances` folds them into the same figure.

### A.3 The ledger figure is the position, not the account balance

`v_account_balances` sums an account across currencies — Binance Spot's
"USDT" figure includes its USDC. An adjustment is written per
`(account, currency)`, so the control pre-fills, and the preview measures,
`reconciliation_adjustments.position_balance` instead. Where the two differ
the card says so in one line rather than showing two numbers that disagree
and explaining neither.

### A.4 A note is required in the viewer and optional in the domain

`record_adjustment` gains `note`, stored in `transactions.notes` — never in
`description`, which rule-012 requires to name both figures and the date in
a shape a machine can read.

The viewer refuses a blank one (422). The CLI does not require it: an
invocation leaves its reason in a shell history and a commit message, and a
click leaves it nowhere. Since `finances doctor` will list this row for as
long as it exists, a plug with no stated reason is sediment with a
timestamp.

### A.5 Dating is unchanged: today

The row is dated `datetime.now(CARACAS_TZ)`. Dating at the ledger's start
is ADR-020's opening-position claim — *"the books began mid-story"* — which
is a different assertion, has its own stable `source_ref`, refuses to go
negative, and stays a CLI act. The viewer offers only the dated
adjustment. §2.1's cost is accepted again: history before the
reconciliation date stays wrong, knowably, with a date attached.

### A.6 The plug does not disappear

Two surfaces, so that a residual can never quietly become the ledger:

- Today carries a line — *"N adjustments · $X unexplained since
  &lt;date&gt;"* — for as long as any exist, linking to the rows. `$X` is the
  sum of the plugs' **magnitudes**, not their net: two opposite plugs
  netting to zero are still two assertions. A plug in a currency the chain
  cannot price is counted, never valued at zero.
- `finances doctor` gains `reconciliation_adjustments`, a **warning**:
  writing one is legitimate, carrying one silently is not.
  `source='opening_balance'` rows are excluded — different claim.

### A.7 What was deliberately not built

**Reconciling a currency other than the account's own.** The control names
one position, the account's declared asset. A Binance USDC plug is a real
need and it needs a way to choose the asset; it is not this.

**A "reconcile everything" sweep.** Every plug is a separate assertion
about a separate position, and a button that writes five of them at once is
a button that gets pressed without reading five previews.

**Editing or reversing a plug from the viewer.** Reconciling the same
position again is already the supported path — the uuid `source_ref` exists
so that a second reconciliation is a second event — and ADR-022's delete
covers removing one outright.
