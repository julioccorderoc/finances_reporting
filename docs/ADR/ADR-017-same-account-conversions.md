# ADR-017: A Transfer Moves Value Between Positions, Not Between Accounts

**Date:** 2026-08-03
**Status:** Accepted
**Amends:** [ADR-002](./ADR-002-double-entry-transfers.md) — redefines what the two legs of a transfer must differ in
**Rule:** [rule-002](../architecture/rules/rule-002-transfers-must-be-paired.md)
**Implemented by:** `2b9878c` (the rule and the order-id pairing), `e1af276` (the legacy tier and the doctor exemption)

> **Note on provenance.** The decision below was taken and implemented in
> `2b9878c` before this record was written — two sessions reached it
> independently on the same day, which is itself the argument for writing the
> ADR. This document is the missing decision record for code already on
> `main`, plus the two extensions in `e1af276`. Where the two attempts
> differed, §2.3 says so explicitly.

## 1. Context

A Binance USDC→USDT conversion writes two rows, both on Binance Spot:

```text
convert:<order>:from   expense   −1 240.00 USDC
convert:<order>:to     income    +1 239.18 USDT
```

Reports exclude `kind = 'transfer'` and nothing else. These rows are `expense`
and `income`, so both are counted. Across the ledger as of 2026-08-03:

| | rows | total |
|---|---|---|
| convert expense | 12 | −11 846.42 USDC |
| convert income | 12 | +11 844.61 USDT |
| **net** | | **−1.82** |

The net is right. The **gross is wrong**: ~11.8k of phantom expense against
~11.8k of phantom income, distorting every monthly income and expense figure
while the bottom line survives. In July 2026, two conversions on one day put
$1 753.84 of expense and $1 754.74 of income into a month whose real Binance
Spot activity was neither.

The obvious fix — make it a transfer — was refused by `create_transfer`:

```python
raise ValueError("both-anchors legs must be on different accounts")
```

That guard was not a bug. It faithfully implemented ADR-002, which states the
model as *"each transfer is two linked transactions … one negative on the
source account, one positive on the destination."* A conversion has no
destination account. It happens inside one. **The model could not express
it**, and that is a modelling gap, not a defect in the code enforcing the
model.

### 1.1 What the invariant is actually for

ADR-002's phrasing conflates two different claims:

1. **The one reports depend on** — *this money moved; it was not earned or
   spent*. That is what `WHERE kind <> 'transfer'` buys.
2. **The one ADR-002 wrote down** — *the two legs sit on different accounts*.

(2) was a proxy for (1), chosen because in April 2026 every known movement
happened to cross an account boundary. Conversions break the proxy without
touching the principle: a USDC→USDT convert unambiguously *is* movement, and
unambiguously is *not* between accounts.

The degenerate case the guard genuinely protects against is narrower than the
guard itself: **two legs on one account in one currency**, which nets to zero
and therefore records a movement that did not happen. Different currencies on
one account do not have that property. Binance Spot holds a USDC balance and a
USDT balance; moving value from one to the other changes both, and both must
change for the account's balances to stay correct.

### 1.2 Two conversions were recorded unlinked

Four rows — 891, 892, 910, 911, two conversions from November 2025 — arrived
one leg per legacy sheet row. Each leg hashed to its own `source_ref`, so the
halves never shared an order id:

```text
891  2025-11-23  income   +1 239.18 USDT  convert:hash:c5d9a3f91fe8725e:to
892  2025-11-23  expense  −1 240.00 USDC  convert:hash:d32d5d23f33ee0cd:from
910  2025-11-30  expense  −1 280.00 USDC  convert:hash:e793a7db2eabbe1b:from
911  2025-11-30  income   +1 278.77 USDT  convert:hash:9a3c752419f66eb0:to
```

Both halves exist and the money is accounted for; only the linkage is absent.

## 2. Decision

**A transfer moves value between two positions, where a position is
`(account_id, currency)`.**

Two legs of a transfer may share an account **if and only if their currencies
differ**. Same account *and* same currency remains an error — it is the
degenerate case §1.1 identifies, and it stays rejected at write time and
flagged by `finances doctor`.

Nothing else in rule-002 changes. A transfer is still exactly two rows, still
shares one `transfer_id`, is still signed, and still sums to zero (after USD
conversion for cross-currency pairs). What does change is rule-002's
sole-writer clause, which this ADR narrows — see §2.4.

### 2.1 Why this model and not a new kind

The decisive property is that **no report changes**. Income and expense are
excluded in four places —

- `finances/db/migrations/001_initial.sql` (`v_monthly_summary`)
- `finances/reports/monthly.py`
- `finances/reports/consolidated_usd.py`
- `finances/domain/integrity.py`

— and every one already excludes `kind = 'transfer'`. A conversion promoted to
`kind='transfer'` is excluded by all four the moment it is written, with no
edit to any of them, and therefore with no way for them to drift apart. Every
alternative in §4 requires teaching a second exclusion to all four sites plus
the web filter surfaces, and each site is an opportunity to miss one and
produce two reports that disagree.

Balances are equally unaffected: they are `SUM(amount) GROUP BY account_id`
and include transfers by design (ADR-002 §3), so the USDC leg and the USDT leg
both land on Binance Spot and each currency's balance stays correct.

### 2.2 Pairing conversions: a reconciliation strategy

Linking the legs is a reconciliation problem, and ADR-002's 2026-04-19
amendment already established the pattern for those. `create_transfer` stays
the writer; `SameAccountConvertPairing` decides what to pair, and runs as a
post-pass in `finances/ingest/binance.py` alongside the realized-basis
rebuild — mirroring how `finances/ingest/provincial.py` triggers
`BankAnchoredP2pPairing`.

It matches in two tiers:

- **By order id.** `convert:<order>:from` and `convert:<order>:to` strip to
  one key. An exact identity, so there is nothing to guess, and it covers
  every conversion the live API has produced.
- **By same-day shape.** For the legacy halves of §1.2, whose keys can never
  match: same account, same calendar day, opposite sign, **different
  currency**, and amounts within 2%. Tightest fit claimed first, each leg
  consumed once.

Eligibility is `transfer_id IS NULL`, so the pass is idempotent: re-running it
is a no-op, and a conversion ingested next month pairs itself.

**No migration is required.** A data migration would fix today's 24 rows and
leave next month's conversion broken again; the strategy fixes both, and does
arithmetic and UUID generation that a SQL migration here cannot. `finances
reconcile converts` runs the same pass on demand, so history can be repaired
without an API round-trip; `--dry-run` rolls back.

### 2.3 Why the legacy halves are paired rather than left to the owner

The first implementation stopped after the order-id tier, on the reasoning
that legs with no shared id are genuinely ambiguous and "only the owner can
say which orphan belongs to which."

That is the right instinct — it is exactly the reasoning ADR-002's 2026-07-26
amendment applied to P2P — but it concedes more than this shape requires. A
P2P deposit and a P2P sell are ambiguous because Provincial statements carry
no transaction id and round amounts collide: three 20 000 Bs deposits against
three 20 000 Bs sells admit no correct assignment. A conversion is not that.
Requiring *all five* of one account, one calendar day, opposite sign, two
different currencies, and agreement within 2% leaves no room for a second
reading; on the live ledger each of the two dates holds exactly one qualifying
leg on each side.

Where candidates do compete, the pass claims the tightest fit and consumes
each leg once. That asserts only that N outgoing legs consumed N incoming
ones — **not** that any individual pair is the true counterparty. Anyone
auditing a specific legacy transfer must treat the linkage as an accounting
convenience, exactly as ADR-002 says for P2P. Anything that fails any of the
five conditions stays unpaired and keeps surfacing in `finances doctor`.

### 2.4 The ingest writes fresh conversions already paired

rule-002 said `create_transfer` was the only code path permitted to insert a
`kind='transfer'` row. That is no longer true, and the exception is deliberate
rather than an oversight, so it is recorded here.

`RawBinanceConvertRow.to_transactions` in `finances/ingest/binance.py` emits
both legs as `kind='transfer'` sharing `transfer_id = "convert:<orderId>"`, and
they are written through `transactions_repo.upsert_by_source_ref` like every
other Binance row. A synced conversion is therefore correct the moment it
lands; §2.2's pass finds nothing to do for it, because eligibility is
`transfer_id IS NULL`.

The alternative — insert both legs, then call `create_transfer` in
both-anchors mode to promote them — was rejected. The ingest's idempotency
comes from upserting on `(source, source_ref)` (rule-010), which is what makes
re-ingesting a day a no-op. `create_transfer`'s both-anchors mode requires two
rows to already exist and has no upsert semantics, so routing through it would
mean writing each leg twice by two different rules and reconciling them. That
buys nothing: the pair the ingest writes is already well-formed and already
shares one `transfer_id`.

The invariant rule-002 actually protects does not depend on a single writer.
Well-formedness is enforced after the fact by `finances doctor` —
`transfer_leg_count`, `transfer_legs_same_account`,
`transfer_same_currency_imbalance` — which judges the rows regardless of who
wrote them. `create_transfer` remains the sole path that *pairs two existing
rows*, which is the case where a guard is genuinely load-bearing, and it is
still the only writer used by every reconciliation strategy, the web triage
pairing, and the backfill.

### 2.5 The conversion spread leaves the P&L

A conversion's two legs do not cancel exactly — Binance's quote embeds a
spread. Previously that residue was counted as income or expense; under this
ADR it is movement, so it leaves the income/expense figures entirely while
remaining fully visible in balances.

It is small and signed both ways: −$1.82 across the ledger's whole history,
but +$0.91 in July 2026 alone. Booking it as a separate fee or gain row was
considered and rejected: it would break the two-legs-per-transfer invariant
that every integrity check depends on, and it would add 12 rows of noise to
account for less than two dollars. A spread is a cost baked into an exchange
rate, not a transaction the owner made.

## 3. Consequences

**The change is confined to the domain, ingest and CLI layers. No report is
touched.**

| File | Change |
|---|---|
| `domain/transfers.py` — both-anchors guard | reject same-account only when the two legs' currencies match |
| `domain/transfers.py` — `validate()` | same relaxation as the guard: same account is a defect only when the currencies match too |
| `domain/transfers.py` — new strategy | `SameAccountConvertPairing`, two tiers (§2.2) |
| `domain/integrity.py` — `transfer_legs_same_account` | add `COUNT(DISTINCT currency) = 1` so it condemns only the degenerate case |
| `domain/integrity.py` — `convert_leg_without_counterpart` | exclude legs that already carry a `transfer_id` (§3.1) |
| `ingest/binance.py` | write a synced conversion's legs already paired (§2.4), and run the strategy as a post-pass |
| `cli/main.py` | `finances reconcile converts`, so existing rows can be repaired without an API call |

`create_transfer`'s *fresh* and *anchor-only* modes keep rejecting
same-account outright. A conversion always presents two real rows to pair, so
it never needs those modes, and leaving them strict keeps the relaxation as
narrow as the problem.

**`validate()` needed the guard relaxed, but no new arithmetic.** It shipped
still rejecting any pair whose legs shared an account, which made every
conversion fail the ledger's own well-formedness check; that check now matches
`create_transfer`. The arithmetic below it was always correct and never got to
run. Its cross-currency arm converts each
leg to USD via `_to_usd`, which returns USD-equivalent currencies unchanged
without consulting a rate (ADR-015). A −1 240 USDC / +1 239.18 USDT pair
resolves to 0.066% relative drift, well inside the existing tolerance, and
convert legs' absent `user_rate` never comes into play.

**`transfer_same_currency_imbalance` is unaffected.** It is already scoped to
pairs with one distinct currency, and a conversion has two.

### 3.1 `convert_leg_without_counterpart` had to be narrowed too

That check recovers an order id by stripping `:from` / `:to` from
`source_ref`, and pairing deliberately does not rewrite `source_ref` — it is
the dedup key (rule-010). So the two legacy conversions stayed listed after
being correctly paired: their hashed keys never match and never will.

The check now excludes legs carrying a `transfer_id`, which restores what it
actually asks — *does this lone leg read as money spent?* A paired leg does
not, because reports exclude it by kind. An unpaired lone leg is still
reported, and the description now names the command that resolves one.

### 3.2 Verified against the live ledger

Run against a copy, then applied to `finances.db` with a backup at
`finances.db.bak-adr017`:

- **12 pairs** from 24 legs, none left unpaired, every pair two legs on one
  account in two currencies.
- `finances doctor`: **0 errors, 2 warnings → 0 errors, 1 warning.**
  `unpaired_p2p_sells` is a separate backlog (missing bank statements) and is
  untouched by this ADR.
- **July 2026:** Binance Spot gross expense −2 706.93 → −953.09; gross income
  +1 754.74 → 0.00; headline `grand_total_usd` 313.1776 → 312.2697.
- **Per-account, per-currency balances byte-identical**, confirmed by
  differencing `SUM(amount) GROUP BY account_id, currency` across the whole
  ledger before and after. Promotion changes `kind` and `transfer_id`; it
  never touches an amount.

### 3.3 What this does not cover

The order-id tier trusts the shared id and does not check drift, so a
conversion into a non-USD-equivalent asset (say USDT→BTC) pairs correctly but
cannot be priced by `validate()`, which has no rate for it — the same gap a
P2P leg without a `user_rate` has. No such row exists in the ledger today.
Adding a rate lookup inside the strategy would duplicate the resolver, which
rule-005 forbids.

The category-based movement predicate in `domain/money.py` remains useful and
is **not** superseded. It covers movement this ADR cannot model — a P2P sell
whose bank statement was never imported, a USDT send swapped for physical
cash — because those have no second leg to pair with. For conversions it
becomes redundant rather than wrong: a promoted convert leg is already
excluded by `kind`. The two mechanisms are complementary; this one removes the
reliance on the owner remembering to categorise each conversion by hand.

## 4. Rejected alternatives

**A new `kind='conversion'`.** Explicit and self-documenting, and the most
tempting. Rejected on cost and risk: SQLite cannot `ALTER` a `CHECK`
constraint, so the migration would have to rebuild the entire `transactions`
table — copy, drop, rename — with its foreign keys, indexes and dependent
views, on the ledger's central table, to accommodate 24 rows. The new kind
would then have to be excluded in the four report sites *plus*
`web/routers/_tx_filter_dep.py`, `web/routers/pages.py`,
`web/services/dashboard.py` and `web/services/triage.py`. Every one of those is
a chance to miss one, and those sites had already been found disagreeing with
each other once.

**Reuse `kind='adjustment'`.** It is already permitted by the CHECK constraint
and has zero rows, so it needs no migration. Rejected on meaning: in
accounting an adjustment is a correction or a write-off, an assertion that a
previous record was wrong. A conversion is a real event that happened exactly
as recorded. The ledger would misdescribe it, and the one spare kind would be
spent on the wrong concept. It also inherits the identical multi-site
exclusion problem as a new kind, so it saves the migration and nothing else.

**Per-asset sub-accounts** (*Binance Spot USDC*, *Binance Spot USDT*). The
most textbook-correct double entry: a conversion becomes an ordinary transfer
between two real accounts and this ADR would be unnecessary. Rejected on blast
radius. It changes the account model itself, needs a migration remapping
existing rows onto new account ids, and touches every account-keyed surface —
balances, net worth, dashboard grouping, `accounts_view`, the P2P pairing
anchor, `earn_positions`, and Binance ingest, which would have to select an
account per asset. It also multiplies accounts every time a new asset is
touched. That is a large, irreversible change to the foundation in exchange
for solving a problem §2 solves in a handful of edits. Should the ledger ever
need per-asset cost bases or multi-asset reporting, this remains the right
answer and this ADR does not foreclose it: positions are already the unit the
model reasons about, so sub-accounts would be a refinement of §2 rather than a
reversal.

## 5. Rule extraction

**Target file:** `docs/architecture/rules/rule-002-transfers-must-be-paired.md`

**Injected constraint:** The two legs sharing a `transfer_id` must differ in
**position** — `(account_id, currency)` — not necessarily in `account_id`.
Legs on the same account are valid when their currencies differ and invalid
when they do not. `domain.transfers.create_transfer` remains the sole path
that *pairs* two existing rows into a transfer; the Binance convert ingest
additionally writes its two legs already paired (§2.4). `finances doctor`
enforces the narrowed rule via `transfer_legs_same_account`.
