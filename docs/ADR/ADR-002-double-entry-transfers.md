# ADR-002: Double-Entry Model for Transfers Between Own Accounts

**Date:** 2026-04-19
**Status:** Accepted

## 1. Context

Today, money moving between the user's own accounts (Binance Funding ↔ Spot, Binance → P2P sell → Bank, Bank → ATM cash) is recorded inconsistently:

- Binance internal transfers appear as paired rows on the Binance side but never on the bank side.
- P2P sells decrease Binance USDT but the corresponding bolívar deposit is missing from the bank export.
- Cash withdrawals vanish entirely.

This causes two distinct failure modes: (a) account balances do not match reality, and (b) income/expense aggregations double-count or miss flows that are pure movement.

Two modeling options:

1. **Double-entry**: each transfer is two linked transactions sharing a `transfer_id` — one negative on the source account, one positive on the destination.
2. **Single-row**: one transaction with `from_account` and `to_account` columns.

## 2. Decision

Use **double-entry**. A transfer is two rows in `transactions`, each on its own account, summing to zero in their respective USD-equivalents (within tolerance), grouped by a UUID `transfer_id` column. Both rows have `kind='transfer'`. Reports filter `kind <> 'transfer'` for income/expense aggregations and use the per-account sum (which includes transfers) for balances.

## 3. Consequences (The "Why")

### Positive

- Per-account balances are a trivial `SUM(amount) GROUP BY account_id` and naturally include transfers in/out.
- Income/expense aggregations exclude transfers cleanly with a single `WHERE kind <> 'transfer'`.
- Each transfer leg can carry its own `user_rate`, capturing the realized rate of a P2P sell on the receiving side independently from the sending side.
- Schema stays uniform — one `transactions` table, no special-case columns.
- Reconciliation gap detection is trivial: `transfer_id` rows that do not have a sibling are findable via `v_unreconciled_transfers`.

### Negative

- Every transfer requires inserting two rows transactionally; `domain.transfers.create_transfer` must be the only path that creates them.
- Backfill must detect implicit transfers in legacy CSVs (e.g. P2P sell without a paired bank deposit) and either generate the missing leg or flag it for review.
- Beginners reading the table see "duplicate" rows; documentation must explain the model.

## 4. Rule Extraction (The "How" for Agents)

**Target File:** `docs/architecture/rules/rule-002-transfers-must-be-paired.md`
**Injected Constraint:** Any insert with `kind='transfer'` must originate from `domain.transfers.create_transfer`, which atomically writes both legs and shares a non-null `transfer_id`. Direct inserts of `kind='transfer'` rows from any other module are forbidden. CI/test must assert `SELECT COUNT(*) FROM transactions WHERE kind='transfer' AND transfer_id IS NULL = 0`.

---

## Amendment 2026-04-19 — Provincial Bank Is the Pairing Anchor

**Context:** The Provincial bank statement is the ledger with ultimate canonical authority for bolívar flows; Binance P2P sells produce inflows that *must* match a Provincial deposit on or near the same date. Anchoring on Binance and looking outward to the bank produces stale pairs when the bank statement is delayed; anchoring on the bank produces correct pairs because the bank row is the ground truth that money actually arrived.

**Amendment:** When pairing a P2P sell to its bolívar receipt, the **Provincial deposit row is the anchor**. The pairing algorithm runs as part of `finances/ingest/provincial.py` (or as a post-pass in `finances/migration/backfill.py`) and walks unpaired Provincial deposits matching the shape of a P2P inflow (large amount, source description matching known counterparties or "transfer recibido"), then searches Binance P2P sell rows within a configurable date window (default ±2 days) for a match by amount × rate. On match, `domain.transfers.create_transfer` writes the pair with the Binance leg pointing at the bank-side leg as the canonical receipt.

**Rule update:** `docs/architecture/rules/rule-002-transfers-must-be-paired.md` is updated to require that, for P2P-derived transfer pairs, the Provincial leg is created/identified first and the Binance leg is paired to it — not vice versa.

---

## Amendment 2026-04-19 — Reconciliation Passes as a General Pattern

**Context:** The bank-anchored P2P pairing routine is the first instance of a broader pattern: a *reconciliation pass* that walks unpaired/unmatched rows on one side, finds candidate matches on the other, and links them. Future features (e.g. receipt↔transaction matching from a mobile app, see EPIC-017+) will use the same shape. The v1 module should expose a generic interface so future strategies plug in without touching the existing one.

**Amendment:** EPIC-006's deliverable is renamed from "transfer pairing engine" to **"reconciliation engine"**. Its public surface is:

- `run_reconciliation_pass(strategy)` — generic entry point.
- `BankAnchoredP2pPairing` — the v1 strategy implementation. Uses `domain.transfers.create_transfer` to write paired rows.
- Future strategies (e.g. `ReceiptToTransactionMatch`) implement the same protocol and are registered without modifying the existing strategy.

`domain.transfers.create_transfer` and the `transfer_id` invariant (rule-002) remain the only path for `kind='transfer'` inserts. Receipt-style reconciliation (which links rows but does not create transfers) will use a different linking column (e.g. `receipt_id`) introduced via a future forward migration; it does not affect the transfer rules.

---

## Amendment 2026-07-26 — Pairing Optimises Totals, Not Identity

**Context:** The 2026-04-19 amendment left the pairing strategy with an
exactly-one uniqueness gate: a Provincial deposit was paired only when
precisely one Binance sell survived the amount tolerance, and ambiguity was
skipped rather than guessed. In production that gate almost never fires.
Sales settle in round amounts — 20 000 Bs is the habitual size — so a deposit
routinely sees two or three equally-good candidates and pairs with none of
them. Thirteen P2P sells were stranded this way, each still carrying
`kind='expense'`, inflating reported spending by money that was never spent.

The gate was protecting a property the data cannot supply. **Provincial
statements carry a date and a free-text reference, but no transaction id.**
There is no field, on either side, that could confirm a particular deposit
came from a particular sell. Given three 20 000 Bs deposits and three
~20 000 Bs sells on one day, "which pairs with which" has no discoverable
answer — only the totals are knowable, and every assignment yields identical
balances, identical account sums, and an identical `kind='transfer'`
population.

Refusing to decide therefore bought no accuracy. It only cost coverage.

**Amendment:** `BankAnchoredP2pPairing` performs a **global greedy
assignment** instead of a per-deposit uniqueness test.

1. Score every eligible (deposit, sell) pair: same calendar day ±`window_days`,
   bolívar-equivalents within `amount_tolerance_ratio`.
2. Sort by closest date, then tightest drift, then `(sell id, bank id)`.
3. Claim down that list, consuming each deposit and each sell at most once.

Whatever is left over stays unpaired and surfaces in triage — that leftover
is the honest signal ("a deposit arrived that no sell explains", or the
reverse), and it is what the human should be looking at.

The tie-break tail on ids makes the assignment total-ordered, so identical
inputs always produce an identical result. The pass stays idempotent.

**Window narrowed to ±1 day (was ±2).** Bank rows have date granularity only,
so hour-level comparison was false precision; dates are now compared by
calendar day. The ±1 band exists to absorb the timezone seam between
Caracas-stamped bank rows and UTC-stamped Binance rows, not to permit genuinely
distant matches. Every pairing in the production ledger resolves same-day, and
±0/±1/±2 produce byte-identical results against it.

**Denomination guard.** `user_rate` stores a bare number with no unit, so a
sell priced at 1.003 **USD** converts to ~1010 and matches a 1010 **Bs**
deposit inside tolerance. The fiat survives only in the description written by
`finances/ingest/binance.py` (`P2P SELL USDT @ <rate> <FIAT> (order <id>)`).
The strategy now parses it back and refuses a pairing whose fiat is known to
differ from the bank leg's currency. A description that does not carry the
shape at all is allowed through, so legacy and backfilled rows keep their
current behaviour.

**What is deliberately not claimed:** that any individual pair is the true
counterparty. The strategy asserts only that N sells consumed N deposits.
Anyone auditing a specific transfer must treat the linkage as an accounting
convenience, not evidence.

**Unchanged:** `create_transfer` remains the sole writer of `kind='transfer'`
(rule-002), the Provincial leg remains the anchor, and each pair is still two
rows sharing one `transfer_id`.

## Amendment 2026-09-04 — The Pairer Reads a Trade Both Ways

**Status:** Accepted (owner decision 2026-09-04)

`BankAnchoredP2pPairing` only ever matched a bank *deposit* against a Binance
*sell*. A P2P **buy** is the same movement of money read backwards — bolívars
leave the bank, USDT arrives — and nothing paired it: not this strategy, and
not the manual picker, whose modal gated on `amount < 0`. The two `P2P BUY
USDT @` rows in the ledger were therefore unpairable by any path, each reading
as income the owner never earned.

**The buy direction is now matched too**: an unpaired bank debit against an
unpaired Binance credit carrying a `user_rate`, same ±`window_days`, same
tolerance, the same denomination guard. The scoring compares magnitudes, so
one implementation serves both readings; the caller decides the direction by
which two row sets it hands in.

**One assignment, not two passes.** Both directions are scored, then claimed
from a single greedy sweep with shared claimed-sets, so a row belongs to at
most one pair whichever way the money went. Signs keep the directions apart
without extra rules: `create_transfer` refuses a same-sign pair, so a deposit
is never scored against a buy.

**Proposals name their direction** (`details["direction"]`), because a
reconciliation report that cannot tell a buy from a sell reads as if the pairer
did the same thing twice.

**The manual picker follows.** The modal opens it on incoming Binance rows as
well, but only where a `user_rate` exists to score against — an ordinary USDC
deposit is money arriving, not a trade against the bank — and the label names
the side of the bank it is looking for.

**Unchanged:** the bank leg is still the anchor, `create_transfer` is still the
sole writer of `kind='transfer'`, and the strategy still claims only that N
trades consumed N bank rows.
