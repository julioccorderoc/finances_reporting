# ADR-021: Every Tier Expires, and an Expired Chain Prices From the Nearest Rate

**Date:** 2026-08-21
**Amended:** 2026-08-23 — §2.5 scopes the ladder to its own quote currency
**Status:** Accepted
**Amends:** [ADR-005](./ADR-005-rate-resolution-priority.md) — bounds tier 4, adds a terminal branch before `needs_review`
**Amends:** [ADR-016](./ADR-016-p2p-median-max-age.md) — extends its cap to the tiers it left out
**Related:** [ADR-013](./ADR-013-realized-cost-basis.md) (the age-cap pattern), [ADR-015](./ADR-015-user-rate-direction.md) (`user_rate` is quote units per dollar), [ADR-012](./ADR-012-local-web-viewer.md) (the triage surface this feeds)
**Rule:** [rule-005](../architecture/rules/rule-005-single-rate-resolver.md)

## 1. Context

Three defects in one chain, found while specifying the Triage redesign
(`design_handoff_triage/`, criteria D1–D6, K2).

### 1.1 The age-cap table describes one tier out of three

ADR-013 capped the realized tier at 14 days and ADR-016 capped the median at 14
days. Only the median cap reached `_TIER_MAX_AGE_DAYS`, the table
`max_age_days()` reads; the realized cap is enforced inline in `resolve()`, and
BCV has no cap at all.

`max_age_days()` exists precisely so a second surface cannot disagree with the
resolver about staleness — that is what ADR-016 §2.1 fixed for the median tier.
It is still wrong for the realized tier: `rates_view.rates_for_day` asks
`max_age_days("binance_p2p_realized")`, is told `None`, and prices the triage
modal's rate panel from a realized rate the resolver itself would refuse. The
same divergence class, one tier over.

### 1.2 BCV carries forward forever

ADR-016 left BCV uncapped deliberately: it is the floor of the chain, and
expiring it converts a stale-but-useful reference into triage work. That
reasoning was sound *while the alternative to BCV was `needs_review`*. §2.2
below removes that alternative, so the argument no longer holds — and an
uncapped tier is a promise that a rate from any distance is as good as one from
today, which is false in a currency that moved ~2.3% in a fortnight.

### 1.3 An unpriceable row is presented as a rate problem it is not

When no tier resolves, the chain returns `(None, "needs_review")` and the row
carries no dollar figure anywhere: not in the ledger, not in a report, not in
the queue. The Triage redesign's answer (criterion D4) is that a row should be
*priced anyway*, from the nearest rate the table holds, and clearly marked as an
approximation — because "$18.40, roughly" is a better ledger entry than a blank,
and because an approximate rate must not block a sitting (D6).

### 1.4 The resolver is currency-blind

`resolve()` never looks at `txn.currency`. It works today only because its one
canonical caller, `money.to_usd`, short-circuits native-USD currencies before
calling it. Every other caller is on its own — and there is one:
`transactions_write.apply_edit` runs the whole VES ladder against native-USD
rows just to derive `needs_review`.

That is currently harmless by luck. It stops being harmless the moment branch 1
fires on a native row, and 142 live USDT rows are exactly that shape: a P2P
fill's `user_rate` is the *bolívar price the fill was struck at*, recorded as
provenance, not a conversion factor for the USDT amount. Dividing 200 USDT by
165.40 would report $1.21 for a $200 row. The two unit bands in that column
(~36 and ~800) are a second reading of the same fact: it is not one unit at all.

## 2. Decision

### 2.1 One table, every tier, 14 days

`_TIER_MAX_AGE_DAYS` gains the realized cap and a BCV cap, both 14 days
inclusive. `resolve()` reads the realized bound from `max_age_days()` instead of
holding a second copy inline. One number governs the whole chain, and any
surface doing its own lookup gets the resolver's own answer by asking.

### 2.2 A terminal branch: nearest-rate approximate pricing

When no tier resolves inside its window, the chain does not give up. It takes
the **nearest** rate in the `rates` table — nearest in either direction, before
or after the transaction's day — and prices the row with it, reporting

    rate_source = "<source>_nearest"    e.g. bcv_nearest, binance_p2p_median_nearest

Ties in distance are broken toward the higher-priority source, so an equidistant
realized rate beats a median and a median beats BCV. The suffix is the whole
provenance: `money.is_approximate(source)` is true for any `*_nearest`, and
`money.is_bcv_sourced` still matches `bcv_nearest` by prefix, so a BCV-derived
approximation stays barred from net worth and from headline totals exactly as
`bcv` and `bcv_carry` are.

`needs_review` / unpriceable survives as the sixth branch and means one thing
now: **the rates table holds nothing at all for this pair.** A row with no
`amount_usd` is a row the ledger genuinely cannot price, not a row that fell off
the end of a window.

Direction matters and is not hidden. A rate from *after* the transaction is a
hindsight price, which is a different (and for a spend, a worse) claim than a
carried one. The resolution therefore reports the rate's own date and a signed
age — positive when the rate predates the row, negative when it postdates it —
so the UI can say "BCV, 3 days later" rather than "approximate".

The chain, in full:

1. `transactions.user_rate` — manual override, no age (**not** consulted for a
   native-USD row; see §2.3)
2. `rates(USDT, VES, 'binance_p2p_realized')` — cost basis, ≤14 days
3. `rates(USDT, VES, 'binance_p2p_median')` — market median, ≤14 days
4. `rates(USD, VES, 'bcv')` — reference floor, **≤14 days (this ADR)**
5. **nearest rate in the table, any direction, `<source>_nearest` (this ADR)**
6. none at all → `needs_review = 1`, no dollar figure

### 2.3 The currency guard moves into the resolver

`resolve()` short-circuits a native-USD currency (`money.NATIVE_USD_CURRENCIES`)
**before branch 1**, returning `(Decimal(1), "native_usd")` — a rate of one
quote unit per dollar, which is arithmetically what a dollar is, so a caller
that divides gets the right answer instead of a special case. `money.to_usd`
keeps its own short-circuit; it is now belt-and-braces rather than the only
belt.

This is what protects the 142 USDT rows carrying `user_rate` as provenance: the
guard sits above branch 1, so their recorded bolívar price can never be mistaken
for a conversion factor.

`rates.py` imports `NATIVE_USD_CURRENCIES` from `domain.money` inside the
function rather than at module scope, because `money` imports `rates`. The
alternative — a second literal set in `rates.py` — is the thing
`tests/test_money_is_the_only_definition.py` exists to forbid.

### 2.4 Approximation is reported, never silently absorbed

* `TransactionCard` gains `approximate: bool` and `rate: Decimal | None`. The
  field name `approximate` is a cross-surface contract; the UI keys its "priced
  roughly" group off it.
* `ConsolidatedRow` gains `is_approximate`. An approximate row is bucketed by
  the tier it came from — `bcv_nearest` is a BCV fallback, everything else is
  headline — because the axis "is this BCV" and the axis "is this an
  approximation" are independent facts and collapsing them would lose one.
  `--strict` violations now include approximate rows: strict mode exists to
  refuse leaning on a weak number, and a rate from outside every window is at
  least as weak as a same-day BCV print.
* `rates_view.rates_for_day` falls back to the nearest rate per tier when the
  backward lookup is empty or expired, marks it `is_approximate`, and prices it.
  A tier that has *nothing* stays unpriced. This is what feeds the design's "or
  take one of these" list (criterion D9), and it is why the nearest lookup lives
  in `rates_repo` rather than inside `resolve()` alone.
* `sheets_sync` needs no new column: it already exports `rate_source`, and the
  suffix is the provenance.

### 2.5 Scope: the ladder is a *bolívar* ladder (amendment 2026-08-23)

Every tier in §2.2 quotes in VES — `USDT/VES` twice, `USD/VES` once. Branches
1–5 therefore apply to **a row denominated in VES, and no other**. A non-native
row in any other currency resolves to branch 6, unpriceable: no rate, no
`amount_usd`, `needs_review` set. That is the same state an empty rates table
produces, and the triage surface already renders it (criterion D5).

§1.4 named the resolver currency-blind and §2.3 fixed the half of it that was
live — the native-USD guard protecting the 142 USDT rows. The other half stayed
open: nothing compared a tier's *quote* currency against `txn.currency`, so a
COP row was divided by a bolívar rate and reported as a confident dollar figure.
Inert on the live ledger (VES/USDT/USDC/USD only), and found while seeding a
genuinely unpriceable row for D5.

`user_rate` is inside the scope, not above it. Per ADR-015 it is *quote units
per dollar*, and the quote unit the ledger means by it is the bolívar; on a
currency the chain has no tier for, the number's unit is unverified. Branch 1 is
consulted only after the currency has a tier — the same reasoning that put the
native-USD guard above it.

The guard is structural. `LADDER_QUOTE_CURRENCIES` is derived from
`_FALLBACK_TIERS`, and `_tiers_for(currency)` narrows the list the in-window and
nearest branches walk. A guard spelled `== "VES"` would be a second copy of the
tier table, which is the defect class §2.1 and ADR-016 §2.1 both exist to close.

**Adding a currency means adding tiers, never borrowing one.** A new currency
needs its own rate pairs in `rates` and its own entries in `_FALLBACK_TIERS`
(with caps in `_TIER_MAX_AGE_DAYS`). Until it has them the ledger says "cannot
price this" — which is the honest answer, and a triage item, rather than a
plausible number nobody can trace.

## 3. Consequences

**Inert on the live ledger.** Measured 2026-08-21 against `finances.db`
(2,672 rows): the deepest carry any VES row needs is **6 days** on the realized
tier and 7 on BCV, no row reaches tier 3 or tier 4, and no row is unpriceable.
So the two new caps expire nothing, the nearest branch fires on nothing, and no
reported total moves. What changes today is the *modal's rate panel*, which
stops pricing from a realized rate older than the resolver's own bound.

The value of the change is forward-looking, and it is the same argument ADR-016
made: coverage gaps are normal (medians cannot be backfilled at all), and the
chain should degrade to a labelled approximation rather than to a blank row or
to a silently-stale number.

**Triage stops keying off the stored flag.** With pricing state computable,
`needs_review` is no longer the definition of a rate problem — the projection
is. The 25 live `needs_review = 1` rows that price fine produce no rate item
(criterion K2). No data is mutated to achieve that; the column is simply no
longer the question being asked. It remains what `apply_edit` writes and what
`reports/needs_review` reads.

**Bucket order follows the redesign.** Rate items are non-blocking (D6) and walk
last: bucket 0 category, 1 pair, 2 approximate. This contradicts acceptance
criterion A3, which lists category/rate/pair; the README's group order wins and
the deviation is logged in `design_handoff_triage/NOTES.md`.

**A nearest lookup is a new repo primitive.** `rates_repo.nearest()` scans both
directions for one `(base, quote, source)`. It is deliberately not a second
chain: it answers "what is the closest row" for one tier, and the ordering
across tiers stays in `resolve()`.

**The currency scope moves no live number either.** The ledger holds four
currencies — VES, USDT, USDC, USD. Three are native-USD and short-circuit above
the ladder; the fourth *is* the ladder's quote currency. So §2.5 changes the
resolution of exactly zero live rows. What it changes is what happens the first
time a fifth currency arrives: a triage item instead of a plausible number.

**What this does not do.** It does not backfill missing rates, and it does not
make an approximation trustworthy. `bcv_nearest` is still barred from net worth
and from the headline; a `binance_p2p_median_nearest` figure still says
"roughly" on the row. The point is that the ledger stops going blank.
