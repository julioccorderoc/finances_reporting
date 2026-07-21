# ADR-013: Bolívar Spending Is Valued at Realized Cost Basis, Not Spend-Day Market Rate

**Date:** 2026-07-21
**Status:** Accepted
**Amends:** [ADR-005](./ADR-005-rate-resolution-priority.md) — reverses its rejection of Option 2
**Rule:** [rule-005](../architecture/rules/rule-005-single-rate-resolver.md)

## 1. Context

ADR-005 built the resolver to answer: *what were these bolívars worth on the day I spent them?* The owner needs it to answer a different question: **how many dollars did this actually cost me?**

The two answers diverge whenever the VES rate moves between acquiring bolívars and spending them. Bolívars bought at 40 VES/USDT and spent a week later, when the market sits at 50, are currently valued at the spend-day rate — understating the real dollar cost by 20%. In a fast-moving currency this is not a rounding error; it is the difference between a ledger the owner can reason about and one they cannot.

ADR-005 considered this. Its Option 2 — "use the user's most recent realized rate from a P2P sell, applied to subsequent rows until the next sell" — was rejected on the grounds that it "spreads one realized rate across unrelated transactions, distorting them."

That reasoning was wrong about which question the ledger is for. The transactions are not unrelated: they are all draws against the same pool of bolívars, and that pool has a cost. Applying the acquisition rate is not distortion; it is cost-basis accounting. The genuine objection to Option 2 is narrower — a realized rate that has gone *stale* misprices spending — and that is addressed with an age cap rather than by discarding the model.

The data required already existed and was unused. Every Binance P2P fill is ingested with `user_rate = unitPrice`, the exact realized VES/USDT price of that trade (`finances/ingest/binance.py`). A P2P **sell** is precisely the moment bolívars are acquired.

## 2. Decision

Insert a **realized cost-basis tier** into the resolver, between the manual override and the market median. The chain becomes:

1. `transactions.user_rate` — manual override, unchanged
2. `rates(USDT, VES, source='binance_p2p_realized')` — **new**, subject to the age cap
3. `rates(USDT, VES, source='binance_p2p_median')` — unchanged
4. `rates(USD, VES, source='bcv')` — unchanged, fallback only
5. none → `needs_review = 1` — unchanged

Three modeling choices, decided by the owner:

- **Latest realized rate, carried forward.** Rejected: weighted-average pool, FIFO lots. Both are more precise when bolívars from several acquisitions are mixed, and both cost materially more to build and reason about than the question warrants. Revisit only if the ledger ever needs tax-grade disposal accounting.
- **Volume-weighted average across same-day sells.** Rejected: last-sell-of-day (one tiny trade could set the day's rate) and median (ignores size).
- **14-day maximum age, then fall through.** Rejected: no guard (a months-old rate would badly misprice spending) and flagging `needs_review` (more triage burden than the owner wants for a case the market tiers already handle). The boundary is inclusive: exactly 14 days old still applies.

**Derivation is materialized, not computed live.** `finances/domain/realized_rates.py` rolls P2P sells into one volume-weighted rate per day and upserts them into the existing `rates` table under `source='binance_p2p_realized'`.

Rejected alternatives: querying transactions live from inside the resolver (couples the resolver to the transactions table and recomputes the VWAP once per row), and a SQL view (cannot supply the `as_of_date` the age guard needs without a parallel lookup path).

Materializing means the resolver reuses `rates_repo.latest_on_or_before` verbatim — and that function already returns `Rate.as_of_date`, which is exactly the input the age guard requires. The realized rate also becomes inspectable data: the owner can see which rate is currently being carried.

The cost of materializing is derived data that can drift if a P2P transaction is later edited or deleted. That is bought off with `rebuild()`, called at the end of every Binance ingest and exposed as `finances rates rebuild-realized`.

## 3. Consequences (The "Why")

### Positive

- Bolívar spending is priced at what it cost, which is the question the ledger exists to answer.
- No data migration and no backfill of stored values. `amount_usd` is computed on every read, so correcting the rate logic re-values all history on the next report.
- The fiat gap is closed without a schema change (see §5).
- Reports needed no change: `is_bcv_fallback` keys off the `bcv` prefix, so the new source is headline-eligible automatically.

### Negative

- Realized rates are derived data and can drift from their source fills; `rebuild()` is the only thing keeping them honest, and it must stay wired into ingest.
- The 14-day cap is a judgement call, not a derived constant. It is a single module-level value (`REALIZED_MAX_AGE_DAYS`) so it can be retuned, but any change silently re-values history.
- A bolívar transaction's USD value now depends on *other* transactions (the P2P sells preceding it), where before it depended only on itself and the rate tables. Debugging one row may mean looking at the fills behind it — which is why the source label distinguishes `binance_p2p_realized` from `binance_p2p_realized_carry`.
- VES **income** is valued at cost basis too, not just expenses (§5).

## 4. Retiring `v_transactions_usd`

The `v_transactions_usd` view (migration 001) computed `amount_usd` inline in SQL with its own hardcoded chain. This was always a violation of rule-005 — it went unnoticed only because it happened to agree with the resolver.

It cannot express the realized tier, so it would now silently disagree with the resolver on exactly the transactions that matter most. Migration `014` drops it. The Python resolver is the sole USD authority.

Rejected: reproducing the chain in SQL (hand-syncing a VWAP and an age guard across two languages forever), and demoting the view to a renamed market reference (two conflicting USD answers in one database is the failure this ledger exists to end).

The invariant the view anchored — monthly totals equal view totals — is re-pointed at the consolidated report, so it now compares two independent Python report builders that share one rate implementation, rather than two competing rate implementations.

## 5. Known limitations

**Fiat is inferred, not stored.** `Transaction.currency` holds the *asset* (USDT); the fill's fiat appears only in the generated description. The derivation parses it from that deterministic text and skips any non-VES fill rather than folding it into the VES average. A real `fiat` column was rejected: adding a `transactions` column breaks every hand-listed `SELECT` feeding `_row_to_transaction`, which is disproportionate for a VES-only ledger. Revisit if a second fiat appears.

**VES income uses the cost-basis rate too.** The tier applies to any VES transaction without a manual `user_rate`, not just expenses. For income this reads as "USD value of bolívars received, at the prevailing cost basis." Accepted for consistency and simplicity, since the owner's income is overwhelmingly USD-side.

**Rates lose precision in storage.** The `rates.rate` column is declared `DECIMAL`, which carries NUMERIC affinity, so SQLite coerces the stored decimal text to a float64. This predates this ADR and affects every rate in the schema. Derivation and resolution stay exact in `Decimal`; only the round-trip quantizes, by roughly 1e-14 relative — immaterial at any amount this ledger will hold. Noted so it is not rediscovered as a bug.

## 6. Rule Extraction (The "How" for Agents)

**Target file:** `docs/architecture/rules/rule-005-single-rate-resolver.md`

**Injected constraint:** The locked priority order gains the realized tier at position 2, subject to `REALIZED_MAX_AGE_DAYS`. `binance_p2p_realized` is headline-eligible. No SQL view may compute `amount_usd`; `v_transactions_usd` is dropped and must not be recreated. All realized rates must be produced by `finances.domain.realized_rates`, never derived ad hoc.
