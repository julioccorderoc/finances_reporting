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

## Amendment — 2026-07-26: a web rate edit rebuilds the basis in the same request

**Status:** Accepted. Extends §2's `rebuild()` clause to the web write
path. Nothing in the resolver chain, the age cap, or §5 changes.

**Context.** §2 accepts materialisation and buys off the resulting drift
with `rebuild()`, "called at the end of every Binance ingest and exposed
as `finances rates rebuild-realized`." That enumeration was complete when
it was written. The ADR-012 viewer's write path landed independently and
never joined the bargain: nothing under `finances/web/` calls `rebuild()`.

`SQL_P2P_SELLS` selects on `source_ref LIKE 'p2p:%' AND amount < 0 AND
user_rate IS NOT NULL`, so saving a `user_rate` through the triage modal
edits an *input* to the materialised tier without recomputing it. The
edit is invisible in the basis until the next Binance ingest, at which
point every bolívar row re-prices at once — attributable to an ingest
that changed nothing rather than to the edit that did. At present: 1172
VES rows priced off this basis, 123 qualifying fills, 3 P2P sells still
missing a `user_rate`, 20 P2P sells reachable from the triage queue.

This is not one of the drifts §5 accepts. §5 documents inference and
precision limits; this is the maintenance call the design depends on
simply not being wired up.

**Decision.**

1. **Rebuild synchronously, inside the write.** A web edit that changes
   `user_rate` on a row matching `SQL_P2P_SELLS` calls
   `realized_rates.rebuild()` in the same request, before the response is
   composed. Measured at **5.1 ms** against the live ledger (123 fills →
   101 daily rates) — under a third of the 16 ms `build_queue` that the
   same triage save already pays twice (ADR-012 Amendment 2026-07-26).

   Rejected: deferring to the end of a triage run (covers only the triage
   surface, and a browser closed mid-run leaves the basis silently stale
   — the very failure being fixed), and a manual "basis is stale" banner
   (makes correctness opt-in, and needs drift detection the sync path
   makes unnecessary).

2. **The hook lives in `apply_edit`, not in the triage route.** That
   service function is the single choke point for every web `user_rate`
   write — the triage modal, the `/transactions` modal, and
   `PATCH /api/transactions/{id}`. Bulk edit is category-only. A stale
   basis is not triage-specific: editing a fill's rate from the
   transactions list has the identical effect on all 1172 VES rows.

3. **Rebuild runs BEFORE the post-write `rates.resolve`.** `apply_edit`
   derives `needs_review` from a resolve that runs after the update, and
   the invariant is that nothing in the response is derived from a basis
   the same request already invalidated.

   This ordering is defensive, not load-bearing: every P2P fill in the
   ledger is denominated in USDT (43 `expense` + 83 `transfer`, all
   `needs_review=0`), so the edited row itself always takes the
   `native_usd` path and its own `needs_review` cannot depend on the
   realized tier. The ordering costs nothing and holds the invariant for
   a fill that is one day not USDT-denominated.

4. **The trigger is narrow, and symmetric.** Rebuild fires only when the
   request set `user_rate`, the post-write row satisfies the
   `SQL_P2P_SELLS` shape (`source_ref LIKE 'p2p:%'`, negative amount),
   and the value actually changed. Clearing a rate to `NULL` counts: it
   removes a fill from the average and must re-derive the day. Category-
   or notes-only saves never rebuild, and neither does re-saving the same
   rate.

5. **Rebuild stays whole-history.** No incremental "just this day" path.
   `rebuild()` upserts all days idempotently, and a second derivation
   would be exactly the ad-hoc realized rate rule-005 forbids.

**Consequences.** One modal save can re-price every VES row from that
fill's day forward. That blast radius is not new — it is what the next
ingest would have applied — but it now lands at the moment the owner
caused it, where it is attributable. The rebuild is not wrapped in an
explicit transaction: the connection runs in autocommit and `apply_edit`
already issues several statements unwrapped, so a failure part-way
through leaves the fill saved and the basis partly upserted. Both are
recoverable by re-running, since `rebuild()` is idempotent, or by
`finances rates rebuild-realized`. Any future non-ingest CLI path that
writes `user_rate` must make the same call; there is none today.

**Follow-up, same day: `rebuild()` now prunes.** The clause above
originally recorded an inherited limit and deferred it — `rebuild()`
upserted and never deleted, so a day whose last qualifying fill went away
kept the row it was last given. Deferring it was wrong: §2 places the
realized tier *above* `binance_p2p_median`, so a stale row does not
linger harmlessly, it keeps winning the chain over the market rate that
has become the honest answer. Making a web edit rebuild on the spot would
otherwise have produced exactly that outcome the first time an owner
cleared a rate.

`rebuild()` therefore materialises a set that mirrors the derivation:
after upserting the computed days it deletes every other
`(USDT, VES, binance_p2p_realized)` row, via
`rates_repo.delete_source_except`. Pruning is scoped to that one triple,
so the ingester-owned tiers are untouched, and an empty derivation
correctly clears the source. Everything involved is derived from
`transactions`, so a wrong prune is always recoverable by re-running.
Both callers named in §2 — the end of a Binance ingest and
`finances rates rebuild-realized` — inherit the behaviour. Verified as a
no-op against the live ledger: 101 realized rows in, 101 out, other
sources unchanged.
