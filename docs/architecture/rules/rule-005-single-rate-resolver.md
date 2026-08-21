# Rule 005 — Single Rate Resolver

**Source ADR:** [ADR-005](../../ADR/ADR-005-rate-resolution-priority.md), extended by [ADR-013](../../ADR/ADR-013-realized-cost-basis.md), [ADR-016](../../ADR/ADR-016-p2p-median-max-age.md) and [ADR-021](../../ADR/ADR-021-nearest-rate-approximate-pricing.md)
**Scope:** All USD-equivalence calculations across the codebase.

**Constraint:** Every USD-equivalent value must be produced by `finances.domain.rates.resolve()`. **No SQL view may compute `amount_usd`.** `v_transactions_usd` was dropped in migration `014` for violating this and must not be recreated; a view needing USD must go through a Python-built materialization step backed by the resolver, never inline ad-hoc rate logic.

**Locked priority order:**

0. **Native-USD currencies short-circuit** (`money.NATIVE_USD_CURRENCIES` — USD/USDT/USDC) before any tier is consulted, returning `(Decimal(1), 'native_usd')` (ADR-021 §2.3). A native row's `user_rate` is *provenance* — the bolívar price a P2P fill was struck at — and must never be read as a conversion factor.
1. `transactions.user_rate` (the user's actual realized rate)
2. `rates(USDT, VES, occurred_date, source='binance_p2p_realized')` — **cost basis**: the volume-weighted rate at which the bolívars were actually acquired, carried forward from the most recent P2P sell. Applies only while no older than `REALIZED_MAX_AGE_DAYS` (14, inclusive); past that the chain falls through. Headline-eligible.
3. `rates(USDT, VES, occurred_date, source='binance_p2p_median')` — market rate; headline-eligible. Applies only while no older than `MEDIAN_MAX_AGE_DAYS` (14, inclusive, per ADR-016); past that the chain falls through. Carried far enough, a stale median converges on BCV and silently turns a market rate into a reference rate.
4. `rates(USD, VES, occurred_date, source='bcv')` — **fallback only**, and capped at `BCV_MAX_AGE_DAYS` (14, inclusive, per ADR-021). BCV is tracked for reference but is never the headline number.
5. **Nearest rate, any direction** — when no tier resolves inside its window, price from the closest row in `rates` (before *or* after the day), preferring the higher-priority source at equal distance, and report `'<source>_nearest'` (ADR-021 §2.2). `money.is_approximate()` is true for any `*_nearest`; `money.is_bcv_sourced()` still matches `bcv_nearest`, so a BCV-derived approximation stays out of every headline and out of net worth.
6. None at all → set `transactions.needs_review = 1`. This now means exactly one thing: **the rates table holds nothing for this pair.**

**Age caps live in one place.** `finances.domain.rates.max_age_days(source)` owns every tier's carry-forward bound — realized, median *and* BCV (ADR-021 §2.1). Any consumer that does its own `latest_on_or_before` lookup — notably `web/services/rates_view.rates_for_day`, which builds the triage rate panel — must apply that bound rather than keep a second copy. A panel that disagrees with the resolver about staleness is the exact defect ADR-016 fixes, and leaving the realized cap inline was the same defect one tier over.

**Pricing state is computed, not stored.** `transactions.needs_review` is what `apply_edit` writes and what `reports/needs_review` reads; it is **not** the definition of "this row has a rate problem". Any surface asking that question — triage above all — must ask the projection (`amount_usd is None`, or `money.is_approximate(rate_source)`), because the stored flag goes stale the moment a rate lands (ADR-021 §3).

**Approximation is carried, never inferred.** `rate_source` is the single carrier: no consumer may re-derive provenance from dates or amounts. `TransactionCard.approximate` and `ConsolidatedRow.is_approximate` are derived from it in one place each.

**Realized rates are derived, never ad hoc.** Only `finances.domain.realized_rates` may produce `binance_p2p_realized` rows. Its `rebuild()` must stay wired into Binance ingest, or the derived rates silently fall behind the fills they come from.

**Headline rule (per ADR-005 amendment 2026-04-19):** No BCV-sourced USD value may appear in headline reports (`finances report consolidated`, Sheets `Monthly` tab, weekly summary). If the resolver returns a BCV-sourced value for a row destined for a headline, the row must be flagged `needs_review = 1` and excluded from the headline aggregate (or shown with a clear "BCV fallback" annotation that the report renderer surfaces).

**Lint check:** `grep -rn "amount_usd\|usd_value\|to_usd" finances/ | grep -v "domain/rates.py\|reports/"` should return zero matches that contain inline arithmetic on rates.
