# Rule 005 — Single Rate Resolver

**Source ADR:** [ADR-005](../../ADR/ADR-005-rate-resolution-priority.md), extended by [ADR-013](../../ADR/ADR-013-realized-cost-basis.md) and [ADR-015](../../ADR/ADR-015-p2p-median-max-age.md)
**Scope:** All USD-equivalence calculations across the codebase.

**Constraint:** Every USD-equivalent value must be produced by `finances.domain.rates.resolve()`. **No SQL view may compute `amount_usd`.** `v_transactions_usd` was dropped in migration `014` for violating this and must not be recreated; a view needing USD must go through a Python-built materialization step backed by the resolver, never inline ad-hoc rate logic.

**Locked priority order:**

1. `transactions.user_rate` (the user's actual realized rate)
2. `rates(USDT, VES, occurred_date, source='binance_p2p_realized')` — **cost basis**: the volume-weighted rate at which the bolívars were actually acquired, carried forward from the most recent P2P sell. Applies only while no older than `REALIZED_MAX_AGE_DAYS` (14, inclusive); past that the chain falls through. Headline-eligible.
3. `rates(USDT, VES, occurred_date, source='binance_p2p_median')` — market rate; headline-eligible. Applies only while no older than `MEDIAN_MAX_AGE_DAYS` (14, inclusive, per ADR-015); past that the chain falls through. Carried far enough, a stale median converges on BCV and silently turns a market rate into a reference rate.
4. `rates(USD, VES, occurred_date, source='bcv')` — **fallback only**, and **uncapped**: it is the floor of the chain, so expiring it would convert a stale reference into triage work. BCV is tracked for reference but is never the headline number
5. None → set `transactions.needs_review = 1`

**Age caps live in one place.** `finances.domain.rates.max_age_days(source)` owns every tier's carry-forward bound. Any consumer that does its own `latest_on_or_before` lookup — notably `web/services/rates_view.rates_for_day`, which builds the triage rate panel — must apply that bound rather than keep a second copy. A panel that disagrees with the resolver about staleness is the exact defect ADR-015 fixes.

**Realized rates are derived, never ad hoc.** Only `finances.domain.realized_rates` may produce `binance_p2p_realized` rows. Its `rebuild()` must stay wired into Binance ingest, or the derived rates silently fall behind the fills they come from.

**Headline rule (per ADR-005 amendment 2026-04-19):** No BCV-sourced USD value may appear in headline reports (`finances report consolidated`, Sheets `Monthly` tab, weekly summary). If the resolver returns a BCV-sourced value for a row destined for a headline, the row must be flagged `needs_review = 1` and excluded from the headline aggregate (or shown with a clear "BCV fallback" annotation that the report renderer surfaces).

**Lint check:** `grep -rn "amount_usd\|usd_value\|to_usd" finances/ | grep -v "domain/rates.py\|reports/"` should return zero matches that contain inline arithmetic on rates.
