# ADR-005: Rate Resolution Priority — User Override > Binance P2P Median > BCV

**Date:** 2026-04-19
**Status:** Accepted

## 1. Context

Every bolívar transaction needs a USD equivalent for the consolidated view. Today the spreadsheet stores two rate columns per row (`Tasa del día` BCV and `Tasa USDT`), with no documented provenance for the USDT value, and Binance transactions store no rate at all. The user wants the rate they actually realized (e.g. on a P2P sell) to be the truth, not the day's market median, when both exist.

Three options:

1. **Pull Binance P2P median for every day, automatically.**
2. **Use the user's most recent realized rate from a P2P sell, applied to subsequent rows until the next sell.**
3. **Hybrid: per-row user override wins, fall back to market median.**

Option 1 ignores actual realized rates. Option 2 spreads one realized rate across unrelated transactions, distorting them. Option 3 captures the truth where it exists and uses a sensible default elsewhere.

## 2. Decision

Use a **priority chain** in `finances/domain/rates.py`:

1. If `transactions.user_rate IS NOT NULL` → use it. (User's actual realized rate wins.)
2. Else look up `rates(USDT, VES, occurred_date, source='binance_p2p_median')`.
3. Else fall back to `rates(USD, VES, occurred_date, source='bcv')`.
4. If none of the above exist (e.g. weekend gap with no carry-forward), set `transactions.needs_review = 1` and surface the row in `v_unreconciled_transfers` adjacent reports.

For days without a fetched P2P or BCV rate, carry forward the last preceding business-day rate and tag the source with a `_carry` suffix (e.g. `binance_p2p_median_carry`) for transparency.

## 3. Consequences (The "Why")

### Positive

- The user's realized rate is preserved exactly where it matters (P2P sells, hand-known cash conversions).
- Routine bolívar expenses get an automatic, defensible USD value with no manual entry.
- The fallback chain means no row silently uses a wrong rate; any gap is explicitly flagged.

### Negative

- Two ingesters (BCV scrape + P2P fetcher) must run reliably or the fallback chain runs out.
- The `_carry` suffix is a subtle distinction; reports must surface it so the user knows which rates are extrapolated.
- A user-override field on every transaction means UI/CLI affordances must always allow it.

## 4. Rule Extraction (The "How" for Agents)

**Target File:** `docs/architecture/rules/rule-005-single-rate-resolver.md`
**Injected Constraint:** All USD-equivalence calculations must go through `finances.domain.rates.resolve()`. No SQL view, report, or ingester may compute `amount_usd` inline using its own rate logic. Diverging implementations are forbidden. The resolver's priority order is locked: `user_rate` → `binance_p2p_median` → `bcv` → flag.

---

## Amendment 2026-04-19 — USDT for Headline, BCV for Reference Only

**Context:** The user explicitly wants USDT-derived USD values to be the headline number in the consolidated weekly summary. BCV is still tracked daily (it is the official rate businesses use, and users occasionally need it as a reference) but is **never** the value reported in the summary unless the resolver has truly exhausted the higher-priority sources.

**Amendment:**

- The consolidated USD summary (`finances report consolidated`, the `Monthly` Sheets tab, and any "headline" USD figure) must use the `binance_p2p_median`-derived value (or the user's `user_rate` override when present). It must **never** use a BCV-derived value as the headline.
- BCV is retained because: (a) it is the legal/government rate, occasionally needed; (b) it is the last-resort fallback in the resolver chain when both `user_rate` and `binance_p2p_median` are absent.
- Reports that include BCV-sourced USD values must surface a column or annotation indicating the rate source, so the user can immediately see when a row used the fallback.
- If the resolver returns a BCV-sourced value for a row that will appear in the headline summary, that row should be flagged `needs_review = 1` and the user prompted to either backfill a `user_rate` override or wait for the P2P fetcher to fill the gap.

**Rule update:** `docs/architecture/rules/rule-005-single-rate-resolver.md` is updated to forbid BCV-sourced values from headline summary outputs.
