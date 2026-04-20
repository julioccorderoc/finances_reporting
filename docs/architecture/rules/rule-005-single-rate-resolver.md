# Rule 005 — Single Rate Resolver

**Source ADR:** [ADR-005](../../ADR/ADR-005-rate-resolution-priority.md)
**Scope:** All USD-equivalence calculations across the codebase.

**Constraint:** Every USD-equivalent value must be produced by `finances.domain.rates.resolve()`. SQL views that need a USD value must call the resolver via a Python-built materialization step or a SQL function backed by it — never inline ad-hoc rate logic.

**Locked priority order:**

1. `transactions.user_rate` (the user's actual realized rate)
2. `rates(USDT, VES, occurred_date, source='binance_p2p_median')` — **this is the source used in the final consolidated USD summary**
3. `rates(USD, VES, occurred_date, source='bcv')` — **fallback only**; BCV is also tracked for reference but never the headline number
4. None → set `transactions.needs_review = 1`

**Headline rule (per ADR-005 amendment 2026-04-19):** No BCV-sourced USD value may appear in headline reports (`finances report consolidated`, Sheets `Monthly` tab, weekly summary). If the resolver returns a BCV-sourced value for a row destined for a headline, the row must be flagged `needs_review = 1` and excluded from the headline aggregate (or shown with a clear "BCV fallback" annotation that the report renderer surfaces).

**Lint check:** `grep -rn "amount_usd\|usd_value\|to_usd" finances/ | grep -v "domain/rates.py\|reports/"` should return zero matches that contain inline arithmetic on rates.
