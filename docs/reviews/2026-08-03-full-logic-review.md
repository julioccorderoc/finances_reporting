# Full logic review — `finances_reporting`

**Date:** 2026-08-03
**Scope:** whether the numbers this system produces are true.
**Method:** read-only `SELECT` against the live `finances.db` (1 896 rows), plus
mutation experiments run only on a throwaway copy in a scratch directory. The
working tree was not modified; no ingest, backfill, sync or write command was
run against the real database.

---

## The one-sentence version

The rate engine is sound — bolívar pricing is accurate to within about a
dollar across nine months. The damage is somewhere else entirely: **roughly
$9 800 of money that merely moved between your own accounts is being counted
as income and expense**, which is why your all-history net reads **−$982**
when the honest figure is **+$8 824**, and why the dashboard tells you your
Binance Spot balance is **negative $4 096**.

---

## Finding 1 — $9 806 of currency movement is counted as spending and income

**Severity: this is the whole problem. Everything in §2 and §3 follows from it.**

### What is wrong

Your ledger decides what counts as spending by looking at one column: `kind`.
`reports/consolidated_usd.py:112` and `reports/monthly.py:232` both say
`WHERE kind <> 'transfer'`. That is the only filter. If a row is not marked
`kind='transfer'`, it is spending or income, full stop.

The trouble is that four separate things in your ledger *are* currency
movement but are not marked `kind='transfer'`:

| | rows | USD | what it actually is |
|---|---|---|---|
| a | 46 | **−7 526.36** | rows **you already labelled** `Internal Transfer` / `External Transfer` |
| b | 34 | **−3 014.71** | P2P sells with no bank counterpart |
| c | 9 | **−5 355.99** | Binance convert legs missing their other half |
| d | 2 | **−1 200.00** | `Cambio $700 / $500 efectivo` — USDT sent out for physical cash |
| | **78 (union)** | **−9 805.87** | |

(The sets overlap — a convert leg is often also tagged `Internal Transfer` —
so the union is smaller than the sum.)

Item (a) is the one that should sting most: **you already told the system these
were transfers.** You went into the viewer and set the category to
`Internal Transfer` on 46 rows. Not one report reads that column. The category
is decoration; only `kind` is load-bearing.

### The query and the number

```sql
-- headline as reported today
SELECT ... -- finances report consolidated
-- total_usd = -982.24  across 1562 non-transfer rows
```

```sql
-- rows you labelled a transfer but which are not kind='transfer'
SELECT t.kind, t.currency, COUNT(*), ROUND(SUM(CAST(t.amount AS REAL)),2)
FROM transactions t JOIN categories c ON c.id = t.category_id
WHERE c.name IN ('Internal Transfer','External Transfer') AND t.kind <> 'transfer'
GROUP BY 1,2;
-- expense USDC   8   -8414.46
-- expense USDT  12   -1932.93
-- expense VES    1   -6000.00      (≈ -$9)
-- income  USDT   3   +3058.37
```

Priced through the resolver, those 46 rows net **−$7 526.36**.

Removing all 78 currency-movement rows from the aggregate:

| | USD |
|---|---|
| headline all-history net, as reported | **−982.24** |
| same, excluding currency movement | **+8 823.63** |
| **error** | **$9 806** |

A cross-check that this is the right answer: your USDC deposits (salary)
total **+17 819.48**; real bolívar spending is **−6 627.54**; Binance Pay
**−2 514.11**; withdrawals **−2 301.41**. That lands near +8 700. The corrected
figure is consistent with the raw inflows and outflows; the reported one is not.

### What it costs you

Every "how am I doing" number you have ever looked at. Nine months of monthly
reports, the spend trend, the category breakdown, the consolidated total. The
sign is wrong: the ledger says you are down $982 over nine months when you are
up $8 824. If you have made any decision off the monthly view — whether you
could afford something, whether spending was rising — you made it on a number
that was wrong by roughly ten thousand dollars.

### What I recommend

Three changes, in this order:

1. **Make the `Internal Transfer` category actually mean something.** The
   cheapest fix with the biggest return: exclude rows whose category is
   `Internal Transfer` / `External Transfer` from income and expense
   aggregation, the same way `kind='transfer'` is excluded. It reuses work you
   have already done by hand and recovers $7 526 immediately. It is a change in
   two report modules, not a data migration.
2. **Give the convert pairs and the cash swaps a real home** (§ Finding 7,
   decision D1 — a USDC→USDT swap inside one account has nowhere to go under
   the current two-account rule).
3. **Backfill the missing bank statements** so the unpaired sells can pair
   (Finding 5).

---

## Finding 2 — the Binance side of the ledger is arithmetically impossible

### What is wrong

```sql
SELECT currency, ROUND(SUM(CAST(amount AS REAL)),2)
FROM transactions WHERE account_id IN (2,3,4) GROUP BY 1;
-- USDC   +5993.88
-- USDT   -6990.30
```

**You cannot hold minus seven thousand USDT.** The ledger claims more USDT left
Binance than ever entered it.

Broken down by where the money came from and went:

| USDT flow | rows | total |
|---|---|---|
| in — converts from USDC | 7 | +6 492.59 |
| in — Earn rewards | 187 | +6.78 |
| out — P2P sells | 129 | −8 674.15 |
| out — Binance Pay | 11 | −2 514.11 |
| out — withdrawals | 11 | −2 301.41 |
| **net** | | **−6 990.30** |

There are **zero USDT deposits** in the entire history. Every USDT you have
ever held, according to this ledger, was created by converting USDC. And only
$6 493 of conversion was recorded arriving, against $11 846 of USDC recorded
leaving — the 5 missing convert legs.

The USDC side, by contrast, reconciles almost perfectly:

```sql
SELECT asset, ROUND(SUM(CAST(principal AS REAL)),2)
FROM earn_positions WHERE ended_at IS NULL GROUP BY 1;
-- USDC 6000.22   USDT 750.11
```

Ledger USDC across all three Binance accounts: **5 993.88**. Actually staked in
Earn: **6 000.22**. Difference: **$6.34**. That is what a healthy asset looks
like.

So the hole is USDT-specific and worth about **$7 740** — of which ~$5 354 is
the known orphan converts, and roughly **$2 386 remains unexplained**.

### What it costs you

You cannot answer "how much do I have" from this ledger, and you would not know
it, because nothing checks. `finances doctor` runs ten invariants and not one of
them asks whether an account balance is possible.

Separately: `earn_positions` records **$6 750.33 currently staked** and **no
report, view or dashboard tile reads that table.** ADR-003 created it; nothing
consumes it.

### What I recommend

Add two checks to `finances doctor`, both of which are a few lines of SQL:

- **negative asset balance** — any `(account, currency)` whose sum is below
  zero is an ERROR, not a warning. This alone would have surfaced the convert
  bug the day it happened.
- **per-asset reconciliation against `earn_positions`** — you have an
  independent, API-sourced statement of what you hold. Use it.

Then find the missing $2 386 of USDT inflow. My suspicion is Binance Pay
receipts or a P2P buy stream that predates the 35-day ingest lookback and was
never covered by the CSV backfill, but I did not confirm it and I am not going
to assert it.

---

## Finding 3 — the dashboard reports negative net worth

```
as_of 2026-08-03: total_usdt = -986.35
   Binance Earn            27.60
   Binance Funding      3 071.92
   Binance Spot        -4 095.94      <-- impossible
   Cash USD                 0.00
   Provincial Bolivares    10.07
```

This is Finding 2 rendered as a headline tile. Correcting only the proven
orphan-convert gap (+$5 356) brings Spot to **+$1 260** and net worth to about
**+$4 370**; adding the unread Earn principal and the unexplained USDT gap
brings it near the ~$6 750 the Earn table implies you actually hold.

There is a second, independent defect in the same module.
`web/services/net_worth.py` is a **fifth separate implementation of "convert to
USD"**, and it does not use the resolver. It converts bolívars using
`binance_p2p_median` **only** — not `user_rate`, not the realized cost basis:

```python
rate_row = rates_repo.latest_on_or_before(
    conn, as_of_date=as_of_date, base="USDT", quote=currency,
    source="binance_p2p_median",     # net_worth.py:138-143
)
```

That source has **8 rows in the entire database**:

```
2026-04-27, 2026-07-09, 2026-07-10, 2026-07-13,
2026-07-20, 2026-07-25, 2026-07-26, 2026-08-02
```

So for any date before 2026-04-27, **your bank account contributes exactly
nothing to net worth** — the tile shows `missing_pairs: ['VES→USDT']` and
silently omits it. I confirmed this: net worth as of 2026-03-15 and 2026-04-20
are byte-identical, both dropping the bolívar balance entirely. And after that
date it carries a rate up to 74 days old with no age cap and no `_carry` label,
while every other surface in the system uses a rate at most 14 days old.

rule-005 says *"All USD-equivalence calculations must go through
`finances.domain.rates.resolve()`. No SQL view, report, or ingester may compute
`amount_usd` inline using its own rate logic."* This module does exactly that,
and it is the single most prominent number in the application.

---

## Finding 4 — the dashboard's "top 5 spend categories" chart shows the five *smallest*

### What is wrong

`web/services/dashboard.py:397-401`:

```python
sorted_cats = sorted(
    cat_total.items(),
    key=lambda kv: (kv[1], kv[0]),      # kv[1] is a NEGATIVE number
    reverse=True,
)
top5 = [name for (name, _) in sorted_cats[:5]]
```

Expense totals are negative. Sorting negative numbers in reverse order puts the
**least** negative first. So "top 5 by total" returns your five *cheapest*
categories.

### The proof

Chart currently renders: `Fees, Loan Repayment, Other Expense, Clothing,
Health, Other`.

Actual spend, same six-month window, computed from the same report object:

| category | USD |
|---|---|
| Other | −7 059.09 |
| Internal Transfer | −2 829.95 |
| Purchases | −1 102.85 |
| Lending | −658.89 |
| Groceries | −654.54 |
| Dating | −406.51 |
| … | |
| Personal Care | −108.30 |
| Health | −101.43 |
| Clothing | −75.23 |
| Other Expense | −60.23 |
| Loan Repayment | −20.00 |
| Fees | **−3.26** |

The chart is built out of `Fees` at **$3.26** while `Purchases` at **$1 103**,
`Lending` at **$659** and `Groceries` at **$655** are all swept into the "Other"
bar. The chart is not merely mis-ordered; the categories it is *about* are the
ones it hides.

### Why this survived the test suite

`monthly_view.py` does the same ranking correctly — `key=lambda kv:
abs(sum(...))` at lines 528-531 and `key=lambda kv: abs(kv[1][0])` at 602-605.
`dashboard.py` is the one that drifted. The tests did not catch it because the
web fixture stores expenses as **positive** numbers (Finding 9), and with
positive inputs `reverse=True` is correct.

### What it costs you

This is the chart on the front page. It has been telling you your spending is
dominated by bank fees.

### What I recommend

One-line fix: `key=lambda kv: (abs(kv[1]), kv[0])`. Then fix the fixture so
the test would have caught it.

---

## Finding 5 — your bank export silently truncates at 99 rows, and 24 days of spending are missing

### What is wrong

Every large Provincial `.xls` export in `inputs/processed/` contains **exactly
99 rows**:

| file | rows | date range it actually covers |
|---|---|---|
| `provincial-april.xls` | **99** | 2026-04-10 … 2026-04-30 |
| `provincial-may.xls` | **99** | 2026-05-15 … 2026-06-01 |
| `provincial-june.xls` | **99** | 2026-06-07 … 2026-06-30 |
| `provincial_2026-04-19.csv` | **99** | 2026-03-31 … 2026-04-19 |
| `provincial-july.xls` | 89 | 2026-07-01 … 2026-07-09 |

Ninety-nine is the bank's page limit. When you ask for a month with more than
99 movements, it gives you the **last** 99 and says nothing. The ingest
faithfully loads whatever it is handed.

The resulting holes in your bank history:

```
gaps of >=3 consecutive days with no bank row:
  2026-01-07 .. 2026-01-11   (5 days)
  2026-05-01 .. 2026-05-14   (14 days)     <- may.xls starts on the 15th
  2026-06-02 .. 2026-06-06   (5 days)      <- june.xls starts on the 7th
```

### What it costs you

Your bank averages **5.2 rows and $33.47 of spending per covered day** across
198 days of data. 24 missing days ≈ **$803 of bolívar spending that is not in
your ledger at all**.

There is independent corroboration. Six of the unpaired P2P sells fall inside
the May gap and one inside the June gap — you sold **$501.84** of USDT for
bolívars during May 1-14 and **$54.20** during June 2-6. Those bolívars were
spent. The spending is not there. $556 of purchased bolívars against a $636
estimate for those 19 days is close agreement from two unrelated directions.

### The check you are one column away from having

`ingest/provincial.py:201` `_locate_columns` already finds the statement's
running-balance column (`Saldo`) — and then discards it. The bank prints, on
every row, what your balance was after that movement. If the ingest compared
the first row's `Saldo` minus its `Monto` against the last balance it had on
file, a truncated export would fail loudly instead of quietly.

### What I recommend

- Export bank statements **weekly**, not monthly, until the balance check
  exists. Under 99 rows means no truncation.
- Add the `Saldo` continuity check to the ingest.
- Add a `doctor` check for multi-day gaps in bank coverage.
- Re-pull May 1-14, June 2-6 and January 7-11.

---

## Finding 6 — the viewer will accept an arbitrarily wrong transfer pair, and nothing will ever object

### What is wrong

I ran this **on a copy of the database**, through the real web service function:

```python
confirm_pair(conn, deposit_id=23, sell_id=4753)
```

- deposit id 23: **2 261 Bs**, dated **2025-11-06**
- sell id 4753: **−200.44 USDT** at 848.1, dated **2026-07-30** — worth
  **169 993 Bs**

Eight months apart. Off by a factor of 75. It was accepted without complaint.
Both rows are now `kind='transfer'` sharing a `transfer_id`, which means both
have permanently dropped out of every income and expense aggregate.

Three guards exist and all three miss it:

- `create_transfer` mode 3 checks amount drift **only when both legs share a
  currency** (`transfers.py:280-285`). Cross-currency pairs get no check at all.
- `finances doctor`'s `transfer_same_currency_imbalance` check explicitly
  exempts cross-currency pairs.
- `transfers.validate()` does return `False` — and **nothing in the write path
  calls it**, and it is broken anyway (Finding 7).

The automatic matcher is careful — ±1 day, 2% tolerance, fiat-compatibility
check. The manual path a human clicks has none of that.

### What it costs you

One misclick permanently deletes a transaction pair from your reports, with no
warning, no audit signal, and no way to notice.

### What I recommend

Have `confirm_pair` compute the drift the pair-picker **already computes for
display** (`web/services/pairing.py:120-121`) and refuse anything beyond a
generous bound — say 10% and 5 days — with an override the owner has to
consciously pass.

---

## Finding 7 — `user_rate` semantics: the decision you asked me to make

You said the real blocker is that `user_rate` has no recorded direction, and
you have not been able to decide what it should mean. The data already answers
this, unambiguously.

### The evidence

```sql
SELECT currency, COUNT(*), MIN(user_rate), MAX(user_rate)
FROM transactions WHERE user_rate IS NOT NULL AND CAST(user_rate AS REAL) < 1;
-- (no rows)
```

**Not one of the 846 `user_rate` values in your ledger is below 1.** If the
column were ambiguous in direction you would see values like `0.0016`
(USDT per VES) mixed in with values like `630` (VES per USDT). There are none.
Every value, on both VES rows and USDT rows, is in the 313–866 band (or 1.003–
1.015 for the four USD-denominated P2P sells).

### The semantics, stated

> **`user_rate` is: how many units of the local currency one USD-pegged unit
> was worth for this transaction.** Quote currency per base, where base is
> always the dollar side.

- On a **VES row** it is VES per USDT. Convert with `amount / user_rate`.
- On a **USDT P2P row** it is the fill's realized price — VES per USDT for the
  122 bolívar fills, USD per USDT for the 4 dollar fills. It is *provenance*,
  not a conversion factor: the row is already dollar-denominated, so the
  resolver correctly never uses it (`consolidated_usd.py:130` short-circuits
  USD/USDT/USDC before consulting the rate at all).

The whole codebase already behaves this way. `rates.resolve` divides.
`realized_rates` divides. Both report modules divide. Your legacy spreadsheet's
`Tasa USDT` column, which the backfill loaded into `user_rate`, was VES per
USDT. **`transfers.validate()` is the single place that multiplies**, and it is
simply a bug — not evidence of an unresolved design question.

### The fix to `validate()`

It should ask each leg the same question the reports ask:

```python
def _leg_usd(leg):
    if leg.currency in {"USD", "USDT", "USDC"}:
        return leg.amount                    # already dollars
    if leg.user_rate is None:
        return None
    return leg.amount / leg.user_rate        # divide, never multiply
```

I verified the corrected form against real data. Priced through the resolver,
your 95 cross-currency transfer pairs net to **$0.72 in total** — they were
correct all along; only the checker was wrong.

**No ADR is needed.** You are not choosing a convention; you are writing down
the one your data has always used. What *would* need a decision is storing the
direction explicitly (e.g. a `rate_base` / `rate_quote` pair), and I would not
bother — one fiat, one convention, and now it is documented.

---

## Finding 8 — the rate engine is healthy, but it is four days from a cliff, and the floor under it is missing

This is the part of the system you said carried the most money and was least
verified. It is in good shape. I want to be equally clear about where it is
fragile.

### What is right

I re-priced all 1 562 non-transfer rows through `rates.resolve`:

| tier | rows | USD |
|---|---|---|
| `user_rate` | 659 | −3 880.40 |
| `native_usd` | 485 | +4 663.02 |
| `binance_p2p_realized` | 223 | −1 389.80 |
| `binance_p2p_realized_carry` | 195 | −375.06 |
| **`bcv` / `bcv_carry`** | **0** | — |
| **`needs_review`** | **0** | — |

**No bolívar row anywhere in your history is priced off BCV, and none fails to
price.** The rule you care most about is holding.

I also checked whether the 659 legacy `user_rate` values (loaded from your old
spreadsheet's `Tasa USDT` column) agree with the cost basis ADR-013 derives
independently:

| month | USD @ your rate | USD @ realized basis | delta |
|---|---|---|---|
| 2025-11 | −1 150.40 | −1 151.76 | −1.37 |
| 2025-12 | −787.28 | −784.98 | +2.30 |
| 2026-01 | −666.07 | −667.91 | −1.85 |
| 2026-02 | −399.39 | −400.71 | −1.32 |
| 2026-03 | −852.77 | −851.81 | +0.96 |
| **total** | **−3 855.90** | **−3 857.17** | **−1.27** |

**$1.27 of disagreement across five months.** Your hand-carried rates and the
machine-derived cost basis are the same number. That is a strong independent
confirmation that the cost-basis model is right and that ADR-013 was the
correct call.

`reports/monthly.py` and `reports/consolidated_usd.py` agree to **1e-10** on
every month — two independently written aggregators over one shared resolver.
That invariant is doing its job.

### Where it is fragile

**The 14-day cap has never fired, and it is close.** Your realized rates cover
103 days from 2025-11-05 to 2026-07-30, and the largest gap between consecutive
rates is **7 days** — comfortably inside the cap. But the last realized rate is
**2026-07-30, four days ago**. Ten more days without a P2P sell and the cap
trips.

**The tier below it is nearly empty.** When realized goes stale, ADR-005 says
fall to `binance_p2p_median`. That table has **8 rows in fifteen months** —
2026-04-27, then nothing until July. For any date before 2026-04-27 the market
tier does not exist at all.

**So the true fall-through is realized → BCV**, and BCV is not merely a
different number:

| date | BCV | P2P median | P2P is higher by |
|---|---|---|---|
| 2026-04-27 | 484.74 | 633.52 | **30.7%** |
| 2026-07-09 | 700.22 | 828.28 | **18.3%** |
| 2026-07-10 | 709.69 | 816.30 | **15.0%** |
| 2026-07-13 | 721.35 | 835.43 | **15.8%** |

And BCV-priced rows do not merely re-price — `monthly.py:206` routes them to
`fallback_usd`, which is **excluded from `total_usd`**. So the failure mode is
not "the month is 18% off". It is *"the month's bolívar spending disappears
from the headline entirely and reappears in a column you have to know to
look at."*

### What I recommend

- **Get `binance_p2p_median` running daily.** It is the designed safety net and
  it is not deployed. Eight rows in fifteen months means the net is not there.
- **Make a stale basis loud.** If `resolve` has to fall past the realized tier,
  that should be a `doctor` warning, not a suffix on a string.

---

## Finding 9 — the same fact written down five times

You asked me to look for another instance of the shape that produced the two
worst bugs so far: *one fact, expressed in many hand-written places, which then
drifted.* It is the dominant structural problem in the codebase. Four live
instances:

**"Which currencies are already dollars"** — defined **four times**:

```
reports/consolidated_usd.py:51      _NATIVE_USD_CURRENCIES  = {"USD","USDT","USDC"}
reports/monthly.py:49               _NATIVE_USD_CURRENCIES  = {"USD","USDT","USDC"}
web/services/transactions_query.py:31  _NATIVE_USD_CURRENCIES = {"USD","USDT","USDC"}
web/services/net_worth.py:31        _NATIVE_USDT_CURRENCIES = {"USD","USDT","USDC"}   <- different name
```

The day you hold BTC or EUR, four places need editing and the fourth has a
different name so it will be the one you miss.

**"Convert to USD"** — written **five times**: `consolidated_usd.py:161`,
`monthly.py:205`, `transactions_query.py:300`, `rates_view.py:228`, and
`net_worth.py:159`. The fifth has already drifted — it is Finding 3.

**"What counts as spending"** — `kind <> 'transfer'` in the two report modules,
but `kind NOT IN ('transfer','adjustment')` in triage (`triage.py:161`, `:228`).
The reports would count an `adjustment` row as income or expense; triage would
not. You have no `adjustment` rows today, so this is latent, not live.

**"What is a P2P sell"** — `source_ref LIKE 'p2p:%' AND amount < 0` in three
places: `realized_rates.py:49`, `integrity.py:275`, `transactions_write.py:67`.
These currently agree, and `transactions_write.py` carries a comment explaining
exactly how its copy differs on purpose — which is the right way to do it if
you are going to duplicate at all.

### Secondary items in the same family

- **Mixed-currency report buckets (11 of them).** `monthly.py` keys buckets on
  `(month, account, category, kind)` but takes `currency` from whichever
  transaction landed first (`monthly.py:334`). Account 2 and account 4 hold
  both USDT and USDC, so 11 rows sum two currencies and label the total with
  one. Harmless today because both are dollar-pegged; wrong the moment they
  are not.
- **`v_account_balances` does the same thing** — sums every currency in an
  account and labels the result with `accounts.currency`. It is why "Binance
  Spot: −4 095.94 USDT" is quoted in USDT when it is actually
  `+5 973.06 USDC − 10 068.99 USDT`.
- **`needs_review` carries two meanings.** rule-006 says it means "the importer
  could not determine a category". ADR-005 sets it when a rate cannot be
  resolved. `web/services/transactions_write.py:145` re-derives it **purely
  from rate resolution**, so a row can be uncategorised and unflagged. There
  are **37 such rows** today (30 expense, 7 income). The viewer's triage queue
  compensates by querying `category_id IS NULL` separately
  (`triage.py:157-165`), so nothing is actually lost — but any surface reading
  the raw column is wrong, and `doctor` does not check it.
- **`Transaction` has no sign validator.** `domain/models.py` accepts
  `kind=EXPENSE, amount=+365.00`. That is what lets the web fixture
  (`tests/web/conftest.py:189-262`) store **every expense as positive** while
  production stores them negative — and that is what hid Finding 4. It also
  means the fixture never contains a single negative amount, so
  `_is_p2p_sell` (`amount < 0`) and `pairing.pairable` (a sign comparison) are
  never exercised on realistic data by the web suite.

---

## Your five known issues — confirmed or corrected

**1. `transfers.validate()` is broken for cross-currency pairs.**
**Confirmed, and the harder half is answered.** It multiplies where it must
divide (`transfers.py:499-500`). It is dormant — nothing in the write path
calls it. The `user_rate` direction question is settled by your own data: see
Finding 7. It is a bug, not an open design question, and it does not need
an ADR.

**2. Five orphaned Binance convert legs, ~5 354 USDC.**
**Correct in substance; the count has grown to 9 rows.** `doctor` now reports
891, 892, 910, 911, 944, 962, 1021, 1038, 1062. Reading them properly: **7
outgoing legs (−7 873.94 USDC) and 2 incoming legs (+2 517.95 USDT)**, netting
**−5 355.99**. Two of those (891/892 on 2025-11-23, 910/911 on 2025-11-30) are
almost certainly real pairs whose per-leg hashes simply never matched — same
date, amounts one dollar apart. So the genuine loss is **5 conversions with no
incoming leg at all**, and that is the ~$5 354 that makes your Binance USDT
balance impossible (Finding 2).

**3. Same-account currency conversions cannot be modelled.**
**Confirmed.** 5 matched convert pairs sit as expense + income on account 2:
**−$3 972.48 of expense against +$3 974.64 of income**, netting $2.16. The net
is right; both gross figures are inflated by ~$3 973. See decision D1 below —
my answer is that the *account* model is wrong, not the double-entry model.

**4. The web fixture stores expenses positive.**
**Confirmed, and it masked a live bug.** Every expense in
`tests/web/conftest.py` is positive (365.00, 100.00, 3650.00, 999.00, 12.50);
the fixture contains no negative amount at all. This is precisely why
Finding 4 — the inverted top-5 spend chart — passed a green suite: with
positive inputs, `reverse=True` sorts correctly.

**5. 31 unpaired P2P sells, believed to be missing bank statements.**
**Now 34, and the belief is right for 27 of them — but the cause is more
specific and more fixable than "missing statements".** The breakdown:

| | count | USD | why |
|---|---|---|---|
| July 11-31 | 14 | −833.09 | bank data stops 2026-07-09 |
| May 1-13 | 6 | −501.84 | **99-row export truncation** (Finding 5) |
| 2025-10-30/31 | 3 | −165.69 | predate bank history; also have no `user_rate`, so the matcher can never see them |
| USD-denominated | 4 | **−1 365.71** | **not bank sells at all** |
| Jun 3 | 1 | −54.20 | **99-row export truncation** |
| genuinely unmatched | 6 | −94.18 | see below |

Two corrections to the belief:

- **Four of them are not bolívar sells.** ids 1126, 1176, 1177, 4752 are
  `P2P SELL USDT @ 1.003 USD`, `@ 1.006 USD`, `@ 1.015 USD`, `@ 1.01 USD` —
  you sold USDT for **dollars**, not bolívars, totalling **$1 365.71**. No bank
  statement will ever pair them. That money left Binance and lands nowhere in
  the ledger, because your `Cash USD` account has **zero transactions**. This
  is the same shape as the two `Cambio efectivo` rows in Finding 1(d).
  *(Credit where due: `realized_rates._fiat_of` correctly identified these as
  USD and kept them out of the bolívar average — I verified no realized rate
  exists for 2026-06-11, 2026-06-23 or 2026-07-31. The fiat guard works.)*

- **The matching algorithm is not at fault.** I built the eligibility graph
  under the production rules (±1 day, 2% drift, fiat-compatible) and compared
  the greedy assignment against a true maximum bipartite matching: **95 pairs
  either way, zero lost to greediness.** The tolerances are what bind, not the
  algorithm. One clear near-miss: sell 1030 (2026-03-03, expects 34 148 Bs)
  against deposit 588 (same day, 35 000 Bs) is **2.4% off against a 2.0% cap**.
  Widening to 3% would claim it.

---

## What `finances doctor` does not check

It runs ten invariants and catches real things. Here is the gap, ranked:

1. **Negative asset balances.** Would have caught Finding 2 immediately.
2. **Cross-currency transfer pairs netting to zero.** Explicitly exempted —
   which is exactly the hole Finding 6 walks through. Now that the resolver can
   price any leg, this check is writable: I ran it, and your 167 legs net to
   **$0.72** in total, with 31 pairs above a one-cent tolerance and the worst at
   $3.82. A $50 threshold would be quiet today and loud on a bad pair.
3. **Gaps in bank coverage.** Would have caught Finding 5.
4. **Rows categorised as a transfer but not `kind='transfer'`.** Finding 1(a),
   $7 526 — one `JOIN` away from being visible.
5. **Uncategorised rows that are not flagged `needs_review`.** 37 today.
6. **`earn_positions` versus ledger balances.** You have an independent source
   of truth and nothing consults it.
7. **Sign/kind agreement** — a positive `expense` or negative `income`.
8. **Rate staleness** — how many days since the last realized rate, against
   the 14-day cap.

Also worth noting: `duplicate_source_ref` can only ever fire if the `UNIQUE`
constraint is dropped. It cannot detect the duplicate that actually matters —
the *same real transaction* ingested under two different `source_ref`s, which is
exactly what the orphan convert legs are.

**One thing that is genuinely solid:** I tested re-ingest idempotency directly,
running all 14 processed statement files back through `ingest_csv(dry_run=True)`
against a copy. **Every file: 0 rows inserted.** 904 rows seen, 904 updated,
nothing duplicated, row count unchanged at 1 896. rule-010 holds.

---

## What needs your decision

### D1 — where does a same-account currency conversion belong?

A USDC→USDT swap inside Binance Spot is one account, and `create_transfer`
requires two. Today it lands as an expense row plus an income row, both counted
(Finding 1, ~$3 973 gross on each side).

You asked whether the double-entry model is wrong or the account model. **The
account model.** Double-entry is right — money left one place and arrived in
another. The error is calling "Binance Spot" one account when it holds two
distinct assets: the ledger already tracks **+5 973.06 USDC and −10 068.99 USDT**
inside a single row labelled "−4 095.94 USDT".

Three options:

- **(a) One account per (venue, asset).** `Binance Spot USDC`, `Binance Spot
  USDT`. A convert becomes an ordinary two-account transfer with no special
  case. Balances become meaningful per asset. Correct, and the most work — an
  account split plus a migration of `account_id` on existing rows.
- **(b) Allow same-account transfers when currencies differ.** Relax the
  `from_account_id == to_account_id` guard for cross-currency pairs only. Small
  change, keeps everything else. Slightly odd double-entry, but honest: the
  money genuinely stayed in one venue.
- **(c) A dedicated `kind='conversion'` excluded from aggregation.** Least
  invasive, but adds a fourth `kind` that every `WHERE kind <> 'transfer'` site
  must learn about — and there are already two of those that disagree with
  triage. This re-creates Finding 9.

**My recommendation: (b) now, (a) if you ever hold a non-dollar asset.** (b)
fixes $3 973 of gross inflation for a one-line guard change. (a) is the right
model but you do not need it until BTC or EUR shows up, and at that point
`v_account_balances` and the four `_NATIVE_USD_CURRENCIES` copies need fixing
anyway.

### D2 — should the `Internal Transfer` category exclude a row from spending?

Finding 1(a), **$7 526.36**, 46 rows. You have already labelled these. The
question is whether a category may override `kind`.

- **Yes** — reports gain `AND category NOT IN ('Internal Transfer','External
  Transfer')`. Instant, uses work already done, no data migration. Cost: two
  columns now decide what counts as spending, which is a second load-bearing
  fact (see Finding 9).
- **No** — instead, promote those 46 rows to `kind='transfer'` properly, which
  means finding or creating each one's counterpart leg. Cleaner model, but 46
  rows of manual pairing and some counterparts do not exist (the cash swaps,
  the USD-fiat sells).

**My recommendation: yes, and treat it as the honest fix rather than a
shortcut.** "Is this money moving or money leaving" is genuinely a categorical
question, and you are already answering it. Make `kind` derived from category
for these two categories, in one shared helper — not copy-pasted into each
report.

### D3 — should `Cash USD` be used?

It has **zero transactions**. Meanwhile **$1 365.71** of USD-denominated P2P
sells and **$1 200** of `Cambio efectivo` USDT sends — **$2 565.71** — leave
Binance and are recorded as expenses because there is nowhere for them to land.
Either that money is genuinely spent (in which case the description "Cambio
$700 efectivo" is misleading), or it became physical cash and should transfer
into `Cash USD`. **You are the only one who knows which.** If it is cash, that
is $2 566 of net worth currently recorded as spending.

---

## The three things I would fix first

**1. Stop counting currency movement as spending — Findings 1 and 3.**
$9 806, and it is the reason every headline number you have is wrong. Start
with decision D2, which recovers $7 526 for a change confined to two report
modules and needs no data migration. Everything else in this report is smaller
than this one item.

**2. Add three checks to `finances doctor` — negative asset balance,
cross-currency transfer netting, bank-coverage gaps.**
Each is a few lines of SQL in `domain/integrity.py`, next to the ten that are
already there. Together they would have caught Findings 2, 5 and 6 on the day
each happened instead of months later. You told me the pattern that hurts you
is *"everything looked fine on screen and the suite was green"* — `doctor` is
the only thing you own that runs against real data, and its blind spots are
exactly where your real defects have been living. This is the fix that changes
how the next bug gets found.

**3. Fix the top-5 spend chart, and the fixture that hid it — Findings 4 and 9.**
The code fix is one `abs()`. I put it third not for its size but for what it
represents: a bug on your front page, in the one number you look at most, that
survived because the test data had the opposite sign to production. Fix the
chart, then make the fixture store expenses negative — and expect that second
change to surface more of these. It is the cheapest way to stop the class of
error that produced this finding from producing the next one.

---

## Appendix — what I did not verify

- The **~$2 386 of unexplained USDT inflow** in Finding 2. I proved the hole
  exists and that $5 354 of it is the orphan converts. I did not identify the
  rest, and I did not call the Binance API to find out.
- **`ingest/binance.py` endpoint coverage.** I confirmed P2P BUY *is* fetched
  (`binance.py:470`), so the absence of P2P buys is a fact about your history,
  not a gap in the ingest. I did not audit whether every inbound flow type has
  an ingester.
- **`reports/sheets_sync.py`, `reports/html_export.py`, `reports/update.py`.**
  Not reviewed. `sheets_sync` consumes `consolidated_usd`, so Finding 1 flows
  into the Sheets mirror unchanged.
- **`migration/backfill.py`.** Read only where it explains where `user_rate`
  came from (`backfill.py:449-497`) and how convert legs were hashed.
- I ran no ingest, backfill, sync or write command against `finances.db`. Every
  mutation test in this report — the pairing experiment, the re-ingest
  idempotency test — ran against a `.backup` copy in a scratch directory. The
  working tree is unchanged (`git status` clean).
