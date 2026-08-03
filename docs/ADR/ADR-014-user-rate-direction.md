# ADR-014 — `user_rate` Is Bolívares Per Dollar, On Every Row

- **Status:** Accepted
- **Date:** 2026-08-03
- **Supersedes:** nothing. Clarifies ADR-005 and ADR-002.

## 1. Context

`transactions.user_rate` stores a bare number:

```text
Jul 7 · Binance Spot · −98.32 USDT · user_rate = 762.80
Jul 7 · Provincial   · +75 000 VES · user_rate = NULL
```

Nothing records what `762.80` is a rate *of*. Bolívares per dollar, or
dollars per bolívar? A human infers it instantly from the magnitude.
Code cannot, and the column sits on rows denominated in bolívares *and*
on rows denominated in USDT, so "it is the row's own currency" is not an
answer either.

The ambiguity was harmless until something needed to convert two legs of
a transfer into one unit to check they cancel.
`domain.transfers.validate` tried, and did this:

```python
a_usd = a.amount * a.user_rate
b_usd = b.amount * b.user_rate
```

Multiplying a bolívar amount by a bolívares-per-dollar rate yields
VES²/USD — a quantity with no meaning. Every cross-currency transfer in
the ledger, 107 of them, reported invalid. Correct ones included.

Nothing in the write path calls `validate`, so no data was harmed. But
the function has never once verified a bank-anchored pairing, which is
the exact case it exists for, and nobody noticed because its answer was
never consulted.

Patching the arithmetic alone was not possible: there was no recorded
fact to patch it *toward*. This ADR supplies the missing fact.

## 2. Decision

**`user_rate` always means bolívares per dollar — the VES/USDT rate —
regardless of which currency the row it sits on is denominated in.**

This is descriptive, not a change: it is what every existing writer
already stores.

- `ingest/binance.py` writes a P2P order's `unitPrice`, quoted VES per
  USDT.
- The legacy Provincial backfill reads the sheet's *Tasa USDT* column,
  the same quantity.
- The triage rate panel presents it that way, and the owner reads it
  that way.

Converting a leg to USD therefore depends on the leg's currency, not on
where the rate is stored:

| leg currency | to USD |
|---|---|
| `USD`, `USDT`, `USDC` | already USD-equivalent — pass through |
| anything else (`VES`) | `amount / user_rate` |

A rate carried on a USD-equivalent leg is descriptive metadata: it
records the rate that applied to the movement, and is *not* applied to
that leg's own amount. This is what makes a bank-anchored pair work — the
Binance leg carries the rate, the bolívar leg is the one that needs it.

**Where a leg needs a rate it does not carry, the transfer's other leg
supplies it.** A pairing is one economic event at one rate; requiring the
figure to be duplicated onto both rows would create two places for it to
drift.

`USD`, `USDT` and `USDC` are named as USD-equivalents in one constant,
`domain.transfers.USD_EQUIVALENT_CURRENCIES`. They are treated as 1:1
with the dollar. Stablecoins depeg by fractions of a percent; the
ledger's tolerances are wider than that, and pretending otherwise would
demand a price feed for a distinction that never changes a decision here.

## 3. Consequences

**Good**

- `validate` can do its job. All 169 transfers in the ledger now verify —
  72 same-currency and 97 cross-currency, none of which had ever passed.
  A real imbalance is detectable for the first time.
- The rule is written down, so the next reader — human or agent — cannot
  silently reinterpret the column. That failure mode is not theoretical:
  `RawBinanceConvertRow` was changed from `tranId` to `orderId` without a
  record, and the legacy convert backfill failed silently for months.
- No migration. No data rewrite. Existing rows already comply.

**Bad**

- The meaning lives in code and in this document, not in the schema. A
  writer that stores an inverted rate will not be rejected by the
  database. Mitigated by `finances doctor`, which now checks
  cross-currency transfers balance — an inverted rate makes a pair miss
  by orders of magnitude and shows up immediately.
- Hard-coding `VES` as "the other currency" is implicit. The rule as
  stated generalises — anything not USD-equivalent divides — but has only
  ever been exercised against bolívares.

**Rejected alternatives**

- *`user_rate` means "multiply this row's amount by this to get USD."*
  Self-describing per row, and appealing for that reason. Rejected
  because every row in the ledger would become wrong on the day it
  shipped: 762.80 × 98.32 is not a dollar figure. The migration would
  have to invert rates on bolívar rows and blank them on USDT rows,
  rewriting history to satisfy a convention.
- *Add a `rate_direction` column.* Explicit and future-proof. Rejected as
  disproportionate: one user, one non-USD currency, and every writer
  already agrees. The cost is a migration plus a field every writer must
  remember to set — a new way to be silently wrong, in exchange for
  flexibility nothing needs. Revisit if a second non-USD currency ever
  enters the ledger.

## 4. Rule Extraction

**Target file:** `docs/architecture/rules/rule-005-single-rate-resolver.md`

**Injected constraint:** `transactions.user_rate` is bolívares per dollar
on every row that carries one. To express a leg in USD: pass it through
when its currency is in `USD_EQUIVALENT_CURRENCIES`, otherwise divide by
the rate. Never multiply a non-USD amount by `user_rate`. A leg lacking a
rate borrows its counterpart's within the same transfer.
