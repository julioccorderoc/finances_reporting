# What `finances doctor` is reporting, and what each finding actually is

Read-only investigation, 2026-09-03, after the ADR-022 dedupe. Nothing was
written. `finances doctor`: **2 errors, 3 warnings, across 16 checks**.

Short version: **one of the five findings is a real hole in the data**
(the Binance wallet split), **two are checks that contradict ADR-019** and
report correct rows as broken, and **two are backlog** — rows waiting for
your judgement.

| Finding | Rows | What it really is |
|---|---|---|
| `transfer_legs_same_account` (ERROR) | 32 | ADR-019 reversal pairs. The check predates the ADR. **False positive.** |
| `negative_asset_balance` (ERROR) | 2 positions | Real. Binance Spot vs Funding are split wrong; see §2. |
| `category_kind_mismatch` (warn) | 20 | 10 are the same ADR-019 pairs; 10 are incoming money filed under expense categories. |
| `uncategorized_not_flagged` (warn) | 7 | Backlog. Includes the $580 cash conversion you already know about. |
| `unpaired_p2p_sells` (warn) | 4 | All four already carry *Internal Transfer*. The check over-reports. |

---

## 1. `transfer_legs_same_account` — 32 rows, and all 32 are correct

Every one of the 16 groups is a **REVERSO CARGO pair**: a bank charge that
failed and its same-day reversal, paired to zero by ADR-019.

```
16 groups   16 with a REVERSO leg   16 netting exactly zero
2026-02-07, 2026-02-27, 2026-05-02, 2026-05-20, 2026-07-12,
2026-07-13, 2026-07-15, 2026-08-31 (×2 each)
```

`domain/reversals.py::pair_reversal` **requires** both legs to be on the
same account in the same currency and to sum to zero. The check says that
shape is an error. Two pieces of the system state opposite rules; the
check was written on 2026-08-03, ADR-019 landed after it, and nobody went
back.

The check is not worthless — a genuinely mis-paired same-account pair
would hide a real movement. What separates the two cases is that a
reversal nets to **exactly** zero by construction. Two ways to fix it,
your call:

- **Cheap:** exempt same-account pairs that net to zero. One SQL clause;
  loses the ability to catch a mis-pair that happens to net to zero.
- **Honest:** have `pair_reversal` mark its pairs (a `reversal-` prefix on
  the shared `transfer_id`, say) and exempt exactly those. Needs an
  ADR-019 amendment and a migration for the 16 existing pairs.

Until then this ERROR is permanent noise, which is worse than it sounds:
`doctor --strict` exits non-zero forever, so the check that would catch a
*real* pairing defect can no longer be used as a gate.

## 2. `negative_asset_balance` — the real one, and it is one defect, not two

Two positions are negative:

```
Binance Spot  USDT   -1,893.09      Binance Spot  USDC   -400.38
```

**The totals are fine; the split is not.** Across the three Binance
accounts the ledger holds **USDT 2,445.39** and **USDC 5,010.60**, and the
Earn part of that is confirmed by Binance's own position endpoint
(`earn_positions`, today's sync): ledger Earn USDT 2,002.92 vs SDK
2,002.23; ledger Earn USDC 5,010.98 vs SDK 5,010.61 — a few tenths of
accrued reward apart. So free (Spot + Funding) USDT is **442.47**, and the
ledger splits that 442.47 as **−1,893.09 on Spot and +2,335.56 on
Funding**.

Where the split comes from — every USDT movement class, by account:

| | Spot | Funding |
|---|---|---|
| internal transfers | **−9,842.05** | **+9,842.05** |
| Earn redemptions | +6,186.43 | +3,606.70 |
| Earn subscriptions | −11,776.44 | — |
| converts (USDC→USDT) | +14,974.41 | — |
| P2P | **−10,265.16** | — |
| Binance Pay | **−3,359.11** | — |
| withdrawals | −2,075.91 | −225.50 |
| ADR-020 opening rows | +14,264.74 | −10,887.69 |

**Funding only ever fills up.** It receives 13,448.75 (transfers +
redemptions) and pays out 225.50, ever. A wallet that money enters and
never leaves is not a wallet — it is a bookkeeping error.

The likely cause is in `ingest/binance.py`: `RawBinanceP2pRow` and
`RawBinancePayRow` both hardcode `to_transaction(spot_account_id=...)`,
so **every P2P order and every Binance Pay event is booked against Spot**.
On Binance, P2P/C2C trades and Binance Pay debit the **Funding** wallet —
which is exactly what the ledger's own transfer rows describe: USDT moves
Spot→Funding (−7,124.66 net on `MAIN_FUNDING`, only +478.61 coming back),
and then the thing that consumes it in Funding is recorded against Spot.

Move `p2p:` + `pay:` (−13,624.27) to Funding and, excluding the ADR-020
rows, the two positions go from **−16,157.83 / +13,223.25** to
**−2,533.56 / −401.02**. Both far closer to zero, and the remaining
−2,533.56 on Spot is the genuine pre-ledger gap ADR-020 exists for — the
2026-08-03 review estimated $2,388–2,865 for exactly this.

The same signature in USDC: Spot −400.38, Funding +400.00, and the +400 is
a single Earn redemption (row 7549, 2026-08-22) that Binance itself says
landed in Funding.

**One question settles it** — open the Binance app and read two numbers:

> Spot USDT and Funding USDT, right now.

- If Funding is roughly 0–450 and Spot holds the rest, the ledger's split
  is wrong by ~2,300 and the fix is the ingest: book `p2p:` and `pay:`
  against Funding (plus a migration for the 156 + 15 existing rows).
- If Funding really holds ~2,335, the split is right and Spot is missing
  ~1,900 of inflows — a different hunt entirely.

**Do not restate the opening position to make this go away.** That is what
happened on 2026-08-04 (plugs sized against a corrupted ledger) and again
with the twins this morning. Fix the split first, then restate once.

### 2b. The opening position is stale anyway

`opening:2:USDT` was sized on 2026-08-08 so the ledger would agree with
Binance. As of that date the ledger's Spot USDT reads **−23.19** — both in
today's ledger and in this morning's pre-dedupe backup. An opening row is
only true for the history that existed when it was written, and history
has landed since (today's Binance sync alone inserted 138 rows, and bank
statements were re-exported in August). ADR-020 designed openings to be
*restated* for exactly this; nothing has re-run `finances reconcile
opening` in four weeks. That is a second reason the current figure is not
evidence of anything.

## 3. `category_kind_mismatch` — 20 rows, two different stories

**Ten are the ADR-019 pairs again** (1765, 1766, 2479, 2595, 2596, 6980,
7197, 7269, 7291, 7699): the charge leg keeps its old category (Fees,
Leisure, Transport) after being promoted to `kind='transfer'`.
`pair_reversal`'s docstring says this is deliberate — "reports ignore
categories on `kind='transfer'` rows" — and that is true:
`SQL_NOT_CURRENCY_MOVEMENT` drops them by kind before the category is ever
consulted. Nothing is double-counted. The check should exempt
`kind='transfer'` rows; same fix batch as §1.

**Ten are real questions for you** — money that came *in*, filed under an
*expense* category:

| id | date | amount | category | description |
|---|---|---|---|---|
| 23 | 2025-11-06 | +2,261 Bs | Other Expense | DR OB 04149578152 102BAN |
| 55 | 2025-11-15 | +5,000 Bs | Other Expense | DR OB 04143702427 191NAC |
| 107 | 2025-11-23 | +10,000 Bs | Leisure | TRAV0014270401000011818 |
| 170 | 2025-12-02 | +4,950 Bs | Other Expense | DR OB 04245566932 102BAN |
| 265 | 2025-12-20 | +2,600 Bs | Leisure | ABO.DRV0027142544 |
| 266 | 2025-12-21 | +5,800 Bs | Transport | ABO.DRV0027142544 |
| 313 | 2025-12-28 | +2,000 Bs | Other Expense | ABO.DRV0027142544 |
| 395 | 2026-01-29 | +2,000 Bs | Other Expense | DR OB 04149578152 102BAN |
| 750 | 2026-04-01 | +3,350 Bs | Other Expense | ABO.DRV0024354755 |
| 6138 | 2026-04-11 | +50 USDC | Gifts | Binance deposit USDC |

They read like reimbursements — somebody paid you back and the category
records what it was *for*. If that is right, the ledger needs an income
category for it (*Loan Repayment* exists) or these want the same
"borrowed money" treatment as the sitting-E cases. Your call; nothing here
is broken until you say what they are.

## 4. `uncategorized_not_flagged` — 7 rows of backlog

Native-USD rows, so the rate resolver never flags them, and no category
was ever set — invisible to every surface that reads `needs_review`:

| id | date | amount | what |
|---|---|---|---|
| 7555 | 2026-08-15 | −580 USDT | The $580 cash conversion (decisions doc §1) |
| 1117 | 2026-05-06 | −300 USDT | Binance Pay send |
| 1118 | 2026-05-05 | −400 USDT | Binance Pay send |
| 1119 | 2026-05-01 | −300 USDT | Binance Pay send |
| 5742 | 2025-10-21 | +49.18 USDT | P2P BUY |
| 7420 | 2026-08-13 | +45.09 USDT | P2P BUY |
| 7419 | 2026-08-24 | +90 USDC | Binance deposit |

The three May sends (1,000 USDT in six days) have the shape of the cash
conversions and of the lending cases — worth a look while sitting E is
open. The two P2P BUYs are currency movement and probably want *Internal
Transfer*.

## 5. `unpaired_p2p_sells` — 4 rows the check should not be showing

855, 1084, 5779, 5980 — all four already carry **Internal Transfer**
(category 17), so `SQL_NOT_CURRENCY_MOVEMENT` already excludes them from
spending. The check's own description says "each is a currency conversion
still counted as an expense", which is no longer true of these rows. The
`convert_leg_without_counterpart` check solved the identical problem by
excluding rows that had since been paired ("without this clause the check
would report finished work forever"); this one needs the same clause for
movement categories.

---

## What to do, in order

1. **Read two numbers off the Binance app** (Spot USDT, Funding USDT).
   Everything in §2 hangs on that, and no code should move first.
2. Then, if the split is confirmed wrong: an ADR for booking `p2p:`/`pay:`
   against Funding, the ingest change, and a migration moving the 171
   existing rows — followed by **one** opening restatement, not before.
3. Independently and cheaply: teach the three checks about ADR-019 and
   about movement categories (§1, §3, §5), so `doctor --strict` becomes
   usable again. Roughly one SQL clause each, tests first.
4. When you have a moment for triage: the ten reimbursement-shaped rows
   (§3) and the seven uncategorised ones (§4).
