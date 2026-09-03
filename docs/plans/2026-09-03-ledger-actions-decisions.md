# Ledger actions — what you asked for on 2026-09-03, and what each needs from you

Five requests arrived while the reskin was being finished. Each touches a
rule the ledger lives by (dedup on re-import, transfers summing to zero,
"cash is CLI-only"), so per CLAUDE.md rule 4 nothing below is implemented
without your yes. Everything was checked read-only against the live
ledger; nothing was written.

**Answer these and the rest follows:**

1. Cash conversions — how many dollar bills did you get for the
   **36,000 Bs on Aug 31**? Then say **go** and the repair runs.
2. The ten duplicate "Binance Pay" rows — **yes/no** to ADR-022 (delete
   with a tombstone), which is what makes removing them safe.
3. "This became cash" as a button in the viewer — **yes/no**.
4. "Set the real balance, create the difference" in the viewer — **yes/no**,
   with the guard rails in §4.
5. Manual transactions in the viewer — **yes/no**, and whether they may
   land on bank/Binance accounts too, or only on Cash.
6. Borrowed money and Lending — the two questions at the end of
   `2026-09-03-borrowed-money-findings.md`.

---

## 1. Changing money into cash — what is true today

- **The two 2025 changes are already recorded, correctly.** Rows 859
  (−700 USDT, "Cambio $700 efectivo Jorge", 2025-11-05) and 863 (−500
  USDT, "Cambio $500 efectivo Terán", 2025-11-09) are transfer legs paired
  with Cash USD rows 5740 (+$700) and 5741 (+$500). They came in with the
  backfill, from your old sheet's descriptions. Cash USD reads $2,184.00
  today, which is what you counted on 2026-08-08. Nothing to add here —
  you were right that these must not be entered again.
- **The two rows in your screenshot are duplicates of them.** 5775
  (−700 USDT, 2025-11-06) and 5774 (−500 USDT, 2025-11-10) are the same
  two sends, fetched a second time by the live sync from Binance's *Pay*
  history (`pay:…`), while the backfill had them from the *withdraw*
  history (`withdraw:hash:…`). Different reference, same money. §2 has
  the full list — there are ten of these.
- **Two conversions are genuinely unrecorded:** the $580 on 2026-08-15
  (row 7555, Binance Pay) and the 36,000 Bs on 2026-08-31 (row 7692,
  Provincial). Note there are two 36,000 Bs debits that day: 7700 failed
  and was reversed the same day (it is paired with its REVERSO CARGO,
  ADR-019); 7692 is the one that went through.

### The repair, ready to run

`scripts/record_cash_conversions_2026_09_03.py` turns each of the two
rows into a transfer with a new Cash USD leg carrying the dollars you
received — exactly the shape rows 859/5740 already have. It goes through
`transfers.create_transfer` (the one write path for transfers) and sets
the bank row's rate to the price you actually got (36,000 ÷ dollars), so
the two legs cancel exactly. Dry-run by default; the dry run on a copy of
the ledger is in the report you got with this document. Effect on the
figures: August spending drops by **$618.83** (the $580 and the $38.83 stop
counting as expenses), Cash USD rises by the dollars you received, net
worth is unchanged.

I need one number: **the dollars you received for the 36,000 Bs** (at the
row's own rate it would be $38.83). Then: go.

## 2. Ten "Binance Pay" rows are twins of legacy "Binance send" rows

| Pay row (live sync) | Legacy row (backfill) | amount | what the legacy row says |
|---|---|---|---|
| 5775 · 2025-11-06 | 859 | −700 USDT | Cambio $700 efectivo Jorge — paired with Cash |
| 5774 · 2025-11-10 | 863 | −500 USDT | Cambio $500 efectivo Terán — paired with Cash |
| 5773 · 2025-11-26 | 893 | −193.22 USDT | Compras en TeLoComproEnUSA (Purchases) |
| 5799 · 2025-11-30 | 912 | −642 USDT | Pago parcial de iPhone (Purchases) |
| 5798 · 2025-12-21 | 943 | −25 USDT | Counterparty 227985716 (Lending) |
| 5866 · 2026-01-01 | 963 | −4 USDT | Suscripción Netflix Enero (Subscriptions) |
| 6137 · 2026-03-04 | 1031 | −142.50 USDT | Counterparty 205781774 (Lending) |
| 6136 · 2026-03-05 | 1035 | −30 USDT | Counterparty 205781774 (Lending) |
| 6135 · 2026-03-06 | 1036 | −4 USDT | Counterparty 205781774 (Lending) |
| 1120 · 2026-04-19 | 1076 | −20 USDT | Counterparty 386971640 (External Transfer) |

Total counted twice: **2,260.72 USDT**. Six of the ten Pay twins carry a
category of their own (Purchases, Other Expense, Subscriptions, Loan
Repayment), so spending in those months is overstated by up to that
much; four sit in Triage waiting for a category they must never get.

Why the balance still looks right: on 2026-08-08 the Binance Spot
opening position (ADR-020) was computed to make the ledger match Binance
— it silently absorbed the 2,260.72 (this is the "adjustment rows mask
dupes" lesson from that day, happening again).

**What fixes it, in order, once you say yes to ADR-022:**

1. Delete the ten Pay rows with tombstones, so a deep re-sync of Binance
   can never bring them back (that is what the tombstone is for).
2. Restate the Spot USDT opening position down by 2,260.72
   (5,637.77 → 3,377.05). ADR-020 made opening rows restatable by their
   stable reference for exactly this. Binance Spot's balance does not
   move — the twins and the compensation cancel — but the history becomes
   true and the monthly figures stop double-counting.
3. Keep both source shapes honest going forward: the sync should skip a
   `pay:` event whose amount and day match an existing `withdraw:` row
   (a small guard in the Binance ingest, with its test). Without it the
   next deep re-sync recreates the same ten twins.

## 3. "This became cash" — a button, not a script, next time

Today the only way to record a conversion is the script above. Proposal:
in the Flow modal of any outgoing Binance or bank row, a **Became cash**
control asking one thing — *dollars received* (pre-filled with the row's
USD value) — that does what the script does: promote the row, insert the
Cash USD leg, set the struck rate on a bolívar row, pair. Toast, row
becomes a transfer, disappears from Triage.

Rule friction: ADR-008 / rule-008 say Cash USD is written **only by the
CLI**. The 2025 cash legs were already written by the backfill, and this
is the same machinery (`create_transfer`), so the honest change is a
one-paragraph amendment to ADR-008: *the cash account is written by the
cash module and by transfer pairing, from the CLI or the viewer; never by
an importer.* I will draft it with the feature if you say yes.

## 4. "Set the real balance, create the difference" — yes, with guard rails

This already exists in the domain: ADR-018's **reconciliation
adjustment** (`finances/domain/reconciliation_adjustments.py`) writes one
`adjustment` row per account and currency for the gap between the ledger
and what the custodian shows, dated the day you did it. The Cash USD row
7405 (+$984 on 2026-08-08, "ledger 1200, custodian 2184") is one. It just
has no surface: the CLI's `reconcile` group only has `converts`,
`opening`, `legacy-dupes`, `reversals`.

Proposal: on the **Accounts** page, each card gets a **Set balance**
control: you type what the bank/Binance/your pocket shows today, the
viewer shows the difference, and a second click writes the adjustment
row with a note you must fill ("counted cash", "Binance app says…").

Two guard rails, because of what §2 just showed — an automatic plug hides
real errors:

- Before writing, the viewer lists what could explain the gap: unpaired
  rows, same-day same-amount twins, uncategorised rows, rows in the last
  30 days flagged approximate. You confirm the plug knowing that.
- Every adjustment row shows on Today ("2 adjustments, $1,012 unexplained
  since…") and in `finances doctor`, so a plug is a visible IOU to the
  history, never a silent fix. ADR-018 already argues this; the surface
  makes it real.

Dating: today (ADR-018 §2.1), never at the ledger's start — that case is
ADR-020's and stays a CLI command.

## 5. Manual transactions

Today: `finances cash add --amount 12 --description "lunch"` records a
**USD cash expense** and nothing else — no cash income, no manual row on
a bank or Binance account, and nothing from the viewer.

Proposal: an **Add transaction** control in the Flow header: account,
date, amount (sign from an expense/income choice), currency (fixed by the
account), description, category, note. Written through the existing
repo with `source='manual'` and a fresh UUID reference (rule-010 allows
exactly this; `transactions.source` is free text by design).

The one thing to decide: **may a manual row land on a bank or Binance
account?** Those accounts are fed by statements and the API, so a manual
row is a promise that the import will *never* bring the same movement —
if it does, you get a twin (§2 again). Two honest answers:

- **Cash only** (safe): manual rows only on Cash USD. Everything else
  waits for its statement.
- **Any account, flagged**: allowed, but `finances doctor` and Triage show
  a manual row that has a same-day same-amount imported neighbour, so a
  twin is caught the day the statement lands. More useful; needs the
  twin check built first.

My recommendation: **cash only now**, and revisit when a real case on a
bank account shows up — every bank movement you have named so far did
arrive in a statement.

## Owner answers (2026-09-03, same day)

1. Cash conversions: "I want to be able to do this myself on the UI" —
   the script stays unrun; the UI design is open (handoff sitting D).
2. Duplicates: "Delete duplicated stuff" — yes to ADR-022 (sitting A).
3. "Became cash" button: not sure, another session (sitting D).
4. Set balance: "let's make it happen" (sitting B).
5. Manual transactions: "build it as if it were for everything, but only
   cash available to act on" (sitting C).
6. Borrowed money: real cases given — company purchase repaid in USDC,
   mom's 10,000 Bs in/out, sister buying USDT (73,283.60 in, 9,100 back),
   "P2P BUY" rows are transfers, two deposits he is sure are P2P
   receptions (sitting E, with the row ids found).

The sittings are laid out in `2026-09-03-next-sittings-handoff.md`.

## 6. Already written up elsewhere

- **Delete a transaction** — ADR-022, proposed. §2 is the first concrete
  use of it.
- **Borrowed money** — `2026-09-03-borrowed-money-findings.md`.
