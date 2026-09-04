# Borrowed money — the owner's decisions (2026-09-04)

Answers `docs/plans/2026-09-03-borrowed-money-findings.md` and the sitting
prompt beside it. Every row below was read first with `?immutable=1`, every
decision was taken by the owner in this session, and the writes ran through
`scripts/borrowed_money_filings_2026_09_04.py` — on a copy first, then on the
live ledger after `finances backup`.

## 1. Taxonomy

| Question | Decision |
|---|---|
| Where does money lent *to* him live? | **Option 1** — a new transfer-kind category `Borrowed` (migration 025). Neither income nor spending; what he still owes is what came in minus what went back. |
| Do `Lending` / `Loan Repayment` become transfer-kind too? | **No.** They keep their expense/income kinds. Money he lends out still counts as spending the month it leaves ($382 of May), and its return as income ($298 of March). Changing that would be an ADR first. |

The disambiguating test lives in `docs/architecture/category-definitions.md`
(`## Transfer` table) with a `## History` line, like every category before it.
No `category_rules` row: nothing in a bank string says "this is a loan" — the
sender's account number is the only signal, and it is a person, not a
merchant. It stays a triage decision.

## 2. Who is who

Confirmed by the owner, against every row each number appears on:

| Number | Who |
|---|---|
| `04165936089` | **His mother.** Six credits, never a payment out. The older `Gigs` (818) and `External Transfer` (218, 7271) filings predate the identification. |
| `V033404180` / `V33404180` | **Natalia**, his sister. |
| `V014648189` / `0014648189` / `V14648189` | **Yaribel.** |
| `V027142544` | *Moises de la playa*, per his own note on an earlier row. |

## 3. The rows, one by one

| Rows | Decision | Why |
|---|---|---|
| 1871, 1869 | `Borrowed` | Hugo lent 6,000 Bs and 6,000 Bs went back the same day. June was reporting $7.94 of income and $7.94 of spending against it. |
| 7564, 7671 | `Borrowed` | His mother's 10,000 Bs on 08-15, repaid on 08-17. (The May candidate pair — 1731/1736 — was **not** this loan.) |
| 7669, 7654 | `External Transfer` | The 73,283.60 Bs from his sister is money he is **holding for her**, not a P2P sale and not income. The 9,100 sent back on 08-22 is part of returning it; ~64,183 Bs of hers is still held. |
| 7573 | `External Transfer` | His mother's 44,477.90 Bs, forwarded, not earned. |
| 1731, 1736 | `External Transfer` | 10,000 in from Yaribel and 10,000 straight back out the same day. The `aniversario abuelos` note stays on 1736 as a note; it is not May spending. |
| 1628, 1629, 1910, 7258, 7344, 7348, 7382, 7717 | `External Transfer` | The Yaribel washing machine, one answer for all eight legs (four Cashea instalments out, four reimbursements in). It was filed under four different categories; none of it is his spending or his income. |
| 1782, 1715, 1964 | `Loan Repayment` | Natalia repaying a loan he granted her — matching 1849, *pago cuota natalia*, already filed that way. |
| 7188 | `Loan Repayment` | Yaribel repaying, like 7382's note *deuda yaribel*. |
| 1875 | `Loan Repayment` | *pago moises de la playa*, as the same sender was filed before. |
| 7659 | `Lending` | He bought something for the company (≈$75). |
| 7419 | `Loan Repayment` + note | The company reimbursing him, 90 USDC. The ~$15 over the purchase is named in the row's note; the ledger has no way to express "the same movement, plus fifteen dollars", and it is not a second movement. |
| 1906 | `Other Income` | +2,029.93 Bs from a number that appears nowhere else. ~$2.20. |

## 4. The 2026-05-11 mispairing

Two identical 20,000 Bs deposits arrived on 2026-05-10. The pairer took one
(6937) for sell 1081, then matched the next sell (1080, ≈19,996 Bs) against
**6935** — a 20,018.42 credit from his mother, an amount no P2P deposit has
(see the *P2P deposits are round numbers* note). The round 20,000 (**6940**)
sat unclaimed.

**Decision:** break that pair, pair 1080 with 6940, and let 6935 go back to
Triage as the ordinary deposit it always was.

`transfers.unpair` refuses it: it replays the pre-image migration 024 records
at pairing time, and **all 286 pairs in the ledger predate that table**. The
refusal is right in general — restoring a leg an importer created as
`kind='transfer'` to `expense` because the amount is negative invents history.
The owner chose the narrow fix over the broad one:

- **taken:** write the pre-image for *these two rows only*, where both are
  knowable — 6935 is a Provincial credit, which that ingest writes as
  `income`; 1080 is a `p2p:` sell, which `ingest.binance` writes as `expense`
  (`RawBinanceP2pRow.to_transaction`) — then let `unpair` do the rest.
- **not taken:** reconstructing pre-images for all 286 pairs, which would
  write 286 rows of history the ledger never observed.

**The trap this hit.** The first dry run wrote `prior_user_rate = NULL` and
`unpair` duly wiped the sell's 648.6. A bank-anchored pairing writes no rate
(a *cash conversion* does — ADR-015), so the pre-image rate for a P2P sell is
the order price the ingest recorded, and it must survive the break. Losing it
would have re-priced a fortnight of bolívar rows off the market median instead
of what those bolívars cost (ADR-013). The script now carries the rate and
asserts it afterwards.

## 5. What it changed

Twenty rows filed (five already carried the right category), the pairing
fixed, **every account balance unchanged** (a category never moves one), and
`doctor` one finding better (`uncategorized_not_flagged` 7 → 6).

| Month | Income before → after | Spending before → after |
|---|---|---|
| 2026-05 | 1,933.16 → 1,919.66 | −2,274.67 → −2,261.13 |
| 2026-06 | 2,159.94 → 2,152.00 | −1,370.60 → −1,362.66 |
| 2026-07 | 2,172.62 → 2,172.62 | −1,620.85 → −1,520.31 |
| 2026-08 | 2,422.91 → 2,236.23 | −2,275.16 → −2,253.67 |

August loses $186.68 of income that was never income, and July $100.54 of
spending that was never his.

## 6. What the sitting also built

- **The buy side of a P2P trade** (ADR-002 amendment 2026-09-04).
  `BankAnchoredP2pPairing` now matches a bank *debit* against a Binance
  *credit* carrying a rate, from one shared assignment, and the manual picker
  opens on incoming Binance rows too — but only where a `user_rate` exists to
  score against.
- **`doctor` check `stale_pair_pre_image`** — a pre-image describing a pairing
  its row is no longer in, which is what any path other than `unpair` leaves
  behind.

## 7. Still open

- **The two `P2P BUY USDT @` rows have no bank counterpart in the ledger.**
  7420 (+45.09 USDT @ 887, 2026-08-13) wants a ≈−39,995 Bs debit that day and
  there is none — the nearest is −44,529.37 on 08-14, to a different payee.
  5742 (+49.18 USDT @ 305, 2025-10-21) cannot be paired either: the ledger
  holds **no VES bank rows at all** for October 2025. Both stay unpaired and
  uncategorised until the owner says where those bolívars went; the machinery
  to pair them exists now.
- **6935** (+20,018.42 from his mother, 2026-05-11) is back in Triage,
  uncategorised, for him to sort like the other credits from her.
- **The sister's USDT** never left through this ledger, because there was no
  sale: he is holding her bolívars. Nothing to pair.
