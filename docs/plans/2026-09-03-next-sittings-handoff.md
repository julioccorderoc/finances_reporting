# Handoff — the ledger-action sittings (2026-09-03)

Five sittings, in order. Each block below is self-contained: paste it into
a fresh session, run from the repo root. Read `CLAUDE.md` first in every
one; work in a worktree (`EnterWorktree`); TDD per rule-011 (`uv run
pytest -q`, judge by dots); never open the live `finances.db` from the
suite; drive Playwright before calling any viewer change done; scratch
copies of the ledger via the sqlite backup API over `file:…?mode=ro`
(recipe in `2026-09-03-viewer-reskin-handoff-prompt.md`). One gotcha for
every sitting that writes: `get_connection()` is autocommit — a rollback
after `create_transfer` / `apply_edit` writes for real; dry-run on a copy
(`scripts/record_cash_conversions_2026_09_03.py::_scratch_copy` is the
pattern).

Owner answers that fix the scope (2026-09-03, on
`2026-09-03-ledger-actions-decisions.md`): **duplicates — delete them;
set-balance — build it; manual transactions — build the form for every
account, only Cash enabled; cash conversions — he wants to do them himself
in the UI, design still open; borrowed money — the cases in sitting E.**

---

## Sitting A — Delete a transaction (ADR-022), then remove the ten Binance twins

> **Done 2026-09-03.** ADR-022 Accepted; migration 023, `transactions_repo.delete`,
> the ingest skip, the Flow-modal control and the `doctor` check shipped
> (commits `274a5c8` tests, `b690cd4` implementation), and the repair ran on the
> live ledger after `backups/finances-20260903-161237-pre-dedupe.db`:
> 3,009 → 2,999 rows, ten tombstones, Spot USDT opening 5,637.7657508 →
> 3,377.0457508, every Binance balance byte-identical before and after.
> November 2025 spending falls from $3,830.08 to $1,794.86; Dec −$25,
> Jan −$4, Mar −$176.50, Apr −$20. Script:
> `scripts/dedupe_binance_pay_twins_2026_09_03.py` (dry-run by default).
> Two deviations from the brief, both forced by the live data and written up
> in `ERRORS.md`: the twin guard compares **Caracas** days (five of the ten
> twins sit on the next UTC day) and matches across **any Binance account**
> (Pay history always reports Spot; five legacy rows are on Funding).
> Left open: the Flow modal is also reachable from the dashboard, where a
> delete now removes the card but the rest of the page (KPI tiles, chart)
> still shows pre-delete figures until a reload.


Owner said yes to deleting duplicates. ADR-022
(`docs/ADR/ADR-022-deleting-a-transaction.md`, Status Proposed) is the
design: a real `DELETE` plus a tombstone `(source, source_ref)` that
`upsert_by_source_ref` honours, so a re-import never resurrects the row.
Flip it to Accepted with the date, then build in this order, tests first:

1. Migration `023_deleted_transactions.sql` (table in ADR-022 §2.1) +
   `tests/test_migration_023_*.py`.
2. `transactions_repo.delete(conn, txn_id, *, reason)` — tombstone +
   delete in one transaction, returns the JSON snapshot; refuses a paired
   row (`transfer_id` set) and `source in ('reconciliation',
   'opening_balance')`; `source='cash_cli'` rows delete without a
   tombstone (ADR-022 §2.2).
3. `upsert_by_source_ref` skips tombstoned refs and reports
   `rows_skipped_deleted`; an integration test ingests a statement,
   deletes a row, re-ingests, sees 0 new.
4. Viewer: `POST /_partial/transactions/{id}/delete`; a ghost **Delete**
   button in the Flow modal footer left of Cancel, confirm text from
   ADR-022 §2.4, toast, row removed from the list, `closeModal`.
   Playwright it.
5. `finances doctor`: list tombstones whose ref is back in `transactions`.

Then the data repair, on a scratch copy first, then live after
`finances backup --label pre-dedupe`:

- Delete these ten `pay:` rows (each is a twin of a legacy
  `withdraw:hash:` row that carries the real meaning; table in
  `2026-09-03-ledger-actions-decisions.md` §2): **5775, 5774, 5773, 5799,
  5798, 5866, 6137, 6136, 6135, 1120**. Total 2,260.72 USDT.
- Restate the Binance Spot USDT opening position (row 7416,
  `source='opening_balance'`, 5,637.7657508 USDT) down by 2,260.72 —
  through `finances reconcile opening` / `domain/opening_positions.py`
  (ADR-020: restatement by stable `source_ref`), never by hand. The Spot
  balance must not move; assert before/after.
- Add the guard that stops it recurring: the Binance ingest skips a
  `pay:` event whose amount, currency and calendar day match an existing
  `withdraw:` row on the same account (test with the 5775/859 shape).
  Note in `ERRORS.md`.

## Sitting B — "Set balance" on the Accounts page (ADR-018 gets a surface)

Owner: "let's make it happen". The domain already has it:
`finances/domain/reconciliation_adjustments.py` writes one
`kind='adjustment'` row per (account, currency) for the gap between the
ledger and the custodian's figure, dated the day it is done (ADR-018
§2.1; row 7405 on Cash USD is one). Nothing calls it from the viewer or
the CLI's `reconcile` group.

Build, tests first:

1. A **Set balance** control on each account card (`partials/account_card.html`,
   `pages/accounts.html`, reports.css on signal tokens, no Tailwind): a
   number field pre-filled with the ledger figure, the currency fixed by
   the account.
2. `POST /_partial/accounts/{id}/reconcile/preview` → a raised panel:
   the difference, and **what could explain it before you plug it** —
   unpaired rows, same-day same-amount twins (the Sitting A lesson),
   uncategorised rows, rows priced `*_nearest`, all in the last 60 days
   on that account, each a link to its modal. A note field (required).
3. `POST /_partial/accounts/{id}/reconcile` → `reconciliation_adjustments`
   writes the row; toast; card re-renders; `HX-Trigger` refreshes the
   Today tiles if present.
4. The plug stays visible: Today's needs-you card (or a new line) says
   "N adjustments · $X unexplained since <date>", and `finances doctor`
   lists them. ADR-018 §2 already argues this; the surface makes it real.
5. Dating is today, never the ledger start (that is ADR-020's CLI path).
   Write a short ADR-018 amendment recording the viewer surface and the
   guard rails; `design_handoff_triage/NOTES.md` discipline for the notes.

## Sitting C — Add a transaction from the viewer (every account shown, only Cash enabled)

Owner: "build it as if it were for everything, but only cash available to
act on". Today the only manual write is `finances cash add` (USD cash
**expense** only, `finances/ingest/cash_cli.py`).

Build, tests first:

1. An **Add transaction** control in the Flow page header's actions slot
   (`page_header` call block). A modal in the house dialog style
   (`.flow-modal-*`): account select listing **every** active account,
   with every non-cash account disabled and a hint ("fed by its
   statement / the API"); date; expense/income choice; amount; currency
   fixed by the account; description; category picker scoped to the kind
   (`picker_payload(conn, kind=…)`); note.
2. Write path: extend `cash_cli` with `add_cash_income` beside
   `add_cash_expense` (same `source='cash_cli'`, UUID `source_ref`,
   rule-010) and route both through it; `POST /_partial/transactions`
   returns the new row's card and pushes it into the list (or a toast
   with a link when the filter hides it).
3. Server-side refusal of any non-cash account (422, plain words), so the
   disabled option is a courtesy, not the guard.
4. ADR-008 amendment, one paragraph: the cash account is written by the
   cash module from the CLI **or the viewer**, never by an importer; other
   accounts stay import-only until a real case appears (owner decision
   2026-09-03). Update rule-008.

## Sitting D — Cash conversions in the UI (design open — think first)

Owner wants to record "I changed USDT / bolívares into dollar bills"
himself, in the viewer, and was not sure about the "Became cash" button
proposed in `2026-09-03-ledger-actions-decisions.md` §3. Facts:

- The shape is settled by the data: rows 859/5740 and 863/5741 are the
  2025 conversions, double-entry transfers (rule-002, ADR-017 positions)
  with a `Cash USD` leg, refs `cash:binance-send:<id>`.
- Two are unrecorded: **7555** (−580 USDT, 2026-08-15, he got $580) and
  **7692** (−36,000 VES, 2026-08-31; the dollars received are still
  unknown — ask). `scripts/record_cash_conversions_2026_09_03.py` does
  both through `create_transfer`; dry-run on a copy is green. Do not run
  it live unless he says so — he prefers the UI.
- API trap: `create_transfer` anchor-only mode copies ONE amount to both
  legs. For a cross-currency conversion insert the cash leg via
  `transactions_repo.insert`, then pair in both-anchors mode; set the
  bank row's `user_rate` to amount ÷ dollars received (ADR-015) so
  `transfers.validate` passes.
- Rule friction: ADR-008 (cash is CLI-only) — Sitting C's amendment
  covers "transfer pairing may write the cash leg" if written broadly.

Decide with him: is it a control in the row's modal ("Became cash",
one field: dollars received, pre-filled with the row's USD value), or a
step inside Sitting C's Add-transaction form ("this is the other side
of…")? Either way it is `create_transfer` + a Cash leg, and Sitting A's
delete is what undoes a mistake.

## Sitting E — Borrowed money, P2P edge cases, and who is who

Read `2026-09-03-borrowed-money-findings.md` first (Option 1: a
transfer-kind `Borrowed` category; the open question on
`Lending`/`Loan Repayment`). The owner then described his real cases;
every row id below was checked read-only. Decide **with** him, then the
migration + doc row + tests as in that document.

**Who is who** (from his notes on other rows): `V033404180` /
`V33404180` = Natalia, his sister ("pago cuota natalia", "5 dolas
natalia"); `V014648189` / `0014648189` / `V14648189` = Yaribel ("deuda
yaribel", "cuota cashea para yaribel lavadora", "prestamo $60 yaribel");
`04165936089` shows up as Gigs / External Transfer (a company?). Confirm
with him before relying on it.

1. **Company purchase reimbursed in USDC.** Row **7659** (−70,195.50 VES,
   2026-08-21, `DR OB J50516215 105MERCA`, uncategorised) is him buying
   something for the company; row **7419** (+90 USDC, 2026-08-24,
   `Binance deposit USDC`, uncategorised) is the reimbursement. Not a
   transfer pair (≈$75 vs $90, outside the 2% tolerance). Fits
   `Lending` (expense) + `Loan Repayment` (income) as the definitions
   read today; the $15 over is his to name (income? price difference?).
2. **Mom lends 10,000 Bs, he pays back 10,000 Bs.** Two candidate pairs,
   he must pick: **1731** (+10,000, 2026-05-29, `TRAV0014648189…`,
   uncategorised) with **1736** (−10,000 same day, `CAR.DRV0014648189`,
   filed Family "aniversario abuelos"); or **7564** (+10,000, 2026-08-15,
   `DR OB 04165936089 102BAN`) with **7671** (−10,000, 2026-08-17,
   `DR OB V14648189 102BANCO`, uncategorised — its same-day twin 7673 is
   Health "peridont farmatodo"). Once picked: both legs → `Borrowed` if
   Option 1 is accepted; otherwise `External Transfer`.
3. **Sister buys USDT from him.** Row **7669** (+73,283.60 VES,
   2026-08-17, from Natalia) is her paying; row **7654** (−9,100 VES,
   2026-08-22, to Natalia) is what he gave back. The USDT he sent her is
   **not in the ledger** for Aug 15 – Sep 3 (the only outgoing Binance
   rows are the paired P2P sells 7427/7424 and Pay rows he has named).
   Ask where and when the USDT went. The honest shape is the P2P one: a
   bank deposit paired with a USDT outflow (`create_transfer`
   both-anchors, `user_rate` = 64,183.60 ÷ USDT sent), which also feeds
   the realized cost-basis tier (ADR-013) — same as any P2P sell. The
   9,100 is either part of the price (then the pair prices the net) or a
   separate transfer to her; he decides. **The 70,000 on 2026-05-17 (row
   1782, same sender)** has no matching P2P sell in May — every May sell
   is small and already paired — so it is probably the same arrangement
   two months earlier; ask.
4. **"P2P BUY USDT @" rows are transfers Binance ↔ Provincial.** Only
   two exist, both unpaired, kind income, no category: **7420** (+45.09
   USDT @ 887 on 2026-08-13 → look for a −39,995 Bs bank debit that day)
   and **5742** (+49.18 USDT @ 305 on 2025-10-21 → −15,000 Bs). The
   bank-anchored strategy (`domain/transfers.py::BankAnchoredP2pPairing`)
   pairs **deposits with sells** only, and the manual picker
   (`pair_candidates.html`) only opens for outgoing Binance rows. Extend
   both to the buy direction (debit ↔ buy), tests first; the two rows
   above are the fixture.
5. **The two deposits he is sure are P2P receptions.**
   - **6940** (+20,000, 2026-05-10, `TRAV0031264379000127958`): two
     identical 20,000 deposits arrived that day from the same sender;
     the pairer attached sell **1081** to the other one (6937) and then
     paired the next sell **1080** (2026-05-11, ≈19,996 Bs) with **6935**
     (+20,018.42 from `04165936089`, an odd amount for a P2P deposit —
     see the "P2P deposits are round numbers" memory). Likely fix:
     1080 ↔ 6940, and 6935 goes back to being whatever it is. That needs
     a **break-a-pair** action, which does not exist (the triage
     "refuse" only dismisses a proposal). Build it in this sitting:
     `transfers.unpair(conn, transfer_id)` restoring both rows' original
     kind (income/expense from the sign) and clearing `transfer_id`, with
     a `doctor` check, then use it here.
   - **1782** (+70,000, 2026-05-17, from Natalia): no sell matches — see
     item 3; it is not a Binance P2P reception, it is his sister.
6. Then the category work from the findings document: 1871/1869 (Hugo)
   → `Borrowed`; the thirteen unclaimed deposits sorted one by one in
   Triage with him; the Yaribel washing-machine thread given one answer.

Deliverable for E: his decisions written into
`docs/architecture/category-definitions.md` (a row and a test per
category, a `## History` line), migration 023/024 as needed, the buy-side
pairing and the unpair action with tests, and the repairs above run on a
copy first, then live after a `finances backup`.
