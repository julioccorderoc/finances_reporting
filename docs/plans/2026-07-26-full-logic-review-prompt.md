# Prompt — full logic review of `finances_reporting`

Paste everything below the line into a fresh session, run from the repo root.

---

## Your job

Audit the **logic** of this personal-finance ledger end to end and tell me what is
wrong. Not style. Not test coverage percentages. Not naming. **Whether the numbers
this system produces are true.**

Read `CLAUDE.md` first, then `docs/PRD.md`, then the ADRs under `docs/ADR/`. They
describe what the system is *supposed* to do. Your job is to find where the code,
the data, or the design fails to deliver that.

## Who you are talking to

I am the sole owner and user. I am not a software engineer. I built this by
prompting, and I do not have the context to catch a subtle error by reading a diff
— which is exactly why I am asking for this review. Several real defects have gone
unnoticed for months because everything looked fine on screen and the test suite
was green.

So:

- **Do not assume I validated a design decision just because it is in the code.**
  Much of it I accepted without understanding the trade-off.
- **Explain findings from first principles.** Assume no context. If you write
  "the resolver falls back to BCV", also say what that means for my numbers.
- **Prioritise by money.** A defect that misstates a total by thousands outranks
  ten tidy-code observations. Tell me the size of each error in dollars or
  bolívares where you can compute it.

## The system in one paragraph

SQLite is the source of truth (`finances.db`, gitignored, ~1 900 transactions).
Data arrives from the Binance API, Provincial bank statements (HTML files named
`.xls`), a BCV rate scrape, and a cash CLI. Everything is normalised through
Pydantic models into one `transactions` table. Reports aggregate it; a local
FastAPI + HTMX viewer displays it and writes back categorisations, rates and
pairings. I live in Venezuela, so most spending is in bolívares while my wealth
is held in USDT — the exchange-rate handling is the most consequential logic in
the project, and the least verified.

## Deliberate quirks — do not report these as bugs

- **SQLite is truth; Google Sheets is a read-only mirror.** Never written by hand.
- **Expense amounts are negative.** `net = income + signed_expense`.
- **Headline USD conversion uses the USDT/P2P rate, never BCV.** BCV is the
  official government rate and is reference-only, displayed but never used for
  valuation. This is intentional and is the single most important rule (ADR-005,
  ADR-013).
- **Transfers are two rows** on different accounts sharing a `transfer_id`,
  summing to zero. Never one row. Transfers are excluded from income/expense
  aggregation.
- **`needs_review` means one narrow thing:** the importer could not determine a
  category. It is not a general "something is wrong" flag.
- **P2P pairing optimises totals, not identity.** Bank statements carry no
  transaction id, so which deposit belongs to which sell is unknowable; the
  matcher consumes N sells against N deposits and leaves the remainder visible.
  See the 2026-07-26 amendment in ADR-002. Do not report the lack of exact
  linkage as a defect — but *do* challenge whether the consequences were fully
  thought through.
- **`legacy/` is read-only reference.** Do not evaluate it.
- **Categories preserve my own historical distinctions** (Dating, Gifts, Family
  are deliberately separate). Do not propose collapsing them.

## Already known — confirm or correct, don't rediscover

Verify each briefly, then move on. If you think any diagnosis is wrong, say so.

1. **`domain/transfers.py` `validate()` is broken for cross-currency pairs.** It
   computes `amount * user_rate` on both legs; for a bolívar leg that yields
   VES², not USD. All 107 cross-currency transfers report invalid. Nothing in the
   write path calls it, so it is dormant. The real blocker is that `user_rate`
   has no recorded direction — nothing says whether it means "VES per USD" or the
   inverse, and it is stored on rows of both currencies. **Tell me what the
   semantics should be**; that is the decision I have been unable to make.
2. **Five orphaned Binance convert legs** (~5 354 USDC of phantom expense).
   Legacy backfill hashed each leg of a USDC→USDT conversion into its own
   `source_ref`, so the halves never shared an order id and the incoming side was
   never recorded. Descriptions still show `→ 0 UNKNOWN`. Surfaced by
   `finances doctor`.
3. **Same-account currency conversions cannot be modelled.** A USDC→USDT swap
   inside Binance Spot is one account, but `create_transfer` requires two
   different accounts. Today it lands as an expense row plus an income row, both
   counted. Is the double-entry model wrong here, or the account model?
4. **The web test fixture stores expense amounts as positive** while production
   stores them negative. Any sign error in the viewer is invisible to those
   tests. Check what this masks.
5. **31 unpaired P2P sells.** Believed to be missing bank statements rather than
   a code defect. Confirm.

## Where to look hardest

Ranked by how much money rides on it:

1. **`domain/rates.py` + `domain/realized_rates.py`** — the four-branch resolver
   and realized cost basis (ADR-005, ADR-013). Every USD figure depends on these.
   Is the 14-day carry sound? What happens at the boundary? Can a stale rate
   silently misprice a month?
2. **`reports/monthly.py`, `reports/consolidated_usd.py`** — do the aggregations
   exclude transfers correctly, handle multi-currency accounts, and treat signs
   consistently? Compute a month by hand from raw rows and compare.
3. **`domain/transfers.py`** — the pairing strategy and `create_transfer`'s three
   modes. Can any path create a pair that does not net to zero?
4. **`ingest/*.py`** — is `source_ref` genuinely deterministic per rule-010? Would
   re-ingesting the same file twice truly insert zero rows? Test it.
5. **`web/services/`** — the write-back paths. Can the viewer put the DB into a
   state the CLI would reject?

## Method

- **Query the real database.** Read-only `SELECT` against `finances.db` is
  expected and encouraged — that is where the evidence is. It has real history;
  use it.
- **Do not run any ingest, backfill, sync or write command.** Ask me first,
  always. If you want to test a mutation, copy the DB and work on the copy.
- **Run `finances doctor`** — it checks the ledger's invariants against real data.
  Then ask what it *fails* to check, and tell me.
- **Verify before asserting.** If you claim a total is wrong, show the query and
  the number. I have been given confident wrong answers before; a finding without
  evidence is worse than no finding.
- **Look for the same class of bug twice.** The two worst defects found so far
  were both "one fact expressed in many hand-written SQL sites, which then
  drifted". Search for other instances of that shape.

## Deliverable

A written report, ordered by financial impact:

- **What is wrong** — plain language, with the query and number that proves it.
- **What it costs me** — in money, or in decisions I would get wrong.
- **What you recommend** — and what it would take.
- **What needs my decision** — anything where the correct behaviour is a
  judgement call, not a fact. State the options and your recommendation.

End with the three things you would fix first, and why those three.

**Do not fix anything during this review.** Report first. I will choose.
