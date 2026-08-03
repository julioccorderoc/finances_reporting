# Prompt — model same-account currency conversions

Paste everything below the line into a fresh session, run from the repo root.

---

## The task

Binance USDC→USDT conversions are recorded as an expense row *and* an income
row, and both are counted in my reports. The money never left my possession —
it only changed denomination. Fix how the ledger models this.

Read `CLAUDE.md` first, then `docs/ADR/ADR-002-double-entry-transfers.md`.

## Who you are talking to

I own this ledger and use it alone. I am not a software engineer — I built this
by prompting. Explain things from first principles and do not assume I
validated any existing design decision just because it is in the code.

## What is wrong, concretely

A conversion writes two rows, both on **Binance Spot**:

```text
convert:<order>:from   expense   −1 240.00 USDC
convert:<order>:to     income    +1 239.18 USDT
```

Reports exclude only `kind = 'transfer'`. These are `expense` and `income`, so
both are counted. Across the ledger:

| | rows | total |
|---|---|---|
| convert expense | 12 | −11 846.42 USDC |
| convert income | 12 | +11 844.61 USDT |
| **net** | | **−1.81** |

The net is right. The **gross is wrong**: roughly 11.8k of phantom expense and
11.8k of phantom income, which distorts every monthly income/expense figure
even though the bottom line survives.

## Why it was not simply made a transfer

`create_transfer` rejects it — [transfers.py:245](finances/domain/transfers.py#L245):

```python
raise ValueError("both-anchors legs must be on different accounts")
```

A conversion happens *inside one account*. The double-entry model (rule-002,
ADR-002) assumes a transfer moves money **between** accounts, so a conversion
cannot be expressed with the tools that exist. That is the real problem, and it
is a modelling question, not a bug.

## Constraints any fix must respect

- `kind` is constrained by the schema: `CHECK (kind IN ('income', 'expense',
  'transfer', 'adjustment'))`. Note `adjustment` **exists and is unused** —
  0 rows — and is **not** excluded from any report.
- Reports exclude transfers in exactly four places. Whatever you choose must be
  honoured in all of them, or the numbers will disagree with each other:
  - [migrations/001_initial.sql:212](finances/db/migrations/001_initial.sql#L212) (`v_monthly_summary`)
  - [reports/monthly.py:232](finances/reports/monthly.py#L232)
  - [reports/consolidated_usd.py:112](finances/reports/consolidated_usd.py#L112)
  - [domain/integrity.py:114](finances/domain/integrity.py#L114)
- `finances doctor` has a check `transfer_legs_same_account` at **ERROR**
  severity. If you allow same-account pairs it must become currency-aware, or
  it will condemn every conversion you create.
- `validate()` assumes two legs on different accounts.
- Balances must stay correct: per-account sums include transfers, and the
  USDC and USDT balances of Binance Spot must both still come out right.

## Approaches to weigh

Do not just pick one — think it through with me first.

1. **Allow same-account transfers when the currencies differ.** Smallest
   change. Weakens the "a transfer moves money between accounts" invariant, and
   the doctor check must learn the exception.
2. **A new `kind`, e.g. `conversion`.** Explicit and self-documenting. Costs a
   migration widening the CHECK, plus the exclusion in all four report sites.
3. **Reuse `kind='adjustment'`.** No migration — it is already permitted and
   unused. But "adjustment" means something else in accounting, and reports
   would still need updating.
4. **Per-asset sub-accounts** (Binance Spot USDC, Binance Spot USDT). The most
   textbook-correct double entry: a conversion becomes an ordinary transfer
   between two real accounts. Heaviest — it changes the account model and needs
   a migration remapping existing rows.

## Also fix while you are in there

Four rows — **891, 892, 910, 911** — are two conversions from Nov 2025 recorded
one leg per sheet row. Each leg hashed to its own `source_ref`, so the halves
never shared an order id:

```text
891  2025-11-23  income   +1 239.18 USDT  convert:hash:c5d9a3f91fe8725e:to
892  2025-11-23  expense  −1 240.00 USDC  convert:hash:d32d5d23f33ee0cd:from
910  2025-11-30  expense  −1 280.00 USDC  convert:hash:e793a7db2eabbe1b:from
911  2025-11-30  income   +1 278.77 USDT  convert:hash:9a3c752419f66eb0:to
```

Both halves exist and the money is accounted for — they are simply not linked.
`finances doctor` flags them under `convert_leg_without_counterpart`. Whatever
model you land on should be able to represent these two pairs.

## Process — this project is strict about it

- **Write the ADR first.** CLAUDE.md forbids changing an architectural rule
  without one. **ADR-014 and ADR-015 are already taken — yours is ADR-016.**
  The next free migration number is **017**.
- **Brainstorm with me before writing code.** Present the options, recommend
  one, explain the trade-off in plain language, and wait.
- **TDD is mandatory** (rule-011): the test commit lands before the
  implementation commit.
- **Run `finances doctor`** before and after. It must report **0 errors** at the
  end. It currently reports 0 errors and 2 warnings.
- **Verify against the real ledger, not just fixtures.** Copy `finances.db` and
  work on the copy. This project has shipped defects that every test passed —
  fixtures are not proof.
- **Do not run any ingest, backfill, sync or write command against
  `finances.db` without asking me.** Read-only `SELECT` is fine and encouraged.
  Back up before any write I approve.

## Important: I may have another session running

This repository is sometimes worked on by two sessions at once, and it has
already caused a branch mix-up and two ADRs claiming the same number. Before
your first commit, run `EnterWorktree` so you are isolated. Check
`git worktree list` and `git branch --show-current` before merging anything,
and confirm you are on `main` — not on someone else's branch.

## Deliverable

- ADR-016 recording the decision and the rejected alternatives, with reasons.
- The implementation, tests first.
- A short note telling me what changed in my numbers: gross monthly income and
  expense before and after, for a month that contains a conversion.
