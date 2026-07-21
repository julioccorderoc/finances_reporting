# Manual P2P pair picker — design

Date: 2026-07-21
Status: approved (Julio, 2026-07-21)

## Problem

56 Binance P2P sells carry no `transfer_id`, alongside 78 unpaired Provincial
income rows. `BankAnchoredP2pPairing` cannot close the gap on its own:

- **Uniqueness gate.** Repeated round 20 000 Bs deposits put 2–3 candidate
  sells inside the same ±2-day window, so the strategy skips rather than guess
  (`finances/domain/transfers.py`, `len(surviving) != 1`). This is correct
  behaviour and will never be automatable.
- **Drift.** Some legs sit outside the 2 % tolerance because the bank credited
  a different amount than the order's fiat total.
- **Bad `user_rate`.** Three sells carry a nonsense rate (e.g. `1.006`), so
  their expected Bs is off by three orders of magnitude.

A companion fix (commit `23416fa`) widened the candidate SQL to include orphan
`kind='transfer'` rows, taking the strategy from 0 proposals to 17. The
remaining ~39 need a human.

There is also no way to *find* the unpaired rows: `/transactions` has no filter
for `transfer_id IS NULL`.

## Scope

Manual pairing driven from the sell side, in the transaction detail modal, plus
a filter to isolate the backlog. Nothing else.

**Out of scope**, flagged for separate work:

- N:1 pairings (one sell paid by two deposits). `create_transfer` is strictly
  two legs per rule-002; splitting needs an ADR.
- `validate()`'s cross-currency arithmetic. It multiplies each leg by its own
  `user_rate` and expects the sum to be zero, but a VES leg times a VES/USDT
  rate yields millions. No existing binance↔provincial pair passes it, and 13
  of 66 provincial transfer legs have no `user_rate` at all.
- Repairing the three sells with junk `user_rate`.

## Design

### 1. `paired` filter

`TransactionsFilter` gains `paired: Literal["any", "yes", "no"] = "any"`,
mirroring `needs_review` exactly:

```python
if f.paired == "yes":
    where.append("t.transfer_id IS NOT NULL")
elif f.paired == "no":
    where.append("t.transfer_id IS NULL")
```

Touched: `finances/web/services/transactions_query.py` (model + SQL builder),
`finances/web/routers/_tx_filter_dep.py` (query parser), and
`finances/web/templates/partials/transactions_filters.html` (a `<select>` beside
needs_review).

Saved views persist the raw `query_string` (`finances/db/repos/saved_views.py`),
so `?sources=binance&paired=no` round-trips with **no migration and no repo
change**. "Unpaired P2P sells" becomes an ordinary saved view.

### 2. Candidate finder

New module `finances/web/services/pairing.py` — a new file rather than more
weight on `triage.py`, which is already the largest service.

```python
def find_pair_candidates(
    conn: sqlite3.Connection,
    *,
    sell_id: int,
    window_days: int = 2,
) -> PairCandidates
```

`PairCandidates` carries `sell` (a `TransactionCard`), `expected_ves`
(`abs(amount) * user_rate`, `None` when the sell has no `user_rate`),
`window_days`, and `candidates: list[PairCandidate]`.

Each `PairCandidate` wraps a `TransactionCard` plus:

- `drift_ratio: Decimal | None` — `abs(amount - expected_ves) / amount`, `None`
  when `expected_ves` is `None`
- `pairable: bool` — `False` when the candidate's amount shares the sell's sign
- `blocked_reason: str | None` — populated when `pairable` is `False`

Candidate set: every transaction with `source='provincial'` and
`transfer_id IS NULL` whose `occurred_at` falls within ±`window_days` of the
sell. **Both income and expense kinds**, deliberately — a deposit recorded under
the wrong kind must stay visible. Sorted by `drift_ratio` ascending with `None`
last, then by `occurred_at`. Each row carries its own weekday date label
(closest match first beats date grouping, and matches the UX-overhaul date
convention).

Pydantic models in and out per rule-009. No mutation.

### 3. Modal section

`modal_transaction.html` gains a PAIR WITH DEPOSIT block, rendered when the
transaction is unpaired, belongs to a Binance account, and has a negative
amount. Its body lives in `partials/pair_candidates.html` so it can re-render
standalone.

Two new routes in `finances/web/routers/partials.py`:

| Route | Purpose |
|---|---|
| `GET /transactions/{sell_id}/pair-candidates?window_days=N` | Render the candidate partial. Backs the "widen to ±7d" button as a plain `hx-get` swap. |
| `POST /transactions/{sell_id}/pair/{deposit_id}` | Call `confirm_pair()`, return the refreshed list partial with `HX-Trigger` closeModal + toast. |

The POST reuses the existing `confirm_pair()` service
(`finances/web/services/triage.py`), so `create_transfer` mode 3 remains the
single write path per rule-002. The existing triage routes are left alone —
they return the triage queue partial, which is the wrong swap target here.

### 4. Same-sign guard

`create_transfer` mode 3 infers the from/to leg from the two amounts' signs and
raises `ValueError` when they match (`finances/domain/transfers.py`, "both
anchors share sign"). A Provincial expense candidate is negative like the sell,
so it cannot be paired without an explicit `from_account_id`.

Those rows still render — visibility across the whole day was an explicit
requirement — but with `pairable=False`, a disabled button, and the reason
"same sign — not a deposit". No new disambiguation parameters are introduced.

### 5. Error handling

| Case | Behaviour |
|---|---|
| Sell has no `user_rate` | Candidates still list; `drift_ratio` renders as `—`; hint to set a rate first |
| Unknown `sell_id` / `deposit_id` | 404 |
| Either row already paired (e.g. a second browser tab) | `confirm_pair` raises `ValueError` → 422 → toast, list re-renders |
| No candidates in window | Empty-state row plus the widen button |

No silent failures.

### 6. Tests (rule-011, TDD — test commit precedes impl commit)

**Service** (`tests/web/test_pairing.py`, new)
- income and expense candidates both returned
- rows with a `transfer_id` excluded
- window boundary: ±`window_days` inclusive, one day beyond excluded
- sorted by drift ascending, `None` drift last
- `expected_ves is None` when the sell has no `user_rate`
- same-sign candidate marked `pairable=False` with a reason

**Filter**
- `paired="no"` returns only `transfer_id IS NULL`; `"yes"` the inverse; `"any"` everything
- an unrecognized value yields 422
- a saved view round-trips the `paired` parameter

**Routes**
- `GET .../pair-candidates` renders; `window_days` widens the set
- `POST .../pair/{deposit_id}` creates the transfer and returns the list partial
- 404 on unknown id, 422 when already paired
- a same-sign candidate's button renders disabled

## Verification

- `pytest -q` fully green (the suite takes >10 min; run it in the background).
- Against the live ledger, `paired=no` + `sources=binance` isolates the
  remaining unpaired sells, and pairing one from the modal drops the count by
  one with both legs sharing a `transfer_id`.
