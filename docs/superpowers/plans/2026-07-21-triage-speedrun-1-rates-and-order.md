# Triage Speedrun, Plan 1 — Rates Panel + Deterministic Order

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make every triage transaction disclose which exchange rate produced its
dollar figure, and make the queue's order deterministic so later plans can number
and navigate it.

**Architecture:** Three changes, no schema migration. (1) An ADR-012 amendment
unlocks the ordering and parking decisions that Plans 2 and 3 depend on. (2) The
triage queue gains an explicit SQL `ORDER BY` and a total-order Python sort key.
(3) A new read-only service, `rates_for_day`, returns the candidate rate series
for a transaction's day — Realized, USDT P2P, BCV, in resolver-priority order —
and the triage modal renders them, marking the winner from the card's existing
`rate_source` rather than re-deriving it.

**Tech Stack:** Python 3.13, FastAPI, Jinja2, HTMX, Alpine.js, Pydantic v2,
sqlite3 stdlib, pytest.

## Global Constraints

- **Spec:** `docs/superpowers/specs/2026-07-21-triage-speedrun-design.md`. Read
  §3.1a, §4 and §5.1 before starting.
- **Worktree:** `.claude/worktrees/triage-speedrun`, branch
  `worktree-triage-speedrun`, based on `origin/main` (`dbd9ced`). Baseline is
  795 tests passing. Never `cd` outside this worktree; the sibling worktrees
  `pair-picker` and `realized-cost-basis` are locked and belong to other work.
- **Test command:** `rtk proxy uv run pytest`. Plain `pytest` and
  `uv run rtk pytest` both fail in this environment.
- **rule-011 (TDD):** test commits precede implementation commits. Every task
  below commits RED separately from GREEN.
- **rule-012:** the web layer issues no `INSERT`/`UPDATE`/`DELETE` of its own and
  must not re-implement domain logic. This plan is read-only against the DB.
- **rule-005 / ADR-005:** BCV is reference-only and never a headline figure. The
  panel labels it as such; it never becomes the "used" rate by this plan's doing.
- **Do not import `finances.domain.realized_rates`.** That module does not exist
  on this base (spec §3.1a). Reference the source string
  `"binance_p2p_realized"` only.
- **Do not touch** `finances/web/routers/_tx_filter_dep.py`,
  `finances/web/services/transactions_query.py`, or
  `finances/web/templates/partials/transactions_filters.html` beyond what a task
  explicitly says. Those files are contested with the manual-pair-picker branch
  (spec §3.1); keep edits minimal and additive.

---

## File Structure

| File | Responsibility | Task |
|---|---|---|
| `docs/ADR/ADR-012-local-web-viewer.md` | Amend §50: ordering + action row | 1 |
| `docs/roadmap.md` | Amend EPIC-025 boundary + verification criteria | 1 |
| `finances/web/services/triage.py` | Add `ORDER BY`; total-order sort key | 2 |
| `tests/web/test_triage_order.py` | New: order determinism | 2 |
| `finances/web/services/rates_view.py` | Add `DayRate` + `rates_for_day` | 3 |
| `tests/web/test_rates_for_day.py` | New: service unit tests | 3 |
| `finances/web/templates/_macros.html` | Add two realized badge keys | 4 |
| `tests/web/test_rate_badges.py` | New: badge key coverage | 4 |
| `finances/web/routers/partials.py` | Pass `day_rates` into the triage modal | 5 |
| `finances/web/templates/partials/modal_transaction_triage.html` | Render the panel | 5 |
| `tests/web/test_triage_rate_panel.py` | New: end-to-end panel rendering | 5 |

`rates_view.py` is the right home for `rates_for_day`: it already owns every
rate-shaped DTO the web layer reads, and this keeps rate presentation in one
file rather than splitting it between the rates page and the triage modal.

## One deliberate deviation from the spec

Spec §5.1 says to *"extend `_CHART_SERIES_SPEC` to three entries so `/rates` and
the modal share one source of truth for labels."* This plan instead adds a
separate `_MODAL_SERIES_SPEC` and leaves `_CHART_SERIES_SPEC` at two entries.

Reason: `binance_p2p_realized` has zero rows on this base and will keep having
zero rows until the ADR-013 branch merges and someone runs a rebuild. Extending
the chart spec would draw a permanently empty third line on the `/rates` page —
visible daily, useful never. The modal is the only surface that benefits from
naming a series it cannot yet show, because there the absence is the
information ("no realized rate within 14 days").

The two constants sit adjacent in the same file with a comment explaining the
split, so the labels stay findable together. Revisit if the realized tier ever
carries real data.

---

### Task 1: ADR-012 amendment and roadmap edit

This is the gate from spec §3.2. It is documentation only — no code, no tests —
but it must land before Tasks 2 and 5, because both change behaviour ADR-012
currently pins.

**Files:**
- Modify: `docs/ADR/ADR-012-local-web-viewer.md`
- Modify: `docs/roadmap.md`

**Interfaces:**
- Consumes: nothing.
- Produces: written authority for the ordering change in Task 2 and for Plans 2
  and 3. No code symbols.

- [ ] **Step 1: Read the two passages you are about to change**

Run:

```bash
grep -n "oldest-issue-first\|Skip → bottom" docs/ADR/ADR-012-local-web-viewer.md
grep -n "session-local, intentionally not persisted\|oldest-first\|skip pushes items" docs/roadmap.md
```

Expected: one hit around `ADR-012:50`, and two or three hits in the EPIC-025
block of `docs/roadmap.md` (Technical Boundary and Verification Criteria).
If a grep returns nothing, stop and report — the file has moved on and this plan
needs re-checking rather than a guessed edit.

- [ ] **Step 2: Append an amendment section to ADR-012**

Do **not** rewrite the original decision text. ADRs are append-only records;
add this at the end of the file:

```markdown
## Amendment — 2026-07-21: triage ordering, navigation, and durable parking

**Status:** Accepted. Supersedes the queue-ordering and action-row clauses of
§50 for the `/triage` surface only. The rest of ADR-012 stands.

**Context.** The original decision optimised for auditability: strict
oldest-issue-first ordering, and a skip that was deliberately session-local so
no deferral could silently become permanent. In practice the queue holds 243
items, of which only 25 need nothing but a category. Chronological interleaving
forces the owner to context-switch between one-click rows and rows requiring
recall of an eight-month-old exchange rate, and the session-local skip is erased
by the Stop-server button that is the designed way to end a session.

**Decision.**

1. **Ordering** is by triage difficulty first, then chronology:
   `(bucket, occurred_at, item_id)` where bucket 0 = missing category only,
   1 = missing a rate, 2 = pair proposal. Within a bucket the original
   oldest-first rule is preserved. `item_id` is a mandatory tiebreak, not a
   preference: 204 of 243 live items share a timestamp, so without it the queue
   order is undefined.
2. **Deferral is durable.** `Skip → bottom of session queue` is replaced by
   `Park`, backed by a `transactions.parked` column. Parked items leave the main
   run and collect in a labelled group. Parking is a triage-queue grouping only;
   it does not alter `needs_review` and does not remove rows from any
   needs-review count.
3. **The action row** becomes `[← →] [Park] [Cancel] [Save & next]`. Saving
   marks an item done in place instead of removing it, so navigation indices
   stay stable.

**Consequences.** Chronological reading of the queue is no longer the default;
`/transactions` remains the date-ordered surface. A parked item can outlive the
condition that caused it to be parked, so the parked group shows its live issue
badges. Ordering is now reproducible across renders, which is a prerequisite for
the `N of M` counter and prev/next navigation.

**References.** `docs/superpowers/specs/2026-07-21-triage-speedrun-design.md`
§3.2, §4, §5.3, §5.5.
```

- [ ] **Step 3: Amend the EPIC-025 block in the roadmap**

In the EPIC-025 Technical Boundary, replace the sentence stating the skip store
is a session-local in-memory `set[str]` that is intentionally not persisted with:

```markdown
Deferral is durable, backed by the `transactions.parked` column (migration 014).
The former in-memory skip store is removed. See ADR-012 Amendment 2026-07-21.
```

In the EPIC-025 Verification Criteria, replace the clause requiring
oldest-first ordering and skip-to-bottom with:

```markdown
Queue is ordered `(bucket, occurred_at, item_id)` — category-only items first,
then rate-missing, then pairs — with type-filter chips and unfiltered counts;
Park removes an item from the main run durably and it reappears in the Parked
group after a server restart.
```

- [ ] **Step 4: Verify nothing else still asserts the old wording**

Run:

```bash
rtk proxy uv run pytest -q -k "roadmap or adr" 2>&1 | tail -5
grep -rn "oldest-first" docs/ | grep -v superpowers
```

Expected: the pytest selection collects no tests asserting doc prose (exit 5
with "no tests ran" is the normal result here, and is fine). The grep should
show only the roadmap line you just rewrote and any unrelated epics. If another epic asserts oldest-first triage
ordering, report it rather than editing it — it is outside this plan's scope.

- [ ] **Step 5: Commit**

```bash
git add docs/ADR/ADR-012-local-web-viewer.md docs/roadmap.md
git commit -m "docs(adr): amend ADR-012 for triage ordering, parking, navigation

Unblocks the triage speedrun spec. Records why difficulty-first ordering
replaces strict oldest-first, why deferral becomes durable, and why item_id
is a mandatory sort tiebreak rather than a nicety."
```

---

### Task 2: Deterministic queue order

Spec §4. This is a standalone bugfix with no UI change. It must land before any
work that numbers or navigates the queue.

**Files:**
- Modify: `finances/web/services/triage.py:143-153` (both queries), `:274` (sort)
- Test: `tests/web/test_triage_order.py` (new)

**Interfaces:**
- Consumes: `build_queue(conn, *, type_filter=None, skipped_ids=None) -> TriageQueue`
  and `TriageItem.item_id: str` — both already exist, unchanged.
- Produces: the guarantee that `build_queue(conn).items` is a pure function of DB
  contents, ordered by `(sort_key, item_id)`. Plans 2 and 3 rely on this.

- [ ] **Step 1: Write the failing test**

Create `tests/web/test_triage_order.py`:

```python
"""Triage queue ordering is deterministic (spec §4).

204 of 243 live triage items share an ``occurred_at`` value, because the
Provincial CSV carries no time component. Sorting on the timestamp alone
therefore leaves ~84% of the queue in whatever order SQLite happened to
return, which changes with the query plan. These tests pin a total order.
"""

from __future__ import annotations

import sqlite3
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from finances.db.repos import accounts as accounts_repo
from finances.db.repos import categories as categories_repo
from finances.db.repos import transactions as transactions_repo
from finances.domain.models import (
    Account,
    AccountKind,
    Transaction,
    TransactionKind,
)
from finances.web.services.triage import build_queue

# Every row shares this timestamp, which is the whole point.
TIED_AT = datetime(2025, 3, 4, tzinfo=UTC)


@pytest.fixture
def tied_db(web_db: sqlite3.Connection) -> sqlite3.Connection:
    """Three triage items at one timestamp, one per issue shape.

    Insertion order is id-ascending, but the two collection queries in
    ``_collect_txn_items`` visit them in a different order: the
    needs_review query yields ids 1 and 3, then the missing-category
    query adds id 2. Any implementation that preserves that visit order
    returns [1, 3, 2].
    """
    account = accounts_repo.insert(
        web_db,
        Account(name="Provincial", kind=AccountKind.BANK, currency="VES"),
    )
    groceries = categories_repo.get_by_name(
        web_db, TransactionKind.EXPENSE, "Groceries"
    )
    assert groceries is not None

    def _txn(ref: str, *, category_id: int | None, needs_review: bool) -> None:
        transactions_repo.insert(
            web_db,
            Transaction(
                account_id=account.id,
                occurred_at=TIED_AT,
                kind=TransactionKind.EXPENSE,
                amount=Decimal("100.00"),
                currency="VES",
                description=ref,
                category_id=category_id,
                source="provincial",
                source_ref=ref,
                needs_review=needs_review,
            ),
        )

    # id 1 — rate issue only (has a category).
    _txn("tied-a", category_id=groceries.id, needs_review=True)
    # id 2 — category issue only.
    _txn("tied-b", category_id=None, needs_review=False)
    # id 3 — both issues, so it appears in BOTH collection queries.
    _txn("tied-c", category_id=None, needs_review=True)

    return web_db


def test_tied_timestamps_order_by_item_id(tied_db: sqlite3.Connection) -> None:
    """Items sharing occurred_at fall back to item_id, not visit order."""
    queue = build_queue(tied_db)
    ids = [item.item_id for item in queue.items]

    assert ids == ["txn:1", "txn:2", "txn:3"]


def test_build_queue_is_repeatable(tied_db: sqlite3.Connection) -> None:
    """Two builds against the same data return the identical sequence."""
    first = [item.item_id for item in build_queue(tied_db).items]
    second = [item.item_id for item in build_queue(tied_db).items]

    assert first == second


def test_merged_item_keeps_both_badges(tied_db: sqlite3.Connection) -> None:
    """Guard: the ordering fix must not disturb badge merging."""
    queue = build_queue(tied_db)
    by_id = {item.item_id: item for item in queue.items}

    assert by_id["txn:3"].txn_issue_badges == ["category", "rate"]
```

- [ ] **Step 2: Run the test to verify it fails**

Run:

```bash
rtk proxy uv run pytest tests/web/test_triage_order.py -v
```

Expected: `test_tied_timestamps_order_by_item_id` FAILS with
`assert ['txn:1', 'txn:3', 'txn:2'] == ['txn:1', 'txn:2', 'txn:3']`.
The other two tests should PASS — they document behaviour you must not break.

If `test_tied_timestamps_order_by_item_id` passes, stop and report: it means the
visit order coincidentally matched, and the test is not proving what it claims.

- [ ] **Step 3: Commit the RED test**

```bash
git add tests/web/test_triage_order.py
git commit -m "test(triage): RED for deterministic queue order

Ties on occurred_at currently resolve to query visit order, so a merged
rate+category item sorts ahead of a category-only item inserted before it."
```

- [ ] **Step 4: Add ORDER BY to both collection queries**

In `finances/web/services/triage.py`, replace the two `conn.execute` calls in
`_collect_txn_items`:

```python
    rate_rows = conn.execute(
        _TXN_QUERY_BASE
        + """
        WHERE t.needs_review = 1
        ORDER BY t.occurred_at, t.id
        """
    ).fetchall()

    cat_rows = conn.execute(
        _TXN_QUERY_BASE
        + """
        WHERE t.category_id IS NULL
          AND t.kind NOT IN ('transfer', 'adjustment')
        ORDER BY t.occurred_at, t.id
        """
    ).fetchall()
```

- [ ] **Step 5: Make the Python sort a total order**

In the same file, in `build_queue`, replace:

```python
    all_items.sort(key=lambda it: it.sort_key)
```

with:

```python
    # (sort_key, item_id) is a TOTAL order. sort_key alone is not: 204 of 243
    # live items share a timestamp, so ties would fall through to SQLite's
    # row order, which changes with the query plan. item_id sorts as a string
    # ("txn:9" after "txn:10"), which is arbitrary but stable — and stability,
    # not numeric ordering, is what prev/next navigation needs.
    all_items.sort(key=lambda it: (it.sort_key, it.item_id))
```

- [ ] **Step 6: Update the module docstring's ordering claim**

In the `build_queue` docstring, replace step 3 of "Order of operations":

```python
      3) Sort all items by (sort_key, item_id) — oldest-first, with item_id
         as a mandatory tiebreak for the many rows sharing a timestamp.
```

- [ ] **Step 7: Run the new tests, then the full suite**

Run:

```bash
rtk proxy uv run pytest tests/web/test_triage_order.py -v
```

Expected: 3 passed.

Then:

```bash
rtk proxy uv run pytest -q 2>&1 | tail -5
```

Expected: 798 passed (795 baseline + 3 new), 0 failures. If `tests/web/test_triage.py`
now fails on an ordering assertion, read that test: if it asserted oldest-first
without a tiebreak it may have been relying on the old accidental order. Fix the
test to assert the new total order — do not weaken the implementation.

- [ ] **Step 8: Commit**

```bash
git add finances/web/services/triage.py
git commit -m "fix(triage): make queue order deterministic

Both collection queries lacked ORDER BY and the Python sort keyed on a bare
datetime, so ties resolved to SQLite row order. 204 of 243 live triage items
share a timestamp, making ~84% of the queue order undefined between renders.
Adds ORDER BY t.occurred_at, t.id and a (sort_key, item_id) total-order key.

Precondition for the N of M counter and prev/next navigation."
```

---

### Task 3: `rates_for_day` service

Spec §5.1. Read-only; returns the candidate rate series for one day.

**Files:**
- Modify: `finances/web/services/rates_view.py`
- Test: `tests/web/test_rates_for_day.py` (new)

**Interfaces:**
- Consumes: `rates_repo.latest_on_or_before(conn, *, as_of_date: date, base: str, quote: str, source: str) -> Rate | None`,
  where `Rate` has `.rate: Decimal` and `.as_of_date: date`.
- Produces:
  - `class DayRate(BaseModel)` with fields `label: str`, `source: str`,
    `rate: Decimal | None`, `as_of_date: date | None`, `is_carry: bool`,
    `is_winner: bool`, `is_reference_only: bool`.
  - `def rates_for_day(conn: sqlite3.Connection, *, day: date, winning_source: str) -> list[DayRate]`
    returning exactly three entries in resolver-priority order: Realized, USDT
    P2P, BCV.
  - `_MODAL_SERIES_SPEC: tuple[tuple[str, str, str, str], ...]` — `(base, quote, source, label)`.

  Task 5 consumes `rates_for_day` and the `DayRate` field names verbatim.

- [ ] **Step 1: Write the failing test**

Create `tests/web/test_rates_for_day.py`:

```python
"""Per-day candidate rate series for the triage modal (spec §5.1).

The panel must show all three tiers the resolver can draw from, mark which
one actually produced the dollar figure, and disclose carry-forward. It must
never re-derive the winner — it is told, via ``winning_source``.
"""

from __future__ import annotations

import sqlite3
from datetime import date
from decimal import Decimal

import pytest

from finances.db.repos import rates as rates_repo
from finances.domain.models import Rate
from finances.web.services.rates_view import rates_for_day

DAY = date(2026, 4, 23)


def _seed(conn: sqlite3.Connection, source: str, day: date, rate: str,
          base: str = "USDT", quote: str = "VES") -> None:
    rates_repo.upsert(
        conn,
        Rate(as_of_date=day, base=base, quote=quote,
             rate=Decimal(rate), source=source),
    )


def test_returns_three_series_in_resolver_priority_order(
    web_db: sqlite3.Connection,
) -> None:
    series = rates_for_day(web_db, day=DAY, winning_source="bcv")

    assert [s.source for s in series] == [
        "binance_p2p_realized",
        "binance_p2p_median",
        "bcv",
    ]


def test_missing_series_renders_as_none_not_error(
    web_db: sqlite3.Connection,
) -> None:
    """binance_p2p_realized has no rows on this base — that is expected."""
    series = rates_for_day(web_db, day=DAY, winning_source="needs_review")
    realized = series[0]

    assert realized.rate is None
    assert realized.as_of_date is None
    assert realized.is_carry is False
    assert realized.is_winner is False


def test_exact_day_match_is_not_carry(web_db: sqlite3.Connection) -> None:
    _seed(web_db, "binance_p2p_median", DAY, "483.31")

    series = rates_for_day(web_db, day=DAY, winning_source="binance_p2p_median")
    p2p = series[1]

    assert p2p.rate == Decimal("483.31")
    assert p2p.as_of_date == DAY
    assert p2p.is_carry is False
    assert p2p.is_winner is True


def test_older_rate_is_carried_and_flagged(web_db: sqlite3.Connection) -> None:
    _seed(web_db, "binance_p2p_median", date(2026, 4, 21), "481.00")

    series = rates_for_day(web_db, day=DAY, winning_source="binance_p2p_median_carry")
    p2p = series[1]

    assert p2p.rate == Decimal("481.00")
    assert p2p.as_of_date == date(2026, 4, 21)
    assert p2p.is_carry is True


def test_carry_suffix_still_matches_the_winner(
    web_db: sqlite3.Connection,
) -> None:
    """'bcv_carry' must mark the 'bcv' series, not fall through to no winner."""
    _seed(web_db, "bcv", date(2026, 4, 20), "36.55", base="USD")

    series = rates_for_day(web_db, day=DAY, winning_source="bcv_carry")

    assert [s.is_winner for s in series] == [False, False, True]


def test_bcv_is_flagged_reference_only(web_db: sqlite3.Connection) -> None:
    """ADR-005: BCV is never a headline figure, even when it is the winner."""
    series = rates_for_day(web_db, day=DAY, winning_source="bcv")

    assert [s.is_reference_only for s in series] == [False, False, True]


@pytest.mark.parametrize("source", ["user_rate", "native_usd", "needs_review"])
def test_non_series_winners_mark_nothing(
    web_db: sqlite3.Connection, source: str
) -> None:
    """user_rate / native_usd / needs_review are not table-backed series."""
    _seed(web_db, "binance_p2p_median", DAY, "483.31")

    series = rates_for_day(web_db, day=DAY, winning_source=source)

    assert not any(s.is_winner for s in series)


def test_bcv_series_reads_usd_ves_not_usdt_ves(
    web_db: sqlite3.Connection,
) -> None:
    """The BCV pair is USD/VES; a USDT/VES bcv row must not be picked up."""
    _seed(web_db, "bcv", DAY, "99.99", base="USDT")

    series = rates_for_day(web_db, day=DAY, winning_source="bcv")

    assert series[2].rate is None
```

- [ ] **Step 2: Run the test to verify it fails**

Run:

```bash
rtk proxy uv run pytest tests/web/test_rates_for_day.py -v
```

Expected: collection error — `ImportError: cannot import name 'rates_for_day'
from 'finances.web.services.rates_view'`.

- [ ] **Step 3: Commit the RED test**

```bash
git add tests/web/test_rates_for_day.py
git commit -m "test(rates): RED for per-day candidate rate series

Covers resolver-priority order, absent series, carry-forward disclosure,
_carry winner matching, BCV reference-only flagging, and the USD/VES vs
USDT/VES pair distinction."
```

- [ ] **Step 4: Implement `DayRate` and `rates_for_day`**

In `finances/web/services/rates_view.py`, add this import near the existing ones:

```python
from finances.db.repos import rates as rates_repo
```

Then add, immediately after the existing `_CHART_SERIES_SPEC` definition:

```python
# The triage modal shows every tier ``rates.resolve`` can draw from, in the
# resolver's own priority order, so the owner can see what was NOT used as
# well as what was. Kept separate from _CHART_SERIES_SPEC on purpose: the
# realized series has no rows on this base (spec §3.1a) and would draw a
# permanently empty line on the /rates chart.
_MODAL_SERIES_SPEC: tuple[tuple[str, str, str, str], ...] = (
    ("USDT", "VES", "binance_p2p_realized", "Realized"),
    ("USDT", "VES", "binance_p2p_median", "USDT P2P"),
    ("USD", "VES", "bcv", "BCV"),
)

# ADR-005: BCV is reference-only and never a headline figure.
_REFERENCE_ONLY_SOURCES = frozenset({"bcv"})

# finances.domain.rates appends this when a rate is carried from an earlier day.
_CARRY_SUFFIX = "_carry"


class DayRate(BaseModel):
    """One candidate rate for a transaction's day, as the modal shows it."""

    model_config = ConfigDict(extra="forbid")

    label: str
    source: str
    rate: Decimal | None
    as_of_date: date | None
    is_carry: bool
    is_winner: bool
    is_reference_only: bool


def rates_for_day(
    conn: sqlite3.Connection, *, day: date, winning_source: str
) -> list[DayRate]:
    """Return the three candidate rate series for ``day``.

    ``winning_source`` is the ``rate_source`` already computed by
    ``rates.resolve`` via ``_project_card``. This function NEVER re-derives
    the winner — duplicating resolver logic here is exactly what rule-012
    forbids. Sources with no table-backed series (``user_rate``,
    ``native_usd``, ``needs_review``) simply mark nothing.
    """
    winner = winning_source.removesuffix(_CARRY_SUFFIX)

    series: list[DayRate] = []
    for base, quote, source, label in _MODAL_SERIES_SPEC:
        found = rates_repo.latest_on_or_before(
            conn, as_of_date=day, base=base, quote=quote, source=source
        )
        series.append(
            DayRate(
                label=label,
                source=source,
                rate=found.rate if found is not None else None,
                as_of_date=found.as_of_date if found is not None else None,
                is_carry=found is not None and found.as_of_date < day,
                is_winner=source == winner,
                is_reference_only=source in _REFERENCE_ONLY_SOURCES,
            )
        )
    return series
```

Add `"DayRate"` and `"rates_for_day"` to the module's `__all__` list, keeping it
alphabetically sorted:

```python
__all__ = [
    "DEFAULT_RANGE_DAYS",
    "DayRate",
    "LatestRateCard",
    "RatePoint",
    "RateSeries",
    "RatesChart",
    "build_latest_rates",
    "build_rates_chart",
    "rates_for_day",
]
```

- [ ] **Step 5: Run the tests to verify they pass**

Run:

```bash
rtk proxy uv run pytest tests/web/test_rates_for_day.py -v
```

Expected: 10 passed (8 test functions, one of which is parametrized 3 ways).

- [ ] **Step 6: Commit**

```bash
git add finances/web/services/rates_view.py
git commit -m "feat(rates): add rates_for_day candidate-series reader

Returns the Realized / USDT P2P / BCV series for one day in resolver-priority
order, with carry-forward dates and a winner flag taken from the caller's
already-resolved rate_source. Reads through rates_repo.latest_on_or_before,
the same primitive rates.resolve uses, so no resolver logic is duplicated."
```

---

### Task 4: Badge keys for the realized tier

Spec §5.1. `rate_source_badge` has no entry for the realized sources, so if that
branch ever merges they render as unstyled raw snake_case.

**Files:**
- Modify: `finances/web/templates/_macros.html:25-33`
- Test: `tests/web/test_rate_badges.py` (new)

**Interfaces:**
- Consumes: the `rate_source_badge(source, is_bcv_fallback=False)` macro.
- Produces: no new symbols. Guarantees every source string `rates.resolve` can
  emit — on this base or after the ADR-013 merge — has a styled label.

- [ ] **Step 1: Write the failing test**

Create `tests/web/test_rate_badges.py`:

```python
"""Every resolver-emittable rate_source has a styled badge (spec §5.1).

Unknown sources fall through to the raw snake_case string with default slate
styling. That is an acceptable last resort but a bad default for sources we
know the resolver can produce.

Rendered through a bare Jinja environment rather than create_app: the macro
uses no custom filters, so there is no need for an app or a database here.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest
from jinja2 import Environment, FileSystemLoader

TEMPLATES_DIR = Path(__file__).resolve().parents[2] / "finances" / "web" / "templates"

# Sources rates.resolve can return, including the ADR-013 realized tier that
# arrives with the manual-pair-picker merge (spec §3.1a).
KNOWN_SOURCES = [
    "user_rate",
    "binance_p2p_realized",
    "binance_p2p_realized_carry",
    "binance_p2p_median",
    "binance_p2p_median_carry",
    "bcv",
    "bcv_carry",
    "native_usd",
    "needs_review",
]


def _render_badge(source: str) -> str:
    env = Environment(loader=FileSystemLoader(str(TEMPLATES_DIR)), autoescape=True)
    template = env.from_string(
        "{% from '_macros.html' import rate_source_badge %}"
        "{{ rate_source_badge(source) }}"
    )
    return template.render(source=source)


def _label_of(html: str) -> str:
    """Pull the visible label text out of a rendered badge span."""
    match = re.search(r">\s*([^<>]+?)\s*</span>", html)
    assert match is not None, f"no label found in: {html!r}"
    return match.group(1)


@pytest.mark.parametrize("source", KNOWN_SOURCES)
def test_every_known_source_has_a_styled_label(source: str) -> None:
    html = _render_badge(source)

    assert f'data-rate-source="{source}"' in html
    # An unmapped source echoes its own raw name as the label. No mapped
    # source has a label equal to its source string, so this is a clean
    # discriminator.
    assert _label_of(html) != source


def test_realized_keys_are_present_in_the_label_map() -> None:
    macros = (TEMPLATES_DIR / "_macros.html").read_text()
    block = re.search(r"label_map = \{(.*?)\n\}", macros, re.S)
    assert block is not None, "label_map block not found in _macros.html"

    assert '"binance_p2p_realized":' in block.group(1)
    assert '"binance_p2p_realized_carry":' in block.group(1)
```

- [ ] **Step 2: Run the test to verify it fails**

Run:

```bash
rtk proxy uv run pytest tests/web/test_rate_badges.py -v
```

Expected: FAILS — `test_realized_badges_read_as_p2p_family` with
`assert '"binance_p2p_realized":' in ...`, and
`test_every_known_source_has_a_styled_label` on the realized entries.

- [ ] **Step 3: Commit the RED test**

```bash
git add tests/web/test_rate_badges.py
git commit -m "test(web): RED for realized-tier rate-source badges"
```

- [ ] **Step 4: Add the two keys**

In `finances/web/templates/_macros.html`, inside `rate_source_badge`'s
`label_map`, add the two realized entries directly above the median ones so the
map reads in resolver-priority order. Realized reuses the sky palette because it
is the same P2P family as the median, one shade deeper to read as more
authoritative:

```jinja
{%- set label_map = {
    "user_rate": ("user", "bg-emerald-100 text-emerald-700 border-emerald-300"),
    "binance_p2p_realized": ("real", "bg-sky-200 text-sky-900 border-sky-400"),
    "binance_p2p_realized_carry": ("real~", "bg-sky-100 text-sky-700 border-sky-300"),
    "binance_p2p_median": ("p2p", "bg-sky-100 text-sky-700 border-sky-300"),
    "binance_p2p_median_carry": ("p2p~", "bg-sky-50 text-sky-600 border-sky-200"),
    "bcv": ("bcv", "bg-amber-100 text-amber-700 border-amber-300"),
    "bcv_carry": ("bcv~", "bg-amber-50 text-amber-600 border-amber-200"),
    "native_usd": ("native", "bg-slate-100 text-slate-600 border-slate-300"),
    "needs_review": ("?", "bg-rose-100 text-rose-700 border-rose-400 font-bold"),
} -%}
```

- [ ] **Step 5: Verify the new Tailwind classes exist in the vendored CSS**

`bg-sky-200`, `text-sky-900` and `border-sky-400` may not be in the compiled
stylesheet. Run:

```bash
for c in "bg-sky-200" "text-sky-900" "border-sky-400" "bg-sky-100" "text-sky-700" "border-sky-300"; do
  printf '%s: ' "$c"
  grep -c "\.$c" finances/web/static/css/*.css 2>/dev/null | paste -sd, -
done
```

Expected: a non-zero count for each. **If any class shows 0**, the vendored
Tailwind build does not include it and the badge will render unstyled. In that
case, reuse only classes already present — swap the realized entries to the
median's existing `bg-sky-100 text-sky-700 border-sky-300` and
`bg-sky-50 text-sky-600 border-sky-200` — rather than regenerating the
stylesheet, which is out of scope for this plan. Note the substitution in the
commit message.

- [ ] **Step 6: Run the tests, then the full suite**

Run:

```bash
rtk proxy uv run pytest tests/web/test_rate_badges.py -v
rtk proxy uv run pytest -q 2>&1 | tail -5
```

Expected: 10 passed (9 parametrized cases + 1), then the full suite green with
no regressions.

- [ ] **Step 7: Commit**

```bash
git add finances/web/templates/_macros.html
git commit -m "feat(web): add badge labels for the realized rate tier

rate_source_badge had no label_map entry for binance_p2p_realized or its
_carry variant, so those sources rendered as unstyled raw snake_case. Adds
both in resolver-priority position, sharing the P2P sky palette."
```

---

### Task 5: Render the rate panel in the triage modal

Spec §5.1. Wires Task 3's service into the modal.

**Files:**
- Modify: `finances/web/routers/partials.py` — `triage_modal_partial`, around
  line 433-472
- Modify: `finances/web/templates/partials/modal_transaction_triage.html` —
  insert a section between the header (ends line 61) and the Provenance section
- Test: `tests/web/test_triage_rate_panel.py` (new)

**Interfaces:**
- Consumes: `rates_for_day(conn, *, day, winning_source) -> list[DayRate]` and
  the `DayRate` field names from Task 3; `card.rate_source`, `card.amount_usd`,
  `card.occurred_at` from the existing `TransactionCard`.
- Produces: template context key `day_rates: list[DayRate]` on
  `partials/modal_transaction_triage.html`, and the DOM contract
  `[data-rate-panel]`, `[data-rate-row="<source>"]`, `[data-rate-winner]`,
  which Plan 3's navigation tests may assert against.

- [ ] **Step 1: Write the failing test**

Create `tests/web/test_triage_rate_panel.py`:

```python
"""Triage modal discloses all three candidate rates (spec §5.1).

The header shows e.g. "20,000.00 VES  $41.38" with no indication of the
divisor. These tests pin that the modal names every candidate rate for the
transaction's day and marks the one that produced the dollar figure.
"""

from __future__ import annotations

import sqlite3
from datetime import UTC, date, datetime
from decimal import Decimal

import pytest
from fastapi.testclient import TestClient

from finances.db.repos import accounts as accounts_repo
from finances.db.repos import rates as rates_repo
from finances.db.repos import transactions as transactions_repo
from finances.domain.models import (
    Account,
    AccountKind,
    Rate,
    Transaction,
    TransactionKind,
)

DAY = date(2026, 4, 23)
DAY_AT = datetime(2026, 4, 23, tzinfo=UTC)


@pytest.fixture
def panel_db(web_db: sqlite3.Connection) -> sqlite3.Connection:
    """One VES expense priced by BCV fallback, plus one native-USD row."""
    provincial = accounts_repo.insert(
        web_db,
        Account(name="Provincial Bolivares", kind=AccountKind.BANK, currency="VES"),
    )
    cash = accounts_repo.insert(
        web_db,
        Account(name="Cash USD", kind=AccountKind.CASH, currency="USD"),
    )

    # Only BCV exists for that day, so the resolver falls through to it.
    rates_repo.upsert(
        web_db,
        Rate(as_of_date=DAY, base="USD", quote="VES",
             rate=Decimal("483.30"), source="bcv"),
    )

    transactions_repo.insert(
        web_db,
        Transaction(
            account_id=provincial.id,
            occurred_at=DAY_AT,
            kind=TransactionKind.TRANSFER,
            amount=Decimal("20000.00"),
            currency="VES",
            description="TRAV0028021997000012403",
            source="provincial",
            source_ref="hash:9a12b3992e998132",
            needs_review=True,
        ),
    )
    transactions_repo.insert(
        web_db,
        Transaction(
            account_id=cash.id,
            occurred_at=DAY_AT,
            kind=TransactionKind.EXPENSE,
            amount=Decimal("12.50"),
            currency="USD",
            description="lunch",
            source="cash_cli",
            source_ref="cash-panel-1",
        ),
    )
    return web_db


def test_panel_lists_all_three_series(
    panel_db: sqlite3.Connection, web_client_factory
) -> None:
    client: TestClient = web_client_factory()
    html = client.get("/_partial/triage/1/modal").text

    assert "data-rate-panel" in html
    for source in ("binance_p2p_realized", "binance_p2p_median", "bcv"):
        assert f'data-rate-row="{source}"' in html, source


def test_panel_marks_the_winning_series(
    panel_db: sqlite3.Connection, web_client_factory
) -> None:
    client: TestClient = web_client_factory()
    html = client.get("/_partial/triage/1/modal").text

    # The winner marker appears exactly once, inside the bcv row.
    assert html.count("data-rate-winner") == 1
    bcv_row = html.split('data-rate-row="bcv"', 1)[1]
    assert "data-rate-winner" in bcv_row.split("</dd>", 1)[0]


def test_absent_series_renders_a_dash_not_a_crash(
    panel_db: sqlite3.Connection, web_client_factory
) -> None:
    client: TestClient = web_client_factory()
    response = client.get("/_partial/triage/1/modal")

    assert response.status_code == 200
    realized_row = response.text.split(
        'data-rate-row="binance_p2p_realized"', 1
    )[1].split("</dd>", 1)[0]
    assert "&mdash;" in realized_row or "—" in realized_row


def test_bcv_row_says_reference_only(
    panel_db: sqlite3.Connection, web_client_factory
) -> None:
    client: TestClient = web_client_factory()
    html = client.get("/_partial/triage/1/modal").text

    bcv_row = html.split('data-rate-row="bcv"', 1)[1].split("</dd>", 1)[0]
    assert "reference only" in bcv_row.lower()


def test_native_usd_row_has_no_panel(
    panel_db: sqlite3.Connection, web_client_factory
) -> None:
    """USD/USDT/USDC rows short-circuit to native_usd; a rate panel is noise."""
    client: TestClient = web_client_factory()
    html = client.get("/_partial/triage/2/modal").text

    assert "data-rate-panel" not in html


def test_panel_does_not_disturb_existing_modal_contract(
    panel_db: sqlite3.Connection, web_client_factory
) -> None:
    """Guard: the picker, rate field and action buttons still render."""
    client: TestClient = web_client_factory()
    html = client.get("/_partial/triage/1/modal").text

    assert 'name="set_category"' in html
    assert 'name="user_rate"' in html
    assert "data-skip-btn" in html
    assert 'hx-post="/_partial/triage/1/edit"' in html
```

- [ ] **Step 2: Run the test to verify it fails**

Run:

```bash
rtk proxy uv run pytest tests/web/test_triage_rate_panel.py -v
```

Expected: 5 of 6 FAIL on the missing `data-rate-panel` / `data-rate-row`
markup. `test_native_usd_row_has_no_panel` and
`test_panel_does_not_disturb_existing_modal_contract` should PASS already —
they are guards.

- [ ] **Step 3: Commit the RED test**

```bash
git add tests/web/test_triage_rate_panel.py
git commit -m "test(triage): RED for the three-rate modal panel"
```

- [ ] **Step 4: Pass `day_rates` into the modal context**

In `finances/web/routers/partials.py`, add to the imports near the other web
service imports:

```python
from finances.web.services.rates_view import rates_for_day
```

Then in `triage_modal_partial`, after the `card = _project_card(...)` call and
before `categories = categories_repo.list_all(conn)`, insert:

```python
    # Native-USD rows (USD/USDT/USDC) never consult a rate, so the panel
    # would be pure noise for them — see spec §5.1.
    day_rates = (
        []
        if card.rate_source == "native_usd"
        else rates_for_day(
            conn,
            day=txn.occurred_at.date(),
            winning_source=card.rate_source,
        )
    )
```

And add the key to the `TemplateResponse` context dict:

```python
        {
            "txn": txn,
            "card": card,
            "categories": categories,
            "top_categories": top_categories(conn, kind=txn.kind),
            "account_name": account_name,
            "day_rates": day_rates,
        },
```

- [ ] **Step 5: Render the panel**

In `finances/web/templates/partials/modal_transaction_triage.html`, insert this
section between the closing `</header>` and the `Provenance` `<section>`:

```jinja
    {% if day_rates %}
    <section class="tx-modal-section" data-rate-panel>
      <h3 class="tx-modal-section-title">Rates for {{ format_date(card.occurred_at) }}</h3>
      <dl class="grid grid-cols-[6rem_1fr] gap-x-4 gap-y-1 text-xs">
        {% for r in day_rates %}
          <dt class="text-slate-600">{{ r.label }}</dt>
          <dd class="tabular-nums" data-rate-row="{{ r.source }}">
            {% if r.rate is none %}
              <span class="text-slate-400">&mdash;</span>
            {% else %}
              <span class="{% if r.is_winner %}font-semibold text-slate-900{% else %}text-slate-600{% endif %}">
                {{ r.rate | fmt_number }}
              </span>
              {% if r.is_carry %}
                <span class="text-slate-500">(from {{ r.as_of_date | fmt_date }})</span>
              {% endif %}
              {% if r.is_winner %}
                <span class="text-emerald-700" data-rate-winner>
                  &larr; used for {{ card.amount_usd | fmt_money }}
                </span>
              {% endif %}
              {% if r.is_reference_only %}
                <span class="text-amber-700">reference only</span>
              {% endif %}
            {% endif %}
          </dd>
        {% endfor %}
      </dl>
      {% if card.rate_source == 'user_rate' %}
        <p class="mt-1 text-[11px] text-slate-500">
          The $ above uses your own rate, not any of these.
        </p>
      {% elif card.amount_usd is none %}
        <p class="mt-1 text-[11px] text-slate-500">
          No rate available for this day &mdash; enter one below to price it.
        </p>
      {% endif %}
    </section>
    {% endif %}
```

- [ ] **Step 6: Confirm the grid utility class is in the vendored CSS**

`grid-cols-[6rem_1fr]` is an arbitrary-value class and may be absent from the
compiled stylesheet. Run:

```bash
grep -c "grid-cols-\[6rem_1fr\]" finances/web/static/css/*.css
```

Expected: non-zero. **If 0**, replace that one class with the pattern already
used by the Provenance block two sections below — `grid grid-cols-2 gap-x-4
gap-y-1` — which is known present. Do not regenerate the Tailwind build here.

- [ ] **Step 7: Run the tests, then the full suite**

Run:

```bash
rtk proxy uv run pytest tests/web/test_triage_rate_panel.py -v
```

Expected: 6 passed.

Then:

```bash
rtk proxy uv run pytest -q 2>&1 | tail -5
```

Expected: 824 passed (795 baseline + 3 + 10 + 10 + 6), 0 failures.
If `tests/web/test_triage.py` or `test_modal_keyboard.py` fails on a changed
modal body, read the assertion: the panel adds markup but removes none, so a
failure there means a raw-string assertion pinned surrounding structure. Adjust
the test to target the element it means, not its neighbours.

- [ ] **Step 8: Commit**

```bash
git add finances/web/routers/partials.py \
        finances/web/templates/partials/modal_transaction_triage.html
git commit -m "feat(triage): show all three candidate rates in the modal

The header showed a dollar figure with no indication of its divisor. The
modal now lists Realized / USDT P2P / BCV for the transaction's day, marks
which produced the figure, discloses carry-forward origin dates, and labels
BCV reference-only per ADR-005. Native-USD rows skip the panel entirely."
```

---

## Verification

After Task 5, confirm the whole plan landed:

```bash
rtk proxy uv run pytest -q 2>&1 | tail -3
git log --oneline dbd9ced..HEAD
```

Expected: 824 passing, and ten commits — five RED/docs, five GREEN — in
test-before-implementation order.

Then look at it in a browser, because none of the above proves it reads well:

```bash
rtk proxy uv run finances web --port 8010
```

Open `http://127.0.0.1:8010/triage`, click any Provincial VES card, and check
the panel against spec §5.1's mock. Stop the server with the nav button.

## What this plan deliberately leaves undone

- `transactions.parked`, the Park button, and the Parked group — Plan 2.
- Easy-first bucket ordering and the header split — Plan 2 (it depends on
  `parked` existing).
- Prev/next arrows, `N of M`, in-place save, the `advanceQueue` redesign, and
  the dirty guard — Plan 3.
- Wiring `realized_rates.rebuild()` into ingest — out of scope on this base;
  spec §8.
