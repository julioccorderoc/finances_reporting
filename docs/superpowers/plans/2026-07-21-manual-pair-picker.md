# Manual P2P Pair Picker Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let Julio pair a Binance P2P sell with the Provincial bolívar deposit by hand, from the transaction detail modal, and isolate the unpaired backlog with a `paired` filter.

**Architecture:** A read-only candidate finder (`finances/web/services/pairing.py`) lists nearby unpaired Provincial rows for a given sell and scores each by drift against `abs(amount) × user_rate`. Two new HTMX partial routes render that list and confirm a pick. Confirmation delegates to the existing `confirm_pair()` → `create_transfer` mode 3 path — no new write logic anywhere.

**Tech Stack:** Python 3.13, FastAPI, Jinja2, HTMX + Alpine, Pydantic v2, sqlite3 stdlib, pytest.

Spec: `docs/superpowers/specs/2026-07-21-manual-pair-picker-design.md`

## Global Constraints

- Pydantic v2 models at every trust boundary; repos accept and return Pydantic, never raw `dict` (rule-009).
- `create_transfer` is the only function that writes `transfer_id`. Reuse `confirm_pair()`; never `UPDATE transactions SET transfer_id` directly (rule-002).
- Monetary arithmetic uses `Decimal`. Never `float`.
- Every SQL value is bound as a parameter. No f-string interpolation of user input.
- TDD: the test commit precedes the implementation commit (rule-011).
- Money display convention: sign before symbol (`-$30.83`), weekday dates (`Sun, May 10`).
- The full suite takes over 10 minutes. Run scoped tests during the loop; run `pytest -q` in the background once at the end.
- Never run `finances ingest`, `finances cash`, `finances backfill`, or `finances sync` against `finances.db`. Read-only `sqlite3` SELECTs are fine.

---

### Task 1: Share the transaction SELECT base

Two modules already hand-list the same 14 transaction columns feeding `_row_to_transaction`. A third (Task 3) would make it three. Adding a column to `transactions` currently means finding every one of those SELECTs by grep. Promote the constant so there is one.

**Files:**
- Modify: `finances/web/services/transactions_query.py` (add `TXN_QUERY_BASE`)
- Modify: `finances/web/services/triage.py:99-109` (delete `_TXN_QUERY_BASE`, import the shared one)
- Test: `tests/web/test_transactions_read.py`

**Interfaces:**
- Consumes: nothing.
- Produces: `finances.web.services.transactions_query.TXN_QUERY_BASE: str` — a `SELECT ... FROM transactions t LEFT JOIN accounts a LEFT JOIN categories c` prefix with no `WHERE`, whose column list satisfies `_row_to_transaction(row)` plus `row["account_name"]` and `row["category_name"]`.

- [ ] **Step 1: Write the failing test**

Append to `tests/web/test_transactions_read.py`:

```python
def test_txn_query_base_selects_every_column_row_to_transaction_needs(
    seeded_web_db: sqlite3.Connection,
) -> None:
    """One shared SELECT prefix, so adding a column has one place to update."""
    from finances.web.services.transactions_query import (
        TXN_QUERY_BASE,
        _row_to_transaction,
    )

    row = seeded_web_db.execute(
        TXN_QUERY_BASE + " WHERE t.source_ref = ?", ("prov-1",)
    ).fetchone()
    assert row is not None

    txn = _row_to_transaction(row)
    assert txn.source_ref == "prov-1"
    assert row["account_name"] == "Provincial"
    assert row["category_name"] == "Groceries"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `rtk proxy uv run pytest tests/web/test_transactions_read.py -k txn_query_base -q`
Expected: FAIL — `ImportError: cannot import name 'TXN_QUERY_BASE'`

- [ ] **Step 3: Add the constant**

In `finances/web/services/transactions_query.py`, after the `_SORT_COLUMN_MAP` block:

```python
# Shared SELECT prefix for every row that will pass through
# _row_to_transaction. Keep the column list and that function in sync —
# they are the pair that breaks when a column is added to transactions.
TXN_QUERY_BASE = """
    SELECT
        t.id, t.account_id, t.occurred_at, t.kind, t.amount, t.currency,
        t.description, t.category_id, t.transfer_id, t.user_rate,
        t.source, t.source_ref, t.needs_review, t.notes,
        a.name AS account_name,
        c.name AS category_name
    FROM transactions t
    LEFT JOIN accounts a ON a.id = t.account_id
    LEFT JOIN categories c ON c.id = t.category_id
"""
```

- [ ] **Step 4: Point triage.py at it**

In `finances/web/services/triage.py`, delete the `_TXN_QUERY_BASE = """..."""` block (lines 99-109) and extend the existing import:

```python
from finances.web.services.transactions_query import (
    TXN_QUERY_BASE,
    TransactionCard,
    _project_card,
    _row_to_transaction,
)
```

Then replace both `_TXN_QUERY_BASE` usages in that file with `TXN_QUERY_BASE`.

Run: `rtk proxy uv run grep -n "_TXN_QUERY_BASE" finances/web/services/triage.py`
Expected: no output.

- [ ] **Step 5: Run the tests**

Run: `rtk proxy uv run pytest tests/web/test_transactions_read.py tests/web/test_triage.py -q`
Expected: PASS, all of them.

- [ ] **Step 6: Commit**

```bash
git add finances/web/services/transactions_query.py finances/web/services/triage.py tests/web/test_transactions_read.py
git commit -m "refactor(web): share TXN_QUERY_BASE between transactions_query and triage"
```

---

### Task 2: `paired` filter on /transactions

**Files:**
- Modify: `finances/web/services/transactions_query.py` (`_PairedLiteral`, `TransactionsFilter.paired`, WHERE clause)
- Modify: `finances/web/routers/_tx_filter_dep.py` (query parameter)
- Modify: `finances/web/templates/partials/transactions_filters.html` (select)
- Test: `tests/web/test_filters_polish.py`

**Interfaces:**
- Consumes: nothing.
- Produces: `TransactionsFilter.paired: Literal["any", "yes", "no"]`, default `"any"`. `"no"` means `transfer_id IS NULL`.

- [ ] **Step 1: Write the failing tests**

Append to `tests/web/test_filters_polish.py`:

```python
def _pair_two_rows(conn: sqlite3.Connection) -> None:
    """Stamp a transfer_id on the seeded Binance income row."""
    conn.execute(
        "UPDATE transactions SET transfer_id = 'tid-test' WHERE source_ref = ?",
        ("bin-1",),
    )


def test_paired_no_returns_only_unpaired_rows(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    _pair_two_rows(seeded_web_db)
    client = web_client_factory()

    resp = client.get("/api/transactions", params={"paired": "no", "date_from": "2009-01-01"})

    assert resp.status_code == 200, resp.text
    descriptions = [row["description"] for row in resp.json()["rows"]]
    assert "Earn payout" not in descriptions
    assert "COM.PAGO bodega" in descriptions


def test_paired_yes_returns_only_paired_rows(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    _pair_two_rows(seeded_web_db)
    client = web_client_factory()

    resp = client.get("/api/transactions", params={"paired": "yes", "date_from": "2009-01-01"})

    assert resp.status_code == 200, resp.text
    descriptions = [row["description"] for row in resp.json()["rows"]]
    assert descriptions == ["Earn payout"]


def test_paired_any_is_the_default_and_returns_everything(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    _pair_two_rows(seeded_web_db)
    client = web_client_factory()

    default = client.get("/api/transactions", params={"date_from": "2009-01-01"})
    explicit = client.get(
        "/api/transactions", params={"paired": "any", "date_from": "2009-01-01"}
    )

    assert default.status_code == 200 and explicit.status_code == 200
    assert len(default.json()["rows"]) == len(explicit.json()["rows"])
    assert len(default.json()["rows"]) > 1


def test_paired_rejects_unknown_value(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    client = web_client_factory()

    resp = client.get("/api/transactions", params={"paired": "maybe"})

    assert resp.status_code == 422
```

If `/api/transactions` returns a key other than `rows`, or the client fixture needs different arguments, mirror whatever the existing tests in that file already do rather than inventing a shape.

- [ ] **Step 2: Run tests to verify they fail**

Run: `rtk proxy uv run pytest tests/web/test_filters_polish.py -k paired -q`
Expected: FAIL — the `paired` parameter is ignored, so `test_paired_no_...` still sees "Earn payout" and `test_paired_rejects_unknown_value` gets a 200.

- [ ] **Step 3: Add the field and the WHERE clause**

In `finances/web/services/transactions_query.py`, beside `_NeedsReviewLiteral`:

```python
_PairedLiteral = Literal["any", "yes", "no"]
```

In `TransactionsFilter`, directly after the `needs_review` field:

```python
    paired: _PairedLiteral = "any"
```

In the WHERE builder, directly after the `needs_review` block:

```python
    if f.paired == "yes":
        where.append("t.transfer_id IS NOT NULL")
    elif f.paired == "no":
        where.append("t.transfer_id IS NULL")
```

- [ ] **Step 4: Parse it from the URL**

In `finances/web/routers/_tx_filter_dep.py`, add the parameter after `needs_review` and the matching keyword to the returned `TransactionsFilter(...)`:

```python
    paired: Literal["any", "yes", "no"] = Query(default="any"),
```

```python
        paired=paired,
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `rtk proxy uv run pytest tests/web/test_filters_polish.py -k paired -q`
Expected: PASS (4 tests).

- [ ] **Step 6: Add the select to the filter bar**

In `finances/web/templates/partials/transactions_filters.html`, immediately after the "Needs review" `<label>` block:

```html
    <label class="text-xs flex flex-col gap-1">
      <span class="text-slate-500">Paired</span>
      <select name="paired" class="border border-slate-300 rounded px-2 py-1 text-sm">
        {% for opt in ['any', 'yes', 'no'] %}
          <option value="{{ opt }}" {% if filter.paired == opt %}selected{% endif %}>{{ opt }}</option>
        {% endfor %}
      </select>
    </label>
```

- [ ] **Step 7: Prove the saved-view round-trip**

Saved views persist the raw query string, so `paired` should survive with no repo change. Prove it rather than assume it. Append to `tests/web/test_saved_views_web.py`, matching the create/list calls the existing tests in that file already use:

```python
def test_saved_view_round_trips_the_paired_filter(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    """paired rides the stored query_string — no schema or repo change."""
    client = web_client_factory()

    created = client.post(
        "/api/saved-views",
        json={"name": "Unpaired P2P sells", "query_string": "sources=binance&paired=no"},
    )
    assert created.status_code in (200, 201), created.text

    listed = client.get("/api/saved-views")
    assert listed.status_code == 200, listed.text
    stored = [v for v in listed.json() if v["name"] == "Unpaired P2P sells"]
    assert stored, listed.text
    assert "paired=no" in stored[0]["query_string"]
```

If that file's tests use a different endpoint path or payload shape, follow theirs — the assertion that matters is `paired=no` surviving the round-trip.

- [ ] **Step 8: Run the wider web suite**

Run: `rtk proxy uv run pytest tests/web/test_filters_polish.py tests/web/test_transactions_read.py tests/web/test_saved_views_web.py -q`
Expected: PASS, all of them.

- [ ] **Step 9: Commit**

```bash
git add finances/web/services/transactions_query.py finances/web/routers/_tx_filter_dep.py finances/web/templates/partials/transactions_filters.html tests/web/test_filters_polish.py tests/web/test_saved_views_web.py
git commit -m "feat(web): paired filter on /transactions"
```

---

### Task 3: Candidate finder service

**Files:**
- Create: `finances/web/services/pairing.py`
- Test: `tests/web/test_pairing.py` (create)

**Interfaces:**
- Consumes: `TXN_QUERY_BASE`, `TransactionCard`, `_project_card`, `_row_to_transaction` from `finances.web.services.transactions_query` (Task 1).
- Produces:
  - `PairCandidate(card: TransactionCard, drift_ratio: Decimal | None, pairable: bool, blocked_reason: str | None)`
  - `PairCandidates(sell: TransactionCard, expected_ves: Decimal | None, window_days: int, candidates: list[PairCandidate])`
  - `find_pair_candidates(conn, *, sell_id: int, window_days: int = 2, bank_source: str = "provincial") -> PairCandidates`, raising `LookupError` when `sell_id` is unknown.

- [ ] **Step 1: Write the failing tests**

Create `tests/web/test_pairing.py`:

```python
"""Candidate finder for manual P2P pairing (sell → bank deposit).

Covers: which rows qualify, how drift is scored, ordering, and the
same-sign guard that create_transfer would otherwise reject.
"""

from __future__ import annotations

import sqlite3
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from finances.db.repos import accounts as accounts_repo
from finances.db.repos import transactions as transactions_repo
from finances.domain.models import Account, AccountKind, Transaction, TransactionKind
from finances.web.services.pairing import find_pair_candidates

SELL_AT = datetime(2026, 5, 10, 15, 0, tzinfo=UTC)


@pytest.fixture
def pairing_db(web_db: sqlite3.Connection) -> sqlite3.Connection:
    """A sell plus five Provincial rows spanning every candidate branch."""
    provincial = accounts_repo.insert(
        web_db,
        Account(
            name="Provincial",
            kind=AccountKind.BANK,
            currency="VES",
            institution="Provincial",
        ),
    )
    binance = accounts_repo.insert(
        web_db,
        Account(
            name="Binance Spot",
            kind=AccountKind.CRYPTO_SPOT,
            currency="USDT",
            institution="Binance",
        ),
    )

    # The sell: 30.83 USDT at 648.65 VES/USDT → 19 997.88 VES expected.
    transactions_repo.insert(
        web_db,
        Transaction(
            account_id=binance.id,
            occurred_at=SELL_AT,
            kind=TransactionKind.EXPENSE,
            amount=Decimal("-30.83"),
            currency="USDT",
            description="P2P SELL USDT @ 648.65 VES",
            user_rate=Decimal("648.65"),
            source="binance",
            source_ref="p2p-sell-1",
        ),
    )

    rows = [
        # Exact deposit, same day.
        ("dep-exact", SELL_AT, TransactionKind.INCOME, Decimal("20000.00")),
        # Far-off deposit, same day — still listed, high drift.
        ("dep-far", SELL_AT, TransactionKind.INCOME, Decimal("1250.00")),
        # Deposit one day later, also close.
        ("dep-next-day", SELL_AT + timedelta(days=1), TransactionKind.INCOME, Decimal("19900.00")),
        # Same-sign row: an expense cannot be the other leg.
        ("exp-same-sign", SELL_AT, TransactionKind.EXPENSE, Decimal("-20000.00")),
        # Outside the ±2 day window.
        ("dep-too-old", SELL_AT - timedelta(days=5), TransactionKind.INCOME, Decimal("20000.00")),
    ]
    for ref, when, kind, amount in rows:
        transactions_repo.insert(
            web_db,
            Transaction(
                account_id=provincial.id,
                occurred_at=when,
                kind=kind,
                amount=amount,
                currency="VES",
                description=ref,
                source="provincial",
                source_ref=ref,
            ),
        )
    return web_db


def _sell_id(conn: sqlite3.Connection) -> int:
    row = conn.execute(
        "SELECT id FROM transactions WHERE source_ref = ?", ("p2p-sell-1",)
    ).fetchone()
    assert row is not None
    return int(row["id"])


def _refs(result) -> list[str]:
    return [c.card.description for c in result.candidates]


def test_expected_ves_is_amount_times_user_rate(pairing_db: sqlite3.Connection) -> None:
    result = find_pair_candidates(pairing_db, sell_id=_sell_id(pairing_db))
    assert result.expected_ves == Decimal("30.83") * Decimal("648.65")


def test_income_and_expense_candidates_are_both_listed(
    pairing_db: sqlite3.Connection,
) -> None:
    """A deposit filed under the wrong kind must stay visible."""
    refs = _refs(find_pair_candidates(pairing_db, sell_id=_sell_id(pairing_db)))
    assert "dep-exact" in refs
    assert "exp-same-sign" in refs


def test_candidates_outside_the_window_are_excluded(
    pairing_db: sqlite3.Connection,
) -> None:
    refs = _refs(find_pair_candidates(pairing_db, sell_id=_sell_id(pairing_db)))
    assert "dep-too-old" not in refs


def test_widening_the_window_pulls_in_the_older_deposit(
    pairing_db: sqlite3.Connection,
) -> None:
    refs = _refs(
        find_pair_candidates(pairing_db, sell_id=_sell_id(pairing_db), window_days=7)
    )
    assert "dep-too-old" in refs


def test_already_paired_rows_are_excluded(pairing_db: sqlite3.Connection) -> None:
    pairing_db.execute(
        "UPDATE transactions SET transfer_id = 'tid-x' WHERE source_ref = ?",
        ("dep-exact",),
    )
    refs = _refs(find_pair_candidates(pairing_db, sell_id=_sell_id(pairing_db)))
    assert "dep-exact" not in refs


def test_candidates_are_sorted_closest_match_first(
    pairing_db: sqlite3.Connection,
) -> None:
    refs = _refs(find_pair_candidates(pairing_db, sell_id=_sell_id(pairing_db)))
    assert refs[0] == "dep-exact"
    assert refs.index("dep-next-day") < refs.index("dep-far")


def test_same_sign_candidate_is_not_pairable(pairing_db: sqlite3.Connection) -> None:
    result = find_pair_candidates(pairing_db, sell_id=_sell_id(pairing_db))
    same_sign = next(c for c in result.candidates if c.card.description == "exp-same-sign")
    assert same_sign.pairable is False
    assert same_sign.blocked_reason is not None
    deposit = next(c for c in result.candidates if c.card.description == "dep-exact")
    assert deposit.pairable is True
    assert deposit.blocked_reason is None


def test_sell_without_user_rate_yields_no_expected_and_no_drift(
    pairing_db: sqlite3.Connection,
) -> None:
    pairing_db.execute(
        "UPDATE transactions SET user_rate = NULL WHERE source_ref = ?", ("p2p-sell-1",)
    )
    result = find_pair_candidates(pairing_db, sell_id=_sell_id(pairing_db))
    assert result.expected_ves is None
    assert result.candidates  # still listed, just unscored
    assert all(c.drift_ratio is None for c in result.candidates)


def test_unknown_sell_id_raises_lookup_error(pairing_db: sqlite3.Connection) -> None:
    with pytest.raises(LookupError):
        find_pair_candidates(pairing_db, sell_id=999999)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `rtk proxy uv run pytest tests/web/test_pairing.py -q`
Expected: FAIL at collection — `ModuleNotFoundError: No module named 'finances.web.services.pairing'`

- [ ] **Step 3: Write the service**

Create `finances/web/services/pairing.py`:

```python
"""Manual pair candidates: a Binance P2P sell → its bank deposit.

``BankAnchoredP2pPairing`` (finances/domain/transfers.py) only proposes a
pairing when exactly one candidate survives its amount tolerance. Round
20 000 Bs deposits repeat often enough that the uniqueness gate skips
them, so the remaining backlog needs a human. This module is the
read-only half of that: it lists what a sell *could* pair with and scores
each option. Writing stays with ``confirm_pair`` → ``create_transfer``.

Read-only. Nothing here mutates the database.
"""

from __future__ import annotations

import sqlite3
from datetime import timedelta
from decimal import Decimal

from pydantic import BaseModel, ConfigDict

from finances.web.services.transactions_query import (
    TXN_QUERY_BASE,
    TransactionCard,
    _project_card,
    _row_to_transaction,
)

_SAME_SIGN_REASON = "same sign — not a deposit"

# Both income and expense kinds are returned deliberately: a deposit
# recorded under the wrong kind must stay visible to the human, who is
# the only one who can recognize it.
_CANDIDATE_SQL = (
    TXN_QUERY_BASE
    + """
    WHERE t.source = :bank_source
      AND t.transfer_id IS NULL
      AND t.occurred_at BETWEEN :start AND :end
    ORDER BY t.occurred_at ASC, t.id ASC
"""
)


class PairCandidate(BaseModel):
    """One row a sell could be paired with, plus how well it fits."""

    model_config = ConfigDict(extra="forbid")

    card: TransactionCard
    drift_ratio: Decimal | None
    pairable: bool
    blocked_reason: str | None = None


class PairCandidates(BaseModel):
    """Everything the pair-picker partial needs to render."""

    model_config = ConfigDict(extra="forbid")

    sell: TransactionCard
    expected_ves: Decimal | None
    window_days: int
    candidates: list[PairCandidate]


def find_pair_candidates(
    conn: sqlite3.Connection,
    *,
    sell_id: int,
    window_days: int = 2,
    bank_source: str = "provincial",
) -> PairCandidates:
    """List unpaired bank rows near ``sell_id``, closest amount first.

    Raises ``LookupError`` when ``sell_id`` does not exist.
    """
    sell_row = conn.execute(
        TXN_QUERY_BASE + " WHERE t.id = ?", (sell_id,)
    ).fetchone()
    if sell_row is None:
        raise LookupError(f"transaction id={sell_id} not found")

    sell_txn = _row_to_transaction(sell_row)
    sell_card = _project_card(
        conn,
        sell_txn,
        account_name=sell_row["account_name"] or "",
        category_name=sell_row["category_name"],
    )

    # The sell is in USDT; user_rate is VES per USDT. Without it there is
    # nothing to score against, but the rows are still worth showing.
    expected_ves: Decimal | None = None
    if sell_txn.user_rate is not None and sell_txn.user_rate > 0:
        expected_ves = abs(sell_txn.amount) * sell_txn.user_rate

    start = sell_txn.occurred_at - timedelta(days=window_days)
    end = sell_txn.occurred_at + timedelta(days=window_days)
    rows = conn.execute(
        _CANDIDATE_SQL,
        {
            "bank_source": bank_source,
            "start": start.isoformat(),
            "end": end.isoformat(),
        },
    ).fetchall()

    sell_is_negative = sell_txn.amount < 0

    candidates: list[PairCandidate] = []
    for row in rows:
        txn = _row_to_transaction(row)
        card = _project_card(
            conn,
            txn,
            account_name=row["account_name"] or "",
            category_name=row["category_name"],
        )

        drift: Decimal | None = None
        if expected_ves is not None and txn.amount != 0:
            drift = abs(abs(txn.amount) - expected_ves) / abs(txn.amount)

        # create_transfer infers the from/to leg from the two signs and
        # rejects a pair that shares one. Surface that up front instead of
        # letting the click 422.
        pairable = (txn.amount < 0) != sell_is_negative

        candidates.append(
            PairCandidate(
                card=card,
                drift_ratio=drift,
                pairable=pairable,
                blocked_reason=None if pairable else _SAME_SIGN_REASON,
            )
        )

    # Closest amount first; unscored rows last; date as the tiebreaker.
    candidates.sort(
        key=lambda c: (
            c.drift_ratio is None,
            c.drift_ratio if c.drift_ratio is not None else Decimal(0),
            c.card.occurred_at,
        )
    )

    return PairCandidates(
        sell=sell_card,
        expected_ves=expected_ves,
        window_days=window_days,
        candidates=candidates,
    )


__all__ = ["PairCandidate", "PairCandidates", "find_pair_candidates"]
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `rtk proxy uv run pytest tests/web/test_pairing.py -q`
Expected: PASS (9 tests).

- [ ] **Step 5: Commit**

```bash
git add finances/web/services/pairing.py tests/web/test_pairing.py
git commit -m "feat(web): pair-candidate finder for manual P2P pairing"
```

---

### Task 4: Candidate partial route and template

**Files:**
- Create: `finances/web/templates/partials/pair_candidates.html`
- Modify: `finances/web/routers/partials.py` (new GET route)
- Test: `tests/web/test_pairing_web.py` (create)

**Interfaces:**
- Consumes: `find_pair_candidates` (Task 3).
- Produces: `GET /_partial/transactions/{sell_id}/pair-candidates?window_days=N` → HTML fragment. The confirm button for candidate `d` posts to `/_partial/transactions/{sell_id}/pair/{d}` (built in Task 5).

- [ ] **Step 1: Write the failing tests**

Create `tests/web/test_pairing_web.py`:

```python
"""HTMX surface for manual P2P pairing: candidate partial + confirm."""

from __future__ import annotations

import sqlite3

import pytest

from tests.web.test_pairing import pairing_db  # noqa: F401  (fixture reuse)


def _txn_id(conn: sqlite3.Connection, source_ref: str) -> int:
    row = conn.execute(
        "SELECT id FROM transactions WHERE source_ref = ?", (source_ref,)
    ).fetchone()
    assert row is not None, f"missing fixture row {source_ref!r}"
    return int(row["id"])


def test_pair_candidates_partial_lists_nearby_deposits(
    pairing_db: sqlite3.Connection, web_client_factory
) -> None:
    client = web_client_factory()
    sell_id = _txn_id(pairing_db, "p2p-sell-1")

    resp = client.get(f"/_partial/transactions/{sell_id}/pair-candidates")

    assert resp.status_code == 200, resp.text
    assert "dep-exact" in resp.text
    assert "dep-too-old" not in resp.text


def test_pair_candidates_partial_widens_the_window(
    pairing_db: sqlite3.Connection, web_client_factory
) -> None:
    client = web_client_factory()
    sell_id = _txn_id(pairing_db, "p2p-sell-1")

    resp = client.get(
        f"/_partial/transactions/{sell_id}/pair-candidates",
        params={"window_days": 7},
    )

    assert resp.status_code == 200, resp.text
    assert "dep-too-old" in resp.text


def test_pair_candidates_partial_disables_same_sign_rows(
    pairing_db: sqlite3.Connection, web_client_factory
) -> None:
    client = web_client_factory()
    sell_id = _txn_id(pairing_db, "p2p-sell-1")

    resp = client.get(f"/_partial/transactions/{sell_id}/pair-candidates")

    assert resp.status_code == 200, resp.text
    assert "same sign" in resp.text
    assert "disabled" in resp.text


def test_pair_candidates_partial_404s_for_unknown_sell(
    pairing_db: sqlite3.Connection, web_client_factory
) -> None:
    client = web_client_factory()

    resp = client.get("/_partial/transactions/999999/pair-candidates")

    assert resp.status_code == 404
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `rtk proxy uv run pytest tests/web/test_pairing_web.py -q`
Expected: FAIL — 404 on every request, because the route does not exist yet.

- [ ] **Step 3: Write the template**

Create `finances/web/templates/partials/pair_candidates.html`:

```html
{# Manual pair picker: unpaired bank rows near this sell, closest first.
   Re-rendered standalone by GET /_partial/transactions/{id}/pair-candidates,
   so it must carry its own wrapper id. #}
<div id="pair-candidates" class="flex flex-col gap-2">
  <p class="text-xs text-slate-500">
    {% if data.expected_ves is not none %}
      expected {{ "{:,.2f}".format(data.expected_ves) }} Bs
    {% else %}
      no user rate on this sell — set one to score the matches
    {% endif %}
  </p>

  {% for candidate in data.candidates %}
    <div class="flex items-center justify-between gap-3 border border-slate-200 rounded px-2 py-1">
      <div class="flex flex-col">
        <span class="text-xs text-slate-500">{{ candidate.card.occurred_at.strftime('%a, %b %d') }}</span>
        <span class="text-sm text-slate-800">
          {{ "{:,.2f}".format(candidate.card.amount_native) }} {{ candidate.card.currency }}
        </span>
        <span class="text-xs text-slate-500 break-all">{{ candidate.card.description }}</span>
      </div>
      <div class="flex items-center gap-2">
        <span class="text-xs {% if candidate.drift_ratio is not none and candidate.drift_ratio < 0.02 %}text-emerald-700{% else %}text-slate-500{% endif %}">
          {% if candidate.drift_ratio is not none %}
            {{ "{:.2%}".format(candidate.drift_ratio) }}
          {% else %}
            —
          {% endif %}
        </span>
        {% if candidate.pairable %}
          <button
            type="button"
            class="text-xs border border-slate-800 rounded px-2 py-1"
            hx-post="/_partial/transactions/{{ data.sell.id }}/pair/{{ candidate.card.id }}"
            hx-target="#tx-list"
            hx-swap="outerHTML"
          >pair</button>
        {% else %}
          <button
            type="button"
            class="text-xs border border-slate-200 text-slate-400 rounded px-2 py-1"
            disabled
            title="{{ candidate.blocked_reason }}"
          >{{ candidate.blocked_reason }}</button>
        {% endif %}
      </div>
    </div>
  {% else %}
    <p class="text-sm text-slate-500">No unpaired bank rows in this window.</p>
  {% endfor %}

  {% if data.window_days < 7 %}
    <button
      type="button"
      class="text-xs text-slate-600 underline self-start"
      hx-get="/_partial/transactions/{{ data.sell.id }}/pair-candidates?window_days=7"
      hx-target="#pair-candidates"
      hx-swap="outerHTML"
    >widen to ±7 days</button>
  {% endif %}
</div>
```

If `TransactionCard` exposes the native currency under a name other than `currency`, use the real field — check the model in `transactions_query.py` rather than guessing.

- [ ] **Step 4: Add the route**

In `finances/web/routers/partials.py`, extend the service imports:

```python
from finances.web.services.pairing import find_pair_candidates
```

and add the route after `transactions_modal_partial`:

```python
@router.get("/transactions/{sell_id}/pair-candidates", include_in_schema=False)
def transactions_pair_candidates_partial(
    request: Request,
    sell_id: int,
    window_days: int = Query(default=2, ge=1, le=30),
    conn: sqlite3.Connection = Depends(get_conn),
):
    """Render unpaired bank rows this sell could pair with.

    Read-only. The pick itself is a separate POST so the write path stays
    on confirm_pair → create_transfer (rule-002).
    """
    try:
        data = find_pair_candidates(conn, sell_id=sell_id, window_days=window_days)
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    templates = request.app.state.templates
    return templates.TemplateResponse(
        request,
        "partials/pair_candidates.html",
        {"data": data},
    )
```

`Query` may not be imported in that module yet — add it to the existing `from fastapi import ...` line if missing.

- [ ] **Step 5: Run tests to verify they pass**

Run: `rtk proxy uv run pytest tests/web/test_pairing_web.py -q`
Expected: PASS (4 tests).

- [ ] **Step 6: Commit**

```bash
git add finances/web/templates/partials/pair_candidates.html finances/web/routers/partials.py tests/web/test_pairing_web.py
git commit -m "feat(web): pair-candidates partial route and template"
```

---

### Task 5: Confirm route and modal wiring

**Files:**
- Modify: `finances/web/routers/partials.py` (new POST route)
- Modify: `finances/web/templates/partials/modal_transaction.html:112` (new section after Description)
- Test: `tests/web/test_pairing_web.py`

**Interfaces:**
- Consumes: `confirm_pair` from `finances.web.services.triage`, the partial from Task 4.
- Produces: `POST /_partial/transactions/{sell_id}/pair/{deposit_id}` → refreshed `#tx-list` partial, `HX-Trigger` carrying `closeModal` and a toast.

- [ ] **Step 1: Write the failing tests**

Append to `tests/web/test_pairing_web.py`:

```python
def test_pair_confirm_creates_the_transfer(
    pairing_db: sqlite3.Connection, web_client_factory
) -> None:
    client = web_client_factory()
    sell_id = _txn_id(pairing_db, "p2p-sell-1")
    deposit_id = _txn_id(pairing_db, "dep-exact")

    resp = client.post(f"/_partial/transactions/{sell_id}/pair/{deposit_id}")

    assert resp.status_code == 200, resp.text
    assert "closeModal" in resp.headers.get("HX-Trigger", "")

    rows = pairing_db.execute(
        "SELECT transfer_id, kind FROM transactions WHERE id IN (?, ?)",
        (sell_id, deposit_id),
    ).fetchall()
    transfer_ids = {row["transfer_id"] for row in rows}
    assert len(transfer_ids) == 1 and None not in transfer_ids
    assert {row["kind"] for row in rows} == {"transfer"}


def test_pair_confirm_422s_when_already_paired(
    pairing_db: sqlite3.Connection, web_client_factory
) -> None:
    client = web_client_factory()
    sell_id = _txn_id(pairing_db, "p2p-sell-1")
    deposit_id = _txn_id(pairing_db, "dep-exact")

    first = client.post(f"/_partial/transactions/{sell_id}/pair/{deposit_id}")
    assert first.status_code == 200, first.text

    second = client.post(f"/_partial/transactions/{sell_id}/pair/{deposit_id}")
    assert second.status_code == 422


def test_pair_confirm_404s_for_unknown_deposit(
    pairing_db: sqlite3.Connection, web_client_factory
) -> None:
    client = web_client_factory()
    sell_id = _txn_id(pairing_db, "p2p-sell-1")

    resp = client.post(f"/_partial/transactions/{sell_id}/pair/999999")

    assert resp.status_code == 404


def test_modal_shows_the_pair_section_for_an_unpaired_sell(
    pairing_db: sqlite3.Connection, web_client_factory
) -> None:
    client = web_client_factory()
    sell_id = _txn_id(pairing_db, "p2p-sell-1")

    resp = client.get(f"/_partial/transactions/{sell_id}/modal")

    assert resp.status_code == 200, resp.text
    assert "pair-candidates" in resp.text


def test_modal_hides_the_pair_section_once_paired(
    pairing_db: sqlite3.Connection, web_client_factory
) -> None:
    client = web_client_factory()
    sell_id = _txn_id(pairing_db, "p2p-sell-1")
    pairing_db.execute(
        "UPDATE transactions SET transfer_id = 'tid-x' WHERE id = ?", (sell_id,)
    )

    resp = client.get(f"/_partial/transactions/{sell_id}/modal")

    assert resp.status_code == 200, resp.text
    assert "pair-candidates" not in resp.text
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `rtk proxy uv run pytest tests/web/test_pairing_web.py -k "confirm or modal" -q`
Expected: FAIL — 404 on the POST route; the modal assertions fail because the section is not rendered yet.

- [ ] **Step 3: Add the confirm route**

In `finances/web/routers/partials.py`, directly after the candidates route. `confirm_pair` is already imported at the top of the module — do not import it twice.

```python
@router.post("/transactions/{sell_id}/pair/{deposit_id}", include_in_schema=False)
def transactions_pair_confirm_partial(
    request: Request,
    sell_id: int,
    deposit_id: int,
    f: TransactionsFilter = Depends(filter_from_query),
    conn: sqlite3.Connection = Depends(get_conn),
):
    """Pair a sell with a hand-picked deposit, then refresh the list.

    Delegates to confirm_pair → create_transfer mode 3, the single write
    path for transfer_id (rule-002). Distinct from the triage confirm
    route only in what it swaps back.
    """
    try:
        confirm_pair(conn, deposit_id=deposit_id, sell_id=sell_id)
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    page = query_transactions(conn, f)
    templates = request.app.state.templates
    response = templates.TemplateResponse(
        request,
        "partials/transactions_list.html",
        {"page": page, "filter": page.filter},
    )
    response.headers["HX-Trigger"] = _hx_trigger_json(
        "closeModal", toast_message="Paired"
    )
    return response
```

- [ ] **Step 4: Wire the modal section**

In `finances/web/templates/partials/modal_transaction.html`, insert between the Description section (ends line 112) and the editable form (`{# 4. Editable form ... #}`):

```html
    {# 3b. Manual pairing — unpaired outgoing Binance rows only ---------- #}
    {% if txn.transfer_id is none and txn.source == 'binance' and txn.amount < 0 %}
    <section class="tx-modal-section">
      <h3 class="tx-modal-section-title">Pair with deposit</h3>
      <div
        hx-get="/_partial/transactions/{{ txn.id }}/pair-candidates"
        hx-trigger="load"
        hx-swap="outerHTML"
      >
        <p class="text-sm text-slate-400">Loading candidates…</p>
      </div>
    </section>
    {% endif %}
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `rtk proxy uv run pytest tests/web/test_pairing_web.py -q`
Expected: PASS (9 tests).

- [ ] **Step 6: Rebuild the vendored CSS**

`finances/web/static/css/tailwind.css` is a build artifact containing only the utility classes Tailwind found in the templates. New classes (`text-emerald-700`, `break-all`, `self-start`) will be missing until it is rebuilt. Recipe from `tailwind/README.md`:

```bash
npx -y tailwindcss@3.4.17 \
  -c tailwind/tailwind.config.js \
  -i tailwind/input.css \
  -o finances/web/static/css/tailwind.css \
  --minify

python - <<'PY'
import re, pathlib
p = pathlib.Path("finances/web/static/css/tailwind.css")
p.write_text(re.sub(r"/\*!.*?\*/", "", p.read_text(), flags=re.S))
PY
```

Then confirm the new classes survived the purge:

```bash
grep -c "emerald-700" finances/web/static/css/tailwind.css
```

Expected: a count of 1 or more. Add `finances/web/static/css/tailwind.css` to the Step 8 commit.

- [ ] **Step 7: Run the full suite in the background**

Run: `rtk proxy uv run pytest -q` with `run_in_background: true`
Expected: exit code 0. It takes over 10 minutes; do not block on it in the foreground.

- [ ] **Step 8: Commit**

```bash
git add finances/web/routers/partials.py finances/web/templates/partials/modal_transaction.html tests/web/test_pairing_web.py
git commit -m "feat(web): manual pair confirm route and modal section"
```

---

## Manual verification

Against the live ledger, read-only unless stated:

1. Start the viewer the way `docs/` describes and open `/transactions`.
2. Set Sources = binance, Paired = no. Save it as a view named "Unpaired P2P sells".
3. Open one of the July 2026 sells. The modal shows PAIR WITH DEPOSIT with an
   expected Bs figure. Provincial data currently ends 2026-07-09, so recent
   sells legitimately show "No unpaired bank rows in this window" — that is the
   stale-CSV case from the spec, not a bug.
4. Open a sell from June 2026. Confirm the closest deposit appears first with a
   drift under 2 %, and that any same-sign expense row renders disabled.
5. Click pair on one row. The modal closes, a "Paired" toast appears, and the
   list refreshes. Verify with:
   `sqlite3 finances.db "SELECT id, kind, transfer_id FROM transactions WHERE id IN (<sell>, <deposit>)"`
   Both rows must be `kind='transfer'` sharing one non-null `transfer_id`.
6. Re-run the saved view — the paired sell is gone from the list.
