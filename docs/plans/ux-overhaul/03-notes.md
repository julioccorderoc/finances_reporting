# WP3 Transaction Notes Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a single free-text `notes` column to transactions, threaded full-stack (migration → Pydantic model → repo → edit service → JSON/HTMX endpoints → modal UI → row card → text search), with a hard guarantee that re-ingest never wipes a manually written note.

**Architecture:** SQLite is the source of truth; a new nullable `notes TEXT` column rides the existing idempotent migration runner. The repo layer (`finances/db/repos/transactions.py`) is the only SQL surface: `update()` gains a `notes` parameter via the existing `_UNSET` sentinel, and `upsert_by_source_ref` gains a COALESCE preserve clause mirroring the category/rate enrichment-preservation contract. The web layer only touches notes through `TransactionEditRequest` → `apply_edit` → `transactions_repo.update()` (rule-012); reads project `notes` onto the existing `TransactionCard` via `_project_card`.

**Tech Stack:** Python 3.13 + uv, sqlite3 stdlib, Pydantic v2, FastAPI + Jinja2 + vendored htmx/Alpine, pytest.

## Global Constraints

- TDD per rule-011 / CLAUDE.md execution rule 5: within each task, the test commit lands BEFORE the implementation commit.
- Run tests with `uv run pytest -q <path>` — never bare `pytest`.
- **Migration numbering deviation from the WP contract:** the contract names `007_add_transaction_notes.sql`, but `finances/db/migrations/007_going_out_and_bank_fee_rules.sql` already exists in the repo; this plan uses the next free prefix, `008_add_transaction_notes.sql` (the runner in `finances/db/migrate.py` sorts by filename and records each file once in `_migrations`, so the rename is safe and unambiguous).
- rule-009: Pydantic v2 at every trust boundary; repos accept/return Pydantic models, never raw dicts.
- rule-012: the web viewer's ONLY transaction write path is `transactions_repo.update()` — no new UPDATE SQL anywhere in `finances/web/`.
- `needs_review` stays derived by the rate resolver (`apply_edit` re-runs `rates.resolve`); never exposed as a manual toggle.
- No new dependencies, no CDN assets; no new JS is needed for this WP (plain HTML textarea + existing HTMX form posts).
- Data lists stay CSS Grid card-rows (no `<table>`); the `.cards` grid defines exactly 6 column tracks and each `card-row` has exactly 6 children, so the note indicator must live INSIDE the existing description cell, never as a 7th grid cell.
- Real expense amounts are NEGATIVE; the `seeded_web_db` fixture stores them positive — the card-render test below uses `amount_native=Decimal("-1234.56")` and no test relies on the fixture's signs.
- Tests never touch the real `finances.db`: repo tests use `seeded_db`/`in_memory_db` (tests/conftest.py), web tests use `web_db`/`seeded_web_db`/`web_client_factory` (tests/web/conftest.py) — all tmp-backed.
- Applying the migration to the LIVE `finances.db` is Julio's call, not the implementing agent's (execution rule 3 + dry-run-default preference). See Task 6 deploy note.
- Every commit message carries the trailer `Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>` (shown as a second `-m` flag in the commands below).
- Exact contract names (consumed by other WPs): `Transaction.notes: str | None = None`; `transactions_repo.update(..., notes=_UNSET)`; `TransactionEditRequest.set_notes: bool = False` + `TransactionEditRequest.notes: str | None = None`; `TransactionCard.notes: str | None = None`.

---

### Task 1: Migration 008 + `Transaction.notes` model field

**Files:**
- Create: `finances/db/migrations/008_add_transaction_notes.sql`
- Modify: `finances/domain/models.py` (Transaction class, after `needs_review: bool = False`, line ~105)
- Modify: `tests/conftest.py` (`TransactionFactory` class attrs, after `description = None`, line ~287)
- Test: `tests/test_transaction_notes_repo.py` (new file)

**Interfaces:**
- Consumes: `in_memory_db` fixture (migrations auto-applied via `apply_migrations`), `transaction_factory` fixture (both from `tests/conftest.py`).
- Produces: `transactions.notes` TEXT column (nullable, no default); `Transaction.notes: str | None = None` — every later task relies on both.

**Steps:**

- [ ] Write the failing tests — create `tests/test_transaction_notes_repo.py` with exactly this content:

```python
"""WP3 — transaction notes: schema, model, and repo thread.

Plan: docs/plans/ux-overhaul/03-notes.md. Per rule-011 these tests land
before the implementation. This file covers the DB side:

* migration 008_add_transaction_notes.sql adds a nullable ``notes`` column,
* ``Transaction.notes`` Pydantic field (default ``None``),
* repo round-trip (insert / get_by_id / get_by_source_ref / list_by_account),
* ``update(notes=...)`` via the ``_UNSET`` sentinel,
* ``upsert_by_source_ref`` NEVER overwrites an existing manual note on
  re-ingest (same enrichment-preservation contract as category_id/user_rate).
"""

from __future__ import annotations

import sqlite3

from finances.db.repos import transactions as transactions_repo


# ---------------------------------------------------------------------------
# Task 1 — schema + model.
# ---------------------------------------------------------------------------


def test_transactions_table_has_notes_column(
    in_memory_db: sqlite3.Connection,
) -> None:
    cols = {
        row["name"]
        for row in in_memory_db.execute("PRAGMA table_info(transactions)").fetchall()
    }
    assert "notes" in cols


def test_transaction_model_accepts_notes_and_defaults_to_none(
    transaction_factory,
) -> None:
    with_note = transaction_factory.build(notes="split with Maria")
    assert with_note.notes == "split with Maria"

    without_note = transaction_factory.build(notes=None)
    assert without_note.notes is None
```

- [ ] Run it and confirm both tests FAIL:

```bash
uv run pytest -q tests/test_transaction_notes_repo.py
```

Expected: `2 failed` — `test_transactions_table_has_notes_column` with `AssertionError: assert 'notes' in {...}` and `test_transaction_model_accepts_notes_and_defaults_to_none` with `pydantic_core._pydantic_core.ValidationError: ... notes ... Extra inputs are not permitted`.

- [ ] Commit the tests:

```bash
git add tests/test_transaction_notes_repo.py
git commit -m "test(db): transactions.notes column + Transaction.notes field" -m "Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

- [ ] Create `finances/db/migrations/008_add_transaction_notes.sql` with exactly this content:

```sql
-- 008_add_transaction_notes.sql
-- UX overhaul WP3 (2026-07-11): single free-text notes column per
-- transaction (docs/plans/ux-overhaul/00-design.md §4).
-- Nullable, no default. Written only through transactions_repo.update()
-- (rule-012); re-ingest preserves it via the upsert COALESCE clause in
-- finances/db/repos/transactions.py::upsert_by_source_ref.
--
-- NOTE: the design spec named this 007_add_transaction_notes.sql, but
-- 007_going_out_and_bank_fee_rules.sql already existed, so this file
-- takes the next free prefix. The runner applies each file exactly once
-- (tracked in _migrations), so ALTER TABLE here is safe.

ALTER TABLE transactions ADD COLUMN notes TEXT;
```

- [ ] In `finances/domain/models.py`, add the field to `Transaction` — change:

```python
    source_ref: str | None = None
    needs_review: bool = False
```

to:

```python
    source_ref: str | None = None
    needs_review: bool = False
    notes: str | None = None
```

- [ ] In `tests/conftest.py`, pin the factory default so `TransactionFactory.build()` stays deterministic (`__allow_none_optionals__ = 0.0` would otherwise generate a random string for every optional field) — change:

```python
    user_rate = None
    description = None
    needs_review = False
```

to:

```python
    user_rate = None
    description = None
    needs_review = False
    notes = None
```

- [ ] Run the task tests, then the schema suite as a regression check:

```bash
uv run pytest -q tests/test_transaction_notes_repo.py
uv run pytest -q tests/test_db_schema.py
```

Expected: `2 passed` for the first command; all passing for the second.

- [ ] Commit the implementation:

```bash
git add finances/db/migrations/008_add_transaction_notes.sql finances/domain/models.py tests/conftest.py
git commit -m "feat(db): add nullable notes column to transactions (migration 008)" -m "Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 2: Repo thread — SELECTs, insert, `update(notes=_UNSET)`, upsert preserve clause

**Files:**
- Modify: `finances/db/repos/transactions.py` (`_row_to_transaction` ~line 47, `insert` ~73, `get_by_id` ~99, `get_by_source_ref` ~111, `upsert_by_source_ref` ~125, `list_by_account` ~197, `update` ~219)
- Test: `tests/test_transaction_notes_repo.py` (append)

**Interfaces:**
- Consumes: `Transaction.notes` (Task 1), `_UNSET` sentinel (already in the file), `seeded_db` + `transaction_factory` fixtures.
- Produces: `update(conn, *, id, category_id=_UNSET, user_rate=_UNSET, needs_review=_UNSET, notes=_UNSET) -> Transaction`; `upsert_by_source_ref` with `notes = COALESCE(transactions.notes, excluded.notes)`; every SELECT/insert carries `notes`. Tasks 3-5 rely on all of these.

Preserve-clause semantics (decided here, deliberately): the existing enrichment pattern for category/rate is `COALESCE(excluded.X, transactions.X)` — incoming non-NULL wins. Notes are stronger: they are manual-only enrichment (only the viewer edit modal writes them), and the spec (00-design.md §4) mandates `COALESCE(transactions.notes, excluded.notes)` — **existing note always wins**; an incoming note only fills a row that has none. Both directions are pinned by tests below.

**Steps:**

- [ ] Append the failing tests to `tests/test_transaction_notes_repo.py`:

```python
# ---------------------------------------------------------------------------
# Task 2 — repo thread: insert/select/update/upsert.
# ---------------------------------------------------------------------------


def test_insert_and_get_round_trips_notes(
    seeded_db: sqlite3.Connection, transaction_factory
) -> None:
    txn = transaction_factory.build(account_id=1, notes="cash from abuela")
    saved = transactions_repo.insert(seeded_db, txn)
    assert saved.id is not None

    fetched = transactions_repo.get_by_id(seeded_db, saved.id)
    assert fetched is not None
    assert fetched.notes == "cash from abuela"


def test_list_by_account_carries_notes(
    seeded_db: sqlite3.Connection, transaction_factory
) -> None:
    transactions_repo.insert(
        seeded_db, transaction_factory.build(account_id=1, notes="visible in lists")
    )
    rows = transactions_repo.list_by_account(seeded_db, 1)
    assert any(t.notes == "visible in lists" for t in rows)


def test_update_sets_notes_only(
    seeded_db: sqlite3.Connection, transaction_factory
) -> None:
    saved = transactions_repo.insert(
        seeded_db, transaction_factory.build(account_id=1)
    )
    assert saved.id is not None

    updated = transactions_repo.update(seeded_db, id=saved.id, notes="ask Maria")

    assert updated.notes == "ask Maria"
    assert updated.category_id == saved.category_id  # untouched
    assert updated.user_rate == saved.user_rate      # untouched
    assert updated.amount == saved.amount            # untouched


def test_update_unset_leaves_notes_untouched(
    seeded_db: sqlite3.Connection, transaction_factory
) -> None:
    saved = transactions_repo.insert(
        seeded_db, transaction_factory.build(account_id=1, notes="keep me")
    )
    assert saved.id is not None

    updated = transactions_repo.update(seeded_db, id=saved.id, needs_review=True)
    assert updated.notes == "keep me"


def test_update_clears_notes_with_explicit_none(
    seeded_db: sqlite3.Connection, transaction_factory
) -> None:
    saved = transactions_repo.insert(
        seeded_db, transaction_factory.build(account_id=1, notes="obsolete")
    )
    assert saved.id is not None

    updated = transactions_repo.update(seeded_db, id=saved.id, notes=None)
    assert updated.notes is None


def test_upsert_reingest_never_wipes_manual_note(
    seeded_db: sqlite3.Connection, transaction_factory
) -> None:
    """MANDATORY (00-design.md §4): re-ingesting the same statement row over
    a manually-noted transaction keeps the note."""
    incoming = transaction_factory.build(
        account_id=1, source="provincial", source_ref="prov-note-1", notes=None
    )
    first = transactions_repo.upsert_by_source_ref(seeded_db, incoming)
    assert first["rows_inserted"] == 1

    transactions_repo.update(
        seeded_db, id=first["id"], notes="rent — split with Maria"
    )

    # Re-ingest: identical (source, source_ref); statements never carry notes.
    second = transactions_repo.upsert_by_source_ref(seeded_db, incoming)
    assert second["rows_inserted"] == 0
    assert second["rows_updated"] == 1

    after = transactions_repo.get_by_source_ref(
        seeded_db, "provincial", "prov-note-1"
    )
    assert after is not None
    assert after.notes == "rent — split with Maria"


def test_upsert_incoming_note_never_overwrites_existing_note(
    seeded_db: sqlite3.Connection, transaction_factory
) -> None:
    incoming = transaction_factory.build(
        account_id=1, source="provincial", source_ref="prov-note-2", notes=None
    )
    row = transactions_repo.upsert_by_source_ref(seeded_db, incoming)
    transactions_repo.update(seeded_db, id=row["id"], notes="manual wins")

    noisy = incoming.model_copy(update={"notes": "machine note"})
    transactions_repo.upsert_by_source_ref(seeded_db, noisy)

    after = transactions_repo.get_by_source_ref(
        seeded_db, "provincial", "prov-note-2"
    )
    assert after is not None
    assert after.notes == "manual wins"


def test_upsert_fills_note_when_row_has_none(
    seeded_db: sqlite3.Connection, transaction_factory
) -> None:
    """Pins the COALESCE clause itself: simply omitting notes from the
    UPDATE branch would pass the two tests above but fail this one."""
    bare = transaction_factory.build(
        account_id=1, source="provincial", source_ref="prov-note-3", notes=None
    )
    transactions_repo.upsert_by_source_ref(seeded_db, bare)

    with_note = bare.model_copy(update={"notes": "added on second import"})
    transactions_repo.upsert_by_source_ref(seeded_db, with_note)

    after = transactions_repo.get_by_source_ref(
        seeded_db, "provincial", "prov-note-3"
    )
    assert after is not None
    assert after.notes == "added on second import"
```

- [ ] Run and confirm the new tests FAIL:

```bash
uv run pytest -q tests/test_transaction_notes_repo.py
```

Expected: `2 passed, 8 failed` (all 8 new tests fail) — the tests that pass `notes=` to `update()` with `TypeError: update() got an unexpected keyword argument 'notes'`; the rest (the insert/list round-trips, `test_update_unset_leaves_notes_untouched`, `test_upsert_fills_note_when_row_has_none`) with assertion failures like `AssertionError: assert None == 'cash from abuela'` (repo INSERT/SELECT/upsert ignore the column).

- [ ] Commit the tests:

```bash
git add tests/test_transaction_notes_repo.py
git commit -m "test(db): thread notes through transactions repo + upsert preservation" -m "Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

- [ ] Implement in `finances/db/repos/transactions.py`. Replace `_row_to_transaction` with:

```python
def _row_to_transaction(row: sqlite3.Row) -> Transaction:
    return Transaction(
        id=row["id"],
        account_id=row["account_id"],
        occurred_at=row["occurred_at"],
        kind=TransactionKind(row["kind"]),
        amount=row["amount"] if isinstance(row["amount"], Decimal) else Decimal(str(row["amount"])),
        currency=row["currency"],
        description=row["description"],
        category_id=row["category_id"],
        transfer_id=row["transfer_id"],
        user_rate=(
            None
            if row["user_rate"] is None
            else (
                row["user_rate"]
                if isinstance(row["user_rate"], Decimal)
                else Decimal(str(row["user_rate"]))
            )
        ),
        source=row["source"],
        source_ref=row["source_ref"],
        needs_review=bool(row["needs_review"]),
        notes=row["notes"],
    )
```

- [ ] Replace `insert` with (13th column + placeholder + param):

```python
def insert(conn: sqlite3.Connection, txn: Transaction) -> Transaction:
    cur = conn.execute(
        """
        INSERT INTO transactions (
            account_id, occurred_at, kind, amount, currency, description,
            category_id, transfer_id, user_rate, source, source_ref,
            needs_review, notes
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            txn.account_id,
            _iso(txn.occurred_at),
            txn.kind.value,
            _to_text(txn.amount),
            txn.currency,
            txn.description,
            txn.category_id,
            txn.transfer_id,
            _to_text(txn.user_rate),
            txn.source,
            txn.source_ref,
            1 if txn.needs_review else 0,
            txn.notes,
        ),
    )
    return txn.model_copy(update={"id": cur.lastrowid})
```

- [ ] Update the three SELECT column lists. In `get_by_id`:

```python
    row = conn.execute(
        """
        SELECT id, account_id, occurred_at, kind, amount, currency, description,
               category_id, transfer_id, user_rate, source, source_ref,
               needs_review, notes
        FROM transactions WHERE id = ?
        """,
        (transaction_id,),
    ).fetchone()
```

In `get_by_source_ref`:

```python
    row = conn.execute(
        """
        SELECT id, account_id, occurred_at, kind, amount, currency, description,
               category_id, transfer_id, user_rate, source, source_ref,
               needs_review, notes
        FROM transactions WHERE source = ? AND source_ref = ?
        """,
        (source, source_ref),
    ).fetchone()
```

In `list_by_account`:

```python
    sql = """
        SELECT id, account_id, occurred_at, kind, amount, currency, description,
               category_id, transfer_id, user_rate, source, source_ref,
               needs_review, notes
        FROM transactions WHERE account_id = ?
        ORDER BY occurred_at DESC, id DESC
    """
```

- [ ] Replace `upsert_by_source_ref`'s params tuple, INSERT statement, and ON CONFLICT clause with:

```python
    existing = get_by_source_ref(conn, txn.source, txn.source_ref)
    params = (
        txn.account_id,
        _iso(txn.occurred_at),
        txn.kind.value,
        _to_text(txn.amount),
        txn.currency,
        txn.description,
        txn.category_id,
        txn.transfer_id,
        _to_text(txn.user_rate),
        txn.source,
        txn.source_ref,
        1 if txn.needs_review else 0,
        txn.notes,
    )
    conn.execute(
        """
        INSERT INTO transactions (
            account_id, occurred_at, kind, amount, currency, description,
            category_id, transfer_id, user_rate, source, source_ref,
            needs_review, notes
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(source, source_ref) DO UPDATE SET
            account_id   = excluded.account_id,
            occurred_at  = excluded.occurred_at,
            -- A paired row's kind came from the pairing pass; a raw
            -- statement re-ingest must not flip it back.
            kind         = CASE WHEN transactions.transfer_id IS NOT NULL
                                THEN transactions.kind ELSE excluded.kind END,
            amount       = excluded.amount,
            currency     = excluded.currency,
            description  = excluded.description,
            -- Enrichment (triage category, pairing, user rate) lives on the
            -- row but is not sourced from raw statements: keep existing
            -- values unless the incoming row actually carries one.
            category_id  = COALESCE(excluded.category_id, transactions.category_id),
            transfer_id  = COALESCE(excluded.transfer_id, transactions.transfer_id),
            user_rate    = COALESCE(excluded.user_rate, transactions.user_rate),
            -- Notes are manual-only enrichment (viewer edit modal). Stronger
            -- than the category/rate pattern: an EXISTING note always wins;
            -- an incoming note only fills a row that has none (00-design §4).
            notes        = COALESCE(transactions.notes, excluded.notes),
            needs_review = CASE WHEN transactions.needs_review = 0 THEN 0
                                ELSE excluded.needs_review END,
            updated_at   = CURRENT_TIMESTAMP
        """,
        params,
    )
```

Also extend the docstring's PRESERVES sentence to mention notes: change `category_id, transfer_id, user_rate, a resolved needs_review, and` → `category_id, transfer_id, user_rate, notes, a resolved needs_review, and`.

- [ ] Replace `update`'s signature and field blocks with:

```python
def update(
    conn: sqlite3.Connection,
    *,
    id: int,
    category_id: int | None | _Unset = _UNSET,
    user_rate: Decimal | None | _Unset = _UNSET,
    needs_review: bool | _Unset = _UNSET,
    notes: str | None | _Unset = _UNSET,
) -> Transaction:
```

(docstring unchanged) and, after the existing `needs_review` block inside the function body, add:

```python
    if not isinstance(notes, _Unset):
        sets.append("notes = ?")
        params.append(notes)
```

- [ ] Run the tests, then the neighboring suites that exercise the repo heavily:

```bash
uv run pytest -q tests/test_transaction_notes_repo.py
uv run pytest -q tests/test_db_coverage_gaps.py tests/test_ingest_provincial.py tests/test_transfers.py
```

Expected: `10 passed` for the first command; all passing for the second (the upsert/insert signatures are internal — callers pass `Transaction` models, unchanged).

- [ ] Commit the implementation:

```bash
git add finances/db/repos/transactions.py
git commit -m "feat(db): thread notes through transactions repo; re-ingest never wipes a note" -m "Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 3: `TransactionCard.notes` + `q` search matches description OR notes

**Files:**
- Modify: `finances/web/services/transactions_query.py` (`TransactionCard` ~line 83, `_build_where` q-branch ~181, `_row_to_transaction` ~190, `_project_card` return sites ~236/253/270, `list_sql` ~308)
- Test: `tests/web/test_transaction_notes_web.py` (new file)

**Interfaces:**
- Consumes: `transactions_repo.update(..., notes=...)` (Task 2), `seeded_web_db` + `web_client_factory` fixtures (tests/web/conftest.py).
- Produces: `TransactionCard.notes: str | None = None` (JSON surface of `/api/transactions` and every card render context — Tasks 4-5 rely on it); `TransactionsFilter.q` matching `t.description OR t.notes`. No change to `filter_from_query` (the `q` query param already exists).

All card construction flows through `_project_card` (the only three `TransactionCard(` constructor sites in `finances/` live in this file; dashboard recent-activity and triage cards reuse it), so adding the field here covers every surface.

**Steps:**

- [ ] Create `tests/web/test_transaction_notes_web.py` with exactly this content:

```python
"""WP3 — transaction notes on the web layer.

Plan: docs/plans/ux-overhaul/03-notes.md. Written before the
implementation per rule-011. Coverage:

* ``TransactionCard.notes`` projected by ``query_transactions``,
* the ``q`` free-text filter matches description OR notes,
* ``apply_edit`` / ``TransactionEditRequest`` set + clear notes (Task 4),
* PATCH /api/transactions/{id} notes round-trip (Task 4),
* modal partials render a prefilled ``notes`` textarea (Task 5),
* form-encoded edit endpoints persist notes (Task 5),
* card_transaction.html shows a note indicator + snippet (Task 5).
"""

from __future__ import annotations

import sqlite3
from datetime import date

from fastapi.testclient import TestClient

from finances.db.repos import transactions as transactions_repo


def _txn_id_by_source_ref(conn: sqlite3.Connection, source_ref: str) -> int:
    row = conn.execute(
        "SELECT id FROM transactions WHERE source_ref = ?", (source_ref,)
    ).fetchone()
    assert row is not None, f"seeded txn {source_ref} not present"
    return int(row["id"])


# ---------------------------------------------------------------------------
# Task 3 — card projection + q search.
# ---------------------------------------------------------------------------


def test_query_transactions_projects_notes(
    seeded_web_db: sqlite3.Connection,
) -> None:
    from finances.web.services.transactions_query import (
        TransactionsFilter,
        query_transactions,
    )

    txn_id = _txn_id_by_source_ref(seeded_web_db, "prov-1")
    transactions_repo.update(seeded_web_db, id=txn_id, notes="bodega tab settled")

    page = query_transactions(
        seeded_web_db, TransactionsFilter(date_from=date(2000, 1, 1))
    )
    card = next(c for c in page.rows if c.id == txn_id)
    assert card.notes == "bodega tab settled"


def test_q_filter_matches_notes(seeded_web_db: sqlite3.Connection) -> None:
    from finances.web.services.transactions_query import (
        TransactionsFilter,
        query_transactions,
    )

    txn_id = _txn_id_by_source_ref(seeded_web_db, "prov-2")
    transactions_repo.update(
        seeded_web_db, id=txn_id, notes="vacation fund with Maria"
    )

    page = query_transactions(
        seeded_web_db,
        TransactionsFilter(date_from=date(2000, 1, 1), q="vacation"),
    )
    assert [c.id for c in page.rows] == [txn_id]


def test_q_filter_still_matches_description(
    seeded_web_db: sqlite3.Connection,
) -> None:
    from finances.web.services.transactions_query import (
        TransactionsFilter,
        query_transactions,
    )

    txn_id = _txn_id_by_source_ref(seeded_web_db, "prov-1")
    page = query_transactions(
        seeded_web_db,
        TransactionsFilter(date_from=date(2000, 1, 1), q="bodega"),
    )
    assert [c.id for c in page.rows] == [txn_id]


def test_api_transactions_q_searches_notes(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    txn_id = _txn_id_by_source_ref(seeded_web_db, "prov-2")
    transactions_repo.update(seeded_web_db, id=txn_id, notes="vacation fund")

    client: TestClient = web_client_factory()
    resp = client.get(
        "/api/transactions", params={"q": "vacation", "date_from": "2000-01-01"}
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["total"] == 1
    assert body["rows"][0]["id"] == txn_id
    assert body["rows"][0]["notes"] == "vacation fund"
```

- [ ] Run and confirm the new tests FAIL:

```bash
uv run pytest -q tests/web/test_transaction_notes_web.py
```

Expected: `3 failed, 1 passed` — `card.notes` raising `AttributeError: 'TransactionCard' object has no attribute 'notes'`, `test_q_filter_matches_notes` with `AssertionError: assert [] == [<id>]`, the API test with `AssertionError: assert 0 == 1`. `test_q_filter_still_matches_description` PASSES at this stage and that is expected: `q` already matches descriptions (the seeded `prov-1` row's `COM.PAGO bodega`); the test is a regression pin, not a new-behavior probe.

- [ ] Commit the tests:

```bash
git add tests/web/test_transaction_notes_web.py
git commit -m "test(web): project notes onto transaction cards; q searches notes" -m "Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

- [ ] Implement in `finances/web/services/transactions_query.py`. Add the field to `TransactionCard` — change:

```python
    kind: str
    category_name: str | None
    needs_review: bool
```

to:

```python
    kind: str
    category_name: str | None
    needs_review: bool
    notes: str | None = None
```

- [ ] In `_build_where`, replace the q-branch:

```python
    if f.q:
        where.append("t.description LIKE ?")
        # Bind the wildcard pattern as a parameter, never interpolated.
        params.append(f"%{f.q}%")
```

with:

```python
    if f.q:
        # Free-text search covers the statement description AND the manual
        # note (WP3). NULL notes are fine: NULL LIKE x is NULL, OR handles it.
        where.append("(t.description LIKE ? OR t.notes LIKE ?)")
        # Bind the wildcard pattern as a parameter, never interpolated.
        pattern = f"%{f.q}%"
        params.extend([pattern, pattern])
```

- [ ] In this file's `_row_to_transaction`, add `notes=row["notes"],` immediately after `needs_review=bool(row["needs_review"]),` (mirrors the Task 2 repo change).

- [ ] In `_project_card`, add `notes=txn.notes,` to ALL THREE `TransactionCard(` constructor calls (the `_NATIVE_USD_CURRENCIES` branch ~line 236, the `rate is None` branch ~line 253, and the final return ~line 270), in each case immediately after `needs_review=...`. Example (final return; apply identically to the other two):

```python
    return TransactionCard(
        id=txn.id,
        occurred_at=txn.occurred_at,
        account_name=account_name,
        description=txn.description or "",
        amount_native=txn.amount,
        currency=txn.currency,
        amount_usd=amount_usd,
        rate_source=source,
        is_bcv_fallback=is_bcv,
        kind=txn.kind.value,
        category_name=category_name,
        needs_review=txn.needs_review,
        notes=txn.notes,
    )
```

- [ ] In `query_transactions`, add `t.notes` to `list_sql`'s SELECT — change:

```python
            t.description, t.category_id, t.transfer_id, t.user_rate,
            t.source, t.source_ref, t.needs_review,
```

to:

```python
            t.description, t.category_id, t.transfer_id, t.user_rate,
            t.source, t.source_ref, t.needs_review, t.notes,
```

- [ ] Run the task tests plus the read-side web suites:

```bash
uv run pytest -q tests/web/test_transaction_notes_web.py
uv run pytest -q tests/web/test_transactions_read.py tests/web/test_dashboard.py tests/web/test_triage.py
```

Expected: `4 passed` for the first; all passing for the second (added field is optional-with-default so existing `TransactionCard(...)` literals in tests stay valid).

- [ ] Commit the implementation:

```bash
git add finances/web/services/transactions_query.py
git commit -m "feat(web): notes on TransactionCard + q filter matches description or notes" -m "Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 4: `TransactionEditRequest.set_notes/notes` → `apply_edit` → PATCH round-trip

**Files:**
- Modify: `finances/web/services/transactions_write.py` (`TransactionEditRequest` ~line 33, `apply_edit` update_kwargs block ~line 79)
- Test: `tests/web/test_transaction_notes_web.py` (append)

**Interfaces:**
- Consumes: `transactions_repo.update(..., notes=...)` (Task 2), `TransactionCard.notes` (Task 3).
- Produces: `TransactionEditRequest(set_notes: bool = False, notes: str | None = None)` — the exact WP contract shape; `PATCH /api/transactions/{txn_id}` accepts it with zero changes to `finances/web/routers/api.py` (the endpoint already deserializes the model and returns the card via `response_model=TransactionCard`).

**Steps:**

- [ ] Append the failing tests to `tests/web/test_transaction_notes_web.py`:

```python
# ---------------------------------------------------------------------------
# Task 4 — TransactionEditRequest / apply_edit / PATCH api.
# ---------------------------------------------------------------------------


def test_apply_edit_sets_notes(seeded_web_db: sqlite3.Connection) -> None:
    from finances.web.services.transactions_write import (
        TransactionEditRequest,
        apply_edit,
    )

    txn_id = _txn_id_by_source_ref(seeded_web_db, "prov-1")
    card = apply_edit(
        seeded_web_db,
        txn_id=txn_id,
        req=TransactionEditRequest(set_notes=True, notes="paid back in cash"),
    )
    assert card.notes == "paid back in cash"

    after = transactions_repo.get_by_id(seeded_web_db, txn_id)
    assert after is not None
    assert after.notes == "paid back in cash"


def test_apply_edit_clears_notes_with_none(
    seeded_web_db: sqlite3.Connection,
) -> None:
    from finances.web.services.transactions_write import (
        TransactionEditRequest,
        apply_edit,
    )

    txn_id = _txn_id_by_source_ref(seeded_web_db, "prov-1")
    transactions_repo.update(seeded_web_db, id=txn_id, notes="stale note")

    card = apply_edit(
        seeded_web_db,
        txn_id=txn_id,
        req=TransactionEditRequest(set_notes=True, notes=None),
    )
    assert card.notes is None

    after = transactions_repo.get_by_id(seeded_web_db, txn_id)
    assert after is not None
    assert after.notes is None


def test_apply_edit_without_set_notes_leaves_note(
    seeded_web_db: sqlite3.Connection,
) -> None:
    from decimal import Decimal

    from finances.web.services.transactions_write import (
        TransactionEditRequest,
        apply_edit,
    )

    txn_id = _txn_id_by_source_ref(seeded_web_db, "prov-1")
    transactions_repo.update(seeded_web_db, id=txn_id, notes="do not touch")

    apply_edit(
        seeded_web_db,
        txn_id=txn_id,
        req=TransactionEditRequest(set_user_rate=True, user_rate=Decimal("36.5")),
    )

    after = transactions_repo.get_by_id(seeded_web_db, txn_id)
    assert after is not None
    assert after.notes == "do not touch"


def test_patch_endpoint_notes_round_trip(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    client: TestClient = web_client_factory()
    txn_id = _txn_id_by_source_ref(seeded_web_db, "prov-3")

    resp = client.patch(
        f"/api/transactions/{txn_id}",
        json={"set_notes": True, "notes": "receipt in the drawer"},
    )
    assert resp.status_code == 200, resp.text
    assert resp.json()["notes"] == "receipt in the drawer"

    # Round trip: the note is persisted AND findable via q.
    resp2 = client.get(
        "/api/transactions", params={"q": "drawer", "date_from": "2000-01-01"}
    )
    assert resp2.status_code == 200
    assert [r["id"] for r in resp2.json()["rows"]] == [txn_id]
```

- [ ] Run and confirm the new tests FAIL:

```bash
uv run pytest -q tests/web/test_transaction_notes_web.py
```

Expected: `5 passed, 3 failed` — the two service tests that construct `TransactionEditRequest(set_notes=...)` with `pydantic_core._pydantic_core.ValidationError: ... set_notes ... Extra inputs are not permitted`, the PATCH test with `assert 422 == 200` (extra-forbid model rejects the body). `test_apply_edit_without_set_notes_leaves_note` PASSES at this stage and that is expected: it never uses the new fields (its request is `set_user_rate` only) and the repo-side notes support it relies on landed in Task 2 — it pins the no-regression path.

- [ ] Commit the tests:

```bash
git add tests/web/test_transaction_notes_web.py
git commit -m "test(web): notes through TransactionEditRequest, apply_edit, PATCH api" -m "Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

- [ ] Implement in `finances/web/services/transactions_write.py`. Replace the `TransactionEditRequest` field block:

```python
    model_config = ConfigDict(extra="forbid")

    set_category: bool = False
    category_id: int | None = None
    set_user_rate: bool = False
    user_rate: Decimal | None = None
```

with:

```python
    model_config = ConfigDict(extra="forbid")

    set_category: bool = False
    category_id: int | None = None
    set_user_rate: bool = False
    user_rate: Decimal | None = None
    set_notes: bool = False
    notes: str | None = None
```

and in the class docstring change `The two ``set_*`` flags disambiguate` → `The ``set_*`` flags disambiguate`.

- [ ] In `apply_edit`, extend the update_kwargs block — change:

```python
    update_kwargs: dict[str, object] = {}
    if req.set_category:
        update_kwargs["category_id"] = req.category_id
    if req.set_user_rate:
        update_kwargs["user_rate"] = req.user_rate
```

to:

```python
    update_kwargs: dict[str, object] = {}
    if req.set_category:
        update_kwargs["category_id"] = req.category_id
    if req.set_user_rate:
        update_kwargs["user_rate"] = req.user_rate
    if req.set_notes:
        update_kwargs["notes"] = req.notes
```

(Also update step 1 of the `apply_edit` docstring: `Apply ``category_id`` and/or ``user_rate`` updates via` → `Apply ``category_id``, ``user_rate`` and/or ``notes`` updates via`.)

- [ ] Run the task tests plus the write-path regression suite:

```bash
uv run pytest -q tests/web/test_transaction_notes_web.py
uv run pytest -q tests/web/test_transactions_write.py
```

Expected: `8 passed` for the first; all passing for the second.

- [ ] Commit the implementation:

```bash
git add finances/web/services/transactions_write.py
git commit -m "feat(web): set_notes/notes on TransactionEditRequest, threaded through apply_edit" -m "Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 5: UI — modal textareas, form endpoints, card note indicator

**Files:**
- Modify: `finances/web/routers/partials.py` (form parsers ~line 169; `transactions_edit_partial` ~line 255; `triage_edit_partial` ~line 392)
- Modify: `finances/web/templates/partials/modal_transaction.html` (hidden inputs ~line 103; form fields after the user_rate label, before `.tx-modal-actions` ~line 127)
- Modify: `finances/web/templates/partials/modal_transaction_triage.html` (hidden inputs ~line 78; form fields before `.tx-modal-actions` ~line 102)
- Modify: `finances/web/templates/partials/card_transaction.html` (description cell, lines 40-42)
- Test: `tests/web/test_transaction_notes_web.py` (append)

**Interfaces:**
- Consumes: `TransactionEditRequest.set_notes/notes` + `apply_edit` (Task 4), `TransactionCard.notes` (Task 3), existing `_parse_form_bool` in partials.py.
- Produces: form contract `set_notes=true` + `notes=<text>` on `POST /_partial/transactions/{id}/edit` and `POST /_partial/triage/{id}/edit`; `data-note-indicator` span inside the card's description cell.

Coordination note (WP2 interaction): the modals currently use the "always-set hidden sentinel" convention (`set_category=true` etc.). This task follows the file's current convention — `set_notes=true` hidden + a textarea PREFILLED with the existing note, so an untouched submit round-trips the note (no wipe hazard, unlike the empty-select category bug WP2 fixes). If WP2 has already landed and replaced the hidden sentinels with dirty-tracking, wire `set_notes` through that same mechanism instead of a hidden constant — the server-side contract in this task (`set_notes`/`notes` form fields) is identical either way. In that WP2-first ordering, also adjust the two modal-render tests accordingly: in `test_modal_partial_renders_notes_textarea_prefilled` and `test_triage_modal_renders_notes_textarea`, replace the `'name="set_notes" value="true"'` assertions with assertions on WP2's dirty-tracking wiring for the notes field (whatever attribute/markup WP2 standardized), keeping the form-post tests unchanged since the server contract is the same either way.

**Steps:**

- [ ] Append the failing tests to `tests/web/test_transaction_notes_web.py`:

```python
# ---------------------------------------------------------------------------
# Task 5 — modal textareas, form endpoints, card indicator.
# ---------------------------------------------------------------------------


def test_modal_partial_renders_notes_textarea_prefilled(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    txn_id = _txn_id_by_source_ref(seeded_web_db, "prov-1")
    transactions_repo.update(
        seeded_web_db, id=txn_id, notes="check against receipt"
    )
    client: TestClient = web_client_factory()

    resp = client.get(f"/_partial/transactions/{txn_id}/modal")
    assert resp.status_code == 200
    body = resp.text
    assert "<textarea" in body
    assert 'name="notes"' in body
    assert "check against receipt" in body
    assert 'name="set_notes" value="true"' in body


def test_triage_modal_renders_notes_textarea(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    txn_id = _txn_id_by_source_ref(seeded_web_db, "prov-needs-review")
    client: TestClient = web_client_factory()

    resp = client.get(f"/_partial/triage/{txn_id}/modal")
    assert resp.status_code == 200
    assert 'name="notes"' in resp.text
    assert 'name="set_notes" value="true"' in resp.text


def test_edit_form_post_saves_notes(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    client: TestClient = web_client_factory()
    txn_id = _txn_id_by_source_ref(seeded_web_db, "prov-1")

    resp = client.post(
        f"/_partial/transactions/{txn_id}/edit",
        data={
            "set_category": "false",
            "category_id": "",
            "set_user_rate": "false",
            "user_rate": "",
            "set_notes": "true",
            "notes": "lunch with the team",
        },
    )
    assert resp.status_code == 200, resp.text

    after = transactions_repo.get_by_id(seeded_web_db, txn_id)
    assert after is not None
    assert after.notes == "lunch with the team"


def test_edit_form_empty_notes_clears(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    client: TestClient = web_client_factory()
    txn_id = _txn_id_by_source_ref(seeded_web_db, "prov-1")
    transactions_repo.update(seeded_web_db, id=txn_id, notes="stale")

    resp = client.post(
        f"/_partial/transactions/{txn_id}/edit",
        data={
            "set_category": "false",
            "category_id": "",
            "set_user_rate": "false",
            "user_rate": "",
            "set_notes": "true",
            "notes": "",
        },
    )
    assert resp.status_code == 200, resp.text

    after = transactions_repo.get_by_id(seeded_web_db, txn_id)
    assert after is not None
    assert after.notes is None


def test_edit_form_without_notes_fields_leaves_note(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    """Old-shape callers (no set_notes/notes fields) must not clear notes."""
    client: TestClient = web_client_factory()
    txn_id = _txn_id_by_source_ref(seeded_web_db, "prov-1")
    transactions_repo.update(seeded_web_db, id=txn_id, notes="survives")

    resp = client.post(
        f"/_partial/transactions/{txn_id}/edit",
        data={
            "set_category": "false",
            "category_id": "",
            "set_user_rate": "true",
            "user_rate": "36.5",
        },
    )
    assert resp.status_code == 200, resp.text

    after = transactions_repo.get_by_id(seeded_web_db, txn_id)
    assert after is not None
    assert after.notes == "survives"


def test_triage_edit_form_saves_notes(
    seeded_web_db: sqlite3.Connection, web_client_factory
) -> None:
    client: TestClient = web_client_factory()
    txn_id = _txn_id_by_source_ref(seeded_web_db, "prov-needs-review")

    resp = client.post(
        f"/_partial/triage/{txn_id}/edit",
        data={
            "set_category": "false",
            "category_id": "",
            "set_user_rate": "true",
            "user_rate": "36.5",
            "set_notes": "true",
            "notes": "rate from that day's P2P screenshot",
        },
    )
    assert resp.status_code == 200, resp.text

    after = transactions_repo.get_by_id(seeded_web_db, txn_id)
    assert after is not None
    assert after.notes == "rate from that day's P2P screenshot"


def _render_card(notes: str | None) -> str:
    from datetime import UTC, datetime
    from decimal import Decimal

    from finances.web.app import create_app
    from finances.web.services.transactions_query import TransactionCard
    from finances.web.settings import WebSettings

    app = create_app(WebSettings(host="127.0.0.1"))
    templates = app.state.templates
    card = TransactionCard(
        id=42,
        occurred_at=datetime(2026, 7, 1, 12, 0, tzinfo=UTC),
        account_name="Provincial",
        description="probe",
        # Real expenses are NEGATIVE — do not copy seeded_web_db's signs.
        amount_native=Decimal("-1234.56"),
        currency="VES",
        amount_usd=Decimal("-33.82"),
        rate_source="binance_p2p_median",
        is_bcv_fallback=False,
        kind="expense",
        category_name="Groceries",
        needs_review=False,
        notes=notes,
    )
    return templates.get_template("partials/card_transaction.html").render(card=card)


def test_card_partial_shows_note_indicator_and_snippet() -> None:
    rendered = _render_card(notes="gift for mom, reimburse half")
    assert "data-note-indicator" in rendered
    assert "gift for mom" in rendered


def test_card_partial_hides_indicator_without_note() -> None:
    rendered = _render_card(notes=None)
    assert "data-note-indicator" not in rendered
```

- [ ] Run and confirm the new tests FAIL:

```bash
uv run pytest -q tests/web/test_transaction_notes_web.py
```

Expected: `10 passed, 6 failed` — modal tests with `AssertionError` on the missing `name="notes"`; form-post tests with `AssertionError: assert None == 'lunch with the team'` (FastAPI silently ignores undeclared form fields, so the note is never saved); `test_card_partial_shows_note_indicator_and_snippet` with `AssertionError` on missing `data-note-indicator`. Two of the new tests PASS pre-impl and that is expected: `test_edit_form_without_notes_fields_leaves_note` (pins the no-regression path — the endpoint ignores the absent fields and the repo leaves notes alone) and `test_card_partial_hides_indicator_without_note` (asserts `data-note-indicator` is ABSENT, which is trivially true before the template change).

- [ ] Commit the tests:

```bash
git add tests/web/test_transaction_notes_web.py
git commit -m "test(web): notes textarea in edit modals, form endpoints, card indicator" -m "Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

- [ ] Implement in `finances/web/routers/partials.py`. Add a parser next to `_parse_form_bool` (~line 197):

```python
def _parse_optional_text(value: str | None) -> str | None:
    """Empty / whitespace-only form input means "clear the field"."""
    if value is None:
        return None
    s = value.strip()
    return s or None
```

- [ ] Update `transactions_edit_partial` — replace its signature and request construction:

```python
@router.post("/transactions/{txn_id}/edit", include_in_schema=False)
def transactions_edit_partial(
    request: Request,
    txn_id: int,
    set_category: str | None = Form(default=None),
    category_id: str | None = Form(default=None),
    set_user_rate: str | None = Form(default=None),
    user_rate: str | None = Form(default=None),
    set_notes: str | None = Form(default=None),
    notes: str | None = Form(default=None),
    conn: sqlite3.Connection = Depends(get_conn),
):
    """Apply the modal-form edit and return the updated card partial.

    On success the response carries ``HX-Trigger: closeModal`` so the
    Alpine listener on ``<body>`` (added in base.html) can clear the
    modal host div.
    """
    req = TransactionEditRequest(
        set_category=_parse_form_bool(set_category),
        category_id=_parse_optional_int(category_id),
        set_user_rate=_parse_form_bool(set_user_rate),
        user_rate=_parse_optional_decimal(user_rate),
        set_notes=_parse_form_bool(set_notes),
        notes=_parse_optional_text(notes),
    )
```

(the rest of the function body — `try: card = apply_edit(...)` through `return response` — is unchanged).

- [ ] Update `triage_edit_partial` identically — replace its signature and request construction:

```python
@router.post("/triage/{txn_id}/edit", include_in_schema=False)
def triage_edit_partial(
    request: Request,
    txn_id: int,
    set_category: str | None = Form(default=None),
    category_id: str | None = Form(default=None),
    set_user_rate: str | None = Form(default=None),
    user_rate: str | None = Form(default=None),
    set_notes: str | None = Form(default=None),
    notes: str | None = Form(default=None),
    conn: sqlite3.Connection = Depends(get_conn),
):
    """Apply the edit and return a fresh queue partial.

    Sets ``HX-Trigger: closeModal, advanceQueue`` so the base.html
    Alpine listener clears the modal host and the page advances to the
    next item.
    """
    req = TransactionEditRequest(
        set_category=_parse_form_bool(set_category),
        category_id=_parse_optional_int(category_id),
        set_user_rate=_parse_form_bool(set_user_rate),
        user_rate=_parse_optional_decimal(user_rate),
        set_notes=_parse_form_bool(set_notes),
        notes=_parse_optional_text(notes),
    )
```

(rest of the function body unchanged).

- [ ] In `finances/web/templates/partials/modal_transaction.html`, extend the hidden sentinels (lines 103-104) — change:

```html
      {# Always-set sentinels: empty value clears the field. #}
      <input type="hidden" name="set_category" value="true">
      <input type="hidden" name="set_user_rate" value="true">
```

to:

```html
      {# Always-set sentinels: empty value clears the field. #}
      <input type="hidden" name="set_category" value="true">
      <input type="hidden" name="set_user_rate" value="true">
      <input type="hidden" name="set_notes" value="true">
```

then insert the notes field between the closing `</label>` of the user_rate block (line ~125) and `<div class="tx-modal-actions">`:

```html
      <label class="block">
        <span class="text-xs uppercase tracking-wide text-slate-500">Notes</span>
        <textarea
          name="notes"
          rows="2"
          class="mt-1 w-full border border-slate-300 rounded px-2 py-1 text-sm"
          placeholder="private note — survives re-ingest"
        >{{ txn.notes if txn.notes is not none else '' }}</textarea>
      </label>
```

(Keep `>{{ ... }}</textarea>` on one flow with no indentation between the tags, or the textarea value gains leading whitespace.)

- [ ] Apply the exact same two edits to `finances/web/templates/partials/modal_transaction_triage.html`: add `<input type="hidden" name="set_notes" value="true">` after the two existing hidden inputs (lines 78-79), and insert the identical `Notes` label/textarea block between the user_rate `</label>` (~line 100) and `<div class="tx-modal-actions">` (~line 102).

- [ ] In `finances/web/templates/partials/card_transaction.html`, replace the description cell (the card-row grid has exactly 6 children — the indicator nests INSIDE this cell, never as a new cell):

```html
  <span class="text-sm text-slate-900 truncate" title="{{ card.description }}">
    {{ card.description if card.description else '—' }}
  </span>
```

with:

```html
  <span class="text-sm text-slate-900 truncate" title="{{ card.description }}">
    {{ card.description if card.description else '—' }}
    {%- if card.notes %}
      <span class="text-xs text-slate-500 italic" data-note-indicator title="{{ card.notes }}">&#9998; {{ card.notes | truncate(40) }}</span>
    {%- endif %}
  </span>
```

(`&#9998;` is the pencil glyph; the full note stays readable via the `title` tooltip; the outer cell's `truncate` keeps the 6-track grid intact on long notes.)

- [ ] Run the task tests plus the full web suite:

```bash
uv run pytest -q tests/web/test_transaction_notes_web.py
uv run pytest -q tests/web
```

Expected: `16 passed` for the first; all passing for the second.

- [ ] Commit the implementation:

```bash
git add finances/web/routers/partials.py finances/web/templates/partials/modal_transaction.html finances/web/templates/partials/modal_transaction_triage.html finances/web/templates/partials/card_transaction.html
git commit -m "feat(web): notes UI — modal textareas, form params, card note snippet" -m "Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 6: Full-suite regression + migration idempotency check

**Files:**
- Test: none new — full-suite run + a scratch-DB check. NEVER run against the real `finances.db`.

**Interfaces:**
- Consumes: everything above.
- Produces: the WP3 verification evidence Julio needs to mark the package complete (execution rule 3).

**Steps:**

- [ ] Run the full unit suite:

```bash
uv run pytest -q
```

Expected: all tests pass, zero failures. If anything fails, fix forward within this WP's files only (no "while I'm in here" changes — execution rule 1).

- [ ] Verify migration idempotency on a throwaway DB (never the real one):

```bash
uv run python -c "
import pathlib, tempfile
from finances.db.connection import get_connection
from finances.db.migrate import apply_migrations
p = pathlib.Path(tempfile.mkdtemp()) / 'scratch.db'
conn = get_connection(p)
first = apply_migrations(conn)
second = apply_migrations(conn)
assert '008_add_transaction_notes.sql' in first, first
assert second == [], second
cols = {r[1] for r in conn.execute('PRAGMA table_info(transactions)')}
assert 'notes' in cols
conn.close()
print('migration idempotency OK')
"
```

Expected output: `migration idempotency OK`.

- [ ] Coverage gate sanity (rule-011: ≥85% domain+db):

```bash
uv run pytest --cov -q
```

Expected: coverage thresholds still met (this WP only adds covered lines).

- [ ] **Deploy note — do NOT execute without Julio's explicit go-ahead** (dry-run-default preference + execution rule 3). When Julio says go, the live DB gets the column via:

```bash
sqlite3 finances.db ".backup backups/finances-backup-$(date +%Y%m%d).db"
uv run python -m finances.db.migrate
```

Expected output of the second command: `applied: 008_add_transaction_notes.sql`. Then a manual smoke: open the viewer, edit any transaction, type a note, save, confirm the pencil snippet appears on the row and the note survives the next `finances ingest provincial` re-ingest of an already-ingested file.

- [ ] Report status to Julio; he marks WP3 complete (execution rule 3 — the agent never does).
