# Triage Speedrun, Plan 2 — Durable Park + Easy-First Buckets

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let the owner blast through the ~25 triage rows that need nothing but a
category, shove genuinely ambiguous rows into a durable Parked pile, and come
back to that pile later — instead of grinding chronologically through 243 items
of mixed difficulty.

**Architecture:** One schema change (`transactions.parked`) replaces an
in-memory, per-process skip set that is destroyed every time the owner presses
the always-visible **Stop server** button. `build_queue` then partitions on that
column and orders by difficulty bucket before date. No new services; the queue
builder and the transactions repo absorb the change.

**Tech Stack:** Python 3.13, SQLite (stdlib `sqlite3`, WAL), Pydantic v2,
FastAPI, Jinja2, HTMX, Alpine.js, pytest + polyfactory.

## Global Constraints

- **Spec:** `docs/superpowers/specs/2026-07-21-triage-speedrun-design.md` §5.3
  and §5.5. Plan 1 (shipped) covered §4 and §5.1.
- **Authority:** the ADR-012 Amendment dated 2026-07-21 already approves durable
  parking and difficulty-first ordering. No further ADR is needed for this plan.
- **Baseline:** `main` at `2d298e3`, **878 tests passing**. Work on a branch.
- **Test command:** `rtk proxy uv run pytest`. Plain `pytest` and
  `uv run rtk pytest` both FAIL here. **`rtk proxy` strips pytest's final
  "N passed" line** — verify counts with
  `rtk proxy uv run pytest -q 2>&1 | tr -cd '.' | wc -c` and
  `... | tr -cd 'FE' | wc -c`, or `--collect-only -q`. Never report a count read
  off a summary line; it is not printed.
- **Next free migration prefix is 015.** `014_drop_v_transactions_usd.sql`
  already exists. Run `ls finances/db/migrations/` and confirm before creating
  the file — this repo already carries a duplicate-`011` collision.
- **rule-009:** Pydantic at every boundary; repos accept and return models.
- **rule-012:** the web layer issues no SQL of its own — parking goes through
  `transactions_repo.update()`. There must be no manual `needs_review` toggle;
  `parked` is a separate flag and must never write `needs_review`.
- **rule-011:** test commits precede implementation commits.
- **Do not** remove parked rows from needs-review counts. Dashboard,
  `report.html`, Sheets sync and `finances report needs-review` keep counting
  them. Parking is a triage-queue grouping only (spec §2).

---

## File Structure

| File | Responsibility | Task |
|---|---|---|
| `finances/db/migrations/015_add_transaction_parked.sql` | Add the column | 1 |
| `finances/domain/models.py` | `Transaction.parked` | 1 |
| `tests/conftest.py` | `TransactionFactory.parked = False` | 1 |
| `finances/db/repos/transactions.py` | 3 SELECTs, 2 INSERTs, 2 mappers, `update()` sentinel | 1 |
| `finances/web/services/transactions_query.py` | `TXN_QUERY_BASE`, second column list, mapper | 1 |
| `finances/reports/consolidated_usd.py`, `finances/reports/monthly.py` | column lists | 1 |
| `finances/web/routers/partials.py` | park/unpark routes; delete skip route | 2 |
| `finances/web/routers/api.py`, `finances/web/routers/pages.py` | drop skip-store wiring | 2 |
| `finances/web/templates/partials/modal_transaction_triage.html`, `modal_pair_confirm.html` | Park button replaces Skip | 2 |
| `finances/web/services/triage.py` | drop skip store; parked partition; buckets; counts | 2,3,4 |
| `finances/web/templates/pages/triage.html`, `partials/triage_queue.html` | Parked group; header counts | 3,4 |

---

## Task 1: `parked` column, model, and fan-out

Data layer only. No behaviour change, no UI change. The column is added and
plumbed everywhere, defaulting to `0`, and nothing reads it yet.

**Files:** as listed for Task 1 above.

**Interfaces:**
- Produces: `Transaction.parked: bool = False`; `transactions_repo.update(...,
  parked=...)` accepting the module's existing `_UNSET` sentinel; a `parked`
  column present in every row a `_row_to_transaction` mapper receives.
- Tasks 2-4 consume all of the above.

- [ ] **Step 1: Confirm the migration prefix is actually free**

```bash
ls finances/db/migrations/
```

Expected: highest is `014_drop_v_transactions_usd.sql`, so yours is `015_`. If a
`015_*` already exists, STOP and report — do not pick 016 silently.

- [ ] **Step 2: Write the failing test**

Create `tests/test_migration_015_parked.py`:

```python
"""transactions.parked — durable triage deferral (spec §5.3).

Replaces a per-process in-memory skip set that was destroyed by the
always-visible Stop-server button, i.e. by the designed way to end a
session. The column must survive re-ingest, because the whole promise of
Park is that a deferral outlives the session that made it.
"""

from __future__ import annotations

import sqlite3
from datetime import UTC, datetime
from decimal import Decimal

from finances.db.repos import accounts as accounts_repo
from finances.db.repos import transactions as transactions_repo
from finances.domain.models import (
    Account,
    AccountKind,
    Transaction,
    TransactionKind,
)


def _txn(account_id: int, **over) -> Transaction:
    base = dict(
        account_id=account_id,
        occurred_at=datetime(2026, 5, 1, tzinfo=UTC),
        kind=TransactionKind.EXPENSE,
        amount=Decimal("-100.00"),
        currency="VES",
        description="COM.PAGO bodega",
        source="provincial",
        source_ref="park-1",
    )
    base.update(over)
    return Transaction(**base)


def test_column_exists_and_defaults_to_zero(seeded_db: sqlite3.Connection) -> None:
    cols = {
        r["name"]: r
        for r in seeded_db.execute("PRAGMA table_info(transactions)").fetchall()
    }
    assert "parked" in cols
    assert cols["parked"]["notnull"] == 1
    assert str(cols["parked"]["dflt_value"]) == "0"


def test_model_defaults_to_not_parked(seeded_db: sqlite3.Connection) -> None:
    account = accounts_repo.insert(
        seeded_db, Account(name="P", kind=AccountKind.BANK, currency="VES")
    )
    stored = transactions_repo.insert(seeded_db, _txn(account.id))

    assert stored.parked is False
    assert transactions_repo.get_by_id(seeded_db, stored.id).parked is False


def test_update_can_park_and_unpark(seeded_db: sqlite3.Connection) -> None:
    account = accounts_repo.insert(
        seeded_db, Account(name="P", kind=AccountKind.BANK, currency="VES")
    )
    stored = transactions_repo.insert(seeded_db, _txn(account.id))

    transactions_repo.update(seeded_db, stored.id, parked=True)
    assert transactions_repo.get_by_id(seeded_db, stored.id).parked is True

    transactions_repo.update(seeded_db, stored.id, parked=False)
    assert transactions_repo.get_by_id(seeded_db, stored.id).parked is False


def test_parking_does_not_touch_needs_review(
    seeded_db: sqlite3.Connection,
) -> None:
    """rule-012: parked is a separate flag, never a needs_review proxy."""
    account = accounts_repo.insert(
        seeded_db, Account(name="P", kind=AccountKind.BANK, currency="VES")
    )
    stored = transactions_repo.insert(
        seeded_db, _txn(account.id, needs_review=True)
    )

    transactions_repo.update(seeded_db, stored.id, parked=True)

    assert transactions_repo.get_by_id(seeded_db, stored.id).needs_review is True


def test_parked_survives_reingest(seeded_db: sqlite3.Connection) -> None:
    """The core promise: re-running ingest must not un-park a row.

    `parked` is deliberately absent from upsert_by_source_ref's
    ON CONFLICT DO UPDATE SET list, so the column is simply left alone.
    """
    account = accounts_repo.insert(
        seeded_db, Account(name="P", kind=AccountKind.BANK, currency="VES")
    )
    first = transactions_repo.upsert_by_source_ref(seeded_db, _txn(account.id))
    transactions_repo.update(seeded_db, first["id"], parked=True)

    # Same source_ref arriving again from a statement re-drop.
    again = transactions_repo.upsert_by_source_ref(
        seeded_db, _txn(account.id, description="COM.PAGO bodega")
    )

    assert again["rows_updated"] == 1
    assert transactions_repo.get_by_id(seeded_db, first["id"]).parked is True


def test_factory_never_parks_randomly() -> None:
    """polyfactory does not pin bools; an unpinned parked would flake tests."""
    from tests.conftest import TransactionFactory

    assert all(TransactionFactory.build().parked is False for _ in range(25))
```

- [ ] **Step 3: Run it to confirm it fails**

```bash
rtk proxy uv run pytest tests/test_migration_015_parked.py -v
```

Expected: FAIL. `test_column_exists_and_defaults_to_zero` fails on
`assert "parked" in cols`; the model tests fail on `AttributeError` /
`ValidationError` for the unknown field.

- [ ] **Step 4: Commit the RED test**

```bash
git add tests/test_migration_015_parked.py
git commit -m "test(db): RED for transactions.parked durable triage deferral"
```

- [ ] **Step 5: Write the migration**

Create `finances/db/migrations/015_add_transaction_parked.sql`:

```sql
-- 015_add_transaction_parked.sql
--
-- Adds transactions.parked — durable triage deferral (ADR-012 Amendment
-- 2026-07-21, spec 2026-07-21-triage-speedrun-design §5.3).
--
-- Replaces the per-process in-memory skip set, which was destroyed on every
-- server stop — including the always-visible Stop-server button that is the
-- designed way to end a session.
--
-- Follows 008_add_transaction_notes.sql: a single ALTER TABLE. The runner
-- applies each file exactly once (keyed on full filename in _migrations), so
-- a bare ADD COLUMN is safe. NOT NULL is permitted here because a non-null
-- DEFAULT is supplied; existing rows backfill to 0.
--
-- Deliberately NOT added to upsert_by_source_ref's ON CONFLICT DO UPDATE SET
-- list: a column absent from that list is left untouched on re-ingest, which
-- is exactly the survival guarantee Park promises.

ALTER TABLE transactions ADD COLUMN parked INTEGER NOT NULL DEFAULT 0
  CHECK (parked IN (0, 1));
```

No index. At ~1,850 rows it buys nothing, and adding one perturbs the query plan
that Plan 1's ordering fix exists to stabilise.

- [ ] **Step 6: Add the model field and pin the factory — same commit**

In `finances/domain/models.py`, in `class Transaction`, after `needs_review`:

```python
    needs_review: bool = False
    parked: bool = False
    notes: str | None = None
```

In `tests/conftest.py`, in `TransactionFactory`, next to `needs_review = False`:

```python
    needs_review = False
    parked = False
```

**This pinning is not optional.** `__allow_none_optionals__ = 0.0` does not pin
booleans, so polyfactory would generate `parked` randomly and silently park
about half of all factory-built rows — surfacing later as intermittent queue
failures that look like ordering bugs.

- [ ] **Step 7: Update every column list**

Nine sites. None uses `SELECT *`, so a miss is a runtime `sqlite3.Row`
`IndexError` on one code path only. Add `parked` immediately after
`needs_review` everywhere, to match the model's field order.

Both mappers — `finances/db/repos/transactions.py:53` and
`finances/web/services/transactions_query.py:218` — add:

```python
        parked=bool(row["parked"]),
```

SELECT column lists (add `parked` after `needs_review`):
- `finances/db/repos/transactions.py:111` (`get_by_id`)
- `finances/db/repos/transactions.py:126` (`get_by_source_ref`)
- `finances/db/repos/transactions.py:218` (list by account)
- `finances/web/services/transactions_query.py:59` (`TXN_QUERY_BASE` — as
  `t.parked`; this one constant feeds `triage.py` ×3 and `pairing.py` ×2)
- `finances/web/services/transactions_query.py:344` (second hand-written list,
  also `t.parked`)
- `finances/reports/consolidated_usd.py:110`
- `finances/reports/monthly.py:~231`

INSERT column lists — add `parked` and one more `?` to each:
- `finances/db/repos/transactions.py:83` (`insert`)
- `finances/db/repos/transactions.py:169` (`upsert_by_source_ref`)

Bind it as `1 if txn.parked else 0`, matching how `needs_review` is bound.

**Do NOT add `parked` to the `ON CONFLICT DO UPDATE SET` block.** Its absence is
the mechanism that makes parking survive re-ingest, and
`test_parked_survives_reingest` pins that.

- [ ] **Step 8: Add the `update()` sentinel**

In `finances/db/repos/transactions.py`, `update()` already takes `_UNSET`
sentinels for its patchable fields. Add `parked` the same way: a keyword-only
parameter defaulting to `_UNSET`, appended to the SET clause and params only
when it is not `_UNSET`, bound as `1 if parked else 0`.

Do **not** add `parked` to the edit-history recording in that function. Migration
009's `transaction_edits.field` has `CHECK (field IN ('category_id','user_rate',
'notes'))`, so writing a `parked` edit row would raise. Parking is a workflow
flag, not a ledger correction — it does not belong in the audit trail.

- [ ] **Step 9: Apply and verify**

```bash
rtk proxy uv run pytest tests/test_migration_015_parked.py -v
rtk proxy uv run pytest -q 2>&1 | tr -cd '.' | wc -c
rtk proxy uv run pytest -q 2>&1 | tr -cd 'FE' | wc -c
```

Expected: 6 passed; then 884 dots and 0 F/E (878 + 6).

If any test fails with `IndexError: No item with that key`, you missed a SELECT
in Step 7 — the traceback names the mapper, and the failing call site names the
query.

- [ ] **Step 10: Commit**

```bash
git add finances/db/migrations/015_add_transaction_parked.sql \
        finances/domain/models.py tests/conftest.py \
        finances/db/repos/transactions.py \
        finances/web/services/transactions_query.py \
        finances/reports/consolidated_usd.py finances/reports/monthly.py
git commit -m "feat(db): add transactions.parked for durable triage deferral

Migration 015 plus the full fan-out: both _row_to_transaction mappers, seven
SELECT column lists, two INSERT lists, and an update() sentinel. Omitted from
ON CONFLICT DO UPDATE SET so parking survives re-ingest. TransactionFactory
pins parked=False in the same commit, since polyfactory does not pin bools and
would otherwise park half of all factory rows at random."
```

---

## Task 2: Park replaces Skip

Delete the in-memory skip store outright and route deferral through the column.

**Files:** `finances/web/services/triage.py`, `finances/web/routers/partials.py`,
`finances/web/routers/api.py`, `finances/web/routers/pages.py`,
`finances/web/templates/partials/modal_transaction_triage.html`,
`finances/web/templates/partials/modal_pair_confirm.html`,
`tests/web/test_triage_park.py` (new).

**Interfaces:**
- Consumes: `transactions_repo.update(conn, id, parked=...)` from Task 1.
- Produces: `POST /_partial/triage/{txn_id}/park` and
  `POST /_partial/triage/{txn_id}/unpark`. `build_queue` loses its
  `skipped_ids` parameter; `get_skip_store` and `app.state.skipped_triage_ids`
  cease to exist.

- [ ] **Step 1: Write the failing test**

Create `tests/web/test_triage_park.py`:

```python
"""Park replaces the session-local Skip (spec §5.3).

The old Skip stored ids in a per-process set on app.state, which the
Stop-server button destroyed. Park writes a column, so a deferral outlives
the session — and outlives a server restart, which these tests simulate by
building a second app against the same database file.
"""

from __future__ import annotations

import sqlite3
from datetime import UTC, datetime
from decimal import Decimal

import pytest
from fastapi.testclient import TestClient

from finances.db.repos import accounts as accounts_repo
from finances.db.repos import transactions as transactions_repo
from finances.domain.models import (
    Account,
    AccountKind,
    Transaction,
    TransactionKind,
)


@pytest.fixture
def park_db(web_db: sqlite3.Connection) -> sqlite3.Connection:
    account = accounts_repo.insert(
        web_db, Account(name="Provincial", kind=AccountKind.BANK, currency="VES")
    )
    for n in range(3):
        transactions_repo.insert(
            web_db,
            Transaction(
                account_id=account.id,
                occurred_at=datetime(2026, 5, 1 + n, tzinfo=UTC),
                kind=TransactionKind.EXPENSE,
                amount=Decimal("-100.00"),
                currency="VES",
                description=f"row {n}",
                source="provincial",
                source_ref=f"park-{n}",
                needs_review=True,
            ),
        )
    return web_db


def test_park_removes_item_from_the_main_queue(
    park_db: sqlite3.Connection, web_client_factory
) -> None:
    client: TestClient = web_client_factory()
    assert "txn:1" in client.get("/api/triage").text

    client.post("/_partial/triage/1/park")

    assert transactions_repo.get_by_id(park_db, 1).parked is True
    assert "txn:1" not in client.get("/api/triage").text


def test_park_survives_a_server_restart(
    park_db: sqlite3.Connection, web_client_factory
) -> None:
    """The exact failure the in-memory skip store had."""
    first: TestClient = web_client_factory()
    first.post("/_partial/triage/1/park")

    # A brand-new app against the same DB file == a restarted server.
    second: TestClient = web_client_factory()

    assert "txn:1" not in second.get("/api/triage").text


def test_unpark_returns_the_item_to_the_queue(
    park_db: sqlite3.Connection, web_client_factory
) -> None:
    client: TestClient = web_client_factory()
    client.post("/_partial/triage/1/park")

    client.post("/_partial/triage/1/unpark")

    assert transactions_repo.get_by_id(park_db, 1).parked is False
    assert "txn:1" in client.get("/api/triage").text


def test_park_does_not_alter_needs_review(
    park_db: sqlite3.Connection, web_client_factory
) -> None:
    client: TestClient = web_client_factory()
    client.post("/_partial/triage/1/park")

    assert transactions_repo.get_by_id(park_db, 1).needs_review is True


def test_park_on_unknown_id_is_404(
    park_db: sqlite3.Connection, web_client_factory
) -> None:
    client: TestClient = web_client_factory()

    assert client.post("/_partial/triage/9999/park").status_code == 404


def test_skip_endpoint_and_store_are_gone(
    park_db: sqlite3.Connection, web_client_factory
) -> None:
    """The old surface must be removed, not left as a second way to defer."""
    import finances.web.services.triage as triage_service

    assert not hasattr(triage_service, "get_skip_store")

    client: TestClient = web_client_factory()
    assert client.post("/_partial/triage/skip/txn:1").status_code == 404


def test_modal_offers_park_not_skip(
    park_db: sqlite3.Connection, web_client_factory
) -> None:
    client: TestClient = web_client_factory()
    html = client.get("/_partial/triage/1/modal").text

    assert "data-park-btn" in html
    assert "data-skip-btn" not in html
```

- [ ] **Step 2: Run it to confirm it fails, then commit RED**

```bash
rtk proxy uv run pytest tests/web/test_triage_park.py -v
git add tests/web/test_triage_park.py
git commit -m "test(triage): RED for durable Park replacing session-local Skip"
```

Expected: most fail on 404 for the park route;
`test_skip_endpoint_and_store_are_gone` fails on `hasattr`.

- [ ] **Step 3: Strip the skip store from the service**

In `finances/web/services/triage.py`:
- delete `get_skip_store` and its `__all__` entry,
- delete the `skipped_ids` parameter from `build_queue` and the partition block
  that uses it (`kept` / `sunk`),
- delete the module-docstring paragraph describing the session-local skip,
- update `build_queue`'s "Order of operations" docstring accordingly.

- [ ] **Step 4: Add the routes**

In `finances/web/routers/partials.py`, replace the
`POST /triage/skip/{item_id:path}` route with two routes following the shape of
the existing `triage_edit_partial` (same `Depends(get_conn)`, same
`request.app.state.templates`, same `HX-Trigger` convention):

```python
@router.post("/triage/{txn_id}/park", include_in_schema=False)
def triage_park_partial(
    request: Request,
    txn_id: int,
    conn: sqlite3.Connection = Depends(get_conn),
):
    """Defer a transaction durably (spec §5.3). Never touches needs_review."""
    return _set_parked(request, conn, txn_id, parked=True)


@router.post("/triage/{txn_id}/unpark", include_in_schema=False)
def triage_unpark_partial(
    request: Request,
    txn_id: int,
    conn: sqlite3.Connection = Depends(get_conn),
):
    return _set_parked(request, conn, txn_id, parked=False)
```

Write `_set_parked` as a module-level helper directly above them: look the
transaction up with `transactions_repo.get_by_id`, raise
`HTTPException(status_code=404, detail=f"transaction id={txn_id} not found")`
when it is `None`, call `transactions_repo.update(conn, txn_id, parked=parked)`,
then return the re-rendered queue partial via the existing
`_render_queue_partial` helper with `HX-Trigger: closeModal`.

Per rule-012 the route issues no SQL of its own — the repo does the write.

Remove the now-dead `get_skip_store` imports and the `skipped_ids=` arguments at
`partials.py:41,461,465,483,487`, `api.py:63,300,304`, `pages.py:54,210,214`.

- [ ] **Step 5: Swap the buttons**

In `modal_transaction_triage.html`, replace the `Skip → bottom` button with:

```jinja
        <button
          type="button"
          data-park-btn
          class="px-3 py-1.5 text-sm border border-amber-300 rounded bg-amber-50 text-amber-800 hover:bg-amber-100"
          hx-post="/_partial/triage/{{ txn.id }}/park"
          hx-target="#triage-queue"
          hx-swap="innerHTML"
        >Park for later</button>
```

Keep the existing `s`/`S` keyboard binding working by changing the selector in
that template's `@keydown.window` handler from `[data-skip-btn]` to
`[data-park-btn]`. Do the equivalent swap in `modal_pair_confirm.html`.

**Note:** `modal_pair_confirm.html` posts a `pair:{id}:{id}` item id, not a
transaction id. Pair proposals are derived live from a matching pass and have no
row to park. Remove the defer button from the pair modal entirely rather than
inventing an id scheme for it, and say so in your report.

- [ ] **Step 6: Verify and commit**

```bash
rtk proxy uv run pytest tests/web/test_triage_park.py tests/web/test_triage.py -v
rtk proxy uv run pytest -q 2>&1 | tr -cd '.' | wc -c
rtk proxy uv run pytest -q 2>&1 | tr -cd 'FE' | wc -c
```

Existing tests in `test_triage.py` assert the skip behaviour and **will fail** —
that surface is deliberately gone. Delete those specific tests and say which in
your report. Do not keep the skip store alive to satisfy them.

```bash
git add -u && git commit -m "feat(triage): durable Park replaces session-local Skip

The skip store lived in app.state and was destroyed by the Stop-server
button, i.e. by the designed way to end a session. Park writes
transactions.parked through the repo instead. Removes get_skip_store,
build_queue's skipped_ids parameter, and the skip route and its wiring in
partials/api/pages. The pair modal loses its defer button: pair proposals are
derived live from a matching pass and have no row to park."
```

---

## Task 3: The Parked group

**Files:** `finances/web/services/triage.py`,
`finances/web/templates/partials/triage_queue.html`,
`finances/web/templates/pages/triage.html`, `tests/web/test_triage_park.py`
(extend).

**Interfaces:**
- Produces: `TriageQueue.parked_items: list[TriageItem]` and
  `TriageQueue.parked_count: int`. `TriageQueue.items` excludes parked rows.
  Task 4 renders `parked_count` in the header.

- [ ] **Step 1: Write the failing test** — append to `tests/web/test_triage_park.py`:

```python
def test_parked_items_are_collected_separately(
    park_db: sqlite3.Connection, web_client_factory
) -> None:
    from finances.web.services.triage import build_queue

    client: TestClient = web_client_factory()
    client.post("/_partial/triage/1/park")

    queue = build_queue(park_db)

    assert [i.item_id for i in queue.items] == ["txn:2", "txn:3"]
    assert [i.item_id for i in queue.parked_items] == ["txn:1"]
    assert queue.parked_count == 1


def test_parked_group_renders_with_an_unpark_action(
    park_db: sqlite3.Connection, web_client_factory
) -> None:
    client: TestClient = web_client_factory()
    client.post("/_partial/triage/1/park")

    html = client.get("/triage").text

    assert "data-parked-group" in html
    assert "/_partial/triage/1/unpark" in html


def test_no_parked_group_when_nothing_is_parked(
    park_db: sqlite3.Connection, web_client_factory
) -> None:
    client: TestClient = web_client_factory()

    assert "data-parked-group" not in client.get("/triage").text
```

Run it, confirm it fails, commit RED.

- [ ] **Step 2: Implement**

Add `WHERE t.parked = 0` to both collection queries in `_collect_txn_items`,
keeping the `ORDER BY t.occurred_at, t.id` that Plan 1 added. Add a
`_collect_parked_items` that runs the same two predicates with `t.parked = 1`,
projected through the same `_project_from_row`, so a parked row still shows its
live issue badges. Add `parked_items` and `parked_count` to `TriageQueue`.

Pair items are never parked — `_collect_pair_items` is untouched.

Render the group in `triage_queue.html`, above the main list, wrapped in
`{% if queue.parked_items %}`, inside a `<details data-parked-group>` collapsed
by default with summary `Parked · {{ queue.parked_count }}`. Each row reuses
`triage_card_txn.html` plus an unpark button posting to
`/_partial/triage/{{ card.id }}/unpark` with `hx-target="#triage-queue"`.

Put it **inside** `#triage-queue` so it re-renders on every queue swap.

- [ ] **Step 3: Verify and commit** (expected: 887 dots, 0 F/E)

---

## Task 4: Easy-first buckets and the header

**Files:** `finances/web/services/triage.py`,
`finances/web/templates/partials/triage_queue.html`,
`finances/web/routers/partials.py` (the `_render_queue_partial` fix),
`tests/web/test_triage_buckets.py` (new).

**Interfaces:**
- Consumes: Task 3's `parked_count`.
- Produces: `TriageItem.bucket: int` and `TriageQueue.bucket_counts:
  dict[int, int]`; ordering `(bucket, occurred_at, item_id)`.

- [ ] **Step 1: Write the failing test**

Create `tests/web/test_triage_buckets.py`:

```python
"""Difficulty-first queue ordering (spec §5.5, ADR-012 Amendment).

The owner's ask: "first run MOST of the ones I have, and tackle the
ambiguous ones at the end." Chronological order interleaves one-click rows
with rows requiring recall of an eight-month-old exchange rate. Buckets put
the cheap work first without giving up oldest-first inside each bucket.
"""

from __future__ import annotations

import sqlite3
from datetime import UTC, datetime
from decimal import Decimal

import pytest
from fastapi.testclient import TestClient

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


@pytest.fixture
def bucket_db(web_db: sqlite3.Connection) -> sqlite3.Connection:
    """A rate-issue row dated EARLIER than a category-only row.

    Under pure oldest-first this returns [rate, category]. Under
    difficulty-first it must return [category, rate]. That inversion is the
    whole point of the change.
    """
    account = accounts_repo.insert(
        web_db, Account(name="Provincial", kind=AccountKind.BANK, currency="VES")
    )
    groceries = categories_repo.get_by_name(
        web_db, TransactionKind.EXPENSE, "Groceries"
    )
    assert groceries is not None

    def _txn(day: int, ref: str, *, category_id, needs_review: bool) -> None:
        transactions_repo.insert(
            web_db,
            Transaction(
                account_id=account.id,
                occurred_at=datetime(2026, 5, day, tzinfo=UTC),
                kind=TransactionKind.EXPENSE,
                amount=Decimal("-100.00"),
                currency="VES",
                description=ref,
                category_id=category_id,
                source="provincial",
                source_ref=ref,
                needs_review=needs_review,
            ),
        )

    # id 1 — OLDER, but a rate issue -> bucket 1
    _txn(1, "old-rate", category_id=groceries.id, needs_review=True)
    # id 2 — NEWER, category only -> bucket 0
    _txn(9, "new-category", category_id=None, needs_review=False)
    # id 3 — both issues -> bucket 1, because a missing rate dominates
    _txn(5, "both", category_id=None, needs_review=True)
    return web_db


def test_difficulty_beats_chronology(bucket_db: sqlite3.Connection) -> None:
    queue = build_queue(bucket_db)

    # txn:2 is the NEWEST row but the only bucket-0 one, so it leads.
    assert [i.item_id for i in queue.items] == ["txn:2", "txn:1", "txn:3"]


def test_bucket_assignment(bucket_db: sqlite3.Connection) -> None:
    by_id = {i.item_id: i for i in build_queue(bucket_db).items}

    assert by_id["txn:2"].bucket == 0          # category only
    assert by_id["txn:1"].bucket == 1          # rate issue
    assert by_id["txn:3"].bucket == 1          # both -> rate dominates


def test_oldest_first_survives_inside_a_bucket(
    bucket_db: sqlite3.Connection,
) -> None:
    """Plan 1's guarantee must not be lost to the new leading sort key."""
    bucket_1 = [i for i in build_queue(bucket_db).items if i.bucket == 1]

    assert [i.item_id for i in bucket_1] == ["txn:1", "txn:3"]
    assert bucket_1[0].sort_key < bucket_1[1].sort_key


def test_tied_timestamps_still_break_on_item_id(
    web_db: sqlite3.Connection,
) -> None:
    """204 of 243 live items share a timestamp — the tiebreak still matters."""
    account = accounts_repo.insert(
        web_db, Account(name="P", kind=AccountKind.BANK, currency="VES")
    )
    for n in range(3):
        transactions_repo.insert(
            web_db,
            Transaction(
                account_id=account.id,
                occurred_at=datetime(2026, 5, 1, tzinfo=UTC),
                kind=TransactionKind.EXPENSE,
                amount=Decimal("-1.00"),
                currency="VES",
                description=f"tied {n}",
                source="provincial",
                source_ref=f"tied-{n}",
                needs_review=True,
            ),
        )

    assert [i.item_id for i in build_queue(web_db).items] == [
        "txn:1",
        "txn:2",
        "txn:3",
    ]


def test_bucket_counts_sum_to_the_item_count(
    bucket_db: sqlite3.Connection,
) -> None:
    queue = build_queue(bucket_db)

    assert sum(queue.bucket_counts.values()) == len(queue.items)
    assert queue.bucket_counts[0] == 1
    assert queue.bucket_counts[1] == 2


def test_header_renders_counts_inside_the_swapped_region(
    bucket_db: sqlite3.Connection, web_client_factory
) -> None:
    """Counts outside #triage-queue would go stale on every save or park."""
    client: TestClient = web_client_factory()
    html = client.get("/triage").text

    queue_region = html.split('id="triage-queue"', 1)[1]
    assert "Ready to categorize" in queue_region
    assert "Missing a rate" in queue_region
    assert "Parked" in queue_region
```

Run it, confirm `test_difficulty_beats_chronology` fails with
`['txn:1', 'txn:3', 'txn:2'] != ['txn:2', 'txn:1', 'txn:3']` — that is the
inversion proving the change is real. If it passes already, STOP and report.
Then commit RED.

- [ ] **Step 2: Implement**

Add `bucket: int` to `TriageItem`. In `_collect_txn_items`, set it from the
merged badge set: `1 if "rate" in badges else 0`. Set `2` in
`_collect_pair_items`. Change the sort in `build_queue` to:

```python
    # Difficulty first, then chronology, then a mandatory id tiebreak.
    # ADR-012 Amendment 2026-07-21. The item_id component is load-bearing:
    # 204 of 243 live items share a timestamp (Provincial CSV has no time
    # component), so without it the order inside a bucket is undefined.
    all_items.sort(key=lambda it: (it.bucket, it.sort_key, it.item_id))
```

Build `bucket_counts` on the unfiltered set, exactly as `counts` already is, so
the header stays truthful when a type-filter chip is active.

Render the header inside `triage_queue.html`, above the parked group:

```
Ready to categorize · N     Missing a rate · N     Pairs · N     Parked · N
```

- [ ] **Step 3: Fix `_render_queue_partial`**

It hardcodes `type_filter=None`, so every save/park/confirm swaps an *unfiltered*
queue in while the chip still renders active and `hx-push-url` has already
written `?type_filter=…` into the URL. With counts now living inside the swapped
region this becomes visible as numbers that contradict the visible list. Thread
the caller's `type_filter` through instead of hardcoding it, and add a test that
a save while a chip is active returns a filtered queue.

- [ ] **Step 4: Verify and commit**

Full suite green. Then look at it, because none of the above proves it reads
well:

```bash
rtk proxy uv run finances serve --port 8010
```

Open `/triage`: the easy rows should be at the top, the header counts should
match what you see, Park should move a row into the collapsed group, and the
group should still be there after you stop and restart the server. That restart
check is the whole point of this plan — do it.

---

## What this plan leaves for Plan 3

Prev/next arrows, the `N of M` counter, in-place save, the `advanceQueue`
redesign (its current DOM heuristic re-opens the just-saved item in a loop once
items stay in place), and the dirty guard. Spec §5.2 and §5.4.
