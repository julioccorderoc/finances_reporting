# ADR-022: Deleting a Transaction Leaves a Tombstone the Ingest Honours

**Date:** 2026-09-03
**Status:** Accepted 2026-09-03 — the owner's answer was "Delete duplicated
stuff"; implemented the same day (migration 023, `transactions_repo.delete`,
the ingest skip, the Flow-modal control, the `doctor` check)
**Amends:** [ADR-010](./ADR-010-deterministic-source-ref.md) — a `(source, source_ref)` can be *retired*, not only deduplicated
**Related:** [ADR-002](./ADR-002-transfers-double-entry.md) (a pair sums to zero), [ADR-012](./ADR-012-local-web-viewer.md) (writes go through the repo APIs), [ADR-019](./ADR-019-bank-reversal-pairing.md) (a reversed charge is *paired*, not deleted)
**Rule:** [rule-010](../architecture/rules/rule-010-deterministic-source-ref.md)

## 1. Context

Owner request, 2026-09-03: *"I also need to be able to cancel a
transaction or to delete it."*

The ledger has no way to make a row go away. It has three ways to make a
row *not count*, and none of them is what a wrong row needs:

- a **transfer-kind category** (`External Transfer`, since migration 022)
  keeps the row out of spending and income — but it still moves the
  account balance, which is right for money that did move and wrong for
  a row that should never have existed;
- **pairing** (ADR-002, ADR-019) nets two real rows to zero — it needs
  two rows;
- **parking** hides a row from the triage queue and nothing else.

Two invariants make a plain `DELETE` wrong for anything the ingest wrote:

1. **Re-ingest same day = 0 new rows** (CLAUDE.md). Dedup is
   `UNIQUE(source, source_ref)`; delete the row and the next
   `finances update` or statement drop inserts it again, silently.
   Every Provincial and Binance row would come back.
2. **A pair sums to zero** (rule-002). Deleting one half of a transfer
   leaves an orphan that no report can explain.

The only foreign key into `transactions` is `transaction_edits`
(migration 009, `ON DELETE CASCADE`), so the edit history of a deleted
row goes with it.

## 2. Decision (proposed)

### 2.1 A delete is a real `DELETE` plus a tombstone

A new table records what the owner removed:

```sql
CREATE TABLE deleted_transactions (
    source      TEXT NOT NULL,
    source_ref  TEXT NOT NULL,
    deleted_at  TEXT NOT NULL,           -- UTC ISO-8601
    reason      TEXT,                    -- optional, the owner's words
    snapshot    TEXT NOT NULL,           -- the row as JSON, for the record
    PRIMARY KEY (source, source_ref)
);
```

`transactions_repo.delete(conn, txn_id, *, reason)` writes the tombstone
and deletes the row in one transaction. The repo returns the snapshot;
the viewer shows it in the toast ("Deleted DR OB … −Bs. 800.00").

### 2.2 The ingest honours the tombstone

`upsert_by_source_ref` (repos/transactions.py) skips any row whose
`(source, source_ref)` is in `deleted_transactions` and counts it under a
new `rows_skipped_deleted` in the ingest report. Backfill goes through the
same path (rule-004), so it is honoured there too. The invariant holds:
re-ingesting a statement after a delete still inserts 0 rows.

`source = 'cash_cli'` rows (`ingest.cash_cli.CASH_CLI_SOURCE` — the draft
said `'cash'`, which is not a source the ledger uses) are exempt from the
tombstone: nothing re-ingests them, and a tombstone would block
re-entering a legitimately identical cash row (same day, amount and words
hash the same).

The counter is added where the two live importers report — `binance`'s
stats dict and `provincial`'s `IngestReport`. Backfill honours the skip
through the same repo call without a counter of its own.

### 2.3 What may be deleted

- Any **unpaired** row. Paired rows (`transfer_id` set) are refused with
  *"This row is one half of a transfer — the pair has to be broken first"*.
  Breaking a pair is not designed here; today no surface does it (the
  triage "refuse" only dismisses a *proposal*).
- Rows written by the reconciliation engine (`source` in
  `reconciliation`, `opening_balance`) are refused: they are the ledger's
  own corrections, and removing one by hand re-opens what it closed.

### 2.4 The surface

One control, in the Flow modal footer, left of Cancel: a ghost button
**Delete**, then a confirm in plain words — *"Delete this row from the
ledger? It will not come back when the statement is imported again."*
Success closes the modal, removes the row from the list, and toasts.
`POST /_partial/transactions/{id}/delete`. No delete from the triage
dialog and no bulk delete in this ADR; both are a second decision.

"Cancel" as a verb is not used: in this viewer it already means *close
without saving*.

## 3. Alternatives considered

- **Plain `DELETE`.** Breaks invariant 1 for every ingested row. Rejected.
- **A `voided_at` column and "exclude voided" in every query.** Keeps the
  row for audit, but the exclusion has to reach every SELECT that feeds a
  report, a balance, the triage queue and the net-worth figure — the
  same blast radius the `_row_to_transaction` note in MEMORY.md warns
  about, with a silent failure mode (one query forgets, one figure is
  wrong). The tombstone puts the rule at the one place rows enter.
- **A transfer-kind `Cancelled` category.** Zero schema work, but the
  row keeps moving the account balance and keeps showing on Flow; it
  answers "this is not spending", not "this never happened".
- **Deleting both halves of a pair at once.** Tempting for a wrong P2P
  pairing, but the right fix there is to break the pair and keep the two
  real rows. Out of scope; noted as the next gap.

## 4. Consequences

- Migration `023_deleted_transactions.sql`, the repo function, the
  ingest skip and its report field, the endpoint, the modal button — each
  with tests first (rule-011), and an integration test that ingests a
  statement, deletes one row, re-ingests and sees 0 new rows.
- `finances doctor` gains a line: tombstones whose `(source, source_ref)`
  is back in `transactions` (would mean an ingest path bypassed the repo).
- The snapshot column means nothing is truly lost; a future "undo" is a
  matter of re-inserting it and dropping the tombstone.
