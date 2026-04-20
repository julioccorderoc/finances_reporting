# Rule 010 — Deterministic `source_ref` for Idempotent Re-Ingestion

**Source ADR:** [ADR-010](../../ADR/ADR-010-idempotent-reingestion.md)
**Scope:** All inserts into `transactions`.

**Constraint:**

- `transactions.source_ref` must be deterministic and source-derived. For sources lacking a stable native ID, use `"hash:" + sha256(occurred_at || amount || description)[:16]`.
- `source_ref IS NULL` is permitted only for `source IN ('cash_cli', 'manual')`. Cash CLI generates a UUIDv4 at insert time.
- Inserts must use `transactions.upsert_by_source_ref` (`INSERT … ON CONFLICT(source, source_ref) DO UPDATE SET updated_at = CURRENT_TIMESTAMP`); raw `INSERT` statements bypassing this helper are forbidden.

**Invariant verified in CI:** Running any ingester twice on identical input produces `import_runs.rows_inserted = 0` on the second run.
