# ADR-010: Idempotent Re-Ingestion via Deterministic `source_ref`

**Date:** 2026-04-19
**Status:** Accepted

## 1. Context

The user explicitly requires that downloading data twice on the same day must not duplicate already-recorded rows. This applies to every source:

- **Binance API** — most endpoints return a stable `orderId`, `txId`, or `tranId`. Use it directly.
- **Provincial CSV** — has a `Referencia` column for many but not all rows; some rows are bare descriptions.
- **BCV / P2P rates** — keyed by `(date, base, quote, source)` already covered by table UNIQUE.
- **Cash CLI** — user-initiated, but a duplicate accidental insert is still an issue.
- **Future mobile API** — must support client-supplied idempotency keys.

The schema's `transactions(source, source_ref)` UNIQUE constraint enforces dedup, but only if `source_ref` is reliably populated.

## 2. Decision

Every ingester must populate `transactions.source_ref` with a **deterministic** value derived from the source row, such that re-running ingestion against the same input produces the same `source_ref`:

1. **Binance**: use the SDK-provided ID (`orderId`, `txId`, `tranId`, `payTradeNo`, `subscriptionId`, etc.). Choice of field is per endpoint and documented in `finances/ingest/binance.py`.
2. **Provincial**: use `Referencia` if present and non-empty; else compute `sha256(occurred_at || amount || description)` truncated to 16 hex chars, prefixed `hash:`.
3. **BCV / P2P rates**: handled by the `rates` table's own UNIQUE constraint; no `source_ref` needed.
4. **Cash CLI**: generate a UUIDv4 at insert time and surface it on stdout for user reference. (Cash entries are user-initiated, so true dedup-on-rerun isn't applicable; the UUID exists for audit/edit later.)
5. **Future mobile API**: require an `Idempotency-Key` header; use it as `source_ref`.

The repo helper `transactions.upsert_by_source_ref(source, source_ref, payload)` uses `INSERT … ON CONFLICT(source, source_ref) DO UPDATE SET updated_at = CURRENT_TIMESTAMP` so re-ingestion is a true no-op for unchanged rows.

## 3. Consequences (The "Why")

### Positive

- Re-running any ingester produces zero net inserts when nothing has changed.
- Mid-day re-fetches (the user's exact use case) are safe.
- The hash strategy for description-only Provincial rows is stable as long as `(date, amount, description)` is.
- Audit log (`import_runs.rows_inserted` vs. `rows_skipped`) tells the user what actually happened on each run.

### Negative

- If the user manually edits a Provincial CSV's description for a row that lacks `Referencia`, the hash changes and the row is re-inserted as a new transaction. Mitigation: never edit historical CSVs after ingest; corrections happen in SQLite.
- A genuine duplicate transaction with the same `(date, amount, description)` but lacking `Referencia` would be deduped incorrectly. Mitigation: such cases get caught by balance reconciliation and are handled by manual `adjustment` entries.

## 4. Rule Extraction (The "How" for Agents)

**Target File:** `docs/architecture/rules/rule-010-deterministic-source-ref.md`
**Injected Constraint:** Every ingester writing to `transactions` must populate `source_ref` with a deterministic, source-derived value (or a hash conforming to the per-source rules above). Inserting a `transactions` row with `source_ref IS NULL` is forbidden except for `cash_cli` (UUID) and explicit `manual` adjustments. Re-running any ingester twice on identical input must result in `import_runs.rows_inserted = 0` on the second run.
