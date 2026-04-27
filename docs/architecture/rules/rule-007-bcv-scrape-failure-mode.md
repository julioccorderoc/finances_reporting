# Rule 007 — BCV Scrape Failure Mode

**Source ADR:** [ADR-007](../../ADR/ADR-007-bcv-automated-scrape.md)
**Scope:** `finances/ingest/bcv.py`.

**Constraint:** On parse failure, the BCV ingester must:

1. Insert a row into `import_runs` with `status='error'` and a populated `error` column (HTML snippet, exception message, or both).
2. Exit the process with a non-zero status code.
3. Leave the existing `rates` rows untouched. **No fallback or estimated value may be written to `rates(source='bcv')` when the scrape fails.**

A retry policy of one attempt with 5-second backoff is permitted; beyond that, fail.

**Generalization (v1 closeout, 2026-04-27).** The same `start_run` / `finish_run` contract that this rule mandates for BCV is now mirrored in every other ingest entry point: `finances/ingest/binance.py::sync_binance`, `finances/ingest/provincial.py::ingest_csv`, `finances/ingest/p2p_rates.py::ingest_p2p_rates`, and the umbrella `finances/migration/backfill.py::run_backfill`. Each writes exactly one `import_runs` row per invocation — `status='success'` with row counts on the happy path, or `status='error'` with the exception summary and a re-raise on failure. The canonical implementation is still `finances/ingest/bcv.py:170-205`; mirror that shape exactly when adding a new ingester. The contract is pinned per-source by `tests/test_ingest_provenance.py` and end-to-end by `tests/integration/test_pipeline.py::test_full_pipeline_idempotent`. `import_state.finish_run` accepts only `status in ('running', 'success', 'error')` — never `'ok'`.
