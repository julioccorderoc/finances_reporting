# Rule 007 — BCV Scrape Failure Mode

**Source ADR:** [ADR-007](../../ADR/ADR-007-bcv-automated-scrape.md)
**Scope:** `finances/ingest/bcv.py`.

**Constraint:** On parse failure, the BCV ingester must:

1. Insert a row into `import_runs` with `status='error'` and a populated `error` column (HTML snippet, exception message, or both).
2. Exit the process with a non-zero status code.
3. Leave the existing `rates` rows untouched. **No fallback or estimated value may be written to `rates(source='bcv')` when the scrape fails.**

A retry policy of one attempt with 5-second backoff is permitted; beyond that, fail.
