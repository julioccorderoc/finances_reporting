# Rule 004 — Backfill Reuses Production Ingest

**Source ADR:** [ADR-004](../../ADR/ADR-004-csv-backfill-strategy.md)
**Scope:** `finances/migration/backfill.py` and any future re-import script.

**Constraint:** Backfill is an orchestrator. It reads CSV rows and feeds them into the same parsing/normalization functions used by `finances/ingest/{binance,provincial,bcv,p2p_rates}.py`. Backfill must not duplicate transaction-shaping logic. If a parsing capability is needed by both backfill and live ingest, it lives in the ingest module and backfill calls it.
