# ADR-004: Backfill All Historical CSVs With Interactive NA Cleanup

**Date:** 2026-04-19
**Status:** Accepted

## 1. Context

The existing CSVs contain ~1,000+ historical rows (Binance + Provincial + BCV) covering several months of real activity. Three options:

1. Backfill everything into the new SQLite, with an interactive pass to resolve the 158 rows currently labeled "NA".
2. Cut over fresh, leaving the old Sheets as a historical archive.
3. Backfill from a recent date only (e.g. 2026-01-01).

Option 2 loses continuity for any year-over-year analysis the user might want later, and the historical data is the test set that proves the new system actually works. Option 3 is a partial commitment that still requires building the same backfill code — only the cutoff differs.

## 2. Decision

Backfill **all** historical CSVs into the new SQLite by routing them through the same ingest modules used in production (`finances/ingest/binance.py`, `finances/ingest/provincial.py`, `finances/ingest/bcv.py`). Run an interactive cleanup pass (`finances backfill cleanup`) that walks every `WHERE needs_review=1` row and prompts the user for category and (where relevant) a `user_rate`. Detect implicit transfers (Binance internal transfer ↔ paired P2P sell ↔ Provincial deposit) and pair them via `domain.transfers.create_transfer`.

## 3. Consequences (The "Why")

### Positive

- The backfill is the most thorough integration test possible — if it produces matching balances, the new system is trustworthy.
- All historical analysis remains possible from day one.
- Reusing production ingest paths means no second codebase to maintain.

### Negative

- One-time interactive cleanup of ~158 rows is slow (estimate: 30–90 minutes).
- Some implicit transfers may be ambiguous (timing mismatch between Binance settlement and bank credit); these will need manual pairing or a `needs_review` flag.
- Backfill blocks shipping until ingest, transfers, and rates engines are all green.

## 4. Rule Extraction (The "How" for Agents)

**Target File:** `docs/architecture/rules/rule-004-backfill-uses-production-ingest.md`
**Injected Constraint:** `finances/migration/backfill.py` must not contain transaction-shaping logic that diverges from `finances/ingest/*`. It is an orchestrator that feeds CSV rows into the same parsing/normalization functions used in production. Any duplicated parsing logic must be extracted into the relevant ingest module.
