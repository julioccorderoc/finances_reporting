# ADR-001: SQLite as Source of Truth, Google Sheets as Read-Only Mirror

**Date:** 2026-04-19
**Status:** Accepted

## 1. Context

Today, financial data lives in four manually-edited Google Sheets exported as CSVs. There is no normalized store, no reliable join across Binance and the bank, no per-account balance, and no way to enforce data quality at write time. The user also plans a mobile app for receipt ingestion, which means a real backend ingestion path is needed regardless of the current Sheets workflow.

Three options were considered:

1. **SQLite locally + Sheets read-only mirror.**
2. **Google Sheets as source of truth** (write directly with `gspread`).
3. **Postgres / Supabase** with a Sheets mirror.

Option 2 keeps the current friction (cleaning data inside Sheets) and leaves no place to enforce constraints. Option 3 is overkill for a solo user with a single workstation; it adds an always-on dependency without proportional value at this stage.

## 2. Decision

Adopt a single normalized **SQLite** database (`finances.db`) at the project root as the **sole source of truth**. All ingestion (Binance API, Provincial CSV, BCV scrape, Binance P2P scrape, Cash CLI, future Telegram, future mobile API) writes into it. Google Sheets becomes a **read-only mirror**, fully overwritten from SQL views by `finances sync sheets`. No code path writes user data into Sheets except this mirror. No human edits Sheets to change ledger state.

## 3. Consequences (The "Why")

### Positive

- Constraints (UNIQUE on `source, source_ref`, CHECK on `kind`, FK on `category_id`) catch data errors at write time.
- Joins, balances, and consolidated USD views become trivial SQL.
- Idempotent ingestion is feasible because dedup is enforced at the DB level.
- The same backend serves the future mobile app with no additional rewrite.
- Local SQLite means no infrastructure cost, no auth complexity, instant queries.

### Negative

- The user can no longer "just edit a row" in Sheets — corrections go through the CLI (or a `finances tx edit` subcommand, future).
- A single-file DB on one machine has no built-in remote backup; explicit backup discipline is required (or pair with Litestream / nightly copy to Drive — out of scope for v1).
- Changes to schema require migrations rather than ad-hoc column edits.

## 4. Rule Extraction (The "How" for Agents)

**Target File:** `docs/architecture/rules/rule-001-no-direct-sheets-writes.md`
**Injected Constraint:** No code outside `finances/reports/sheets_sync.py` may import `gspread` or otherwise write to Google Sheets. Any module that does is a violation of ADR-001 and must be reverted or migrated.
