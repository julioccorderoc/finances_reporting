# Product Requirements Document — Finances Reporting

**Version:** 1.0
**Last Updated:** 2026-04-19
**Owner:** Julio Cordero
**Status:** Approved (drives Roadmap v1)

---

## 1. Problem Statement

The current setup is three isolated Python scripts (`download_binance.py`, `extract_bcv.py`, `extract_provincial.py`) feeding four manually-edited Google Sheets exported as CSVs (`Binance.csv`, `Provincial.csv`, `BCV.csv`, `Categories.csv`). It "works" but has structural problems that make day-to-day tracking painful and analysis untrustworthy:

- **No source-of-truth ledger.** Each source lives in a silo. There is no way to reconcile across them, no way to compute account balances, no way to query "where did this money go" across accounts.
- **~19% of bank rows are unclassified** (158 "NA" categories in `Provincial.csv`).
- **Transfers are mishandled.** P2P sells in Binance never appear as deposits anywhere; cash withdrawals vanish; some income gets double-counted because it lives in both Binance and Provincial.
- **Two rate columns per row** (`Tasa del día` BCV + `Tasa USDT`) with no clear provenance for the USDT rate; Binance has no rates at all.
- **Dates and rates embedded mid-row** in Provincial — fragile, ugly, hostile to analysis.
- **No state tracking** — Binance script re-fetches 21 days every run and would create duplicates if write was automated.
- **No cash tracking, no Earn position tracking, no Sheets writes, no incremental sync.**
- **BCV scraper reads a hand-saved HTML file** — a recurring manual chore.

## 2. Goals & Non-Goals

### Goals (v1)

1. Hassle-free expense recording — minimize manual work and copy/paste.
2. A single consolidated USD view across every account.
3. Per-account running balances that match what the bank/exchange UI shows.
4. Correct transfer handling — money moving between own accounts must not appear as income or expense.
5. Investment (Binance Earn) tracking with current principal per product.
6. A data path that is mobile-app-ready (so a future receipt-parsing app can write into the same ledger).
7. Idempotent, incremental ingestion — re-running a sync never creates duplicates.

### Non-Goals (v1)

- Budgeting, forecasting, savings goals.
- Tax reporting / capital-gains computation.
- Multi-user or shared-household accounting.
- Tracking spot crypto P&L beyond USDT/USDC/VES/USD/EUR balances.
- Web UI (CLI + Sheets mirror is enough for v1).

## 3. Users & Primary Flows

**Primary user:** solo (Julio).

**Primary cadence is weekly.** The user reports tracked one ingestion gap of ~21 days in the current (manual) setup; the Binance ingester's default lookback window is 35 days (5 weeks) to absorb missed cycles without manual intervention. Re-running ingestion on the same day (or any earlier already-ingested day) is a no-op by design (per ADR-010).

| Cadence | Flow |
| --- | --- |
| Passive (background, optional) | Daily Binance pull, daily BCV scrape, daily Binance P2P median rate fetch — schedulable but not required. |
| Weekly (primary) | `finances ingest all` pulls everything since the last run; `finances report needs-review` triages uncategorized rows; `finances report balances` confirms balances match the bank and Binance UIs; `finances report consolidated` emits the USDT-based headline USD summary. |
| Ad-hoc (when applicable) | `finances cash add` to log a USD-cash expense (low frequency — bolívar cash flows through the bank). |
| Monthly | `finances sync sheets` pushes all four tabs (`Transactions`, `Balances`, `Monthly`, `Needs Review`) to the shared spreadsheet. |
| Future | Mobile app captures receipts and POSTs structured entries to an ingest endpoint. |

## 4. Architectural Decisions (Reference)

All major choices are captured as ADRs. Implementation agents must read these before executing any epic:

- ADR-001 — SQLite source of truth, Sheets read-only mirror
- ADR-002 — Double-entry transfers (+ Provincial-anchored P2P pairing amendment)
- ADR-003 — Binance Earn as own account + `earn_positions` table
- ADR-004 — Backfill all historical CSVs with interactive NA cleanup
- ADR-005 — Rate resolution priority (+ USDT-for-headline, BCV-reference-only amendment)
- ADR-006 — Category taxonomy revamp + auto-suggest rules
- ADR-007 — BCV automated scrape
- ADR-008 — Cash scope (USD only, CLI)
- ADR-009 — Pydantic for normalization at all trust boundaries
- ADR-010 — Idempotent re-ingestion via deterministic `source_ref`

## 5. Data Model Summary

A single normalized SQLite database (`finances.db`) is the source of truth. The Google Sheets workbook becomes a read-only mirror generated from SQL views.

Core tables:

- `accounts` — Binance Spot, Binance Funding, Binance Earn, Provincial bank, Cash USD (extensible).
- `categories` — clean taxonomy (`income | expense | transfer | adjustment` × subcategory) with an `active` flag.
- `category_rules` — regex patterns matched against transaction descriptions for auto-suggestion.
- `transactions` — every movement, signed, with `kind`, `category_id`, `transfer_id` (for double-entry pairing), `user_rate` override, `source` + `source_ref` (dedup guard), and a `needs_review` flag.
- `rates` — daily exchange rates keyed on `(as_of_date, base, quote, source)`; sources include `bcv`, `binance_p2p_median`, `manual`.
- `earn_positions` — current and historical Binance Earn principal per product, with APY snapshot.
- `import_state`, `import_runs` — per-source incremental cursor + audit trail.

Transfers between own accounts use **double-entry**: one transfer creates two transactions sharing a `transfer_id`, summing to zero across accounts. They never appear in income/expense aggregations.

Rate resolution (per ADR-005 priority): per-row `user_rate` override → Binance P2P daily median → BCV fallback → mark `needs_review=1`. **USDT-derived values are the headline number in every consolidated USD report;** BCV is tracked for reference and legal/government comparisons but never drives the headline. See ADR-005 amendment 2026-04-19.

**Normalization (per ADR-009):** Every external input (Binance JSON, Provincial CSV, BCV HTML, CLI prompt) is parsed into a Pydantic v2 `BaseModel` at the trust boundary; domain models and repository inputs/outputs are all Pydantic.

**Idempotent ingestion (per ADR-010):** Re-running any ingester on the same day (or any already-covered window) inserts zero new rows. Every transaction carries a deterministic `source_ref` (stable exchange/bank ID when available, otherwise a content hash) enforced by the `UNIQUE(source, source_ref)` constraint.

**Bank-anchored transfer pairing (per ADR-002 amendment):** Provincial bank deposits are the canonical anchor when matching a Binance P2P sell to its bolívar receipt. The pairing algorithm scans unpaired bank deposits first, then searches Binance P2P sells within a ±2-day window for the match.

## 6. Source Integrations

| Source | Mechanism | Cadence | New in v1 |
| --- | --- | --- | --- |
| Binance API | Incremental REST pull (P2P, deposits/withdrawals, converts, internal transfers, Earn rewards, Pay) keyed on `import_state.last_synced_at`; default lookback 35 days to absorb missed weeks | Weekly | State tracking, Earn-positions sync, dedup via stable SDK IDs |
| Provincial bank | CSV drop in `inputs/`, parsed and ingested; **canonical anchor for P2P transfer pairing** | Per-statement (weekly) | Writes to SQLite; anchors transfer pairing |
| BCV | Live HTTP scrape (httpx + BeautifulSoup), cached daily in `rates` | Daily (schedulable) | Replaces hand-saved HTML; reference-only (never headline) |
| Binance P2P | Public market endpoint, USDT/VES median; powers the consolidated USD headline | Daily (schedulable) | New |
| Cash USD | `finances cash add` interactive CLI | Ad-hoc | New |
| **Future** Telegram bot | Inbound bot message → cash entry | Ad-hoc | EPIC-015 placeholder |
| **Future** Mobile receipt API | Authenticated POST → transaction insert | Ad-hoc | EPIC-016 placeholder |

## 7. Reporting Outputs

SQL views drive every report:

- `v_account_balances` — sum-of-amounts per account (native + USD).
- `v_transactions_usd` — every transaction enriched with `amount_usd`.
- `v_monthly_summary` — month × category × account.
- `v_unreconciled_transfers` — transfers without a paired sibling row.

The Sheets mirror exposes four tabs: `Transactions`, `Balances`, `Monthly`, `Needs Review`. The mirror is rewritten on demand by `finances sync sheets`; nobody edits it by hand.

## 8. Success Metrics

| Metric | Target |
| --- | --- |
| Unclassified rows post-backfill | 0 |
| Account balance reconciliation vs. native UI | within 0.01 (native currency) |
| Re-ingest dedup correctness (same-day rerun) | 0 new rows on second run |
| Unreconciled transfer rows | 0 |
| Headline USD summary rows sourced from BCV (not user_rate or P2P) | 0 |
| Weekly ingest + sync runtime | < 60s |
| Time to record one USD-cash expense via CLI | < 15s |

## 9. Risks & Mitigations

| Risk | Mitigation |
| --- | --- |
| Binance API rate limits | Already-implemented `time.sleep(0.1)` between calls; incremental sync drastically reduces volume vs. the current 21-day re-fetch. |
| BCV site layout changes | Snapshot the parsed table per fetch into `import_runs.error` on parse failure; fall back to last known rate; alert via CLI exit code. |
| Binance P2P endpoint drift | Cache last successful rate per day; degrade to BCV fallback. |
| Rate gaps on weekends/holidays | Carry the last preceding business-day rate forward; mark with `source='binance_p2p_median_carry'` for transparency. |
| Categorization rules conflict | Rules use explicit `priority` column; lowest number wins; `needs_review=1` if no rule matches. |
| SQLite contention with concurrent CLI + cron | WAL mode; readers don't block writers. |

## 10. Future Extension Points

The v1 architecture is built so the following extensions can be added later **without breaking changes** — only forward migrations and additive code. They are deliberately not in v1, but the foundation must not preclude them.

### 10.1 Receipt-First Categorization (planned, EPIC-017)

A future mobile app captures receipts at point-of-sale and pushes structured entries (`amount`, `category`, `description`, `merchant`, `receipt_url`, `tax_relevant`). The receipt becomes the source of truth for category and description; later, ingested bank/Binance rows are reconciled against pending receipts.

**Foundation hooks already in place:**

- ADR-006 amendment defines categorization as an **open-ended priority chain** so a `receipt → user override → engine rules → needs_review` order can be added without touching existing rule code.
- ADR-002 amendment + EPIC-006 generalize transfer pairing into a **reconciliation-passes pattern** (`run_reconciliation_pass(strategy)`); a `ReceiptToTransactionMatch` strategy plugs in alongside `BankAnchoredP2pPairing`.
- `transactions.source` is `TEXT` (not an enum); accepts `'receipt_match'` or any new value with no migration.
- ADR-009 (Pydantic at boundaries) means a future `Receipt` model and POST endpoint validate inputs identically to ingest paths.

**Future schema additions (forward migration):** a `receipts` table and a nullable `transactions.receipt_id` FK. No v1 changes required.

### 10.2 Cloud Receipt Storage (planned, EPIC-018)

Receipt images need to live somewhere persistent for tax retention (multi-year). Recommended target: **Google Drive** (already in the user's Workspace stack, sits next to the Sheets mirror, single auth). Alternatives (S3, Supabase Storage) are open if circumstances change. Decision deferred until EPIC-018 lands.

### 10.3 Receipt CLI Stop-Gap (planned, EPIC-019)

Before the mobile app ships: a `finances receipt add <file> --amount ... --category ... --description "..."` command uploads to the chosen storage and inserts the `receipts` row. Same code path the future mobile API will call.

### 10.4 Tax Export (planned, EPIC-020)

A `finances report tax --year YYYY` view emits a deductible-receipts CSV (with cloud URLs) suitable for tax filing. Reads from the `receipts` table; no impact on the transactional ledger.

### 10.5 Mobile Receipt API (planned, EPIC-016)

Authenticated POST endpoint backing the mobile app. Prerequisites are pinned in EPIC-016's roadmap entry; all of them are v1 deliverables.

## 11. Open Questions

- Telegram bot UX (EPIC-015): commands vs. natural language?
- Mobile receipt API (EPIC-016): auth model (single API token vs. proper OAuth)?
- Are there other banks/exchanges to onboard in the next 6 months?

None of the above block v1.
