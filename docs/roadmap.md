# ROADMAP

**Version:** 1.0
**Last Updated:** 2026-04-19
**Primary Human Owner:** Julio Cordero

## Operating Rules for the Planner Agent

- You may only move one Epic to **Active** at a time *per execution lane*. Epics in different waves with no overlapping files may run in parallel under separate agents.
- Before marking an Epic **Complete**, you must verify all its Verification Criteria are met in the `main` branch.
- Do not parse or extract Epics that depend on incomplete prerequisites.
- Wave 0 (EPIC-001) blocks all other work. Wave 1 (EPIC-002, EPIC-003) blocks Wave 2. Wave 2 epics are mutually parallel-safe — they touch separate modules. Wave 3 depends on Wave 2 completions as listed under each Epic's `Dependencies`.
- Documentation epics (e.g. EPIC-003) may run in parallel with code epics in their wave.
- Every Epic must reference the ADR(s) it implements; deviations require a new ADR before merging.

## Wave Map

| Wave | Epics | Parallel? | Gate |
| --- | --- | --- | --- |
| 0 | EPIC-001 | No | Package layout merged, `pyproject.toml` updated, legacy scripts archived |
| 1 | EPIC-002, EPIC-003 | Yes (different concerns) | Schema + migrations green; PRD/ADRs merged |
| 2 | EPIC-004 … EPIC-011 | Yes (8-way) | All ingest + domain modules green; unit tests pass |
| 3 | EPIC-012, EPIC-013, EPIC-014 | Partial (013 ‖ 014 after 012) | Backfill clean; balances reconcile; Sheets mirror live |
| 4 (future) | EPIC-015, EPIC-016 | Yes | Out of scope for v1 cut |

## Epic Ledger

---

### EPIC-001 — Project Foundation & Restructure

**Status:** Pending
**Wave:** 0
**Dependencies:** —
**ADRs:** ADR-001 (informs target layout)

**Business Objective:** Replace the flat-script layout with a proper package so subsequent epics can be developed and tested in parallel without colliding on a single `main.py`-sized blob. Make the project installable, runnable as a CLI, and gitignore the new `finances.db`.

**Technical Boundary:**

- Create `finances/` package with empty submodules: `db/`, `domain/`, `ingest/`, `reports/`, `migration/`, `cli/`.
- Move `download_binance.py`, `extract_bcv.py`, `extract_provincial.py`, `table_bcv.html`, and `main.py` into `legacy/` (do **not** delete — they are reference for ports).
- Update `pyproject.toml`: add `typer`, `gspread`, `google-auth`, `httpx`, `pydantic>=2`, `pytest`, `pytest-cov`; add `[project.scripts] finances = "finances.cli.main:app"`.
- Add `finances/config.py` (env loading via `python-dotenv`, paths, Caracas timezone constant).
- Update `.gitignore`: `finances.db`, `finances.db-*`, `finances.db-journal`, `output/`.

**Verification Criteria (Definition of Done):**

- `uv pip install -e .` succeeds.
- `finances --help` prints the Typer help banner.
- `legacy/` contains all four old scripts; root no longer has them.
- `pytest -q tests/` runs (zero tests yet is acceptable).
- `git status` clean after staging.

---

### EPIC-002 — SQLite Schema + Repository Layer

**Status:** Pending
**Wave:** 1
**Dependencies:** EPIC-001
**ADRs:** ADR-001, ADR-002, ADR-003, ADR-009, ADR-010

**Business Objective:** Provide the source-of-truth ledger that every subsequent epic writes into. Without it, ingest and reporting epics have nothing to talk to.

**Technical Boundary:**

- Author `finances/db/migrations/001_initial.sql` with tables: `accounts`, `categories`, `category_rules`, `transactions`, `rates`, `earn_positions`, `import_state`, `import_runs` (per the locked schema in the plan file).
- Create SQL views: `v_account_balances`, `v_transactions_usd`, `v_monthly_summary`, `v_unreconciled_transfers`.
- Implement `finances/db/connection.py` (sqlite3 with WAL mode, foreign keys ON).
- Implement `finances/db/migrate.py` runner (idempotent, applies any unapplied `00X_*.sql` in order; tracks applied set in a `_migrations` table).
- Implement `finances/domain/models.py` as **Pydantic v2** `BaseModel` subclasses (`Account`, `Category`, `Transaction`, `Rate`, `EarnPosition`) with strict validators per ADR-009.
- Implement `finances/db/repos/{accounts,categories,transactions,rates,positions,import_state}.py` with thin CRUD that **accepts and returns Pydantic models only** (per ADR-009). The transactions repo provides `upsert_by_source_ref(source, source_ref, model) -> int` that uses `INSERT … ON CONFLICT(source, source_ref) DO UPDATE SET updated_at = CURRENT_TIMESTAMP` (per ADR-010).
- Tests: `tests/test_db_schema.py` exercises every table, the dedup unique constraint, and re-insert idempotency (insert same row twice → second call returns `rows_inserted=0`).

**Verification Criteria (Definition of Done):**

- `python -m finances.db.migrate` creates `finances.db` and exits 0.
- Re-running the migrate command is a no-op.
- `pytest tests/test_db_schema.py` is green.
- All four views exist and return rows on a seeded fixture.
- `transactions` `(source, source_ref)` unique constraint rejects duplicates.
- `repos.transactions.upsert_by_source_ref` is idempotent (second call with identical input returns `rows_inserted=0`).
- All domain models in `finances/domain/models.py` are Pydantic `BaseModel` subclasses; `grep -rn "from dataclasses import dataclass" finances/domain/` returns empty.

---

### EPIC-003 — Documentation Deliverables (PRD + Roadmap + ADRs)

**Status:** Active
**Wave:** 1
**Dependencies:** EPIC-001 (for repo structure)
**ADRs:** ADR-001 through ADR-008

**Business Objective:** Lock and publish the architectural decisions and product requirements so subsequent epics can be executed by independent agents without re-litigating choices.

**Technical Boundary:**

- `docs/PRD.md` — committed.
- `docs/roadmap.md` — this file.
- `docs/ADR/ADR-001..ADR-008.md` — one per locked decision, using the user-provided template.
- `docs/architecture/rules/` — short rule-extraction files referenced by each ADR.

**Verification Criteria (Definition of Done):**

- All eight ADRs exist, each with a non-empty `Rule Extraction` block referencing a file under `docs/architecture/rules/`.
- All referenced rule files exist and contain a single, machine-readable constraint statement.
- `docs/PRD.md` and `docs/roadmap.md` contain no TODO markers.

---

### EPIC-004 — Category Taxonomy Revamp + Rules Engine

**Status:** Pending
**Wave:** 2
**Dependencies:** EPIC-002
**ADRs:** ADR-006

**Business Objective:** Eliminate the 158 unclassified bank rows by giving every transaction a clean place to land, and reduce future classification toil with description-based auto-suggest rules.

**Technical Boundary:**

- Implement `finances/domain/categorization.py`: a regex-rules engine that takes a description + source + account and returns a `category_id` or `None`.
- Seed data: a Python module (or YAML) that the migration runner writes into `categories` and `category_rules`.
- v1 taxonomy (drop "Ant"; drop "No ID" as a destination — use `needs_review=1` instead): `income` (Salary, Gigs, Interest, Other Income); `expense` (Food, Transport, Health, Family, Lifestyle, Subscriptions, Purchases, Fees, Tools, Other Expense); `transfer` (Internal Transfer, External Transfer / Lending); `adjustment` (Reconciliation, FX Diff).
- Tests: `tests/test_categorization.py` covers rule priority, no-match → `needs_review`, source/account scoping.

**Verification Criteria (Definition of Done):**

- Seeded `categories` table contains the v1 taxonomy.
- `category_rules` seeded with at least one rule per common description pattern observed in `data/Finanzas - Provincial.csv` and `data/Finanzas - Binance.csv`.
- `pytest tests/test_categorization.py` is green.
- A dry-run script `finances categorize --dry-run --source provincial` reports the % of rows the rules would auto-classify.

---

### EPIC-005 — Rate Resolution Engine

**Status:** Pending
**Wave:** 2
**Dependencies:** EPIC-002
**ADRs:** ADR-005

**Business Objective:** Provide a single, auditable function that converts any transaction's native amount into USD, using the user's actual realized rate when available.

**Technical Boundary:**

- Implement `finances/domain/rates.py` with `resolve(txn) -> (rate, source)` following priority: `transactions.user_rate` → `rates(USDT, VES, day, 'binance_p2p_median')` → `rates(USD, VES, day, 'bcv')` → `None` + flag `needs_review`.
- Carry-forward strategy for missing days: use last preceding business-day rate, mark `source` with `_carry` suffix.
- Tests: `tests/test_rates.py` covers all four branches and carry-forward.

**Verification Criteria (Definition of Done):**

- `pytest tests/test_rates.py` is green.
- Engine never raises on missing rates; always returns either a value or `None` + sets `needs_review`.

---

### EPIC-006 — Transfer Pairing Engine (Double-Entry)

**Status:** Pending
**Wave:** 2
**Dependencies:** EPIC-002
**ADRs:** ADR-002 (+ amendment for reconciliation-passes pattern)

**Business Objective:** Make movements between own accounts visible per-account but invisible in income/expense reports, eliminating the current double-counting risk. **Establish the reconciliation-pass pattern** so future features (e.g. receipt↔transaction matching, EPIC-017+) plug in as new strategies without touching the existing code.

**Technical Boundary:**

- Implement `finances/domain/reconciliation.py` exposing the generic interface:
  - `class ReconciliationStrategy(Protocol)` — `match() -> list[MatchProposal]`, `apply(proposal) -> None`.
  - `run_reconciliation_pass(strategy: ReconciliationStrategy) -> ReconciliationReport` — the public entry point.
- Implement `finances/domain/transfers.py` (the first strategy lives here):
  - `create_transfer(from_account, to_account, amount, ..., anchor_transaction_id=None)` writes two transactions sharing a UUID `transfer_id`, signed appropriately. When `anchor_transaction_id` is supplied, it identifies the canonical leg (used by the bank-anchored P2P pairing path).
  - `class BankAnchoredP2pPairing(ReconciliationStrategy)` — the v1 strategy. Walks unpaired Provincial deposits, finds a matching unpaired Binance P2P sell within `±window_days` (default 2), and on apply calls `create_transfer` with the bank row as anchor.
  - `validate(transfer_id)` confirms the two legs sum to zero in their respective USD-equivalents (within tolerance 0.01).
  - `find_unreconciled()` queries `v_unreconciled_transfers`.
- Tests: `tests/test_transfers.py` covers happy path, validation failure, the unreconciled detector, and the bank-anchored strategy via `run_reconciliation_pass(BankAnchoredP2pPairing(window_days=2))` with mixed paired/unpaired fixtures.

**Forward-compatibility note:** `ReconciliationStrategy` is the seam through which future strategies (e.g. `ReceiptToTransactionMatch` in EPIC-017) attach. Do not specialize the engine to transfers only.

**Verification Criteria (Definition of Done):**

- `pytest tests/test_transfers.py` is green.
- `v_unreconciled_transfers` returns 0 rows after a successful `create_transfer` call.
- A deliberately broken pair (one leg missing) shows up in `v_unreconciled_transfers`.

---

### EPIC-007 — Binance Ingest Refactor + Earn Positions

**Status:** Pending
**Wave:** 2
**Dependencies:** EPIC-002 (also benefits from EPIC-004, EPIC-006 once available; coordinate via interfaces, not file overlap)
**ADRs:** ADR-003, ADR-009, ADR-010

**Business Objective:** Pull Binance data incrementally, write it into the ledger, and track Earn investment principal so the user finally has visibility into their investments.

**Technical Boundary:**

- Implement `finances/ingest/binance.py`:
  - Port server-time-sync logic from `legacy/download_binance.py`.
  - Use `import_state.last_synced_at` with a configurable lookback window (default **35 days** = 5 weeks of buffer for missed weekly cycles; the user has historically gone 21 days between sessions).
  - Define `RawBinance<Endpoint>Row` Pydantic models per endpoint (P2P, deposit, withdraw, convert, transfer, earn-reward, pay) with strict validators; map to canonical `Transaction` via `to_transaction()` per ADR-009.
  - Use stable SDK-provided IDs (`orderId`, `txId`, `tranId`, `payTradeNo`, `subscriptionId`) as `source_ref`; never use a content hash for Binance rows since IDs are always present (per ADR-010).
  - Write transactions via `repos/transactions.upsert_by_source_ref`.
  - Internal transfers (Funding↔Spot) emit a transfer pair via `domain.transfers.create_transfer`. **P2P sells do NOT create their bank-side leg here** — that pairing is owned by `finances/ingest/provincial.py` per ADR-002 amendment (bank-anchored).
- Implement `finances/domain/earn.py` + `finances/ingest/binance.py` Earn integration:
  - On each sync, query `simple_earn_flexible_position` (already in SDK), upsert into `earn_positions`.
  - Earn rewards become `income` transactions categorized as `Interest` and credited to the `Binance Earn` account.
- Tests: `tests/test_ingest_binance.py` with mocked SDK responses; explicit idempotency assertion (run twice on identical mock → second call inserts 0).

**Verification Criteria (Definition of Done):**

- `finances ingest binance` exits 0 on a fresh DB and on a re-run; second run inserts 0 transactions.
- `earn_positions` rows match the Binance Earn UI.
- Internal Funding↔Spot transfers appear as paired rows with shared `transfer_id`.
- `--since` and `--lookback-days` flags respected (default 35 days).

---

### EPIC-008 — Provincial Bank Ingest Refactor (P2P Pairing Anchor)

**Status:** Pending
**Wave:** 2
**Dependencies:** EPIC-002 (benefits from EPIC-004; coordinates with EPIC-006 transfer pairing and EPIC-007 Binance ingest)
**ADRs:** ADR-001, ADR-002 (amendment), ADR-009, ADR-010

**Business Objective:** Ingest Provincial bank CSV statements directly into the ledger, eliminating the messy mid-row date and dual-rate columns. **Own the canonical anchor for P2P transfer pairing** per ADR-002 amendment — the bank deposit is the ground truth.

**Technical Boundary:**

- Implement `finances/ingest/provincial.py`:
  - Define `RawProvincialRow` as a Pydantic v2 model with strict validators (per ADR-009): `Decimal`-safe `Monto`, Caracas-timezone `Fecha`, normalized `Tipo` enum.
  - Port the `Decimal`-safe parsing and Caracas-timezone date logic from `legacy/extract_provincial.py`.
  - **`source_ref` strategy (per ADR-010):** use bank `Referencia` if present and non-empty; else compute `"hash:" + sha256(occurred_at || amount || description)[:16]`.
  - Apply categorization rules; unmatched rows get `needs_review=1`.
  - **Bank-anchored P2P pairing pass:** after inserting bank rows, call `finances.domain.reconciliation.run_reconciliation_pass(BankAnchoredP2pPairing(window_days=2))`. The strategy scans unpaired Provincial deposits matching the shape of a P2P inflow (large amount, descriptions matching known counterparties or generic "transfer recibido"), searches `transactions WHERE source='binance' AND source_ref LIKE 'p2p%' AND transfer_id IS NULL` within a `±2-day` window (configurable), and on match calls `domain.transfers.create_transfer` with the bank row as the canonical anchor.
- Tests: `tests/test_ingest_provincial.py` with a fixture CSV; explicit re-ingest idempotency test; explicit P2P-pairing test with a paired Binance fixture.

**Verification Criteria (Definition of Done):**

- A re-ingest of the same CSV inserts 0 new rows (per ADR-010).
- The 843 historical rows from `data/Finanzas - Provincial.csv` import successfully (executed during EPIC-012).
- After running provincial + binance ingesters on a paired fixture, `SELECT COUNT(*) FROM transactions WHERE kind='transfer' AND transfer_id IS NULL` = 0.
- `RawProvincialRow.model_validate` rejects a fixture with a malformed `Monto` value with `pydantic.ValidationError`.

---

### EPIC-009 — BCV Automated Scraper

**Status:** Pending
**Wave:** 2
**Dependencies:** EPIC-002
**ADRs:** ADR-007

**Business Objective:** Eliminate the manual `table_bcv.html` save chore and keep BCV reference rates fresh automatically.

**Technical Boundary:**

- Implement `finances/ingest/bcv.py` using `httpx` + `BeautifulSoup`.
- Define a `RawBcvRow` Pydantic model (date + USD rate + EUR rate) with strict validators per ADR-009.
- Parse USD and EUR rows, write to `rates(base='USD', quote='VES', source='bcv')` and `rates(base='EUR', quote='VES', source='bcv')`.
- On parse failure: log to `import_runs.error`, exit non-zero, do not corrupt existing rates (per rule-007).
- Tests: `tests/test_ingest_bcv.py` against a snapshot HTML fixture; mangled-fixture test asserts non-zero exit and no DB mutation.

**Verification Criteria (Definition of Done):**

- `finances ingest bcv` populates today's USD and EUR rows.
- A snapshot-driven unit test passes.
- Layout-change simulation (mangled fixture) → non-zero exit, no DB corruption.

---

### EPIC-010 — Binance P2P Rate Fetcher

**Status:** Pending
**Wave:** 2
**Dependencies:** EPIC-002
**ADRs:** ADR-005

**Business Objective:** Provide an automatic baseline USDT/VES rate for transactions where the user did not record a personal realized rate.

**Technical Boundary:**

- Implement `finances/ingest/p2p_rates.py` using Binance's public P2P search endpoint.
- Define `RawP2pAdvert` Pydantic model with strict validators per ADR-009.
- Compute the median of the top-N adverts (configurable, default N=10) for both BUY and SELL sides; store both, plus the midpoint as `source='binance_p2p_median'`. **This is the rate that powers the consolidated USD headline summary** (per ADR-005 amendment).
- Tests: `tests/test_ingest_p2p_rates.py` with mocked HTTP; idempotency test (second run same day = 0 new rows via `rates` UNIQUE constraint).

**Verification Criteria (Definition of Done):**

- `finances ingest p2p-rates` writes one row per `(date, base='USDT', quote='VES', source='binance_p2p_median')` per day.
- Re-running the same day is idempotent (`UNIQUE` constraint covers it).

---

### EPIC-011 — Cash CLI Tool

**Status:** Pending
**Wave:** 2
**Dependencies:** EPIC-002 (benefits from EPIC-004)
**ADRs:** ADR-008

**Business Objective:** Allow USD-cash expenses to be recorded in under 15 seconds without leaving the terminal, since cash is low-frequency but currently invisible.

**Technical Boundary:**

- Implement `finances/ingest/cash_cli.py` and the `finances cash add` Typer subcommand.
- Prompts: amount (USD), date (default today), category (auto-suggest by recent uses), description.
- Writes a single `expense` transaction on the `Cash USD` account with `source='cash_cli'`.

**Verification Criteria (Definition of Done):**

- `finances cash add --amount 12 --description "lunch"` creates a row and exits 0.
- The row appears in `v_account_balances` for `Cash USD` and `v_transactions_usd`.

---

### EPIC-012 — One-Time Backfill Migration

**Status:** Pending
**Wave:** 3
**Dependencies:** EPIC-002, EPIC-004, EPIC-005, EPIC-006, EPIC-007, EPIC-008, EPIC-009
**ADRs:** ADR-004

**Business Objective:** Preserve historical continuity by importing every existing CSV row into the new ledger, with an interactive pass to resolve the 158 NA rows.

**Technical Boundary:**

- Implement `finances/migration/backfill.py`:
  - Reads `data/Finanzas - Binance.csv`, `data/Finanzas - Provincial.csv`, `data/Finanzas - BCV.csv`.
  - Routes through the same ingest modules used in production (no parallel logic).
  - Detects and pairs likely transfers (Binance internal transfer ↔ P2P sell ↔ Provincial deposit).
- Implement `finances/migration/interactive_cleanup.py`: a Typer subcommand that walks `WHERE needs_review=1` and prompts the user for category + (optional) `user_rate`.

**Verification Criteria (Definition of Done):**

- `finances backfill --from data/` completes without raising.
- After cleanup: `SELECT COUNT(*) FROM transactions WHERE needs_review=1` = 0.
- `SELECT COUNT(*) FROM transactions WHERE kind='transfer' AND transfer_id IS NULL` = 0.
- Total transactions ≈ 172 (Binance) + 843 (Provincial) + paired transfer rows.
- Spot-check: 5 random Bs transactions show the expected USD value via `v_transactions_usd`.

---

### EPIC-013 — Reporting Views + Account Balances

**Status:** Pending
**Wave:** 3
**Dependencies:** EPIC-002, EPIC-005
**ADRs:** ADR-001

**Business Objective:** Expose per-account running balances and a USD-consolidated transaction view so the user can answer "how much do I have where" and "where did the money go" with one command.

**Technical Boundary:**

- Implement `finances/reports/balances.py`, `finances/reports/consolidated_usd.py`, `finances/reports/monthly.py`.
- Wire into Typer: `finances report balances`, `finances report consolidated`, `finances report monthly`, `finances report needs-review`.
- Output formats: pretty table to stdout (default), `--json` and `--csv` flags.
- **Headline rule (per ADR-005 amendment):** `finances report consolidated` and the `Monthly` Sheets tab must use only `user_rate`- or `binance_p2p_median`-sourced USD values. Rows whose USD value would be sourced from BCV are either excluded from the headline aggregate or annotated with a clear "BCV fallback" tag and counted in a separate "fallback" column.

**Verification Criteria (Definition of Done):**

- `finances report balances` returns Binance Spot, Funding, Earn, Provincial, Cash USD balances; each within 0.01 (native) of the source UI on a freshly-backfilled DB.
- `finances report monthly` returns rows summing to the same total as `v_transactions_usd` for the month.
- `finances report consolidated --strict` exits non-zero if any headline row would use a BCV-sourced rate; `finances report consolidated` (default) annotates them.

---

### EPIC-014 — Google Sheets Read-Only Mirror

**Status:** Pending
**Wave:** 3
**Dependencies:** EPIC-013
**ADRs:** ADR-001

**Business Objective:** Let the user view and share data in Sheets without making Sheets the source of truth, so cleanups and corrections happen exactly once (in SQLite) and propagate.

**Technical Boundary:**

- Implement `finances/reports/sheets_sync.py` using `gspread` + service-account auth (OAuth credentials in `.env`).
- Tabs created/updated: `Transactions`, `Balances`, `Monthly`, `Needs Review`.
- Sync is destructive per tab (clear + write) — never merges. The mirror is fully derived state.
- Add a sentinel row at the top of each tab: `"⚠ READ-ONLY MIRROR — edit finances.db, not this sheet"`.

**Verification Criteria (Definition of Done):**

- `finances sync sheets --spreadsheet-id <id>` populates all four tabs in < 30s.
- Row counts in each tab match the corresponding SQL view.
- The sentinel row is present and non-editable (frozen).

---

### EPIC-015 — Telegram Bot for Cash Entry (Future)

**Status:** Deferred
**Wave:** 4
**Dependencies:** EPIC-011
**ADRs:** TBD

**Business Objective:** Allow USD-cash entries from a phone without opening a terminal.

**Technical Boundary:** Out of scope for v1. Placeholder.

**Verification Criteria (Definition of Done):** Defined when activated.

---

### EPIC-016 — Mobile App Receipt Ingest API (Future)

**Status:** Deferred
**Wave:** 4
**Dependencies:** EPIC-002, EPIC-004
**ADRs:** TBD

**Business Objective:** Authenticated POST endpoint that the planned receipt-parsing mobile app calls to insert structured transactions.

**Technical Boundary:** Out of scope for v1. Placeholder.

**Verification Criteria (Definition of Done):** Defined when activated.

---

## Parallel Execution Guide (For the Implementation Agent)

This roadmap is written so that another session — invoked once per epic — can execute work without having to re-derive context. Read in this order: this file → the ADRs referenced in the chosen epic's `ADRs:` line → the rules under `docs/architecture/rules/` referenced by those ADRs → the relevant `legacy/*.py` for ported logic.

### Execution rules for the agent

1. Run **only the epic the user names**. Do not bundle "while I'm in here" work; that violates the parallel-safe contract.
2. Before starting, verify all `Dependencies:` listed by the epic are marked `Complete` in this file. If any are not, stop and report.
3. Each epic's `Verification Criteria` is the gate. The agent does not mark the epic Complete; the user does, after running the verification commands.
4. If implementation requires a new architectural decision not covered by an existing ADR, stop, write a draft ADR, and ask the user to confirm before proceeding.

### What can run truly in parallel (no shared file writes)

| Wave | Epics | Can run concurrently? | Why |
| --- | --- | --- | --- |
| 0 | EPIC-001 | n/a (single epic) | Establishes package structure |
| 1 | EPIC-002, EPIC-003 | **Yes** | EPIC-002 writes `finances/db/**`, `finances/domain/models.py`, `tests/test_db_schema.py`. EPIC-003 writes `docs/**`. Disjoint. |
| 2 | EPIC-004, EPIC-005, EPIC-006, EPIC-007, EPIC-008, EPIC-009, EPIC-010, EPIC-011 | **Mostly yes — see caveats** | Each touches its own module. See the matrix below. |
| 3 | EPIC-012 (then EPIC-013 ‖ EPIC-014) | EPIC-013 ‖ EPIC-014 only | EPIC-012 must finish before reporting/sync run on real data. After 012, 013 (`finances/reports/{balances,consolidated_usd,monthly}.py`) and 014 (`finances/reports/sheets_sync.py`) are disjoint. |

### Wave 2 file-ownership matrix (use to detect collisions)

| Epic | Owns (writes) | Reads / depends on |
| --- | --- | --- |
| EPIC-004 Categories | `finances/domain/categorization.py`, `finances/db/migrations/002_seed_categories.sql`, `tests/test_categorization.py` | `finances/db/repos/categories.py` (read) |
| EPIC-005 Rates | `finances/domain/rates.py`, `tests/test_rates.py` | `finances/db/repos/rates.py` (read), `finances/db/repos/transactions.py` (read) |
| EPIC-006 Transfers | `finances/domain/transfers.py`, `tests/test_transfers.py` | `finances/db/repos/transactions.py` (write through helper) |
| EPIC-007 Binance | `finances/ingest/binance.py`, `finances/domain/earn.py`, `tests/test_ingest_binance.py` | EPIC-002 repos; coordinates with EPIC-006 (calls `create_transfer`) and EPIC-004 (calls `categorization.suggest`) — **interface contracts must be agreed before parallel start** |
| EPIC-008 Provincial | `finances/ingest/provincial.py`, `tests/test_ingest_provincial.py` | EPIC-002 repos; same coordination caveat as EPIC-007. **Owns the bank-anchored P2P pairing call site.** |
| EPIC-009 BCV | `finances/ingest/bcv.py`, `tests/test_ingest_bcv.py`, `tests/fixtures/bcv_snapshot.html` | EPIC-002 `repos/rates` |
| EPIC-010 P2P Rates | `finances/ingest/p2p_rates.py`, `tests/test_ingest_p2p_rates.py` | EPIC-002 `repos/rates` |
| EPIC-011 Cash CLI | `finances/ingest/cash_cli.py`, `finances/cli/main.py` (cash subcommand only), `tests/test_cash_cli.py` | EPIC-002 repos |

**Coordination caveat (EPIC-006 ↔ EPIC-007 ↔ EPIC-008):** these three reference each other through function signatures, not through file overlap. The recommended order if parallelizing them is:

1. EPIC-006 first ships `domain.transfers.create_transfer` with a stable signature (a stub returning `NotImplementedError` is fine for the dependents to import).
2. EPIC-007 and EPIC-008 then proceed in parallel, both importing the stub.
3. EPIC-006 fills in the implementation; the dependents pick it up automatically once merged.

### Suggested parallel-launch script (for a multi-agent runner)

```text
# After EPIC-001 + EPIC-002 + EPIC-003 are Complete:
parallel: EPIC-006   # owns transfers.create_transfer signature
then parallel:
  EPIC-004
  EPIC-005
  EPIC-007
  EPIC-008
  EPIC-009
  EPIC-010
  EPIC-011

# After all of Wave 2 is Complete:
sequential: EPIC-012
then parallel:
  EPIC-013
  EPIC-014
```

### Epic invocation template (for the user when commanding the next session)

> "Read `docs/roadmap.md` and execute **EPIC-XXX** end-to-end. Stop at the Verification Criteria; report what to run to confirm."
