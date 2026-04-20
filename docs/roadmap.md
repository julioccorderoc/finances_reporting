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
| 1 | EPIC-002, EPIC-003, EPIC-002b | Yes (3-way; disjoint files) | Schema + migrations green; PRD/ADRs merged; testing infra + CI live; coverage gate enforced |
| 2 | EPIC-004 … EPIC-011 | Yes (8-way) | All ingest + domain modules green; unit tests pass; per-epic TDD checklist met (rule-011) |
| 3 | EPIC-012, EPIC-013, EPIC-014, EPIC-021 | Partial (012 first; then 013 ‖ 014 ‖ 021) | Backfill clean; balances reconcile; Sheets mirror live; integration suite green |
| 4 (future) | EPIC-015, EPIC-016, EPIC-017, EPIC-018, EPIC-019, EPIC-020 | Partial (017 → 018 → 019; 020 after 017; 016 after 017+018) | Out of scope for v1 cut |

## Epic Ledger

---

### EPIC-001 — Project Foundation & Restructure

**Status:** Complete
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

**Status:** Complete
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

**Status:** Complete
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
- `docs/PRD.md` and `docs/roadmap.md` contain no unresolved placeholder markers (see the repository contribution notes for the banned-token list). The two deferred-epic `ADRs:` lines under EPIC-015 and EPIC-016 are the only permitted exception and are intentional per the wave-4 design.

---

### EPIC-002b — Testing Infrastructure & TDD Foundation

**Status:** Complete
**Wave:** 1
**Dependencies:** EPIC-001 (uses the package layout). Runs in parallel with EPIC-002 and EPIC-003 — disjoint files.
**ADRs:** ADR-011

**Business Objective:** Land shared testing infrastructure and TDD discipline before any Wave 2 agent writes a line of production code, so the parallel epics inherit a uniform, CI-enforced testing floor instead of inventing their own conventions.

**Technical Boundary:**

- Add dev dependencies via `uv add --dev`: `pytest-cov`, `hypothesis`, `polyfactory`, `responses`, `pytest-mock`.
- Author `tests/conftest.py` with shared fixtures:
  - `in_memory_db()` — fresh in-memory SQLite with the EPIC-002 migrations applied.
  - `seeded_db()` — `in_memory_db` + the v1 categories/accounts seed (so Wave 2 epics don't each rebuild the seed).
  - Pydantic factories via `polyfactory` for `Account`, `Category`, `Transaction`, `Rate`, `EarnPosition`.
  - `mocked_binance_sdk()` and `mocked_http()` (responses) helpers with conventions documented in module docstrings.
- Add `pyproject.toml` `[tool.coverage]` section:
  - `[tool.coverage.report] fail_under = 85` baseline; per-package overrides via `[tool.coverage.run] source = ["finances"]` and a custom `--cov-config` invocation that bumps `finances/ingest/**` to 70 and excludes `finances/cli/**` + `finances/reports/**` from the gate in v1.
- Add `pytest.ini` (or `[tool.pytest.ini_options]` in `pyproject.toml`) registering markers `snapshot`, `smoke`.
- Add CI workflow `.github/workflows/ci.yml` (or `pre-commit` hook if no GitHub setup yet) that runs `pytest --cov` on every PR and blocks merge on failure or coverage regression.
- Write `docs/architecture/rules/rule-011-tdd-discipline.md` (already created; verify it lives in the repo).
- One-time retroactive pass: measure EPIC-002's coverage post-merge; if below 85% on `finances/db/**` or `finances/domain/models.py`, add tests to bring it to threshold (does not block EPIC-002 completion, but blocks EPIC-002b).

**Verification Criteria (Definition of Done):**

- `pytest --cov` runs locally and in CI; coverage report renders.
- The five dev dependencies are installed and pinned in `uv.lock`.
- `tests/conftest.py` exposes `in_memory_db`, `seeded_db`, factories for all Pydantic models.
- A sample property-based test (e.g. `tests/test_smoke_property.py`) exercises hypothesis to prove the integration works.
- CI workflow exists and is green on a no-op PR.
- Coverage of merged EPIC-002 code is ≥85% for `finances/db/**` and `finances/domain/models.py`; gaps closed by tests added in this epic.

**TDD discipline (per ADR-011 / rule-011):** EPIC-002b is itself a bootstrap epic — exempt from the commit-ordering TDD-evidence rule, but the infrastructure it ships must enable the rule for every downstream epic.

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

**TDD discipline (per ADR-011 / rule-011):**

- Every public function in `finances/domain/categorization.py` has ≥1 happy-path test AND ≥1 failure-mode test.
- Coverage of `finances/domain/categorization.py` ≥ 85%.
- Test commits precede implementation commits in the branch history.
- Property-based tests (hypothesis) exercise rule priority ordering and source/account scoping (mandatory per rule-011).

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

**TDD discipline (per ADR-011 / rule-011):**

- Every public function in `finances/domain/rates.py` has ≥1 happy-path test AND ≥1 failure-mode test.
- Coverage of `finances/domain/rates.py` ≥ 85%.
- Test commits precede implementation commits in the branch history.
- Property-based tests (hypothesis) exercise the 4-branch priority chain and carry-forward logic (mandatory per rule-011).

---

### EPIC-006 — Reconciliation Engine (Double-Entry Transfers as First Strategy)

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

**TDD discipline (per ADR-011 / rule-011):**

- Every public function in `finances/domain/reconciliation.py` and `finances/domain/transfers.py` has ≥1 happy-path test AND ≥1 failure-mode test.
- Coverage of both modules ≥ 85%.
- Test commits precede implementation commits in the branch history.
- The `ReconciliationStrategy` Protocol is exercised by ≥1 test that uses a fake strategy implementation, proving the seam is genuinely pluggable for future strategies (EPIC-017).

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

**TDD discipline (per ADR-011 / rule-011):**

- Every public function in `finances/ingest/binance.py` and `finances/domain/earn.py` has ≥1 happy-path test AND ≥1 failure-mode test.
- Coverage of `finances/ingest/binance.py` ≥ 70%; `finances/domain/earn.py` ≥ 85%.
- Test commits precede implementation commits in the branch history.
- Binance SDK is mocked via `pytest-mock` per the convention documented in `tests/conftest.py` (rule-011); no live API calls in the suite.

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

**TDD discipline (per ADR-011 / rule-011):**

- Every public function in `finances/ingest/provincial.py` has ≥1 happy-path test AND ≥1 failure-mode test.
- Coverage of `finances/ingest/provincial.py` ≥ 70%.
- Test commits precede implementation commits in the branch history.
- The bank-anchored P2P pairing call site is covered by a dedicated test using a synthetic Provincial deposit + Binance P2P fixture pair.

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

**TDD discipline (per ADR-011 / rule-011):**

- Every public function in `finances/ingest/bcv.py` has ≥1 happy-path test AND ≥1 failure-mode test.
- Coverage of `finances/ingest/bcv.py` ≥ 70%.
- HTTP calls mocked via `responses`; no live BCV fetches in the suite.
- The snapshot-based parsing tests are tagged `@pytest.mark.snapshot` and are exempt from the TDD-evidence (commit-ordering) rule per rule-011.
- All other tests follow standard test-first commit ordering.

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

**TDD discipline (per ADR-011 / rule-011):**

- Every public function in `finances/ingest/p2p_rates.py` has ≥1 happy-path test AND ≥1 failure-mode test.
- Coverage of `finances/ingest/p2p_rates.py` ≥ 70%.
- HTTP calls mocked via `responses`; no live Binance P2P fetches in the suite.
- Test commits precede implementation commits in the branch history.

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

**TDD discipline (per ADR-011 / rule-011):**

- Every public function in `finances/ingest/cash_cli.py` has ≥1 happy-path test AND ≥1 failure-mode test.
- Coverage of `finances/ingest/cash_cli.py` ≥ 70%.
- Test commits precede implementation commits in the branch history.
- The Typer CLI is tested via Typer's `CliRunner` so non-interactive invocations are fully covered.

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

**TDD discipline (per ADR-011 / rule-011):**

- The backfill orchestrator is thin (per rule-004); coverage focus is on the interactive cleanup pass and the implicit-transfer detection helpers.
- Every public function in `finances/migration/backfill.py` and `finances/migration/interactive_cleanup.py` has ≥1 happy-path test AND ≥1 failure-mode test.
- Test commits precede implementation commits in the branch history.
- Coverage gate not enforced on `finances/migration/**` in v1 (one-time scripts), but the test suite must include at least one end-to-end test on a small synthetic CSV slice; the full real-data backfill is acceptance-tested by the user, not pytest.

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

**TDD discipline (per ADR-011 / rule-011):**

- Every public function in `finances/reports/{balances,consolidated_usd,monthly}.py` has ≥1 happy-path test AND ≥1 failure-mode test.
- Coverage gate not enforced on `finances/reports/**` in v1; reporting correctness is exhaustively covered by the integration suite (EPIC-021).
- Test commits precede implementation commits in the branch history.
- Headline rule (per ADR-005 amendment) is covered by a dedicated test asserting `--strict` exits non-zero when a BCV-sourced row would appear in the consolidated headline.

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

**TDD discipline (per ADR-011 / rule-011):**

- `gspread` calls are mocked via `pytest-mock`; no live Sheets writes in the suite.
- Every public function in `finances/reports/sheets_sync.py` has ≥1 happy-path test AND ≥1 failure-mode test.
- Coverage gate not enforced on `finances/reports/**` in v1; live correctness is acceptance-tested by the user against a sandbox spreadsheet.
- A dedicated test asserts the sentinel row is written first and that the destructive-per-tab semantics (clear + write, no merge) is preserved.
- Test commits precede implementation commits in the branch history.

---

### EPIC-021 — Integration Test Suite (End-to-End Pipeline)

**Status:** Pending
**Wave:** 3
**Dependencies:** EPIC-002, EPIC-002b, EPIC-004 through EPIC-012, EPIC-013
**ADRs:** ADR-011

**Business Objective:** Provide a re-runnable end-to-end test that exercises the full ingest → reconcile → report pipeline against synthetic but realistic fixtures. Without this, EPIC-012 (one-time backfill) is the only thing exercising integration, and the next regression has no cheap reproduction path.

**Technical Boundary:**

- Author `tests/integration/` package separate from unit tests:
  - `tests/integration/fixtures/binance_api/` — JSON snapshots of every Binance endpoint covering: P2P sells with paired Provincial deposits, internal transfers, deposits, withdrawals, converts, Earn rewards, Earn positions.
  - `tests/integration/fixtures/provincial.csv` — synthetic bank statement with deposits, expenses, fees, ATM withdrawals, transfers between own accounts; deliberately includes the shapes that should pair with the Binance fixtures.
  - `tests/integration/fixtures/bcv_snapshot.html` — pinned snapshot of the BCV page.
  - `tests/integration/fixtures/p2p_response.json` — mocked P2P median fixture.
- `tests/integration/test_pipeline.py`:
  - `test_full_pipeline_idempotent`: runs `finances ingest all` twice on a fresh DB, asserts second run inserts 0 rows (per ADR-010).
  - `test_balances_reconcile`: after full pipeline, asserts every account balance matches an expected ledger sum within 0.01.
  - `test_no_unreconciled_transfers`: asserts `v_unreconciled_transfers` returns 0 rows.
  - `test_no_needs_review_after_cleanup`: simulates the interactive cleanup pass (with deterministic auto-pick) and asserts `needs_review = 1` count drops to 0.
  - `test_consolidated_usd_excludes_bcv_headlines` (per ADR-005 amendment): asserts no headline row uses BCV.
  - `test_p2p_pair_anchored_to_bank` (per ADR-002 amendment): asserts paired transfers identify the Provincial leg as anchor.
  - `test_earn_position_sum_matches_subscriptions_minus_redemptions` (per ADR-003).
- Mark all integration tests with `@pytest.mark.integration` and run them via `pytest -m integration` separately from unit tests; CI runs both.
- Add `make integration` (or `uv run pytest -m integration`) helper.

**Verification Criteria (Definition of Done):**

- `pytest -m integration` is green.
- Total integration suite runtime < 30s on a developer machine.
- A deliberate regression (e.g. setting `transfer_id=NULL` on a paired row, or breaking the rate resolver) causes at least one integration test to fail with a clear message.
- CI runs integration tests on every PR.

**TDD discipline (per ADR-011 / rule-011):** Integration tests are written before the pipeline integrations they cover are wired together. `@pytest.mark.smoke` is permitted for genuinely wiring-only tests.

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
**Dependencies:** EPIC-002, EPIC-004, EPIC-006, EPIC-009 (ADR-009 Pydantic models), EPIC-017, EPIC-018
**ADRs:** TBD (will reference ADR-009, future ADR-011, future ADR-012)

**Business Objective:** Authenticated POST endpoint that the planned receipt-parsing mobile app calls to push structured receipts into the ledger.

**v1 prerequisites already in place (do not regress):**

- Pydantic v2 domain models (ADR-009) — request bodies validate via the same models.
- Repos accept Pydantic instances only; no raw `dict` (rule-009).
- Deterministic `source_ref` strategy supports an `Idempotency-Key` header (ADR-010).
- Reconciliation-passes pattern (`run_reconciliation_pass(strategy)`) is the seam for `ReceiptToTransactionMatch`.
- Categorization priority chain (ADR-006 amendment) admits a "receipt-supplied category" tier without modifying the rules engine.

**Technical Boundary:** Out of scope for v1. Placeholder. When activated: FastAPI (or similar) + auth + a single `POST /receipts` endpoint backed by `repos.receipts.upsert_by_source_ref`.

**Verification Criteria (Definition of Done):** Defined when activated.

---

### EPIC-017 — Receipts Schema + Reconciliation Strategy (Future)

**Status:** Deferred
**Wave:** 4
**Dependencies:** EPIC-002, EPIC-006
**ADRs:** TBD (will be ADR-011)

**Business Objective:** Make the receipt the source of truth for category + description, and reconcile incoming bank/Binance rows against pending receipts so the user's manual categorization happens once, at point-of-sale.

**Anticipated technical boundary (sketch only):**

- Forward migration: add `receipts` table (`id, captured_at, amount, currency, merchant, category_id, description, receipt_url, tax_relevant, tax_year, source, status, matched_transaction_id, source_ref, created_at`) and `transactions.receipt_id` nullable FK.
- Pydantic `Receipt` model in `finances/domain/models.py` (additive — does not change existing models).
- `finances/db/repos/receipts.py` — same shape as `transactions` repo (`upsert_by_source_ref`).
- `finances/domain/reconciliation_strategies/receipt_match.py` — implements `ReconciliationStrategy`. Matches by `(amount within tolerance, ±5 days, optional merchant fuzzy match)`.
- `finances/domain/categorization.py` extension: insert a "receipt-supplied" tier at the top of the priority chain (per ADR-006 amendment).
- Provincial + Binance ingesters call `run_reconciliation_pass(ReceiptToTransactionMatch())` after their existing reconciliation passes.

**Open architectural questions (decide before activation):** matching tolerance (% on `amount_usd` vs. native), behavior for receipts that never match (keep pending vs. auto-promote to synthetic Cash USD expense), tax fields scope.

**Verification Criteria (Definition of Done):** Defined when activated.

---

### EPIC-018 — Cloud Receipt Storage (Future)

**Status:** Deferred
**Wave:** 4
**Dependencies:** EPIC-001 (config + auth conventions)
**ADRs:** TBD (will be ADR-012)

**Business Objective:** Persistent, audit-grade storage for receipt images, retained across multiple tax years.

**Anticipated boundary (sketch only):**

- Choose storage target — recommended **Google Drive** (already in the user's Workspace stack; integrates with the existing Sheets mirror auth; the `gws-drive-upload` skill is already available locally). Alternatives if circumstances change: S3, Supabase Storage.
- `finances/storage/receipts.py` provides `upload(local_path) -> ReceiptUrl` and `verify(url) -> bool`.
- Folder layout in Drive: `Finances/Receipts/<YYYY>/<YYYY-MM>/<receipt_id>_<merchant>.<ext>`.
- Auth lives in `.env` (extends existing Workspace credentials, does not require a new account).

**Verification Criteria (Definition of Done):** Defined when activated.

---

### EPIC-019 — Receipt CLI Stop-Gap (Future)

**Status:** Deferred
**Wave:** 4
**Dependencies:** EPIC-017, EPIC-018
**ADRs:** TBD

**Business Objective:** Before the mobile app exists, allow receipt entry from the workstation. Bridges the gap and exercises the same code path the future mobile API will call.

**Anticipated boundary (sketch only):**

- `finances receipt add <file>` — Typer subcommand with `--amount`, `--category`, `--description`, `--merchant`, `--captured-at`, `--tax-relevant` flags.
- Calls `storage.receipts.upload()` (EPIC-018), then `repos.receipts.upsert_by_source_ref()` (EPIC-017).
- After insert, runs `run_reconciliation_pass(ReceiptToTransactionMatch())` so the new receipt immediately attempts to find a bank match.

**Verification Criteria (Definition of Done):** Defined when activated.

---

### EPIC-020 — Tax Export (Future)

**Status:** Deferred
**Wave:** 4
**Dependencies:** EPIC-017
**ADRs:** TBD

**Business Objective:** Produce a deductible-receipts export per tax year, with cloud URLs, suitable for filing or sharing with an accountant.

**Anticipated boundary (sketch only):**

- `finances report tax --year YYYY [--format csv|pdf]` — reads `receipts WHERE tax_relevant=1 AND tax_year=YYYY`, joins to `transactions` for matched amounts, emits CSV by default.
- View `v_receipts_tax` materializes the join.

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
| 1 | EPIC-002, EPIC-003, EPIC-002b | **Yes (3-way)** | EPIC-002 writes `finances/db/**` + `finances/domain/models.py`. EPIC-003 writes `docs/**`. EPIC-002b writes `tests/conftest.py`, `pyproject.toml` `[tool.coverage]`/`[tool.pytest.ini_options]` sections, `.github/workflows/ci.yml`. Disjoint. (EPIC-002b's retroactive coverage pass on EPIC-002 must wait until EPIC-002 lands.) |
| 2 | EPIC-004, EPIC-005, EPIC-006, EPIC-007, EPIC-008, EPIC-009, EPIC-010, EPIC-011 | **Mostly yes — see caveats** | Each touches its own module. See the matrix below. |
| 3 | EPIC-012, EPIC-013, EPIC-014, EPIC-021 | **Partial: EPIC-012 sequential; then EPIC-013 ‖ EPIC-014 ‖ EPIC-021** | EPIC-012 must finish before reporting/sync/integration run on real data. After 012, 013 (`finances/reports/{balances,consolidated_usd,monthly}.py`), 014 (`finances/reports/sheets_sync.py`), and 021 (`tests/integration/**`) are file-disjoint. |

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
# Wave 0
sequential: EPIC-001

# Wave 1 — 3-way parallel (EPIC-002 + EPIC-003 + EPIC-002b)
parallel:
  EPIC-002    # schema + repos + Pydantic models
  EPIC-003    # docs (PRD/roadmap/ADRs)
  EPIC-002b   # testing infra + CI + TDD foundation

# Wave 2 — after Wave 1 Complete
parallel: EPIC-006   # owns reconciliation.run_reconciliation_pass + transfers.create_transfer signatures
then parallel:
  EPIC-004
  EPIC-005
  EPIC-007
  EPIC-008
  EPIC-009
  EPIC-010
  EPIC-011

# Wave 3 — after all of Wave 2 Complete
sequential: EPIC-012   # one-time backfill
then parallel:
  EPIC-013
  EPIC-014
  EPIC-021   # integration test suite
```

### Epic invocation template (for the user when commanding the next session)

> "Read `docs/roadmap.md` and execute **EPIC-NNN** end-to-end. Stop at the Verification Criteria; report what to run to confirm."
