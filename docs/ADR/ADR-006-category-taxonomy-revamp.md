# ADR-006: Full Category Taxonomy Revamp + Description-Based Auto-Suggest Rules

**Date:** 2026-04-19
**Status:** Accepted

## 1. Context

The current category list (24 categories defined in `Finanzas - Categories.csv`) has known problems:

- "Ant" is defined as "small amounts that are not tracked" — a contradiction (it is tracked).
- "No ID" is a destination for unidentified transactions, hiding the fact that they are unclassified.
- "Transit" is vague.
- No explicit "Internal Transfer" category exists, even though Binance has 80+ transfer rows.
- Casing is inconsistent ("Income" vs. "Inflow" across files).
- ~19% of bank rows (158) have category = NA.

Categorization is currently 100% manual.

Three options:

1. Full rework with new taxonomy + auto-suggest rules.
2. Keep current categories, just clean the NA rows.
3. Migrate as-is, refine over time.

Option 2 leaves the broken taxonomy in place. Option 3 means living with the messy categories until a future refactor that may never happen.

## 2. Decision

Adopt a **full v1 taxonomy rework** alongside a **rules engine** that auto-suggests categories from the transaction description (regex patterns scoped optionally by source/account, with explicit priority).

v1 taxonomy (top-level kinds × subcategories):

- **income**: Salary, Gigs, Interest, Other Income, Loan Repayment
- **expense**: Groceries, Transport, Health, Family, Lifestyle, Subscriptions, Purchases, Fees, Tools, Other Expense, Dating, Gifts, Leisure, Personal Care, Utilities, Lending, Education, Clothing
- **transfer**: Internal Transfer, External Transfer (lending given/repaid)
- **adjustment**: Reconciliation, FX Diff

Drop "Ant". Drop "No ID" as a destination — replaced by `transactions.needs_review = 1`. Add explicit "Internal Transfer" so the double-entry pairs from ADR-002 land cleanly.

## 3. Consequences (The "Why")

### Positive

- Cleaner reports; no more "Ant" black hole.
- The `needs_review` flag is a real queue, not a hidden bucket.
- Auto-suggest reduces day-to-day toil; the rules table is editable, so the user can teach the system without touching code.
- Explicit "Internal Transfer" category makes ADR-002 pairs queryable.

### Negative

- One-time disruption: existing 158 NA rows must be classified during EPIC-012 backfill cleanup.
- The taxonomy is a v1 — expect refinements after a few weeks of real use; ADR-006 must be amended (not silently mutated) when that happens.

## 4. Rule Extraction (The "How" for Agents)

**Target File:** `docs/architecture/rules/rule-006-categorization-pipeline.md`
**Injected Constraint:** All category assignments must flow through `finances.domain.categorization.suggest()`. If the function returns `None`, the transaction is inserted with `category_id = NULL` and `needs_review = 1`. No ingester or backfill path may bypass the engine to assign categories directly. Adding new categories or rules must update the seed data in `finances/db/migrations/` (a forward migration) rather than ad-hoc inserts.

---

## Amendment 2026-04-19 — Open-Ended Priority Chain

**Context:** A future feature (mobile app receipt entry; see PRD "Future Extension Points" and EPIC-017+) will introduce a new categorization source: receipts captured at point-of-sale carry a user-supplied category that is more authoritative than any auto-suggest rule. The categorization model must accommodate new source-of-truth tiers without rewriting existing rule code.

**Amendment:** `finances.domain.categorization.suggest()` is the *current* entry point, but it is one tier of an **open-ended priority chain**:

1. *(future)* Receipt-supplied category (when a matching `receipts` row exists)
2. *(future or current)* Per-transaction user override
3. Auto-suggest rules engine (current `suggest()` implementation)
4. None → `needs_review = 1`

Future tiers must be added by extending the resolver in `finances.domain.categorization`, **not** by giving ingesters their own category logic. The rule-006 constraint (no ingester sets `category_id` directly) holds for every future tier.

**Forward-compatibility requirement:** the v1 implementation of `suggest()` must accept its inputs as a Pydantic model (per ADR-009) so additional context (receipt match, override) can be added as optional fields without breaking callers.

---

## Amendment 2026-04-20 — Legacy-Taxonomy Alignment

Amendment 2026-04-20: renamed `Food`→`Groceries` (per legacy-taxonomy alignment: Groceries = home food, Leisure = going-out food). Added expense categories `Leisure, Personal Care, Utilities, Lending, Education` and income category `Loan Repayment`. See migration 004.

## Amendment 2026-04-20 (v1.2) — Clothing

Added expense category `Clothing` (migration 005). Julio's legacy ledger uses `Sub-Category=Clothing` for apparel purchases (~10 rows in the backfill). Discussed mapping to `Purchases` or `Lifestyle` and rejected both — Clothing is distinct enough to warrant its own bucket.
