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

- **income**: Salary, Gigs, Interest, Other Income
- **expense**: Food, Transport, Health, Family, Lifestyle, Subscriptions, Purchases, Fees, Tools, Other Expense
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
