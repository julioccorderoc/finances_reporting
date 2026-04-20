# Rule 006 — All Categorization Goes Through the Engine

**Source ADR:** [ADR-006](../../ADR/ADR-006-category-taxonomy-revamp.md)
**Scope:** All transaction inserts.

**Constraint:** `category_id` on `transactions` is set by `finances.domain.categorization.suggest()` or by an explicit human override (CLI/cleanup). No ingester may bypass the engine to assign categories from raw source-specific logic. If the engine returns `None`, the row is inserted with `category_id = NULL` and `needs_review = 1`.

**Schema/seed discipline:** Adding a new category or rule requires a forward migration under `finances/db/migrations/` (e.g. `005_add_category_X.sql`). Ad-hoc inserts into `categories` or `category_rules` from application code are forbidden.
