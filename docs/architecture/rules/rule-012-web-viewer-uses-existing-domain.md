# Rule 012 — Web Viewer Reuses Existing Domain; No Parallel Logic

**Source:** ADR-012

## Constraint

Any code under `finances/web/` that performs a write must call existing functions in `finances/db/repos/*` or `finances/domain/*`. The viewer may not:

- execute SQL `INSERT` / `UPDATE` / `DELETE` directly,
- implement its own categorization, rate-resolution, or transfer-pairing logic,
- bypass Pydantic validation at the request boundary.

The `transactions.needs_review` flag is **derived** from `rates.resolve`. It must be recomputed on every transaction write. The viewer must not expose a manual `needs_review` toggle.

## LAN access

The viewer must default to binding `127.0.0.1`. Binding `0.0.0.0` requires a non-empty bearer token enforced via middleware before any route handler executes.

## Rationale

Keeps CLI and viewer write paths in lockstep (rule-004 spirit), preserves the rate-resolver invariants (rule-005), preserves the categorization-engine invariants (rule-006), and prevents the viewer from becoming a second front-door whose semantics drift from the canonical ones.
