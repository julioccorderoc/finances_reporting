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

## Process lifecycle

**Source:** ADR-012 Amendment — 2026-07-26.

`finances serve` runs the viewer under a reload supervisor on the localhost
bind. The following are enforced by tests and must not be relaxed:

1. The reload child rebuilds `WebSettings` from the environment and **must
   fail closed**. `host` is never defaulted in a child and never non-localhost:
   the default value is the one that disables the bearer middleware.
2. Any new `WebSettings` field must be added to `_FIELD_TO_ENV` in the same
   commit. A field missing from that mapping does not raise — it silently
   reverts to its default in the child.
3. `BearerTokenMiddleware` stays the **last** `add_middleware` call, because
   `add_middleware` prepends. Anything registered after it wraps outside auth
   and would answer unauthenticated requests.
4. The reload watch set stays scoped to `finances/`. Generated artifacts
   (`report.html`, `finances.db`) must remain outside every watched directory
   — a watched `*.html` written on shutdown is an unbreakable restart loop.
5. Every `TemplateResponse(...)` call site passes a literal template name and
   a literal dict, and must supply every variable the template requires
   *including through its includes*. A template variable that is genuinely
   optional is declared so with `{% if x is defined %}`, which the contract
   test then honours. Sprinkling `is defined` to silence CI hollows the
   contract out — treat new guards as reviewable.
6. The template environment does not hot-reload (`env.auto_reload = False`).
   A process serves one coherent snapshot of code and markup, or it restarts.

## Rationale

Keeps CLI and viewer write paths in lockstep (rule-004 spirit), preserves the rate-resolver invariants (rule-005), preserves the categorization-engine invariants (rule-006), and prevents the viewer from becoming a second front-door whose semantics drift from the canonical ones.
