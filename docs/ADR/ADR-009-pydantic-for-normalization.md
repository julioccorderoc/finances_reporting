# ADR-009: Pydantic for Normalization at All Trust Boundaries

**Date:** 2026-04-19
**Status:** Accepted

## 1. Context

Data flows into the system from four heterogeneous sources (Binance API JSON, Provincial bank CSV, BCV HTML scrape, Cash CLI prompts) plus, in the future, a mobile-app HTTP body. Each source has its own field names, types, date formats, decimal conventions, and partial nullability. Without a single normalization layer, every ingester and downstream consumer must defensively parse and validate, leading to inconsistent treatment of edge cases (e.g. `Decimal("0.00")` vs. `0.0` vs. `"0,00"`, naive vs. aware datetimes, currency code casing).

Two options:

1. **Plain dataclasses + ad-hoc parsers per ingester.** What `legacy/extract_provincial.py` does today.
2. **Pydantic v2 models as the canonical normalization layer.** Every external input is parsed into a `RawX` model with strict validators; the validated model is converted to a domain `Transaction` (also Pydantic) before reaching the repo.

## 2. Decision

Use **Pydantic v2** for all data structures that cross trust boundaries. Specifically:

- `finances/domain/models.py` defines `Account`, `Category`, `Transaction`, `Rate`, `EarnPosition` as Pydantic `BaseModel` subclasses with strict validators (e.g. `Decimal` coercion, ISO-8601 datetimes, currency-code uppercasing, sign validation).
- Each ingester defines its own `Raw<Source>Row` Pydantic model that mirrors the source's field names and types, then maps to the canonical `Transaction` via a `to_transaction()` method.
- Repos accept and return Pydantic instances; they translate to/from SQLite rows internally.
- Pydantic validation errors become explicit `ValidationError` exceptions that bubble to `import_runs.error`, never silently coerce.

## 3. Consequences (The "Why")

### Positive

- One place to enforce normalization rules (currency casing, sign conventions, date timezones).
- Future mobile-app POST endpoint gets request validation for free.
- Pydantic's `model_validate` + JSON-schema export simplifies any future API.
- Tests are easier — invalid inputs raise immediately rather than corrupting downstream data.

### Negative

- Adds Pydantic v2 as a hard dependency.
- Slight runtime cost vs. raw dicts; negligible at this volume.
- Developers must remember to validate at the boundary, not in the middle of business logic.

## 4. Rule Extraction (The "How" for Agents)

**Target File:** `docs/architecture/rules/rule-009-pydantic-at-boundaries.md`
**Injected Constraint:** All data crossing a trust boundary (external API response, CSV row, HTTP request body, CLI prompt) must be parsed into a Pydantic model before reaching domain or repo code. No raw `dict` may be passed into `finances/db/repos/*` functions. Domain models in `finances/domain/models.py` are Pydantic v2 `BaseModel` subclasses; replacing them with `dataclass` or `TypedDict` is forbidden.
