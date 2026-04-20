# Rule 009 — Pydantic at All Trust Boundaries

**Source ADR:** [ADR-009](../../ADR/ADR-009-pydantic-for-normalization.md)
**Scope:** All ingesters, repos, CLI commands, and any future HTTP endpoints.

**Constraint:**

- Every external input (API JSON, CSV row, HTTP body, CLI prompt) is parsed into a Pydantic v2 `BaseModel` before reaching domain or repo code.
- Domain models in `finances/domain/models.py` (`Account`, `Category`, `Transaction`, `Rate`, `EarnPosition`) are Pydantic `BaseModel` subclasses. Replacing any of them with `@dataclass` or `TypedDict` is forbidden.
- Repos under `finances/db/repos/` accept and return only Pydantic instances; passing a raw `dict` is forbidden.
- Validation errors raise `pydantic.ValidationError` and are recorded into `import_runs.error`; silent coercion is forbidden.
