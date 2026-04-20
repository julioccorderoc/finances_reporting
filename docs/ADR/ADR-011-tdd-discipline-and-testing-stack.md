# ADR-011: Test-Driven Development Discipline + Testing Stack

**Date:** 2026-04-19
**Status:** Accepted

## 1. Context

Wave 2 of the roadmap launches up to eight epics in parallel under separate agents. Each epic carries a "Tests:" line and a `pytest is green` verification gate. That is not enough:

- A trivial test that exercises only the happy path passes the gate without meaningful coverage.
- Fixture, mock, and Pydantic-factory conventions diverge across modules when not centralized.
- Regressions surface only in EPIC-012 (backfill), where ~1,000 real rows hit untested paths and reproductions are expensive.
- There is no CI to enforce anything; nothing fails fast.

Two options:

1. **Bake testing into each epic's "Tests:" line and trust the verification gate.** This is what we have. It is what produces "trivial shim" failures.
2. **A small dedicated infrastructure epic + an explicit TDD discipline + a CI-enforced coverage floor.** Higher up-front cost; meaningful confidence floor.

EPIC-002 and EPIC-003 are already in flight when this ADR is written and are grandfathered (see Consequences).

## 2. Decision

Adopt **test-driven development** as the default discipline for every Wave 2 epic and beyond, backed by a shared testing infrastructure landed in a new **EPIC-002b**.

**Stack (locked):**

- `pytest` — test runner.
- `pytest-cov` — coverage measurement and CI gate.
- `hypothesis` — property-based tests for the rate resolver and the categorization engine (both have non-trivial branching that benefits from generated inputs).
- `polyfactory` — Pydantic model factories (works natively with ADR-009 models).
- `responses` — HTTP mocking for the BCV scraper and Binance P2P fetcher.
- `pytest-mock` — used sparingly for the Binance SDK.

**TDD discipline:**

- Red → green → refactor. Each failing test is committed before the implementation that makes it pass.
- Every public function in an epic's owned modules ships with ≥1 happy-path test AND ≥1 failure-mode test.
- Coverage gate: **≥85%** for `finances/domain/**`, **≥70%** for `finances/ingest/**`. No gate (yet) for `finances/cli/**` or `finances/reports/**`.
- CI runs `pytest --cov --cov-fail-under=...` on every PR; merge is blocked on failure.

**Exemptions (called out explicitly):**

- Snapshot-based tests for the BCV scraper are inherently test-after — you cannot write a test for HTML you have not snapshotted. Tag with `@pytest.mark.snapshot`.
- Wave 0/1 bootstrap epics (EPIC-001, EPIC-002, EPIC-003, EPIC-002b itself) are exempt from the TDD-evidence rule (commit ordering). They remain subject to coverage thresholds. EPIC-002b includes a one-time pass to bring EPIC-002 coverage up to threshold retroactively.

## 3. Consequences (The "Why")

### Positive

- The "pytest is green" gate becomes meaningful — a passing build implies real coverage and at least two tests per public function.
- Property-based tests on the rate resolver catch branching bugs that example-based tests miss (the priority chain has 4+ paths).
- Shared fixtures and factories mean an epic agent does not invent its own fixture style; switching between epics has zero friction.
- CI enforcement means the discipline is auditable, not aspirational.
- Bug discovery shifts left from EPIC-012 (backfill, against real data) to per-epic unit tests.

### Negative

- ~20-30% extra time per Wave 2 epic.
- Coverage gates can fight legitimate defensive branches that cannot be triggered. Mitigation: `# pragma: no cover` is permitted on documented unreachable paths.
- Some up-front boilerplate (conftest, factories) before payback.
- TDD-evidence rule (commit ordering) requires agents to discipline their commit cadence; some will resist.

## 4. Rule Extraction (The "How" for Agents)

**Target File:** `docs/architecture/rules/rule-011-tdd-discipline.md`
**Injected Constraint:** Every Wave 2 epic and beyond appends the TDD discipline checklist (3 items: tests-per-function, coverage threshold, commit ordering) to its Verification Criteria. CI enforces the coverage threshold. Tests exempt from TDD-evidence (snapshot, smoke) must be tagged with the `@pytest.mark.snapshot` or `@pytest.mark.smoke` marker so the audit can ignore them.
