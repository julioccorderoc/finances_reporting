# Rule 011 — TDD Discipline + Coverage Floor

**Source ADR:** [ADR-011](../../ADR/ADR-011-tdd-discipline-and-testing-stack.md)
**Scope:** All Wave 2 epics and beyond. Wave 0/1 bootstrap epics (EPIC-001, EPIC-002, EPIC-003, EPIC-002b) are exempt from the TDD-evidence rule (commit ordering); they remain subject to coverage thresholds.

## Constraints

1. **Tests per function.** Every public function in the epic's owned modules has ≥1 happy-path test AND ≥1 failure-mode test.
2. **Coverage thresholds:**
   - `finances/domain/**` ≥ 85%
   - `finances/ingest/**` ≥ 70%
   - `finances/cli/**` and `finances/reports/**` — no gate in v1 (revisit after EPIC-021).
3. **TDD evidence.** Test commits precede implementation commits in the branch history. Reviewer or CI can audit by walking the branch.

## Stack (locked, do not substitute)

- Runner: `pytest`
- Coverage: `pytest-cov` (CI uses `--cov-fail-under=<threshold>`)
- Property-based: `hypothesis` (mandatory for rate resolver, categorization engine)
- Pydantic factories: `polyfactory`
- HTTP mocking: `responses` when code uses `requests`; `pytest-mock` (`mocker.patch.object(<module>.httpx, "Client")`) when code uses `httpx`. **BCV and P2P scrapers use `httpx` per ADR-007 / EPIC-010 and therefore cannot be mocked with `responses` — use `pytest-mock`. See [MEMORY.md](../../../MEMORY.md) 2026-04-19 entry for the canonical mock shape.**
- SDK mocking: `pytest-mock` (Binance)

## Exemptions

- Snapshot-based tests (e.g. parsing `tests/fixtures/bcv_snapshot.html`) are test-after by nature. Tag with `@pytest.mark.snapshot` so the TDD-evidence audit ignores them.
- Smoke tests that exercise wiring rather than logic may use `@pytest.mark.smoke`.
- Documented unreachable defensive branches may use `# pragma: no cover`.

## CI enforcement

`pytest --cov=finances --cov-fail-under=85 --cov-config=pyproject.toml` runs on every PR. The `pyproject.toml` `[tool.coverage]` section sets per-package thresholds. PR merge is blocked on failure.
