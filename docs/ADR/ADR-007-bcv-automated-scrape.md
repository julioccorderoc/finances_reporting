# ADR-007: Automated Daily BCV Scrape (Replace Manual HTML Save)

**Date:** 2026-04-19
**Status:** Accepted

## 1. Context

`extract_bcv.py` currently parses `table_bcv.html`, a file the user manually downloads from the BCV website and saves into the project directory. This is a recurring chore and is the only step in the entire pipeline that requires opening a browser. The user does not actively use BCV rates (P2P realized rates win per ADR-005), but BCV is required as the last-resort fallback in the rate resolution chain and is occasionally needed for reference.

Three options:

1. Replace the manual save with a live HTTP scraper that runs daily and caches into `rates`.
2. Leave the manual workflow alone since BCV is reference-only.
3. Drop BCV entirely.

Option 2 keeps a sharp manual edge that the user already wants to file off. Option 3 removes the fallback that ADR-005 depends on.

## 2. Decision

Implement `finances/ingest/bcv.py` as a live HTTP scrape using `httpx` + `BeautifulSoup`. Parse USD and EUR rows from the BCV exchange-rate page, write them to `rates(base IN ('USD','EUR'), quote='VES', source='bcv')`. Run daily as part of the standard `finances ingest all` flow. On parse failure, log to `import_runs.error`, exit non-zero, and **do not** mutate existing `rates` rows. Keep `legacy/extract_bcv.py` and `legacy/table_bcv.html` until the live scraper has been observed working for at least 14 days.

## 3. Consequences (The "Why")

### Positive

- Zero recurring manual work for BCV.
- BCV remains available as the rate-resolution fallback per ADR-005.
- Failure mode is explicit (non-zero exit, error in `import_runs`), not silent.

### Negative

- Adds a network dependency to the ingest pipeline; the BCV site can be slow or down.
- If BCV changes the page structure, the scraper breaks until updated.
- Need to decide on a retry/backoff policy for transient failures (deferred to implementation; default: one retry with 5s backoff, then fail and flag).

## 4. Rule Extraction (The "How" for Agents)

**Target File:** `docs/architecture/rules/rule-007-bcv-scrape-failure-mode.md`
**Injected Constraint:** `finances/ingest/bcv.py` must, on parse failure: (a) write a row to `import_runs` with `status='error'` and a populated `error` column, (b) exit with a non-zero status code, and (c) leave existing `rates` rows untouched. It is forbidden to write fallback or estimated values into `rates(source='bcv')` when the scrape fails.
