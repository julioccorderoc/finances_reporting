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

## 5. Addendum (2026-04-27): Endpoint Migration to BCV Homepage

The original target page (`/estadisticas/tipo-de-cambio-de-referencia-smc`) returns HTTP 404 as of April 2026. BCV moved the daily reference rates onto the homepage (`https://www.bcv.org.ve/`).

### Implementation changes

- `BCV_URL` is now `https://www.bcv.org.ve/`.
- `parse_bcv_html` rewritten for the homepage's div-based layout: rates live under `<div id="dolar">` / `<div id="euro">`, the value date is read from the `content` attribute of `<span class="date-display-single">` (ISO format), and value strings carry 8 decimal places (e.g. `485,22510000`). USD and EUR are extracted; CNY/TRY/RUB blocks on the homepage are ignored per current scope.
- A new dependency `truststore>=0.10` (added to `pyproject.toml`) backs the httpx client's SSL verification. BCV's server ships an incomplete certificate chain and certifi alone cannot validate it; `truststore.SSLContext` delegates verification to the OS native trust store, which performs AIA fetching to recover the missing intermediates. macOS, Windows, and Linux all work.

### Cadence (clarification, not a change)

The homepage publishes one snapshot — today's rates. This was always the case for the human-canonical surface; the previous endpoint's apparent multi-day history was incidental to that endpoint and is not a guarantee BCV ever made. Daily cadence is therefore required to capture every business day.

### Historical backfill (out of scope)

BCV remains the canonical source for current rates. When historical rates are needed (one-off or periodic), they are sourced from `https://www.tcambio.app/historial-tasas-bcv/year-YYYY`. That backfill is **not** part of `finances/ingest/bcv.py` and is **not** routed through the daily scrape contract above. A future EPIC may automate it; treat as manual until then.

### Failure-mode rule unchanged

Rule-007 (parse failure → `import_runs.error`, non-zero exit, no `rates` mutation) applies verbatim under the new endpoint. The new parser raises `BcvParseError` whenever any required element (`#dolar`, `#euro`, or the date span) is missing or unparseable, and the existing wrapping in `ingest_bcv` routes that into the rule-007 path.

### `--dry-run` (related but orthogonal)

Concurrent with this addendum, `ingest_bcv` gained a `dry_run: bool = False` keyword arg. Dry-run skips `start_run`/`finish_run`/`upsert_state` and replaces the terminal `COMMIT` with `ROLLBACK`. It is the safe way for an automated agent to verify the live scrape works without polluting `rates` or `import_runs`. See the BCV pilot tests in `tests/test_ingest_bcv.py`.
