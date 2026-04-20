# ADR-008: Cash Tracking Limited to USD Physical Cash, Entered via CLI (Telegram Bot Later)

**Date:** 2026-04-19
**Status:** Accepted

## 1. Context

Cash tracking is missing from the current system. The user clarified that bolívar cash flows through the Provincial bank account (cash-in/cash-out via ATM is captured by the bank statement), so the only true unobserved cash channel is **USD physical cash** — which is low-frequency but currently invisible.

Capture-mechanism options considered:

- Mobile receipt parser (future, EPIC-016).
- Quick CLI prompt on the workstation.
- Direct row in a Sheets "Cash" tab.
- Telegram/iMessage bot.

Sheets-direct violates ADR-001 (no human edits to the mirror). Mobile and Telegram are correctly future work. The CLI is sufficient for the current low frequency.

## 2. Decision

For v1, support **USD-cash** entries only, via `finances cash add`, an interactive Typer subcommand with optional flags for non-interactive use. Bs-cash is intentionally out of scope (it flows through the bank). A Telegram bot (EPIC-015) and mobile-app receipt API (EPIC-016) are deferred to v2.

## 3. Consequences (The "Why")

### Positive

- Matches actual usage frequency — USD cash is rare enough that a CLI is fine.
- Avoids inventing a Bs-cash model that would duplicate bank-tracked flows.
- Single account `Cash USD` keeps the schema simple.
- The CLI command is the same code path the future Telegram bot will call, so v2 doesn't require a rewrite.

### Negative

- Recording cash requires being at the workstation; if the user pays cash on the road, the entry waits until they're back.
- Two future ingestion channels (Telegram, mobile) are explicit deferrals; until they ship, mobile-while-away cash entries simply queue mentally.

## 4. Rule Extraction (The "How" for Agents)

**Target File:** `docs/architecture/rules/rule-008-cash-account-scope.md`
**Injected Constraint:** Only one cash account exists in v1: `Cash USD` (currency=`USD`, kind=`cash`). Any introduction of a `Cash Bs` or any other cash account in v1 violates ADR-008 and requires a new ADR. The `finances/ingest/cash_cli.py` module must default the target account to `Cash USD` and reject `--account` flags pointing elsewhere.
