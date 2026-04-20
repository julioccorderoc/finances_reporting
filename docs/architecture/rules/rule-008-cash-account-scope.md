# Rule 008 — Cash Account Scope

**Source ADR:** [ADR-008](../../ADR/ADR-008-cash-usd-only-cli.md)
**Scope:** Account creation and the cash CLI.

**Constraint:** v1 has exactly one cash account: `Cash USD` (currency=`USD`, kind=`cash`). Introducing a `Cash Bs` or any other cash-kind account in v1 violates this rule and requires a new ADR. `finances/ingest/cash_cli.py` defaults to `Cash USD` and rejects `--account` flags pointing to non-USD-cash accounts.
