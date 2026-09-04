# Rule 008 — Cash Account Scope

**Source ADR:** [ADR-008](../../ADR/ADR-008-cash-usd-only-cli.md) (+ Amendment 2026-09-03)
**Scope:** Account creation, the cash module, and every manual write surface.

**Constraint:** v1 has exactly one cash account: `Cash USD` (currency=`USD`, kind=`cash`). Introducing a `Cash Bs` or any other cash-kind account in v1 violates this rule and requires a new ADR. `finances/ingest/cash_cli.py` defaults to `Cash USD` and rejects `--account` flags pointing to non-USD-cash accounts.

**Constraint (Amendment 2026-09-03):** `Cash USD` is written by the cash module — `cash_cli.add_cash_expense` / `cash_cli.add_cash_income` — called from the CLI **or** from the viewer's Add-transaction dialog. No importer writes to it, and no surface may write to it except through that module: one `source='cash_cli'`, one UUIDv4 `source_ref` scheme (rule-010), the expense sign applied in one place. Every other account is import-only, because it mirrors an outside record. A viewer surface that offers manual entry must still list every active account, disable each non-cash one with the reason it is closed, and refuse a non-cash account **server-side** — the disabled `<option>` is a courtesy, never the guard. Making a second account hand-writable requires a new ADR.
