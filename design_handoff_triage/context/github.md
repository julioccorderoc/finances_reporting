# github.md

repo: julioccorderoc/finances_reporting
branch: main

The Ledger prototype in `ui_kits/finances/` is being built as the replacement UI
for this repo's local web viewer (`finances/web/`). SQLite is the source of
truth; the prototype currently runs on fixture data in `data.js`.

## Last sync

date: 2026-08-17T19:10:48Z

### Updated in this project

- Read the repo for the first time — no code imported, no screens changed.
- Wrote `REPO-RECONCILE.md`: what the prototype has wrong, what it is ahead of,
  and what the repo tracks with nowhere to show it.
- Triage is the one surface worth keeping faithful — and the prototype is missing
  most of it (rate items, pair items, difficulty order, Park).
- Plans and Ahead are the roadmap, not errors; each needs a table that does not
  exist yet. Cashea installments are the clearest one.

## Screen map

| Prototype screen | Repo files it must answer to |
| --- | --- |
| `ReviewModal.jsx` | `finances/web/services/triage.py`, `finances/domain/categorization.py`, `finances/domain/rates.py`, `finances/domain/triage_admin.py` |
| `FlowScreen.jsx` | `finances/web/services/transactions_query.py`, `finances/db/repos/transactions.py`, `finances/db/repos/saved_views.py` |
| `AccountsScreen.jsx` | `finances/web/services/accounts_view.py`, `finances/web/services/net_worth.py`, `v_account_balances` (001_initial.sql), ADR-018, ADR-020 |
| `TodayScreen.jsx` | `finances/web/services/dashboard.py` — safe-to-spend has no source |
| `PlansScreen.jsx`, `ForecastView.jsx` | none yet — the roadmap; see `REPO-RECONCILE.md` §B1 for the tables each needs |
| `AddTxModal.jsx` | `finances/web/services/transactions_write.py`, `finances/ingest/cash_cli.py`, `finances/domain/transfers.py` |
| `data.js` categories + rules | `finances/db/migrations/002_seed_categories.sql`, `017_amount_scoped_rules.sql`, `docs/architecture/category-definitions.md` |
| `Fin.Amount`, date rendering | `finances/format.py`, `docs/plans/ux-overhaul/00-design.md` |
