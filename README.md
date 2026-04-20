# finances_reporting

Personal finances ledger. SQLite is the source of truth; Google Sheets is a
read-only mirror. CLI-driven ingest and reporting.

Authoritative context: see [CLAUDE.md](./CLAUDE.md), [docs/PRD.md](docs/PRD.md),
[docs/roadmap.md](docs/roadmap.md), and the ADRs under [docs/ADR/](docs/ADR/).

## Setup

```bash
uv pip install -e .
python -m finances.db.migrate     # create finances.db and apply migrations
finances --help
```

### Environment

Configured via `.env` at the project root:

- `BINANCE_API_KEY` / `BINANCE_API_SECRET` — used by `finances ingest binance`.
- `GOOGLE_SERVICE_ACCOUNT_FILE` (path to a service-account JSON key file)
  **or** `GOOGLE_SERVICE_ACCOUNT_JSON` (inline JSON) — used by
  `finances sync sheets`. The service account must be shared on the target
  spreadsheet as an editor. File path wins when both are set.

## CLI

```bash
finances ingest binance                       # incremental Binance sync
finances ingest provincial <csv>              # bank CSV ingest + P2P pairing
finances ingest bcv                           # BCV reference-rate scrape
finances ingest p2p-rates                     # Binance P2P median rate
finances cash add --amount 12 --description "lunch"
finances report balances
finances report consolidated [--strict]
finances report monthly
finances report needs-review
finances sync sheets --spreadsheet-id <id>    # read-only Sheets mirror (EPIC-014)
finances backfill --from data/                # one-time historical import
```

## Tests

```bash
pytest -q                    # unit suite
pytest -m integration        # end-to-end pipeline (EPIC-021)
pytest --cov                 # coverage gate per rule-011
```
