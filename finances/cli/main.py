from __future__ import annotations

from datetime import datetime, time
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

import typer

from finances.config import DB_PATH, binance_credentials
from finances.db.connection import get_connection
from finances.db.migrate import apply_migrations

app = typer.Typer(help="Finances reporting CLI")

cash_app = typer.Typer(help="Cash USD entries (rule-008 — v1 supports USD cash only).")
app.add_typer(cash_app, name="cash")

ingest_app = typer.Typer(help="Ingest data from external sources.")
app.add_typer(ingest_app, name="ingest")


@app.callback()
def _root() -> None:
    """Finances reporting CLI."""


def _make_binance_client() -> Any:
    from binance.spot import Spot

    api_key, api_secret = binance_credentials()
    return Spot(api_key=api_key, api_secret=api_secret)


@ingest_app.command("binance")
def ingest_binance(
    since: datetime | None = typer.Option(
        None,
        "--since",
        help="ISO timestamp to start ingest from (overrides lookback and stored state).",
    ),
    lookback_days: int = typer.Option(
        35,
        "--lookback-days",
        help="Fallback window when no --since and no stored import_state (default 35).",
    ),
) -> None:
    """Incrementally sync Binance endpoints into the ledger (EPIC-007)."""
    from finances.ingest.binance import sync_binance

    conn = get_connection(DB_PATH)
    apply_migrations(conn)
    try:
        client = _make_binance_client()
        result = sync_binance(
            conn, client=client, since=since, lookback_days=lookback_days
        )
    finally:
        conn.close()

    typer.echo(
        f"binance sync: inserted={result['rows_inserted']} "
        f"updated={result['rows_updated']} "
        f"earn={result['earn_positions']} errors={len(result['errors'])}"
    )
    for err in result["errors"]:
        typer.echo(f"  err: {err}", err=True)
    if result["errors"]:
        raise typer.Exit(code=1)


@ingest_app.command("bcv")
def ingest_bcv() -> None:
    """Fetch BCV reference rates (USD/VES, EUR/VES) and upsert into rates (EPIC-009)."""
    from finances.ingest.bcv import ingest_bcv as run_bcv

    conn = get_connection(DB_PATH)
    apply_migrations(conn)
    try:
        inserted = run_bcv(conn)
    finally:
        conn.close()
    typer.echo(f"bcv: inserted {inserted} rate rows")


def _parse_cash_amount(raw: str) -> Decimal:
    try:
        return Decimal(raw)
    except InvalidOperation as exc:
        raise typer.BadParameter(f"amount must be a number: {raw!r}") from exc


def _parse_cash_date(raw: str | None) -> datetime:
    from finances import config as _config

    if raw is None:
        today = datetime.now(tz=_config.CARACAS_TZ).date()
        return datetime.combine(today, time(0, 0), tzinfo=_config.CARACAS_TZ)
    try:
        parsed_date = datetime.strptime(raw, "%Y-%m-%d").date()
    except ValueError as exc:
        raise typer.BadParameter(f"--date must be YYYY-MM-DD: {raw!r}") from exc
    return datetime.combine(parsed_date, time(0, 0), tzinfo=_config.CARACAS_TZ)


@cash_app.command("add")
def cash_add(
    amount: str = typer.Option(
        ..., "--amount", "-a", help="Positive USD amount (expense sign is applied)."
    ),
    description: str = typer.Option(
        ..., "--description", "-d", help="Short note describing the expense."
    ),
    date: str | None = typer.Option(
        None,
        "--date",
        help="Date of expense in YYYY-MM-DD (Caracas TZ). Defaults to today.",
    ),
    category: str | None = typer.Option(
        None, "--category", help="Optional expense category name."
    ),
    account: str | None = typer.Option(
        None,
        "--account",
        help="Reserved; must be 'Cash USD' (rule-008). Leave unset for the default.",
    ),
) -> None:
    """Record a USD cash expense on the Cash USD account (EPIC-011)."""
    from finances import config as _config
    from finances.db.connection import get_connection
    from finances.db.repos import categories as categories_repo
    from finances.domain.models import TransactionKind
    from finances.ingest.cash_cli import (
        CASH_USD_ACCOUNT_NAME,
        add_cash_expense,
        suggest_recent_categories,
    )

    if account is not None and account != CASH_USD_ACCOUNT_NAME:
        typer.echo(
            f"--account must be {CASH_USD_ACCOUNT_NAME!r} (rule-008); got {account!r}.",
            err=True,
        )
        raise typer.Exit(code=2)

    amount_decimal = _parse_cash_amount(amount)
    if amount_decimal <= 0:
        typer.echo("--amount must be a positive number of USD.", err=True)
        raise typer.Exit(code=2)

    occurred_at = _parse_cash_date(date)

    conn = get_connection(_config.DB_PATH)
    try:
        category_id: int | None = None
        if category is not None:
            found = categories_repo.get_by_name(
                conn, TransactionKind.EXPENSE, category
            )
            if found is None:
                typer.echo(
                    f"category {category!r} not found among expense categories.",
                    err=True,
                )
                raise typer.Exit(code=2)
            category_id = found.id

        txn = add_cash_expense(
            conn,
            amount=amount_decimal,
            description=description,
            occurred_at=occurred_at,
            category_id=category_id,
        )
        recent = suggest_recent_categories(conn, txn.account_id, limit=3)
    finally:
        conn.close()

    typer.echo(
        f"Recorded cash expense id={txn.id} amount={txn.amount} {txn.currency} "
        f"on {CASH_USD_ACCOUNT_NAME} ({txn.occurred_at.date().isoformat()})."
    )
    if recent:
        hints = ", ".join(c.name for c in recent)
        typer.echo(f"Recent categories on this account: {hints}")


@app.command("categorize")
def categorize(
    dry_run: bool = typer.Option(
        False,
        "--dry-run",
        help="Report what would match without writing category_id changes.",
    ),
    source: str | None = typer.Option(
        None,
        "--source",
        help="Restrict to transactions with this source (e.g. 'provincial', 'binance').",
    ),
    db_path: Path = typer.Option(
        None,
        "--db-path",
        help="Path to the SQLite database. Defaults to finances.config.DB_PATH.",
    ),
) -> None:
    """Run the categorization engine over stored transactions (EPIC-004).

    In ``--dry-run`` mode, prints how many rows the seeded rules would classify
    without mutating any data — used to measure rule coverage before running
    the backfill.
    """
    from finances.domain.categorization import CategorizationRequest, suggest

    target_db = Path(db_path) if db_path is not None else Path(DB_PATH)
    conn = get_connection(target_db)
    apply_migrations(conn)
    try:
        params: tuple[Any, ...]
        if source is not None:
            query = (
                "SELECT id, description, source, account_id "
                "FROM transactions WHERE source = ?"
            )
            params = (source,)
        else:
            query = "SELECT id, description, source, account_id FROM transactions"
            params = ()
        rows = conn.execute(query, params).fetchall()

        total = len(rows)
        matched = 0
        for row in rows:
            if suggest(
                conn,
                CategorizationRequest(
                    description=row["description"],
                    source=row["source"],
                    account_id=row["account_id"],
                ),
            ) is not None:
                matched += 1
    finally:
        conn.close()

    scope = source or "all sources"
    if total == 0:
        typer.echo(f"categorize dry-run [{scope}]: no transactions to categorize.")
        return

    pct = matched / total * 100.0
    if dry_run:
        typer.echo(
            f"categorize dry-run [{scope}]: {matched}/{total} = {pct:.1f}% "
            "would auto-classify."
        )
        return

    typer.echo(
        "Refusing to write: re-categorization is owned by EPIC-012 backfill.\n"
        "Re-run with --dry-run to see preview counts.",
        err=True,
    )
    raise typer.Exit(code=2)


if __name__ == "__main__":
    app()
