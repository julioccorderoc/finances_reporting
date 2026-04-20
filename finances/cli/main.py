from __future__ import annotations

from datetime import datetime, time
from decimal import Decimal, InvalidOperation

import typer

app = typer.Typer(help="Finances reporting CLI")

cash_app = typer.Typer(help="Cash USD entries (rule-008 — v1 supports USD cash only).")
app.add_typer(cash_app, name="cash")


@app.callback()
def _root() -> None:
    """Finances reporting CLI."""


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


if __name__ == "__main__":
    app()
